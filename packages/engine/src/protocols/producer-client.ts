/**
 * The producer's sender: queuing, retries, timeouts and idempotence (M4-T7, M4 Q7; Kafka's
 * RecordAccumulator, Sender and TransactionManager; M4-decisions K7, K8).
 *
 * Every record the application sends (acks=1 or all) goes into its partition's queue. The sender
 * sends queued records to the partition's leader as the producer's metadata knows it, with at
 * most `max.in.flight.requests.per.connection` (5) unanswered requests per partition. Each record
 * is its own request (no batching, see simplifications).
 *
 * - An answer with a **retriable** error (NOT_LEADER_OR_FOLLOWER, NOT_ENOUGH_REPLICAS, a timeout,
 *   a dropped connection, …) puts the record back in the queue. The partition sends nothing more
 *   until its in-flight requests are answered, waits retry.backoff.ms × 2^(attempt−1) (capped at
 *   retry.backoff.max.ms, ±20 % jitter, KIP-580), or not at all when its leader changed
 *   (KIP-951), then sends the queue again in order. ProduceRetried marks each retry.
 * - InvalidMetadataException errors, client-side timeouts and dropped connections make the
 *   producer refresh its metadata (K7); NOT_ENOUGH_REPLICAS does not.
 * - A record not acknowledged within delivery.timeout.ms of send() fails (DELIVERY_TIMEOUT), as
 *   does one whose retries run out or whose error is not retriable.
 * - The client waits request.timeout.ms for each answer (REQUEST_TIMED_OUT, client side).
 * - A partition with no known leader, or whose leader's process is down (connection refused),
 *   sends nothing and looks again after retry.backoff.ms.
 *
 * Idempotence (`enable.idempotence`, the 4.x default): records carry the producer id (PID), the
 * producer epoch and a per-partition sequence number. The leader appends only the next sequence,
 * answers a retried duplicate with its original offset, and answers a gap with
 * OUT_OF_ORDER_SEQUENCE_NUMBER; then the producer bumps its epoch and renumbers the partition's
 * unacknowledged records from 0 (KIP-360, K8). As in Kafka 4.x, idempotence is on only with
 * acks=all: a producer configured with acks=0/1 (and enable.idempotence left at its default)
 * runs without it, so its retries can write duplicates.
 *
 * acks=0 records are fire and forget: sent once to the known leader, never retried.
 */

import type { Acks } from '@kp/schema';
import type { EngineContext } from '../model/context.ts';
import type { LogRecord } from '../model/partition.ts';
import { isAlive, type ProducerState } from '../model/state.ts';
import type { EngineTask, ProduceError } from '../model/tasks.ts';
import { knownLeader, refreshIfStale } from './metadata.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/** max.in.flight.requests.per.connection default (per partition here: one record per request). */
export const MAX_IN_FLIGHT = 5;

/** reconnect.backoff.ms default: the wait before connecting again to a broker that refused. */
export const RECONNECT_BACKOFF_MS = 50;

/** A record the producer has not finished with: queued or in flight. */
export interface PendingRecord {
  readonly traceId: string;
  readonly key: string;
  readonly topic: string;
  readonly partition: number;
  readonly record: Omit<LogRecord, 'offset' | 'leaderEpoch' | 'pid' | 'producerEpoch' | 'sequence'>;
  readonly acks: Exclude<Acks, '0'>;
  readonly idempotent: boolean;
  /** Send order within the producer: the queue order for non-idempotent records. */
  readonly order: number;
  sequence: number;
  attempts: number;
  /** The broker of the last attempt (to notice a leader change, KIP-951). */
  lastBroker: string | null;
  inFlight: { readonly broker: string; readonly attempt: number } | null;
}

/** One partition's part of the accumulator and sender. */
export interface PartitionSender {
  readonly key: string;
  readonly topic: string;
  readonly partition: number;
  /** Idempotent producer epoch for this partition (bumped on OUT_OF_ORDER_SEQUENCE_NUMBER). */
  epoch: number;
  nextSeq: number;
  /** Waiting to be sent, in sequence (send) order. */
  readonly queue: PendingRecord[];
  /** Sent and not answered, in send order. */
  readonly inFlight: PendingRecord[];
  /** A retriable failure happened: send nothing until the in-flight requests are answered. */
  retrying: boolean;
  /** An OUT_OF_ORDER answer: bump the epoch and renumber before sending again. */
  bumpEpoch: boolean;
  backoffUntil: number;
  /** Only the ProducerDrain task with this token is current. */
  drainToken: number;
}

const senderOf = (producer: ProducerState, topic: string, partition: number): PartitionSender => {
  const key = `${topic}-${String(partition)}`;
  let sender = producer.senders.get(key);
  if (sender === undefined) {
    sender = {
      key,
      topic,
      partition,
      epoch: 0,
      nextSeq: 0,
      queue: [],
      inFlight: [],
      retrying: false,
      bumpEpoch: false,
      backoffUntil: 0,
      drainToken: 0,
    };
    producer.senders.set(key, sender);
  }
  return sender;
};

/** KafkaProducer#send for acks=1/all: queue the record and try to send it. */
export function enqueueRecord(
  ctx: EngineContext,
  producer: ProducerState,
  topic: string,
  partition: number,
  record: PendingRecord['record'],
  acks: Exclude<Acks, '0'>,
): void {
  const sender = senderOf(producer, topic, partition);
  const idempotent = acks === 'all' && producer.config.enableIdempotence;
  const pending: PendingRecord = {
    traceId: record.traceId,
    key: sender.key,
    topic,
    partition,
    record,
    acks,
    idempotent,
    order: producer.stats.sent,
    sequence: idempotent ? sender.nextSeq++ : -1,
    attempts: 0,
    lastBroker: null,
    inFlight: null,
  };
  producer.pending.set(record.traceId, pending);
  sender.queue.push(pending);
  ctx.kernel.scheduleIn(producer.config.deliveryTimeoutMs, {
    type: 'ProduceDeliveryTimeout',
    producerId: producer.config.id,
    incarnation: producer.incarnation,
    traceId: record.traceId,
  });
  drain(ctx, producer, sender);
}

/** Sends what the partition may send now, or schedules the next look. */
function drain(ctx: EngineContext, producer: ProducerState, sender: PartitionSender): void {
  const now = ctx.kernel.now();
  if (sender.inFlight.length > 0 && (sender.retrying || sender.bumpEpoch)) return;
  sender.retrying = false;
  if (sender.bumpEpoch) {
    // KIP-360: a new epoch starts at sequence 0; renumber what is left, in order.
    sender.bumpEpoch = false;
    sender.epoch += 1;
    sender.queue.sort((a, b) => a.order - b.order);
    sender.queue.forEach((r, i) => {
      if (r.idempotent) r.sequence = i;
    });
    sender.nextSeq = sender.queue.filter((r) => r.idempotent).length;
  }
  if (sender.queue.length === 0) return;
  if (now < sender.backoffUntil) {
    scheduleDrain(ctx, producer, sender, sender.backoffUntil);
    return;
  }
  for (const changed of refreshIfStale(ctx, producer.metadata, producer.config.metadataMaxAgeMs)) {
    producer.sticky.delete(changed);
  }
  const topic = ctx.state.topics.get(sender.topic);
  const leader =
    topic === undefined ? null : knownLeader(producer.metadata, topic, sender.partition);
  const broker = leader === null ? undefined : ctx.state.brokers.get(leader);
  if (leader === null || broker === undefined || !isAlive(broker.status)) {
    // No leader, or the connection is refused: refresh and look again after
    // reconnect.backoff.ms (or retry.backoff.ms, whichever is longer).
    producer.metadata.updateRequested = true;
    scheduleDrain(
      ctx,
      producer,
      sender,
      now + Math.max(RECONNECT_BACKOFF_MS, producer.config.retryBackoffMs),
    );
    return;
  }
  while (sender.inFlight.length < MAX_IN_FLIGHT) {
    const next = sender.queue.shift();
    if (next === undefined) break;
    sendAttempt(ctx, producer, sender, next, leader);
  }
}

function scheduleDrain(
  ctx: EngineContext,
  producer: ProducerState,
  sender: PartitionSender,
  at: number,
): void {
  sender.drainToken += 1;
  ctx.kernel.scheduleAt(Math.max(at, ctx.kernel.now()), {
    type: 'ProducerDrain',
    producerId: producer.config.id,
    incarnation: producer.incarnation,
    key: sender.key,
    token: sender.drainToken,
  });
}

function sendAttempt(
  ctx: EngineContext,
  producer: ProducerState,
  sender: PartitionSender,
  pending: PendingRecord,
  broker: string,
): void {
  pending.attempts += 1;
  pending.lastBroker = broker;
  pending.inFlight = { broker, attempt: pending.attempts };
  sender.inFlight.push(pending);
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'LeaderAppend',
    producerId: producer.config.id,
    incarnation: producer.incarnation,
    broker,
    topic: sender.topic,
    partition: sender.partition,
    acks: pending.acks,
    timeoutMs: producer.config.requestTimeoutMs,
    attempt: pending.attempts,
    record: {
      ...pending.record,
      pid: pending.idempotent ? producer.incarnation : -1,
      producerEpoch: pending.idempotent ? sender.epoch : -1,
      sequence: pending.sequence,
    },
  });
  ctx.kernel.scheduleIn(producer.config.requestTimeoutMs, {
    type: 'ProduceRequestTimeout',
    producerId: producer.config.id,
    incarnation: producer.incarnation,
    traceId: pending.traceId,
    attempt: pending.attempts,
  });
}

/** Errors the producer retries (RetriableException subclasses, K7). */
function isRetriable(error: ProduceError): boolean {
  switch (error) {
    case 'UNKNOWN_TOPIC_OR_PARTITION':
    case 'NOT_LEADER_OR_FOLLOWER':
    case 'NOT_ENOUGH_REPLICAS':
    case 'NOT_ENOUGH_REPLICAS_AFTER_APPEND':
    case 'REQUEST_TIMED_OUT':
    case 'NETWORK_EXCEPTION':
    case 'OUT_OF_ORDER_SEQUENCE_NUMBER':
      return true;
    case 'INVALID_PRODUCER_EPOCH':
    case 'DELIVERY_TIMEOUT':
      return false;
  }
}

/** Errors after which the producer refreshes its metadata (InvalidMetadataException, disconnects). */
function needsMetadata(error: ProduceError): boolean {
  return (
    error === 'UNKNOWN_TOPIC_OR_PARTITION' ||
    error === 'NOT_LEADER_OR_FOLLOWER' ||
    error === 'NETWORK_EXCEPTION' ||
    error === 'REQUEST_TIMED_OUT'
  );
}

/** ExponentialBackoff(retry.backoff.ms, 2, retry.backoff.max.ms, 0.2) for attempt n ≥ 1. */
function backoffMs(producer: ProducerState, attempts: number): number {
  const { retryBackoffMs, retryBackoffMaxMs } = producer.config;
  const base = Math.min(
    retryBackoffMs * 2 ** Math.max(0, attempts - 1),
    Math.max(retryBackoffMs, retryBackoffMaxMs),
  );
  const jitter = 0.8 + 0.4 * producer.retryRng.next();
  return Math.round(base * jitter);
}

/** The answer (or the client's verdict) for one send attempt. */
export function onAttemptResult(
  ctx: EngineContext,
  producer: ProducerState,
  traceId: string,
  attempt: number,
  result: Task<'ProduceResponse'>['result'],
): void {
  const pending = producer.pending.get(traceId);
  if (pending?.inFlight?.attempt !== attempt) return; // an older attempt, or already settled
  const sender = senderOf(producer, pending.topic, pending.partition);
  const index = sender.inFlight.indexOf(pending);
  if (index >= 0) sender.inFlight.splice(index, 1);
  pending.inFlight = null;
  const { config } = producer;
  if (result.ok) {
    producer.pending.delete(traceId);
    producer.stats.acked += 1;
    ctx.state.ackedTraceIds.add(traceId);
    ctx.emit({
      type: 'ProduceAcked',
      producerId: config.id,
      topic: pending.topic,
      partition: pending.partition,
      traceId,
      offset: result.offset,
    });
    drain(ctx, producer, sender);
    return;
  }
  const { error } = result;
  if (needsMetadata(error)) producer.metadata.updateRequested = true;
  if (!isRetriable(error) || pending.attempts > config.retries) {
    fail(ctx, producer, pending, error);
    drain(ctx, producer, sender);
    return;
  }
  if (error === 'OUT_OF_ORDER_SEQUENCE_NUMBER') sender.bumpEpoch = true;
  // Back in the queue, in send order; the partition waits for its in-flight requests.
  sender.queue.push(pending);
  sender.queue.sort((a, b) => a.order - b.order);
  sender.retrying = true;
  refreshIfStale(ctx, producer.metadata, config.metadataMaxAgeMs);
  const topic = ctx.state.topics.get(pending.topic);
  const leader =
    topic === undefined ? null : knownLeader(producer.metadata, topic, pending.partition);
  // KIP-951: no backoff when the retry goes to a different (new) leader.
  const wait =
    leader !== null && leader !== pending.lastBroker ? 0 : backoffMs(producer, pending.attempts);
  sender.backoffUntil = Math.max(sender.backoffUntil, ctx.kernel.now() + wait);
  ctx.emit({
    type: 'ProduceRetried',
    producerId: config.id,
    topic: pending.topic,
    partition: pending.partition,
    traceId,
    attempt: pending.attempts + 1,
    error,
    backoffMs: wait,
  });
  drain(ctx, producer, sender);
}

function fail(
  ctx: EngineContext,
  producer: ProducerState,
  pending: PendingRecord,
  error: ProduceError,
): void {
  producer.pending.delete(pending.traceId);
  producer.stats.failed += 1;
  ctx.emit({
    type: 'ProduceFailed',
    producerId: producer.config.id,
    topic: pending.topic,
    partition: pending.partition,
    traceId: pending.traceId,
    error,
  });
}

export function handleProducerDrain(
  ctx: EngineContext,
  producer: ProducerState,
  task: Task<'ProducerDrain'>,
): void {
  const sender = producer.senders.get(task.key);
  if (sender === undefined || sender.drainToken !== task.token) return;
  drain(ctx, producer, sender);
}

/** request.timeout.ms passed without an answer: the attempt fails (client side). */
export function handleProduceRequestTimeout(
  ctx: EngineContext,
  producer: ProducerState,
  task: Task<'ProduceRequestTimeout'>,
): void {
  onAttemptResult(ctx, producer, task.traceId, task.attempt, {
    ok: false,
    error: 'REQUEST_TIMED_OUT',
  });
}

/** delivery.timeout.ms passed since send(): the record fails wherever it is (K7). */
export function handleProduceDeliveryTimeout(
  ctx: EngineContext,
  producer: ProducerState,
  task: Task<'ProduceDeliveryTimeout'>,
): void {
  const pending = producer.pending.get(task.traceId);
  if (pending === undefined) return;
  const sender = senderOf(producer, pending.topic, pending.partition);
  for (const list of [sender.queue, sender.inFlight]) {
    const index = list.indexOf(pending);
    if (index >= 0) list.splice(index, 1);
  }
  pending.inFlight = null;
  fail(ctx, producer, pending, 'DELIVERY_TIMEOUT');
  drain(ctx, producer, sender);
}

/** A broker's process stopped: requests in flight to it fail with NETWORK_EXCEPTION at once. */
export function onBrokerDisconnected(ctx: EngineContext, brokerId: string): void {
  for (const producer of ctx.state.producers.values()) {
    for (const sender of [...producer.senders.values()]) {
      for (const pending of [...sender.inFlight]) {
        if (pending.inFlight?.broker !== brokerId) continue;
        onAttemptResult(ctx, producer, pending.traceId, pending.inFlight.attempt, {
          ok: false,
          error: 'NETWORK_EXCEPTION',
        });
      }
    }
  }
}

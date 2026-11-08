/**
 * Producing records (Kafka docs, Design §"The Producer"; ProducerConfig `acks`).
 *
 * Flow for one record:
 *   ProducerTick (client)  → choose key, choose partition (partitioner.ts), RecordProduced
 *   LeaderAppend (broker)  → append to the partition log, RecordAppended      (+ network delay)
 *   ProduceResponse        → ProduceAcked with the offset, or ProduceFailed   (+ network delay)
 *
 * acks (ProducerConfig ACKS_DOC):
 * - acks=0: "the producer will not wait for any acknowledgment from the server at all … The
 *   offset given back for each record will always be set to -1." Modelled as ProduceAcked at send
 *   time with offset null, before the broker has the record.
 * - acks=1: the leader acknowledges after writing to its local log.
 * - acks=all: the leader waits until every replica in the ISR has the record (the HW passes it),
 *   and refuses the write when the ISR is below min.insync.replicas (M4-T3, K6). With one replica
 *   the ISR is the leader alone, so acks=1 and acks=all behave the same.
 *
 * The partition count used by the partitioner is the producer's cached metadata (metadata.ts),
 * so partitions added to a topic are used only after the next metadata refresh.
 *
 * Trace ids are "<producerId>@<incarnation>#<n>": the incarnation is this producer's life
 * (a removed and re-added producer gets a new one) and n counts its records (it keeps counting
 * across rate changes), so a trace id is never reused.
 */

import type { KeyStrategy } from '@kp/schema';
import type { Rng } from '../core/rng.ts';
import type { EngineContext } from '../model/context.ts';
import { appendRecord, leaderLog, type ReplicaLog } from '../model/partition.ts';
import { canServe } from './controller.ts';
import {
  effectiveMinIsr,
  onLeaderAppended,
  onLeaderStateChanged,
  parkProduce,
  sendProduceResponse,
  type ProduceTarget,
} from './replication.ts';
import {
  createClientMetadata,
  knownLeader,
  knownPartitionCount,
  refreshIfStale,
} from './metadata.ts';
import {
  enqueueRecord,
  handleProduceDeliveryTimeout,
  handleProduceRequestTimeout,
  handleProducerDrain,
  onAttemptResult,
} from './producer-client.ts';
import { isAlive, topicsOfProducer, type ProducerState, type TopicState } from '../model/state.ts';
import type { EngineTask, ProduceError } from '../model/tasks.ts';
import {
  createStickyState,
  partitionForKey,
  recordStickyAppend,
  stickyPartition,
} from './partitioner.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/** Sim time of the producer's n-th scheduled record (n ≥ 1). Integer ms, no drift. */
export function tickTime(producer: ProducerState, n: number): number {
  return producer.startedAt + Math.floor((n * 1000) / producer.config.ratePerSec);
}

/** Registers the producer's run-time state and schedules its first record. */
export function startProducer(ctx: EngineContext, config: ProducerState['config']): ProducerState {
  const incarnation = ++ctx.state.counters.incarnation;
  const label = `producer:${config.id}#${String(incarnation)}`;
  const producer: ProducerState = {
    config,
    incarnation,
    schedule: 0,
    startedAt: ctx.kernel.now(),
    keyRng: ctx.kernel.rng.fork(`${label}:keys`),
    partitionRng: ctx.kernel.rng.fork(`${label}:partitions`),
    sticky: new Map(),
    metadata: createClientMetadata(ctx.kernel.now()),
    senders: new Map(),
    pending: new Map(),
    retryRng: ctx.kernel.rng.fork(`${label}:retries`),
    stats: { sent: 0, acked: 0, failed: 0 },
  };
  ctx.state.producers.set(config.id, producer);
  restartSchedule(ctx, producer);
  return producer;
}

/**
 * Starts a new send schedule from now: the first record is due 1000 / rate ms later. Ticks of the
 * previous schedule are ignored. Used at start and when the rate changes (UpdateConfig); stats,
 * RNG streams, sticky state and in-flight requests are kept.
 */
export function restartSchedule(ctx: EngineContext, producer: ProducerState): void {
  producer.schedule += 1;
  producer.startedAt = ctx.kernel.now();
  ctx.kernel.scheduleAt(tickTime(producer, 1), {
    type: 'ProducerTick',
    producerId: producer.config.id,
    incarnation: producer.incarnation,
    schedule: producer.schedule,
    n: 1,
  });
}

const current = (
  ctx: EngineContext,
  producerId: string,
  incarnation: number,
): ProducerState | undefined => {
  const producer = ctx.state.producers.get(producerId);
  return producer?.incarnation === incarnation ? producer : undefined;
};

export function handleProducerTick(ctx: EngineContext, task: Task<'ProducerTick'>): void {
  const producer = current(ctx, task.producerId, task.incarnation);
  if (producer === undefined) return; // removed: its schedule ends here
  if (producer.schedule !== task.schedule) return; // the rate changed: a newer schedule runs
  for (const topic of topicsOfProducer(ctx.state, producer.config.id)) {
    const key = chooseKey(producer.config.keyStrategy, producer.keyRng);
    sendRecord(
      ctx,
      producer,
      topic,
      key,
      `${producer.config.id}-${String(producer.stats.sent + 1)}`,
    );
  }
  ctx.kernel.scheduleAt(tickTime(producer, task.n + 1), { ...task, n: task.n + 1 });
}

/** KafkaProducer#send: pick the partition on the client, then send the request to the leader. */
export function sendRecord(
  ctx: EngineContext,
  producer: ProducerState,
  topic: TopicState,
  key: string | null,
  value: string,
): void {
  const { config } = producer;
  // The partitioner uses the producer's cached metadata. When a refresh shows a new partition
  // count, the sticky partition for that topic is chosen afresh.
  for (const changed of refreshIfStale(ctx, producer.metadata, config.metadataMaxAgeMs)) {
    producer.sticky.delete(changed);
  }
  const numPartitions = knownPartitionCount(producer.metadata, topic);
  let partition: number;
  if (key === null) {
    let sticky = producer.sticky.get(topic.config.id);
    if (sticky === undefined) {
      sticky = createStickyState();
      producer.sticky.set(topic.config.id, sticky);
    }
    partition = stickyPartition(sticky, numPartitions, producer.partitionRng);
    recordStickyAppend(
      sticky,
      config.messageSizeBytes,
      config.batchSize,
      numPartitions,
      producer.partitionRng,
    );
  } else {
    partition = partitionForKey(key, numPartitions);
  }

  producer.stats.sent += 1;
  const traceId = `${config.id}@${String(producer.incarnation)}#${String(producer.stats.sent)}`;
  const now = ctx.kernel.now();
  ctx.emit({
    type: 'RecordProduced',
    producerId: config.id,
    topic: topic.config.id,
    partition,
    key,
    value,
    traceId,
    acks: config.acks,
  });
  if (config.acks === '0') {
    producer.stats.acked += 1;
    ctx.state.ackedTraceIds.add(traceId);
    ctx.emit({
      type: 'ProduceAcked',
      producerId: config.id,
      topic: topic.config.id,
      partition,
      traceId,
      offset: null,
    });
  }
  const record = {
    key,
    value,
    timestamp: now,
    producerId: config.id,
    traceId,
    sizeBytes: config.messageSizeBytes,
  };
  if (config.acks !== '0') {
    enqueueRecord(ctx, producer, topic.config.id, partition, record, config.acks);
    return;
  }
  // acks=0: sent once to the leader the producer knows, if it can connect; never retried.
  const leader = knownLeader(producer.metadata, topic, partition);
  const broker = leader === null ? undefined : ctx.state.brokers.get(leader);
  if (leader === null || broker === undefined || !isAlive(broker.status)) {
    producer.metadata.updateRequested = true;
    return;
  }
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'LeaderAppend',
    producerId: config.id,
    incarnation: producer.incarnation,
    broker: leader,
    topic: topic.config.id,
    partition,
    acks: '0',
    timeoutMs: config.requestTimeoutMs,
    attempt: 1,
    record: { ...record, pid: -1, producerEpoch: -1, sequence: -1 },
  });
}

/**
 * A produce request reaches the broker (Partition#appendRecordsToLeader, K6):
 * - not the leader (any more) → NOT_LEADER_OR_FOLLOWER;
 * - acks=all with the ISR below min.insync.replicas → NOT_ENOUGH_REPLICAS, nothing appended;
 * - otherwise append; acks=1 is answered now, acks=all once the HW passes the record
 *   (DelayedProduce, protocols/replication.ts) or after the request's timeout.
 */
export function handleLeaderAppend(ctx: EngineContext, task: Task<'LeaderAppend'>): void {
  const topic = ctx.state.topics.get(task.topic);
  const partition = topic?.partitions[task.partition];
  const target: ProduceTarget = {
    producerId: task.producerId,
    incarnation: task.incarnation,
    topic: task.topic,
    partition: task.partition,
    traceId: task.record.traceId,
    attempt: task.attempt,
  };
  const respond = (result: Task<'ProduceResponse'>['result']): void => {
    if (task.acks === '0') return; // nobody is waiting for an answer
    sendProduceResponse(ctx, target, result);
  };
  const broker = ctx.state.brokers.get(task.broker);
  if (broker === undefined || !isAlive(broker.status)) return; // no process to answer (T7)
  if (!canServe(ctx, task.broker)) {
    // A restarted broker that is not registered and unfenced yet serves nothing.
    respond({ ok: false, error: 'NOT_LEADER_OR_FOLLOWER' });
    return;
  }
  if (topic === undefined || partition === undefined) {
    // The topic was deleted while the request was in flight.
    respond({ ok: false, error: 'UNKNOWN_TOPIC_OR_PARTITION' });
    return;
  }
  const leader = leaderLog(partition);
  if (leader === undefined || partition.leader !== task.broker) {
    respond({ ok: false, error: 'NOT_LEADER_OR_FOLLOWER' });
    return;
  }
  if (task.acks === 'all' && partition.isr.length < effectiveMinIsr(topic.config, partition)) {
    respond({ ok: false, error: 'NOT_ENOUGH_REPLICAS' });
    return;
  }
  // Idempotence (ProducerAppendInfo#checkSequence, K8): a duplicate gets its original offset.
  const check = checkSequence(leader, task.record);
  if (check.kind === 'error') {
    respond({ ok: false, error: check.error });
    return;
  }
  let offset: number;
  if (check.kind === 'duplicate') {
    offset = check.offset;
  } else {
    const appended = appendRecord(partition, task.record);
    offset = appended.offset;
    ctx.emit({
      type: 'RecordAppended',
      topic: task.topic,
      partition: task.partition,
      offset,
      key: appended.key,
      value: appended.value,
      producerId: appended.producerId,
      traceId: appended.traceId,
    });
    // Followers waiting in a long poll get the record; with no other replica in the ISR the HW
    // moves at once (and parked consumer fetches may complete).
    onLeaderAppended(ctx, partition);
    onLeaderStateChanged(ctx, topic, partition);
  }
  if (task.acks === 'all' && partition.highWatermark <= offset) {
    parkProduce(
      ctx,
      target,
      { broker: task.broker, topicId: topic.topicId },
      offset,
      task.timeoutMs,
    );
    return;
  }
  respond({ ok: true, offset });
}

type SequenceCheck =
  | { readonly kind: 'append' }
  | { readonly kind: 'duplicate'; readonly offset: number }
  | { readonly kind: 'error'; readonly error: ProduceError };

/**
 * ProducerAppendInfo#checkSequence and the duplicate check against the last 5 batches (K8):
 * unknown producer → any sequence; a newer epoch must start at 0; the same epoch needs
 * lastSeq + 1; a sequence already written (and still remembered) is a duplicate.
 */
function checkSequence(leader: ReplicaLog, record: Task<'LeaderAppend'>['record']): SequenceCheck {
  if (record.pid < 0) return { kind: 'append' };
  const state = leader.producers.get(record.pid);
  if (state === undefined) return { kind: 'append' };
  if (record.producerEpoch < state.epoch) return { kind: 'error', error: 'INVALID_PRODUCER_EPOCH' };
  if (record.producerEpoch > state.epoch) {
    return record.sequence === 0
      ? { kind: 'append' }
      : { kind: 'error', error: 'OUT_OF_ORDER_SEQUENCE_NUMBER' };
  }
  if (record.sequence === state.lastSeq + 1) return { kind: 'append' };
  const duplicate = state.recent.find((r) => r.seq === record.sequence);
  if (duplicate !== undefined) return { kind: 'duplicate', offset: duplicate.offset };
  return { kind: 'error', error: 'OUT_OF_ORDER_SEQUENCE_NUMBER' };
}

export function handleProduceResponse(ctx: EngineContext, task: Task<'ProduceResponse'>): void {
  const producer = current(ctx, task.producerId, task.incarnation);
  if (producer === undefined) return; // the producer is gone; nobody receives the response
  onAttemptResult(ctx, producer, task.traceId, task.attempt, task.result);
}

/** Producer tasks for the current life of a producer (M4-T7). */
export function handleProducerTask(
  ctx: EngineContext,
  task: Task<'ProducerDrain' | 'ProduceRequestTimeout' | 'ProduceDeliveryTimeout'>,
): void {
  const producer = current(ctx, task.producerId, task.incarnation);
  if (producer === undefined) return;
  switch (task.type) {
    case 'ProducerDrain':
      handleProducerDrain(ctx, producer, task);
      return;
    case 'ProduceRequestTimeout':
      handleProduceRequestTimeout(ctx, producer, task);
      return;
    case 'ProduceDeliveryTimeout':
      handleProduceDeliveryTimeout(ctx, producer, task);
      return;
  }
}

/** Picks the next record key from the producer's key strategy, using its seeded RNG. */
export function chooseKey(strategy: KeyStrategy, rng: Rng): string | null {
  switch (strategy.type) {
    case 'null':
      return null;
    case 'fixed':
      return strategy.key;
    case 'random':
      return rng.pick(strategy.keys);
    case 'skewed':
      return rng.next() < strategy.hotShare ? strategy.hotKey : rng.pick(strategy.otherKeys);
  }
}

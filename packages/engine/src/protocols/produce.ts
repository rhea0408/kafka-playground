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
 * - acks=all: the leader waits for the full ISR. With one replica (M1) the ISR is the leader
 *   alone, so acks=1 and acks=all behave the same until replication arrives (M4).
 */

import type { KeyStrategy } from '@kp/schema';
import type { Rng } from '../core/rng.ts';
import type { EngineContext } from '../model/context.ts';
import { appendRecord } from '../model/partition.ts';
import { topicsOfProducer, type ProducerState, type TopicState } from '../model/state.ts';
import type { EngineTask } from '../model/tasks.ts';
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
    startedAt: ctx.kernel.now(),
    keyRng: ctx.kernel.rng.fork(`${label}:keys`),
    partitionRng: ctx.kernel.rng.fork(`${label}:partitions`),
    sticky: new Map(),
    stats: { sent: 0, acked: 0, failed: 0 },
  };
  ctx.state.producers.set(config.id, producer);
  ctx.kernel.scheduleAt(tickTime(producer, 1), {
    type: 'ProducerTick',
    producerId: config.id,
    incarnation,
    n: 1,
  });
  return producer;
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
  const numPartitions = topic.partitions.length;
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
  const traceId = `${config.id}#${String(producer.stats.sent)}`;
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
    ctx.emit({
      type: 'ProduceAcked',
      producerId: config.id,
      topic: topic.config.id,
      partition,
      traceId,
      offset: null,
    });
  }
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'LeaderAppend',
    producerId: config.id,
    incarnation: producer.incarnation,
    topic: topic.config.id,
    partition,
    acks: config.acks,
    record: { key, value, timestamp: now, producerId: config.id, traceId },
  });
}

export function handleLeaderAppend(ctx: EngineContext, task: Task<'LeaderAppend'>): void {
  const partition = ctx.state.topics.get(task.topic)?.partitions[task.partition];
  const respond = (result: Task<'ProduceResponse'>['result']): void => {
    if (task.acks === '0') return; // nobody is waiting for an answer
    ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
      type: 'ProduceResponse',
      producerId: task.producerId,
      incarnation: task.incarnation,
      topic: task.topic,
      partition: task.partition,
      traceId: task.record.traceId,
      result,
    });
  };
  if (partition === undefined) {
    // The topic was deleted while the request was in flight.
    respond({ ok: false, error: 'UNKNOWN_TOPIC_OR_PARTITION' });
    return;
  }
  const appended = appendRecord(partition, task.record);
  ctx.emit({
    type: 'RecordAppended',
    topic: task.topic,
    partition: task.partition,
    offset: appended.offset,
    key: appended.key,
    value: appended.value,
    producerId: appended.producerId,
    traceId: appended.traceId,
  });
  respond({ ok: true, offset: appended.offset });
}

export function handleProduceResponse(ctx: EngineContext, task: Task<'ProduceResponse'>): void {
  const producer = current(ctx, task.producerId, task.incarnation);
  if (producer === undefined) return; // the producer is gone; nobody receives the response
  const { producerId, topic, partition, traceId } = task;
  if (task.result.ok) {
    producer.stats.acked += 1;
    ctx.emit({
      type: 'ProduceAcked',
      producerId,
      topic,
      partition,
      traceId,
      offset: task.result.offset,
    });
  } else {
    producer.stats.failed += 1;
    ctx.emit({
      type: 'ProduceFailed',
      producerId,
      topic,
      partition,
      traceId,
      error: task.result.error,
    });
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

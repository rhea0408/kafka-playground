/**
 * Consuming records with one consumer per group (M1; group membership and rebalancing are M3).
 * Modelled on the classic KafkaConsumer (Kafka docs, Design §"The Consumer"; KafkaConsumer and
 * ConsumerConfig javadoc).
 *
 * The application loop is `while (running) { records = poll(); process(records); }`:
 * - poll() returns at most max.poll.records records *in total* over all assigned partitions,
 *   starting at each partition's position, and only below the high watermark.
 * - Processing takes processingTimeMs per record; the next poll() happens after that. An empty
 *   poll waits pollIntervalMs (game setting standing in for the poll timeout / long poll).
 * - Position = next offset to fetch. Committed offset = what the group has saved; a restarted
 *   consumer resumes from it.
 * - No committed offset (or one that is out of range): auto.offset.reset decides the start.
 *   "earliest" = log start offset; "latest" = the end offset, which for read_uncommitted
 *   consumers is the high watermark (KafkaConsumer#endOffsets javadoc).
 * - enable.auto.commit: "offsets are committed automatically with a frequency controlled by the
 *   config auto.commit.interval.ms". Modelled as in the classic consumer: at the start of poll(),
 *   once the interval has passed, the positions reached by earlier polls are committed. That is
 *   why the javadoc requires processing everything from one poll before the next (at-least-once).
 * - close() commits pending offsets when auto-commit is on; a crash (KillConsumer) does not.
 */

import type { EngineContext } from '../model/context.ts';
import { partitionName, readRecords } from '../model/partition.ts';
import { topicsOfConsumer, type ConsumerState, type GroupState } from '../model/state.ts';
import type { EngineTask } from '../model/tasks.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

export function groupOf(ctx: EngineContext, groupId: string): GroupState {
  let group = ctx.state.groups.get(groupId);
  if (group === undefined) {
    group = { committed: new Map() };
    ctx.state.groups.set(groupId, group);
  }
  return group;
}

/** Registers a running consumer and schedules its first poll right away. */
export function startConsumer(ctx: EngineContext, config: ConsumerState['config']): ConsumerState {
  const consumer: ConsumerState = {
    config,
    incarnation: 0,
    status: 'stopped',
    positions: new Map(),
    lastAutoCommitAt: 0,
    nextPartitionIndex: 0,
    stats: { consumed: 0 },
  };
  ctx.state.consumers.set(config.id, consumer);
  groupOf(ctx, config.groupId);
  runConsumer(ctx, consumer);
  return consumer;
}

/** Starts (or restarts) the poll loop as a fresh life: no positions, commit timer from now. */
export function runConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  consumer.incarnation = ++ctx.state.counters.incarnation;
  consumer.status = 'running';
  consumer.positions.clear();
  consumer.lastAutoCommitAt = ctx.kernel.now();
  consumer.nextPartitionIndex = 0;
  ctx.kernel.scheduleIn(0, {
    type: 'ConsumerPoll',
    consumerId: consumer.config.id,
    incarnation: consumer.incarnation,
  });
}

/** A crash: the process stops; nothing is committed; positions are lost. */
export function killConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  consumer.status = 'stopped';
  consumer.positions.clear();
  consumer.incarnation = ++ctx.state.counters.incarnation; // pending polls become stale
}

/** KafkaConsumer#close(): commits pending offsets first when auto-commit is on. */
export function closeConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  if (consumer.status === 'running' && consumer.config.enableAutoCommit) {
    commitPositions(ctx, consumer, [...consumer.positions.keys()]);
  }
  killConsumer(ctx, consumer);
}

interface Assigned {
  readonly topic: string;
  readonly partition: number;
  readonly key: string;
}

/** Every partition of every connected topic, in connection order then partition order. */
function assignment(ctx: EngineContext, consumer: ConsumerState): Assigned[] {
  return topicsOfConsumer(ctx.state, consumer.config.id).flatMap((topic) =>
    topic.partitions.map((p) => ({
      topic: topic.config.id,
      partition: p.index,
      key: partitionName(topic.config.id, p.index),
    })),
  );
}

export function handleConsumerPoll(ctx: EngineContext, task: Task<'ConsumerPoll'>): void {
  const consumer = ctx.state.consumers.get(task.consumerId);
  if (consumer?.incarnation !== task.incarnation || consumer.status !== 'running') return;
  const { config } = consumer;
  const now = ctx.kernel.now();
  const assigned = assignment(ctx, consumer);

  // Partitions no longer assigned (disconnected or deleted topic): KafkaConsumer#assign commits
  // the old assignment first when auto-commit is on, then forgets those positions.
  const assignedKeys = new Set(assigned.map((a) => a.key));
  const dropped = [...consumer.positions.keys()].filter((k) => !assignedKeys.has(k));
  if (dropped.length > 0) {
    if (config.enableAutoCommit) commitPositions(ctx, consumer, dropped);
    for (const k of dropped) consumer.positions.delete(k);
  }

  // Auto-commit what earlier polls returned, once the interval has passed.
  if (config.enableAutoCommit && now - consumer.lastAutoCommitAt >= config.autoCommitIntervalMs) {
    commitPositions(ctx, consumer, [...consumer.positions.keys()]);
    consumer.lastAutoCommitAt = now;
  }

  const group = groupOf(ctx, config.groupId);
  let budget = config.maxPollRecords;
  let fetched = 0;
  const count = assigned.length;
  const first = count === 0 ? 0 : consumer.nextPartitionIndex % count;
  for (let i = 0; i < count && budget > 0; i++) {
    const a = assigned[(first + i) % count];
    const partition =
      a === undefined ? undefined : ctx.state.topics.get(a.topic)?.partitions[a.partition];
    if (a === undefined || partition === undefined) continue;

    let position = consumer.positions.get(a.key);
    if (position === undefined) {
      const committed = group.committed.get(a.key);
      const inRange =
        committed !== undefined &&
        committed >= partition.logStartOffset &&
        committed <= partition.highWatermark;
      position = inRange
        ? committed
        : config.autoOffsetReset === 'earliest'
          ? partition.logStartOffset
          : partition.highWatermark;
    }

    const records = readRecords(partition, position, budget);
    for (const r of records) {
      ctx.emit({
        type: 'RecordConsumed',
        consumerId: config.id,
        groupId: config.groupId,
        topic: a.topic,
        partition: a.partition,
        offset: r.offset,
        key: r.key,
        value: r.value,
        traceId: r.traceId,
      });
      position = r.offset + 1;
    }
    consumer.positions.set(a.key, position);
    budget -= records.length;
    fetched += records.length;
  }
  if (count > 0) consumer.nextPartitionIndex = (first + 1) % count;
  consumer.stats.consumed += fetched;

  const delay = fetched > 0 ? fetched * config.processingTimeMs : config.pollIntervalMs;
  ctx.kernel.scheduleIn(delay, task);
}

/** Commits the consumer's current position for each given partition, if it moved. */
function commitPositions(
  ctx: EngineContext,
  consumer: ConsumerState,
  keys: readonly string[],
): void {
  const group = groupOf(ctx, consumer.config.groupId);
  for (const key of keys) {
    const position = consumer.positions.get(key);
    if (position === undefined || group.committed.get(key) === position) continue;
    group.committed.set(key, position);
    const dash = key.lastIndexOf('-');
    ctx.emit({
      type: 'OffsetCommitted',
      groupId: consumer.config.groupId,
      consumerId: consumer.config.id,
      topic: key.slice(0, dash),
      partition: Number(key.slice(dash + 1)),
      offset: position,
    });
  }
}

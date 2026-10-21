/**
 * Consuming records with one consumer per group (M1/M2; group membership and rebalancing are M3).
 * Modelled on the classic KafkaConsumer (the default `group.protocol` in Kafka 4.x): Kafka docs,
 * Design §"The Consumer"; KafkaConsumer, ClassicKafkaConsumer, FetchCollector and ConsumerConfig.
 *
 * The application loop is `while (running) { records = poll(); process(records); }`.
 *
 * poll() (ClassicKafkaConsumer#poll / #pollForFetches):
 * 1. Housekeeping: drop partitions that are no longer assigned (committing them first when
 *    auto-commit is on), and auto-commit once auto.commit.interval.ms has passed. As in the
 *    classic consumer this happens inside poll(), which is why the javadoc asks the application
 *    to finish processing one poll's records before the next (at-least-once).
 * 2. Partitions without a position start from the group's committed offset; without one (or if
 *    it is out of range), auto.offset.reset decides: "earliest" = log start offset, "latest" = the
 *    end offset, which for read_uncommitted consumers is the high watermark.
 * 3. "If data is available already, return it immediately": records already in the fetch buffer
 *    are returned first, at most max.poll.records in total; leftovers stay buffered
 *    (FetchCollector#collectFetch). RecordConsumed is emitted here, when poll() hands a record to
 *    the application, and the position moves past it.
 * 4. Fetches are sent for assigned partitions with nothing buffered, one request per leader that
 *    has no request in flight ("won't resend pending fetches"). When records were returned this
 *    happens before returning them, so the next fetch runs while the application processes
 *    ("send off the next round of fetches … to enable pipelining").
 * 5. With nothing to return, the application stays blocked in poll() until a fetch response
 *    arrives, or until the next auto-commit is due (pollForFetches caps its wait with
 *    coordinator.timeToNextPoll, which includes the auto-commit deadline).
 *
 * A fetch response is discarded for partitions that are no longer assigned, or whose position no
 * longer matches the fetch offset ("discarding stale fetch response", FetchCollector).
 * OFFSET_OUT_OF_RANGE resets the position with auto.offset.reset; UNKNOWN_TOPIC_OR_PARTITION
 * requests a metadata refresh. The broker side of the long poll is in fetch.ts.
 *
 * Commits go to the group coordinator, which stores them only for partitions that exist
 * (KafkaApis#handleOffsetCommitRequest answers UNKNOWN_TOPIC_OR_PARTITION otherwise) and deletes
 * a topic's offsets when the topic is deleted (GroupCoordinator#onPartitionsDeleted). A position
 * remembers the topic ID it was read from, and is not committed to a re-created topic with the
 * same name (simplifications: "Commits after a topic is re-created").
 *
 * Processing takes processingTimeMs per record (game setting); the next poll() happens after it.
 * close() commits pending offsets when auto-commit is on; a crash (KillConsumer) does not. Both
 * start a new incarnation, so fetch responses and poll tasks of the old life are ignored.
 */

import type { EngineContext } from '../model/context.ts';
import { partitionName, type LogRecord, type PartitionState } from '../model/partition.ts';
import { topicsOfConsumer, type ConsumerState, type GroupState } from '../model/state.ts';
import type { EngineTask, FetchPartition } from '../model/tasks.ts';
import {
  createClientMetadata,
  knownPartitionCount,
  refreshIfStale,
  resetClientMetadata,
} from './metadata.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/**
 * A topic was deleted: every group's committed offsets for it go too. In KRaft the broker's
 * metadata publisher calls GroupCoordinator#onPartitionsDeleted for the deleted partitions, which
 * removes their offsets, so a topic later re-created with the same name starts with none.
 */
export function deleteCommittedOffsets(ctx: EngineContext, topic: string): void {
  for (const group of ctx.state.groups.values()) {
    for (const key of [...group.committed.keys()]) {
      if (key.slice(0, key.lastIndexOf('-')) === topic) group.committed.delete(key);
    }
  }
}

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
    buffer: [],
    fetchesInFlight: new Set(),
    inPoll: false,
    pollToken: 0,
    metadata: createClientMetadata(ctx.kernel.now()),
    stats: { consumed: 0 },
  };
  ctx.state.consumers.set(config.id, consumer);
  groupOf(ctx, config.groupId);
  runConsumer(ctx, consumer);
  return consumer;
}

/** Starts (or restarts) the poll loop as a fresh process: no positions, buffer or metadata. */
export function runConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  const now = ctx.kernel.now();
  consumer.incarnation = ++ctx.state.counters.incarnation;
  consumer.status = 'running';
  forgetProcessState(consumer);
  resetClientMetadata(consumer.metadata, now);
  consumer.lastAutoCommitAt = now;
  consumer.nextPartitionIndex = 0;
  schedulePoll(ctx, consumer, now);
}

/** A crash: the process stops; nothing is committed; positions and fetched data are lost. */
export function killConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  consumer.status = 'stopped';
  forgetProcessState(consumer);
  // Pending polls and in-flight fetch responses belong to the old incarnation and are ignored.
  consumer.incarnation = ++ctx.state.counters.incarnation;
}

/** KafkaConsumer#close(): commits pending offsets first when auto-commit is on. */
export function closeConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  if (consumer.status === 'running' && consumer.config.enableAutoCommit) {
    commitPositions(ctx, consumer, [...consumer.positions.keys()]);
  }
  killConsumer(ctx, consumer);
}

/**
 * Wakes a consumer that is blocked in poll() so its next poll() iteration happens now, e.g. after
 * it was connected to a topic (subscribing to a new topic) while it had nothing to fetch.
 */
export function wakeConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  if (consumer.status === 'running' && consumer.inPoll) {
    schedulePoll(ctx, consumer, ctx.kernel.now());
  }
}

function forgetProcessState(consumer: ConsumerState): void {
  consumer.positions.clear();
  consumer.buffer.length = 0;
  consumer.fetchesInFlight.clear();
  consumer.inPoll = false;
}

/** Schedules the one current ConsumerPoll task; any earlier one becomes stale. */
function schedulePoll(ctx: EngineContext, consumer: ConsumerState, at: number): void {
  consumer.pollToken += 1;
  ctx.kernel.scheduleAt(at, {
    type: 'ConsumerPoll',
    consumerId: consumer.config.id,
    incarnation: consumer.incarnation,
    token: consumer.pollToken,
  });
}

const running = (
  ctx: EngineContext,
  consumerId: string,
  incarnation: number,
): ConsumerState | undefined => {
  const consumer = ctx.state.consumers.get(consumerId);
  return consumer?.incarnation === incarnation && consumer.status === 'running'
    ? consumer
    : undefined;
};

export function handleConsumerPoll(ctx: EngineContext, task: Task<'ConsumerPoll'>): void {
  const consumer = running(ctx, task.consumerId, task.incarnation);
  if (consumer?.pollToken !== task.token) return;
  poll(ctx, consumer);
}

interface Assigned {
  readonly topic: string;
  readonly partition: number;
  readonly key: string;
}

/**
 * Every partition of every connected topic that the consumer's metadata knows about, in
 * connection order then partition order (like subscribe(); one member gets everything).
 */
function assignment(ctx: EngineContext, consumer: ConsumerState): Assigned[] {
  refreshIfStale(ctx, consumer.metadata, consumer.config.metadataMaxAgeMs);
  return topicsOfConsumer(ctx.state, consumer.config.id).flatMap((topic) =>
    Array.from({ length: knownPartitionCount(consumer.metadata, topic) }, (_, index) => ({
      topic: topic.config.id,
      partition: index,
      key: partitionName(topic.config.id, index),
    })),
  );
}

const partitionOf = (ctx: EngineContext, a: Assigned): PartitionState | undefined =>
  ctx.state.topics.get(a.topic)?.partitions[a.partition];

/** One poll() call by the application (see the file comment for the steps). */
function poll(ctx: EngineContext, consumer: ConsumerState): void {
  const { config } = consumer;
  const now = ctx.kernel.now();
  const assigned = assignment(ctx, consumer);

  // Partitions no longer assigned (disconnected or deleted topic): KafkaConsumer#assign commits
  // the old assignment first when auto-commit is on, then forgets those positions and their
  // buffered data.
  const assignedKeys = new Set(assigned.map((a) => a.key));
  const dropped = [...consumer.positions.keys()].filter((k) => !assignedKeys.has(k));
  if (dropped.length > 0) {
    if (config.enableAutoCommit) commitPositions(ctx, consumer, dropped);
    for (const k of dropped) consumer.positions.delete(k);
  }
  for (let i = consumer.buffer.length - 1; i >= 0; i--) {
    const chunk = consumer.buffer[i];
    if (chunk !== undefined && !assignedKeys.has(chunk.key)) consumer.buffer.splice(i, 1);
  }

  // Auto-commit what earlier polls returned, once the interval has passed.
  if (config.enableAutoCommit && now - consumer.lastAutoCommitAt >= config.autoCommitIntervalMs) {
    commitPositions(ctx, consumer, [...consumer.positions.keys()]);
    consumer.lastAutoCommitAt = now;
  }

  for (const a of assigned) {
    const topic = ctx.state.topics.get(a.topic);
    const partition = topic?.partitions[a.partition];
    if (topic !== undefined && partition !== undefined && !consumer.positions.has(a.key)) {
      consumer.positions.set(a.key, {
        offset: startingOffset(ctx, consumer, a.key, partition),
        topicId: topic.topicId,
      });
    }
  }

  const returned = drainBuffer(ctx, consumer);
  sendFetches(ctx, consumer, assigned);
  if (returned > 0) {
    consumer.inPoll = false;
    schedulePoll(ctx, consumer, now + returned * config.processingTimeMs);
  } else {
    // Blocked in poll(): a fetch response wakes it, and so does the auto-commit deadline.
    consumer.inPoll = true;
    if (config.enableAutoCommit) {
      schedulePoll(ctx, consumer, consumer.lastAutoCommitAt + config.autoCommitIntervalMs);
    } else {
      consumer.pollToken += 1; // nothing else is due; a stale wake-up must not run
    }
  }
}

/** Committed offset if it is in range, otherwise auto.offset.reset. */
function startingOffset(
  ctx: EngineContext,
  consumer: ConsumerState,
  key: string,
  partition: PartitionState,
): number {
  const committed = groupOf(ctx, consumer.config.groupId).committed.get(key);
  const inRange =
    committed !== undefined &&
    committed >= partition.logStartOffset &&
    committed <= partition.highWatermark;
  if (inRange) return committed;
  return resetOffset(consumer, partition);
}

/** auto.offset.reset: "earliest" = log start offset, "latest" = HW (KafkaConsumer#endOffsets). */
function resetOffset(consumer: ConsumerState, partition: PartitionState): number {
  return consumer.config.autoOffsetReset === 'earliest'
    ? partition.logStartOffset
    : partition.highWatermark;
}

/** Hands buffered records to the application, at most max.poll.records. Returns the count. */
function drainBuffer(ctx: EngineContext, consumer: ConsumerState): number {
  const { config } = consumer;
  let budget = config.maxPollRecords;
  let returned = 0;
  while (budget > 0) {
    const chunk = consumer.buffer[0];
    if (chunk === undefined) break;
    const taken: LogRecord[] = chunk.records.splice(0, budget);
    for (const r of taken) {
      ctx.emit({
        type: 'RecordConsumed',
        consumerId: config.id,
        groupId: config.groupId,
        topic: chunk.topic,
        partition: chunk.partition,
        offset: r.offset,
        key: r.key,
        value: r.value,
        traceId: r.traceId,
      });
      consumer.positions.set(chunk.key, { offset: r.offset + 1, topicId: chunk.topicId });
    }
    if (chunk.records.length === 0) consumer.buffer.shift();
    budget -= taken.length;
    returned += taken.length;
  }
  consumer.stats.consumed += returned;
  return returned;
}

/**
 * Sends one FetchRequest per leader for the assigned partitions that have nothing buffered and
 * no fetch in flight to their leader. The partition order rotates by one each time, so with
 * max.poll.records smaller than a fetch, no partition is always drained last.
 */
function sendFetches(ctx: EngineContext, consumer: ConsumerState, assigned: Assigned[]): void {
  const count = assigned.length;
  if (count === 0) return;
  const first = consumer.nextPartitionIndex % count;
  const byLeader = new Map<string, FetchPartition[]>();
  for (let i = 0; i < count; i++) {
    const a = assigned[(first + i) % count];
    if (a === undefined) continue;
    const partition = partitionOf(ctx, a);
    if (partition === undefined) {
      // The cached metadata names a partition that does not exist (the topic was re-created
      // smaller): ask for fresh metadata.
      consumer.metadata.updateRequested = true;
      continue;
    }
    const position = consumer.positions.get(a.key);
    if (position === undefined || consumer.fetchesInFlight.has(partition.leader)) continue;
    if (consumer.buffer.some((chunk) => chunk.key === a.key)) continue;
    const list = byLeader.get(partition.leader) ?? [];
    list.push({ topic: a.topic, partition: a.partition, fetchOffset: position.offset });
    byLeader.set(partition.leader, list);
  }
  for (const [broker, partitions] of byLeader) {
    consumer.fetchesInFlight.add(broker);
    ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
      type: 'FetchRequest',
      consumerId: consumer.config.id,
      incarnation: consumer.incarnation,
      fetchId: ++ctx.state.counters.fetch,
      broker,
      partitions,
      minBytes: consumer.config.fetchMinBytes,
      maxWaitMs: consumer.config.fetchMaxWaitMs,
    });
  }
  if (byLeader.size > 0) consumer.nextPartitionIndex = (first + 1) % count;
}

/** A fetch response reaches the consumer: buffer its records; wake poll() if it is waiting. */
export function handleFetchResponse(ctx: EngineContext, task: Task<'FetchResponse'>): void {
  const consumer = running(ctx, task.consumerId, task.incarnation);
  if (consumer === undefined) return; // the consumer died or restarted: nobody reads this
  consumer.fetchesInFlight.delete(task.broker);
  for (const result of task.partitions) {
    const key = partitionName(result.topic, result.partition);
    // Discard data for partitions no longer assigned or whose position moved on (stale fetch).
    if (consumer.positions.get(key)?.offset !== result.fetchOffset) continue;
    if (result.error === 'UNKNOWN_TOPIC_OR_PARTITION' || result.topicId === null) {
      consumer.metadata.updateRequested = true;
      continue;
    }
    // The broker checked the fetch offset against this topic life: the position belongs to it.
    const { topicId } = result;
    if (result.error === 'OFFSET_OUT_OF_RANGE') {
      const partition = ctx.state.topics.get(result.topic)?.partitions[result.partition];
      if (partition !== undefined) {
        consumer.positions.set(key, { offset: resetOffset(consumer, partition), topicId });
      }
      continue;
    }
    consumer.positions.set(key, { offset: result.fetchOffset, topicId });
    if (result.records.length > 0) {
      consumer.buffer.push({
        key,
        topic: result.topic,
        partition: result.partition,
        topicId,
        records: [...result.records],
      });
    }
  }
  // An empty response also ends the wait: poll() loops and sends the next fetch.
  if (consumer.inPoll) poll(ctx, consumer);
}

/**
 * Commits the consumer's current position for each given partition, if it moved. The coordinator
 * refuses partitions that do not exist (UNKNOWN_TOPIC_OR_PARTITION, KafkaApis
 * #handleOffsetCommitRequest); the consumer only logs a failed auto-commit, so nothing is stored
 * or emitted. A position read from an earlier life of a re-created topic is refused the same way
 * (simplifications: "Commits after a topic is re-created").
 */
function commitPositions(
  ctx: EngineContext,
  consumer: ConsumerState,
  keys: readonly string[],
): void {
  const group = groupOf(ctx, consumer.config.groupId);
  for (const key of keys) {
    const position = consumer.positions.get(key);
    if (position === undefined || group.committed.get(key) === position.offset) continue;
    const dash = key.lastIndexOf('-');
    const topic = key.slice(0, dash);
    const partition = Number(key.slice(dash + 1));
    const live = ctx.state.topics.get(topic);
    if (live?.partitions[partition] === undefined || live.topicId !== position.topicId) continue;
    group.committed.set(key, position.offset);
    ctx.emit({
      type: 'OffsetCommitted',
      groupId: consumer.config.groupId,
      consumerId: consumer.config.id,
      topic,
      partition,
      offset: position.offset,
    });
  }
}

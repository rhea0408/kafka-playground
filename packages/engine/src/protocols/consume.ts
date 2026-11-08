/**
 * Consuming records: the classic KafkaConsumer's poll loop (the default `group.protocol` in Kafka
 * 4.x), as a member of a consumer group. Kafka docs, Design §"The Consumer"; KafkaConsumer,
 * ClassicKafkaConsumer, FetchCollector and ConsumerConfig. Group membership (joining, heartbeats,
 * rebalances, commits) is in consumer-coordinator.ts; the broker side in group-coordinator.ts.
 *
 * The application loop is `while (running) { records = poll(); process(records); }`.
 *
 * poll() (ClassicKafkaConsumer#poll / #pollForFetches):
 * 1. Group housekeeping (ConsumerCoordinator#poll): apply a finished rebalance, rejoin when
 *    needed, leave when nothing is subscribed. poll() does not wait for a rebalance to finish; it
 *    goes on with the partitions the member owns (none for an eager member that is rejoining).
 * 2. Auto-commit once auto.commit.interval.ms has passed (inside poll(), as in the classic
 *    consumer; that is why the javadoc asks to finish processing one poll's records before the
 *    next: at-least-once).
 * 3. Owned partitions without a position start from the group's committed offset; without one (or
 *    if it is out of range), auto.offset.reset decides: "earliest" = log start offset, "latest" =
 *    the end offset, which for read_uncommitted consumers is the high watermark.
 * 4. "If data is available already, return it immediately": records already in the fetch buffer
 *    are returned first, at most max.poll.records in total; leftovers stay buffered
 *    (FetchCollector#collectFetch). RecordConsumed is emitted here, when poll() hands a record to
 *    the application, and the position moves past it.
 * 5. Fetches are sent for owned partitions with nothing buffered, one request per leader that has
 *    no request in flight ("won't resend pending fetches"). When records were returned this
 *    happens before returning them, so the next fetch runs while the application processes
 *    ("send off the next round of fetches … to enable pipelining").
 * 6. With nothing to return, the application stays blocked in poll() until a fetch response, a
 *    rebalance step or the next auto-commit deadline wakes it (pollForFetches caps its wait with
 *    coordinator.timeToNextPoll).
 *
 * The poll timer (max.poll.interval.ms) runs while the application is outside poll().
 *
 * A fetch response is discarded for partitions that are no longer owned, or whose position no
 * longer matches the fetch offset ("discarding stale fetch response", FetchCollector).
 * OFFSET_OUT_OF_RANGE resets the position with auto.offset.reset; UNKNOWN_TOPIC_OR_PARTITION
 * requests a metadata refresh; UNKNOWN_TOPIC_ID restarts the partition. The broker side of the
 * long poll is in fetch.ts.
 *
 * Processing takes processingTimeMs per record (game setting); the next poll() happens after it.
 * The commit mode (game setting, M3 Q7) stands in for the application's commit calls:
 * `sync-after-processing` calls commitSync() after processing a batch and waits for the answer
 * before polling again; `async-after-processing` calls commitAsync() and polls at once (a failure
 * is only reported); `sync-before-processing` calls commitSync() as soon as poll() returns and
 * processes the batch after the answer (at-most-once); `auto` leaves it to enable.auto.commit.
 *
 * close() (StopConsumer after the current batch, RemoveComponent, a group.id change) commits
 * when auto-commit is on and leaves the group; a crash (KillConsumer) does neither. Both start a
 * new incarnation, so responses and tasks of the old life are ignored.
 *
 * Teaching aid (M3 Q9, no Kafka equivalent): every record handed to the application is compared
 * with the highest offset its group has already handed out for that partition, giving
 * DuplicateProcessed (at or below it) or RecordsSkipped (a gap above it). A crash takes back the
 * records of the current batch that were not processed yet.
 */

import { assertNever } from '../core/assert-never.ts';
import type { EngineContext } from '../model/context.ts';
import { createGroup } from '../model/group.ts';
import { partitionName, type LogRecord, type PartitionState } from '../model/partition.ts';
import {
  createClientGroupState,
  isAlive,
  topicsOfConsumer,
  type ConsumerState,
  type GroupState,
} from '../model/state.ts';
import type { EngineTask, FetchPartition } from '../model/tasks.ts';
import { tpKey } from './assignors/index.ts';
import {
  commitPositions,
  handleConsumerHeartbeat,
  handleConsumerJoinPrepareTimeout,
  handleConsumerPollTimeout,
  handleCoordinatorResponse,
  leaveGroup,
  ownedPartitionKeys,
  pollCoordinator,
  resetClientGroup,
  startPollTimer,
  stopPollTimer,
  type PollHooks,
} from './consumer-coordinator.ts';
import { createClientMetadata, refreshIfStale, resetClientMetadata } from './metadata.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/** retry.backoff.ms default: the wait before asking for a coordinator again. */
const RETRY_BACKOFF_MS = 100;

/**
 * A topic was deleted: every group's committed offsets for it go too. In KRaft the broker's
 * metadata publisher calls GroupCoordinator#onPartitionsDeleted for the deleted partitions, which
 * removes their offsets, so a topic later re-created with the same name starts with none.
 */
export function deleteCommittedOffsets(ctx: EngineContext, topic: string): void {
  for (const group of ctx.state.groups.values()) {
    for (const map of [group.committed, group.processed]) {
      for (const key of [...map.keys()]) {
        if (key.slice(0, key.lastIndexOf('-')) === topic) map.delete(key);
      }
    }
  }
}

/** The group's coordinator-side state (created on first use, as the coordinator does). */
export function groupOf(ctx: EngineContext, groupId: string): GroupState {
  let group = ctx.state.groups.get(groupId);
  if (group === undefined) {
    group = createGroup(groupId);
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
    group: createClientGroupState(),
    stopRequested: false,
    batch: null,
    appCommit: null,
    stats: { consumed: 0 },
  };
  ctx.state.consumers.set(config.id, consumer);
  runConsumer(ctx, consumer);
  return consumer;
}

/**
 * Starts (or restarts) the poll loop as a fresh process: no positions, buffer, metadata or group
 * membership; it joins its group as a new member.
 */
export function runConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  const now = ctx.kernel.now();
  consumer.incarnation = ++ctx.state.counters.incarnation;
  consumer.status = 'running';
  forgetProcessState(consumer);
  resetClientMetadata(consumer.metadata, now);
  resetClientGroup(consumer);
  consumer.lastAutoCommitAt = now;
  consumer.nextPartitionIndex = 0;
  schedulePoll(ctx, consumer, now);
}

/**
 * A crash: the process stops. Nothing is committed, no LeaveGroup is sent and heartbeats stop, so
 * the coordinator only notices after session.timeout.ms. Records of the current batch that were
 * not processed yet are taken back from the duplicate/skip detector.
 */
export function killConsumer(
  ctx: EngineContext,
  consumer: ConsumerState,
  status: 'crashed' | 'stopped' = 'crashed',
): void {
  takeBackUnprocessed(ctx, consumer);
  endProcess(ctx, consumer, status);
}

/**
 * KafkaConsumer#close(): commits the owned partitions' positions when auto-commit is on, then
 * leaves the group (LeaveGroup), so the group rebalances at once. The client does not wait for
 * the answers; the requests are already on their way. The current batch counts as processed.
 */
export function closeConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  if (consumer.status === 'running') {
    if (consumer.config.enableAutoCommit) {
      commitPositions(ctx, consumer, ownedPartitionKeys(consumer));
    }
    leaveGroup(ctx, consumer, 'leave', hooks(ctx));
  }
  endProcess(ctx, consumer, 'stopped');
}

/** The process is gone: its state is forgotten and tasks of this life become stale. */
function endProcess(ctx: EngineContext, consumer: ConsumerState, status: 'crashed' | 'stopped') {
  consumer.status = status;
  forgetProcessState(consumer);
  resetClientGroup(consumer);
  // Pending polls, heartbeats and in-flight responses belong to the old incarnation and are ignored.
  consumer.incarnation = ++ctx.state.counters.incarnation;
}

/**
 * StopConsumer: a graceful shutdown. A consumer blocked in poll() closes at once (wakeup());
 * one that is processing finishes its batch first (and commits as its commit mode says).
 */
export function stopConsumer(ctx: EngineContext, consumer: ConsumerState): void {
  if (consumer.inPoll && consumer.appCommit === null) {
    closeNow(ctx, consumer);
    return;
  }
  consumer.stopRequested = true;
}

function closeNow(ctx: EngineContext, consumer: ConsumerState): void {
  closeConsumer(ctx, consumer);
  ctx.emit({ type: 'ConsumerStopped', consumerId: consumer.config.id, reason: 'close' });
}

/**
 * Crash during processing: records not processed yet are taken back, so whoever reads them next
 * is not flagged as a duplicate (they were never processed), and a commit that already covered
 * them shows up as RecordsSkipped.
 */
function takeBackUnprocessed(ctx: EngineContext, consumer: ConsumerState): void {
  const { batch } = consumer;
  if (batch === null || consumer.inPoll) return;
  const group = ctx.state.groups.get(consumer.config.groupId);
  if (group === undefined) return;
  const per = consumer.config.processingTimeMs;
  const now = ctx.kernel.now();
  const done =
    batch.startedAt === null
      ? 0
      : per === 0
        ? batch.records.length
        : Math.floor((now - batch.startedAt) / per);
  const lastHanded = new Map<string, number>();
  const firstUndone = new Map<string, number>();
  batch.records.forEach((r, i) => {
    lastHanded.set(r.key, r.offset);
    if (i >= done && !firstUndone.has(r.key)) firstUndone.set(r.key, r.offset);
  });
  for (const [key, first] of firstUndone) {
    // Only if nobody else has handed out later records of this partition meanwhile.
    if (group.processed.get(key) === lastHanded.get(key)) group.processed.set(key, first - 1);
  }
}

/**
 * Wakes a consumer that is blocked in poll() so its next poll() iteration happens now, e.g. after
 * it was connected to a topic (subscribing to a new topic) or a rebalance step arrived.
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
  consumer.stopRequested = false;
  consumer.batch = null;
  consumer.appCommit = null;
}

/** Drops one partition's position and buffered records (it was revoked or lost). */
function forgetPartition(consumer: ConsumerState, key: string): void {
  consumer.positions.delete(key);
  for (let i = consumer.buffer.length - 1; i >= 0; i--) {
    if (consumer.buffer[i]?.key === key) consumer.buffer.splice(i, 1);
  }
}

/** What the group coordinator code may do to the poll loop. */
function hooks(ctx: EngineContext): PollHooks {
  return {
    wake: (consumer) => {
      wakeConsumer(ctx, consumer);
    },
    forget: forgetPartition,
    fatal: (consumer) => {
      // The exception ends the application's loop: the process exits without closing.
      killConsumer(ctx, consumer);
      ctx.emit({ type: 'ConsumerStopped', consumerId: consumer.config.id, reason: 'error' });
    },
    commitAnswered: (consumer, commitId) => {
      appCommitAnswered(ctx, consumer, commitId);
    },
  };
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

/**
 * The current ConsumerPoll task: either the application finished processing its batch and calls
 * poll() again (after its after-processing commit), or it is woken inside a blocked poll().
 */
export function handleConsumerPoll(ctx: EngineContext, task: Task<'ConsumerPoll'>): void {
  const consumer = running(ctx, task.consumerId, task.incarnation);
  if (consumer?.pollToken !== task.token) return;
  if (!consumer.inPoll && consumer.batch !== null) {
    consumer.batch = null;
    if (commitAfterProcessing(ctx, consumer)) return; // blocked in commitSync()
  }
  if (consumer.stopRequested) {
    closeNow(ctx, consumer);
    return;
  }
  poll(ctx, consumer);
}

/** The commit mode's after-processing commit. True while commitSync() waits for its answer. */
function commitAfterProcessing(ctx: EngineContext, consumer: ConsumerState): boolean {
  const mode = consumer.config.commitMode;
  if (mode !== 'sync-after-processing' && mode !== 'async-after-processing') return false;
  const commitId = commitPositions(ctx, consumer, ownedPartitionKeys(consumer));
  if (mode === 'async-after-processing' || commitId === null) return false;
  consumer.appCommit = { commitId, then: 'poll' };
  consumer.pollToken += 1;
  return true;
}

/** commitSync() returned (accepted or failed; a failure is reported by CommitFailed). */
function appCommitAnswered(ctx: EngineContext, consumer: ConsumerState, commitId: number): void {
  const pending = consumer.appCommit;
  if (pending?.commitId !== commitId) return;
  consumer.appCommit = null;
  const now = ctx.kernel.now();
  if (pending.then === 'process' && consumer.batch !== null) {
    consumer.batch.startedAt = now;
    schedulePoll(
      ctx,
      consumer,
      now + consumer.batch.records.length * consumer.config.processingTimeMs,
    );
    return;
  }
  if (consumer.stopRequested) closeNow(ctx, consumer);
  else poll(ctx, consumer);
}

/** A coordinator answer reaches the consumer process (its network thread). */
export function handleConsumerCoordinatorResponse(
  ctx: EngineContext,
  task: Task<'CoordinatorResponse'>,
): void {
  const consumer = running(ctx, task.consumerId, task.incarnation);
  if (consumer !== undefined) handleCoordinatorResponse(ctx, consumer, task, hooks(ctx));
}

export function handleHeartbeatTask(ctx: EngineContext, task: Task<'ConsumerHeartbeat'>): void {
  const consumer = running(ctx, task.consumerId, task.incarnation);
  if (consumer !== undefined) handleConsumerHeartbeat(ctx, consumer, task);
}

export function handlePollTimeoutTask(ctx: EngineContext, task: Task<'ConsumerPollTimeout'>): void {
  const consumer = running(ctx, task.consumerId, task.incarnation);
  if (consumer !== undefined) handleConsumerPollTimeout(ctx, consumer, task);
}

export function handleJoinPrepareTimeoutTask(
  ctx: EngineContext,
  task: Task<'ConsumerJoinPrepareTimeout'>,
): void {
  const consumer = running(ctx, task.consumerId, task.incarnation);
  if (consumer !== undefined) handleConsumerJoinPrepareTimeout(consumer, task, hooks(ctx));
}

interface Assigned {
  readonly topic: string;
  readonly partition: number;
  readonly key: string;
}

const partitionOf = (ctx: EngineContext, a: Assigned): PartitionState | undefined =>
  ctx.state.topics.get(a.topic)?.partitions[a.partition];

/** One poll() call by the application (see the file comment for the steps). */
function poll(ctx: EngineContext, consumer: ConsumerState): void {
  const { config } = consumer;
  const now = ctx.kernel.now();
  stopPollTimer(consumer);
  refreshIfStale(ctx, consumer.metadata, config.metadataMaxAgeMs);
  const subscription = topicsOfConsumer(ctx.state, config.id).map((t) => t.config.id);

  const next = pollCoordinator(ctx, consumer, subscription, hooks(ctx));
  if (next !== 'fetch') {
    consumer.inPoll = true;
    if (next === 'retry') schedulePoll(ctx, consumer, now + RETRY_BACKOFF_MS);
    else consumer.pollToken += 1; // woken by a connection or a coordinator answer
    return;
  }

  // Auto-commit what earlier polls returned, once the interval has passed. Only a member in a
  // stable generation can commit (a rejoining member's commit would fail locally).
  const stable = consumer.group.state === 'stable';
  if (
    config.enableAutoCommit &&
    stable &&
    now - consumer.lastAutoCommitAt >= config.autoCommitIntervalMs
  ) {
    commitPositions(ctx, consumer, ownedPartitionKeys(consumer));
    consumer.lastAutoCommitAt = now;
  }

  const assigned: Assigned[] = consumer.group.owned.map((tp) => ({
    topic: tp.topic,
    partition: tp.partition,
    key: tpKey(tp),
  }));
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
  const unreachable = sendFetches(ctx, consumer, assigned);
  if (returned.length > 0) {
    consumer.inPoll = false;
    consumer.batch = { records: returned, startedAt: now };
    startPollTimer(ctx, consumer);
    if (config.commitMode === 'sync-before-processing') {
      // commitSync() before processing: the positions already point past this batch.
      const commitId = commitPositions(ctx, consumer, ownedPartitionKeys(consumer));
      if (commitId !== null) {
        consumer.batch.startedAt = null;
        consumer.appCommit = { commitId, then: 'process' };
        consumer.pollToken += 1;
        return;
      }
    }
    schedulePoll(ctx, consumer, now + returned.length * config.processingTimeMs);
  } else {
    // Blocked in poll(): a fetch response wakes it, and so does the auto-commit deadline.
    consumer.inPoll = true;
    if (unreachable) {
      // A partition's leader can't be reached: look again after retry.backoff.ms.
      const autoCommitAt =
        config.enableAutoCommit && stable
          ? consumer.lastAutoCommitAt + config.autoCommitIntervalMs
          : Number.POSITIVE_INFINITY;
      schedulePoll(ctx, consumer, Math.min(now + RETRY_BACKOFF_MS, autoCommitAt));
    } else if (config.enableAutoCommit && stable) {
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
  const committed = ctx.state.groups.get(consumer.config.groupId)?.committed.get(key);
  const inRange =
    committed !== undefined &&
    committed >= partition.logStartOffset &&
    committed <= partition.logEndOffset;
  if (inRange) return committed;
  return resetOffset(consumer, partition);
}

/** auto.offset.reset: "earliest" = log start offset, "latest" = HW (KafkaConsumer#endOffsets). */
function resetOffset(consumer: ConsumerState, partition: PartitionState): number {
  return consumer.config.autoOffsetReset === 'earliest'
    ? partition.logStartOffset
    : partition.highWatermark;
}

/**
 * Hands buffered records to the application, at most max.poll.records, and returns them in
 * order. Each record also goes through the duplicate/skip detector (M3 Q9).
 */
function drainBuffer(
  ctx: EngineContext,
  consumer: ConsumerState,
): { readonly key: string; readonly offset: number }[] {
  const { config } = consumer;
  const handed: { key: string; offset: number }[] = [];
  let budget = config.maxPollRecords;
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
      detectDuplicateOrSkip(ctx, consumer, chunk.topic, chunk.partition, r);
      consumer.positions.set(chunk.key, { offset: r.offset + 1, topicId: chunk.topicId });
      handed.push({ key: chunk.key, offset: r.offset });
    }
    if (chunk.records.length === 0) consumer.buffer.shift();
    budget -= taken.length;
  }
  consumer.stats.consumed += handed.length;
  return handed;
}

/** M3 Q9: compares the record with the highest offset the group has handed out so far. */
function detectDuplicateOrSkip(
  ctx: EngineContext,
  consumer: ConsumerState,
  topic: string,
  partition: number,
  record: LogRecord,
): void {
  const group = groupOf(ctx, consumer.config.groupId);
  const key = partitionName(topic, partition);
  const highest = group.processed.get(key);
  const base = { groupId: group.groupId, consumerId: consumer.config.id, topic, partition };
  if (highest !== undefined && record.offset <= highest) {
    ctx.emit({
      type: 'DuplicateProcessed',
      ...base,
      offset: record.offset,
      traceId: record.traceId,
    });
    return;
  }
  if (highest !== undefined && record.offset > highest + 1) {
    ctx.emit({ type: 'RecordsSkipped', ...base, from: highest + 1, to: record.offset - 1 });
  }
  group.processed.set(key, record.offset);
}

/**
 * Sends one FetchRequest per leader for the assigned partitions that have nothing buffered and
 * no fetch in flight to their leader. The partition order rotates by one each time, so with
 * max.poll.records smaller than a fetch, no partition is always drained last.
 */
function sendFetches(ctx: EngineContext, consumer: ConsumerState, assigned: Assigned[]): boolean {
  let unreachable = false;
  const count = assigned.length;
  if (count === 0) return false;
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
    // TODO(M4-T7): the client's metadata decides where to fetch. No leader, or a leader whose
    // process is down (the connection is refused): nothing is sent, and poll() retries after
    // retry.backoff.ms (NetworkClient: the node is not ready).
    const { leader } = partition;
    const leaderBroker = leader === null ? undefined : ctx.state.brokers.get(leader);
    if (leader === null || leaderBroker === undefined || !isAlive(leaderBroker.status)) {
      unreachable = true;
      continue;
    }
    if (position === undefined || consumer.fetchesInFlight.has(leader)) continue;
    if (consumer.buffer.some((chunk) => chunk.key === a.key)) continue;
    const list = byLeader.get(leader) ?? [];
    list.push({
      topic: a.topic,
      partition: a.partition,
      topicId: position.topicId,
      fetchOffset: position.offset,
    });
    byLeader.set(leader, list);
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
  return unreachable;
}

/** A fetch response reaches the consumer: buffer its records; wake poll() if it is waiting. */
export function handleFetchResponse(ctx: EngineContext, task: Task<'FetchResponse'>): void {
  const consumer = running(ctx, task.consumerId, task.incarnation);
  if (consumer === undefined) return; // the consumer died or restarted: nobody reads this
  consumer.fetchesInFlight.delete(task.broker);
  for (const result of task.partitions) {
    const key = partitionName(result.topic, result.partition);
    // Discard data for partitions no longer assigned or whose position moved on (stale fetch).
    const position = consumer.positions.get(key);
    if (position?.offset !== result.fetchOffset || position.topicId !== result.topicId) continue;
    const { topicId } = result;
    switch (result.error) {
      case 'UNKNOWN_TOPIC_OR_PARTITION':
      case 'NOT_LEADER_OR_FOLLOWER':
        // InvalidMetadataException: refresh, and fetch again from the new leader (K7).
        consumer.metadata.updateRequested = true;
        continue;
      case 'UNKNOWN_TOPIC_ID':
        // The topic was re-created: the position belongs to its old life. Forget it, so the next
        // poll() starts the partition again from the committed offset (deleted with the old
        // topic) or auto.offset.reset (simplifications: "Commits after a topic is re-created").
        consumer.metadata.updateRequested = true;
        consumer.positions.delete(key);
        continue;
      case 'OFFSET_OUT_OF_RANGE': {
        const partition = ctx.state.topics.get(result.topic)?.partitions[result.partition];
        if (partition !== undefined) {
          consumer.positions.set(key, { offset: resetOffset(consumer, partition), topicId });
        }
        continue;
      }
      case null:
        break;
      default:
        assertNever(result);
    }
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

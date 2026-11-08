/**
 * Replication between brokers (Kafka docs, Design §"Replication"; Partition.scala,
 * ReplicaFetcherThread / AbstractFetcherThread, ReplicaManager; M4-decisions K3–K6, ADR 0005).
 *
 * Followers copy the leader's log by **fetching**, just like consumers, except that they read up
 * to the leader's log end (not the HW) and their fetch offset tells the leader how far they are:
 *
 *   follower → ReplicaFetchRequest (its LEO per partition)          (+ network delay)
 *   leader   → updates the follower's LEO and last-caught-up time, recomputes the HW,
 *              answers at once if it has records past the fetch offset, else parks the request
 *              for replica.fetch.wait.max.ms (a long poll, M4 Q4)
 *   leader   → ReplicaFetchResponse (records, the leader's HW)       (+ network delay)
 *   follower → appends, sets its HW to min(leader HW, own LEO), fetches again at once
 *
 * One fetcher per (follower broker, leader broker) pair carries every partition the follower
 * follows on that leader, with at most one request in flight (AbstractFetcherManager).
 *
 * The HW (Partition#maybeIncrementLeaderHW, K5): the minimum LEO over the replicas in the
 * maximal ISR, plus replicas that are caught up and eligible to join it; it only moves forward,
 * and not at all while the ISR is smaller than min.insync.replicas ("strict min ISR", KIP-966,
 * K1). A follower whose LEO the leader doesn't know yet (-1) holds the HW back.
 *
 * acks=all produce requests wait in the DelayedProduce purgatory until the HW passes their
 * record (Partition#checkEnoughReplicasReachOffset, K6), or time out after the request's timeout
 * (request.timeout.ms) with REQUEST_TIMED_OUT.
 */

import type { Topic } from '@kp/schema';
import type { EngineContext } from '../model/context.ts';
import {
  appendToReplica,
  leaderLog,
  type FollowerState,
  type PartitionState,
} from '../model/partition.ts';
import type { TopicState } from '../model/state.ts';
import type {
  EngineTask,
  ParkedProduce,
  ReplicaFetchPartition,
  ReplicaFetchRequestTask,
  ReplicaFetchResult,
} from '../model/tasks.ts';
import { onHighWatermarkAdvanced } from './fetch.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/** replica.fetch.min.bytes default: answer as soon as there is any record to copy. */
const REPLICA_FETCH_MIN_RECORDS = 1;

const fetcherKey = (follower: string, leader: string): string => `${follower}>${leader}`;

/** The broker if its process is running in this incarnation, else undefined. */
function runningBroker(ctx: EngineContext, id: string, incarnation?: number) {
  const broker = ctx.state.brokers.get(id);
  if (broker?.status !== 'running') return undefined;
  if (incarnation !== undefined && broker.incarnation !== incarnation) return undefined;
  return broker;
}

/** min.insync.replicas as the leader applies it: at most the replication factor (K6). */
export function effectiveMinIsr(topic: Topic, partition: PartitionState): number {
  return Math.min(topic.minInsyncReplicas, partition.replicas.length);
}

/** The ISR the HW waits for: the committed ISR, or a larger one while a change is pending (K4). */
export function maximalIsr(partition: PartitionState): readonly string[] {
  return partition.pendingIsr ?? partition.isr;
}

/** replica.lag.time.max.ms of the leader's broker. */
function replicaLagTimeMaxMs(ctx: EngineContext, partition: PartitionState): number {
  const leader = partition.leader === null ? undefined : ctx.state.brokers.get(partition.leader);
  return leader?.config.replicaLagTimeMaxMs ?? 30_000;
}

/** ReplicaState#isCaughtUp (K3). */
export function isCaughtUp(
  follower: FollowerState,
  leaderLogEndOffset: number,
  now: number,
  replicaLagTimeMaxMs: number,
): boolean {
  return (
    follower.logEndOffset === leaderLogEndOffset ||
    now - follower.lastCaughtUpTimeMs <= replicaLagTimeMaxMs
  );
}

/** The leader's view of a follower, created on first use (LEO unknown). */
export function followerState(partition: PartitionState, broker: string): FollowerState {
  let state = partition.followers.get(broker);
  if (state === undefined) {
    state = {
      logEndOffset: -1,
      lastCaughtUpTimeMs: 0,
      lastFetchTimeMs: 0,
      lastFetchLeaderLogEndOffset: 0,
    };
    partition.followers.set(broker, state);
  }
  return state;
}

/**
 * Replica#updateFetchStateOrThrow (K3): a fetch at `fetchOffset` tells the leader the follower's
 * LEO. The follower counts as caught up at this fetch if it asked for everything the leader had,
 * or at its previous fetch if it has now reached what the leader had then.
 */
export function updateFollowerFetchState(
  partition: PartitionState,
  broker: string,
  fetchOffset: number,
  now: number,
): void {
  const leader = leaderLog(partition);
  if (leader === undefined) return;
  const f = followerState(partition, broker);
  if (fetchOffset >= leader.logEndOffset)
    f.lastCaughtUpTimeMs = Math.max(f.lastCaughtUpTimeMs, now);
  else if (fetchOffset >= f.lastFetchLeaderLogEndOffset) {
    f.lastCaughtUpTimeMs = Math.max(f.lastCaughtUpTimeMs, f.lastFetchTimeMs);
  }
  f.logEndOffset = fetchOffset;
  f.lastFetchLeaderLogEndOffset = leader.logEndOffset;
  f.lastFetchTimeMs = now;
}

/**
 * Partition#maybeIncrementLeaderHW (K5). Returns true when the HW moved; the caller then wakes
 * whatever waits on it (consumer fetches, acks=all produces).
 */
export function maybeIncrementHighWatermark(
  ctx: EngineContext,
  topic: TopicState,
  partition: PartitionState,
): boolean {
  const leader = leaderLog(partition);
  if (leader === undefined) return false;
  // Strict min ISR (KIP-966): no progress while the committed ISR is below min.insync.replicas.
  if (partition.isr.length < effectiveMinIsr(topic.config, partition)) return false;
  const now = ctx.kernel.now();
  const lag = replicaLagTimeMaxMs(ctx, partition);
  const isr = maximalIsr(partition);
  let hw = leader.logEndOffset;
  for (const broker of partition.replicas) {
    if (broker === partition.leader) continue;
    const f = partition.followers.get(broker);
    const inIsr = isr.includes(broker);
    // shouldWaitForReplicaToJoinIsr: a caught-up, eligible replica outside the ISR also counts.
    const joining =
      !inIsr &&
      f !== undefined &&
      f.logEndOffset >= 0 &&
      isCaughtUp(f, leader.logEndOffset, now, lag) &&
      runningBroker(ctx, broker) !== undefined;
    if (!inIsr && !joining) continue;
    hw = Math.min(hw, f?.logEndOffset ?? -1);
  }
  if (hw <= leader.highWatermark) return false;
  leader.highWatermark = hw;
  return true;
}

/** Recomputes the HW and, if it moved, wakes consumer fetches and acks=all produces. */
export function onLeaderStateChanged(
  ctx: EngineContext,
  topic: TopicState,
  partition: PartitionState,
): void {
  if (!maybeIncrementHighWatermark(ctx, topic, partition)) return;
  onHighWatermarkAdvanced(ctx, partition);
  completeDelayedProduces(ctx, topic, partition);
}

// ── Fetchers ──────────────────────────────────────────────────────────────────────────────────

/** Starts the fetcher of every follower of `partition` that has none in flight. */
export function startFollowers(ctx: EngineContext, partition: PartitionState): void {
  const { leader } = partition;
  if (leader === null) return;
  for (const broker of partition.replicas) {
    if (broker !== leader) ensureReplicaFetcher(ctx, broker, leader);
  }
}

/**
 * Sends the next fetch from `follower` to `leader` if none is in flight: every partition the
 * follower holds whose leader is `leader`, from the follower's own LEO with its last epoch (K9).
 */
export function ensureReplicaFetcher(ctx: EngineContext, follower: string, leader: string): void {
  const { state } = ctx;
  const key = fetcherKey(follower, leader);
  if (state.replicaFetchers.has(key)) return;
  const followerBroker = runningBroker(ctx, follower);
  if (followerBroker === undefined || runningBroker(ctx, leader) === undefined) return;
  const partitions: ReplicaFetchPartition[] = [];
  for (const topic of state.topics.values()) {
    for (const p of topic.partitions) {
      if (p.leader !== leader || !p.replicas.includes(follower) || follower === leader) continue;
      const own = p.replicaLogs.get(follower);
      if (own === undefined) continue;
      partitions.push({
        topic: topic.config.id,
        partition: p.index,
        topicId: topic.topicId,
        fetchOffset: own.logEndOffset,
        lastFetchedEpoch: own.epochs[own.epochs.length - 1]?.epoch ?? -1,
        currentLeaderEpoch: p.leaderEpoch,
      });
    }
  }
  if (partitions.length === 0) return;
  state.replicaFetchers.add(key);
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'ReplicaFetchRequest',
    fetchId: ++state.counters.replicaFetch,
    follower,
    followerIncarnation: followerBroker.incarnation,
    leader,
    partitions,
    maxWaitMs: followerBroker.config.replicaFetchWaitMaxMs,
  });
}

/** A follower's fetch reaches the leader: record its progress, then answer or park. */
export function handleReplicaFetchRequest(ctx: EngineContext, task: ReplicaFetchRequestTask): void {
  if (runningBroker(ctx, task.leader) === undefined) return; // lost with the broker (T5)
  const now = ctx.kernel.now();
  for (const p of task.partitions) {
    const topic = ctx.state.topics.get(p.topic);
    const partition = topic?.partitions[p.partition];
    if (topic === undefined || partition === undefined || topic.topicId !== p.topicId) continue;
    if (partition.leader !== task.leader || partition.leaderEpoch !== p.currentLeaderEpoch)
      continue;
    updateFollowerFetchState(partition, task.follower, p.fetchOffset, now);
    onLeaderStateChanged(ctx, topic, partition);
  }
  if (task.maxWaitMs === 0 || hasDataFor(ctx, task)) {
    respondToReplicaFetch(ctx, task);
    return;
  }
  ctx.state.parkedReplicaFetches.set(task.fetchId, task);
  ctx.kernel.scheduleIn(task.maxWaitMs, { type: 'ReplicaFetchExpire', fetchId: task.fetchId });
}

/** replica.fetch.wait.max.ms passed: answer with whatever there is (usually nothing). */
export function handleReplicaFetchExpire(
  ctx: EngineContext,
  task: Task<'ReplicaFetchExpire'>,
): void {
  const parked = ctx.state.parkedReplicaFetches.get(task.fetchId);
  if (parked === undefined) return;
  ctx.state.parkedReplicaFetches.delete(task.fetchId);
  respondToReplicaFetch(ctx, parked);
}

/** The leader appended to `partition`: parked follower fetches for it can complete. */
export function onLeaderAppended(ctx: EngineContext, partition: PartitionState): void {
  for (const [fetchId, parked] of ctx.state.parkedReplicaFetches) {
    const touches = parked.partitions.some(
      (p) => p.topic === partition.topic && p.partition === partition.index,
    );
    if (!touches || !hasDataFor(ctx, parked)) continue;
    ctx.state.parkedReplicaFetches.delete(fetchId);
    respondToReplicaFetch(ctx, parked);
  }
}

/** Whether any partition in the request has records past its fetch offset, or an error. */
function hasDataFor(ctx: EngineContext, task: ReplicaFetchRequestTask): boolean {
  let records = 0;
  for (const p of task.partitions) {
    const partition = ctx.state.topics.get(p.topic)?.partitions[p.partition];
    const leader = partition === undefined ? undefined : leaderLog(partition);
    if (partition === undefined || leader === undefined || partition.leader !== task.leader)
      return true;
    records += Math.max(0, leader.logEndOffset - p.fetchOffset);
  }
  return records >= REPLICA_FETCH_MIN_RECORDS;
}

/** Builds the response from the leader's log now and sends it back (+ network delay). */
function respondToReplicaFetch(ctx: EngineContext, task: ReplicaFetchRequestTask): void {
  const now = ctx.kernel.now();
  const partitions = task.partitions.map((p): ReplicaFetchResult => {
    const topic = ctx.state.topics.get(p.topic);
    const partition = topic?.partitions[p.partition];
    if (topic === undefined || partition === undefined || topic.topicId !== p.topicId) {
      return { ...p, error: 'UNKNOWN_TOPIC_OR_PARTITION' };
    }
    const leader = leaderLog(partition);
    if (leader === undefined || partition.leader !== task.leader) {
      return { ...p, error: 'NOT_LEADER_OR_FOLLOWER' };
    }
    if (partition.leaderEpoch !== p.currentLeaderEpoch)
      return { ...p, error: 'FENCED_LEADER_EPOCH' };
    // A parked fetch completing reads the log again, which updates the follower's state again
    // (DelayedFetch#onComplete → readFromLog).
    updateFollowerFetchState(partition, task.follower, p.fetchOffset, now);
    const base = leader.logStartOffset;
    return {
      ...p,
      error: null,
      records: leader.log.slice(Math.max(0, p.fetchOffset - base)),
      highWatermark: leader.highWatermark,
    };
  });
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'ReplicaFetchResponse',
    fetchId: task.fetchId,
    follower: task.follower,
    followerIncarnation: task.followerIncarnation,
    leader: task.leader,
    partitions,
  });
}

/** The leader's answer reaches the follower: append, adopt the HW, fetch again. */
export function handleReplicaFetchResponse(
  ctx: EngineContext,
  task: Task<'ReplicaFetchResponse'>,
): void {
  ctx.state.replicaFetchers.delete(fetcherKey(task.follower, task.leader));
  if (runningBroker(ctx, task.follower, task.followerIncarnation) === undefined) return;
  for (const result of task.partitions) {
    if (result.error !== null) continue; // the leader moved: the controller's metadata decides (T5)
    const topic = ctx.state.topics.get(result.topic);
    const partition = topic?.partitions[result.partition];
    if (topic === undefined || partition === undefined || topic.topicId !== result.topicId)
      continue;
    if (partition.leader === task.follower) continue; // it became the leader meanwhile
    const own = partition.replicaLogs.get(task.follower);
    if (own === undefined || own.logEndOffset !== result.fetchOffset) continue; // stale
    const from = own.logEndOffset;
    for (const record of result.records) {
      if (record.offset === own.logEndOffset) appendToReplica(own, record);
    }
    own.highWatermark = Math.max(
      own.highWatermark,
      Math.min(result.highWatermark, own.logEndOffset),
    );
    if (own.logEndOffset > from) {
      ctx.emit({
        type: 'ReplicaFetched',
        broker: task.follower,
        leader: task.leader,
        topic: result.topic,
        partition: result.partition,
        fromOffset: from,
        toOffset: own.logEndOffset,
      });
    }
  }
  ensureReplicaFetcher(ctx, task.follower, task.leader);
}

// ── acks=all: the DelayedProduce purgatory ─────────────────────────────────────────────────────

/** Where a produce response goes: the producer life and the record it answers. */
export type ProduceTarget = Omit<ParkedProduce, 'produceId' | 'offset'>;

/** Sends a produce response back to the producer (+ network delay). */
export function sendProduceResponse(
  ctx: EngineContext,
  target: ProduceTarget,
  result: Task<'ProduceResponse'>['result'],
): void {
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'ProduceResponse',
    producerId: target.producerId,
    incarnation: target.incarnation,
    topic: target.topic,
    partition: target.partition,
    traceId: target.traceId,
    result,
  });
}

/** Parks an acks=all produce until the HW passes `offset` or `timeoutMs` passes (K6). */
export function parkProduce(
  ctx: EngineContext,
  target: ProduceTarget,
  offset: number,
  timeoutMs: number,
): void {
  const produceId = ++ctx.state.counters.produce;
  ctx.state.parkedProduces.set(produceId, { ...target, produceId, offset });
  ctx.kernel.scheduleIn(timeoutMs, { type: 'DelayedProduceExpire', produceId });
}

/**
 * Answers parked acks=all produces of `partition` whose record is now below the HW
 * (checkEnoughReplicasReachOffset, K6): success, or NOT_ENOUGH_REPLICAS_AFTER_APPEND when the
 * ISR shrank below min.insync.replicas meanwhile.
 */
export function completeDelayedProduces(
  ctx: EngineContext,
  topic: TopicState,
  partition: PartitionState,
): void {
  for (const [id, parked] of ctx.state.parkedProduces) {
    if (parked.topic !== partition.topic || parked.partition !== partition.index) continue;
    if (partition.highWatermark <= parked.offset) continue;
    ctx.state.parkedProduces.delete(id);
    const enough = maximalIsr(partition).length >= effectiveMinIsr(topic.config, partition);
    sendProduceResponse(
      ctx,
      parked,
      enough
        ? { ok: true, offset: parked.offset }
        : { ok: false, error: 'NOT_ENOUGH_REPLICAS_AFTER_APPEND' },
    );
  }
}

/** The produce request's timeout passed while it waited: REQUEST_TIMED_OUT. */
export function handleDelayedProduceExpire(
  ctx: EngineContext,
  task: Task<'DelayedProduceExpire'>,
): void {
  const parked = ctx.state.parkedProduces.get(task.produceId);
  if (parked === undefined) return;
  ctx.state.parkedProduces.delete(task.produceId);
  sendProduceResponse(ctx, parked, { ok: false, error: 'REQUEST_TIMED_OUT' });
}

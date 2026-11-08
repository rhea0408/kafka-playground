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
import { isrEligible, sendAlterPartition } from './controller.ts';
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

function newFollowerState(inIsr: boolean, now: number): FollowerState {
  return {
    logEndOffset: -1,
    // Kafka's 0 means 1970, long ago; the sim clock starts at 0, so "never" is -Infinity.
    lastCaughtUpTimeMs: inIsr ? now : Number.NEGATIVE_INFINITY,
    lastFetchTimeMs: Number.NEGATIVE_INFINITY,
    lastFetchLeaderLogEndOffset: 0,
  };
}

/**
 * The leader's view of a follower. `resetFollowers` creates them when the partition gets its
 * leader; one created here (a replica the leader never heard of) starts "never caught up".
 */
export function followerState(partition: PartitionState, broker: string): FollowerState {
  let state = partition.followers.get(broker);
  if (state === undefined) {
    state = newFollowerState(false, 0);
    partition.followers.set(broker, state);
  }
  return state;
}

/**
 * Partition#makeLeader → Replica#resetReplicaState: the leader knows no follower's LEO yet, and
 * counts the ISR followers as caught up now, the others never (K3).
 */
export function resetFollowers(ctx: EngineContext, partition: PartitionState): void {
  const now = ctx.kernel.now();
  partition.followers.clear();
  for (const broker of partition.replicas) {
    if (broker === partition.leader) continue;
    partition.followers.set(broker, newFollowerState(partition.isr.includes(broker), now));
  }
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
  if (fetchOffset >= leader.logEndOffset) {
    f.lastCaughtUpTimeMs = Math.max(f.lastCaughtUpTimeMs, now);
  } else if (fetchOffset >= f.lastFetchLeaderLogEndOffset) {
    f.lastCaughtUpTimeMs = Math.max(f.lastCaughtUpTimeMs, f.lastFetchTimeMs);
  }
  f.logEndOffset = fetchOffset;
  f.lastFetchLeaderLogEndOffset = Math.max(leader.logEndOffset, f.lastFetchLeaderLogEndOffset);
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

// ── ISR management (M4-T4) ───────────────────────────────────────────────────────────────────

/** Starts a broker's periodic ISR expiration check (every replica.lag.time.max.ms / 2). */
export function scheduleIsrExpiration(ctx: EngineContext, brokerId: string): void {
  const broker = ctx.state.brokers.get(brokerId);
  if (broker === undefined) return;
  ctx.kernel.scheduleIn(Math.max(1, Math.floor(broker.config.replicaLagTimeMaxMs / 2)), {
    type: 'IsrExpirationCheck',
    broker: brokerId,
    incarnation: broker.incarnation,
  });
}

/**
 * ReplicaManager#maybeShrinkIsr → Partition#maybeShrinkIsr (K3): every partition this broker
 * leads drops the followers that have not caught up within replica.lag.time.max.ms. Skipped
 * while an AlterPartition is in flight.
 */
export function handleIsrExpirationCheck(
  ctx: EngineContext,
  task: Task<'IsrExpirationCheck'>,
): void {
  if (runningBroker(ctx, task.broker, task.incarnation) === undefined) return; // an old life
  const now = ctx.kernel.now();
  for (const topic of ctx.state.topics.values()) {
    for (const partition of topic.partitions) {
      if (partition.leader !== task.broker || partition.pendingIsr !== null) continue;
      const leader = leaderLog(partition);
      if (leader === undefined) continue;
      const lag = replicaLagTimeMaxMs(ctx, partition);
      const outOfSync = partition.isr.filter((b) => {
        if (b === partition.leader) return false;
        const f = partition.followers.get(b);
        return f === undefined || !isCaughtUp(f, leader.logEndOffset, now, lag);
      });
      if (outOfSync.length === 0) continue;
      sendAlterPartition(
        ctx,
        topic,
        partition,
        partition.isr.filter((b) => !outOfSync.includes(b)),
      );
    }
  }
  scheduleIsrExpiration(ctx, task.broker);
}

/**
 * Partition#maybeExpandIsr (K4): a follower outside the ISR rejoins once its LEO reaches the HW
 * and the start of the current leader epoch, if its broker is eligible (KIP-841).
 */
function maybeExpandIsr(
  ctx: EngineContext,
  topic: TopicState,
  partition: PartitionState,
  follower: string,
): void {
  if (partition.pendingIsr !== null || partition.isr.includes(follower)) return;
  const f = partition.followers.get(follower);
  const leader = leaderLog(partition);
  if (f === undefined || leader === undefined) return;
  if (f.logEndOffset < leader.highWatermark) return;
  if (f.logEndOffset < partition.leaderEpochStartOffset) return;
  if (!isrEligible(ctx, follower)) return;
  sendAlterPartition(ctx, topic, partition, [...partition.isr, follower]);
}

/** The controller answered an AlterPartition: the leader adopts the committed ISR. */
export function handleAlterPartitionResponse(
  ctx: EngineContext,
  task: Task<'AlterPartitionResponse'>,
): void {
  if (runningBroker(ctx, task.leader, task.leaderIncarnation) === undefined) return;
  const topic = ctx.state.topics.get(task.topic);
  const partition = topic?.partitions[task.partition];
  if (topic === undefined || partition === undefined || topic.topicId !== task.topicId) return;
  if (partition.leader !== task.leader) return; // it lost leadership meanwhile
  partition.pendingIsr = null;
  // With the confirmed (possibly smaller) ISR the HW may move, unblocking acks=all produces.
  onLeaderStateChanged(ctx, topic, partition);
}

// ── Fetchers ──────────────────────────────────────────────────────────────────────────────────

/** replica.fetch.backoff.ms default: the wait after a failed fetch (AbstractFetcherThread). */
export const REPLICA_FETCH_BACKOFF_MS = 1000;

/** Starts the fetcher of every follower of `partition` that has none in flight. */
export function startFollowers(ctx: EngineContext, partition: PartitionState): void {
  const { leader } = partition;
  if (leader === null) return;
  if (partition.followers.size === 0) resetFollowers(ctx, partition);
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
  const leaderBroker = runningBroker(ctx, leader);
  if (followerBroker === undefined || leaderBroker === undefined) return;
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
  state.replicaFetchers.set(key, followerBroker.incarnation);
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'ReplicaFetchRequest',
    fetchId: ++state.counters.replicaFetch,
    follower,
    followerIncarnation: followerBroker.incarnation,
    leader,
    leaderIncarnation: leaderBroker.incarnation,
    partitions,
    maxWaitMs: followerBroker.config.replicaFetchWaitMaxMs,
  });
}

/**
 * Frees a fetcher slot held by the follower life `incarnation` (or an older one) and, if the
 * follower is running, fetches again at once.
 */
function releaseFetcher(
  ctx: EngineContext,
  follower: string,
  incarnation: number,
  leader: string,
): void {
  const key = fetcherKey(follower, leader);
  const owner = ctx.state.replicaFetchers.get(key);
  if (owner !== undefined && owner > incarnation) return; // a newer life fetches already
  ctx.state.replicaFetchers.delete(key);
  ensureReplicaFetcher(ctx, follower, leader);
}

/** A follower's fetch reaches the leader: record its progress, then answer or park. */
export function handleReplicaFetchRequest(ctx: EngineContext, task: ReplicaFetchRequestTask): void {
  if (runningBroker(ctx, task.leader, task.leaderIncarnation) === undefined) {
    disconnect(ctx, task); // the leader process is gone: the connection fails
    return;
  }
  // A request from an earlier life of the follower doesn't count (Kafka fences it by broker
  // epoch in updateFetchStateOrThrow); its answer is dropped on arrival.
  if (runningBroker(ctx, task.follower, task.followerIncarnation) !== undefined) {
    const now = ctx.kernel.now();
    for (const p of task.partitions) {
      const topic = ctx.state.topics.get(p.topic);
      const partition = topic?.partitions[p.partition];
      if (topic === undefined || partition === undefined || topic.topicId !== p.topicId) continue;
      if (partition.leader !== task.leader || partition.leaderEpoch !== p.currentLeaderEpoch) {
        continue;
      }
      updateFollowerFetchState(partition, task.follower, p.fetchOffset, now);
      onLeaderStateChanged(ctx, topic, partition);
      maybeExpandIsr(ctx, topic, partition, task.follower);
    }
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
    if (partition.leaderEpoch !== p.currentLeaderEpoch) {
      return { ...p, error: 'FENCED_LEADER_EPOCH' };
    }
    // A parked fetch completing reads the log again but does not update the follower's state
    // (ReplicaManager#readFromLog: updateFetchState = !readFromPurgatory).
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
  if (runningBroker(ctx, task.follower, task.followerIncarnation) === undefined) {
    // Sent by an earlier life of the follower; its current life may need the slot.
    releaseFetcher(ctx, task.follower, task.followerIncarnation, task.leader);
    return;
  }
  ctx.state.replicaFetchers.delete(fetcherKey(task.follower, task.leader));
  for (const result of task.partitions) {
    if (result.error !== null) continue; // the leader moved: the controller's metadata decides (T5)
    const topic = ctx.state.topics.get(result.topic);
    const partition = topic?.partitions[result.partition];
    if (topic === undefined || partition === undefined || topic.topicId !== result.topicId) {
      continue;
    }
    if (partition.leader === task.follower) continue; // it became the leader meanwhile
    const own = partition.replicaLogs.get(task.follower);
    if (own === undefined || own.logEndOffset !== result.fetchOffset) continue; // stale
    const from = own.logEndOffset;
    for (const record of result.records) {
      if (record.offset === own.logEndOffset) appendToReplica(own, record);
    }
    // Log#updateHighWatermark: the leader's HW, kept within the follower's own log.
    own.highWatermark = Math.max(
      own.logStartOffset,
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

/** The follower learns its fetch failed (connection to a dead leader) one network delay later. */
function disconnect(ctx: EngineContext, task: ReplicaFetchRequestTask): void {
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'ReplicaFetchDisconnected',
    follower: task.follower,
    followerIncarnation: task.followerIncarnation,
    leader: task.leader,
  });
}

/** A failed fetch frees the slot; the follower tries again after replica.fetch.backoff.ms. */
export function handleReplicaFetchDisconnected(
  ctx: EngineContext,
  task: Task<'ReplicaFetchDisconnected'>,
): void {
  const key = fetcherKey(task.follower, task.leader);
  const owner = ctx.state.replicaFetchers.get(key);
  if (owner !== undefined && owner <= task.followerIncarnation)
    ctx.state.replicaFetchers.delete(key);
  if (runningBroker(ctx, task.follower, task.followerIncarnation) === undefined) {
    ensureReplicaFetcher(ctx, task.follower, task.leader); // a newer life, if any, carries on
    return;
  }
  ctx.kernel.scheduleIn(REPLICA_FETCH_BACKOFF_MS, { ...task, type: 'ReplicaFetchBackoff' });
}

export function handleReplicaFetchBackoff(
  ctx: EngineContext,
  task: Task<'ReplicaFetchBackoff'>,
): void {
  if (runningBroker(ctx, task.follower, task.followerIncarnation) === undefined) return;
  ensureReplicaFetcher(ctx, task.follower, task.leader);
}

/**
 * A broker process stopped (crash or shutdown): what it held in memory is gone. Parked follower
 * fetches at it fail on the followers' side, its produce purgatory vanishes (producers time
 * out), and its own fetchers stop.
 */
export function onBrokerStopped(ctx: EngineContext, brokerId: string): void {
  const { state } = ctx;
  for (const [fetchId, parked] of state.parkedReplicaFetches) {
    if (parked.leader !== brokerId) continue;
    state.parkedReplicaFetches.delete(fetchId);
    disconnect(ctx, parked);
  }
  for (const [id, parked] of state.parkedProduces) {
    if (parked.broker === brokerId) state.parkedProduces.delete(id);
  }
  for (const key of [...state.replicaFetchers.keys()]) {
    if (key.startsWith(`${brokerId}>`)) state.replicaFetchers.delete(key);
  }
}

/** A topic was deleted: parked follower fetches and acks=all produces for it complete now. */
export function onTopicRemovedReplication(ctx: EngineContext, topic: string): void {
  const { state } = ctx;
  for (const [fetchId, parked] of state.parkedReplicaFetches) {
    if (!parked.partitions.some((p) => p.topic === topic)) continue;
    state.parkedReplicaFetches.delete(fetchId);
    respondToReplicaFetch(ctx, parked);
  }
  for (const [id, parked] of state.parkedProduces) {
    if (parked.topic !== topic) continue;
    state.parkedProduces.delete(id);
    // DelayedProduce#tryComplete: getPartitionOrError fails.
    sendProduceResponse(ctx, parked, { ok: false, error: 'UNKNOWN_TOPIC_OR_PARTITION' });
  }
}

// ── acks=all: the DelayedProduce purgatory ─────────────────────────────────────────────────────

/** Where a produce response goes: the producer life and the record it answers. */
export type ProduceTarget = Pick<
  ParkedProduce,
  'producerId' | 'incarnation' | 'topic' | 'partition' | 'traceId'
>;

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

/**
 * Parks an acks=all produce at leader `broker` until the HW passes `offset` or `timeoutMs`
 * passes (K6).
 */
export function parkProduce(
  ctx: EngineContext,
  target: ProduceTarget,
  where: { readonly broker: string; readonly topicId: number },
  offset: number,
  timeoutMs: number,
): void {
  const produceId = ++ctx.state.counters.produce;
  ctx.state.parkedProduces.set(produceId, {
    ...target,
    ...where,
    brokerIncarnation: ctx.state.brokers.get(where.broker)?.incarnation ?? -1,
    produceId,
    offset,
  });
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
    if (parked.topicId !== topic.topicId) continue;
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

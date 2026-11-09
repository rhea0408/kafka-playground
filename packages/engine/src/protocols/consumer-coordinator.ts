/**
 * The consumer's side of the classic group protocol (ADR 0004), ported from the Kafka 4.x client:
 * AbstractCoordinator (joinGroupIfNeeded, the JoinGroup / SyncGroup / Heartbeat response handlers,
 * HeartbeatThread, maybeLeaveGroup) and ConsumerCoordinator (poll, onJoinPrepare, onJoinComplete,
 * onLeaderElected, rejoinNeededOrPending, sendOffsetCommitRequest). Facts: M3-decisions K2, K9–K14.
 *
 * - Joining happens inside poll(): the application thread sends JoinGroup only when it calls
 *   poll(), so a member busy processing delays the whole group's rebalance ("stop the world").
 *   poll() does not wait for the join to finish (K12): it keeps fetching partitions the member
 *   still owns, which with an eager assignor is none.
 * - onJoinPrepare (K10): commits first when auto-commit is on and waits for the answer; then an
 *   eager member revokes everything, a cooperative one only partitions of topics it no longer
 *   subscribes to. A member that was kicked out (generation reset) loses its partitions instead.
 *   Like Kafka's single autoCommitOffsetRequestFuture, that commit is sent once per rebalance: once
 *   it is answered the next poll() sends JoinGroup without committing again (M4-decisions D1), and
 *   if it is not answered within rebalance.timeout.ms the member joins anyway (joinPrepareTimer).
 * - The JoinGroup answer is handled when it arrives (JoinGroupResponseHandler): it sets the member
 *   id and the new generation, then the leader runs the assignor on its own metadata and sends the
 *   assignment in SyncGroup; followers send an empty SyncGroup. A successful SyncGroup answer makes
 *   the member STABLE at once (SyncGroupResponseHandler), so heartbeats and commits use the new
 *   generation; the assignment itself takes effect at the application's next poll()
 *   (onJoinComplete); a cooperative member that had to give partitions up rejoins at once (K14).
 *   (M4-decisions D2.)
 * - A JoinGroup or SyncGroup that fails without a reset (REBALANCE_IN_PROGRESS,
 *   COORDINATOR_NOT_AVAILABLE) leaves the member PREPARING or COMPLETING, with its member id,
 *   generation and partitions: commits fail locally, heartbeats go on only while COMPLETING, and
 *   the next poll() sends JoinGroup again without onJoinPrepare (needsJoinPrepare stays false).
 *   (M4-decisions D4.)
 * - The heartbeat thread sends a Heartbeat every heartbeat.interval.ms while the member is in a
 *   generation (not while its JoinGroup is pending). REBALANCE_IN_PROGRESS asks for a rejoin;
 *   UNKNOWN_MEMBER_ID / ILLEGAL_GENERATION mean the member was removed, so it resets.
 * - The same thread watches max.poll.interval.ms: if poll() is not called in time it sends
 *   LeaveGroup ("consumer poll timeout has expired.") and resets the member id and generation
 *   (K13). The application finds out at its next poll().
 * - Only the leader watches metadata for new partitions; every member rejoins when its own
 *   subscription changes (K11).
 */

import type { Assignor } from '@kp/schema';
import type { EngineContext } from '../model/context.ts';
import type { JoinReason, MemberMetadata } from '../model/group.ts';
import { partitionName } from '../model/partition.ts';
import { createClientGroupState, isAlive, type ConsumerState } from '../model/state.ts';
import { canServe } from './controller.ts';
import type {
  CommitOffset,
  CoordinatorRequest,
  CoordinatorResponse,
  EngineTask,
  GroupError,
} from '../model/tasks.ts';
import {
  ASSIGNORS,
  comparePartitions,
  rebalanceProtocol,
  tpKey,
  type AssignorMember,
  type TopicPartition,
} from './assignors/index.ts';
import { knownPartitionCount } from './metadata.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;
type Response<A extends CoordinatorResponse['api']> = Extract<CoordinatorResponse, { api: A }>;

/** The hooks the poll loop (consume.ts) gives the coordinator. */
export interface PollHooks {
  /** Runs poll() again now if the application is blocked in it. */
  readonly wake: (consumer: ConsumerState) => void;
  /** Forgets a partition's position and buffered records (revoked or lost). */
  readonly forget: (consumer: ConsumerState, key: string) => void;
  /** An error poll() throws to the application, which then dies (a crash). */
  readonly fatal: (consumer: ConsumerState, error: GroupError) => void;
  /** An OffsetCommit was answered (the application may be blocked in commitSync()). */
  readonly commitAnswered: (consumer: ConsumerState, commitId: number) => void;
}

/** A fresh client: not in a group, no member id (a restarted process starts like this). */
export function resetClientGroup(consumer: ConsumerState): void {
  consumer.group = createClientGroupState();
}

export const isRebalancing = (consumer: ConsumerState): boolean =>
  consumer.group.state === 'preparing' || consumer.group.state === 'completing';

/**
 * Whether the client knows a reachable coordinator (AbstractCoordinator#ensureCoordinatorReady).
 * If not, it marks it unknown and sends FindCoordinator to a live broker (M4-T8).
 */
export function coordinatorReady(ctx: EngineContext, consumer: ConsumerState): boolean {
  const { group } = consumer;
  if (group.coordinator !== null) {
    const broker = ctx.state.brokers.get(group.coordinator);
    if (broker !== undefined && isAlive(broker.status)) return true;
    markCoordinatorUnknown(consumer);
  }
  if (group.findInFlight) return false;
  // leastLoadedNode: a broker in the client's metadata (registered and unfenced) it can reach.
  const brokers = [...ctx.state.brokers.values()];
  const target =
    brokers.find((b) => canServe(ctx, b.config.id)) ?? brokers.find((b) => isAlive(b.status));
  if (target === undefined) return false;
  group.findInFlight = true;
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'FindCoordinatorRequest',
    groupId: consumer.config.groupId,
    consumerId: consumer.config.id,
    incarnation: consumer.incarnation,
    broker: target.config.id,
  });
  return false;
}

/** AbstractCoordinator#markCoordinatorUnknown. */
export function markCoordinatorUnknown(consumer: ConsumerState): void {
  consumer.group.coordinator = null;
}

/** The FindCoordinator answer reaches the client. */
export function onFindCoordinatorResponse(
  consumer: ConsumerState,
  coordinator: string | null,
  hooks: PollHooks,
): void {
  consumer.group.findInFlight = false;
  consumer.group.coordinator = coordinator;
  // COORDINATOR_NOT_AVAILABLE: the scheduled retry (retry.backoff.ms) asks again, not a wake-up.
  if (coordinator !== null) hooks.wake(consumer);
}

/**
 * The coordinator's broker went away with requests in flight (a dropped connection): the client
 * marks it unknown; a pending JoinGroup or SyncGroup is retried, pending commits fail as
 * retriable (COORDINATOR_NOT_AVAILABLE).
 */
export function onCoordinatorDisconnected(
  ctx: EngineContext,
  consumer: ConsumerState,
  hooks: PollHooks,
  brokerId: string,
): void {
  const { group } = consumer;
  if (group.coordinator === brokerId) {
    markCoordinatorUnknown(consumer);
    if ((group.state === 'preparing' || group.state === 'completing') && !group.joinRetry) {
      retryJoin(consumer, 'rejoin-after-error');
    }
  }
  for (const [commitId, target] of [...group.commitsInFlight]) {
    if (target !== brokerId) continue;
    onCommitResponse(
      ctx,
      consumer,
      { api: 'OffsetCommit', commitId, error: 'COORDINATOR_NOT_AVAILABLE' },
      hooks,
    );
  }
  hooks.wake(consumer);
}

/** Sends a request to the group's coordinator (one network delay). False if there is none. */
function send(ctx: EngineContext, consumer: ConsumerState, request: CoordinatorRequest): boolean {
  const broker = consumer.group.coordinator;
  if (broker === null) return false;
  if (request.api === 'OffsetCommit') consumer.group.commitsInFlight.set(request.commitId, broker);
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'CoordinatorRequest',
    broker,
    groupId: consumer.config.groupId,
    consumerId: consumer.config.id,
    incarnation: consumer.incarnation,
    request,
  });
  return true;
}

export function requestRejoin(consumer: ConsumerState, reason: JoinReason): void {
  if (consumer.group.rejoinNeeded) return;
  consumer.group.rejoinNeeded = true;
  consumer.group.rejoinReason = reason;
}

/**
 * The member was removed from the group or left it (AbstractCoordinator#resetStateAndGeneration):
 * no generation, and with `memberId` also no member id. Its partitions are lost at the next poll().
 */
function resetGeneration(consumer: ConsumerState, memberId: boolean): void {
  const { group } = consumer;
  group.generation = -1;
  if (memberId) group.memberId = '';
  group.state = 'unjoined';
  group.isLeader = false;
  group.leaderCounts = null;
  group.pendingAssignment = null;
  group.joinRetry = false;
  // resetStateAndRejoin sets needsJoinPrepare, so the rejoin runs onJoinPrepare (partitions lost).
  // A commit answered or in flight before the reset must not let it skip its own commit.
  group.needsJoinPrepare = true;
  group.prepareCommitId = null;
  group.prepareCommitDone = false;
  group.joinPrepareToken += 1;
  group.heartbeatToken += 1;
  requestRejoin(consumer, 'rejoin-after-error');
}

/**
 * A JoinGroup or SyncGroup failed without a reset (JoinGroupResponseHandler /
 * SyncGroupResponseHandler only raise the error; joinGroupIfNeeded requests a rejoin and retries).
 * The state stays PREPARING or COMPLETING: Kafka's initiateJoinGroup failure listener does not
 * change it (4.0, 4.1), so the member is not STABLE and its commits fail locally.
 */
function retryJoin(consumer: ConsumerState, reason: JoinReason): void {
  consumer.group.joinRetry = true;
  requestRejoin(consumer, reason);
}

// ── Inside poll() ─────────────────────────────────────────────────────────────────────────────

/**
 * What poll() may do after the coordinator's housekeeping: fetch owned partitions, wait (nothing
 * subscribed, or an eager member's onJoinPrepare commit is in flight), or retry after
 * retry.backoff.ms because no coordinator is available.
 */
export type CoordinatorPoll = 'fetch' | 'wait' | 'retry';

/** ConsumerCoordinator#poll, run at the start of every poll() with the current subscription. */
export function pollCoordinator(
  ctx: EngineContext,
  consumer: ConsumerState,
  subscription: readonly string[],
  hooks: PollHooks,
): CoordinatorPoll {
  const { group } = consumer;
  if (subscription.length === 0) {
    // unsubscribe(): commit (auto-commit), give partitions up and leave (maybeLeaveGroup).
    if (group.memberId !== '' || group.owned.length > 0) {
      if (consumer.config.enableAutoCommit) commitPositions(ctx, consumer, ownedKeys(consumer));
      leaveGroup(ctx, consumer, 'leave', hooks);
    }
    return 'wait';
  }
  if (!coordinatorReady(ctx, consumer)) return 'retry';

  if (group.pendingAssignment !== null) onJoinComplete(ctx, consumer, hooks);
  if (
    group.joinedSubscription !== null &&
    group.joinedSubscription.join('\u0000') !== subscription.join('\u0000')
  ) {
    requestRejoin(consumer, 'subscription-change');
  }
  if (group.isLeader && group.state === 'stable' && leaderMetadataChanged(consumer)) {
    requestRejoin(consumer, 'metadata-change');
  }
  if (group.prepareCommitId !== null) {
    // onJoinPrepare is waiting for its commit; an eager member's partitions are pending
    // revocation, so it returns nothing meanwhile (maybeMarkPartitionsPendingRevocation).
    return rebalanceProtocol(consumer.config.partitionAssignmentStrategy) === 'cooperative'
      ? 'fetch'
      : 'wait';
  }
  const needsJoin =
    group.state === 'unjoined' ||
    group.joinRetry ||
    (group.rejoinNeeded && group.state === 'stable');
  const waitingForCommit = needsJoin && !joinGroup(ctx, consumer, subscription, hooks);
  return waitingForCommit &&
    rebalanceProtocol(consumer.config.partitionAssignmentStrategy) === 'eager'
    ? 'wait'
    : 'fetch';
}

/** Leader only (K11): did a refresh change a partition count the last assignment used? */
function leaderMetadataChanged(consumer: ConsumerState): boolean {
  const snapshot = consumer.group.leaderCounts;
  if (snapshot === null) return false;
  for (const [topic, count] of snapshot) {
    if (consumer.metadata.partitionCounts.get(topic) !== count) return true;
  }
  return false;
}

/**
 * joinGroupIfNeeded: onJoinPrepare, then JoinGroup. Returns false when JoinGroup has to wait for
 * onJoinPrepare's auto-commit to be answered (it goes out at the poll() after that).
 *
 * ConsumerCoordinator#onJoinPrepare sends that commit only while autoCommitOffsetRequestFuture is
 * null and keeps the future until it is done, so a rebalance commits once. A cooperative member
 * keeps fetching meanwhile; committing its newer positions again at every poll() would never let
 * the JoinGroup out (the M3 livelock, M4-decisions D1).
 */
function joinGroup(
  ctx: EngineContext,
  consumer: ConsumerState,
  subscription: readonly string[],
  hooks: PollHooks,
): boolean {
  const { group, config } = consumer;
  if (group.needsJoinPrepare) {
    const lost = group.generation < 0 || group.memberId === '';
    if (!lost && config.enableAutoCommit && !group.prepareCommitDone) {
      const commitId = commitPositions(ctx, consumer, ownedKeys(consumer));
      if (commitId !== null) {
        group.prepareCommitId = commitId;
        startJoinPrepareTimer(ctx, consumer);
        return false;
      }
    }
    group.prepareCommitDone = false;
    group.joinPrepareToken += 1;
    onJoinPrepare(ctx, consumer, subscription, lost, hooks);
    group.needsJoinPrepare = false;
  }
  const reason: JoinReason = group.memberId === '' ? 'new-member' : group.rejoinReason;
  const metadata: MemberMetadata = {
    protocols: config.partitionAssignmentStrategy,
    subscription,
    ownedPartitions: group.owned,
    stickyUserData: group.lastOwned,
    ownedGeneration: group.lastOwnedGeneration,
    sessionTimeoutMs: config.sessionTimeoutMs,
    rebalanceTimeoutMs: config.maxPollIntervalMs,
  };
  group.state = 'preparing';
  group.rejoinNeeded = false;
  group.joinRetry = false;
  group.joinedSubscription = [...subscription];
  group.heartbeatToken += 1; // the heartbeat thread is disabled while the JoinGroup is pending
  send(ctx, consumer, { api: 'JoinGroup', memberId: group.memberId, metadata, reason });
  return true;
}

/** ConsumerCoordinator#onJoinPrepare (K10). */
function onJoinPrepare(
  ctx: EngineContext,
  consumer: ConsumerState,
  subscription: readonly string[],
  lost: boolean,
  hooks: PollHooks,
): void {
  const { group, config } = consumer;
  const emitFor = (type: 'PartitionsRevoked' | 'PartitionsLost', partitions: TopicPartition[]) => {
    if (partitions.length === 0) return;
    ctx.emit({ type, consumerId: config.id, groupId: config.groupId, partitions });
    for (const tp of partitions) hooks.forget(consumer, tpKey(tp));
  };
  if (lost) {
    emitFor('PartitionsLost', group.owned);
    group.owned = [];
    group.lastOwned = [];
    group.lastOwnedGeneration = -1;
    return;
  }
  // StickyAssignor (eager) sends its previous assignment as user data; cooperative members send
  // the partitions they keep. Range and round-robin ignore it.
  group.lastOwned = group.owned;
  group.lastOwnedGeneration = group.generation;
  if (rebalanceProtocol(config.partitionAssignmentStrategy) === 'eager') {
    emitFor('PartitionsRevoked', group.owned);
    group.owned = [];
    return;
  }
  const subscribed = new Set(subscription);
  const gone = group.owned.filter((tp) => !subscribed.has(tp.topic));
  emitFor('PartitionsRevoked', gone);
  group.owned = group.owned.filter((tp) => subscribed.has(tp.topic));
  group.lastOwned = group.owned;
}

/** ConsumerCoordinator#onJoinComplete: the SyncGroup answer takes effect (inside poll()). */
function onJoinComplete(ctx: EngineContext, consumer: ConsumerState, hooks: PollHooks): void {
  const { group, config } = consumer;
  const result = group.pendingAssignment;
  if (result === null) return;
  group.pendingAssignment = null;
  group.generation = result.generation;
  group.state = 'stable';
  group.needsJoinPrepare = true; // joinGroupIfNeeded, after onJoinComplete
  const assigned = new Set(result.partitions.map(tpKey));
  const before = new Set(group.owned.map(tpKey));
  const revoked = group.owned.filter((tp) => !assigned.has(tpKey(tp)));
  const added = result.partitions.filter((tp) => !before.has(tpKey(tp)));
  if (revoked.length > 0) {
    ctx.emit({
      type: 'PartitionsRevoked',
      consumerId: config.id,
      groupId: config.groupId,
      partitions: revoked,
    });
    for (const tp of revoked) hooks.forget(consumer, tpKey(tp));
    // Cooperative: the revoked partitions can only be reassigned in a follow-up rebalance.
    requestRejoin(consumer, 'revoked-partitions');
  }
  group.owned = [...result.partitions];
  ctx.emit({
    type: 'PartitionsAssigned',
    consumerId: config.id,
    groupId: config.groupId,
    generation: result.generation,
    partitions: added,
  });
  if (group.owned.length === 0) {
    ctx.emit({
      type: 'ConsumerIdle',
      consumerId: config.id,
      groupId: config.groupId,
      generation: result.generation,
    });
  }
}

const ownedKeys = (consumer: ConsumerState): string[] => consumer.group.owned.map(tpKey);

// ── Responses (the client's network thread) ───────────────────────────────────────────────────

export function handleCoordinatorResponse(
  ctx: EngineContext,
  consumer: ConsumerState,
  task: Task<'CoordinatorResponse'>,
  hooks: PollHooks,
): void {
  // A response for an earlier group (group.id changed since) is ignored.
  if (task.groupId !== consumer.config.groupId) return;
  const { response } = task;
  switch (response.api) {
    case 'JoinGroup':
      onJoinResponse(ctx, consumer, response, hooks);
      return;
    case 'SyncGroup':
      onSyncResponse(consumer, response, hooks);
      return;
    case 'Heartbeat':
      onHeartbeatResponse(consumer, response.error, hooks);
      return;
    case 'OffsetCommit':
      onCommitResponse(ctx, consumer, response, hooks);
      return;
  }
}

function onJoinResponse(
  ctx: EngineContext,
  consumer: ConsumerState,
  response: Response<'JoinGroup'>,
  hooks: PollHooks,
): void {
  const { group } = consumer;
  if (group.state !== 'preparing' || group.joinRetry) return;
  if (response.error !== null) {
    switch (response.error) {
      case 'INCONSISTENT_GROUP_PROTOCOL':
        // Thrown to the application from poll(); the playground's application then dies.
        hooks.fatal(consumer, response.error);
        return;
      case 'UNKNOWN_MEMBER_ID':
        resetGeneration(consumer, true);
        break;
      case 'ILLEGAL_GENERATION':
        resetGeneration(consumer, false);
        break;
      case 'NOT_COORDINATOR':
      case 'COORDINATOR_NOT_AVAILABLE':
        // markCoordinatorUnknown, then FindCoordinator before the retry (M3 carry-over).
        markCoordinatorUnknown(consumer);
        retryJoin(consumer, 'rejoin-after-error');
        break;
      case 'REBALANCE_IN_PROGRESS':
        retryJoin(consumer, 'rejoin-after-error');
        break;
    }
    hooks.wake(consumer);
    return;
  }
  // JoinGroupResponseHandler: the new generation (and member id) take effect now, so heartbeats
  // sent while SyncGroup is pending carry it (M4-decisions D2).
  group.memberId = response.memberId;
  group.generation = response.generation;
  group.state = 'completing';
  group.protocol = response.protocol as Assignor;
  group.isLeader = response.leader === response.memberId;
  startHeartbeats(ctx, consumer);
  let assignment: (readonly [string, TopicPartition[]])[] | null = null;
  if (group.isLeader) {
    // onLeaderElected: the leader assigns with its own metadata for every subscribed topic.
    const counts = new Map<string, number>();
    for (const topic of new Set(response.members.flatMap((m) => m.topics))) {
      const live = ctx.state.topics.get(topic);
      if (live !== undefined) counts.set(topic, knownPartitionCount(consumer.metadata, live));
    }
    group.leaderCounts = counts;
    const assignor = ASSIGNORS[group.protocol];
    assignment = [...assignor(response.members as AssignorMember[], counts)].map(
      ([memberId, partitions]) => [memberId, [...partitions]] as const,
    );
  } else {
    group.leaderCounts = null;
  }
  send(ctx, consumer, {
    api: 'SyncGroup',
    memberId: response.memberId,
    generation: response.generation,
    assignment,
  });
}

function onSyncResponse(
  consumer: ConsumerState,
  response: Response<'SyncGroup'>,
  hooks: PollHooks,
): void {
  const { group } = consumer;
  if (group.state !== 'completing' || group.joinRetry) return;
  if (response.error !== null) {
    if (response.error === 'UNKNOWN_MEMBER_ID') resetGeneration(consumer, true);
    else if (response.error === 'ILLEGAL_GENERATION') resetGeneration(consumer, false);
    else {
      if (response.error === 'NOT_COORDINATOR' || response.error === 'COORDINATOR_NOT_AVAILABLE') {
        markCoordinatorUnknown(consumer);
      }
      retryJoin(consumer, 'rebalance-in-progress');
    }
    hooks.wake(consumer);
    return;
  }
  // SyncGroupResponseHandler sets the state to STABLE on the network thread: commits and heartbeats
  // from here on use the new generation, before poll() applies the assignment (onJoinComplete).
  group.state = 'stable';
  group.pendingAssignment = {
    generation: response.generation,
    partitions: [...response.assignment].sort(comparePartitions),
  };
  hooks.wake(consumer);
}

function onHeartbeatResponse(
  consumer: ConsumerState,
  error: GroupError | null,
  hooks: PollHooks,
): void {
  const { group } = consumer;
  switch (error) {
    case null:
      return;
    case 'REBALANCE_IN_PROGRESS':
      // Only a member in a stable generation needs to act; one mid-join is already rejoining.
      if (group.state === 'stable') requestRejoin(consumer, 'rebalance-in-progress');
      break;
    case 'UNKNOWN_MEMBER_ID':
      resetGeneration(consumer, true);
      break;
    case 'ILLEGAL_GENERATION':
      resetGeneration(consumer, false);
      break;
    case 'INCONSISTENT_GROUP_PROTOCOL':
      return;
    case 'NOT_COORDINATOR':
    case 'COORDINATOR_NOT_AVAILABLE':
      markCoordinatorUnknown(consumer);
      break;
  }
  hooks.wake(consumer);
}

// ── Heartbeat thread ──────────────────────────────────────────────────────────────────────────

/** Enables the heartbeat thread: the first Heartbeat goes out heartbeat.interval.ms from now. */
function startHeartbeats(ctx: EngineContext, consumer: ConsumerState): void {
  consumer.group.heartbeatToken += 1;
  ctx.kernel.scheduleIn(consumer.config.heartbeatIntervalMs, {
    type: 'ConsumerHeartbeat',
    consumerId: consumer.config.id,
    incarnation: consumer.incarnation,
    token: consumer.group.heartbeatToken,
  });
}

export function handleConsumerHeartbeat(
  ctx: EngineContext,
  consumer: ConsumerState,
  task: Task<'ConsumerHeartbeat'>,
): void {
  const { group } = consumer;
  if (task.token !== group.heartbeatToken) return;
  if (group.state !== 'stable' && group.state !== 'completing') return; // thread disabled
  // The heartbeat thread looks the coordinator up itself when it is unknown.
  if (coordinatorReady(ctx, consumer)) {
    send(ctx, consumer, {
      api: 'Heartbeat',
      memberId: group.memberId,
      generation: group.generation,
    });
  }
  ctx.kernel.scheduleIn(consumer.config.heartbeatIntervalMs, { ...task });
}

/** The application left poll(): the poll timer (max.poll.interval.ms) starts. */
export function startPollTimer(ctx: EngineContext, consumer: ConsumerState): void {
  consumer.group.pollTimeoutToken += 1;
  ctx.kernel.scheduleIn(consumer.config.maxPollIntervalMs, {
    type: 'ConsumerPollTimeout',
    consumerId: consumer.config.id,
    incarnation: consumer.incarnation,
    token: consumer.group.pollTimeoutToken,
  });
}

/** The application called poll(): the poll timer stops until poll() returns again. */
export function stopPollTimer(consumer: ConsumerState): void {
  consumer.group.pollTimeoutToken += 1;
}

/** AbstractCoordinator.HeartbeatThread: "consumer poll timeout has expired" (K13). */
export function handleConsumerPollTimeout(
  ctx: EngineContext,
  consumer: ConsumerState,
  task: Task<'ConsumerPollTimeout'>,
): void {
  const { group } = consumer;
  if (task.token !== group.pollTimeoutToken || group.memberId === '') return;
  send(ctx, consumer, { api: 'LeaveGroup', memberId: group.memberId, reason: 'poll-timeout' });
  resetGeneration(consumer, true);
}

/** joinPrepareTimer: onJoinPrepare waits at most rebalance.timeout.ms for its commit. */
function startJoinPrepareTimer(ctx: EngineContext, consumer: ConsumerState): void {
  consumer.group.joinPrepareToken += 1;
  ctx.kernel.scheduleIn(consumer.config.maxPollIntervalMs, {
    type: 'ConsumerJoinPrepareTimeout',
    consumerId: consumer.config.id,
    incarnation: consumer.incarnation,
    token: consumer.group.joinPrepareToken,
  });
}

/**
 * ConsumerCoordinator#onJoinPrepare: "Asynchronous auto-commit of offsets failed: joinPrepare
 * timeout. Will continue to join group". The late answer, if it comes, is ignored.
 */
export function handleConsumerJoinPrepareTimeout(
  consumer: ConsumerState,
  task: Task<'ConsumerJoinPrepareTimeout'>,
  hooks: PollHooks,
): void {
  const { group } = consumer;
  if (task.token !== group.joinPrepareToken || group.prepareCommitId === null) return;
  group.prepareCommitId = null;
  group.prepareCommitDone = true;
  hooks.wake(consumer);
}

// ── Leaving ───────────────────────────────────────────────────────────────────────────────────

/**
 * close() / unsubscribe(): the partitions are given up (after a synchronous auto-commit, which the
 * caller does first) and LeaveGroup is sent if the member has an id. The client does not wait for
 * the LeaveGroup answer.
 *
 * ConsumerCoordinator#onLeavePrepare (4.0, trunk) calls onPartitionsLost instead of
 * onPartitionsRevoked when the generation is reset or the member id unknown (a zombie, review F-9)
 * or a rebalance is in progress (rebalanceInProgress(): PREPARING or COMPLETING, our
 * isRebalancing): in each case the partitions may already belong to someone else.
 */
export function leaveGroup(
  ctx: EngineContext,
  consumer: ConsumerState,
  reason: 'leave',
  hooks: PollHooks,
): void {
  const { group, config } = consumer;
  if (group.owned.length > 0) {
    const lost = group.generation < 0 || group.memberId === '' || isRebalancing(consumer);
    ctx.emit({
      type: lost ? 'PartitionsLost' : 'PartitionsRevoked',
      consumerId: config.id,
      groupId: config.groupId,
      partitions: group.owned,
    });
    for (const tp of group.owned) hooks.forget(consumer, tpKey(tp));
  }
  if (group.memberId !== '')
    send(ctx, consumer, { api: 'LeaveGroup', memberId: group.memberId, reason });
  resetClientGroup(consumer);
}

// ── Commits ───────────────────────────────────────────────────────────────────────────────────

/**
 * ConsumerCoordinator#sendOffsetCommitRequest for the given partitions' positions, all of them:
 * auto-commit sends subscriptions.allConsumed() every interval even when nothing moved
 * (maybeAutoCommitOffsetsAsync), and the client never reads the coordinator's committed offsets
 * to skip any (review F-8). Nothing to send (no position) is a no-op success, as an empty offsets
 * map is in Kafka (RequestFuture.voidSuccess()). A member outside a stable generation fails at once
 * without contacting the broker: REBALANCE_IN_PROGRESS while it rejoins, otherwise
 * CommitFailedException ("…not part of an active group…", K13). Returns the request id, or null
 * when nothing was sent.
 */
export function commitPositions(
  ctx: EngineContext,
  consumer: ConsumerState,
  keys: readonly string[],
): number | null {
  const { group } = consumer;
  const offsets: CommitOffset[] = [];
  for (const key of keys) {
    const position = consumer.positions.get(key);
    if (position === undefined) continue;
    const dash = key.lastIndexOf('-');
    offsets.push({
      topic: key.slice(0, dash),
      partition: Number(key.slice(dash + 1)),
      offset: position.offset,
      topicId: position.topicId,
    });
  }
  if (offsets.length === 0) return null;
  if (group.state !== 'stable' || group.generation < 0 || group.memberId === '') {
    emitCommitFailed(
      ctx,
      consumer,
      isRebalancing(consumer) ? 'REBALANCE_IN_PROGRESS' : 'COMMIT_FAILED',
      offsets,
    );
    return null;
  }
  const commitId = ++ctx.state.counters.commit;
  const sent = send(ctx, consumer, {
    api: 'OffsetCommit',
    commitId,
    memberId: group.memberId,
    generation: group.generation,
    offsets,
  });
  if (!sent) {
    // No known coordinator: the commit fails as retriable (COORDINATOR_NOT_AVAILABLE).
    emitCommitFailed(ctx, consumer, 'COORDINATOR_NOT_AVAILABLE', offsets);
    coordinatorReady(ctx, consumer);
    return null;
  }
  return commitId;
}

function emitCommitFailed(
  ctx: EngineContext,
  consumer: ConsumerState,
  error: GroupError | 'COMMIT_FAILED',
  offsets: readonly CommitOffset[],
): void {
  ctx.emit({
    type: 'CommitFailed',
    consumerId: consumer.config.id,
    groupId: consumer.config.groupId,
    memberId: consumer.group.memberId,
    generation: consumer.group.generation,
    error,
    partitions: offsets.map((o) => ({ topic: o.topic, partition: o.partition })),
  });
}

function onCommitResponse(
  ctx: EngineContext,
  consumer: ConsumerState,
  response: Response<'OffsetCommit'>,
  hooks: PollHooks,
): void {
  const { group } = consumer;
  group.commitsInFlight.delete(response.commitId);
  if (response.error !== null) {
    emitCommitFailed(ctx, consumer, response.error, []);
    if (response.error === 'NOT_COORDINATOR' || response.error === 'COORDINATOR_NOT_AVAILABLE') {
      markCoordinatorUnknown(consumer);
    }
    if (response.error === 'UNKNOWN_MEMBER_ID') resetGeneration(consumer, true);
    else if (response.error === 'ILLEGAL_GENERATION') resetGeneration(consumer, false);
    else if (response.error === 'REBALANCE_IN_PROGRESS')
      requestRejoin(consumer, 'rebalance-in-progress');
  }
  if (response.commitId === group.prepareCommitId) {
    // onJoinPrepare's commit is done (or failed): the JoinGroup goes out at the next poll(),
    // without another commit.
    group.prepareCommitId = null;
    group.prepareCommitDone = true;
    hooks.wake(consumer);
  }
  hooks.commitAnswered(consumer, response.commitId);
}

/** Partition keys a consumer owns, for snapshots and commits. */
export const ownedPartitionKeys = (consumer: ConsumerState): string[] =>
  consumer.group.owned.map((tp) => partitionName(tp.topic, tp.partition));

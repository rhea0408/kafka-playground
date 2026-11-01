/**
 * The group coordinator, broker side, for the classic group protocol (ADR 0004). Ported from the
 * Kafka 4.x group-coordinator module: GroupMetadataManager (classicGroupJoin, classicGroupSync,
 * classicGroupHeartbeat, classicGroupLeave, prepareRebalance, completeClassicGroupJoin,
 * tryCompleteInitialRebalanceElseSchedule, schedulePendingSync, expireClassicGroupMemberHeartbeat)
 * and ClassicGroup (selectProtocol, validateOffsetCommit). The facts are listed with sources in
 * docs/milestones/M3-decisions.md (K1–K9).
 *
 * States: Empty → PreparingRebalance → CompletingRebalance → Stable, and back to
 * PreparingRebalance on any membership change.
 * - PreparingRebalance (the join phase): every member must (re)send JoinGroup. A rebalance that
 *   starts from Empty waits group.initial.rebalance.delay.ms, then another window as long as new
 *   members keep arriving, capped by the rebalance timeout; it never ends early (K3). Otherwise the
 *   phase ends as soon as every known member has rejoined, or after the rebalance timeout (the
 *   largest max.poll.interval.ms), removing members that did not rejoin.
 * - When the join phase ends the generation increments, the assignor is chosen by vote, and every
 *   member gets a JoinGroup response; the leader's carries all members' subscriptions (K6, K7).
 *   With no members left the group becomes Empty instead (K5).
 * - CompletingRebalance: members send SyncGroup; the leader's carries the assignment it computed,
 *   which makes the group Stable and answers everyone. Members that do not sync within the
 *   rebalance timeout are removed (K8).
 * - Heartbeats: REBALANCE_IN_PROGRESS while PreparingRebalance, which is how members learn they
 *   must rejoin; a member silent for session.timeout.ms is removed, unless it is waiting in
 *   JoinGroup or SyncGroup (K4).
 * - OffsetCommit: UNKNOWN_MEMBER_ID, then ILLEGAL_GENERATION, then REBALANCE_IN_PROGRESS only in
 *   CompletingRebalance; accepted while PreparingRebalance. Partition ownership is not checked (K1).
 *
 * Requests arrive one network delay after the client sent them; responses take one more. Every
 * request is answered from the state at arrival.
 */

import type { Assignor } from '@kp/schema';
import type { EngineContext } from '../model/context.ts';
import {
  createGroup,
  rebalanceTimeoutMs,
  type GroupMember,
  type GroupState,
  type JoinReason,
  type MemberMetadata,
  type MemberRemovedReason,
  type RebalanceReason,
} from '../model/group.ts';
import { partitionName } from '../model/partition.ts';
import type {
  CoordinatorRequest,
  CoordinatorResponse,
  EngineTask,
  GroupError,
} from '../model/tasks.ts';
import { comparePartitions, type AssignorMember } from './assignors/index.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;
type Request<A extends CoordinatorRequest['api']> = Extract<CoordinatorRequest, { api: A }>;

interface Sender {
  readonly groupId: string;
  readonly consumerId: string;
  readonly incarnation: number;
}

/** The coordinator's state for `groupId`, created on first use (like the first JoinGroup). */
export function coordinatorGroup(ctx: EngineContext, groupId: string): GroupState {
  let group = ctx.state.groups.get(groupId);
  if (group === undefined) {
    group = createGroup(groupId);
    ctx.state.groups.set(groupId, group);
  }
  return group;
}

export function handleCoordinatorRequest(
  ctx: EngineContext,
  task: Task<'CoordinatorRequest'>,
): void {
  const { request } = task;
  switch (request.api) {
    case 'JoinGroup':
      joinGroup(ctx, task, request);
      return;
    case 'SyncGroup':
      syncGroup(ctx, task, request);
      return;
    case 'Heartbeat':
      heartbeat(ctx, task, request);
      return;
    case 'LeaveGroup':
      leaveGroup(ctx, task.groupId, request);
      return;
    case 'OffsetCommit':
      respond(ctx, task, {
        api: 'OffsetCommit',
        commitId: request.commitId,
        error: offsetCommit(ctx, task, request),
      });
      return;
  }
}

function respond(ctx: EngineContext, to: Sender, response: CoordinatorResponse): void {
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'CoordinatorResponse',
    groupId: to.groupId,
    consumerId: to.consumerId,
    incarnation: to.incarnation,
    response,
  });
}

const respondTo =
  (ctx: EngineContext, group: GroupState, member: GroupMember) =>
  (response: CoordinatorResponse): void => {
    respond(
      ctx,
      { groupId: group.groupId, consumerId: member.consumerId, incarnation: member.incarnation },
      response,
    );
  };

// ── JoinGroup ─────────────────────────────────────────────────────────────────────────────────

function joinGroup(ctx: EngineContext, from: Sender, request: Request<'JoinGroup'>): void {
  const group = coordinatorGroup(ctx, from.groupId);
  const fail = (error: GroupError): void => {
    respond(ctx, from, { api: 'JoinGroup', error });
  };
  // A member whose assignors share nothing with the group's cannot join
  // (INCONSISTENT_GROUP_PROTOCOL; ClassicGroup#supportsProtocols).
  if (!supportsProtocols(group, request.metadata.protocols, request.memberId)) {
    fail('INCONSISTENT_GROUP_PROTOCOL');
    return;
  }
  if (request.memberId === '') {
    joinNewMember(ctx, group, from, request.metadata);
    return;
  }
  const member = group.members.get(request.memberId);
  if (member === undefined || group.state === 'Empty' || group.state === 'Dead') {
    fail('UNKNOWN_MEMBER_ID');
    return;
  }
  joinExistingMember(ctx, group, member, request.metadata, request.reason);
}

function supportsProtocols(
  group: GroupState,
  protocols: readonly Assignor[],
  memberId: string,
): boolean {
  const others = [...group.members.values()].filter((m) => m.memberId !== memberId);
  if (others.length === 0) return protocols.length > 0;
  return protocols.some((p) => others.every((m) => m.metadata.protocols.includes(p)));
}

/**
 * A member joining for the first time gets a member id: Kafka uses `<client.id>-<UUID>`; ours is
 * `<consumer id>-<8 hex digits from the seeded RNG>`, so member ids sort by consumer id first.
 * (JoinGroup v4+ first answers MEMBER_ID_REQUIRED with the id and the client rejoins with it; that
 * extra round trip is not modelled, K18.)
 */
function joinNewMember(
  ctx: EngineContext,
  group: GroupState,
  from: Sender,
  metadata: MemberMetadata,
): void {
  const suffix = ctx.kernel.rng.nextUint32().toString(16).padStart(8, '0');
  const member: GroupMember = {
    memberId: `${from.consumerId}-${suffix}`,
    consumerId: from.consumerId,
    incarnation: from.incarnation,
    metadata,
    awaitingJoin: true,
    awaitingSync: false,
    heartbeatToken: 0,
    assignment: [],
  };
  group.members.set(member.memberId, member);
  group.leader ??= member.memberId;
  ctx.emit({
    type: 'MemberJoined',
    groupId: group.groupId,
    memberId: member.memberId,
    consumerId: member.consumerId,
  });
  scheduleSessionExpiry(ctx, group, member);
  if (group.state === 'PreparingRebalance') {
    if (group.previousState === 'Empty') group.newMemberAdded = true;
    maybeCompleteJoin(ctx, group);
  } else {
    prepareRebalance(ctx, group, 'member-joined');
  }
}

function sameMetadata(a: MemberMetadata, b: MemberMetadata): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/** GroupMetadataManager#classicGroupJoinExistingMember (K9). */
function joinExistingMember(
  ctx: EngineContext,
  group: GroupState,
  member: GroupMember,
  metadata: MemberMetadata,
  reason: JoinReason,
): void {
  const changed = !sameMetadata(member.metadata, metadata);
  member.metadata = metadata;
  member.awaitingJoin = true;
  scheduleSessionExpiry(ctx, group, member);
  switch (group.state) {
    case 'PreparingRebalance':
      maybeCompleteJoin(ctx, group);
      return;
    case 'CompletingRebalance':
    case 'Stable':
      // The leader always forces a rebalance (it may have seen a metadata change that its
      // subscription does not show); a follower only if what it sent changed.
      if (changed || group.leader === member.memberId) {
        prepareRebalance(ctx, group, rebalanceReasonFor(reason, changed));
      } else {
        // Same metadata: the member gets the current generation back and syncs again.
        member.awaitingJoin = false;
        sendJoinResponse(ctx, group, member);
      }
      return;
    case 'Empty':
    case 'Dead':
      return; // handled in joinGroup
  }
}

function rebalanceReasonFor(reason: JoinReason, changed: boolean): RebalanceReason {
  switch (reason) {
    case 'subscription-change':
      return 'subscription-change';
    case 'revoked-partitions':
      return 'revoked-partitions';
    case 'metadata-change':
      return 'metadata-change';
    case 'new-member':
    case 'rebalance-in-progress':
    case 'rejoin-after-error':
      return changed ? 'subscription-change' : 'metadata-change';
  }
}

// ── The join phase ────────────────────────────────────────────────────────────────────────────

/** GroupMetadataManager#prepareRebalance. */
function prepareRebalance(ctx: EngineContext, group: GroupState, reason: RebalanceReason): void {
  if (group.state === 'CompletingRebalance') {
    // resetAndPropagateAssignmentWithError: parked SyncGroups are answered with an error.
    for (const m of group.members.values()) {
      if (!m.awaitingSync) continue;
      m.awaitingSync = false;
      respondTo(ctx, group, m)({ api: 'SyncGroup', error: 'REBALANCE_IN_PROGRESS' });
    }
  }
  group.pendingSync.clear();
  const initial = group.state === 'Empty';
  group.previousState = group.state;
  group.state = 'PreparingRebalance';
  group.newMemberAdded = false;
  const token = ++group.timerToken;
  ctx.emit({
    type: 'RebalanceStarted',
    groupId: group.groupId,
    generation: group.generation,
    reason,
  });
  const timeout = rebalanceTimeoutMs(group);
  if (initial) {
    const delayMs = initialRebalanceDelayMs(ctx);
    ctx.kernel.scheduleIn(delayMs, {
      type: 'GroupJoinTimer',
      groupId: group.groupId,
      token,
      initial: { delayMs, remainingMs: Math.max(timeout - delayMs, 0) },
    });
    return;
  }
  ctx.kernel.scheduleIn(timeout, {
    type: 'GroupJoinTimer',
    groupId: group.groupId,
    token,
    initial: null,
  });
  maybeCompleteJoin(ctx, group);
}

/**
 * The coordinator broker's group.initial.rebalance.delay.ms. With one broker it is that broker's;
 * the first broker's value is used if the coordinator cannot be told apart (M3 has one broker).
 */
function initialRebalanceDelayMs(ctx: EngineContext): number {
  for (const broker of ctx.state.brokers.values())
    return broker.config.groupInitialRebalanceDelayMs;
  return 3000;
}

/** The join phase timer fired: tryCompleteInitialRebalanceElseSchedule, or the rebalance timeout. */
export function handleGroupJoinTimer(ctx: EngineContext, task: Task<'GroupJoinTimer'>): void {
  const group = ctx.state.groups.get(task.groupId);
  if (group?.timerToken !== task.token || group.state !== 'PreparingRebalance') return;
  const { initial } = task;
  if (initial !== null && group.newMemberAdded && initial.remainingMs !== 0) {
    group.newMemberAdded = false;
    const delayMs = Math.min(initialRebalanceDelayMs(ctx), initial.remainingMs);
    ctx.kernel.scheduleIn(delayMs, {
      type: 'GroupJoinTimer',
      groupId: group.groupId,
      token: task.token,
      initial: { delayMs, remainingMs: Math.max(initial.remainingMs - initial.delayMs, 0) },
    });
    return;
  }
  completeJoin(ctx, group);
}

/** Ends a non-initial join phase as soon as every known member has rejoined. */
function maybeCompleteJoin(ctx: EngineContext, group: GroupState): void {
  if (group.state !== 'PreparingRebalance' || group.previousState === 'Empty') return;
  for (const m of group.members.values()) if (!m.awaitingJoin) return;
  completeJoin(ctx, group);
}

/** GroupMetadataManager#completeClassicGroupJoin + ClassicGroup#initNextGeneration. */
function completeJoin(ctx: EngineContext, group: GroupState): void {
  for (const m of [...group.members.values()]) {
    if (!m.awaitingJoin) removeMember(ctx, group, m, 'rebalance-timeout');
  }
  // A leader that did not rejoin was removed above; the first remaining member takes over.
  group.timerToken += 1;
  group.generation += 1;
  if (group.members.size === 0) {
    group.previousState = group.state;
    group.state = 'Empty';
    group.protocol = null;
    group.leader = null;
    ctx.emit({
      type: 'RebalanceCompleted',
      groupId: group.groupId,
      generation: group.generation,
      protocol: null,
      leader: null,
      members: [],
    });
    return;
  }
  group.protocol = selectProtocol(group);
  group.previousState = group.state;
  group.state = 'CompletingRebalance';
  group.pendingSync.clear();
  for (const m of group.members.values()) {
    m.awaitingJoin = false;
    m.assignment = [];
    group.pendingSync.add(m.memberId);
    scheduleSessionExpiry(ctx, group, m);
    sendJoinResponse(ctx, group, m);
  }
  ctx.kernel.scheduleIn(rebalanceTimeoutMs(group), {
    type: 'GroupSyncTimer',
    groupId: group.groupId,
    token: group.timerToken,
  });
}

/**
 * ClassicGroup#selectProtocol: the assignors every member supports are candidates; each member
 * votes for its most preferred candidate; most votes wins. Kafka breaks a tie by HashMap order;
 * we take the first candidate voted for, by the member that has been in the group longest (K6).
 */
function selectProtocol(group: GroupState): Assignor {
  const members = [...group.members.values()];
  const candidates = members[0]?.metadata.protocols.filter((p) =>
    members.every((m) => m.metadata.protocols.includes(p)),
  );
  const votes = new Map<Assignor, number>();
  for (const m of members) {
    const vote = m.metadata.protocols.find((p) => candidates?.includes(p) === true);
    if (vote !== undefined) votes.set(vote, (votes.get(vote) ?? 0) + 1);
  }
  let best: Assignor | undefined;
  for (const [protocol, count] of votes) {
    if (best === undefined || count > (votes.get(best) ?? 0)) best = protocol;
  }
  // supportsProtocols keeps at least one common assignor, so there is always a vote.
  return best ?? 'range';
}

function sendJoinResponse(ctx: EngineContext, group: GroupState, member: GroupMember): void {
  const isLeader = group.leader === member.memberId;
  respondTo(
    ctx,
    group,
    member,
  )({
    api: 'JoinGroup',
    error: null,
    memberId: member.memberId,
    generation: group.generation,
    protocol: group.protocol ?? 'range',
    leader: group.leader ?? '',
    members: isLeader
      ? [...group.members.values()].map((m): AssignorMember => ({
          memberId: m.memberId,
          topics: m.metadata.subscription,
          ownedPartitions: m.metadata.ownedPartitions,
          generation: m.metadata.ownedGeneration,
        }))
      : [],
  });
}

// ── SyncGroup ─────────────────────────────────────────────────────────────────────────────────

function syncGroup(ctx: EngineContext, from: Sender, request: Request<'SyncGroup'>): void {
  const group = ctx.state.groups.get(from.groupId);
  const member = group?.members.get(request.memberId);
  const fail = (error: GroupError): void => {
    respond(ctx, from, { api: 'SyncGroup', error });
  };
  if (group === undefined || member === undefined) {
    fail('UNKNOWN_MEMBER_ID');
    return;
  }
  if (request.generation !== group.generation) {
    fail('ILLEGAL_GENERATION');
    return;
  }
  switch (group.state) {
    case 'Empty':
    case 'Dead':
      fail('UNKNOWN_MEMBER_ID');
      return;
    case 'PreparingRebalance':
      fail('REBALANCE_IN_PROGRESS');
      return;
    case 'CompletingRebalance': {
      member.awaitingSync = true;
      group.pendingSync.delete(member.memberId);
      scheduleSessionExpiry(ctx, group, member);
      if (request.assignment === null || group.leader !== member.memberId) return;
      const byMember = new Map(request.assignment);
      for (const m of group.members.values()) {
        m.assignment = [...(byMember.get(m.memberId) ?? [])].sort(comparePartitions);
      }
      group.previousState = group.state;
      group.state = 'Stable';
      ctx.emit({
        type: 'RebalanceCompleted',
        groupId: group.groupId,
        generation: group.generation,
        protocol: group.protocol,
        leader: group.leader,
        members: [...group.members.values()].map((m) => ({
          memberId: m.memberId,
          consumerId: m.consumerId,
          partitions: m.assignment,
        })),
      });
      for (const m of group.members.values()) {
        if (!m.awaitingSync) continue;
        m.awaitingSync = false;
        sendSyncResponse(ctx, group, m);
      }
      return;
    }
    case 'Stable':
      group.pendingSync.delete(member.memberId);
      scheduleSessionExpiry(ctx, group, member);
      sendSyncResponse(ctx, group, member);
      return;
  }
}

function sendSyncResponse(ctx: EngineContext, group: GroupState, member: GroupMember): void {
  respondTo(
    ctx,
    group,
    member,
  )({
    api: 'SyncGroup',
    error: null,
    generation: group.generation,
    assignment: member.assignment,
  });
}

/** Members that got a JoinGroup response but never sent SyncGroup are removed (K8). */
export function handleGroupSyncTimer(ctx: EngineContext, task: Task<'GroupSyncTimer'>): void {
  const group = ctx.state.groups.get(task.groupId);
  if (group?.timerToken !== task.token) return;
  if (group.state !== 'CompletingRebalance' && group.state !== 'Stable') return;
  if (group.pendingSync.size === 0) return;
  for (const memberId of [...group.pendingSync]) {
    const m = group.members.get(memberId);
    if (m !== undefined) removeMember(ctx, group, m, 'sync-timeout');
  }
  prepareRebalance(ctx, group, 'sync-timeout');
}

// ── Heartbeats and session timeouts ───────────────────────────────────────────────────────────

function heartbeat(ctx: EngineContext, from: Sender, request: Request<'Heartbeat'>): void {
  const group = ctx.state.groups.get(from.groupId);
  const member = group?.members.get(request.memberId);
  const answer = (error: GroupError | null): void => {
    respond(ctx, from, { api: 'Heartbeat', error });
  };
  if (group === undefined || member === undefined) {
    answer('UNKNOWN_MEMBER_ID');
    return;
  }
  if (request.generation !== group.generation) {
    answer('ILLEGAL_GENERATION');
    return;
  }
  scheduleSessionExpiry(ctx, group, member);
  answer(group.state === 'PreparingRebalance' ? 'REBALANCE_IN_PROGRESS' : null);
}

/** (Re)starts the member's session timer from now. */
function scheduleSessionExpiry(ctx: EngineContext, group: GroupState, member: GroupMember): void {
  member.heartbeatToken += 1;
  ctx.kernel.scheduleIn(member.metadata.sessionTimeoutMs, {
    type: 'MemberSessionExpire',
    groupId: group.groupId,
    memberId: member.memberId,
    token: member.heartbeatToken,
  });
}

/** GroupMetadataManager#expireClassicGroupMemberHeartbeat. */
export function handleMemberSessionExpire(
  ctx: EngineContext,
  task: Task<'MemberSessionExpire'>,
): void {
  const group = ctx.state.groups.get(task.groupId);
  const member = group?.members.get(task.memberId);
  if (group === undefined || member?.heartbeatToken !== task.token) return;
  // A member waiting in JoinGroup or SyncGroup has "satisfied" its heartbeat (K4).
  if (member.awaitingJoin || member.awaitingSync) {
    scheduleSessionExpiry(ctx, group, member);
    return;
  }
  removeMember(ctx, group, member, 'session-timeout');
  afterMemberLeft(ctx, group, 'session-timeout');
}

// ── LeaveGroup ────────────────────────────────────────────────────────────────────────────────

function leaveGroup(ctx: EngineContext, groupId: string, request: Request<'LeaveGroup'>): void {
  const group = ctx.state.groups.get(groupId);
  const member = group?.members.get(request.memberId);
  if (group === undefined || member === undefined) return; // UNKNOWN_MEMBER_ID, nobody waits
  removeMember(ctx, group, member, request.reason);
  afterMemberLeft(ctx, group, request.reason === 'poll-timeout' ? 'poll-timeout' : 'member-left');
}

function afterMemberLeft(ctx: EngineContext, group: GroupState, reason: RebalanceReason): void {
  switch (group.state) {
    case 'Stable':
    case 'CompletingRebalance':
      prepareRebalance(ctx, group, reason);
      return;
    case 'PreparingRebalance':
      maybeCompleteJoin(ctx, group);
      return;
    case 'Empty':
    case 'Dead':
      return;
  }
}

/** Removes a member; a parked JoinGroup is answered UNKNOWN_MEMBER_ID; the leader moves on. */
function removeMember(
  ctx: EngineContext,
  group: GroupState,
  member: GroupMember,
  reason: MemberRemovedReason,
): void {
  group.members.delete(member.memberId);
  group.pendingSync.delete(member.memberId);
  member.heartbeatToken += 1;
  if (member.awaitingJoin) {
    respondTo(ctx, group, member)({ api: 'JoinGroup', error: 'UNKNOWN_MEMBER_ID' });
  }
  if (group.leader === member.memberId) {
    // Kafka picks another member in HashMap order; we pick the longest-standing one (K7).
    const next =
      [...group.members.values()].find((m) => m.awaitingJoin) ?? [...group.members.values()][0];
    group.leader = next?.memberId ?? null;
  }
  ctx.emit({
    type: 'MemberRemoved',
    groupId: group.groupId,
    memberId: member.memberId,
    consumerId: member.consumerId,
    reason,
  });
}

// ── OffsetCommit ──────────────────────────────────────────────────────────────────────────────

/**
 * ClassicGroup#validateOffsetCommit, then stores the offsets of partitions that exist in the topic
 * life the client read them from. Returns the request's error, or null when accepted.
 */
function offsetCommit(
  ctx: EngineContext,
  from: Sender,
  request: Request<'OffsetCommit'>,
): GroupError | null {
  const group = coordinatorGroup(ctx, from.groupId);
  const error = validateOffsetCommit(group, request.memberId, request.generation);
  if (error !== null) return error;
  for (const o of request.offsets) {
    const topic = ctx.state.topics.get(o.topic);
    // UNKNOWN_TOPIC_OR_PARTITION for this partition only (KafkaApis#handleOffsetCommitRequest);
    // a topic re-created since the client read it counts as unknown (simplifications).
    if (topic?.partitions[o.partition] === undefined || topic.topicId !== o.topicId) continue;
    const key = partitionName(o.topic, o.partition);
    group.committed.set(key, o.offset);
    ctx.emit({
      type: 'OffsetCommitted',
      groupId: group.groupId,
      consumerId: from.consumerId,
      memberId: request.memberId === '' ? null : request.memberId,
      generation: request.generation,
      topic: o.topic,
      partition: o.partition,
      offset: o.offset,
    });
  }
  return null;
}

/** K1. A commit without member id and generation (-1) is only accepted by an Empty group. */
export function validateOffsetCommit(
  group: GroupState,
  memberId: string,
  generation: number,
): GroupError | null {
  if (group.state === 'Dead') return 'COORDINATOR_NOT_AVAILABLE';
  if (generation < 0 && memberId === '') {
    return group.state === 'Empty' ? null : 'UNKNOWN_MEMBER_ID';
  }
  if (!group.members.has(memberId)) return 'UNKNOWN_MEMBER_ID';
  if (generation !== group.generation) return 'ILLEGAL_GENERATION';
  if (group.state === 'CompletingRebalance') return 'REBALANCE_IN_PROGRESS';
  return null;
}

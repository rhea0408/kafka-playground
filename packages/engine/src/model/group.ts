/**
 * A consumer group as its group coordinator holds it (classic protocol; Kafka's ClassicGroup and
 * ClassicGroupMember in the group-coordinator module). See protocols/group-coordinator.ts for the
 * behaviour and ADR 0004 for why the classic protocol.
 */

import type { Assignor } from '@kp/schema';
import type { SimTime } from '../core/clock.ts';
import type { TopicPartition } from '../protocols/assignors/index.ts';

/** ClassicGroupState. `Dead` is not reached in M3 (no group deletion or offset expiry). */
export type GroupStateName =
  'Empty' | 'PreparingRebalance' | 'CompletingRebalance' | 'Stable' | 'Dead';

/** Why a rebalance started (RebalanceStarted.reason). */
export type RebalanceReason =
  /** A new member sent JoinGroup. */
  | 'member-joined'
  /** A member sent LeaveGroup (close(), unsubscribe(), or a group.id change). */
  | 'member-left'
  /** The coordinator heard no heartbeat for session.timeout.ms. */
  | 'session-timeout'
  /** A member's heartbeat thread left because poll() was not called within max.poll.interval.ms. */
  | 'poll-timeout'
  /** The leader rejoined, e.g. because its metadata showed new partitions. */
  | 'metadata-change'
  /** A member rejoined with a different subscription (topics) or assignor list. */
  | 'subscription-change'
  /** Cooperative: a member gave up partitions and rejoined so they can be reassigned. */
  | 'revoked-partitions'
  /** Members that got a JoinGroup response did not send SyncGroup within the rebalance timeout. */
  | 'sync-timeout';

/** Why a member left the group (MemberRemoved.reason). */
export type MemberRemovedReason =
  | 'leave'
  | 'poll-timeout'
  | 'session-timeout'
  /** It did not rejoin before the join phase ended (rebalance timeout). */
  | 'rebalance-timeout'
  | 'sync-timeout';

/** Why a client sends JoinGroup (KIP-800 adds a reason string to JoinGroup v8+). */
export type JoinReason =
  | 'new-member'
  | 'rebalance-in-progress'
  | 'metadata-change'
  | 'subscription-change'
  | 'revoked-partitions'
  | 'rejoin-after-error';

/** What a member told the coordinator in its last JoinGroup. */
export interface MemberMetadata {
  /** partition.assignment.strategy, in preference order (the JoinGroup protocols). */
  readonly protocols: readonly Assignor[];
  /** Subscribed topics. */
  readonly subscription: readonly string[];
  /**
   * Subscription#ownedPartitions: the partitions it still holds when it sends JoinGroup
   * (`subscriptions.assignedPartitionsList()`). Empty for an eager member, which revoked
   * everything in onJoinPrepare.
   */
  readonly ownedPartitions: readonly TopicPartition[];
  /**
   * StickyAssignor's user data: its previous assignment, kept even after an eager revoke, so the
   * eager sticky assignor can stay sticky (StickyAssignor#subscriptionUserData).
   */
  readonly stickyUserData: readonly TopicPartition[];
  /** The generation of those partitions (in the assignors' user data). */
  readonly ownedGeneration: number;
  readonly sessionTimeoutMs: number;
  /** The rebalance timeout: the client's max.poll.interval.ms. */
  readonly rebalanceTimeoutMs: number;
}

export interface GroupMember {
  readonly memberId: string;
  /** The client: consumer component id and its life, so responses reach the right process. */
  readonly consumerId: string;
  readonly incarnation: number;
  metadata: MemberMetadata;
  /** A JoinGroup from this member is waiting for the join phase to complete. */
  awaitingJoin: boolean;
  /** A SyncGroup from this member is waiting for the leader's assignment. */
  awaitingSync: boolean;
  /** Only the session-expiry task carrying this token is current. */
  heartbeatToken: number;
  /**
   * When the current session-expiry task fires: the last heartbeat (or join / sync) plus
   * session.timeout.ms. While a JoinGroup or SyncGroup is parked, the task re-arms instead of
   * removing the member (K4).
   */
  sessionExpiresAt: SimTime;
  /** Partitions from the last SyncGroup of the leader. */
  assignment: readonly TopicPartition[];
}

/**
 * Coordinator-side group state plus the committed offsets (what Kafka keeps in __consumer_offsets)
 * and the teaching aid's per-partition high mark (M3 Q9).
 */
export interface GroupState {
  readonly groupId: string;
  state: GroupStateName;
  /** The state before the current one: a rebalance that started from Empty is the initial one. */
  previousState: GroupStateName;
  /** Incremented each time the join phase completes (including when the group becomes Empty). */
  generation: number;
  /** The assignor chosen for the current generation; null while Empty. */
  protocol: Assignor | null;
  /** Member id of the leader; null without members. */
  leader: string | null;
  /** Members in join order (the leader is re-elected from the front). */
  readonly members: Map<string, GroupMember>;
  /** Initial rebalance: a member joined since the last delay window started. */
  newMemberAdded: boolean;
  /** Only the join / sync timer carrying this token is current. */
  timerToken: number;
  /** Members that got a JoinGroup response and have not sent SyncGroup yet. */
  readonly pendingSync: Set<string>;
  /** Committed offsets: "topic-partition" → next offset to read. */
  readonly committed: Map<string, number>;
  /**
   * Teaching aid (not Kafka): the highest offset per partition the group's members have handed
   * to the application, for DuplicateProcessed / RecordsSkipped.
   */
  readonly processed: Map<string, number>;
}

export function createGroup(groupId: string): GroupState {
  return {
    groupId,
    state: 'Empty',
    previousState: 'Empty',
    generation: 0,
    protocol: null,
    leader: null,
    members: new Map(),
    newMemberAdded: false,
    timerToken: 0,
    pendingSync: new Set(),
    committed: new Map(),
    processed: new Map(),
  };
}

/** The group's rebalance timeout: the largest member rebalance timeout (ClassicGroup). */
export function rebalanceTimeoutMs(group: GroupState): number {
  let max = 0;
  for (const m of group.members.values()) max = Math.max(max, m.metadata.rebalanceTimeoutMs);
  return max;
}

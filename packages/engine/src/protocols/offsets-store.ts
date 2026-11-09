/**
 * The group coordinator's storage in __consumer_offsets (M4-T8, M4 Q8; Kafka's CoordinatorRuntime,
 * OffsetMetadataManager and GroupMetadataManager; M4-decisions K10).
 *
 * - **Commits** are appended to the group's __consumer_offsets partition, one record per offset
 *   (key "offset/<group>/<topic>/<partition>"), and answered only once the partition's HW has
 *   passed them, i.e. they are on every in-sync replica (CoordinatorPartitionWriter writes with
 *   acks=1, the runtime defers the response to the HW). They become the group's committed offsets
 *   at that moment (OffsetCommitted). After offsets.commit.timeout.ms (5 s) the commit fails with
 *   COORDINATOR_NOT_AVAILABLE; if the coordinator loses the partition first, NOT_COORDINATOR.
 * - **Group metadata** (generation, protocol, leader, members and their assignments) is appended
 *   when a rebalance completes (key "group/<group>"). Simplified: the SyncGroup response does not
 *   wait for it to be replicated.
 * - **Failover:** when the partition's leadership moves, the new leader loads its groups from its
 *   own replica of the log. A group whose last record has members comes back **Stable** with those
 *   members, and every member's session timer starts again with a full session.timeout.ms, so no
 *   rebalance happens (GroupMetadataManager#onLoaded). Commits that never reached the new leader
 *   are gone; they were never answered.
 */

import type { Assignor } from '@kp/schema';
import type { EngineContext } from '../model/context.ts';
import {
  createGroup,
  type GroupMember,
  type GroupState,
  type MemberMetadata,
} from '../model/group.ts';
import { appendRecord, leaderLog, partitionName, type PartitionState } from '../model/partition.ts';
import { isAlive, type TopicState } from '../model/state.ts';
import type { CommitOffset, EngineTask } from '../model/tasks.ts';
import type { TopicPartition } from './assignors/index.ts';
import { OFFSETS_TOPIC, partitionFor } from './coordinator.ts';
import { onLeaderAppended, onLeaderStateChanged } from './replication.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/** The writer id of the coordinator's records. */
const COORDINATOR_WRITER = '__group_coordinator';

/** offsets.commit.timeout.ms default (GroupCoordinatorConfig). */
export const OFFSETS_COMMIT_TIMEOUT_MS = 5000;

/** Size of an internal record, for fetch byte counts (a key, an offset and some metadata). */
const INTERNAL_RECORD_BYTES = 100;

/** A commit written to the log and waiting for the HW. */
export interface PendingCommit {
  readonly commitId: number;
  readonly groupId: string;
  readonly consumerId: string;
  readonly incarnation: number;
  readonly memberId: string;
  readonly generation: number;
  /** The coordinator (leader of the __consumer_offsets partition) and its epoch at the write. */
  readonly broker: string;
  readonly leaderEpoch: number;
  readonly partition: number;
  /** Log offset of the commit's last record: answered once the HW passes it. */
  readonly lastOffset: number;
  readonly offsets: readonly CommitOffset[];
  readonly acceptedAt: number;
}

interface GroupRecord {
  readonly generation: number;
  readonly protocol: Assignor | null;
  readonly leader: string | null;
  readonly members: readonly {
    readonly memberId: string;
    readonly consumerId: string;
    readonly incarnation: number;
    readonly metadata: MemberMetadata;
    readonly assignment: readonly TopicPartition[];
  }[];
}

const offsetKey = (groupId: string, topic: string, partition: number) =>
  `offset/${groupId}/${topic}/${String(partition)}`;
const groupKey = (groupId: string) => `group/${groupId}`;

function offsetsPartition(ctx: EngineContext, groupId: string): PartitionState | undefined {
  return ctx.state.topics.get(OFFSETS_TOPIC)?.partitions[partitionFor(groupId)];
}

/** Appends an internal record at the leader of the group's partition; its offset, or null. */
function appendInternal(
  ctx: EngineContext,
  groupId: string,
  key: string,
  value: string,
  traceId: string,
): number | null {
  const topic = ctx.state.topics.get(OFFSETS_TOPIC);
  const partition = topic?.partitions[partitionFor(groupId)];
  if (topic === undefined || partition === undefined || leaderLog(partition) === undefined) {
    return null;
  }
  const leader = ctx.state.brokers.get(partition.leader ?? '');
  if (leader === undefined || !isAlive(leader.status)) return null; // no coordinator process
  const appended = appendRecord(partition, {
    key,
    value,
    timestamp: ctx.kernel.now(),
    producerId: COORDINATOR_WRITER,
    traceId,
    sizeBytes: INTERNAL_RECORD_BYTES,
  });
  onLeaderAppended(ctx, partition);
  return appended.offset;
}

/**
 * Writes a validated commit to the log and parks it until the HW passes it. `respond` sends the
 * OffsetCommit answer to the client.
 */
export function writeCommit(
  ctx: EngineContext,
  commit: Omit<PendingCommit, 'broker' | 'leaderEpoch' | 'partition' | 'lastOffset' | 'acceptedAt'>,
): void {
  const partition = offsetsPartition(ctx, commit.groupId);
  const topic = ctx.state.topics.get(OFFSETS_TOPIC);
  if (partition === undefined || topic === undefined || partition.leader === null) return;
  let lastOffset = -1;
  commit.offsets.forEach((o, i) => {
    lastOffset =
      appendInternal(
        ctx,
        commit.groupId,
        offsetKey(commit.groupId, o.topic, o.partition),
        JSON.stringify({ offset: o.offset, topicId: o.topicId }),
        `commit@${String(commit.commitId)}#${String(i)}`,
      ) ?? lastOffset;
  });
  ctx.state.pendingCommits.set(commit.commitId, {
    ...commit,
    broker: partition.leader,
    leaderEpoch: partition.leaderEpoch,
    partition: partition.index,
    lastOffset,
    acceptedAt: ctx.kernel.now(),
  });
  ctx.kernel.scheduleIn(OFFSETS_COMMIT_TIMEOUT_MS, {
    type: 'OffsetCommitTimeout',
    commitId: commit.commitId,
  });
  onLeaderStateChanged(ctx, topic, partition);
}

/** Records the group's metadata after a rebalance completed (Stable or Empty). */
export function writeGroupMetadata(ctx: EngineContext, group: GroupState): void {
  const record: GroupRecord = {
    generation: group.generation,
    protocol: group.protocol,
    leader: group.leader,
    members: [...group.members.values()].map((m) => ({
      memberId: m.memberId,
      consumerId: m.consumerId,
      incarnation: m.incarnation,
      metadata: m.metadata,
      assignment: m.assignment,
    })),
  };
  const topic = ctx.state.topics.get(OFFSETS_TOPIC);
  const partition = offsetsPartition(ctx, group.groupId);
  const offset = appendInternal(
    ctx,
    group.groupId,
    groupKey(group.groupId),
    JSON.stringify(record),
    `group@${group.groupId}#${String(group.generation)}`,
  );
  if (offset !== null && topic !== undefined && partition !== undefined) {
    onLeaderStateChanged(ctx, topic, partition);
  }
}

/** The __consumer_offsets HW moved: commits now below it are stored and answered. */
export function onOffsetsHighWatermark(ctx: EngineContext, partition: PartitionState): void {
  for (const [id, pending] of ctx.state.pendingCommits) {
    if (pending.partition !== partition.index) continue;
    if (partition.leader !== pending.broker || partition.leaderEpoch !== pending.leaderEpoch)
      continue;
    if (partition.highWatermark <= pending.lastOffset) continue;
    ctx.state.pendingCommits.delete(id);
    const group = ctx.state.groups.get(pending.groupId);
    if (group !== undefined) {
      for (const o of pending.offsets) {
        group.committed.set(partitionName(o.topic, o.partition), o.offset);
        ctx.emit({
          type: 'OffsetCommitted',
          groupId: group.groupId,
          consumerId: pending.consumerId,
          memberId: pending.memberId === '' ? null : pending.memberId,
          generation: pending.generation,
          topic: o.topic,
          partition: o.partition,
          offset: o.offset,
          acceptedAt: pending.acceptedAt,
        });
      }
    }
    answerCommit(ctx, pending, null);
  }
}

function answerCommit(
  ctx: EngineContext,
  pending: PendingCommit,
  error: 'COORDINATOR_NOT_AVAILABLE' | 'NOT_COORDINATOR' | null,
): void {
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'CoordinatorResponse',
    groupId: pending.groupId,
    consumerId: pending.consumerId,
    incarnation: pending.incarnation,
    response: { api: 'OffsetCommit', commitId: pending.commitId, error },
  });
}

/** offsets.commit.timeout.ms passed before the commit was replicated (K10). */
export function handleOffsetCommitTimeout(
  ctx: EngineContext,
  task: Task<'OffsetCommitTimeout'>,
): void {
  const pending = ctx.state.pendingCommits.get(task.commitId);
  if (pending === undefined) return;
  ctx.state.pendingCommits.delete(task.commitId);
  answerCommit(ctx, pending, 'COORDINATOR_NOT_AVAILABLE');
}

/** A broker process stopped: its pending commits vanish (the clients see a disconnect). */
export function onBrokerStoppedForCommits(ctx: EngineContext, brokerId: string): void {
  for (const [id, pending] of ctx.state.pendingCommits) {
    if (pending.broker === brokerId) ctx.state.pendingCommits.delete(id);
  }
}

/**
 * A __consumer_offsets partition got a new leader (or none): the old coordinator's pending
 * commits fail with NOT_COORDINATOR, and the new leader loads the partition's groups from its
 * replica (CoordinatorRuntime scheduleLoadOperation; loading is instant here).
 */
export function onOffsetsLeaderChanged(
  ctx: EngineContext,
  _topic: TopicState,
  partition: PartitionState,
  previousLeader: string | null,
): void {
  for (const [id, pending] of ctx.state.pendingCommits) {
    if (pending.partition !== partition.index) continue;
    ctx.state.pendingCommits.delete(id);
    answerCommit(ctx, pending, 'NOT_COORDINATOR');
  }
  // GroupMetadataManager#onUnloaded: the old coordinator answers every parked JoinGroup and
  // SyncGroup with NOT_COORDINATOR (a broker that stays up, e.g. after a preferred election).
  for (const [groupId, old] of ctx.state.groups) {
    if (partitionFor(groupId) !== partition.index) continue;
    for (const m of old.members.values()) {
      const api = m.awaitingJoin ? 'JoinGroup' : m.awaitingSync ? 'SyncGroup' : null;
      if (api === null) continue;
      m.awaitingJoin = false;
      m.awaitingSync = false;
      ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
        type: 'CoordinatorResponse',
        groupId,
        consumerId: m.consumerId,
        incarnation: m.incarnation,
        response: { api, error: 'NOT_COORDINATOR' },
      });
    }
  }
  const leader = leaderLog(partition);
  if (leader === undefined) return;
  for (const [groupId, old] of [...ctx.state.groups]) {
    if (partitionFor(groupId) !== partition.index) continue;
    const loaded = loadGroup(ctx, groupId, leader.log, old);
    ctx.state.groups.set(groupId, loaded);
    ctx.emit({
      type: 'CoordinatorMoved',
      groupId,
      from: previousLeader,
      to: partition.leader ?? '',
      state: loaded.state,
      generation: loaded.generation,
    });
  }
}

/** Rebuilds a group from the log (GroupMetadataManager replay, then onLoaded). */
function loadGroup(
  ctx: EngineContext,
  groupId: string,
  log: PartitionState['log'],
  old: GroupState,
): GroupState {
  const group = createGroup(groupId);
  group.timerToken = old.timerToken + 1;
  for (const [k, v] of old.processed) group.processed.set(k, v); // teaching aid, not Kafka state
  const prefix = `offset/${groupId}/`;
  let metadata: GroupRecord | null = null;
  for (const record of log) {
    if (record.producerId !== COORDINATOR_WRITER) continue; // only the coordinator's own records
    const key = record.key ?? '';
    if (key === groupKey(groupId)) {
      metadata = JSON.parse(record.value) as GroupRecord;
    } else if (key.startsWith(prefix)) {
      const rest = key.slice(prefix.length);
      const slash = rest.lastIndexOf('/');
      const topicName = rest.slice(0, slash);
      const index = Number(rest.slice(slash + 1));
      const value = JSON.parse(record.value) as { offset: number; topicId: number };
      const topic = ctx.state.topics.get(topicName);
      // Offsets of deleted (or since re-created) topics don't come back.
      if (topic?.partitions[index] === undefined || topic.topicId !== value.topicId) continue;
      group.committed.set(partitionName(topicName, index), value.offset);
    }
  }
  if (metadata !== null) {
    group.generation = metadata.generation;
    group.protocol = metadata.protocol;
    group.leader = metadata.leader;
    group.state = metadata.members.length > 0 ? 'Stable' : 'Empty';
    group.previousState = group.state;
    const now = ctx.kernel.now();
    for (const m of metadata.members) {
      const previous = old.members.get(m.memberId);
      const member: GroupMember = {
        memberId: m.memberId,
        consumerId: m.consumerId,
        incarnation: m.incarnation,
        metadata: m.metadata,
        awaitingJoin: false,
        awaitingSync: false,
        heartbeatToken: (previous?.heartbeatToken ?? 0) + 1,
        sessionExpiresAt: now + m.metadata.sessionTimeoutMs,
        assignment: m.assignment,
      };
      group.members.set(m.memberId, member);
      // onLoaded → rescheduleClassicGroupMemberHeartbeat: a full session from now.
      ctx.kernel.scheduleIn(m.metadata.sessionTimeoutMs, {
        type: 'MemberSessionExpire',
        groupId,
        memberId: m.memberId,
        token: member.heartbeatToken,
      });
    }
  }
  return group;
}

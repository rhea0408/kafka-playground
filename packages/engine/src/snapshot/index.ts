/**
 * Snapshots: plain, JSON-serialisable copies of engine state for the UI (architecture §4.1, §5)
 * and, later, the AI tutor (FR-7.11). A snapshot never changes after it is built and the UI
 * cannot mutate the simulation through it: every object in it is a fresh copy, except log
 * records, which are frozen when appended (model/partition.ts) and can therefore be shared.
 *
 * Diffs between two snapshots, for sending frames across the worker boundary: diff.ts.
 */

import type { Assignor, Broker, Connection, Consumer, Producer, Topic } from '@kp/schema';
import type { SimTime } from '../core/clock.ts';
import type { GroupStateName } from '../model/group.ts';
import { partitionName, type LogRecord } from '../model/partition.ts';
import type {
  BrokerProcessStatus,
  ConsumerState,
  EngineState,
  GroupState,
  ProducerStats,
} from '../model/state.ts';
import type { TopicPartition } from '../protocols/assignors/index.ts';

export interface PartitionSnapshot {
  readonly index: number;
  readonly leader: string;
  readonly logStartOffset: number;
  readonly logEndOffset: number;
  readonly highWatermark: number;
  /** The most recent records (at most `recordsPerPartition`), oldest first. */
  readonly records: readonly LogRecord[];
}

export interface TopicSnapshot {
  /**
   * Topic ID (KIP-516): new each time a topic with this name is created, so a deleted and
   * re-created topic can be told apart from the old one.
   */
  readonly topicId: number;
  readonly config: Topic;
  readonly partitions: readonly PartitionSnapshot[];
}

export interface ProducerSnapshot {
  readonly config: Producer;
  readonly stats: ProducerStats;
}

/** One partition this consumer owns, from its own point of view. */
export interface ConsumerPartitionSnapshot {
  readonly topic: string;
  readonly partition: number;
  /** Next offset this consumer will fetch; null before its first poll or while stopped. */
  readonly position: number | null;
  /** Next offset the group has saved; null if it never committed for this partition. */
  readonly committed: number | null;
  readonly highWatermark: number;
  /**
   * Consumer lag as kafka-consumer-groups.sh reports it: end offset − committed offset. The end
   * offset of a read_uncommitted consumer is the high watermark (KafkaConsumer#endOffsets), which
   * equals the LEO while each partition has one replica. Null without a committed offset (the
   * tool shows "-").
   */
  readonly lag: number | null;
  /** Records this consumer has not fetched yet: HW − position (client metric records-lag). */
  readonly fetchLag: number | null;
}

/**
 * - `joining`: running, but not (or no longer) in a stable generation: its JoinGroup or SyncGroup
 *   is pending, or it must rejoin.
 * - `left-group`: running and out of the group (its member id was reset: it left after
 *   max.poll.interval.ms, or the coordinator no longer knows it), but it still holds partitions
 *   and keeps processing the batch it has. Kafka's client keeps its assignment until the next
 *   poll(), whose onJoinPrepare calls onPartitionsLost (ConsumerCoordinator#onJoinPrepare). Its
 *   commits fail; the coordinator may already have given its partitions to someone else (a
 *   "zombie", FR-3.2).
 * - `stable`: in a stable generation with partitions; `idle`: in one without any.
 * - `stopped`: closed gracefully (StopConsumer); `crashed`: killed or died on an error.
 */
export type ConsumerStatus = 'joining' | 'left-group' | 'stable' | 'idle' | 'stopped' | 'crashed';

export interface ConsumerSnapshot {
  readonly config: Consumer;
  readonly status: ConsumerStatus;
  /**
   * Which life of the process this is: a new number on every start, stop, crash and restart.
   * Compare with `GroupMemberSnapshot.incarnation` to tell this process's member entry from one
   * the coordinator still holds for an earlier life.
   */
  readonly incarnation: number;
  /** StopConsumer was asked: it closes once it has processed its current batch. */
  readonly stopping: boolean;
  /** Its member id in the group, null before the coordinator gave it one. */
  readonly memberId: string | null;
  /** The generation of its current assignment, null outside one. */
  readonly generation: number | null;
  readonly consumed: number;
  /** The partitions it owns now, sorted. */
  readonly partitions: readonly ConsumerPartitionSnapshot[];
  /** Sum of the known lags of its partitions: a lower bound while `uncommitted` > 0. */
  readonly totalLag: number;
  /** How many of its partitions have no committed offset (their lag is unknown, shown "-"). */
  readonly uncommitted: number;
}

/** One partition of a topic the group reads. */
export interface GroupPartitionSnapshot {
  readonly topic: string;
  readonly partition: number;
  /** Consumer id of the member that owns it in the group's current assignment, or null. */
  readonly owner: string | null;
  readonly committed: number | null;
  readonly highWatermark: number;
  readonly logEndOffset: number;
  /** HW − committed (kafka-consumer-groups.sh LAG); null without a committed offset. */
  readonly lag: number | null;
}

export interface GroupMemberSnapshot {
  readonly memberId: string;
  readonly consumerId: string;
  /**
   * The incarnation of the client process that joined as this member. When it differs from the
   * consumer's current one, the member belongs to an earlier life (crashed, or killed and
   * restarted) and stays in the group until its session times out.
   */
  readonly incarnation: number;
  /** Its partitions in the current generation (empty while a rebalance is in progress). */
  readonly partitions: readonly TopicPartition[];
  /**
   * When the coordinator removes it unless it heartbeats first (last heartbeat or join + its
   * session.timeout.ms). Null while its JoinGroup or SyncGroup is parked: that counts as a
   * heartbeat, so the session does not run out then (GroupMetadataManager#
   * expireClassicGroupMemberHeartbeat).
   */
  readonly sessionExpiresAt: SimTime | null;
}

export interface GroupSnapshot {
  readonly groupId: string;
  /** The coordinator's state (ClassicGroupState). */
  readonly state: GroupStateName;
  readonly generation: number;
  /** The assignor chosen for this generation; null while Empty. */
  readonly protocol: Assignor | null;
  /** Member id of the leader; null without members. */
  readonly leader: string | null;
  /** In join order. */
  readonly members: readonly GroupMemberSnapshot[];
  readonly committed: readonly {
    readonly topic: string;
    readonly partition: number;
    readonly offset: number;
  }[];
  /**
   * Every partition of the topics the members subscribe to, and of topics the group has committed
   * offsets for, with owner, committed offset and lag (FR-2.9).
   */
  readonly partitions: readonly GroupPartitionSnapshot[];
  /**
   * Sum of the known partition lags. A lower bound while `uncommitted` > 0: kafka-consumer-groups
   * shows "-" for a partition without a committed offset, not 0 (review F-5).
   */
  readonly totalLag: number;
  /** How many of `partitions` have no committed offset. */
  readonly uncommitted: number;
}

export interface BrokerSnapshot {
  readonly config: Broker;
  /** The broker process: running, stopped (controlled shutdown) or crashed. */
  readonly status: BrokerProcessStatus;
}

export interface EngineSnapshot {
  readonly t: SimTime;
  readonly brokers: readonly BrokerSnapshot[];
  readonly topics: readonly TopicSnapshot[];
  readonly producers: readonly ProducerSnapshot[];
  readonly consumers: readonly ConsumerSnapshot[];
  readonly groups: readonly GroupSnapshot[];
  readonly connections: readonly Connection[];
}

export interface SnapshotOptions {
  readonly recordsPerPartition: number;
}

/**
 * Deep copy of a small plain-JSON value (component configs, connections). Much cheaper than a
 * JSON round trip, and only used for the few objects the engine might replace or that hold
 * nested arrays; bulk data (records) is shared frozen instead.
 */
function copyPlain<T>(value: T): T {
  if (Array.isArray(value)) return value.map(copyPlain) as T;
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, v] of Object.entries(value)) out[key] = copyPlain(v);
    return out as T;
  }
  return value;
}

export function buildSnapshot(
  state: EngineState,
  t: SimTime,
  options: SnapshotOptions,
): EngineSnapshot {
  return {
    t,
    brokers: [...state.brokers.values()].map((b) => ({
      config: copyPlain(b.config),
      status: b.status,
    })),
    topics: [...state.topics.values()].map((topic) => ({
      topicId: topic.topicId,
      config: copyPlain(topic.config),
      partitions: topic.partitions.map((p) => ({
        index: p.index,
        leader: p.leader,
        logStartOffset: p.logStartOffset,
        logEndOffset: p.logEndOffset,
        highWatermark: p.highWatermark,
        // A fresh array of frozen records: appending to the log later does not change it.
        records: options.recordsPerPartition > 0 ? p.log.slice(-options.recordsPerPartition) : [],
      })),
    })),
    producers: [...state.producers.values()].map((p) => ({
      config: copyPlain(p.config),
      stats: { ...p.stats },
    })),
    consumers: [...state.consumers.values()].map((c) => consumerSnapshot(state, c)),
    groups: [...state.groups.values()].map((g) => groupSnapshot(state, g)),
    connections: state.connections.map(copyPlain),
  };
}

function consumerStatus(consumer: ConsumerState): ConsumerStatus {
  if (consumer.status === 'stopped') return 'stopped';
  if (consumer.status === 'crashed') return 'crashed';
  const { group } = consumer;
  // After ILLEGAL_GENERATION the member keeps its id (only the generation is reset), so it shows
  // `joining` while it still holds partitions it is about to lose: the coordinator still counts it
  // as a member, it is not out of the group.
  if (group.memberId === '' && group.owned.length > 0) return 'left-group';
  if (group.state !== 'stable' || group.pendingAssignment !== null) return 'joining';
  return group.owned.length === 0 ? 'idle' : 'stable';
}

function consumerSnapshot(state: EngineState, consumer: ConsumerState): ConsumerSnapshot {
  const committed = state.groups.get(consumer.config.groupId)?.committed;
  const partitions = consumer.group.owned.flatMap((tp): ConsumerPartitionSnapshot[] => {
    const p = state.topics.get(tp.topic)?.partitions[tp.partition];
    if (p === undefined) return [];
    const key = partitionName(tp.topic, tp.partition);
    const position = consumer.positions.get(key)?.offset ?? null;
    const saved = committed?.get(key) ?? null;
    return [
      {
        topic: tp.topic,
        partition: tp.partition,
        position,
        committed: saved,
        highWatermark: p.highWatermark,
        lag: saved === null ? null : Math.max(0, p.highWatermark - saved),
        fetchLag: position === null ? null : Math.max(0, p.highWatermark - position),
      },
    ];
  });
  const { group } = consumer;
  return {
    config: copyPlain(consumer.config),
    status: consumerStatus(consumer),
    incarnation: consumer.incarnation,
    stopping: consumer.stopRequested,
    memberId: group.memberId === '' ? null : group.memberId,
    generation: group.generation < 0 ? null : group.generation,
    consumed: consumer.stats.consumed,
    partitions,
    totalLag: partitions.reduce((sum, p) => sum + (p.lag ?? 0), 0),
    uncommitted: partitions.filter((p) => p.lag === null).length,
  };
}

const splitKey = (key: string): TopicPartition => {
  const dash = key.lastIndexOf('-');
  return { topic: key.slice(0, dash), partition: Number(key.slice(dash + 1)) };
};

function groupSnapshot(state: EngineState, group: GroupState): GroupSnapshot {
  const members = [...group.members.values()];
  const owner = new Map<string, string>();
  for (const m of members) {
    for (const tp of m.assignment) owner.set(partitionName(tp.topic, tp.partition), m.consumerId);
  }
  // Topics: what the members subscribe to, then topics with committed offsets, in that order.
  const topics = [
    ...new Set([
      ...members.flatMap((m) => m.metadata.subscription),
      ...[...group.committed.keys()].map((k) => splitKey(k).topic),
    ]),
  ];
  const partitions = topics.flatMap((topic) =>
    (state.topics.get(topic)?.partitions ?? []).map((p): GroupPartitionSnapshot => {
      const key = partitionName(topic, p.index);
      const committed = group.committed.get(key) ?? null;
      return {
        topic,
        partition: p.index,
        owner: owner.get(key) ?? null,
        committed,
        highWatermark: p.highWatermark,
        logEndOffset: p.logEndOffset,
        lag: committed === null ? null : Math.max(0, p.highWatermark - committed),
      };
    }),
  );
  return {
    groupId: group.groupId,
    state: group.state,
    generation: group.generation,
    protocol: group.protocol,
    leader: group.leader,
    members: members.map((m) => ({
      memberId: m.memberId,
      consumerId: m.consumerId,
      incarnation: m.incarnation,
      partitions: m.assignment.map((tp) => ({ topic: tp.topic, partition: tp.partition })),
      sessionExpiresAt: m.awaitingJoin || m.awaitingSync ? null : m.sessionExpiresAt,
    })),
    committed: [...group.committed.entries()].map(([key, offset]) => ({
      ...splitKey(key),
      offset,
    })),
    partitions,
    totalLag: partitions.reduce((sum, p) => sum + (p.lag ?? 0), 0),
    uncommitted: partitions.filter((p) => p.lag === null).length,
  };
}

export {
  applyDiff,
  diffSnapshot,
  isEmptyDiff,
  type KeyedDiff,
  type PartitionDiff,
  type SnapshotDiff,
  type TopicDiff,
} from './diff.ts';

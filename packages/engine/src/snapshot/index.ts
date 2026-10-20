/**
 * Snapshots: plain, JSON-serialisable copies of engine state for the UI (architecture §4.1, §5)
 * and, later, the AI tutor (FR-7.11). A snapshot never changes after it is built and the UI
 * cannot mutate the simulation through it: every object in it is a fresh copy, except log
 * records, which are frozen when appended (model/partition.ts) and can therefore be shared.
 *
 * Diffs between two snapshots, for sending frames across the worker boundary: diff.ts.
 */

import type { Broker, Connection, Consumer, Producer, Topic } from '@kp/schema';
import type { SimTime } from '../core/clock.ts';
import { partitionName, type LogRecord } from '../model/partition.ts';
import { topicsOfConsumer, type EngineState, type ProducerStats } from '../model/state.ts';

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

/** One assigned partition, from one consumer's point of view. */
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

export interface ConsumerSnapshot {
  readonly config: Consumer;
  readonly status: 'running' | 'stopped';
  readonly consumed: number;
  readonly partitions: readonly ConsumerPartitionSnapshot[];
  /** Sum of the known per-partition lags. */
  readonly totalLag: number;
}

export interface GroupSnapshot {
  readonly groupId: string;
  readonly committed: readonly {
    readonly topic: string;
    readonly partition: number;
    readonly offset: number;
  }[];
}

export interface BrokerSnapshot {
  readonly config: Broker;
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
    brokers: [...state.brokers.values()].map((b) => ({ config: copyPlain(b.config) })),
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
    consumers: [...state.consumers.values()].map((consumer) => {
      const committed = state.groups.get(consumer.config.groupId)?.committed;
      const partitions = topicsOfConsumer(state, consumer.config.id).flatMap((topic) =>
        topic.partitions.map((p): ConsumerPartitionSnapshot => {
          const key = partitionName(topic.config.id, p.index);
          const position = consumer.positions.get(key)?.offset ?? null;
          const saved = committed?.get(key) ?? null;
          return {
            topic: topic.config.id,
            partition: p.index,
            position,
            committed: saved,
            highWatermark: p.highWatermark,
            lag: saved === null ? null : Math.max(0, p.highWatermark - saved),
            fetchLag: position === null ? null : Math.max(0, p.highWatermark - position),
          };
        }),
      );
      return {
        config: copyPlain(consumer.config),
        status: consumer.status,
        consumed: consumer.stats.consumed,
        partitions,
        totalLag: partitions.reduce((sum, p) => sum + (p.lag ?? 0), 0),
      };
    }),
    groups: [...state.groups.entries()].map(([groupId, group]) => ({
      groupId,
      committed: [...group.committed.entries()].map(([key, offset]) => {
        const dash = key.lastIndexOf('-');
        return { topic: key.slice(0, dash), partition: Number(key.slice(dash + 1)), offset };
      }),
    })),
    connections: state.connections.map(copyPlain),
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

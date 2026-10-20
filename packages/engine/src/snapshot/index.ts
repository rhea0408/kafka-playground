/**
 * Snapshots: plain, JSON-serialisable copies of engine state for the UI (architecture §4.1, §5)
 * and, later, the AI tutor (FR-7.11). Nothing in a snapshot aliases engine state, so the UI
 * cannot mutate the simulation through it.
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

export interface EngineSnapshot {
  readonly t: SimTime;
  readonly brokers: readonly { readonly config: Broker }[];
  readonly topics: readonly TopicSnapshot[];
  readonly producers: readonly ProducerSnapshot[];
  readonly consumers: readonly ConsumerSnapshot[];
  readonly groups: readonly GroupSnapshot[];
  readonly connections: readonly Connection[];
}

export interface SnapshotOptions {
  readonly recordsPerPartition: number;
}

const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

export function buildSnapshot(
  state: EngineState,
  t: SimTime,
  options: SnapshotOptions,
): EngineSnapshot {
  const snapshot: EngineSnapshot = {
    t,
    brokers: [...state.brokers.values()].map((b) => ({ config: b.config })),
    topics: [...state.topics.values()].map((topic) => ({
      config: topic.config,
      partitions: topic.partitions.map((p) => ({
        index: p.index,
        leader: p.leader,
        logStartOffset: p.logStartOffset,
        logEndOffset: p.logEndOffset,
        highWatermark: p.highWatermark,
        records: options.recordsPerPartition > 0 ? p.log.slice(-options.recordsPerPartition) : [],
      })),
    })),
    producers: [...state.producers.values()].map((p) => ({ config: p.config, stats: p.stats })),
    consumers: [...state.consumers.values()].map((consumer) => {
      const committed = state.groups.get(consumer.config.groupId)?.committed;
      const partitions = topicsOfConsumer(state, consumer.config.id).flatMap((topic) =>
        topic.partitions.map((p): ConsumerPartitionSnapshot => {
          const key = partitionName(topic.config.id, p.index);
          const position = consumer.positions.get(key) ?? null;
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
        config: consumer.config,
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
    connections: state.connections,
  };
  return copy(snapshot);
}

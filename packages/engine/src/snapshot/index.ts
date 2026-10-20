/**
 * Snapshots: plain, JSON-serialisable copies of engine state for the UI (architecture §4.1, §5).
 * Nothing in a snapshot aliases engine state, so the UI cannot mutate the simulation through it.
 */

import type { Broker, Connection, Consumer, Producer, Topic } from '@kp/schema';
import type { SimTime } from '../core/clock.ts';
import type { LogRecord } from '../model/partition.ts';
import type { EngineState } from '../model/state.ts';

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

export interface EngineSnapshot {
  readonly t: SimTime;
  readonly brokers: readonly { readonly config: Broker }[];
  readonly topics: readonly TopicSnapshot[];
  readonly producers: readonly { readonly config: Producer }[];
  readonly consumers: readonly { readonly config: Consumer }[];
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
  return copy({
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
    producers: [...state.producers.values()].map((p) => ({ config: p.config })),
    consumers: [...state.consumers.values()].map((c) => ({ config: c.config })),
    connections: state.connections,
  });
}

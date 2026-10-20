/**
 * Hand-built engine snapshots for component tests of the live node state (M2-T8). Configs go
 * through the schema, so defaults are the real ones.
 */

import type {
  ConsumerPartitionSnapshot,
  ConsumerSnapshot,
  EngineSnapshot,
  LogRecord,
  PartitionSnapshot,
} from '@kp/engine';
import {
  parseComponent,
  type Broker,
  type Component,
  type Consumer,
  type Producer,
  type Topic,
} from '@kp/schema';

function parse(input: Record<string, unknown>): Component {
  const result = parseComponent(input);
  if (!result.ok) throw new Error(result.errors.join(' '));
  return result.value;
}

export function record(offset: number, key: string | null): LogRecord {
  return {
    offset,
    key,
    value: `v${String(offset)}`,
    timestamp: offset * 10,
    producerId: 'p1',
    traceId: `p1@1#${String(offset)}`,
    sizeBytes: 1024,
  };
}

/** A partition whose log holds offsets 0 … leo-1; the snapshot window keeps the last `window`. */
export function partition(
  index: number,
  leo: number,
  options: { hw?: number; keys?: readonly (string | null)[]; window?: number } = {},
): PartitionSnapshot {
  const keys = options.keys ?? ['A', 'B', null];
  const window = options.window ?? 20;
  const first = Math.max(0, leo - window);
  const records: LogRecord[] = [];
  for (let o = first; o < leo; o++) records.push(record(o, keys[o % keys.length] ?? null));
  return {
    index,
    leader: 'b1',
    logStartOffset: 0,
    logEndOffset: leo,
    highWatermark: options.hw ?? leo,
    records,
  };
}

export function consumerPartition(
  p: number,
  hw: number,
  position: number | null,
  committed: number | null,
  topic = 't1',
): ConsumerPartitionSnapshot {
  return {
    topic,
    partition: p,
    position,
    committed,
    highWatermark: hw,
    lag: committed === null ? null : Math.max(0, hw - committed),
    fetchLag: position === null ? null : Math.max(0, hw - position),
  };
}

export function consumer(
  id: string,
  partitions: readonly ConsumerPartitionSnapshot[],
  status: 'running' | 'stopped' = 'running',
): ConsumerSnapshot {
  return {
    config: parse({ kind: 'consumer', id }) as Consumer,
    status,
    consumed: 0,
    partitions,
    totalLag: partitions.reduce((sum, p) => sum + (p.lag ?? 0), 0),
  };
}

export function producer(id: string, sent: number, acked: number, failed: number) {
  return { config: parse({ kind: 'producer', id }) as Producer, stats: { sent, acked, failed } };
}

/** b1; producers p1, p2; topic t1 with two partitions; consumer c1 reading t1. */
export function fixtureSnapshot(overrides: Partial<EngineSnapshot> = {}): EngineSnapshot {
  return {
    t: 1000,
    brokers: [{ config: parse({ kind: 'broker', id: 'b1' }) as Broker }],
    topics: [
      {
        topicId: 1,
        config: parse({ kind: 'topic', id: 't1', partitions: 2 }) as Topic,
        partitions: [partition(0, 12), partition(1, 3, { keys: ['C'] })],
      },
    ],
    producers: [producer('p1', 15, 14, 1), producer('p2', 0, 0, 0)],
    consumers: [consumer('c1', [consumerPartition(0, 12, 12, 10), consumerPartition(1, 3, 3, 3)])],
    groups: [],
    connections: [
      { from: 'p1', to: 't1' },
      { from: 't1', to: 'c1' },
    ],
    ...overrides,
  };
}

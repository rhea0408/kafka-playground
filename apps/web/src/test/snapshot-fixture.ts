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
  GroupSnapshot,
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
    leaderEpoch: 0,
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
    replicas: [{ broker: 'b1', logEndOffset: leo, highWatermark: options.hw ?? leo }],
    isr: ['b1'],
    elr: [],
    underReplicated: false,
    underMinIsr: false,
    leaderEpoch: 0,
    partitionEpoch: 0,
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
  status: ConsumerSnapshot['status'] = 'stable',
  incarnation = 1,
): ConsumerSnapshot {
  const inGroup = status === 'stable' || status === 'idle';
  return {
    config: parse({ kind: 'consumer', id }) as Consumer,
    status,
    incarnation,
    stopping: false,
    memberId: inGroup ? `${id}-0000000a` : null,
    generation: inGroup ? 1 : null,
    consumed: 0,
    partitions,
    totalLag: partitions.reduce((sum, p) => sum + (p.lag ?? 0), 0),
    uncommitted: partitions.filter((p) => p.lag === null).length,
  };
}

/** A group with committed offsets only (no members), for partition-log tests. */
export function groupWithCommits(
  groupId: string,
  committed: readonly { topic: string; partition: number; offset: number }[],
): GroupSnapshot {
  return {
    groupId,
    state: 'Empty',
    generation: 0,
    protocol: null,
    leader: null,
    members: [],
    committed,
    partitions: [],
    totalLag: 0,
    uncommitted: 0,
  };
}

export function producer(id: string, sent: number, acked: number, failed: number) {
  return { config: parse({ kind: 'producer', id }) as Producer, stats: { sent, acked, failed } };
}

/** b1; producers p1, p2; topic t1 with two partitions; consumer c1 reading t1. */
export function fixtureSnapshot(overrides: Partial<EngineSnapshot> = {}): EngineSnapshot {
  return {
    t: 1000,
    brokers: [
      {
        config: parse({ kind: 'broker', id: 'b1' }) as Broker,
        status: 'running',
        partitionsLed: 2,
        partitionsFollowed: 0,
      },
    ],
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

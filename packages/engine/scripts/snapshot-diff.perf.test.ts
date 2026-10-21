/**
 * Benchmark sanity check for M2-T3: with ~1,000 partitions, building a snapshot and diffing it
 * against the previous one must fit well inside one 33 ms UI frame. Lives in scripts/ because it
 * reads the wall clock (performance.now), which engine source may not.
 *
 * Robust against slow CI: the median of several frames is checked, with a generous bound (one
 * whole frame). Measured numbers are recorded in docs/milestones/M2-decisions.md (T3).
 */

import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine, diffSnapshot, type EngineSnapshot } from '../src/index.ts';

const TOPICS = 10;
const PARTITIONS_PER_TOPIC = 100; // 1,000 partitions
const FRAME_MS = 33;

/** 400 distinct keys, so keyed records land on (nearly) every partition. */
const keys = Array.from({ length: 400 }, (_, i) => `key-${String(i)}`);
const topicIds = Array.from({ length: TOPICS }, (_, i) => `topic-${String(i)}`);
const topology: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1' }],
  topics: topicIds.map((id) => ({ kind: 'topic' as const, id, partitions: PARTITIONS_PER_TOPIC })),
  producers: topicIds.map((_, i) => ({
    kind: 'producer' as const,
    id: `p${String(i)}`,
    ratePerSec: 400,
    keyStrategy: { type: 'random' as const, keys },
  })),
  consumers: [0, 1].map((i) => ({
    kind: 'consumer' as const,
    id: `c${String(i)}`,
    autoOffsetReset: 'earliest' as const,
  })),
  connections: [
    ...topicIds.map((to, i) => ({ from: `p${String(i)}`, to })),
    ...topicIds.map((from, i) => ({ from, to: `c${String(i % 2)}` })),
  ],
};

const median = (xs: readonly number[]): number => {
  const sorted = [...xs].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? Number.NaN;
};

describe('snapshot + diff benchmark', () => {
  it('builds and diffs a 1,000-partition snapshot well within one 33 ms frame', () => {
    const engine = createEngine({ seed: 7, topology });
    engine.runUntil(6000); // ~24,000 records: fills most partitions' 20-record snapshot windows
    let prev: EngineSnapshot = engine.getSnapshot();
    const snapshotMs: number[] = [];
    const diffMs: number[] = [];
    const totalMs: number[] = [];
    for (let frame = 0; frame < 15; frame++) {
      engine.runUntil(engine.now() + FRAME_MS);
      const t0 = performance.now();
      const next = engine.getSnapshot();
      const t1 = performance.now();
      diffSnapshot(prev, next);
      const t2 = performance.now();
      snapshotMs.push(t1 - t0);
      diffMs.push(t2 - t1);
      totalMs.push(t2 - t0);
      prev = next;
    }
    const partitions = prev.topics.reduce((n, t) => n + t.partitions.length, 0);
    expect(partitions).toBe(1000);
    const records = prev.topics.reduce(
      (n, t) => n + t.partitions.reduce((m, p) => m + p.records.length, 0),
      0,
    );
    expect(records).toBeGreaterThan(15_000);
    // Shown with `pnpm --filter @kp/engine test -- --reporter=verbose` or on failure.
    console.info(
      `snapshot+diff, 1000 partitions: median snapshot ${median(snapshotMs).toFixed(2)} ms, ` +
        `diff ${median(diffMs).toFixed(2)} ms, total ${median(totalMs).toFixed(2)} ms ` +
        `(${String(records)} records in the snapshot)`,
    );
    expect(median(totalMs)).toBeLessThan(FRAME_MS);
  }, 60_000); // the warm-up simulates ~24,000 records; slow CI machines need time for that
});

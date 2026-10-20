import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine } from '../engine.ts';

/**
 * One partition, so every number can be worked out by hand:
 * - p1 sends key "A" at 2 msg/s: records at t = 500, 1000, 1500, …; each reaches the broker 1 ms
 *   later (501, 1001, …), so by t = 2999 offsets 0-4 exist and LEO = HW = 5.
 * - c1's fetch waits at the broker (long poll) and is answered when a record is appended; the
 *   response arrives 1 ms later, so poll() returns offset n at 502, 1002, 1502, … While blocked
 *   in poll() it wakes for auto-commit every 1000 ms: at t = 1000 it commits 1, at t = 2000 it
 *   commits 3 (offsets 1 and 2 were returned at 1002 and 1502), at t = 3000 it commits 5.
 */
const topology: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1' }],
  topics: [{ kind: 'topic', id: 'orders', partitions: 1 }],
  producers: [
    { kind: 'producer', id: 'p1', ratePerSec: 2, keyStrategy: { type: 'fixed', key: 'A' } },
  ],
  consumers: [
    {
      kind: 'consumer',
      id: 'c1',
      autoOffsetReset: 'earliest',
      autoCommitIntervalMs: 1000,
    },
  ],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'orders', to: 'c1' },
  ],
};

const consumerPartition = (engine: ReturnType<typeof createEngine>) =>
  engine.getSnapshot().consumers[0]?.partitions[0];

describe('snapshot and lag', () => {
  it('matches the hand-computed scenario: lag = HW − committed', () => {
    const engine = createEngine({ seed: 1, topology });

    engine.runUntil(2999);
    expect(consumerPartition(engine)).toEqual({
      topic: 'orders',
      partition: 0,
      position: 5, // all of offsets 0-4 have been fetched…
      committed: 3, // …but the last commit (t = 2000) saved 3
      highWatermark: 5,
      lag: 2,
      fetchLag: 0,
    });
    expect(engine.getSnapshot().consumers[0]?.totalLag).toBe(2);

    engine.runUntil(3000); // the poll at t = 3000 commits 5
    expect(consumerPartition(engine)).toMatchObject({ committed: 5, lag: 0 });
  });

  it('lag keeps growing while the consumer is down, and the committed offset stays put', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(3500);
    engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    engine.runUntil(6000);
    // Records reached the broker at 501, 1001, …, 5501: 11 records, HW = 11. Committed is still 5.
    const snap = engine.getSnapshot();
    expect(snap.consumers[0]).toMatchObject({ status: 'stopped', totalLag: 6 });
    expect(snap.consumers[0]?.partitions[0]).toMatchObject({
      position: null,
      committed: 5,
      highWatermark: 11,
      lag: 6,
      fetchLag: null,
    });
    expect(snap.groups).toEqual([
      { groupId: 'c1-group', committed: [{ topic: 'orders', partition: 0, offset: 5 }] },
    ]);
  });

  it('reports lag as null (shown as "-") before the group has committed anything', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(800);
    expect(consumerPartition(engine)).toMatchObject({ committed: null, lag: null, position: 1 });
    expect(engine.getSnapshot().consumers[0]?.totalLag).toBe(0);
  });

  it('contains the partition offsets and the most recent records', () => {
    const engine = createEngine({ seed: 1, topology, snapshotRecordsPerPartition: 2 });
    engine.runUntil(2999);
    const partition = engine.getSnapshot().topics[0]?.partitions[0];
    expect(partition).toMatchObject({
      index: 0,
      leader: 'b1',
      logStartOffset: 0,
      logEndOffset: 5,
      highWatermark: 5,
    });
    expect(partition?.records.map((r) => [r.offset, r.key, r.value, r.timestamp])).toEqual([
      [3, 'A', 'p1-4', 2000],
      [4, 'A', 'p1-5', 2500],
    ]);
  });

  it('is JSON-serialisable and does not alias engine state', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(2000);
    const snap = engine.getSnapshot();
    expect(JSON.parse(JSON.stringify(snap))).toEqual(snap);
    const before = JSON.stringify(snap);
    engine.runUntil(5000);
    expect(JSON.stringify(snap)).toBe(before);
    (snap.topics[0]?.partitions[0]?.records as unknown[]).length = 0;
    expect(engine.getSnapshot().topics[0]?.partitions[0]?.records.length).toBeGreaterThan(0);
  });

  it('includes producer stats and consumer counts', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(2999);
    const snap = engine.getSnapshot();
    expect(snap.producers[0]?.stats).toEqual({ sent: 5, acked: 5, failed: 0 });
    expect(snap.consumers[0]).toMatchObject({ status: 'running', consumed: 5 });
  });
});

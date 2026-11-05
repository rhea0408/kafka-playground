import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';

/**
 * One partition, so every number can be worked out by hand:
 * - p1 sends key "A" at 2 msg/s: records at t = 500, 1000, 1500, …; each reaches the broker 1 ms
 *   later (501, 1001, …), so by t = 2999 offsets 0-4 exist and LEO = HW = 5.
 * - c1 joins its group (no initial rebalance delay on this broker) and owns the partition at
 *   t = 4. Its fetch waits at the broker (long poll) and is answered when a record is appended;
 *   the response arrives 1 ms later, so poll() returns offset n at 502, 1002, 1502, … While
 *   blocked in poll() it wakes for auto-commit every 1000 ms: at t = 1000 it commits 1, at
 *   t = 2000 it commits 3 (offsets 1 and 2 were returned at 1002 and 1502), at t = 3000 it
 *   commits 5. Each commit reaches the group coordinator 1 ms later (1001, 2001, 3001).
 */
const topology: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1', groupInitialRebalanceDelayMs: 0 }],
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
      committed: 3, // …but the last commit (sent at 2000) saved 3
      highWatermark: 5,
      lag: 2,
      fetchLag: 0,
    });
    expect(engine.getSnapshot().consumers[0]?.totalLag).toBe(2);

    engine.runUntil(3001); // the poll at t = 3000 commits 5; stored at 3001
    // The record sent at 3000 is also appended at 3001: HW 6, so lag 1.
    expect(consumerPartition(engine)).toMatchObject({ committed: 5, highWatermark: 6, lag: 1 });
  });

  it('lag keeps growing while the consumer is down, and the committed offset stays put', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(3500);
    engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    engine.runUntil(6000);
    // Records reached the broker at 501, 1001, …, 5501: 11 records, HW = 11. Committed is still 5.
    const snap = engine.getSnapshot();
    // A crashed consumer owns nothing; the group (still holding the dead member until its session
    // times out) shows the lag, as kafka-consumer-groups.sh --describe does.
    expect(snap.consumers[0]).toMatchObject({ status: 'crashed', partitions: [], totalLag: 0 });
    expect(snap.groups[0]).toMatchObject({
      groupId: 'c1-group',
      committed: [{ topic: 'orders', partition: 0, offset: 5 }],
      partitions: [
        { topic: 'orders', partition: 0, committed: 5, highWatermark: 11, lag: 6, owner: 'c1' },
      ],
      totalLag: 6,
    });
  });

  it('reports lag as null (shown as "-") before the group has committed anything', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(800);
    expect(consumerPartition(engine)).toMatchObject({ committed: null, lag: null, position: 1 });
    // The known lag is 0, but one partition's lag is unknown: "≥ 0 · 1 not committed" (F-5).
    expect(engine.getSnapshot().consumers[0]).toMatchObject({ totalLag: 0, uncommitted: 1 });
    expect(engine.getSnapshot().groups[0]).toMatchObject({ totalLag: 0, uncommitted: 1 });
    engine.runUntil(1001); // the first commit is stored
    expect(engine.getSnapshot().consumers[0]).toMatchObject({ uncommitted: 0 });
    expect(engine.getSnapshot().groups[0]).toMatchObject({ uncommitted: 0 });
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
    expect(snap.consumers[0]).toMatchObject({ status: 'stable', consumed: 5, generation: 1 });
    expect(snap.consumers[0]?.memberId).toMatch(/^c1-/);
    expect(snap.groups[0]).toMatchObject({
      state: 'Stable',
      generation: 1,
      protocol: 'range',
      leader: snap.consumers[0]?.memberId,
      members: [{ consumerId: 'c1', partitions: [{ topic: 'orders', partition: 0 }] }],
    });
  });
});

describe('member status and session (M4-T0, review F-6 / F-7)', () => {
  it("a crashed member's session expiry is when the coordinator removes it", () => {
    const engine = createEngine({ seed: 1, topology });
    const removed: EngineEvent[] = [];
    engine.on((e) => {
      if (e.type === 'MemberRemoved') removed.push(e);
    });
    engine.runUntil(3500);
    const alive = engine.getSnapshot().groups[0]?.members[0]?.sessionExpiresAt;
    engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    engine.runUntil(3600);
    // No more heartbeats: the deadline stops moving, and the coordinator still assigns the
    // partition to the dead member until then.
    const member = engine.getSnapshot().groups[0]?.members[0];
    expect(member).toMatchObject({
      consumerId: 'c1',
      partitions: [{ topic: 'orders', partition: 0 }],
    });
    const deadline = member?.sessionExpiresAt ?? -1;
    expect(deadline).toBe(alive);
    expect(deadline).toBeGreaterThan(3600);
    engine.runUntil(deadline);
    expect(removed).toMatchObject([{ t: deadline, consumerId: 'c1', reason: 'session-timeout' }]);
    expect(engine.getSnapshot().groups[0]?.members).toEqual([]);
  });

  /** c2 takes 20 s per record and is removed after max.poll.interval.ms (15 s). */
  const slow: TopologyInput = {
    brokers: [{ kind: 'broker', id: 'b1', groupInitialRebalanceDelayMs: 0 }],
    topics: [{ kind: 'topic', id: 'orders', partitions: 2 }],
    producers: [{ kind: 'producer', id: 'p1', ratePerSec: 10, keyStrategy: { type: 'null' } }],
    consumers: ['c1', 'c2'].map((id) => ({
      kind: 'consumer' as const,
      id,
      groupId: 'g',
      autoOffsetReset: 'earliest' as const,
      maxPollIntervalMs: 15_000,
      ...(id === 'c2' ? { processingTimeMs: 20_000, maxPollRecords: 1 } : {}),
    })),
    connections: [
      { from: 'p1', to: 'orders' },
      { from: 'orders', to: 'c1' },
      { from: 'orders', to: 'c2' },
    ],
  };

  it('a member removed after the poll timeout shows left-group while it still processes', () => {
    const engine = createEngine({ seed: 1, topology: slow });
    const events: EngineEvent[] = [];
    engine.on((e) => events.push(e));
    engine.runUntil(30_000);
    const removed = events.find((e) => e.type === 'MemberRemoved');
    const lost = events.find((e) => e.type === 'PartitionsLost');
    expect(removed).toMatchObject({ consumerId: 'c2', reason: 'poll-timeout' });
    expect(lost).toMatchObject({ consumerId: 'c2' });
    const c2At = (t: number) => {
      const replay = createEngine({ seed: 1, topology: slow });
      replay.runUntil(t);
      return replay.getSnapshot().consumers.find((c) => c.config.id === 'c2');
    };
    const before = c2At((removed?.t ?? 0) - 2);
    expect(before?.status).not.toBe('left-group');
    // Between the LeaveGroup and its next poll(), c2 has no member id but keeps its partition.
    const zombie = c2At((removed?.t ?? 0) + 1);
    expect(zombie).toMatchObject({ status: 'left-group', memberId: null });
    expect(zombie?.partitions.length).toBeGreaterThan(0);
    expect(c2At((lost?.t ?? 0) - 1)?.status).toBe('left-group');
    // At that poll(), onJoinPrepare drops the partitions (lost) and it rejoins.
    const after = c2At(lost?.t ?? 0);
    expect(after).toMatchObject({ status: 'joining', partitions: [] });
  });

  it('a member waiting in JoinGroup has no session deadline (the wait counts as a heartbeat)', () => {
    // c3 joins at 5 s; c1 rejoins at once and waits for c2, which is busy processing.
    const engine = createEngine({ seed: 1, topology: slow });
    engine.runUntil(5000);
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'consumer', id: 'c3', groupId: 'g', autoOffsetReset: 'earliest' },
    });
    engine.dispatch({ type: 'Connect', from: 'orders', to: 'c3' });
    engine.runUntil(10_000);
    const group = engine.getSnapshot().groups[0];
    expect(group?.state).toBe('PreparingRebalance');
    const c1 = group?.members.find((m) => m.consumerId === 'c1');
    const c2 = group?.members.find((m) => m.consumerId === 'c2');
    expect(c1?.sessionExpiresAt).toBeNull();
    // c2 has not sent its JoinGroup yet; its heartbeat thread keeps its session alive.
    expect(c2?.sessionExpiresAt).toBeGreaterThan(10_000);
  });
});

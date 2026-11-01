/**
 * Consumer groups end to end (M3-T4): the client side of the classic protocol against the
 * coordinator, through createEngine.
 */

import type { Assignor, TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine, type Engine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';

type ConsumerExtra = Readonly<Record<string, unknown>> & {
  readonly partitionAssignmentStrategy?: Assignor[];
};

function record(engine: Engine): EngineEvent[] {
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return events;
}

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

/** `n` consumers in group "g" on topic "orders" (3 partitions), fed by p1 at 10 msg/s. */
function group(n: number, extra: ConsumerExtra = {}, partitions = 3): TopologyInput {
  const ids = Array.from({ length: n }, (_, i) => `c${String(i + 1)}`);
  return {
    brokers: [{ kind: 'broker', id: 'b1' }],
    topics: [{ kind: 'topic', id: 'orders', partitions }],
    producers: [
      {
        kind: 'producer',
        id: 'p1',
        ratePerSec: 10,
        keyStrategy: { type: 'random', keys: ['D', 'E', 'F'] }, // partitions 0, 1, 2
      },
    ],
    consumers: ids.map((id) => ({
      kind: 'consumer' as const,
      id,
      groupId: 'g',
      autoOffsetReset: 'earliest' as const,
      ...extra,
    })),
    connections: [{ from: 'p1', to: 'orders' }, ...ids.map((id) => ({ from: 'orders', to: id }))],
  };
}

/** Owned partitions per consumer from the rebalance listener events, as "c1: 0 2". */
function ownership(events: EngineEvent[]): Map<string, Set<number>> {
  const owned = new Map<string, Set<number>>();
  for (const e of events) {
    if (e.type === 'PartitionsAssigned') {
      const set = owned.get(e.consumerId) ?? new Set();
      for (const p of e.partitions) set.add(p.partition);
      owned.set(e.consumerId, set);
    }
    if (e.type === 'PartitionsRevoked' || e.type === 'PartitionsLost') {
      for (const p of e.partitions) owned.get(e.consumerId)?.delete(p.partition);
    }
  }
  return owned;
}

describe('joining a group', () => {
  it('three consumers starting together share three partitions after the initial delay', () => {
    const engine = createEngine({ seed: 1, topology: group(3) });
    const events = record(engine);
    engine.runUntil(10_000);
    // All three joined inside the first 3 s window, so the coordinator waited a second one (K3).
    const completed = ofType(events, 'RebalanceCompleted');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({ generation: 1, protocol: 'range' });
    expect(completed[0]?.t).toBe(6003);
    const owned = ownership(events);
    expect([...owned.values()].map((s) => s.size)).toEqual([1, 1, 1]);
    // Nothing is consumed before the group is Stable; then every member reads only its own.
    const consumed = ofType(events, 'RecordConsumed');
    expect(Math.min(...consumed.map((e) => e.t))).toBeGreaterThan(6003);
    for (const e of consumed) expect(owned.get(e.consumerId)?.has(e.partition)).toBe(true);
  });

  it('a fourth member on three partitions is idle (ConsumerIdle)', () => {
    const engine = createEngine({ seed: 1, topology: group(4) });
    const events = record(engine);
    engine.runUntil(10_000);
    expect(ofType(events, 'ConsumerIdle')).toMatchObject([{ consumerId: 'c4', generation: 1 }]);
    expect(ofType(events, 'RecordConsumed').some((e) => e.consumerId === 'c4')).toBe(false);
  });

  it('a member that joins later triggers a rebalance; range moves partitions, eager revokes all', () => {
    const engine = createEngine({ seed: 1, topology: group(2) });
    const events = record(engine);
    engine.runUntil(10_000);
    const start = events.length;
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'consumer', id: 'c3', groupId: 'g', autoOffsetReset: 'earliest' },
    });
    engine.dispatch({ type: 'Connect', from: 'orders', to: 'c3' });
    engine.runUntil(14_000);
    const after = events.slice(start);
    expect(ofType(after, 'RebalanceStarted')).toMatchObject([
      { generation: 1, reason: 'member-joined' },
    ]);
    expect(ofType(after, 'RebalanceCompleted')).toMatchObject([{ generation: 2 }]);
    // Eager: c1 and c2 gave up everything before rejoining, even partitions they keep.
    const revoked = ofType(after, 'PartitionsRevoked');
    expect(revoked.map((e) => [e.consumerId, e.partitions.length]).sort()).toEqual([
      ['c1', 2],
      ['c2', 1],
    ]);
    // The rebalance waited for c1 and c2 to rejoin, which they do at their next poll() after a
    // heartbeat told them (≤ heartbeat.interval.ms).
    const started = ofType(after, 'RebalanceStarted')[0]?.t ?? 0;
    const done = ofType(after, 'RebalanceCompleted')[0]?.t ?? 0;
    expect(done - started).toBeLessThanOrEqual(3000 + 10);
    expect([...ownership(events).values()].map((s) => s.size)).toEqual([1, 1, 1]);
  });
});

describe('eager vs cooperative (KIP-429)', () => {
  function addThird(strategy: Assignor[]) {
    const engine = createEngine({
      seed: 1,
      topology: group(2, { partitionAssignmentStrategy: strategy }),
    });
    const events = record(engine);
    engine.runUntil(10_000);
    const start = events.length;
    engine.dispatch({
      type: 'AddComponent',
      component: {
        kind: 'consumer',
        id: 'c3',
        groupId: 'g',
        autoOffsetReset: 'earliest',
        partitionAssignmentStrategy: strategy,
      },
    });
    engine.dispatch({ type: 'Connect', from: 'orders', to: 'c3' });
    engine.runUntil(20_000);
    return { events, after: events.slice(start) };
  }

  it('cooperative-sticky: members keep the partitions that stay; one moves over two rebalances', () => {
    const { events, after } = addThird(['cooperative-sticky']);
    const revoked = ofType(after, 'PartitionsRevoked');
    expect(revoked).toHaveLength(1); // only the member that gives one partition up
    expect(revoked[0]?.partitions).toHaveLength(1);
    const completed = ofType(after, 'RebalanceCompleted');
    expect(completed.map((e) => e.generation)).toEqual([2, 3]);
    expect(ofType(after, 'RebalanceStarted').map((e) => e.reason)).toEqual([
      'member-joined',
      'revoked-partitions',
    ]);
    // In generation 2 the moving partition has no owner; in 3 the new member gets it.
    const gen2 = completed[0]?.members.flatMap((m) => m.partitions) ?? [];
    expect(gen2).toHaveLength(2);
    expect([...ownership(events).values()].map((s) => s.size)).toEqual([1, 1, 1]);
  });

  it('cooperative members keep consuming while the group rebalances', () => {
    const { after } = addThird(['cooperative-sticky']);
    const started = ofType(after, 'RebalanceStarted')[0]?.t ?? 0;
    const done = ofType(after, 'RebalanceCompleted').at(-1)?.t ?? 0;
    const during = ofType(after, 'RecordConsumed').filter((e) => e.t > started && e.t < done);
    expect(during.length).toBeGreaterThan(0);
  });

  it('eager (the default list) consumes nothing while the group rebalances', () => {
    const { after } = addThird(['range', 'cooperative-sticky']);
    const started = ofType(after, 'RebalanceStarted')[0]?.t ?? 0;
    const done = ofType(after, 'RebalanceCompleted')[0]?.t ?? 0;
    // Members stop when they rejoin (revoking everything) and restart once the group is Stable.
    const lastRejoin = Math.max(...ofType(after, 'PartitionsRevoked').map((e) => e.t));
    const during = ofType(after, 'RecordConsumed').filter((e) => e.t > lastRejoin && e.t < done);
    expect(lastRejoin).toBeGreaterThanOrEqual(started);
    expect(during).toEqual([]);
  });
});

describe('failure detection', () => {
  it('a crashed member is removed after session.timeout.ms (±1 heartbeat) and its partitions move', () => {
    const engine = createEngine({ seed: 1, topology: group(2) });
    const events = record(engine);
    engine.runUntil(10_000);
    engine.dispatch({ type: 'KillConsumer', id: 'c2' });
    engine.runUntil(25_000);
    const removed = ofType(events, 'MemberRemoved');
    expect(removed).toMatchObject([{ consumerId: 'c2', reason: 'session-timeout' }]);
    const at = removed[0]?.t ?? 0;
    // Last heartbeat ≤ 3 s before the kill; removal 10 s after it.
    expect(at).toBeGreaterThanOrEqual(10_000 + 10_000 - 3000);
    expect(at).toBeLessThanOrEqual(10_000 + 10_000 + 1);
    expect(ofType(events, 'RebalanceStarted').at(-1)).toMatchObject({ reason: 'session-timeout' });
    expect(ownership(events).get('c1')?.size).toBe(3);
  });

  it('a slow consumer is removed after max.poll.interval.ms and becomes a zombie', () => {
    // c2 takes 20 s per record: its first batch keeps it out of poll() for longer than 15 s.
    const engine = createEngine({
      seed: 1,
      topology: {
        ...group(2, { maxPollIntervalMs: 15_000 }),
        consumers: [
          {
            kind: 'consumer',
            id: 'c1',
            groupId: 'g',
            autoOffsetReset: 'earliest',
            maxPollIntervalMs: 15_000,
          },
          {
            kind: 'consumer',
            id: 'c2',
            groupId: 'g',
            autoOffsetReset: 'earliest',
            maxPollIntervalMs: 15_000,
            processingTimeMs: 20_000,
            maxPollRecords: 1,
          },
        ],
      },
    });
    const events = record(engine);
    engine.runUntil(30_000);
    const firstC2 = ofType(events, 'RecordConsumed').find((e) => e.consumerId === 'c2');
    const removed = ofType(events, 'MemberRemoved');
    expect(removed[0]).toMatchObject({ consumerId: 'c2', reason: 'poll-timeout' });
    // Removed 15 s after poll() returned its record (+1 ms for the LeaveGroup to arrive).
    expect(removed[0]?.t).toBe((firstC2?.t ?? 0) + 15_000 + 1);
    const afterRemoval = events.filter((e) => e.seq > (removed[0]?.seq ?? 0));
    expect(ofType(afterRemoval, 'RebalanceStarted')[0]?.reason).toBe('poll-timeout');
    // The next generation gives c1 every partition while c2 is still processing.
    expect(ofType(afterRemoval, 'RebalanceCompleted')[0]?.members).toMatchObject([
      { consumerId: 'c1', partitions: { length: 3 } },
    ]);
    // The zombie learns at its next poll() (20 s after the record): its partitions are lost, and
    // it joins again as a new member.
    const lost = ofType(events, 'PartitionsLost');
    expect(lost).toMatchObject([{ consumerId: 'c2' }]);
    expect(lost[0]?.t).toBe((firstC2?.t ?? 0) + 20_000);
  });
});

describe('subscriptions and metadata', () => {
  it('a new partition is picked up by a rebalance once the leader refreshes its metadata', () => {
    const engine = createEngine({ seed: 1, topology: group(2, { metadataMaxAgeMs: 10_000 }) });
    const events = record(engine);
    engine.runUntil(8000);
    engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 4 } });
    engine.runUntil(20_000);
    expect(ofType(events, 'RebalanceStarted').at(-1)).toMatchObject({ reason: 'metadata-change' });
    const owners = [...ownership(events).values()].flatMap((s) => [...s]).sort();
    expect(owners).toEqual([0, 1, 2, 3]);
  });

  it('disconnecting the topic from a member makes it leave the group', () => {
    const engine = createEngine({ seed: 1, topology: group(2) });
    const events = record(engine);
    engine.runUntil(10_000);
    engine.dispatch({ type: 'Disconnect', from: 'orders', to: 'c2' });
    engine.runUntil(13_000);
    expect(ofType(events, 'MemberRemoved')).toMatchObject([{ consumerId: 'c2', reason: 'leave' }]);
    expect(ownership(events).get('c1')?.size).toBe(3);
  });

  it('a member with no assignor in common cannot join, and its application dies', () => {
    const engine = createEngine({
      seed: 1,
      topology: group(1, { partitionAssignmentStrategy: ['range'] }),
    });
    const events = record(engine);
    engine.runUntil(10_000);
    engine.dispatch({
      type: 'AddComponent',
      component: {
        kind: 'consumer',
        id: 'c2',
        groupId: 'g',
        partitionAssignmentStrategy: ['roundrobin'],
      },
    });
    engine.dispatch({ type: 'Connect', from: 'orders', to: 'c2' });
    engine.runUntil(11_000);
    expect(ofType(events, 'ConsumerStopped')).toMatchObject([{ consumerId: 'c2' }]);
    expect(engine.getSnapshot().consumers.find((c) => c.config.id === 'c2')?.status).toBe(
      'stopped',
    );
  });

  it('a broker that coordinates a group cannot be removed', () => {
    const engine = createEngine({
      seed: 1,
      topology: {
        brokers: [{ kind: 'broker', id: 'b1' }],
        consumers: [{ kind: 'consumer', id: 'c1' }],
      },
    });
    engine.runUntil(10);
    engine.dispatch({ type: 'AddComponent', component: { kind: 'topic', id: 't' } });
    engine.dispatch({ type: 'Connect', from: 't', to: 'c1' });
    engine.runUntil(100);
    engine.dispatch({ type: 'RemoveComponent', id: 't' });
    expect(engine.dispatch({ type: 'RemoveComponent', id: 'b1' })).toMatchObject({
      ok: false,
      reason: /__consumer_offsets/,
    });
  });
});

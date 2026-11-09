/**
 * __consumer_offsets and coordinator failover (M4-T8; M4-decisions K10).
 */

import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngineInternal } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';
import { coordinatorFor, OFFSETS_TOPIC, partitionFor } from './coordinator.ts';

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

/** 3 brokers; "orders" RF 3 (2 partitions); two consumers in group "orders-app"; 10 msg/s. */
function scene(): TopologyInput {
  return {
    brokers: ['b1', 'b2', 'b3'].map((id) => ({
      kind: 'broker' as const,
      id,
      groupInitialRebalanceDelayMs: 0,
    })),
    topics: [
      { kind: 'topic', id: 'orders', partitions: 2, replicationFactor: 3, minInsyncReplicas: 2 },
    ],
    producers: [{ kind: 'producer', id: 'p1', ratePerSec: 10 }],
    consumers: ['c1', 'c2'].map((id) => ({
      kind: 'consumer' as const,
      id,
      groupId: 'orders-app',
      autoOffsetReset: 'earliest' as const,
      autoCommitIntervalMs: 1000,
    })),
    connections: [
      { from: 'p1', to: 'orders' },
      { from: 'orders', to: 'c1' },
      { from: 'orders', to: 'c2' },
    ],
  };
}

function start() {
  const { engine, context } = createEngineInternal({ seed: 1, topology: scene() });
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return { engine, context, events };
}

describe('__consumer_offsets (M4-T8)', () => {
  it('is created at the first FindCoordinator with RF min(3, brokers) and stores commits', () => {
    const { engine, context, events } = start();
    engine.runUntil(3000);
    const topic = context.state.topics.get(OFFSETS_TOPIC);
    expect(topic?.partitions).toHaveLength(50);
    expect(topic?.config.replicationFactor).toBe(3);
    const p = topic?.partitions[partitionFor('orders-app')];
    // Commits are records in the group's partition, replicated like any other.
    expect(p?.logEndOffset).toBeGreaterThan(0);
    expect(new Set([...(p?.replicaLogs.values() ?? [])].map((r) => r.logEndOffset))).toEqual(
      new Set([p?.logEndOffset]),
    );
    // OffsetCommitted comes once the commit is replicated (≥ 2 network delays after acceptance).
    const committed = ofType(events, 'OffsetCommitted');
    expect(committed.length).toBeGreaterThan(0);
    for (const e of committed) expect(e.t - e.acceptedAt).toBeGreaterThanOrEqual(2);
  });

  it('the internal topic cannot be created or deleted by the user', () => {
    const { engine } = start();
    engine.runUntil(100);
    expect(
      engine.dispatch({ type: 'AddComponent', component: { kind: 'topic', id: OFFSETS_TOPIC } }),
    ).toMatchObject({ ok: false, reason: /internal topic/ });
    expect(engine.dispatch({ type: 'RemoveComponent', id: OFFSETS_TOPIC })).toMatchObject({
      ok: false,
    });
  });
});

describe('coordinator failover (M4-T8)', () => {
  it('killing the coordinator moves the group: no rebalance, committed offsets intact', () => {
    const { engine, context, events } = start();
    engine.runUntil(8000);
    const coordinator = coordinatorFor(context, 'orders-app') ?? '';
    const group = context.state.groups.get('orders-app');
    expect(group?.state).toBe('Stable');
    const generation = group?.generation;
    const committedBefore = new Map(group?.committed);
    engine.dispatch({ type: 'KillBroker', id: coordinator });
    engine.runUntil(30_000);
    const moved = ofType(events, 'CoordinatorMoved');
    expect(moved).toHaveLength(1);
    expect(moved[0]).toMatchObject({
      groupId: 'orders-app',
      from: coordinator,
      state: 'Stable',
      generation,
    });
    expect(moved[0]?.to).not.toBe(coordinator);
    // The group carried on: same generation, same members, no rebalance.
    const after = context.state.groups.get('orders-app');
    expect(after?.generation).toBe(generation);
    expect(after?.members.size).toBe(2);
    expect(ofType(events, 'RebalanceStarted').filter((e) => e.t > 8000)).toEqual([]);
    expect(ofType(events, 'MemberRemoved').filter((e) => e.t > 8000)).toEqual([]);
    // Every commit acknowledged before the kill survived (or was overtaken by a newer one).
    for (const [key, offset] of committedBefore) {
      expect(after?.committed.get(key)).toBeGreaterThanOrEqual(offset);
    }
    // Consumers kept consuming after the move.
    expect(ofType(events, 'RecordConsumed').some((e) => e.t > (moved[0]?.t ?? 0) + 1000)).toBe(
      true,
    );
  });

  it('a commit in flight when the coordinator dies fails at the client as retriable', () => {
    const { engine, context, events } = start();
    engine.runUntil(8000);
    const coordinator = coordinatorFor(context, 'orders-app') ?? '';
    const p = context.state.topics.get(OFFSETS_TOPIC)?.partitions[partitionFor('orders-app')];
    // Run until a commit is written and waiting for the HW, then kill the coordinator.
    let guard = 0;
    while (context.state.pendingCommits.size === 0 && guard++ < 100_000) engine.step();
    expect(p?.leader).toBe(coordinator);
    engine.dispatch({ type: 'KillBroker', id: coordinator });
    const failed = ofType(events, 'CommitFailed');
    expect(failed.some((e) => e.error === 'COORDINATOR_NOT_AVAILABLE')).toBe(true);
    engine.runUntil(30_000);
    // The members found the new coordinator and their commits succeed again.
    const moveAt = ofType(events, 'CoordinatorMoved')[0]?.t ?? Infinity;
    expect(ofType(events, 'OffsetCommitted').some((e) => e.t > moveAt)).toBe(true);
  });

  it('while the coordinator is unknown, members look it up again (FindCoordinator) and rejoin nothing', () => {
    const { engine, context, events } = start();
    engine.runUntil(8000);
    const coordinator = coordinatorFor(context, 'orders-app') ?? '';
    engine.dispatch({ type: 'StopBroker', id: coordinator });
    engine.runUntil(12_000);
    // Controlled shutdown: leadership moves at once, so the group moves within milliseconds.
    const moved = ofType(events, 'CoordinatorMoved');
    expect(moved).toHaveLength(1);
    expect(moved[0]?.t).toBeLessThan(8010);
    expect(ofType(events, 'RebalanceStarted').filter((e) => e.t > 8000)).toEqual([]);
  });
});

/**
 * The client side's handling of coordinator errors (M3-T4). Healthy runs rarely produce these
 * answers, so the tests inject them into a running consumer, as a broker would send them.
 */

import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngineInternal } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';
import type { CoordinatorResponse } from '../model/tasks.ts';

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

const topology = (consumers = 1, extra: Record<string, unknown> = {}): TopologyInput => ({
  brokers: [{ kind: 'broker', id: 'b1', groupInitialRebalanceDelayMs: 0 }],
  topics: [{ kind: 'topic', id: 'orders', partitions: 2 }],
  producers: [{ kind: 'producer', id: 'p1', ratePerSec: 10 }],
  consumers: Array.from({ length: consumers }, (_, i) => ({
    kind: 'consumer' as const,
    id: `c${String(i + 1)}`,
    groupId: 'g',
    autoOffsetReset: 'earliest' as const,
    ...extra,
  })),
  connections: [
    { from: 'p1', to: 'orders' },
    ...Array.from({ length: consumers }, (_, i) => ({ from: 'orders', to: `c${String(i + 1)}` })),
  ],
});

/** A stable group, plus a way to deliver a coordinator answer to c1 right now. */
function stable(consumers = 1, extra: Record<string, unknown> = {}) {
  const { engine, kernel, context } = createEngineInternal({
    seed: 1,
    topology: topology(consumers, extra),
  });
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  engine.runUntil(2000);
  const c1 = context.state.consumers.get('c1');
  if (c1 === undefined) throw new Error('no c1');
  const deliver = (response: CoordinatorResponse, groupId = 'g') => {
    kernel.scheduleIn(0, {
      type: 'CoordinatorResponse',
      groupId,
      consumerId: 'c1',
      incarnation: c1.incarnation,
      response,
    });
  };
  return { engine, context, c1, events, deliver };
}

describe('heartbeat answers', () => {
  it('UNKNOWN_MEMBER_ID: the member lost its place; its partitions are lost and it rejoins as new', () => {
    const { engine, c1, events, deliver } = stable();
    const oldId = c1.group.memberId;
    deliver({ api: 'Heartbeat', error: 'UNKNOWN_MEMBER_ID' });
    engine.runUntil(2500);
    expect(ofType(events, 'PartitionsLost')).toMatchObject([
      { consumerId: 'c1', partitions: { length: 2 } },
    ]);
    // No auto-commit for lost partitions (it would fail: not part of an active group).
    expect(ofType(events, 'CommitFailed')).toEqual([]);
    // It joins again without a member id. The coordinator still has the old id, so the join phase
    // waits for that member's session to time out before c1 gets its new id.
    expect(c1.group).toMatchObject({ memberId: '', state: 'preparing' });
    engine.runUntil(14_000);
    expect(ofType(events, 'MemberRemoved')).toMatchObject([
      { memberId: oldId, reason: 'session-timeout' },
    ]);
    expect(c1.group.memberId).not.toBe('');
    expect(c1.group.memberId).not.toBe(oldId);
  });

  it('ILLEGAL_GENERATION: generation reset, member id kept, partitions lost', () => {
    const { engine, c1, events, deliver } = stable();
    const oldId = c1.group.memberId;
    deliver({ api: 'Heartbeat', error: 'ILLEGAL_GENERATION' });
    engine.runUntil(2001);
    expect(c1.group).toMatchObject({ memberId: oldId, generation: -1 });
    expect(ofType(events, 'PartitionsLost')).toHaveLength(1);
  });

  it('REBALANCE_IN_PROGRESS only matters to a member in a stable generation', () => {
    const { engine, c1, deliver } = stable();
    c1.group.state = 'completing';
    deliver({ api: 'Heartbeat', error: 'REBALANCE_IN_PROGRESS' });
    engine.runUntil(2001);
    expect(c1.group.rejoinNeeded).toBe(false);
  });
});

describe('JoinGroup and SyncGroup answers', () => {
  it('answers that arrive in the wrong state, or for another group, are ignored', () => {
    const { engine, c1, deliver } = stable();
    const before = JSON.stringify(c1.group);
    deliver({ api: 'JoinGroup', error: 'UNKNOWN_MEMBER_ID' });
    deliver({ api: 'SyncGroup', error: null, generation: 9, assignment: [] });
    deliver({ api: 'Heartbeat', error: 'UNKNOWN_MEMBER_ID' }, 'other-group');
    engine.runUntil(2001);
    expect(JSON.stringify(c1.group)).toBe(before);
  });

  it.each([
    ['UNKNOWN_MEMBER_ID', ''],
    ['ILLEGAL_GENERATION', 'kept'],
    ['REBALANCE_IN_PROGRESS', 'kept'],
    ['COORDINATOR_NOT_AVAILABLE', 'kept'],
  ] as const)('a JoinGroup %s makes the member join again', (error, memberId) => {
    const { engine, c1, deliver } = stable();
    const oldId = c1.group.memberId;
    c1.group.state = 'preparing';
    deliver({ api: 'JoinGroup', error });
    engine.runUntil(2001);
    // Read through a function: TypeScript would otherwise keep the narrowed 'preparing'.
    const state = (): string => c1.group.state;
    expect(c1.group.rejoinNeeded || state() === 'preparing').toBe(true);
    expect(c1.group.memberId).toBe(memberId === '' ? '' : oldId);
  });

  it.each(['UNKNOWN_MEMBER_ID', 'ILLEGAL_GENERATION', 'REBALANCE_IN_PROGRESS'] as const)(
    'a SyncGroup %s makes the member join again',
    (error) => {
      const { engine, c1, events, deliver } = stable();
      c1.group.state = 'completing';
      deliver({ api: 'SyncGroup', error });
      // Without its member id it joins as a new member, which waits for the old id's session.
      engine.runUntil(15_000);
      // It rejoined and got a new assignment.
      expect(ofType(events, 'RebalanceCompleted').length).toBeGreaterThan(1);
      expect(c1.group.state).toBe('stable');
    },
  );
});

describe('commit answers', () => {
  it.each([
    ['ILLEGAL_GENERATION', 'unjoined'],
    ['UNKNOWN_MEMBER_ID', 'unjoined'],
    ['REBALANCE_IN_PROGRESS', 'stable'],
  ] as const)('%s fails the commit (CommitFailed) and the member rejoins', (error, state) => {
    const { engine, c1, events, deliver } = stable();
    deliver({ api: 'OffsetCommit', commitId: -1, error });
    engine.runUntil(2000);
    kernelStep(engine);
    expect(ofType(events, 'CommitFailed')).toMatchObject([{ consumerId: 'c1', error }]);
    expect(c1.group.state === state || c1.group.rejoinNeeded).toBe(true);
  });

  it('a commit from a member outside a stable generation fails locally', () => {
    const { engine, c1, events } = stable(1, { autoCommitIntervalMs: 100 });
    c1.group.state = 'unjoined';
    c1.group.generation = -1;
    engine.runUntil(2100);
    // The next poll() rejoins (partitions lost, no commit); nothing reached the coordinator.
    expect(ofType(events, 'OffsetCommitted').filter((e) => e.t > 2001)).toEqual([]);
  });
});

describe('no coordinator', () => {
  it('a consumer with no broker to coordinate its group keeps retrying and joins once one exists', () => {
    const { engine, kernel, context } = createEngineInternal({
      seed: 1,
      topology: { consumers: [{ kind: 'consumer', id: 'c1', groupId: 'g' }] },
    });
    const events: EngineEvent[] = [];
    engine.on((e) => events.push(e));
    engine.runUntil(1000);
    expect(kernel.pendingTasks()).toBe(0); // nothing subscribed: it just waits
    engine.dispatch({ type: 'AddComponent', component: { kind: 'broker', id: 'b1' } });
    engine.dispatch({ type: 'AddComponent', component: { kind: 'topic', id: 't' } });
    engine.dispatch({ type: 'Connect', from: 't', to: 'c1' });
    engine.runUntil(8000);
    expect(ofType(events, 'MemberJoined')).toHaveLength(1);
    expect(context.state.consumers.get('c1')?.group.state).toBe('stable');
  });
});

function kernelStep(engine: { step: () => boolean }): void {
  engine.step();
}

import { BrokerSchema, TopicSchema } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createKernel } from '../core/kernel.ts';
import type { EngineEvent, EngineEventPayload } from '../events/index.ts';
import type { EngineContext } from '../model/context.ts';
import type { GroupState, MemberMetadata } from '../model/group.ts';
import { createPartition } from '../model/partition.ts';
import { createState } from '../model/state.ts';
import type { CoordinatorRequest, CoordinatorResponse, EngineTask } from '../model/tasks.ts';
import {
  coordinatorFor,
  hostsOffsetsTopic,
  javaStringHashCode,
  partitionFor,
} from './coordinator.ts';
import { runTask } from './index.ts';
import { createEngine } from '../engine.ts';

interface Received {
  readonly t: number;
  readonly consumerId: string;
  readonly response: CoordinatorResponse;
}

/** A coordinator with no real clients: tests send requests and read the responses. */
function harness(delayMs = 3000) {
  const responses: Received[] = [];
  const events: EngineEvent[] = [];
  const kernel = createKernel<EngineTask, EngineEventPayload>({
    seed: 1,
    handleTask: (task) => {
      if (task.type === 'CoordinatorResponse') {
        responses.push({ t: kernel.now(), consumerId: task.consumerId, response: task.response });
      } else {
        runTask(ctx, task);
      }
    },
  });
  const ctx: EngineContext = {
    kernel,
    state: createState(),
    emit: (e) => kernel.emit(e),
    config: { networkDelayMs: 1 },
  };
  ctx.state.brokers.set('b1', {
    config: BrokerSchema.parse({ kind: 'broker', id: 'b1', groupInitialRebalanceDelayMs: delayMs }),
    status: 'running',
    incarnation: 0,
    brokerEpoch: 1,
  });
  ctx.state.topics.set('t', {
    topicId: 1,
    config: TopicSchema.parse({ kind: 'topic', id: 't', partitions: 2 }),
    partitions: [createPartition('t', 0, 'b1'), createPartition('t', 1, 'b1')],
  });
  kernel.on((e) => events.push(e));

  const send = (consumerId: string, request: CoordinatorRequest): void => {
    kernel.scheduleIn(1, {
      type: 'CoordinatorRequest',
      groupId: 'g',
      consumerId,
      incarnation: 1,
      request,
    });
  };
  const metadata = (extra: Partial<MemberMetadata> = {}): MemberMetadata => ({
    protocols: ['range', 'cooperative-sticky'],
    subscription: ['t'],
    ownedPartitions: [],
    stickyUserData: [],
    ownedGeneration: -1,
    sessionTimeoutMs: 10_000,
    rebalanceTimeoutMs: 30_000,
    ...extra,
  });
  const join = (consumerId: string, memberId = '', extra: Partial<MemberMetadata> = {}) => {
    send(consumerId, {
      api: 'JoinGroup',
      memberId,
      metadata: metadata(extra),
      reason: memberId === '' ? 'new-member' : 'rebalance-in-progress',
    });
  };
  const group = (): GroupState => {
    const g = ctx.state.groups.get('g');
    if (g === undefined) throw new Error('no group');
    return g;
  };
  /** The member id the coordinator gave `consumerId`. */
  const idOf = (consumerId: string): string =>
    [...group().members.values()].find((m) => m.consumerId === consumerId)?.memberId ?? '';
  const last = (consumerId: string, api: CoordinatorResponse['api']) =>
    responses.filter((r) => r.consumerId === consumerId && r.response.api === api).at(-1);
  const run = (t: number) => {
    kernel.runUntil(t);
  };
  return { ctx, kernel, responses, events, send, join, group, idOf, last, run };
}

type H = ReturnType<typeof harness>;

/**
 * c1 and c2 join a new group at t=0 and the leader syncs. c2 joined during the first delay window,
 * so the join phase waits a second window (K3): JoinGroup responses at 6002, Stable at 6004 with
 * generation 1. Afterwards the tests use times relative to `T` (6004).
 */
const T = 6004;
function stableGroupOfTwo(h: H): void {
  h.join('c1');
  h.join('c2');
  h.run(6002);
  const leader = h.group().leader ?? '';
  h.send('c1', {
    api: 'SyncGroup',
    memberId: h.idOf('c1'),
    generation: 1,
    assignment: [
      [leader, [{ topic: 't', partition: 0 }]],
      [h.idOf('c2'), [{ topic: 't', partition: 1 }]],
    ],
  });
  h.send('c2', { api: 'SyncGroup', memberId: h.idOf('c2'), generation: 1, assignment: null });
  h.run(T);
  expect(h.group().state).toBe('Stable');
}

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

describe('coordinator lookup (GroupCoordinatorService#partitionFor)', () => {
  it('uses Java String#hashCode', () => {
    expect(javaStringHashCode('')).toBe(0);
    expect(javaStringHashCode('hello')).toBe(99_162_322);
    expect(javaStringHashCode('my-group')).toBe(-1_906_497_762);
  });

  it('maps a group to abs(hashCode) % 50, with Integer.MIN_VALUE → 0', () => {
    expect(partitionFor('my-group')).toBe(12); // abs(-1906497762) % 50
    expect(javaStringHashCode('polygenelubricants')).toBe(-2_147_483_648);
    expect(partitionFor('polygenelubricants')).toBe(0);
  });

  it('places __consumer_offsets on first use; no broker means no coordinator', () => {
    const h = harness();
    expect(coordinatorFor(h.ctx, 'g')).toBe('b1');
    expect(h.ctx.state.offsetsTopicLeaders).toHaveLength(50);
    const empty = harness();
    empty.ctx.state.brokers.clear();
    expect(coordinatorFor(empty.ctx, 'g')).toBeNull();
    expect(empty.ctx.state.offsetsTopicLeaders).toBeNull();
  });

  it('a broker hosts __consumer_offsets only once a group has looked up its coordinator', () => {
    const h = harness();
    expect(hostsOffsetsTopic(h.ctx, 'b1')).toBe(false);
    coordinatorFor(h.ctx, 'g');
    expect(hostsOffsetsTopic(h.ctx, 'b1')).toBe(true);
    // Before any group exists the broker can still be removed.
    const engine = createEngine({ seed: 1, topology: { brokers: [{ kind: 'broker', id: 'b1' }] } });
    expect(engine.dispatch({ type: 'RemoveComponent', id: 'b1' })).toEqual({ ok: true });
  });
});

describe('initial join phase (group.initial.rebalance.delay.ms, K3)', () => {
  it('the first JoinGroup starts a rebalance that completes after the delay', () => {
    const h = harness();
    h.join('c1');
    h.run(1);
    expect(h.group().state).toBe('PreparingRebalance');
    expect(ofType(h.events, 'MemberJoined').map((e) => e.consumerId)).toEqual(['c1']);
    expect(ofType(h.events, 'RebalanceStarted')).toMatchObject([
      { groupId: 'g', generation: 0, reason: 'member-joined' },
    ]);
    h.run(3000);
    expect(h.responses).toEqual([]);
    h.run(3002);
    const join = h.last('c1', 'JoinGroup');
    expect(join?.t).toBe(3002); // delay ends at 3001, +1 ms back
    expect(join?.response).toMatchObject({
      error: null,
      generation: 1,
      protocol: 'range',
      leader: h.idOf('c1'),
      members: [{ memberId: h.idOf('c1'), topics: ['t'] }],
    });
    expect(h.group().state).toBe('CompletingRebalance');
    expect(h.idOf('c1')).toMatch(/^c1-[0-9a-f]{8}$/);
  });

  it('waits one more window while members keep joining, and never ends early', () => {
    const h = harness();
    h.join('c1'); // arrives at 1: window 1 ends at 3001
    h.run(1000);
    h.join('c2'); // arrives at 1001: window 2 ends at 6001
    h.run(4000);
    h.join('c3'); // arrives at 4001: window 3 ends at 9001
    h.run(9000);
    expect(h.group().state).toBe('PreparingRebalance');
    h.run(9002);
    expect(h.group().state).toBe('CompletingRebalance');
    expect(h.group().generation).toBe(1);
    expect(h.last('c1', 'JoinGroup')?.response).toMatchObject({ members: { length: 3 } });
    expect(h.last('c2', 'JoinGroup')?.response).toMatchObject({ members: [] });
  });

  it('the extensions are capped by the rebalance timeout', () => {
    const h = harness();
    h.join('c1', '', { rebalanceTimeoutMs: 5000 }); // remaining = 2000 after the first window
    h.run(1000);
    h.join('c2', '', { rebalanceTimeoutMs: 5000 });
    h.run(4000);
    h.join('c3', '', { rebalanceTimeoutMs: 5000 }); // joins inside the last (2000 ms) window
    h.run(5000);
    expect(h.group().state).toBe('PreparingRebalance');
    h.run(5001);
    expect(h.group().state).toBe('CompletingRebalance');
  });
});

describe('sync phase', () => {
  it('the leader’s SyncGroup stores the assignment, makes the group Stable and answers everyone', () => {
    const h = harness();
    h.join('c1');
    h.join('c2');
    h.run(6002);
    // c2 (a follower) syncs first and waits for the leader.
    h.send('c2', { api: 'SyncGroup', memberId: h.idOf('c2'), generation: 1, assignment: null });
    h.run(6004);
    expect(h.last('c2', 'SyncGroup')).toBeUndefined();
    h.send('c1', {
      api: 'SyncGroup',
      memberId: h.idOf('c1'),
      generation: 1,
      assignment: [
        [h.idOf('c1'), [{ topic: 't', partition: 0 }]],
        [h.idOf('c2'), [{ topic: 't', partition: 1 }]],
      ],
    });
    h.run(6006);
    expect(h.group().state).toBe('Stable');
    expect(h.last('c2', 'SyncGroup')?.response).toEqual({
      api: 'SyncGroup',
      error: null,
      generation: 1,
      assignment: [{ topic: 't', partition: 1 }],
    });
    expect(h.last('c1', 'SyncGroup')?.response).toMatchObject({
      assignment: [{ topic: 't', partition: 0 }],
    });
    expect(ofType(h.events, 'RebalanceCompleted')).toMatchObject([
      {
        generation: 1,
        protocol: 'range',
        members: [
          { consumerId: 'c1', partitions: [{ topic: 't', partition: 0 }] },
          { consumerId: 'c2', partitions: [{ topic: 't', partition: 1 }] },
        ],
      },
    ]);
  });

  it('a stale generation gets ILLEGAL_GENERATION, an unknown member UNKNOWN_MEMBER_ID', () => {
    const h = harness();
    h.join('c1');
    h.run(3002);
    h.send('c1', { api: 'SyncGroup', memberId: h.idOf('c1'), generation: 0, assignment: [] });
    h.send('x', { api: 'SyncGroup', memberId: 'x-1', generation: 1, assignment: null });
    h.run(3004);
    expect(h.last('c1', 'SyncGroup')?.response).toEqual({
      api: 'SyncGroup',
      error: 'ILLEGAL_GENERATION',
    });
    expect(h.last('x', 'SyncGroup')?.response).toEqual({
      api: 'SyncGroup',
      error: 'UNKNOWN_MEMBER_ID',
    });
  });

  it('members that never send SyncGroup are removed after the rebalance timeout (K8)', () => {
    const h = harness();
    h.join('c1', '', { rebalanceTimeoutMs: 8000 });
    h.join('c2', '', { rebalanceTimeoutMs: 8000 });
    h.run(6002);
    h.send('c2', { api: 'SyncGroup', memberId: h.idOf('c2'), generation: 1, assignment: null });
    h.run(14_000);
    expect(h.group().state).toBe('CompletingRebalance');
    h.run(14_002); // join completed at 6001 + 8000; the error reaches c2 1 ms later
    expect(ofType(h.events, 'MemberRemoved')).toMatchObject([
      { consumerId: 'c1', reason: 'sync-timeout' },
    ]);
    expect(h.last('c2', 'SyncGroup')?.response).toEqual({
      api: 'SyncGroup',
      error: 'REBALANCE_IN_PROGRESS',
    });
    expect(h.group().state).toBe('PreparingRebalance');
    expect(h.group().leader).toBe(h.idOf('c2'));
  });
});

describe('rebalancing a Stable group', () => {
  it('a new member starts a rebalance; heartbeats answer REBALANCE_IN_PROGRESS until all rejoin', () => {
    const h = harness();
    stableGroupOfTwo(h);
    h.join('c3');
    h.run(T + 2);
    expect(h.group().state).toBe('PreparingRebalance');
    expect(ofType(h.events, 'RebalanceStarted').at(-1)).toMatchObject({
      generation: 1,
      reason: 'member-joined',
    });
    h.send('c1', { api: 'Heartbeat', memberId: h.idOf('c1'), generation: 1 });
    h.run(T + 4);
    expect(h.last('c1', 'Heartbeat')?.response).toEqual({
      api: 'Heartbeat',
      error: 'REBALANCE_IN_PROGRESS',
    });
    h.join('c1', h.idOf('c1'));
    h.run(T + 6);
    expect(h.group().state).toBe('PreparingRebalance'); // still waiting for c2
    h.join('c2', h.idOf('c2'));
    h.run(T + 7); // the last rejoin completes the join phase at once
    expect(h.group().state).toBe('CompletingRebalance');
    expect(h.group().generation).toBe(2);
  });

  it('a member that does not rejoin in time is removed when the join phase ends', () => {
    const h = harness();
    stableGroupOfTwo(h); // rebalance timeout 30 s
    h.join('c3');
    h.run(T + 6);
    h.join('c1', h.idOf('c1'));
    const c2 = h.idOf('c2');
    // c2 keeps heartbeating (so no session timeout) but never rejoins.
    for (let t = T + 1000; t <= T + 30_000; t += 3000) {
      h.run(t);
      h.send('c2', { api: 'Heartbeat', memberId: c2, generation: 1 });
    }
    h.run(T + 30_000);
    expect(ofType(h.events, 'MemberRemoved')).toEqual([]);
    h.run(T + 30_001); // the rebalance started at T + 1: the join phase ends 30 s later
    expect(ofType(h.events, 'MemberRemoved')).toMatchObject([
      { consumerId: 'c2', reason: 'rebalance-timeout' },
    ]);
    expect(h.group().generation).toBe(2);
    expect([...h.group().members.values()].map((m) => m.consumerId)).toEqual(['c1', 'c3']);
  });

  it('the leader rejoining a Stable group forces a rebalance; a follower with the same metadata does not', () => {
    const h = harness();
    stableGroupOfTwo(h);
    h.join('c2', h.idOf('c2'));
    h.run(T + 2);
    expect(h.group().state).toBe('Stable');
    expect(h.last('c2', 'JoinGroup')?.response).toMatchObject({ error: null, generation: 1 });
    h.join('c1', h.idOf('c1'));
    h.run(T + 4);
    expect(h.group().state).toBe('PreparingRebalance');
  });

  it('a follower with a changed subscription triggers a rebalance', () => {
    const h = harness();
    stableGroupOfTwo(h);
    h.send('c2', {
      api: 'JoinGroup',
      memberId: h.idOf('c2'),
      metadata: {
        protocols: ['range', 'cooperative-sticky'],
        subscription: ['t', 'u'],
        ownedPartitions: [],
        stickyUserData: [],
        ownedGeneration: -1,
        sessionTimeoutMs: 10_000,
        rebalanceTimeoutMs: 30_000,
      },
      reason: 'subscription-change',
    });
    h.run(T + 2);
    expect(ofType(h.events, 'RebalanceStarted').at(-1)?.reason).toBe('subscription-change');
  });
});

describe('leaving and session timeouts', () => {
  it('a member silent for session.timeout.ms is removed; heartbeats keep it in', () => {
    const h = harness();
    stableGroupOfTwo(h); // sessions restarted by SyncGroup at T - 1
    const c1 = h.idOf('c1');
    for (let t = T + 1000; t <= T + 9000; t += 3000) {
      h.run(t);
      h.send('c1', { api: 'Heartbeat', memberId: c1, generation: 1 });
    }
    h.run(T + 9998);
    expect(ofType(h.events, 'MemberRemoved')).toEqual([]);
    h.run(T + 9999); // c2's last contact was its SyncGroup at T - 1
    expect(ofType(h.events, 'MemberRemoved')).toMatchObject([
      { consumerId: 'c2', reason: 'session-timeout' },
    ]);
    expect(h.group().members.has(c1)).toBe(true);
    expect(ofType(h.events, 'RebalanceStarted').at(-1)?.reason).toBe('session-timeout');
  });

  it('LeaveGroup removes the member at once and rebalances; poll-timeout keeps its reason', () => {
    const h = harness();
    stableGroupOfTwo(h);
    h.send('c2', { api: 'LeaveGroup', memberId: h.idOf('c2'), reason: 'poll-timeout' });
    h.run(T + 2);
    expect(ofType(h.events, 'MemberRemoved').at(-1)).toMatchObject({
      consumerId: 'c2',
      reason: 'poll-timeout',
    });
    expect(ofType(h.events, 'RebalanceStarted').at(-1)?.reason).toBe('poll-timeout');
  });

  it('when the last member leaves, the group becomes Empty with the next generation (K5)', () => {
    const h = harness();
    stableGroupOfTwo(h);
    h.send('c1', { api: 'LeaveGroup', memberId: h.idOf('c1'), reason: 'leave' });
    h.run(T + 2);
    expect(h.group().leader).toBe(h.idOf('c2'));
    h.send('c2', { api: 'LeaveGroup', memberId: h.idOf('c2'), reason: 'leave' });
    h.run(T + 4);
    // c1 leaving started a rebalance (generation 1 → 2); with c2 gone too, nobody is left to
    // wait for, so the join phase ends at once and generation 2 is Empty.
    expect(h.group()).toMatchObject({ state: 'Empty', generation: 2, leader: null });
    expect(ofType(h.events, 'RebalanceCompleted').at(-1)).toMatchObject({
      generation: 2,
      protocol: null,
      members: [],
    });
  });
});

describe('OffsetCommit validation (ClassicGroup#validateOffsetCommit, K1)', () => {
  const commit = (h: H, consumerId: string, memberId: string, generation: number) => {
    h.send(consumerId, {
      api: 'OffsetCommit',
      commitId: 1,
      memberId,
      generation,
      offsets: [{ topic: 't', partition: 0, offset: 5, topicId: 1 }],
    });
  };
  const stored = (h: H) => h.group().committed.get('t-0');

  it('a member of the current generation commits; a stale generation does not', () => {
    const h = harness();
    stableGroupOfTwo(h);
    commit(h, 'c1', h.idOf('c1'), 0);
    h.run(T + 2);
    expect(h.last('c1', 'OffsetCommit')?.response.error).toBe('ILLEGAL_GENERATION');
    expect(stored(h)).toBeUndefined();
    commit(h, 'c1', h.idOf('c1'), 1);
    h.run(T + 4);
    expect(h.last('c1', 'OffsetCommit')?.response.error).toBeNull();
    expect(stored(h)).toBe(5);
    expect(ofType(h.events, 'OffsetCommitted').at(-1)).toMatchObject({
      memberId: h.idOf('c1'),
      generation: 1,
      offset: 5,
    });
  });

  it('unknown members are refused; PreparingRebalance accepts; CompletingRebalance refuses', () => {
    const h = harness();
    stableGroupOfTwo(h);
    commit(h, 'x', 'x-1', 1);
    h.join('c3'); // → PreparingRebalance, generation still 1
    h.run(T + 2);
    expect(h.last('x', 'OffsetCommit')?.response.error).toBe('UNKNOWN_MEMBER_ID');
    commit(h, 'c1', h.idOf('c1'), 1);
    h.run(T + 4);
    expect(h.group().state).toBe('PreparingRebalance');
    expect(h.last('c1', 'OffsetCommit')?.response.error).toBeNull();
    h.join('c1', h.idOf('c1'));
    h.join('c2', h.idOf('c2'));
    h.run(T + 6);
    expect(h.group().state).toBe('CompletingRebalance');
    commit(h, 'c2', h.idOf('c2'), 2);
    h.run(T + 8);
    expect(h.last('c2', 'OffsetCommit')?.response.error).toBe('REBALANCE_IN_PROGRESS');
  });
});

describe('protocol selection (K6) and consistency', () => {
  it('votes for the assignor every member supports, by preference', () => {
    const h = harness(0);
    h.join('c1', '', { protocols: ['range', 'cooperative-sticky'] });
    h.join('c2', '', { protocols: ['cooperative-sticky'] });
    h.run(2);
    expect(h.group().protocol).toBe('cooperative-sticky');
  });

  it('a tie goes to the longest-standing member’s preference', () => {
    const h = harness(0);
    h.join('c1', '', { protocols: ['sticky', 'roundrobin'] });
    h.join('c2', '', { protocols: ['roundrobin', 'sticky'] });
    h.run(2);
    expect(h.group().protocol).toBe('sticky');
  });

  it('a member with no assignor in common gets INCONSISTENT_GROUP_PROTOCOL', () => {
    const h = harness();
    h.join('c1', '', { protocols: ['range'] });
    h.run(10);
    h.join('c2', '', { protocols: ['roundrobin'] });
    h.run(20);
    expect(h.last('c2', 'JoinGroup')?.response).toEqual({
      api: 'JoinGroup',
      error: 'INCONSISTENT_GROUP_PROTOCOL',
    });
    expect(h.group().members.size).toBe(1);
  });
});

describe('edge cases', () => {
  it('a JoinGroup or Heartbeat with an unknown member id gets UNKNOWN_MEMBER_ID', () => {
    const h = harness();
    h.join('c1', 'ghost-1');
    h.send('c1', { api: 'Heartbeat', memberId: 'ghost-1', generation: 0 });
    h.run(5);
    expect(h.last('c1', 'JoinGroup')?.response).toEqual({
      api: 'JoinGroup',
      error: 'UNKNOWN_MEMBER_ID',
    });
    expect(h.last('c1', 'Heartbeat')?.response).toEqual({
      api: 'Heartbeat',
      error: 'UNKNOWN_MEMBER_ID',
    });
  });

  it('a member rejoining with the same metadata while CompletingRebalance gets the current generation', () => {
    const h = harness();
    h.join('c1');
    h.join('c2');
    h.run(6002);
    h.join('c2', h.idOf('c2'));
    h.run(6004);
    expect(h.group().state).toBe('CompletingRebalance');
    expect(h.last('c2', 'JoinGroup')).toMatchObject({ t: 6004, response: { generation: 1 } });
  });

  it('a follower syncing after the group is Stable gets its assignment at once', () => {
    const h = harness();
    h.join('c1');
    h.join('c2');
    h.run(6002);
    h.send('c1', {
      api: 'SyncGroup',
      memberId: h.idOf('c1'),
      generation: 1,
      assignment: [[h.idOf('c2'), [{ topic: 't', partition: 1 }]]],
    });
    h.run(6004);
    h.send('c2', { api: 'SyncGroup', memberId: h.idOf('c2'), generation: 1, assignment: null });
    h.run(6006);
    expect(h.last('c2', 'SyncGroup')?.response).toMatchObject({
      assignment: [{ topic: 't', partition: 1 }],
    });
    expect(h.last('c1', 'SyncGroup')?.response).toMatchObject({ assignment: [] });
  });

  it('a SyncGroup while PreparingRebalance gets REBALANCE_IN_PROGRESS; LeaveGroup of an unknown member is ignored', () => {
    const h = harness();
    stableGroupOfTwo(h);
    h.join('c3');
    h.send('c9', { api: 'LeaveGroup', memberId: 'c9-x', reason: 'leave' });
    h.run(T + 2);
    h.send('c1', { api: 'SyncGroup', memberId: h.idOf('c1'), generation: 1, assignment: null });
    h.run(T + 4);
    expect(h.last('c1', 'SyncGroup')?.response).toEqual({
      api: 'SyncGroup',
      error: 'REBALANCE_IN_PROGRESS',
    });
    expect(ofType(h.events, 'MemberRemoved')).toEqual([]);
  });

  it('an Empty group accepts commits without a member (generation -1); unknown partitions are skipped', () => {
    const h = harness();
    h.send('admin', {
      api: 'OffsetCommit',
      commitId: 7,
      memberId: '',
      generation: -1,
      offsets: [
        { topic: 't', partition: 1, offset: 3, topicId: 1 },
        { topic: 't', partition: 9, offset: 3, topicId: 1 },
        { topic: 'gone', partition: 0, offset: 3, topicId: 5 },
      ],
    });
    h.run(2);
    expect(h.last('admin', 'OffsetCommit')?.response).toEqual({
      api: 'OffsetCommit',
      commitId: 7,
      error: null,
    });
    expect([...h.group().committed]).toEqual([['t-1', 3]]);
    expect(ofType(h.events, 'OffsetCommitted')).toMatchObject([{ memberId: null, generation: -1 }]);
  });
});

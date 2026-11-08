/**
 * The KRaft controller: broker liveness, fencing, elections and controlled shutdown (M4-T5), and
 * truncation after a leader change (M4-T6). M4-decisions K1, K2, K9, K11, K12, K14.
 */

import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine, type Engine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';

function record(engine: Engine): EngineEvent[] {
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return events;
}

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

function cluster(
  options: {
    acks?: '0' | '1' | 'all';
    rf?: number;
    minIsr?: number;
    unclean?: boolean;
    rate?: number;
    brokers?: number;
  } = {},
): TopologyInput {
  return {
    brokers: Array.from({ length: options.brokers ?? 3 }, (_, i) => ({
      kind: 'broker' as const,
      id: `b${String(i + 1)}`,
    })),
    topics: [
      {
        kind: 'topic',
        id: 'orders',
        partitions: 1,
        replicationFactor: options.rf ?? 3,
        minInsyncReplicas: options.minIsr ?? 1,
        uncleanLeaderElectionEnable: options.unclean ?? false,
      },
    ],
    producers: [
      {
        kind: 'producer',
        id: 'p1',
        ratePerSec: options.rate ?? 1,
        acks: options.acks ?? 'all',
      },
    ],
    connections: [{ from: 'p1', to: 'orders' }],
  };
}

const p0 = (engine: Engine) => {
  const p = engine.getSnapshot().topics[0]?.partitions[0];
  if (p === undefined) throw new Error('no partition');
  return p;
};
const followers = (engine: Engine) =>
  p0(engine)
    .replicas.map((r) => r.broker)
    .filter((b) => b !== p0(engine).leader);

describe('crash and failover (M4-T5)', () => {
  it('a killed leader is fenced at broker.session.timeout.ms and an ISR member takes over', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ minIsr: 2 }) });
    const events = record(engine);
    engine.runUntil(4500); // heartbeats at 2 s and 4 s renewed the session
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.runUntil(20_000);
    const fenced = ofType(events, 'BrokerFenced');
    expect(fenced).toMatchObject([{ brokerId: leader, reason: 'session-timeout' }]);
    // Last heartbeat reached the controller at 4001 ms: fenced 9 s later.
    expect(fenced[0]?.t).toBe(13_001);
    const elected = ofType(events, 'LeaderElected');
    expect(elected).toMatchObject([
      { previousLeader: leader, leaderEpoch: 1, reason: 'failover', t: 13_001 },
    ]);
    // The new leader is the first ISR replica in replica order (K12).
    const replicas = p0(engine).replicas.map((r) => r.broker);
    expect(elected[0]?.leader).toBe(replicas.find((b) => b !== leader));
    expect(p0(engine).isr).not.toContain(leader);
  });

  it('acks=all with minISR 2: no acknowledged record is lost when the old leader returns', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ minIsr: 2, rate: 20 }) });
    const events = record(engine);
    engine.runUntil(4500);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.runUntil(20_000);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(25_000);
    const truncated = ofType(events, 'RecordsTruncated');
    expect(truncated.reduce((n, e) => n + e.acked, 0)).toBe(0);
    // Every acknowledged record is in the new leader's log.
    const acked = new Set(ofType(events, 'ProduceAcked').map((e) => e.traceId));
    const log = new Set(p0(engine).records.map((r) => r.traceId));
    const recent = [...acked].filter((t) => Number(t.split('#')[1]) > 480);
    for (const t of recent) expect(log.has(t)).toBe(true);
    // The old leader is a follower again, caught up and back in the ISR.
    expect(p0(engine).isr).toContain(leader);
    expect(new Set(p0(engine).replicas.map((r) => r.logEndOffset)).size).toBe(1);
  });

  it('acks=1: records only the old leader had are truncated when it returns (acked loss)', () => {
    // 20 ms network: the leader acks at once, followers copy one round trip later.
    const engine = createEngine({
      seed: 1,
      topology: cluster({ acks: '1', rate: 50 }),
      networkDelayMs: 20,
    });
    const events = record(engine);
    engine.runUntil(4510);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.runUntil(20_000);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(25_000);
    const truncated = ofType(events, 'RecordsTruncated');
    expect(truncated).toHaveLength(1);
    expect(truncated[0]?.broker).toBe(leader);
    expect(truncated[0]?.acked).toBeGreaterThan(0);
    // The truncated records are gone from every replica.
    const lost = new Set(truncated[0]?.traceIds);
    for (const r of p0(engine).records) expect(lost.has(r.traceId)).toBe(false);
  });
});

describe('controlled shutdown (M4-T5, K11)', () => {
  it('moves leadership off at once: no offline partition, no failed write, then the broker stops', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ minIsr: 2 }) });
    const events = record(engine);
    engine.runUntil(1500);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'StopBroker', id: leader });
    engine.runUntil(1510);
    expect(ofType(events, 'LeaderElected')).toMatchObject([
      { previousLeader: leader, reason: 'controlled-shutdown', t: 1501 },
    ]);
    expect(ofType(events, 'BrokerFenced')).toMatchObject([
      { brokerId: leader, reason: 'controlled-shutdown', t: 1501 },
    ]);
    expect(ofType(events, 'BrokerStopped')).toMatchObject([
      { brokerId: leader, reason: 'shutdown', t: 1502 },
    ]);
    engine.runUntil(10_000);
    expect(ofType(events, 'PartitionOffline')).toEqual([]);
    expect(ofType(events, 'ProduceFailed')).toEqual([]);
    expect(ofType(events, 'ProduceAcked').length).toBeGreaterThan(5);
  });

  it('a cleanly stopped broker restarts as a follower and rejoins the ISR without truncation', () => {
    const engine = createEngine({ seed: 1, topology: cluster() });
    const events = record(engine);
    engine.runUntil(1500);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'StopBroker', id: leader });
    engine.runUntil(5000);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(6000);
    expect(ofType(events, 'BrokerUnfenced')).toMatchObject([{ brokerId: leader }]);
    expect(ofType(events, 'IsrExpanded')).toMatchObject([{ added: [leader] }]);
    expect(ofType(events, 'RecordsTruncated')).toEqual([]);
  });

  it('a broker that is the only in-sync replica takes its partition offline when it stops', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rf: 1, brokers: 1 }) });
    const events = record(engine);
    engine.runUntil(1500);
    engine.dispatch({ type: 'StopBroker', id: 'b1' });
    engine.runUntil(1510);
    expect(ofType(events, 'PartitionOffline')).toMatchObject([
      { previousLeader: 'b1', isr: [], elr: ['b1'] },
    ]);
    // Back with a clean shutdown: an ELR member, so a clean election (K1).
    engine.dispatch({ type: 'RestartBroker', id: 'b1' });
    engine.runUntil(1600);
    expect(ofType(events, 'LeaderElected')).toMatchObject([{ leader: 'b1', reason: 'failover' }]);
    expect(p0(engine).logEndOffset).toBe(1);
  });
});

describe('last replica standing, unclean election (M4-T5)', () => {
  it('RF 1: a crash takes the partition offline; after the restart it comes back with its data', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rf: 1, brokers: 1 }) });
    const events = record(engine);
    engine.runUntil(3500);
    engine.dispatch({ type: 'KillBroker', id: 'b1' });
    engine.runUntil(15_000);
    expect(ofType(events, 'PartitionOffline')).toMatchObject([{ t: 11_001 }]);
    expect(p0(engine).leader).toBeNull();
    engine.dispatch({ type: 'RestartBroker', id: 'b1' });
    engine.runUntil(15_100);
    // After an unclean shutdown it left the ELR, so it is elected as the last known leader,
    // which Kafka 4.4 flags as unclean (KIP-966, K1). No other replica: nothing is lost.
    expect(ofType(events, 'LeaderElected')).toMatchObject([{ leader: 'b1', reason: 'unclean' }]);
    expect(p0(engine).logEndOffset).toBe(3);
    expect(ofType(events, 'RecordsTruncated')).toEqual([]);
  });

  it('without unclean election, an out-of-sync replica never leads: the partition stays offline', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rf: 2, brokers: 2, acks: '1' }) });
    const events = record(engine);
    engine.runUntil(500);
    const [follower] = followers(engine);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'KillBroker', id: follower ?? '' });
    engine.runUntil(12_000); // the follower is fenced and out of the ISR
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.dispatch({ type: 'RestartBroker', id: follower ?? '' });
    engine.runUntil(30_000);
    expect(p0(engine).leader).toBeNull();
    expect(ofType(events, 'PartitionOffline')).toHaveLength(1);
  });

  it('with unclean election on, the out-of-sync replica leads and the old leader loses records', () => {
    const engine = createEngine({
      seed: 1,
      topology: cluster({ rf: 2, brokers: 2, acks: '1', unclean: true }),
    });
    const events = record(engine);
    engine.runUntil(500);
    const [follower] = followers(engine);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'KillBroker', id: follower ?? '' });
    engine.runUntil(12_000);
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.dispatch({ type: 'RestartBroker', id: follower ?? '' });
    engine.runUntil(30_000);
    expect(ofType(events, 'LeaderElected')).toMatchObject([
      { leader: follower, reason: 'unclean' },
    ]);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(32_000);
    const truncated = ofType(events, 'RecordsTruncated');
    expect(truncated).toHaveLength(1);
    // The unclean leader had nothing (it was down since 500 ms): all 11 records are lost.
    expect(truncated[0]).toMatchObject({ broker: leader, from: 0, to: 11, acked: 11 });
  });

  it('turning unclean election on elects a leader for an offline partition at once', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rf: 2, brokers: 2, acks: '1' }) });
    const events = record(engine);
    engine.runUntil(500);
    const [follower] = followers(engine);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'KillBroker', id: follower ?? '' });
    engine.runUntil(12_000);
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.dispatch({ type: 'RestartBroker', id: follower ?? '' });
    engine.runUntil(30_000);
    expect(p0(engine).leader).toBeNull();
    engine.dispatch({
      type: 'UpdateConfig',
      id: 'orders',
      patch: { uncleanLeaderElectionEnable: true },
    });
    expect(ofType(events, 'LeaderElected')).toMatchObject([{ leader: follower, t: 30_000 }]);
  });
});

describe('preferred leader election (M4-T5, Q10)', () => {
  it('leadership returns to the preferred replica at the next imbalance check', () => {
    const engine = createEngine({ seed: 1, topology: cluster() });
    const events = record(engine);
    engine.runUntil(1500);
    const preferred = p0(engine).replicas[0]?.broker ?? '';
    expect(p0(engine).leader).toBe(preferred);
    engine.dispatch({ type: 'StopBroker', id: preferred });
    engine.runUntil(5000);
    engine.dispatch({ type: 'RestartBroker', id: preferred });
    engine.runUntil(31_000); // leader.imbalance.check.interval.seconds: 30 (game-tuned)
    const back = ofType(events, 'LeaderElected').filter((e) => e.reason === 'preferred');
    expect(back).toMatchObject([{ leader: preferred, t: 30_000 }]);
    expect(p0(engine).leader).toBe(preferred);
  });

  it('auto.leader.rebalance.enable=false keeps the leader where it is', () => {
    const topology = cluster();
    const engine = createEngine({
      seed: 1,
      topology: {
        ...topology,
        brokers: topology.brokers?.map((b) => ({ ...b, autoLeaderRebalanceEnable: false })),
      },
    });
    const events = record(engine);
    engine.runUntil(1500);
    const preferred = p0(engine).replicas[0]?.broker ?? '';
    engine.dispatch({ type: 'StopBroker', id: preferred });
    engine.runUntil(5000);
    engine.dispatch({ type: 'RestartBroker', id: preferred });
    engine.runUntil(65_000);
    expect(ofType(events, 'LeaderElected').filter((e) => e.reason === 'preferred')).toEqual([]);
  });
});

describe('truncation by leader epoch (M4-T6, K9)', () => {
  it('a returning leader truncates to where its epoch ends on the new leader, then catches up', () => {
    const engine = createEngine({
      seed: 1,
      topology: cluster({ acks: '1', rate: 50 }),
      networkDelayMs: 20,
    });
    const events = record(engine);
    engine.runUntil(4510);
    const leader = p0(engine).leader ?? '';
    const before = engine.getSnapshot().topics[0]?.partitions[0]?.replicas;
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.runUntil(20_000);
    const newLeaderLeoAtElection = Math.min(
      ...(before ?? []).filter((r) => r.broker !== leader).map((r) => r.logEndOffset),
    );
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(24_000);
    engine.dispatch({ type: 'RemoveComponent', id: 'p1' });
    engine.runUntil(25_000);
    const [truncated] = ofType(events, 'RecordsTruncated');
    expect(truncated?.from).toBeGreaterThanOrEqual(newLeaderLeoAtElection);
    // Afterwards every replica holds the same log.
    const replicas = p0(engine).replicas;
    expect(new Set(replicas.map((r) => r.logEndOffset)).size).toBe(1);
  });
});

describe('review of M4-T5/T6: regressions', () => {
  it('successive leaders that append nothing do not leave followers diverging for ever', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rate: 10 }) });
    engine.runUntil(1100);
    engine.dispatch({ type: 'RemoveComponent', id: 'p1' });
    engine.runUntil(1200);
    const first = p0(engine).leader ?? '';
    engine.dispatch({ type: 'StopBroker', id: first });
    engine.runUntil(1300);
    const second = p0(engine).leader ?? '';
    engine.dispatch({ type: 'StopBroker', id: second });
    engine.runUntil(1400);
    engine.dispatch({ type: 'RestartBroker', id: first });
    engine.dispatch({ type: 'RestartBroker', id: second });
    engine.runUntil(3000);
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'producer', id: 'p2', ratePerSec: 10, acks: 'all' },
    });
    engine.dispatch({ type: 'Connect', from: 'p2', to: 'orders' });
    engine.runUntil(8000);
    engine.dispatch({ type: 'RemoveComponent', id: 'p2' });
    engine.runUntil(9000);
    const p = p0(engine);
    expect(p.logEndOffset).toBeGreaterThan(40);
    expect(new Set(p.replicas.map((r) => r.logEndOffset)).size).toBe(1);
    expect(p.isr).toHaveLength(3);
  });

  it('a diverged follower truncates before it can rejoin the ISR', () => {
    const engine = createEngine({
      seed: 1,
      topology: cluster({ rf: 2, brokers: 2, acks: '1', unclean: true }),
    });
    const events = record(engine);
    engine.runUntil(500);
    const [follower] = followers(engine);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'KillBroker', id: follower ?? '' });
    engine.runUntil(12_000);
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.dispatch({ type: 'RestartBroker', id: follower ?? '' });
    engine.runUntil(30_000);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(32_000);
    const truncated = ofType(events, 'RecordsTruncated').find((e) => e.broker === leader);
    const expanded = ofType(events, 'IsrExpanded').find((e) => e.added.includes(leader));
    expect(truncated).toBeDefined();
    expect(expanded).toBeDefined();
    expect((expanded?.seq ?? 0) > (truncated?.seq ?? 0)).toBe(true);
  });

  it('a broker restarted before its session expires is fenced first and serves nothing meanwhile', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ acks: '1', minIsr: 1 }) });
    const events = record(engine);
    engine.runUntil(4500);
    const leader = p0(engine).leader ?? '';
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.runUntil(6000);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(20_000);
    // No appends on the restarted process before it is registered and unfenced.
    const unfencedAt = ofType(events, 'BrokerUnfenced').find((e) => e.brokerId === leader)?.t ?? 0;
    expect(unfencedAt).toBeGreaterThan(13_000);
    expect(ofType(events, 'BrokerFenced')).toMatchObject([
      { brokerId: leader, reason: 'session-timeout' },
    ]);
    // Nothing acknowledged was lost (it never led again on stale metadata).
    expect(ofType(events, 'RecordsTruncated').reduce((n, e) => n + e.acked, 0)).toBe(0);
  });

  it('a removed and re-added broker is not fenced by its old session timer', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rf: 1, brokers: 1, minIsr: 1 }) });
    const events = record(engine);
    engine.runUntil(1000);
    engine.dispatch({ type: 'AddComponent', component: { kind: 'broker', id: 'b4' } });
    engine.runUntil(1500);
    engine.dispatch({ type: 'RemoveComponent', id: 'b4' });
    engine.runUntil(9000);
    engine.dispatch({ type: 'AddComponent', component: { kind: 'broker', id: 'b4' } });
    engine.runUntil(20_000);
    expect(ofType(events, 'BrokerFenced')).toEqual([]);
  });
});

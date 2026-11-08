/**
 * Producer retries, timeouts and idempotence during failover (M4-T7; M4-decisions K7, K8).
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
    acks?: '1' | 'all';
    rf?: number;
    minIsr?: number;
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
        minInsyncReplicas: options.minIsr ?? 2,
      },
    ],
    producers: [
      { kind: 'producer', id: 'p1', ratePerSec: options.rate ?? 20, acks: options.acks ?? 'all' },
    ],
    connections: [{ from: 'p1', to: 'orders' }],
  };
}

const p0 = (engine: Engine) => {
  const p = engine.getSnapshot().topics[0]?.partitions[0];
  if (p === undefined) throw new Error('no partition');
  return p;
};

describe('producer retries during failover (M4-T7)', () => {
  it('a leader kill under load with acks=all: no ProduceFailed, no duplicate in the log', () => {
    const engine = createEngine({ seed: 1, topology: cluster() });
    const events = record(engine);
    engine.runUntil(4500);
    engine.dispatch({ type: 'KillBroker', id: p0(engine).leader ?? '' });
    engine.runUntil(20_000);
    engine.dispatch({ type: 'RemoveComponent', id: 'p1' }); // let everything settle
    engine.runUntil(22_000);
    // Requests in flight to the dead leader failed with NETWORK_EXCEPTION and were retried.
    const retried = ofType(events, 'ProduceRetried');
    expect(retried.some((e) => e.error === 'NETWORK_EXCEPTION')).toBe(true);
    expect(ofType(events, 'ProduceFailed')).toEqual([]);
    // Every record the producer created until it was removed is in the log once.
    const created = ofType(events, 'RecordProduced').map((e) => e.traceId);
    const acked = ofType(events, 'ProduceAcked').map((e) => e.traceId);
    // All but the record created at the instant the producer was removed (no one to ack it).
    expect(acked.length).toBeGreaterThanOrEqual(created.length - 1);
    const leader = p0(engine);
    const log = leader.records; // the last 20; check the whole log through the replica LEOs
    expect(new Set(log.map((r) => r.traceId)).size).toBe(log.length);
    expect(leader.logEndOffset).toBe(created.length);
  });

  it('a record retried after its answer was lost lands once (idempotent dedup)', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rate: 1 }) });
    const events = record(engine);
    engine.runUntil(1001); // record 1 appended at the leader, its acks=all answer pending
    // The leader shuts down cleanly: leadership moves; the parked write is answered
    // NOT_LEADER_OR_FOLLOWER, so the producer sends it again to the new leader, which has it.
    engine.dispatch({ type: 'StopBroker', id: p0(engine).leader ?? '' });
    engine.runUntil(3000);
    expect(ofType(events, 'ProduceRetried')).toMatchObject([
      { traceId: 'p1@1#1', error: 'NOT_LEADER_OR_FOLLOWER', backoffMs: 0 },
    ]);
    const appended = ofType(events, 'RecordAppended').filter((e) => e.traceId === 'p1@1#1');
    expect(appended).toHaveLength(1);
    expect(ofType(events, 'ProduceAcked').find((e) => e.traceId === 'p1@1#1')?.offset).toBe(0);
  });

  it('a partition offline for 40 s: records fail with DELIVERY_TIMEOUT after delivery.timeout.ms', () => {
    const engine = createEngine({
      seed: 1,
      topology: cluster({ rf: 1, brokers: 1, minIsr: 1, rate: 1 }),
    });
    const events = record(engine);
    engine.runUntil(1500);
    engine.dispatch({ type: 'KillBroker', id: 'b1' });
    engine.runUntil(40_000);
    engine.dispatch({ type: 'RestartBroker', id: 'b1' });
    engine.runUntil(45_000);
    const failed = ofType(events, 'ProduceFailed');
    // Records created from 2 s on waited for a leader; those older than 30 s timed out.
    expect(failed.length).toBeGreaterThan(5);
    for (const f of failed) expect(f.error).toBe('DELIVERY_TIMEOUT');
    expect(failed[0]?.t).toBe(32_000);
    // Records younger than 30 s at the restart got through.
    expect(ofType(events, 'ProduceAcked').some((e) => e.t > 40_000)).toBe(true);
  });

  it('retries back off exponentially (KIP-580), capped at retry.backoff.max.ms', () => {
    const engine = createEngine({ seed: 3, topology: cluster({ rate: 1, minIsr: 3 }) });
    const events = record(engine);
    engine.runUntil(500);
    // One follower down: the ISR shrinks below minISR 3 and every write is NOT_ENOUGH_REPLICAS.
    const follower = p0(engine).replicas.find((r) => r.broker !== p0(engine).leader)?.broker;
    engine.dispatch({ type: 'KillBroker', id: follower ?? '' });
    engine.runUntil(25_000);
    const retried = ofType(events, 'ProduceRetried');
    const first = retried[0]?.traceId;
    const waits = retried.filter((e) => e.traceId === first).map((e) => e.backoffMs);
    expect(waits.length).toBeGreaterThan(5);
    // 100, 200, 400, 800, then 1000 ms, each ±20 %.
    [100, 200, 400, 800, 1000, 1000].forEach((base, i) => {
      expect(waits[i]).toBeGreaterThanOrEqual(base * 0.8);
      expect(waits[i]).toBeLessThanOrEqual(base * 1.2);
    });
  });

  it('acks=1 has no idempotence: a retry after a lost answer can write a duplicate', () => {
    const engine = createEngine({
      seed: 1,
      topology: cluster({ acks: '1', rate: 1 }),
      networkDelayMs: 20,
    });
    const events = record(engine);
    engine.runUntil(1010); // record 1 is on its way to the leader (arrives at 1020)
    // A controlled shutdown takes ~2 network delays; the append at 1020 is answered by a leader
    // that is still up, but its answer races the leadership move.
    engine.dispatch({ type: 'KillBroker', id: p0(engine).leader ?? '' });
    engine.runUntil(20_000);
    const appended = ofType(events, 'RecordAppended').filter((e) => e.traceId === 'p1@1#1');
    // The request reached a dead broker: never appended there, retried, appended once elsewhere.
    expect(appended.length).toBe(1);
    expect(ofType(events, 'ProduceRetried')[0]).toMatchObject({ error: 'NETWORK_EXCEPTION' });
  });
});

describe('idempotence after truncation (K8)', () => {
  it('a sequence gap after truncation is OUT_OF_ORDER_SEQUENCE_NUMBER; the producer bumps its epoch and goes on', () => {
    // RF 2, minISR 1, acks=all, idempotent. The follower is killed; the leader keeps writing
    // alone (ISR = leader). The leader is killed and the old follower comes back as an unclean
    // leader without those records: the producer's next sequence is a gap there.
    const engine = createEngine({
      seed: 1,
      topology: {
        ...cluster({ rf: 2, brokers: 2, minIsr: 1, rate: 2 }),
        topics: [
          {
            kind: 'topic',
            id: 'orders',
            partitions: 1,
            replicationFactor: 2,
            minInsyncReplicas: 1,
            uncleanLeaderElectionEnable: true,
          },
        ],
      },
    });
    const events = record(engine);
    engine.runUntil(3000);
    const leader = p0(engine).leader ?? '';
    const follower = p0(engine).replicas.find((r) => r.broker !== leader)?.broker ?? '';
    engine.dispatch({ type: 'KillBroker', id: follower });
    engine.runUntil(14_000); // follower fenced at ~9 s; the leader writes alone
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.dispatch({ type: 'RestartBroker', id: follower });
    engine.runUntil(30_000);
    expect(ofType(events, 'LeaderElected').at(-1)).toMatchObject({
      leader: follower,
      reason: 'unclean',
    });
    // The new leader had the producer's state up to the records it copied; the next sequence
    // the producer sends is beyond that: a gap.
    expect(
      ofType(events, 'ProduceRetried').some((e) => e.error === 'OUT_OF_ORDER_SEQUENCE_NUMBER'),
    ).toBe(true);
    // After the epoch bump, writes succeed again.
    const lastAck = ofType(events, 'ProduceAcked').at(-1);
    expect(lastAck?.t).toBeGreaterThan(24_000);
  });
});

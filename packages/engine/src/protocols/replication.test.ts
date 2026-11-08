/**
 * Follower fetching, the high watermark and acks=all (M4-T3; M4-decisions K3–K6). Network delay
 * is 1 ms each way, so one round trip is 2 ms.
 */

import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine, createEngineInternal, type Engine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';
import { completeDelayedProduces, parkProduce } from './replication.ts';

function record(engine: Engine): EngineEvent[] {
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return events;
}

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

/** Three brokers, topic "orders" (1 partition, RF 3), producer p1 sending one record at 1000 ms. */
function cluster(
  options: { acks?: '0' | '1' | 'all'; rf?: number; minIsr?: number; consumer?: boolean } = {},
): TopologyInput {
  return {
    brokers: [
      { kind: 'broker', id: 'b1' },
      { kind: 'broker', id: 'b2' },
      { kind: 'broker', id: 'b3' },
    ],
    topics: [
      {
        kind: 'topic',
        id: 'orders',
        partitions: 1,
        replicationFactor: options.rf ?? 3,
        minInsyncReplicas: options.minIsr ?? 1,
      },
    ],
    producers: [{ kind: 'producer', id: 'p1', ratePerSec: 1, acks: options.acks ?? 'all' }],
    consumers: options.consumer
      ? [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest', fetchMaxWaitMs: 100 }]
      : [],
    connections: [
      { from: 'p1', to: 'orders' },
      ...(options.consumer ? [{ from: 'orders', to: 'c1' }] : []),
    ],
  };
}

const partition0 = (engine: Engine) => engine.getSnapshot().topics[0]?.partitions[0];

describe('follower fetching and the high watermark', () => {
  it('followers copy each record with one fetch; ReplicaFetched names the offsets', () => {
    const engine = createEngine({ seed: 1, topology: cluster() });
    const events = record(engine);
    engine.runUntil(1100);
    const p = partition0(engine);
    const leader = p?.leader;
    expect(p?.replicas.map((r) => r.logEndOffset)).toEqual([1, 1, 1]);
    const fetched = ofType(events, 'ReplicaFetched');
    expect(fetched.map((e) => [e.leader, e.fromOffset, e.toOffset, e.t])).toEqual([
      [leader, 0, 1, 1002],
      [leader, 0, 1, 1002],
    ]);
    expect(new Set(fetched.map((e) => e.broker))).toEqual(
      new Set(p?.replicas.map((r) => r.broker).filter((b) => b !== leader)),
    );
  });

  it('the HW moves only after every ISR member fetched past the record', () => {
    const engine = createEngine({ seed: 1, topology: cluster() });
    engine.runUntil(1002); // followers have the record, but the leader doesn't know yet
    expect(partition0(engine)?.highWatermark).toBe(0);
    engine.runUntil(1003); // their next fetches (offset 1) reach the leader
    expect(partition0(engine)?.highWatermark).toBe(1);
    engine.runUntil(1004); // the followers learn the HW from the next response
    engine.runUntil(1600); // … which comes after replica.fetch.wait.max.ms (nothing new)
    expect(partition0(engine)?.replicas.map((r) => r.highWatermark)).toEqual([1, 1, 1]);
  });

  it('acks=1 is answered after the leader append (+2 ms), acks=all after the ISR has it (+4 ms)', () => {
    for (const [acks, at] of [
      ['1', 1002],
      ['all', 1004],
    ] as const) {
      const engine = createEngine({ seed: 1, topology: cluster({ acks }) });
      const events = record(engine);
      engine.runUntil(1100);
      expect(ofType(events, 'ProduceAcked').map((e) => [e.offset, e.t])).toEqual([[0, at]]);
    }
  });

  it('with one replica acks=all is answered as before (+2 ms)', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rf: 1 }) });
    const events = record(engine);
    engine.runUntil(1100);
    expect(ofType(events, 'ProduceAcked').map((e) => e.t)).toEqual([1002]);
    expect(ofType(events, 'ReplicaFetched')).toEqual([]);
  });

  it('a crashed follower holds the HW back until the controller fences it (9 s)', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ consumer: true }) });
    const events = record(engine);
    engine.runUntil(500);
    const p = partition0(engine);
    const follower = p?.replicas.map((r) => r.broker).find((b) => b !== p.leader) ?? '';
    engine.dispatch({ type: 'KillBroker', id: follower });
    engine.runUntil(8999);
    expect(partition0(engine)?.logEndOffset).toBe(8);
    expect(partition0(engine)?.highWatermark).toBe(0);
    expect(ofType(events, 'RecordConsumed')).toEqual([]); // consumers read below the HW only
    engine.runUntil(9100);
    // Its session (renewed at registration, t=0) ran out at 9 s: fenced, out of the ISR.
    expect(ofType(events, 'BrokerFenced')).toMatchObject([
      { brokerId: follower, reason: 'session-timeout', t: 9000 },
    ]);
    expect(ofType(events, 'IsrShrunk')).toMatchObject([{ removed: [follower], t: 9000 }]);
    // The parked acks=all writes complete once the HW moves (+1 ms back to the producer).
    expect(
      ofType(events, 'ProduceAcked')
        .slice(0, 8)
        .map((e) => e.t),
    ).toEqual(Array(8).fill(9001));
    expect(ofType(events, 'ProduceFailed')).toEqual([]);
  });
});

describe('min.insync.replicas', () => {
  /**
   * RF 3, minISR 2: both followers stop at 500 ms, so the leader shrinks the ISR to itself at the
   * expiration check at 15 s (replica.lag.time.max.ms 10 s, checked every 5 s). Events from 16 s.
   */
  function underMinIsr(acks: '1' | 'all') {
    const engine = createEngine({
      seed: 1,
      topology: cluster({ acks, minIsr: 2, consumer: true }),
    });
    engine.runUntil(500);
    const p = partition0(engine);
    for (const r of p?.replicas ?? []) {
      if (r.broker !== p?.leader) engine.dispatch({ type: 'KillBroker', id: r.broker });
    }
    engine.runUntil(16_000);
    expect(partition0(engine)?.isr).toEqual([p?.leader]);
    expect(partition0(engine)?.underMinIsr).toBe(true);
    return { engine, events: record(engine) };
  }

  it('acks=all with the ISR below minISR fails with NOT_ENOUGH_REPLICAS, nothing is appended', () => {
    const { engine, events } = underMinIsr('all');
    engine.runUntil(17_100);
    // New writes are refused; writes parked before the shrink time out meanwhile.
    const failed = ofType(events, 'ProduceFailed');
    expect(failed.filter((e) => e.error === 'NOT_ENOUGH_REPLICAS').map((e) => e.t)).toEqual([
      16_002, 17_002,
    ]);
    expect(failed.filter((e) => e.error !== 'NOT_ENOUGH_REPLICAS').map((e) => e.error)).toEqual([
      'REQUEST_TIMED_OUT',
      'REQUEST_TIMED_OUT',
    ]);
    expect(ofType(events, 'RecordAppended')).toEqual([]);
  });

  it('strict min ISR: acks=1 writes are appended and acked, but the HW does not move', () => {
    const { engine, events } = underMinIsr('1');
    const hw = partition0(engine)?.highWatermark;
    engine.runUntil(20_100);
    expect(ofType(events, 'ProduceAcked')).toHaveLength(5);
    const p = partition0(engine);
    expect(p?.logEndOffset).toBe(20);
    expect(p?.highWatermark).toBe(hw);
    expect(ofType(events, 'RecordConsumed')).toEqual([]);
  });

  it('minISR above RF is capped at RF, as the leader does (effective min ISR)', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ rf: 2, minIsr: 3 }) });
    const events = record(engine);
    engine.runUntil(1100);
    expect(ofType(events, 'ProduceAcked').map((e) => e.t)).toEqual([1004]);
  });

  it('a parked write whose ISR shrank below minISR gets NOT_ENOUGH_REPLICAS_AFTER_APPEND', () => {
    // A narrow race in Kafka (K6): the HW already covers the record when the check runs.
    const { engine, context } = createEngineInternal({
      seed: 1,
      topology: cluster({ minIsr: 2 }),
    });
    const events = record(engine);
    const topic = context.state.topics.get('orders');
    const p = topic?.partitions[0];
    if (topic === undefined || p === undefined || p.leader === null) throw new Error('setup');
    const target = {
      producerId: 'p1',
      incarnation: 1,
      topic: 'orders',
      partition: 0,
      traceId: 'x',
    };
    parkProduce(context, target, { broker: p.leader, topicId: topic.topicId }, 0, 10_000);
    const leaderLog = p.replicaLogs.get(p.leader);
    if (leaderLog === undefined) throw new Error('setup');
    leaderLog.highWatermark = 1;
    p.isr = [p.leader];
    completeDelayedProduces(context, topic, p);
    engine.runUntil(10);
    expect(context.state.parkedProduces.size).toBe(0);
    const answers = events.filter((e) => e.type === 'ProduceFailed' || e.type === 'ProduceAcked');
    expect(answers).toHaveLength(1);
    expect(answers[0]).toMatchObject({
      type: 'ProduceFailed',
      traceId: 'x',
      error: 'NOT_ENOUGH_REPLICAS_AFTER_APPEND',
    });
  });
});

describe('consumer fetch offsets (M2 F-c TODO)', () => {
  it('an offset between the HW and the LEO gets an empty answer; above the LEO, OFFSET_OUT_OF_RANGE', async () => {
    const { readFetchPartition } = await import('./fetch.ts');
    const { engine, context } = createEngineInternal({ seed: 1, topology: cluster() });
    engine.runUntil(1001); // appended at the leader, not yet replicated
    const p = context.state.topics.get('orders')?.partitions[0];
    expect([p?.highWatermark, p?.logEndOffset]).toEqual([0, 1]);
    const at = (fetchOffset: number) =>
      readFetchPartition(context, { topic: 'orders', partition: 0, topicId: 1, fetchOffset });
    expect(at(1)).toMatchObject({ error: null, records: [] });
    expect(at(2)).toMatchObject({ error: 'OFFSET_OUT_OF_RANGE' });
    expect(at(0)).toMatchObject({ error: null, records: [] });
  });
});

describe('replication robustness (review of M4-T3)', () => {
  const leaderOf = (engine: Engine) => partition0(engine)?.leader ?? '';
  const followersOf = (engine: Engine) => {
    const p = partition0(engine);
    return p?.replicas.map((r) => r.broker).filter((b) => b !== p.leader) ?? [];
  };

  it('followers keep fetching after their leader was killed and restarted while they waited', () => {
    const engine = createEngine({ seed: 1, topology: cluster() });
    engine.runUntil(1100); // both followers' fetches are parked at the leader
    const leader = leaderOf(engine);
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.runUntil(1500);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    // Its registration is refused until its old session expires (~9 s): meanwhile it is fenced,
    // a follower takes over, and the old leader then registers and catches up as a follower.
    engine.runUntil(13_000);
    engine.dispatch({ type: 'RemoveComponent', id: 'p1' });
    engine.runUntil(15_000);
    const p = partition0(engine);
    expect(p?.leader).not.toBe(leader);
    expect(p?.isr).toContain(leader);
    expect(new Set(p?.replicas.map((r) => r.logEndOffset)).size).toBe(1);
    expect(p?.logEndOffset).toBeGreaterThan(1);
    expect(p?.highWatermark).toBe(p?.logEndOffset);
  });

  it('a follower restarted while its fetch was in flight fetches again', () => {
    const engine = createEngine({ seed: 1, topology: cluster() });
    engine.runUntil(1100);
    const [follower] = followersOf(engine);
    engine.dispatch({ type: 'KillBroker', id: follower ?? '' });
    engine.dispatch({ type: 'RestartBroker', id: follower ?? '' });
    engine.runUntil(5100);
    const p = partition0(engine);
    expect(p?.replicas.map((r) => r.logEndOffset)).toEqual([5, 5, 5]);
    expect(p?.highWatermark).toBe(5);
  });

  it('deleting a topic answers its parked acks=all writes; a re-created topic never acks them', () => {
    const engine = createEngine({ seed: 1, topology: cluster() });
    const events = record(engine);
    engine.runUntil(500);
    for (const b of followersOf(engine)) engine.dispatch({ type: 'KillBroker', id: b });
    engine.runUntil(3500); // records 0–2 are parked (the HW is stuck)
    engine.dispatch({ type: 'RemoveComponent', id: 'orders' });
    expect(
      engine.dispatch({
        type: 'AddComponent',
        component: { kind: 'topic', id: 'orders', replicationFactor: 1 },
      }),
    ).toEqual({ ok: true });
    engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' });
    engine.runUntil(7100);
    const old = (e: { traceId: string }) => ['p1@1#1', 'p1@1#2', 'p1@1#3'].includes(e.traceId);
    expect(ofType(events, 'ProduceAcked').filter(old)).toEqual([]);
    expect(
      ofType(events, 'ProduceFailed')
        .filter(old)
        .map((e) => e.error),
    ).toEqual([
      'UNKNOWN_TOPIC_OR_PARTITION',
      'UNKNOWN_TOPIC_OR_PARTITION',
      'UNKNOWN_TOPIC_OR_PARTITION',
    ]);
    // Records sent to the new topic are acked with its own offsets.
    expect(ofType(events, 'ProduceAcked').map((e) => e.offset)).toEqual([0, 1, 2, 3]);
  });

  it('partitions added to a replicated topic are replicated too', () => {
    const engine = createEngine({
      seed: 1,
      topology: {
        ...cluster(),
        producers: [{ kind: 'producer', id: 'p1', ratePerSec: 10, metadataMaxAgeMs: 0 }],
      },
    });
    engine.runUntil(100);
    engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 3 } });
    engine.runUntil(5000);
    const partitions = engine.getSnapshot().topics[0]?.partitions ?? [];
    expect(partitions).toHaveLength(3);
    expect(partitions.slice(1).some((p) => p.logEndOffset > 0)).toBe(true);
    for (const p of partitions) {
      expect(p.replicas).toHaveLength(3);
      expect(new Set(p.replicas.map((r) => r.logEndOffset)).size).toBe(1);
      expect(p.highWatermark).toBe(p.logEndOffset);
    }
  });

  it('acks=0 with RF 3: nothing waits at the leader, records still replicate', () => {
    const { engine, context } = createEngineInternal({ seed: 1, topology: cluster({ acks: '0' }) });
    engine.runUntil(1001);
    expect(context.state.parkedProduces.size).toBe(0);
    engine.runUntil(1100);
    expect(partition0(engine)?.highWatermark).toBe(1);
  });

  it('acks=1 with RF 3: the consumer gets a record only after the followers have it', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ acks: '1', consumer: true }) });
    const events = record(engine);
    engine.runUntil(8000);
    const appended = ofType(events, 'RecordAppended');
    const consumed = ofType(events, 'RecordConsumed');
    expect(consumed.length).toBeGreaterThan(0);
    for (const c of consumed) {
      const append = appended.find((a) => a.offset === c.offset);
      const replicated = ofType(events, 'ReplicaFetched').filter(
        (r) => r.toOffset > c.offset && r.t <= c.t,
      );
      expect(append).toBeDefined();
      expect(new Set(replicated.map((r) => r.broker)).size).toBe(2);
    }
  });
});

describe('ISR management (M4-T4)', () => {
  /**
   * RF 3, acks=all: one follower stops replicating at 500 ms but keeps heartbeating to the
   * controller (as with a stuck fetcher thread), so only the leader's lag check removes it.
   */
  function oneFollowerStops() {
    const { engine, context, kernel } = createEngineInternal({
      seed: 1,
      topology: cluster({ consumer: true }),
    });
    const events = record(engine);
    engine.runUntil(500);
    const p = partition0(engine);
    const follower = p?.replicas.map((r) => r.broker).find((b) => b !== p.leader) ?? '';
    const epoch = context.state.brokers.get(follower)?.brokerEpoch ?? -1;
    engine.dispatch({ type: 'KillBroker', id: follower });
    for (let t = 2000; t <= 18_000; t += 2000) {
      kernel.scheduleAt(t, {
        type: 'BrokerHeartbeatRequest',
        broker: follower,
        incarnation: 0,
        brokerEpoch: epoch,
        wantShutDown: false,
      });
    }
    return { engine, events, follower, leader: p?.leader ?? '' };
  }

  it('a follower that stops replicating leaves the ISR after replica.lag.time.max.ms (± the check interval)', () => {
    const { engine, events, follower, leader } = oneFollowerStops();
    engine.runUntil(20_000);
    const shrunk = ofType(events, 'IsrShrunk');
    expect(shrunk).toHaveLength(1);
    // Last caught up at ~500 ms; out of sync once 10 s have passed; checks run every 5 s.
    expect(shrunk[0]?.t).toBeGreaterThanOrEqual(10_500);
    expect(shrunk[0]?.t).toBeLessThanOrEqual(10_500 + 5000 + 2);
    expect(shrunk[0]).toMatchObject({ removed: [follower], elr: [] });
    expect(partition0(engine)?.isr).not.toContain(follower);
    expect(partition0(engine)?.isr).toContain(leader);
    expect(partition0(engine)?.underReplicated).toBe(true);
    expect(partition0(engine)?.partitionEpoch).toBe(1);
  });

  it('the HW moves on as soon as the slow follower leaves the ISR: parked acks=all writes complete', () => {
    const { engine, events } = oneFollowerStops();
    engine.runUntil(20_000);
    const shrunkAt = ofType(events, 'IsrShrunk')[0]?.t ?? 0;
    const acked = ofType(events, 'ProduceAcked');
    // Records waiting when the shrink is confirmed (+1 ms) are acked right after (+1 ms).
    expect(acked[0]?.t).toBe(shrunkAt + 2);
    // Records appended before the shrink but within request.timeout.ms were waiting: acked now.
    const failed = ofType(events, 'ProduceFailed');
    for (const f of failed) expect(f.error).toBe('REQUEST_TIMED_OUT');
    expect(acked.length + failed.length).toBe(19);
    // The consumer sees everything below the HW.
    expect(ofType(events, 'RecordConsumed').length).toBeGreaterThan(0);
  });

  it('a follower that catches up rejoins the ISR', () => {
    const { engine, events, follower } = oneFollowerStops();
    // Its (injected) heartbeats stop at 18 s: fenced at 27 s; restarted at 28 s, it registers.
    engine.runUntil(28_000);
    engine.dispatch({ type: 'RestartBroker', id: follower });
    engine.runUntil(28_100);
    const expanded = ofType(events, 'IsrExpanded');
    expect(expanded).toHaveLength(1);
    expect(expanded[0]).toMatchObject({ added: [follower] });
    expect(expanded[0]?.t).toBeLessThan(28_010);
    expect(partition0(engine)?.isr).toContain(follower);
    expect(partition0(engine)?.underReplicated).toBe(false);
    const p = partition0(engine);
    expect(new Set(p?.replicas.map((r) => r.logEndOffset)).size).toBe(1);
  });

  it('shrinking below min.insync.replicas moves the removed replicas into the ELR (KIP-966)', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ minIsr: 2 }) });
    const events = record(engine);
    engine.runUntil(500);
    const p = partition0(engine);
    const followers = p?.replicas.map((r) => r.broker).filter((b) => b !== p.leader) ?? [];
    for (const b of followers) engine.dispatch({ type: 'KillBroker', id: b });
    engine.runUntil(16_000);
    // Both are fenced at 9 s and removed one after the other. Only the second removal takes the
    // ISR below minISR 2, so only that replica becomes an ELR member (K1).
    const shrunk = ofType(events, 'IsrShrunk');
    expect(shrunk).toHaveLength(2);
    expect(shrunk[0]?.elr).toEqual([]);
    expect(shrunk[1]?.elr).toEqual(shrunk[1]?.removed);
    expect(partition0(engine)?.elr).toHaveLength(1);
  });

  it('expanding back to min.insync.replicas clears the ELR', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ minIsr: 2 }) });
    engine.runUntil(500);
    const p = partition0(engine);
    const followers = p?.replicas.map((r) => r.broker).filter((b) => b !== p.leader) ?? [];
    for (const b of followers) engine.dispatch({ type: 'KillBroker', id: b });
    engine.runUntil(16_000);
    engine.dispatch({ type: 'RestartBroker', id: followers[0] ?? '' });
    engine.runUntil(17_000);
    expect(partition0(engine)?.isr).toHaveLength(2);
    expect(partition0(engine)?.elr).toEqual([]);
  });
});

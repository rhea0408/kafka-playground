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

  it('a follower that stops fetching holds the HW back; acks=all times out (REQUEST_TIMED_OUT)', () => {
    const engine = createEngine({ seed: 1, topology: cluster({ consumer: true }) });
    const events = record(engine);
    engine.runUntil(500);
    const p = partition0(engine);
    const follower = p?.replicas.map((r) => r.broker).find((b) => b !== p.leader) ?? '';
    // Until T5 a killed broker only stops its own fetching; the leader keeps it in the ISR.
    engine.dispatch({ type: 'KillBroker', id: follower });
    engine.runUntil(12_100);
    expect(partition0(engine)?.logEndOffset).toBe(12);
    expect(partition0(engine)?.highWatermark).toBe(0);
    const failed = ofType(events, 'ProduceFailed');
    // request.timeout.ms (10 s game-tuned) after each append at 1001, 2001, …
    expect(failed[0]).toMatchObject({ error: 'REQUEST_TIMED_OUT', t: 11_002 });
    expect(ofType(events, 'ProduceAcked')).toEqual([]);
    // The consumer reads only below the HW: nothing.
    expect(ofType(events, 'RecordConsumed')).toEqual([]);
  });
});

describe('min.insync.replicas', () => {
  /** RF 3, minISR 2, with the ISR forced down to the leader alone. */
  function underMinIsr(acks: '1' | 'all') {
    const { engine, context } = createEngineInternal({
      seed: 1,
      topology: cluster({ acks, minIsr: 2, consumer: true }),
    });
    const p = context.state.topics.get('orders')?.partitions[0];
    if (p === undefined || p.leader === null) throw new Error('no partition');
    p.isr = [p.leader];
    return { engine, events: record(engine) };
  }

  it('acks=all with the ISR below minISR fails with NOT_ENOUGH_REPLICAS, nothing is appended', () => {
    const { engine, events } = underMinIsr('all');
    engine.runUntil(1100);
    expect(ofType(events, 'ProduceFailed').map((e) => [e.error, e.t])).toEqual([
      ['NOT_ENOUGH_REPLICAS', 1002],
    ]);
    expect(ofType(events, 'RecordAppended')).toEqual([]);
  });

  it('strict min ISR: acks=1 writes are appended and acked, but the HW does not move', () => {
    const { engine, events } = underMinIsr('1');
    engine.runUntil(5100);
    expect(ofType(events, 'ProduceAcked')).toHaveLength(5);
    const p = partition0(engine);
    expect(p?.logEndOffset).toBe(5);
    expect(p?.highWatermark).toBe(0);
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
    parkProduce(context, target, 0, 10_000);
    const leaderLog = p.replicaLogs.get(p.leader);
    if (leaderLog === undefined) throw new Error('setup');
    leaderLog.highWatermark = 1;
    p.isr = [p.leader];
    completeDelayedProduces(context, topic, p);
    engine.runUntil(10);
    expect(context.state.parkedProduces.size).toBe(0);
    // The answer goes to producer p1's incarnation 1 (dropped if that life is gone).
    expect(events.filter((e) => e.type === 'ProduceFailed' || e.type === 'ProduceAcked')).toEqual(
      ofType(events, 'ProduceFailed').filter((e) => e.error === 'NOT_ENOUGH_REPLICAS_AFTER_APPEND'),
    );
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

import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine, replayCommandLog, type Engine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';
import { createRng } from '../core/rng.ts';
import { chooseKey } from './produce.ts';
import { partitionForKey } from './partitioner.ts';

function record(engine: Engine): EngineEvent[] {
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return events;
}

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

function topology(producer: Record<string, unknown> = {}, partitions = 3): TopologyInput {
  return {
    brokers: [{ kind: 'broker', id: 'b1' }],
    topics: [{ kind: 'topic', id: 'orders', partitions }],
    producers: [{ kind: 'producer', id: 'p1', ...producer }],
    connections: [{ from: 'p1', to: 'orders' }],
  };
}

describe('producer rate', () => {
  it('2 msg/s gives one record every 500 ms: 20 records in 10 s', () => {
    const engine = createEngine({ seed: 1, topology: topology({ ratePerSec: 2 }) });
    const events = record(engine);
    engine.runUntil(10_000);
    const times = ofType(events, 'RecordProduced').map((e) => e.t);
    expect(times).toEqual(Array.from({ length: 20 }, (_, i) => (i + 1) * 500));
  });

  it('3 msg/s spaces records by floor(n * 1000 / 3) without drift: 30 in 10 s', () => {
    const engine = createEngine({ seed: 1, topology: topology({ ratePerSec: 3 }) });
    const events = record(engine);
    engine.runUntil(10_000);
    const times = ofType(events, 'RecordProduced').map((e) => e.t);
    expect(times).toHaveLength(30);
    expect(times.slice(0, 4)).toEqual([333, 666, 1000, 1333]);
    expect(times.at(-1)).toBe(10_000);
  });

  it('a producer added later starts its schedule from that moment', () => {
    const engine = createEngine({
      seed: 1,
      topology: { ...topology(), producers: [], connections: [] },
    });
    const events = record(engine);
    engine.runUntil(250);
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'producer', id: 'p1', ratePerSec: 4 },
    });
    engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' });
    engine.runUntil(1250);
    expect(ofType(events, 'RecordProduced').map((e) => e.t)).toEqual([500, 750, 1000, 1250]);
  });

  it('a producer with no topic produces nothing, and starts once connected', () => {
    const engine = createEngine({ seed: 1, topology: { ...topology(), connections: [] } });
    const events = record(engine);
    engine.runUntil(3000);
    expect(ofType(events, 'RecordProduced')).toEqual([]);
    engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' });
    engine.runUntil(4000);
    expect(ofType(events, 'RecordProduced').map((e) => e.t)).toEqual([4000]);
  });

  it('removing a producer stops it; re-adding the same id starts a fresh schedule', () => {
    const engine = createEngine({ seed: 1, topology: topology({ ratePerSec: 2 }) });
    const events = record(engine);
    engine.runUntil(1200);
    engine.dispatch({ type: 'RemoveComponent', id: 'p1' });
    engine.runUntil(3000);
    expect(ofType(events, 'RecordProduced').map((e) => e.t)).toEqual([500, 1000]);
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'producer', id: 'p1', ratePerSec: 2 },
    });
    engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' });
    engine.runUntil(4000);
    expect(ofType(events, 'RecordProduced').map((e) => e.t)).toEqual([500, 1000, 3500, 4000]);
    // The new life gets a new incarnation, so its trace ids never repeat the old ones.
    expect(ofType(events, 'RecordProduced').map((e) => e.traceId)).toEqual([
      'p1@1#1',
      'p1@1#2',
      'p1@2#1',
      'p1@2#2',
    ]);
  });
});

describe('event order and acks', () => {
  it.each(['1', 'all'] as const)(
    'acks=%s: RecordProduced → RecordAppended (+1 ms) → ProduceAcked with the offset (+1 ms)',
    (acks) => {
      const engine = createEngine({
        seed: 1,
        topology: topology({ acks, keyStrategy: { type: 'fixed', key: 'A' } }),
      });
      const events = record(engine);
      engine.runUntil(1100);
      const p = partitionForKey('A', 3);
      expect(events.map((e) => [e.t, e.type])).toEqual([
        [1000, 'RecordProduced'],
        [1001, 'RecordAppended'],
        [1002, 'ProduceAcked'],
      ]);
      expect(events[1]).toMatchObject({
        topic: 'orders',
        partition: p,
        offset: 0,
        key: 'A',
        traceId: 'p1@1#1',
      });
      expect(events[2]).toMatchObject({ partition: p, offset: 0, traceId: 'p1@1#1' });
    },
  );

  it('acks=0: acked at send time with no offset, before the broker has appended it', () => {
    const engine = createEngine({ seed: 1, topology: topology({ acks: '0' }) });
    const events = record(engine);
    engine.runUntil(1100);
    expect(events.map((e) => [e.t, e.type])).toEqual([
      [1000, 'RecordProduced'],
      [1000, 'ProduceAcked'],
      [1001, 'RecordAppended'],
    ]);
    expect(events[1]).toMatchObject({ offset: null });
  });

  it('respects a configured network delay', () => {
    const engine = createEngine({ seed: 1, networkDelayMs: 20, topology: topology() });
    const events = record(engine);
    engine.runUntil(1100);
    expect(events.map((e) => e.t)).toEqual([1000, 1020, 1040]);
  });

  it('reports UNKNOWN_TOPIC_OR_PARTITION when the topic is deleted while a request is in flight', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    const events = record(engine);
    engine.runUntil(1000);
    engine.dispatch({ type: 'RemoveComponent', id: 'orders' });
    engine.runUntil(1100);
    expect(ofType(events, 'ProduceFailed')).toEqual([
      expect.objectContaining({ t: 1002, error: 'UNKNOWN_TOPIC_OR_PARTITION', traceId: 'p1@1#1' }),
    ]);
    expect(engine.getSnapshot().producers[0]?.stats).toEqual({ sent: 1, acked: 0, failed: 1 });
  });

  it('counts sent and acked records in the snapshot', () => {
    const engine = createEngine({ seed: 1, topology: topology({ ratePerSec: 2 }) });
    engine.runUntil(5001);
    expect(engine.getSnapshot().producers[0]?.stats).toEqual({ sent: 10, acked: 9, failed: 0 });
  });
});

describe('partitioning in the producer', () => {
  it('keyed records go to partitionForKey(key) and keep their order within the partition', () => {
    const engine = createEngine({
      seed: 5,
      topology: topology({
        ratePerSec: 10,
        keyStrategy: { type: 'random', keys: ['A', 'B', 'C'] },
      }),
    });
    const events = record(engine);
    engine.runUntil(10_000);
    const appended = ofType(events, 'RecordAppended');
    for (const e of appended) expect(e.partition).toBe(partitionForKey(e.key ?? '', 3));
    for (const key of ['A', 'B', 'C']) {
      const traceNumbers = appended
        .filter((e) => e.key === key)
        .map((e) => Number(e.traceId.split('#')[1]));
      expect(traceNumbers).toEqual([...traceNumbers].sort((x, y) => x - y));
    }
  });

  it('null keys stick to a partition for batch.size / message size records', () => {
    const engine = createEngine({
      seed: 3,
      topology: topology({ ratePerSec: 10, batchSize: 4096, messageSizeBytes: 1024 }),
    });
    const events = record(engine);
    engine.runUntil(4000);
    const partitions = ofType(events, 'RecordProduced').map((e) => e.partition);
    expect(partitions).toHaveLength(40);
    for (let b = 0; b < 10; b++) expect(new Set(partitions.slice(b * 4, b * 4 + 4)).size).toBe(1);
  });

  it('ProduceManual sends one record now with the given key and value', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    const events = record(engine);
    engine.runUntil(10);
    expect(
      engine.dispatch({ type: 'ProduceManual', producerId: 'p1', key: 'B', value: 'hello' }),
    ).toEqual({ ok: true });
    engine.runUntil(20);
    expect(ofType(events, 'RecordAppended')).toEqual([
      expect.objectContaining({
        t: 11,
        key: 'B',
        value: 'hello',
        partition: partitionForKey('B', 3),
      }),
    ]);
    expect(
      engine.dispatch({ type: 'ProduceManual', producerId: 'nope', key: null, value: 'x' }),
    ).toMatchObject({ ok: false });
  });
});

describe('chooseKey', () => {
  it('null / fixed return the strategy key', () => {
    const rng = createRng(1);
    expect(chooseKey({ type: 'null' }, rng)).toBeNull();
    expect(chooseKey({ type: 'fixed', key: 'K' }, rng)).toBe('K');
  });

  it('random keys are roughly uniform', () => {
    const rng = createRng(1);
    const counts = new Map<string | null, number>();
    for (let i = 0; i < 3000; i++) {
      const k = chooseKey({ type: 'random', keys: ['A', 'B', 'C'] }, rng);
      counts.set(k, (counts.get(k) ?? 0) + 1);
    }
    for (const k of ['A', 'B', 'C']) expect(counts.get(k)).toBeGreaterThan(900);
  });

  it('skewed keys use the hot key for about hotShare of records', () => {
    const rng = createRng(1);
    let hot = 0;
    for (let i = 0; i < 10_000; i++) {
      if (
        chooseKey({ type: 'skewed', hotKey: 'H', hotShare: 0.8, otherKeys: ['x', 'y'] }, rng) ===
        'H'
      )
        hot++;
    }
    expect(hot).toBeGreaterThan(7800);
    expect(hot).toBeLessThan(8200);
  });
});

describe('replay with scheduled work', () => {
  it('reproduces a run where commands were dispatched between tasks of the same instant', () => {
    const options = {
      seed: 8,
      topology: {
        ...topology({ ratePerSec: 4, keyStrategy: { type: 'random', keys: ['A', 'B'] } }),
        producers: [
          {
            kind: 'producer' as const,
            id: 'p1',
            ratePerSec: 4,
            keyStrategy: { type: 'random' as const, keys: ['A', 'B'] },
          },
          { kind: 'producer' as const, id: 'p2', ratePerSec: 4 },
        ],
        connections: [
          { from: 'p1', to: 'orders' },
          { from: 'p2', to: 'orders' },
        ],
      },
    };
    const original = createEngine(options);
    const originalEvents = record(original);
    original.runUntil(999);
    original.step(); // p1's tick at t=1000 runs; p2's tick at t=1000 is still queued
    expect(original.now()).toBe(1000);
    original.dispatch({ type: 'ProduceManual', producerId: 'p2', key: 'Z', value: 'mid-instant' });
    original.step();
    original.dispatch({ type: 'Disconnect', from: 'p1', to: 'orders' });
    original.runUntil(3000);
    original.dispatch({ type: 'RemoveComponent', id: 'p2' });
    original.runUntil(6000);

    const replayedEvents: EngineEvent[] = [];
    const replayed = replayCommandLog(options, original.commandLog, (e) => replayedEvents.push(e));
    replayed.runUntil(6000);
    expect(JSON.stringify(replayedEvents)).toBe(JSON.stringify(originalEvents));
    expect(replayed.getSnapshot()).toEqual(original.getSnapshot());

    // The naive replay (run everything up to t, then dispatch) gives a different event order.
    const naive = createEngine(options);
    const naiveEvents = record(naive);
    for (const entry of original.commandLog) {
      naive.runUntil(entry.t);
      naive.dispatch(entry.command);
    }
    naive.runUntil(6000);
    expect(JSON.stringify(naiveEvents)).not.toBe(JSON.stringify(originalEvents));
  });
});

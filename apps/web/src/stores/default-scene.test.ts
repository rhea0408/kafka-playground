import { createEngine, partitionForKey, type EngineEvent } from '@kp/engine';
import { describe, expect, it } from 'vitest';
import { GRID_SIZE, nodeSize } from '../canvas/geometry.ts';
import {
  DEFAULT_CONSUMER,
  DEFAULT_KEYS,
  DEFAULT_PARTITIONS,
  DEFAULT_TOPIC,
  defaultSceneState,
  initialSceneFor,
  sceneById,
  SCENES,
} from './default-scene.ts';
import { DEFAULT_SEED } from '../sim/bootstrap.ts';
import { INITIAL_TOPOLOGY_STATE, toTopologyInput } from './topology-store.ts';

describe('default scene keys', () => {
  it('D, E and F land on three different partitions (murmur2 % 3): 0, 1, 2', () => {
    expect(DEFAULT_KEYS.map((key) => partitionForKey(key, DEFAULT_PARTITIONS))).toEqual([0, 1, 2]);
  });

  it('A, B and C would all land on partition 1 (why the scene does not use them)', () => {
    expect(['A', 'B', 'C'].map((key) => partitionForKey(key, 3))).toEqual([1, 1, 1]);
  });
});

describe('defaultSceneState', () => {
  const scene = defaultSceneState();

  it('is accepted by the engine and matches the parsed configs the engine holds', () => {
    const engine = createEngine({ seed: 7, topology: toTopologyInput(scene) });
    const snapshot = engine.getSnapshot();
    expect(snapshot.topics.map((t) => [t.config.id, t.partitions.length])).toEqual([
      [DEFAULT_TOPIC, DEFAULT_PARTITIONS],
    ]);
    const producer = snapshot.producers[0]?.config;
    expect(producer).toEqual(scene.components.find((c) => c.kind === 'producer'));
    expect(producer?.ratePerSec).toBe(2);
    expect(snapshot.consumers[0]?.config.groupId).toBe('orders-app');
  });

  it('has a grid-snapped position for every component and no overlapping nodes', () => {
    const rects = scene.components.map((c) => {
      const position = scene.positions[c.id];
      if (position === undefined) throw new Error(`no position for ${c.id}`);
      expect(position.x % GRID_SIZE).toBe(0);
      expect(position.y % GRID_SIZE).toBe(0);
      const size =
        c.kind === 'topic'
          ? nodeSize(c.kind, c.partitions, 1, c.replicationFactor)
          : nodeSize(c.kind);
      return { ...position, ...size };
    });
    for (const [i, a] of rects.entries()) {
      for (const b of rects.slice(i + 1)) {
        const overlap =
          a.x < b.x + b.width &&
          b.x < a.x + a.width &&
          a.y < b.y + b.height &&
          b.y < a.y + a.height;
        expect(overlap).toBe(false);
      }
    }
    // Fits the smallest supported canvas (1366 × 768 → 766 × 512 at zoom 1): no minimap.
    expect(Math.max(...rects.map((r) => r.x + r.width))).toBeLessThanOrEqual(766);
    expect(Math.max(...rects.map((r) => r.y + r.height))).toBeLessThanOrEqual(512);
  });

  it('fills every partition and keeps committed lag at or below rate × auto-commit interval', () => {
    const engine = createEngine({ seed: 7, topology: toTopologyInput(scene) });
    let maxLag = 0;
    for (let t = 100; t <= 120_000; t += 100) {
      engine.runUntil(t);
      const consumer = engine.getSnapshot().consumers.find((c) => c.config.id === DEFAULT_CONSUMER);
      maxLag = Math.max(maxLag, consumer?.totalLag ?? 0);
    }
    const partitions = engine.getSnapshot().topics[0]?.partitions ?? [];
    expect(partitions.every((p) => p.highWatermark > 0)).toBe(true);
    // 2 msg/s × auto.commit.interval.ms 5 s = 10 records between commits (e2e bound: 15).
    expect(maxLag).toBeGreaterThan(0);
    expect(maxLag).toBeLessThanOrEqual(10);
  });
});

describe('initialSceneFor', () => {
  it('opens the default scene unless ?scene=empty asks for the bare canvas', () => {
    expect(initialSceneFor('')).toEqual(defaultSceneState());
    expect(initialSceneFor('?fps')).toEqual(defaultSceneState());
    expect(initialSceneFor('?scene=empty')).toBe(INITIAL_TOPOLOGY_STATE);
    expect(initialSceneFor('?fps&scene=empty')).toBe(INITIAL_TOPOLOGY_STATE);
  });
});

describe('M3 scene presets (M3-T12)', () => {
  const run = (id: string, steps: (engine: ReturnType<typeof createEngine>) => void) => {
    const scene = sceneById(id);
    if (scene === undefined) throw new Error(id);
    const engine = createEngine({ seed: DEFAULT_SEED, topology: toTopologyInput(scene.state()) });
    const events: EngineEvent[] = [];
    engine.on((e) => events.push(e));
    steps(engine);
    return events;
  };

  it('every scene parses, and ?scene=<id> picks it', () => {
    for (const scene of SCENES) expect(() => toTopologyInput(scene.state())).not.toThrow();
    expect(
      initialSceneFor('?scene=idle').components.filter((c) => c.kind === 'consumer'),
    ).toHaveLength(4);
    expect(initialSceneFor('?scene=nope')).toEqual(defaultSceneState());
  });

  it('idle: four members on three partitions leave one idle', () => {
    const events = run('idle', (e) => {
      e.runUntil(10_000);
    });
    expect(events.filter((e) => e.type === 'ConsumerIdle')).toHaveLength(1);
  });

  it('crash: killing a member mid-batch shows duplicates after the session timeout', () => {
    const events = run('crash', (e) => {
      e.runUntil(13_050); // after the first auto-commit (~11 s), mid-batch
      e.dispatch({ type: 'KillConsumer', id: 'c1' });
      e.runUntil(30_000);
    });
    expect(events.some((e) => e.type === 'MemberRemoved' && e.reason === 'session-timeout')).toBe(
      true,
    );
    expect(events.some((e) => e.type === 'DuplicateProcessed')).toBe(true);
    expect(events.some((e) => e.type === 'RecordsSkipped')).toBe(false);
  });

  it('slow: c2 exceeds max.poll.interval.ms and is removed (poll timeout)', () => {
    const events = run('slow', (e) => {
      e.runUntil(90_000);
    });
    expect(
      events.some(
        (e) => e.type === 'MemberRemoved' && e.consumerId === 'c2' && e.reason === 'poll-timeout',
      ),
    ).toBe(true);
  });
});

describe('M4 scene presets (M4-T12)', () => {
  function start(id: string) {
    const scene = sceneById(id);
    if (scene === undefined) throw new Error(`no scene ${id}`);
    const state = scene.state();
    const engine = createEngine({
      seed: DEFAULT_SEED,
      topology: toTopologyInput(state),
      ...(state.networkDelayMs === undefined ? {} : { networkDelayMs: state.networkDelayMs }),
    });
    const events: EngineEvent[] = [];
    engine.on((e) => events.push(e));
    const p0Leader = () =>
      engine.getSnapshot().topics.find((t) => t.config.id === 'orders')?.partitions[0]?.leader ??
      '';
    return { engine, events, p0Leader };
  }
  const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
    events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

  it('every scene has three brokers and an RF 3 topic', () => {
    for (const id of ['failover', 'acks1-loss', 'not-enough-replicas', 'coordinator-failover']) {
      const state = sceneById(id)?.state();
      expect(state?.components.filter((c) => c.kind === 'broker')).toHaveLength(3);
      expect(state?.components.find((c) => c.kind === 'topic')).toMatchObject({
        replicationFactor: 3,
      });
    }
  });

  it('leader failover: a new leader about 9 s after the kill, nothing failed, nothing acked lost', () => {
    const { engine, events, p0Leader } = start('failover');
    engine.runUntil(5000);
    const leader = p0Leader();
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.runUntil(16_000);
    const elected = ofType(events, 'LeaderElected').find(
      (e) => e.topic === 'orders' && e.partition === 0,
    );
    expect(elected?.previousLeader).toBe(leader);
    expect(elected?.t).toBeGreaterThan(5000 + 7000);
    expect(elected?.t).toBeLessThanOrEqual(5000 + 9001);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(25_000);
    expect(ofType(events, 'ProduceFailed')).toEqual([]);
    expect(ofType(events, 'RecordsTruncated').reduce((n, e) => n + e.acked, 0)).toBe(0);
    expect(engine.getSnapshot().topics[0]?.partitions[0]?.isr).toContain(leader);
  });

  it('acks=1 data loss: the restarted old leader truncates acknowledged records', () => {
    const { engine, events, p0Leader } = start('acks1-loss');
    engine.runUntil(5000);
    const leader = p0Leader();
    engine.dispatch({ type: 'KillBroker', id: leader });
    engine.runUntil(17_000);
    engine.dispatch({ type: 'RestartBroker', id: leader });
    engine.runUntil(22_000);
    const truncated = ofType(events, 'RecordsTruncated').filter((e) => e.topic === 'orders');
    expect(truncated.reduce((n, e) => n + e.acked, 0)).toBeGreaterThan(0);
    expect(engine.getSnapshot().topics[0]?.partitions[0]?.lostAcked).toBeGreaterThan(0);
  });

  it('not enough replicas: with two followers gone, acks=all is refused and retried', () => {
    const { engine, events, p0Leader } = start('not-enough-replicas');
    engine.runUntil(5000);
    const p0 = engine.getSnapshot().topics[0]?.partitions[0];
    for (const r of p0?.replicas ?? []) {
      if (r.broker !== p0Leader()) engine.dispatch({ type: 'KillBroker', id: r.broker });
    }
    engine.runUntil(20_000);
    expect(ofType(events, 'ProduceRetried').some((e) => e.error === 'NOT_ENOUGH_REPLICAS')).toBe(
      true,
    );
    expect(engine.getSnapshot().producers[0]?.retrying).toBe('NOT_ENOUGH_REPLICAS');
  });

  it('coordinator failover: the group moves and stays Stable with its offsets', () => {
    const { engine, events } = start('coordinator-failover');
    engine.runUntil(10_000);
    const coordinator = engine
      .getSnapshot()
      .brokers.find((b) => b.coordinatorFor.includes('orders-app'));
    const generation = engine.getSnapshot().groups[0]?.generation;
    engine.dispatch({ type: 'KillBroker', id: coordinator?.config.id ?? '' });
    engine.runUntil(30_000);
    expect(ofType(events, 'CoordinatorMoved')).toMatchObject([
      { groupId: 'orders-app', state: 'Stable', generation },
    ]);
    const group = engine.getSnapshot().groups.find((g) => g.groupId === 'orders-app');
    expect(group?.state).toBe('Stable');
    expect(group?.generation).toBe(generation);
  });
});

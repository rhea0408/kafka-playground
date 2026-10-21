import { createEngine, partitionForKey } from '@kp/engine';
import { describe, expect, it } from 'vitest';
import { GRID_SIZE, nodeSize } from '../canvas/geometry.ts';
import {
  DEFAULT_CONSUMER,
  DEFAULT_KEYS,
  DEFAULT_PARTITIONS,
  DEFAULT_TOPIC,
  defaultSceneState,
  initialSceneFor,
} from './default-scene.ts';
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
      const size = nodeSize(c.kind, c.kind === 'topic' ? c.partitions : 1);
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

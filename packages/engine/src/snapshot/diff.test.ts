import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine } from '../engine.ts';
import { applyDiff, diffSnapshot, isEmptyDiff } from './diff.ts';

const topology: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1' }],
  topics: [{ kind: 'topic', id: 'orders', partitions: 2 }],
  producers: [
    { kind: 'producer', id: 'p1', ratePerSec: 4, keyStrategy: { type: 'fixed', key: 'A' } },
  ],
  consumers: [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest' }],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'orders', to: 'c1' },
  ],
};

/** True if `value` holds only plain objects, arrays and primitives (structured-clone and JSON safe). */
function isPlainData(value: unknown): boolean {
  if (value === null || ['string', 'number', 'boolean', 'undefined'].includes(typeof value)) {
    return true;
  }
  if (Array.isArray(value)) return value.every(isPlainData);
  if (typeof value !== 'object') return false;
  const proto: unknown = Object.getPrototypeOf(value);
  return (proto === Object.prototype || proto === null) && Object.values(value).every(isPlainData);
}

describe('diffSnapshot / applyDiff', () => {
  it('produces an empty diff when nothing changed', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(3000);
    const a = engine.getSnapshot();
    const b = engine.getSnapshot();
    expect(diffSnapshot(a, b)).toEqual({});
    expect(isEmptyDiff(diffSnapshot(a, b))).toBe(true);
    expect(diffSnapshot(a, a)).toEqual({});
    expect(applyDiff(a, {})).toBe(a);
  });

  it('sends only the sim time when only time moved', () => {
    const engine = createEngine({ seed: 1, topology: { brokers: [{ kind: 'broker', id: 'b1' }] } });
    const a = engine.getSnapshot();
    engine.runUntil(5000);
    expect(diffSnapshot(a, engine.getSnapshot())).toEqual({ t: 5000 });
  });

  it('sends only the new records of a partition, and how many drop off the window', () => {
    const engine = createEngine({ seed: 1, topology, snapshotRecordsPerPartition: 3 });
    // Key "A" → one partition. Records are produced every 250 ms and appended 1 ms later.
    engine.runUntil(1000); // appended at 251, 501, 751: offsets 0-2
    const prev = engine.getSnapshot();
    engine.runUntil(1600); // appended at 1001, 1251, 1501: offsets 3-5
    const next = engine.getSnapshot();
    const diff = diffSnapshot(prev, next);
    const topic = diff.topics?.changed?.[0];
    const changed = topic !== undefined && 'partitions' in topic ? topic.partitions : undefined;
    expect(changed).toHaveLength(1);
    const p = changed?.[0];
    expect(p !== undefined && 'append' in p ? p.append.map((r) => r.offset) : null).toEqual([
      3, 4, 5,
    ]);
    expect(p !== undefined && 'drop' in p ? p.drop : null).toBe(3); // the window holds 3 records
    expect(applyDiff(prev, diff)).toEqual(next);
  });

  it('lists removed ids and sends a re-added component in its new place', () => {
    const engine = createEngine({
      seed: 1,
      topology: {
        ...topology,
        producers: [
          { kind: 'producer', id: 'p1', ratePerSec: 4 },
          { kind: 'producer', id: 'p2', ratePerSec: 4 },
        ],
      },
    });
    engine.runUntil(1000);
    const prev = engine.getSnapshot();
    expect(engine.dispatch({ type: 'RemoveComponent', id: 'c1' }).ok).toBe(true);
    expect(engine.dispatch({ type: 'RemoveComponent', id: 'p1' }).ok).toBe(true);
    expect(
      engine.dispatch({ type: 'AddComponent', component: { kind: 'producer', id: 'p1' } }).ok,
    ).toBe(true);
    const next = engine.getSnapshot();
    const diff = diffSnapshot(prev, next);
    expect(diff.consumers?.removed).toEqual(['c1']);
    expect(diff.producers?.order).toEqual(['p2', 'p1']);
    expect(applyDiff(prev, diff)).toEqual(next);
  });

  it('sends a deleted and re-created topic whole, even when its offsets look the same', () => {
    const one: TopologyInput = { ...topology, consumers: [], connections: [] };
    const engine = createEngine({ seed: 1, topology: one });
    engine.dispatch({ type: 'ProduceManual', producerId: 'p1', key: 'A', value: 'old' });
    engine.runUntil(10);
    const prev = engine.getSnapshot();
    engine.dispatch({ type: 'RemoveComponent', id: 'orders' });
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'topic', id: 'orders', partitions: 2 },
    });
    engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' });
    engine.dispatch({ type: 'ProduceManual', producerId: 'p1', key: 'A', value: 'new' });
    engine.runUntil(20);
    const next = engine.getSnapshot();
    expect(next.topics[0]?.topicId).not.toBe(prev.topics[0]?.topicId);
    const diff = diffSnapshot(prev, next);
    expect(diff.topics?.changed?.[0]).toHaveProperty('full');
    expect(applyDiff(prev, diff)).toEqual(next);
  });

  it('is plain data and does not modify prev', () => {
    const engine = createEngine({ seed: 3, topology });
    engine.runUntil(2000);
    const prev = engine.getSnapshot();
    const before = JSON.stringify(prev);
    engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 4 } });
    engine.dispatch({ type: 'UpdateConfig', id: 'p1', patch: { ratePerSec: 9 } });
    engine.runUntil(4000);
    const next = engine.getSnapshot();
    const diff = diffSnapshot(prev, next);
    expect(isPlainData(diff)).toBe(true);
    expect(JSON.parse(JSON.stringify(diff))).toEqual(diff);
    const applied = applyDiff(prev, diff);
    expect(applied).toEqual(next);
    expect(JSON.stringify(prev)).toBe(before);
    // Structural sharing: the unchanged broker list is the same array.
    expect(applied.brokers).toBe(prev.brokers);
  });

  it('throws when a diff does not fit the snapshot it is applied to', () => {
    const engine = createEngine({ seed: 1, topology });
    const empty = createEngine({ seed: 1 }).getSnapshot();
    const base = engine.getSnapshot();
    engine.runUntil(1000);
    const diff = diffSnapshot(base, engine.getSnapshot());
    expect(() => applyDiff(empty, diff)).toThrow(/does not fit/);
  });
});

describe('getSnapshot copies', () => {
  it('keeps an old snapshot unchanged while the engine keeps running and changing', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(2000);
    const snap = engine.getSnapshot();
    const before = JSON.stringify(snap);
    engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 3 } });
    engine.dispatch({ type: 'UpdateConfig', id: 'p1', patch: { ratePerSec: 7 } });
    engine.dispatch({ type: 'UpdateConfig', id: 'c1', patch: { maxPollRecords: 3 } });
    engine.dispatch({ type: 'Disconnect', from: 'orders', to: 'c1' });
    engine.runUntil(6000);
    expect(JSON.stringify(snap)).toBe(before);
  });

  it('cannot be used to change the engine', () => {
    const engine = createEngine({ seed: 1, topology });
    engine.runUntil(2000);
    const snap = engine.getSnapshot();
    const producer = snap.producers[0];
    const record = snap.topics[0]?.partitions.find((p) => p.records.length > 0)?.records[0];
    (producer?.config as { ratePerSec: number }).ratePerSec = 1000;
    (producer?.stats as { sent: number }).sent = -1;
    (snap.connections as unknown[]).length = 0;
    // Records are shared with the engine but frozen.
    expect(() => {
      (record as { value: string }).value = 'changed';
    }).toThrow(TypeError);
    const fresh = engine.getSnapshot();
    expect(fresh.producers[0]?.config.ratePerSec).toBe(4);
    expect(fresh.producers[0]?.stats.sent).toBeGreaterThan(0);
    expect(fresh.connections).toHaveLength(2);
  });
});

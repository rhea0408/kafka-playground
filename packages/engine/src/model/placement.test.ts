import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { createRng } from '../core/rng.ts';
import { placeReplicas, type PlacementBroker } from './placement.ts';

const up = (...ids: string[]): PlacementBroker[] => ids.map((id) => ({ id, fenced: false }));

describe('placeReplicas (StripedReplicaPlacer, rack-unaware)', () => {
  it('puts every partition on the only broker', () => {
    expect(placeReplicas(3, 1, up('b1'), createRng(1))).toEqual([['b1'], ['b1'], ['b1']]);
  });

  it('gives each partition consecutive brokers, one further along per partition', () => {
    const placed = placeReplicas(3, 3, up('b3', 'b1', 'b2'), createRng(1));
    const sorted = ['b1', 'b2', 'b3'];
    const start = sorted.indexOf(placed[0]?.[0] ?? '');
    placed.forEach((replicas, p) => {
      expect(replicas).toEqual([0, 1, 2].map((i) => sorted[(start + p + i) % 3]));
    });
  });

  it('spreads preferred leaders evenly: each broker leads one of every N partitions', () => {
    for (let seed = 0; seed < 20; seed++) {
      const placed = placeReplicas(9, 2, up('b1', 'b2', 'b3'), createRng(seed));
      const leads = new Map<string, number>();
      for (const [leader] of placed)
        leads.set(leader as string, (leads.get(leader as string) ?? 0) + 1);
      expect([...leads.values()]).toEqual([3, 3, 3]);
    }
  });

  it('never leads on a fenced broker; fenced brokers are only followers, after unfenced ones', () => {
    const brokers = [...up('b1', 'b3'), { id: 'b2', fenced: true }];
    for (const replicas of placeReplicas(6, 3, brokers, createRng(4))) {
      expect(replicas[0]).not.toBe('b2');
      expect(replicas[2]).toBe('b2');
    }
  });

  it('places RF distinct brokers per partition (property)', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 5 }),
        fc.integer({ min: 1, max: 12 }),
        fc.integer({ min: 0, max: 1000 }),
        (brokerCount, partitions, seed) => {
          const ids = Array.from({ length: brokerCount }, (_, i) => `b${String(i)}`);
          const rf = 1 + (seed % brokerCount);
          const placed = placeReplicas(partitions, rf, up(...ids), createRng(seed));
          expect(placed).toHaveLength(partitions);
          for (const replicas of placed) {
            expect(new Set(replicas).size).toBe(rf);
          }
        },
      ),
    );
  });

  it('is deterministic for the same RNG stream', () => {
    expect(placeReplicas(4, 2, up('a', 'b'), createRng(9).fork('placement:t'))).toEqual(
      placeReplicas(4, 2, up('a', 'b'), createRng(9).fork('placement:t')),
    );
  });

  it('needs an unfenced broker and RF ≤ brokers', () => {
    expect(() => placeReplicas(1, 1, [], createRng(1))).toThrow(/unfenced broker/);
    expect(() => placeReplicas(1, 2, up('b1'), createRng(1))).toThrow(/replication factor/);
  });
});

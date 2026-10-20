import { describe, expect, it } from 'vitest';
import { createRng } from './rng';

const take = (n: number, f: () => number): number[] => Array.from({ length: n }, f);

describe('createRng', () => {
  it('gives the same sequence for the same seed', () => {
    const a = createRng(42);
    const b = createRng(42);
    expect(take(1000, a.nextUint32)).toEqual(take(1000, b.nextUint32));
  });

  it('gives different sequences for different seeds', () => {
    expect(take(10, createRng(1).nextUint32)).not.toEqual(take(10, createRng(2).nextUint32));
  });

  it('is pinned to known outputs, so an accidental algorithm change fails loudly', () => {
    // Regression guard captured from this implementation. If you change the algorithm on
    // purpose, update these values: saved command logs will then replay differently.
    expect(take(5, createRng(42).nextUint32)).toMatchInlineSnapshot(`
      [
        1028872839,
        2516511472,
        400437680,
        853279530,
        1920286840,
      ]
    `);
  });

  it('rejects seeds outside [0, 2^32)', () => {
    for (const bad of [-1, 1.5, 2 ** 32, Number.NaN]) {
      expect(() => createRng(bad)).toThrow(RangeError);
    }
  });

  it('next() stays in [0, 1)', () => {
    const rng = createRng(7);
    for (let i = 0; i < 10_000; i++) {
      const x = rng.next();
      expect(x).toBeGreaterThanOrEqual(0);
      expect(x).toBeLessThan(1);
    }
  });

  it('int(min, max) is inclusive, hits every value and is roughly uniform', () => {
    const rng = createRng(3);
    const counts = new Map<number, number>();
    for (let i = 0; i < 60_000; i++) {
      const x = rng.int(-2, 3);
      counts.set(x, (counts.get(x) ?? 0) + 1);
    }
    expect([...counts.keys()].sort((p, q) => p - q)).toEqual([-2, -1, 0, 1, 2, 3]);
    for (const c of counts.values()) {
      expect(c).toBeGreaterThan(9_000);
      expect(c).toBeLessThan(11_000);
    }
  });

  it('int(min, max) rejects bad ranges', () => {
    const rng = createRng(3);
    expect(() => rng.int(5, 4)).toThrow(RangeError);
    expect(() => rng.int(0, 1.5)).toThrow(RangeError);
    expect(rng.int(9, 9)).toBe(9);
  });

  it('pick() returns an element and rejects an empty array', () => {
    const rng = createRng(5);
    const items = ['A', 'B', 'C'] as const;
    const seen = new Set(take(100, () => items.indexOf(rng.pick(items))));
    expect(seen).toEqual(new Set([0, 1, 2]));
    expect(() => rng.pick([])).toThrow(RangeError);
  });

  describe('fork(label)', () => {
    it('is reproducible for the same seed and label', () => {
      expect(take(10, createRng(9).fork('producer:p1').nextUint32)).toEqual(
        take(10, createRng(9).fork('producer:p1').nextUint32),
      );
    });

    it('gives different streams for different labels, and differs from the parent', () => {
      const root = createRng(9);
      const p1 = take(10, root.fork('producer:p1').nextUint32);
      const p2 = take(10, root.fork('producer:p2').nextUint32);
      expect(p1).not.toEqual(p2);
      expect(p1).not.toEqual(take(10, createRng(9).nextUint32));
    });

    it('does not depend on how much the parent has already drawn', () => {
      const fresh = createRng(9);
      const used = createRng(9);
      take(500, used.nextUint32);
      expect(take(10, used.fork('consumer:c1').nextUint32)).toEqual(
        take(10, fresh.fork('consumer:c1').nextUint32),
      );
    });
  });
});

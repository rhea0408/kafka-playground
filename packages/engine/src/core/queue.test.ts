import { describe, expect, it } from 'vitest';
import { createEventQueue } from './queue.ts';

const drain = <T>(q: ReturnType<typeof createEventQueue<T>>): T[] => {
  const out: T[] = [];
  for (let e = q.pop(); e !== undefined; e = q.pop()) out.push(e.item);
  return out;
};

describe('createEventQueue', () => {
  it('pops in time order', () => {
    const q = createEventQueue<string>();
    q.push(300, 'c');
    q.push(100, 'a');
    q.push(200, 'b');
    expect(drain(q)).toEqual(['a', 'b', 'c']);
  });

  it('breaks ties at the same time by insertion order (seq)', () => {
    const q = createEventQueue<string>();
    const names = ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh'];
    for (const n of names) q.push(1000, n);
    q.push(500, 'earlier');
    expect(drain(q)).toEqual(['earlier', ...names]);
  });

  it('assigns increasing seq numbers, including across pops', () => {
    const q = createEventQueue<number>();
    const a = q.push(10, 1);
    q.pop();
    const b = q.push(10, 2);
    expect(b.seq).toBeGreaterThan(a.seq);
  });

  it('peek() does not remove; pop() on empty returns undefined', () => {
    const q = createEventQueue<string>();
    expect(q.peek()).toBeUndefined();
    expect(q.pop()).toBeUndefined();
    q.push(5, 'x');
    expect(q.peek()?.item).toBe('x');
    expect(q.size).toBe(1);
    expect(q.pop()?.item).toBe('x');
    expect(q.size).toBe(0);
  });

  it('rejects invalid sim times', () => {
    const q = createEventQueue<string>();
    expect(() => q.push(1.5, 'x')).toThrow(RangeError);
    expect(() => q.push(-1, 'x')).toThrow(RangeError);
  });

  // Sanity check, not a benchmark: Vitest fails the test if it exceeds the timeout. Using the
  // runner's timeout keeps wall-clock reads (performance.now) out of the engine package.
  it('handles 100k push/pop within 2 s', { timeout: 2000 }, () => {
    const q = createEventQueue<number>();
    const n = 100_000;
    // Deterministic pseudo-random times without an Rng dependency in this test.
    for (let i = 0; i < n; i++) q.push((i * 7919) % 10_007, i);
    let last = -1;
    for (let i = 0; i < n; i++) {
      const e = q.pop();
      if (e === undefined) throw new Error('queue drained early');
      expect(e.time).toBeGreaterThanOrEqual(last);
      last = e.time;
    }
    expect(q.size).toBe(0);
  });
});

import { describe, expect, it } from 'vitest';
import { RingBuffer } from './ring-buffer.ts';

describe('RingBuffer', () => {
  it('keeps items in order until full', () => {
    const ring = new RingBuffer<number>(3);
    ring.push(1);
    ring.push(2);
    expect(ring.toArray()).toEqual([1, 2]);
    expect(ring.size).toBe(2);
  });

  it('drops the oldest items when full', () => {
    const ring = new RingBuffer<number>(3);
    for (let i = 1; i <= 7; i++) ring.push(i);
    expect(ring.toArray()).toEqual([5, 6, 7]);
    expect(ring.size).toBe(3);
  });

  it('clear empties it', () => {
    const ring = new RingBuffer<number>(2);
    ring.push(1);
    ring.push(2);
    ring.push(3);
    ring.clear();
    expect(ring.toArray()).toEqual([]);
    ring.push(4);
    expect(ring.toArray()).toEqual([4]);
  });

  it('rejects a capacity below 1', () => {
    expect(() => new RingBuffer<number>(0)).toThrow(RangeError);
    expect(() => new RingBuffer<number>(1.5)).toThrow(RangeError);
  });
});

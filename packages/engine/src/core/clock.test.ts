import { describe, expect, it } from 'vitest';
import { createClock } from './clock';

describe('createClock', () => {
  it('starts at 0 by default and moves forward', () => {
    const clock = createClock();
    expect(clock.now()).toBe(0);
    clock.advanceTo(500);
    clock.advanceTo(500);
    expect(clock.now()).toBe(500);
  });

  it('never goes backwards', () => {
    const clock = createClock(100);
    expect(() => {
      clock.advanceTo(99);
    }).toThrow(/backwards/);
    expect(clock.now()).toBe(100);
  });

  it('only accepts non-negative integer milliseconds', () => {
    const clock = createClock();
    for (const bad of [1.5, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => {
        clock.advanceTo(bad);
      }).toThrow(RangeError);
    }
    expect(() => createClock(-5)).toThrow(RangeError);
  });
});

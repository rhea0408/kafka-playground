import { describe, expect, it } from 'vitest';
import { MAX_SPEED, MIN_SPEED } from './protocol.ts';
import { cycleSpeed, formatSpeed, SPEED_PRESETS } from './speed.ts';

describe('speed presets', () => {
  it('are the FR-2.1 choices within the host limits', () => {
    expect(SPEED_PRESETS).toEqual([0.25, 0.5, 1, 2, 5, 10]);
    expect(SPEED_PRESETS[0]).toBe(MIN_SPEED);
    expect(SPEED_PRESETS[SPEED_PRESETS.length - 1]).toBe(MAX_SPEED);
  });

  it('cycle up and down, wrapping at the ends', () => {
    expect(cycleSpeed(1)).toBe(2);
    expect(cycleSpeed(5)).toBe(10);
    expect(cycleSpeed(10)).toBe(0.25);
    expect(cycleSpeed(1, -1)).toBe(0.5);
    expect(cycleSpeed(0.25, -1)).toBe(10);
  });

  it('move a non-preset speed to the nearest preset in that direction', () => {
    expect(cycleSpeed(3)).toBe(5);
    expect(cycleSpeed(3, -1)).toBe(2);
  });

  it('format with a multiplication sign', () => {
    expect(SPEED_PRESETS.map(formatSpeed)).toEqual(['0.25×', '0.5×', '1×', '2×', '5×', '10×']);
  });
});

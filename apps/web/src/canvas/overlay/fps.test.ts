import { describe, expect, it } from 'vitest';
import { FpsMeter } from './fps.ts';

describe('FpsMeter', () => {
  it('counts the frames in the last second', () => {
    const meter = new FpsMeter();
    let fps = 0;
    for (let i = 0; i <= 120; i++) fps = meter.frame(i * (1000 / 60));
    expect(fps).toBe(60);
    expect(meter.fps(5000)).toBe(0);
  });

  it('starts over after reset', () => {
    const meter = new FpsMeter();
    meter.frame(0);
    meter.frame(10);
    meter.reset();
    expect(meter.frame(20)).toBe(1);
  });
});

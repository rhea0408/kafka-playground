import { describe, expect, it } from 'vitest';
import { makeEdgePath, pointAt, type Anchor } from './path.ts';

const right = (x: number, y: number): Anchor => ({ x, y, side: 'right' });
const left = (x: number, y: number): Anchor => ({ x, y, side: 'left' });

describe('makeEdgePath', () => {
  it('uses React Flow control points (curvature 0.25, getBezierPath)', () => {
    // Source right → target left, target further right: offset = 0.5 × dx on both sides.
    const path = makeEdgePath(right(0, 0), left(200, 100));
    expect(path.c).toEqual([0, 0, 100, 0, 100, 100, 200, 100]);
  });

  it('bends back with 0.25 × 25 × √distance when the target is behind the source', () => {
    const path = makeEdgePath(right(100, 0), left(0, 0));
    const offset = 0.25 * 25 * Math.sqrt(100);
    expect(path.c).toEqual([100, 0, 100 + offset, 0, -offset, 0, 0, 0]);
  });

  it('measures a straight line exactly', () => {
    const path = makeEdgePath(right(0, 0), left(300, 0));
    expect(path.length).toBeCloseTo(300, 6);
  });

  it('measures a curve longer than its chord', () => {
    const path = makeEdgePath(right(0, 0), left(200, 150));
    expect(path.length).toBeGreaterThan(250);
    expect(path.length).toBeLessThan(200 + 150);
  });
});

describe('pointAt', () => {
  it('returns the ends at 0 and 1 and clamps outside', () => {
    const path = makeEdgePath(right(10, 20), left(210, 120));
    expect(pointAt(path, 0)).toEqual({ x: 10, y: 20 });
    expect(pointAt(path, -1)).toEqual({ x: 10, y: 20 });
    expect(pointAt(path, 1)).toEqual({ x: 210, y: 120 });
    expect(pointAt(path, 2)).toEqual({ x: 210, y: 120 });
  });

  it('moves at constant speed along a straight edge', () => {
    const path = makeEdgePath(right(0, 50), left(400, 50));
    for (const f of [0.1, 0.25, 0.5, 0.9]) {
      const p = pointAt(path, f);
      expect(p.x).toBeCloseTo(400 * f, 0);
      expect(p.y).toBeCloseTo(50, 9);
    }
  });

  it('covers equal arc lengths in equal fractions on a curve', () => {
    const path = makeEdgePath(right(0, 0), left(300, 200));
    const steps = 50;
    const lengths: number[] = [];
    let prev = pointAt(path, 0);
    for (let i = 1; i <= steps; i++) {
      const p = pointAt(path, i / steps);
      lengths.push(Math.hypot(p.x - prev.x, p.y - prev.y));
      prev = p;
    }
    const expected = path.length / steps;
    for (const l of lengths) expect(Math.abs(l - expected)).toBeLessThan(expected * 0.05);
  });

  it('is symmetric on an S curve: the middle is the centre of the two ends', () => {
    const path = makeEdgePath(right(0, 0), left(200, 100));
    const mid = pointAt(path, 0.5);
    expect(mid.x).toBeCloseTo(100, 1);
    expect(mid.y).toBeCloseTo(50, 1);
  });

  it('handles a zero-length path', () => {
    const path = makeEdgePath(right(5, 5), left(5, 5));
    expect(path.length).toBeCloseTo(0, 9);
    expect(pointAt(path, 0.5).x).toBeCloseTo(5, 9);
    expect(pointAt(path, 0.5).y).toBeCloseTo(5, 9);
  });
});

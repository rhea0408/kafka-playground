import { describe, expect, it } from 'vitest';
import { isAtBottom, tailRange, visibleRange } from './windowing.ts';

describe('visibleRange', () => {
  const base = { rowHeight: 20, viewportHeight: 100 };

  it('is empty for an empty list', () => {
    expect(visibleRange({ ...base, count: 0, scrollTop: 0 })).toEqual({ start: 0, end: 0 });
  });

  it('covers the rows in view at the top', () => {
    expect(visibleRange({ ...base, count: 1000, scrollTop: 0 })).toEqual({ start: 0, end: 5 });
  });

  it('includes a partly visible row at either edge', () => {
    // 30..130 px: rows 1 (20–40, partly) to 6 (120–140, partly).
    expect(visibleRange({ ...base, count: 1000, scrollTop: 30 })).toEqual({ start: 1, end: 7 });
  });

  it('adds overscan on both sides, clamped to the list', () => {
    expect(visibleRange({ ...base, count: 1000, scrollTop: 200, overscan: 3 })).toEqual({
      start: 7,
      end: 18,
    });
    expect(visibleRange({ ...base, count: 1000, scrollTop: 0, overscan: 3 })).toEqual({
      start: 0,
      end: 8,
    });
  });

  it('clamps a scroll offset beyond the end (the list just shrank)', () => {
    expect(visibleRange({ ...base, count: 10, scrollTop: 5000 })).toEqual({ start: 5, end: 10 });
    expect(visibleRange({ ...base, count: 10, scrollTop: -50 })).toEqual({ start: 0, end: 5 });
  });

  it('renders everything when the list is shorter than the viewport', () => {
    expect(visibleRange({ ...base, count: 3, scrollTop: 0 })).toEqual({ start: 0, end: 3 });
  });

  it('renders at least one row for a zero-height viewport', () => {
    expect(visibleRange({ rowHeight: 20, viewportHeight: 0, count: 5, scrollTop: 0 })).toEqual({
      start: 0,
      end: 1,
    });
  });

  it('never renders more than viewport / rowHeight + 2 + 2 × overscan rows', () => {
    for (let scrollTop = 0; scrollTop < 200_000; scrollTop += 777) {
      const r = visibleRange({ ...base, count: 10_000, scrollTop, overscan: 6 });
      expect(r.end - r.start).toBeLessThanOrEqual(100 / 20 + 2 + 12);
      expect(r.start).toBeGreaterThanOrEqual(0);
      expect(r.end).toBeLessThanOrEqual(10_000);
    }
  });
});

describe('tailRange', () => {
  it('is the last rows that fit', () => {
    expect(tailRange(10_000, 20, 100)).toEqual({ start: 9995, end: 10_000 });
    expect(tailRange(10_000, 20, 100, 2)).toEqual({ start: 9993, end: 10_000 });
    expect(tailRange(3, 20, 100)).toEqual({ start: 0, end: 3 });
  });
});

describe('isAtBottom', () => {
  it('is true at the end, with a small tolerance', () => {
    expect(isAtBottom(900, 100, 1000)).toBe(true);
    expect(isAtBottom(898.5, 100, 1000)).toBe(true);
    expect(isAtBottom(880, 100, 1000)).toBe(false);
    expect(isAtBottom(0, 100, 50)).toBe(true);
  });
});

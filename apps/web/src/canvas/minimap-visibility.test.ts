import { describe, expect, it } from 'vitest';
import { isContentOffscreen, isMinimapVisible, toggleMinimap } from './minimap-visibility.ts';

const visible = { x: 0, y: 0, width: 800, height: 600 };

describe('minimap visibility (FR-1.7, B11)', () => {
  it('is hidden when everything fits', () => {
    expect(isContentOffscreen({ x: 40, y: 40, width: 500, height: 300 }, visible)).toBe(false);
    expect(isContentOffscreen({ x: 0, y: 0, width: 800, height: 600 }, visible)).toBe(false);
  });

  it('shows when content sticks out on any side', () => {
    expect(isContentOffscreen({ x: -20, y: 40, width: 100, height: 100 }, visible)).toBe(true);
    expect(isContentOffscreen({ x: 40, y: -20, width: 100, height: 100 }, visible)).toBe(true);
    expect(isContentOffscreen({ x: 750, y: 40, width: 100, height: 100 }, visible)).toBe(true);
    expect(isContentOffscreen({ x: 40, y: 550, width: 100, height: 100 }, visible)).toBe(true);
  });

  it('shows when zoomed in so far that the content no longer fits', () => {
    // Zoom 4 on an 800 × 600 canvas shows 200 × 150 flow units.
    expect(
      isContentOffscreen(
        { x: 40, y: 40, width: 226, height: 84 },
        { x: 0, y: 0, width: 200, height: 150 },
      ),
    ).toBe(true);
  });

  it('ignores sub-pixel overhang and has nothing off-screen when empty or unmeasured', () => {
    expect(isContentOffscreen({ x: -0.5, y: 0, width: 800.5, height: 600 }, visible)).toBe(false);
    expect(isContentOffscreen(null, visible)).toBe(false);
    expect(
      isContentOffscreen({ x: 0, y: 0, width: 10, height: 10 }, { ...visible, width: 0 }),
    ).toBe(false);
  });

  it('the toggle overrides the automatic choice', () => {
    expect(isMinimapVisible(true, null)).toBe(true);
    expect(isMinimapVisible(false, null)).toBe(false);
    expect(isMinimapVisible(true, toggleMinimap(true))).toBe(false);
    expect(isMinimapVisible(false, toggleMinimap(false))).toBe(true);
  });
});

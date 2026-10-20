import { describe, expect, it } from 'vitest';
import type { Dot } from './dots.ts';
import { drawDots, readPalette, RING_TOKEN, SHADE_TOKENS } from './draw.ts';

function dot(over: Partial<Dot>): Dot {
  return {
    id: 1,
    kind: 'A1',
    traceId: 't',
    letter: 'A',
    shade: 0,
    from: { nodeId: 'p1', handleId: 'out' },
    to: { nodeId: 't1', handleId: 'p0:in' },
    mode: 'move',
    progress: 0,
    fadeMs: 0,
    x: 50,
    y: 50,
    alpha: 1,
    placed: true,
    ...over,
  };
}

/** Records the calls the draw code makes. */
function fakeContext() {
  const calls: string[] = [];
  const ctx = {
    setTransform: (...a: number[]) => calls.push(`setTransform(${a.join(',')})`),
    clearRect: () => calls.push('clearRect'),
    beginPath: () => undefined,
    arc: (x: number, y: number) => calls.push(`arc(${String(x)},${String(y)})`),
    fill: () => undefined,
    stroke: () => undefined,
    fillText: (t: string) => calls.push(`text(${t})`),
    font: '',
    textAlign: '',
    textBaseline: '',
    lineWidth: 0,
    strokeStyle: '',
    fillStyle: '',
    globalAlpha: 1,
  };
  return { ctx: ctx as unknown as CanvasRenderingContext2D, calls };
}

const PALETTE = readPalette((token) => ` ${token} `);
const TARGET = { width: 400, height: 300, dpr: 2, viewport: { x: 10, y: 20, zoom: 0.5 } };

describe('readPalette', () => {
  it('reads every shade from the ui-spec tokens (no literal colors)', () => {
    expect(PALETTE.fill).toEqual(SHADE_TOKENS.map((t) => t.fill));
    expect(PALETTE.letter).toEqual(SHADE_TOKENS.map((t) => t.letter));
    expect(PALETTE.ring).toBe(RING_TOKEN);
  });
});

describe('drawDots', () => {
  it('applies the device pixel ratio and React Flow viewport transform, then draws placed dots', () => {
    const { ctx, calls } = fakeContext();
    const drawn = drawDots(
      ctx,
      [
        dot({}),
        dot({ id: 2, placed: false }),
        dot({ id: 3, alpha: 0 }),
        dot({ id: 4, letter: '∅' }),
      ],
      TARGET,
      PALETTE,
    );
    expect(drawn).toBe(2);
    expect(calls).toEqual([
      'setTransform(2,0,0,2,0,0)',
      'clearRect',
      'setTransform(1,0,0,1,20,40)',
      'arc(50,50)',
      'text(A)',
      'arc(50,50)',
      'text(∅)',
    ]);
  });

  it('skips dots outside the visible area', () => {
    const { ctx } = fakeContext();
    // Visible flow x: (0 - 10) / 0.5 = -20 … (400 - 10) / 0.5 = 780, plus one dot of margin.
    expect(
      drawDots(ctx, [dot({ x: 790 }), dot({ x: 900 }), dot({ x: -40 })], TARGET, PALETTE),
    ).toBe(1);
  });

  it('only clears when there are no dots', () => {
    const { ctx, calls } = fakeContext();
    expect(drawDots(ctx, [], TARGET, PALETTE)).toBe(0);
    expect(calls).toEqual(['setTransform(2,0,0,2,0,0)', 'clearRect']);
  });
});

/**
 * Draws message dots on the overlay canvas (M2-T9). Kept apart from the dot model so the model
 * stays DOM-free; this file only needs a 2D context and the colors read from the CSS tokens.
 */

import type { Viewport } from '../geometry.ts';
import { DOT_SIZE, type Dot, type DotShade } from './dots.ts';

/** Colors from the ui-spec tokens (read with getComputedStyle), one fill / letter pair per shade. */
export interface DotPalette {
  readonly fill: readonly [string, string, string];
  readonly letter: readonly [string, string, string];
  /** Thin ring so a dot stands out on an edge of the same grey. */
  readonly ring: string;
}

/** Token names per shade: dark, mid, light grey (ui-spec "Key colors: greys + letter labels"). */
export const SHADE_TOKENS: readonly {
  readonly fill: string;
  readonly letter: string;
}[] = [
  { fill: '--color-status', letter: '--color-surface' },
  { fill: '--color-border', letter: '--color-status' },
  { fill: '--color-muted', letter: '--color-status' },
];

export const RING_TOKEN = '--color-surface';

/** Reads the palette from computed CSS custom properties. */
export function readPalette(get: (token: string) => string): DotPalette {
  const at = (i: number, part: 'fill' | 'letter'): string => {
    const t = SHADE_TOKENS[i];
    return t === undefined ? '' : get(t[part]).trim();
  };
  return {
    fill: [at(0, 'fill'), at(1, 'fill'), at(2, 'fill')],
    letter: [at(0, 'letter'), at(1, 'letter'), at(2, 'letter')],
    ring: get(RING_TOKEN).trim(),
  };
}

export interface DrawTarget {
  /** CSS px of the canvas element. */
  readonly width: number;
  readonly height: number;
  readonly dpr: number;
  readonly viewport: Viewport;
}

const RADIUS = DOT_SIZE / 2;
/** wireframes: `font: 700 9px/16px` on a 16 px dot. */
const FONT = '700 9px system-ui, sans-serif';

/**
 * Clears the canvas and draws every placed dot that is inside the visible area. Returns how many
 * dots were drawn. The flow → screen transform is React Flow's viewport (pan x, y and zoom), so
 * dots follow pan and zoom exactly like the nodes.
 */
export function drawDots(
  ctx: CanvasRenderingContext2D,
  dots: readonly Dot[],
  target: DrawTarget,
  palette: DotPalette,
): number {
  const { width, height, dpr, viewport } = target;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);
  if (dots.length === 0) return 0;
  const { x: tx, y: ty, zoom } = viewport;
  ctx.setTransform(dpr * zoom, 0, 0, dpr * zoom, dpr * tx, dpr * ty);
  // Visible flow rectangle, with one dot of margin.
  const minX = -tx / zoom - DOT_SIZE;
  const minY = -ty / zoom - DOT_SIZE;
  const maxX = (width - tx) / zoom + DOT_SIZE;
  const maxY = (height - ty) / zoom + DOT_SIZE;
  ctx.font = FONT;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.lineWidth = 1;
  ctx.strokeStyle = palette.ring;
  let drawn = 0;
  for (const dot of dots) {
    if (!dot.placed || dot.alpha <= 0) continue;
    if (dot.x < minX || dot.x > maxX || dot.y < minY || dot.y > maxY) continue;
    const shade: DotShade = dot.shade;
    ctx.globalAlpha = dot.alpha;
    ctx.beginPath();
    ctx.arc(dot.x, dot.y, RADIUS, 0, Math.PI * 2);
    ctx.fillStyle = palette.fill[shade];
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = palette.letter[shade];
    ctx.fillText(dot.letter, dot.x, dot.y + 0.5);
    drawn++;
  }
  ctx.globalAlpha = 1;
  return drawn;
}

/**
 * Edge path geometry for the message overlay (M2-T9). Pure: no React, no DOM.
 *
 * The curve is the same cubic bezier React Flow draws for its default edge (`getBezierPath` in
 * @xyflow/system, curvature 0.25), so a dot that runs from a producer's `out` handle to a partition
 * row's anchor follows the shape the player expects. Dots move at a constant speed along the
 * curve, so positions are looked up by arc length, not by the bezier parameter.
 */

export type Side = 'left' | 'right' | 'top' | 'bottom';

export interface Point {
  readonly x: number;
  readonly y: number;
}

/** A point where a path starts or ends, and the node side it leaves / enters from. */
export interface Anchor extends Point {
  readonly side: Side;
}

/** React Flow's default edge curvature. */
export const EDGE_CURVATURE = 0.25;

/** Straight segments used to measure the curve. 24 keeps the error under 0.5 px on long edges. */
export const PATH_SEGMENTS = 24;

export interface EdgePath {
  /** Control points p0 (source), p1, p2, p3 (target) as x0, y0, x1, y1, …. */
  readonly c: readonly [number, number, number, number, number, number, number, number];
  /** Cumulative length at each of the PATH_SEGMENTS + 1 sample points. */
  readonly cum: Float64Array;
  readonly length: number;
}

/** Same as @xyflow/system `calculateControlOffset`. */
function controlOffset(distance: number, curvature: number): number {
  return distance >= 0 ? 0.5 * distance : curvature * 25 * Math.sqrt(-distance);
}

/** Same as @xyflow/system `getControlWithCurvature`. */
function control(side: Side, from: Point, to: Point, curvature: number): Point {
  switch (side) {
    case 'left':
      return { x: from.x - controlOffset(from.x - to.x, curvature), y: from.y };
    case 'right':
      return { x: from.x + controlOffset(to.x - from.x, curvature), y: from.y };
    case 'top':
      return { x: from.x, y: from.y - controlOffset(from.y - to.y, curvature) };
    case 'bottom':
      return { x: from.x, y: from.y + controlOffset(to.y - from.y, curvature) };
  }
}

function cubicAt(c: EdgePath['c'], t: number): Point {
  const u = 1 - t;
  const a = u * u * u;
  const b = 3 * u * u * t;
  const d = 3 * u * t * t;
  const e = t * t * t;
  return {
    x: a * c[0] + b * c[2] + d * c[4] + e * c[6],
    y: a * c[1] + b * c[3] + d * c[5] + e * c[7],
  };
}

/** The bezier React Flow would draw from `source` to `target`, measured for arc-length lookup. */
export function makeEdgePath(
  source: Anchor,
  target: Anchor,
  curvature: number = EDGE_CURVATURE,
): EdgePath {
  const s = control(source.side, source, target, curvature);
  const t = control(target.side, target, source, curvature);
  const c = [source.x, source.y, s.x, s.y, t.x, t.y, target.x, target.y] as const;
  const cum = new Float64Array(PATH_SEGMENTS + 1);
  let prev = cubicAt(c, 0);
  for (let i = 1; i <= PATH_SEGMENTS; i++) {
    const p = cubicAt(c, i / PATH_SEGMENTS);
    cum[i] = (cum[i - 1] ?? 0) + Math.hypot(p.x - prev.x, p.y - prev.y);
    prev = p;
  }
  return { c, cum, length: cum[PATH_SEGMENTS] ?? 0 };
}

/**
 * The point at `fraction` (0…1) of the path's length. Clamped to the ends. Linear between the
 * measured samples, which is exact for the length and within a fraction of a pixel for the curve.
 */
export function pointAt(path: EdgePath, fraction: number): Point {
  const { c, cum, length } = path;
  if (fraction <= 0 || length === 0) return { x: c[0], y: c[1] };
  if (fraction >= 1) return { x: c[6], y: c[7] };
  const target = fraction * length;
  // Binary search for the segment that contains `target`.
  let lo = 0;
  let hi = PATH_SEGMENTS;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if ((cum[mid] ?? 0) < target) lo = mid;
    else hi = mid;
  }
  const l0 = cum[lo] ?? 0;
  const l1 = cum[hi] ?? l0;
  const t0 = lo / PATH_SEGMENTS;
  const t1 = hi / PATH_SEGMENTS;
  const t = l1 === l0 ? t0 : t0 + ((target - l0) / (l1 - l0)) * (t1 - t0);
  return cubicAt(c, t);
}

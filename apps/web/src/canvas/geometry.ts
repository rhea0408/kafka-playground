/**
 * Canvas geometry: node sizes from docs/ui/ui-spec.md ("Canvas node sizes"), the snap grid, and the
 * handle / anchor ids the message overlay (T9) uses to find partition and consumer rows. Pure: no
 * React, no DOM, so it is unit-tested in Node and shared by the nodes and the overlay.
 */

import type { ComponentKind } from '@kp/schema';
import type { XYPosition } from '../stores/topology-store.ts';

export interface Size {
  readonly width: number;
  readonly height: number;
}

export interface Rect extends XYPosition, Size {}

/** Snap grid and dotted-background spacing: wireframe 05 draws its dots every 16 px. */
export const GRID_SIZE = 16;

export const PRODUCER_SIZE: Size = { width: 150, height: 72 };
/** Sandbox broker (ui-spec: 226 × 92 inside a level's cluster frame, 84 in the Sandbox). */
export const BROKER_SIZE: Size = { width: 226, height: 84 };

/** Topic: 230 wide; a 36 px header, then one 48 px slot per partition (8 px gap + 40 px row). */
export const TOPIC_WIDTH = 230;
export const TOPIC_HEADER_HEIGHT = 36;
export const TOPIC_ROW_SLOT = 48;
export const TOPIC_ROW_GAP = 8;
export const TOPIC_ROW_HEIGHT = TOPIC_ROW_SLOT - TOPIC_ROW_GAP;

/** Consumer group: 220 wide (12 px padding each side of a 196 px row), header 36, rows 40 + 8 gap. */
export const GROUP_WIDTH = 220;
export const GROUP_HEADER_HEIGHT = 36;
export const GROUP_ROW_WIDTH = 196;
export const GROUP_ROW_HEIGHT = 40;
export const GROUP_ROW_SLOT = 48;
/** Space under the last row (wireframe 05: 2 rows → 140 px tall). */
export const GROUP_BOTTOM_PADDING = 8;

export function topicHeight(partitions: number): number {
  return TOPIC_HEADER_HEIGHT + TOPIC_ROW_SLOT * partitions;
}

export function groupHeight(rows: number): number {
  return GROUP_HEADER_HEIGHT + GROUP_ROW_SLOT * rows + GROUP_BOTTOM_PADDING;
}

/** Vertical centre of partition row `index`, relative to the topic node's top edge. */
export function partitionRowCenterY(index: number): number {
  return TOPIC_HEADER_HEIGHT + TOPIC_ROW_SLOT * index + TOPIC_ROW_GAP + TOPIC_ROW_HEIGHT / 2;
}

/** Vertical centre of consumer row `index`, relative to the group node's top edge. */
export function groupRowCenterY(index: number): number {
  return GROUP_HEADER_HEIGHT + GROUP_ROW_SLOT * index + TOPIC_ROW_GAP + GROUP_ROW_HEIGHT / 2;
}

/**
 * Size of a node on the canvas. `partitions` is used for topics and `rows` for consumer groups
 * (one row per consumer; always 1 in M2, M3 adds more).
 */
export function nodeSize(kind: ComponentKind, partitions = 1, rows = 1): Size {
  switch (kind) {
    case 'producer':
      return PRODUCER_SIZE;
    case 'broker':
      return BROKER_SIZE;
    case 'topic':
      return { width: TOPIC_WIDTH, height: topicHeight(partitions) };
    case 'consumer':
      return { width: GROUP_WIDTH, height: groupHeight(rows) };
  }
}

// ── Handle and anchor ids ──────────────────────────────────────────────────────────────────────
// Connectable handles: producer `out` → topic `in`; topic `out` → consumer row `row:<consumerId>`.
// Anchors are invisible, non-connectable handles React Flow measures, so the overlay (T9) can
// read a partition row's position from `node.internals.handleBounds` by id.

export const HANDLE_OUT = 'out';
export const HANDLE_IN = 'in';

/** Anchor on the left edge of partition row `index` (where A1 dots arrive). */
export function partitionInAnchorId(index: number): string {
  return `p${String(index)}:in`;
}

/** Anchor on the right edge of partition row `index` (where A2 dots leave). */
export function partitionOutAnchorId(index: number): string {
  return `p${String(index)}:out`;
}

/** Target handle of a consumer's row inside its group node (A2 dots arrive here). */
export function consumerRowHandleId(consumerId: string): string {
  return `row:${consumerId}`;
}

// ── Coordinates ───────────────────────────────────────────────────────────────────────────────

export interface Viewport {
  readonly x: number;
  readonly y: number;
  readonly zoom: number;
}

export function snapToGrid(position: XYPosition, grid = GRID_SIZE): XYPosition {
  return { x: Math.round(position.x / grid) * grid, y: Math.round(position.y / grid) * grid };
}

/** A point in the canvas element (px from its top-left) → flow coordinates. */
export function containerToFlow(point: XYPosition, viewport: Viewport): XYPosition {
  return { x: (point.x - viewport.x) / viewport.zoom, y: (point.y - viewport.y) / viewport.zoom };
}

/** The part of the flow the canvas element shows, in flow coordinates. */
export function visibleFlowRect(viewport: Viewport, canvas: Size): Rect {
  return {
    ...containerToFlow({ x: 0, y: 0 }, viewport),
    width: canvas.width / viewport.zoom,
    height: canvas.height / viewport.zoom,
  };
}

/** Top-left for a node of `size` centred on `point`, snapped to the grid. */
export function centredTopLeft(point: XYPosition, size: Size): XYPosition {
  return snapToGrid({ x: point.x - size.width / 2, y: point.y - size.height / 2 });
}

/**
 * First position from `start` (stepping diagonally by two grid cells) that no other node has
 * exactly. Keeps nodes added by keyboard / click at the canvas centre from stacking invisibly.
 */
export function freePosition(
  taken: Iterable<XYPosition>,
  start: XYPosition,
  step = GRID_SIZE * 2,
): XYPosition {
  const keys = new Set<string>();
  for (const p of taken) keys.add(`${String(p.x)},${String(p.y)}`);
  let candidate = start;
  for (let i = 0; i < 1000 && keys.has(`${String(candidate.x)},${String(candidate.y)}`); i++) {
    candidate = { x: candidate.x + step, y: candidate.y + step };
  }
  return candidate;
}

export interface NodeRect extends Rect {
  readonly id: string;
}

/** The topmost node under a flow point (last in render order wins), if any. */
export function nodeAtPoint(nodes: readonly NodeRect[], point: XYPosition): string | undefined {
  for (let i = nodes.length - 1; i >= 0; i--) {
    const n = nodes[i];
    if (n === undefined) continue;
    if (point.x >= n.x && point.x <= n.x + n.width && point.y >= n.y && point.y <= n.y + n.height) {
      return n.id;
    }
  }
  return undefined;
}

/** Smallest rectangle around every node, or null when there are none. */
export function boundsOf(rects: Iterable<Rect>): Rect | null {
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  for (const r of rects) {
    minX = Math.min(minX, r.x);
    minY = Math.min(minY, r.y);
    maxX = Math.max(maxX, r.x + r.width);
    maxY = Math.max(maxY, r.y + r.height);
  }
  return minX === Infinity ? null : { x: minX, y: minY, width: maxX - minX, height: maxY - minY };
}

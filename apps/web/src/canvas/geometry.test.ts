import { describe, expect, it } from 'vitest';
import {
  boundsOf,
  centredTopLeft,
  consumerRowHandleId,
  containerToFlow,
  freePosition,
  groupHeight,
  nodeAtPoint,
  nodeSize,
  partitionInAnchorId,
  partitionOutAnchorId,
  partitionRowCenterY,
  snapToGrid,
  topicHeight,
  visibleFlowRect,
} from './geometry.ts';

describe('node sizes (ui-spec "Canvas node sizes")', () => {
  it('match the spec', () => {
    expect(nodeSize('producer')).toEqual({ width: 150, height: 72 });
    expect(nodeSize('broker')).toEqual({ width: 226, height: 84 });
    expect(nodeSize('topic', 1)).toEqual({ width: 230, height: 36 + 48 });
    expect(nodeSize('topic', 4)).toEqual({ width: 230, height: 228 }); // wireframe 05
    expect(nodeSize('consumer')).toEqual({ width: 220, height: 92 });
  });

  it('a consumer group grows by 48 px per row (wireframe 05: 2 rows → 140)', () => {
    expect(groupHeight(2)).toBe(140);
    expect(topicHeight(3)).toBe(36 + 3 * 48);
  });

  it('partition row centres sit in the middle of each 40 px row below the header', () => {
    expect(partitionRowCenterY(0)).toBe(36 + 8 + 20);
    expect(partitionRowCenterY(2)).toBe(36 + 2 * 48 + 8 + 20);
  });

  it('anchor and row handle ids are distinct per partition and consumer', () => {
    expect(partitionInAnchorId(0)).toBe('p0:in');
    expect(partitionOutAnchorId(3)).toBe('p3:out');
    expect(consumerRowHandleId('c1')).toBe('row:c1');
  });
});

describe('coordinates', () => {
  it('snaps to the 16 px grid', () => {
    expect(snapToGrid({ x: 7, y: 8 })).toEqual({ x: 0, y: 16 });
    expect(snapToGrid({ x: -9, y: 33 })).toEqual({ x: -16, y: 32 });
  });

  it('converts canvas-element points to flow coordinates', () => {
    expect(containerToFlow({ x: 100, y: 50 }, { x: 0, y: 0, zoom: 1 })).toEqual({ x: 100, y: 50 });
    expect(containerToFlow({ x: 100, y: 50 }, { x: 20, y: 10, zoom: 2 })).toEqual({ x: 40, y: 20 });
  });

  it('computes the visible flow rectangle', () => {
    expect(visibleFlowRect({ x: -100, y: 40, zoom: 0.5 }, { width: 800, height: 400 })).toEqual({
      x: 200,
      y: -80,
      width: 1600,
      height: 800,
    });
  });

  it('centres a node on a drop point, snapped', () => {
    expect(centredTopLeft({ x: 300, y: 200 }, { width: 150, height: 72 })).toEqual({
      x: 224,
      y: 160,
    });
  });

  it('moves off positions that are taken', () => {
    const start = { x: 0, y: 0 };
    expect(freePosition([], start)).toEqual(start);
    expect(
      freePosition(
        [
          { x: 0, y: 0 },
          { x: 32, y: 32 },
        ],
        start,
      ),
    ).toEqual({ x: 64, y: 64 });
  });

  it('finds the topmost node under a point', () => {
    const nodes = [
      { id: 'a', x: 0, y: 0, width: 100, height: 100 },
      { id: 'b', x: 50, y: 50, width: 100, height: 100 },
    ];
    expect(nodeAtPoint(nodes, { x: 10, y: 10 })).toBe('a');
    expect(nodeAtPoint(nodes, { x: 60, y: 60 })).toBe('b');
    expect(nodeAtPoint(nodes, { x: 500, y: 0 })).toBeUndefined();
  });

  it('bounds every rectangle, or null for none', () => {
    expect(boundsOf([])).toBeNull();
    expect(
      boundsOf([
        { x: 10, y: 20, width: 10, height: 10 },
        { x: -5, y: 0, width: 5, height: 50 },
      ]),
    ).toEqual({ x: -5, y: 0, width: 25, height: 50 });
  });
});

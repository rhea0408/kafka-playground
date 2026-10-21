import { describe, expect, it } from 'vitest';
import { partitionRowCenterY, TOPIC_WIDTH } from '../geometry.ts';
import {
  anchorOf,
  consumerRow,
  partitionIn,
  partitionOut,
  producerOut,
  resolverFor,
  type InternalNodeLike,
} from './anchors.ts';

const producer: InternalNodeLike = {
  measured: { width: 150, height: 72 },
  internals: {
    positionAbsolute: { x: 100, y: 200 },
    handleBounds: {
      source: [{ id: 'out', x: 145, y: 31, width: 10, height: 10, position: 'right' }],
      target: null,
    },
  },
};

const topic: InternalNodeLike = {
  measured: { width: TOPIC_WIDTH, height: 180 },
  internals: {
    positionAbsolute: { x: 400, y: 160 },
    handleBounds: {
      source: [{ id: 'p0:out', x: 229.5, y: 63.5, width: 1, height: 1, position: 'right' }],
      target: [{ id: 'p0:in', x: -0.5, y: 63.5, width: 1, height: 1, position: 'left' }],
    },
  },
};

describe('anchorOf', () => {
  it('uses the middle of the handle side an edge attaches to (getHandlePosition)', () => {
    expect(anchorOf(producer, 'out')).toEqual({ x: 255, y: 236, side: 'right' });
    expect(anchorOf(topic, 'p0:in')).toEqual({ x: 399.5, y: 224, side: 'left' });
    expect(anchorOf(topic, 'p0:out')).toEqual({ x: 630.5, y: 224, side: 'right' });
  });

  it('falls back to the layout constants for unmeasured partition anchors', () => {
    expect(anchorOf(topic, 'p2:in')).toEqual({
      x: 400,
      y: 160 + partitionRowCenterY(2),
      side: 'left',
    });
    expect(anchorOf(topic, 'p2:out')).toEqual({
      x: 400 + TOPIC_WIDTH,
      y: 160 + partitionRowCenterY(2),
      side: 'right',
    });
  });

  it('is undefined for a missing node or an unknown handle', () => {
    expect(anchorOf(undefined, 'out')).toBeUndefined();
    expect(anchorOf(producer, 'row:c1')).toBeUndefined();
    expect(anchorOf({ internals: { positionAbsolute: { x: 0, y: 0 } } }, 'p0:in')).toBeUndefined();
  });
});

describe('endpoint refs', () => {
  it('name the handles from geometry.ts', () => {
    expect(producerOut('p1')).toEqual({ nodeId: 'p1', handleId: 'out' });
    expect(partitionIn('t1', 2)).toEqual({ nodeId: 't1', handleId: 'p2:in' });
    expect(partitionOut('t1', 2)).toEqual({ nodeId: 't1', handleId: 'p2:out' });
    expect(consumerRow('c1')).toEqual({ nodeId: 'c1', handleId: 'row:c1' });
  });

  it('resolve through a node lookup', () => {
    const resolve = resolverFor(new Map([['p1', producer]]));
    expect(resolve(producerOut('p1'))).toEqual({ x: 255, y: 236, side: 'right' });
    expect(resolve(producerOut('p2'))).toBeUndefined();
  });
});

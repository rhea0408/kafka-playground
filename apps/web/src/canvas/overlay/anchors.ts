/**
 * Where a dot's path starts and ends, read from React Flow's measured handles (M2-T9). Pure: it
 * takes the parts of a React Flow internal node it needs, so it is unit-tested without a DOM.
 *
 * - A1: producer `out` handle → the partition row's `p<i>:in` anchor.
 * - A2: the partition row's `p<i>:out` anchor → the member row's `row:<consumerId>` anchor in its
 *   group node: the dot ends at the member that consumed the record (M3-T8).
 *
 * The anchors are invisible handles on each partition row (M2-decisions T7-4), so React Flow
 * measures them with the node and they follow any layout change. If a node has not been measured
 * yet, partition anchors fall back to the same position computed from the layout constants.
 */

import {
  consumerRowHandleId,
  HANDLE_OUT,
  partitionInAnchorId,
  partitionOutAnchorId,
  partitionRowCenterY,
} from '../geometry.ts';
import { groupNodeId } from '../flow-mapping.ts';
import type { Anchor, Side } from './path.ts';

/** Which handle of which node. */
export interface EndpointRef {
  readonly nodeId: string;
  readonly handleId: string;
}

/** The fields of a React Flow handle bound the overlay reads (`@xyflow/system` `Handle`). */
export interface HandleBoundLike {
  readonly id?: string | null;
  readonly x: number;
  readonly y: number;
  readonly width: number;
  readonly height: number;
  /** React Flow's `Position` enum; its values are these strings. */
  readonly position: string;
}

/** The fields of a React Flow `InternalNode` the overlay reads. */
export interface InternalNodeLike {
  readonly measured?: { readonly width?: number; readonly height?: number };
  readonly width?: number;
  /** The node's data: a topic node's `component` gives its RF (row height, M4 follow-up G-3). */
  readonly data?: unknown;
  readonly internals: {
    readonly positionAbsolute: { readonly x: number; readonly y: number };
    readonly handleBounds?: {
      readonly source: readonly HandleBoundLike[] | null;
      readonly target: readonly HandleBoundLike[] | null;
    };
  };
}

export type AnchorResolver = (ref: EndpointRef) => Anchor | undefined;

export function producerOut(producerId: string): EndpointRef {
  return { nodeId: producerId, handleId: HANDLE_OUT };
}

export function partitionIn(topicId: string, partition: number): EndpointRef {
  return { nodeId: topicId, handleId: partitionInAnchorId(partition) };
}

export function partitionOut(topicId: string, partition: number): EndpointRef {
  return { nodeId: topicId, handleId: partitionOutAnchorId(partition) };
}

/** A member's row inside its group node (`group:<groupId>`): anchor `row:<consumerId>` (M3-T8). */
export function consumerRow(groupId: string, consumerId: string): EndpointRef {
  return { nodeId: groupNodeId(groupId), handleId: consumerRowHandleId(consumerId) };
}

function toSide(position: string): Side | undefined {
  return position === 'left' || position === 'right' || position === 'top' || position === 'bottom'
    ? position
    : undefined;
}

function findHandle(node: InternalNodeLike, id: string): HandleBoundLike | undefined {
  const bounds = node.internals.handleBounds;
  if (bounds === undefined) return undefined;
  return bounds.source?.find((h) => h.id === id) ?? bounds.target?.find((h) => h.id === id);
}

const PARTITION_ANCHOR = /^p(\d+):(in|out)$/;

/** A topic node's replication factor, from its data; 1 for anything else. */
function replicationFactorOf(node: InternalNodeLike): number {
  const data = node.data;
  if (typeof data !== 'object' || data === null || !('component' in data)) return 1;
  const component: unknown = data.component;
  if (typeof component !== 'object' || component === null) return 1;
  return 'replicationFactor' in component && typeof component.replicationFactor === 'number'
    ? component.replicationFactor
    : 1;
}

/** Partition anchor position from the layout constants, for a node React Flow hasn't measured. */
function partitionFallback(node: InternalNodeLike, handleId: string): Anchor | undefined {
  const match = PARTITION_ANCHOR.exec(handleId);
  const width = node.measured?.width ?? node.width;
  if (match === null || width === undefined) return undefined;
  const { x, y } = node.internals.positionAbsolute;
  const out = match[2] === 'out';
  return {
    x: out ? x + width : x,
    y: y + partitionRowCenterY(Number(match[1]), replicationFactorOf(node)),
    side: out ? 'right' : 'left',
  };
}

/**
 * The flow-coordinate point an edge would attach to on this handle: the same rule as
 * `getHandlePosition` in @xyflow/system (middle of the handle's outer side).
 */
export function anchorOf(node: InternalNodeLike | undefined, handleId: string): Anchor | undefined {
  if (node === undefined) return undefined;
  const handle = findHandle(node, handleId);
  if (handle === undefined) return partitionFallback(node, handleId);
  const side = toSide(handle.position);
  if (side === undefined) return undefined;
  const x = node.internals.positionAbsolute.x + handle.x;
  const y = node.internals.positionAbsolute.y + handle.y;
  switch (side) {
    case 'top':
      return { x: x + handle.width / 2, y, side };
    case 'right':
      return { x: x + handle.width, y: y + handle.height / 2, side };
    case 'bottom':
      return { x: x + handle.width / 2, y: y + handle.height, side };
    case 'left':
      return { x, y: y + handle.height / 2, side };
  }
}

/** A resolver over a node lookup (React Flow's `nodeLookup`, or a plain Map in tests). */
export function resolverFor(lookup: ReadonlyMap<string, InternalNodeLike>): AnchorResolver {
  return (ref) => anchorOf(lookup.get(ref.nodeId), ref.handleId);
}

import { useConnection, useStoreApi, type InternalNode } from '@xyflow/react';
import { useTopologyStore } from '../stores/topology-store.ts';
import { checkConnection, orientConnection } from './connection-check.ts';
import { containerToFlow, nodeAtPoint, type NodeRect } from './geometry.ts';

/** Node rectangles in flow coordinates, in render order, from React Flow's node lookup. */
export function nodeRects(lookup: ReadonlyMap<string, InternalNode>): NodeRect[] {
  const rects: NodeRect[] = [];
  for (const node of lookup.values()) {
    rects.push({
      id: node.id,
      ...node.internals.positionAbsolute,
      width: node.measured.width ?? node.width ?? 0,
      height: node.measured.height ?? node.height ?? 0,
    });
  }
  return rects;
}

/**
 * The reason a link can't be made, shown next to the pointer while a connection is dragged over a
 * node it can't connect to (FR-1.2). Text, not just a red line: the reason teaches the data flow.
 * Screen readers get the same reason from the warning toast when the drag ends.
 */
export function ConnectionReason({ reason, x, y }: { reason: string; x: number; y: number }) {
  return (
    <div
      data-testid="connection-reason"
      aria-hidden="true"
      className="pointer-events-none absolute z-10 flex max-w-[300px] items-start gap-1.5 rounded border border-danger bg-danger-bg px-2 py-1 text-xs text-danger-text"
      style={{ left: x + 16, top: y + 16 }}
    >
      <span>⚠</span>
      <span>
        <strong>Can&apos;t connect.</strong> {reason}
      </span>
    </div>
  );
}

/** Watches the connection being dragged and shows `ConnectionReason` when it would be refused. */
export function ConnectionHint() {
  const connection = useConnection();
  const store = useStoreApi();
  const topology = useTopologyStore();
  if (!connection.inProgress) return null;

  const { transform, nodeLookup } = store.getState();
  const viewport = { x: transform[0], y: transform[1], zoom: transform[2] };
  const startId = connection.fromNode.id;
  const endId =
    connection.toNode?.id ??
    nodeAtPoint(nodeRects(nodeLookup), containerToFlow(connection.pointer, viewport));
  if (endId === undefined || endId === startId) return null;

  const { from, to } = orientConnection(startId, endId, connection.fromHandle.type === 'target');
  const check = checkConnection(topology, from, to);
  if (check.ok) return null;
  return (
    <ConnectionReason reason={check.reason} x={connection.pointer.x} y={connection.pointer.y} />
  );
}

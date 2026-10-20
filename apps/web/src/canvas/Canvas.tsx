import {
  applyEdgeChanges,
  applyNodeChanges,
  Background,
  BackgroundVariant,
  MarkerType,
  ReactFlow,
  useReactFlow,
  useStoreApi,
  type DefaultEdgeOptions,
  type Edge,
  type EdgeChange,
  type IsValidConnection,
  type NodeChange,
  type OnBeforeDelete,
  type OnConnectEnd,
  type OnSelectionChangeFunc,
} from '@xyflow/react';
import { useCallback, useEffect, useState, type DragEvent } from 'react';
import { useActions } from '../actions/actions-context.tsx';
import { useTopologyStore } from '../stores/topology-store.ts';
import { select } from '../stores/ui-store.ts';
import {
  connectChecked,
  deleteSelection,
  isComponentKind,
  PALETTE_MIME,
} from './canvas-commands.ts';
import { CanvasNavigation } from './CanvasNavigation.tsx';
import { checkConnection, orientConnection } from './connection-check.ts';
import { ConnectionHint, nodeRects } from './ConnectionHint.tsx';
import { toFlowEdges, toFlowNodes, type FlowNode } from './flow-mapping.ts';
import { centredTopLeft, GRID_SIZE, nodeAtPoint, nodeSize } from './geometry.ts';
import { NODE_TYPES } from './nodes.tsx';
import { MessageOverlay } from './overlay/MessageOverlay.tsx';

const SNAP_GRID: [number, number] = [GRID_SIZE, GRID_SIZE];
const DELETE_KEYS = ['Delete', 'Backspace'];
const DEFAULT_VIEWPORT = { x: 0, y: 0, zoom: 1 };

/** Edges: 2 px with an arrowhead showing the data direction (wireframe 05). */
const DEFAULT_EDGE_OPTIONS: DefaultEdgeOptions = {
  markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16, color: 'var(--kp-edge)' },
};

function clientPoint(event: MouseEvent | TouchEvent): { x: number; y: number } | undefined {
  if ('changedTouches' in event) {
    const touch = event.changedTouches[0];
    return touch === undefined ? undefined : { x: touch.clientX, y: touch.clientY };
  }
  return { x: event.clientX, y: event.clientY };
}

/**
 * The React Flow canvas (M2-T7): nodes from `topologyStore`, edits through the actions module
 * (architecture §5, one-way data flow). React Flow keeps its own transient state (selection,
 * dragging, measured sizes); configs, connections and positions come from the store, which only
 * changes when the engine accepted a command (or, for positions, on drag stop).
 */
export function Canvas() {
  const actions = useActions();
  const flow = useReactFlow<FlowNode>();
  const store = useStoreApi<FlowNode>();
  const topology = useTopologyStore();
  const [nodes, setNodes] = useState<FlowNode[]>(() => toFlowNodes(topology));
  const [edges, setEdges] = useState<Edge[]>(() => toFlowEdges(topology));

  useEffect(() => {
    setNodes((prev) => toFlowNodes(topology, prev));
    setEdges((prev) => toFlowEdges(topology, prev));
  }, [topology]);

  // Removal goes through onBeforeDelete → commands; React Flow never removes anything itself.
  // A position change that is not mid-drag (the end of a drag, or an arrow-key move of the
  // selection) is written to the store.
  const onNodesChange = useCallback(
    (changes: NodeChange<FlowNode>[]) => {
      for (const c of changes) {
        if (c.type === 'position' && c.dragging === false && c.position !== undefined) {
          actions.moveNode(c.id, c.position);
        }
      }
      setNodes((prev) =>
        applyNodeChanges(
          changes.filter((c) => c.type !== 'remove'),
          prev,
        ),
      );
    },
    [actions],
  );
  const onEdgesChange = useCallback((changes: EdgeChange[]) => {
    setEdges((prev) =>
      applyEdgeChanges(
        changes.filter((c) => c.type !== 'remove'),
        prev,
      ),
    );
  }, []);

  const onSelectionChange = useCallback<OnSelectionChangeFunc<FlowNode>>(
    ({ nodes: selectedNodes, edges: selectedEdges }) => {
      select({ nodeIds: selectedNodes.map((n) => n.id), edgeIds: selectedEdges.map((e) => e.id) });
    },
    [],
  );

  const onBeforeDelete = useCallback<OnBeforeDelete<FlowNode>>(
    async ({ nodes: toDelete, edges: edgesToDelete }) => {
      await deleteSelection(
        actions,
        toDelete.map((n) => n.id),
        // React Flow adds the edges of deleted nodes; only explicitly selected edges matter.
        edgesToDelete.filter((e) => e.selected === true).map((e) => e.id),
      );
      return false;
    },
    [actions],
  );

  const isValidConnection = useCallback<IsValidConnection>(
    (c) => checkConnection(useTopologyStore.getState(), c.source, c.target).ok,
    [],
  );

  // One place for every finished drag: dropped on a handle or anywhere on a node's body.
  const onConnectEnd = useCallback<OnConnectEnd>(
    (event, state) => {
      const start = state.fromNode;
      if (start === null) return;
      let endId = state.toNode?.id;
      if (endId === undefined) {
        const point = clientPoint(event);
        if (point === undefined) return;
        endId = nodeAtPoint(
          nodeRects(store.getState().nodeLookup),
          flow.screenToFlowPosition(point),
        );
      }
      if (endId === undefined || endId === start.id) return;
      const { from, to } = orientConnection(start.id, endId, state.fromHandle.type === 'target');
      void connectChecked(actions, from, to);
    },
    [actions, flow, store],
  );

  const onDragOver = useCallback((event: DragEvent) => {
    if (!event.dataTransfer.types.includes(PALETTE_MIME)) return;
    event.preventDefault();
    event.dataTransfer.dropEffect = 'copy';
  }, []);

  const onDrop = useCallback(
    (event: DragEvent) => {
      const kind = event.dataTransfer.getData(PALETTE_MIME);
      if (!isComponentKind(kind)) return;
      event.preventDefault();
      const point = flow.screenToFlowPosition({ x: event.clientX, y: event.clientY });
      void actions.addComponent(kind, centredTopLeft(point, nodeSize(kind)));
    },
    [actions, flow],
  );

  return (
    <ReactFlow<FlowNode>
      className="kp-canvas"
      nodes={nodes}
      edges={edges}
      nodeTypes={NODE_TYPES}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onSelectionChange={onSelectionChange}
      onBeforeDelete={onBeforeDelete}
      isValidConnection={isValidConnection}
      onConnectEnd={onConnectEnd}
      onDragOver={onDragOver}
      onDrop={onDrop}
      defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
      defaultViewport={DEFAULT_VIEWPORT}
      snapToGrid
      snapGrid={SNAP_GRID}
      deleteKeyCode={DELETE_KEYS}
      multiSelectionKeyCode={['Meta', 'Control', 'Shift']}
      minZoom={0.25}
      maxZoom={2}
      proOptions={{ hideAttribution: true }}
    >
      <Background
        variant={BackgroundVariant.Dots}
        gap={GRID_SIZE}
        size={1.5}
        color="var(--color-grid)"
      />
      <MessageOverlay />
      <ConnectionHint />
      <CanvasNavigation />
    </ReactFlow>
  );
}

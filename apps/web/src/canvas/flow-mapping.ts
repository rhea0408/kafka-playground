/**
 * topologyStore → React Flow nodes and edges. Pure, so the mapping is unit-tested without a DOM.
 *
 * Every component is one node with the component id as node id. A consumer is drawn as a group
 * node (header = its `group.id`, one row = the consumer) so M3 only has to put more rows in it.
 * Brokers have no edges: a topic's partitions are placed on brokers by the cluster, not wired by
 * hand (`canConnect`); T8 shows the placement as leader chips on the partition rows.
 */

import type { Broker, Component, Consumer, Connection, Producer, Topic } from '@kp/schema';
import type { Edge, Node } from '@xyflow/react';
import type { TopologyState } from '../stores/topology-store.ts';
import { consumerRowHandleId, HANDLE_IN, HANDLE_OUT, nodeSize } from './geometry.ts';

export type ProducerNodeData = { readonly component: Producer };
export type TopicNodeData = { readonly component: Topic };
export type BrokerNodeData = { readonly component: Broker };
/** One group on the canvas. In M2 every consumer is alone in its group, so `members` has one entry. */
export type ConsumerGroupNodeData = {
  readonly groupId: string;
  readonly members: readonly Consumer[];
};

export type ProducerFlowNode = Node<ProducerNodeData, 'producer'>;
export type TopicFlowNode = Node<TopicNodeData, 'topic'>;
export type BrokerFlowNode = Node<BrokerNodeData, 'broker'>;
export type ConsumerGroupFlowNode = Node<ConsumerGroupNodeData, 'consumerGroup'>;
export type FlowNode = ProducerFlowNode | TopicFlowNode | BrokerFlowNode | ConsumerGroupFlowNode;

const ARROW = '→';

/** Edge id for a connection: `p1→t1`. Component ids never contain `→`. */
export function edgeId(connection: Connection): string {
  return `${connection.from}${ARROW}${connection.to}`;
}

export function parseEdgeId(id: string): Connection | undefined {
  const at = id.indexOf(ARROW);
  if (at <= 0 || at === id.length - 1) return undefined;
  return { from: id.slice(0, at), to: id.slice(at + ARROW.length) };
}

const KIND_LABEL = {
  producer: 'Producer',
  topic: 'Topic',
  broker: 'Broker',
  consumer: 'Consumer',
} as const;

function baseNode(component: Component, position: { x: number; y: number }) {
  const partitions = component.kind === 'topic' ? component.partitions : 1;
  const size = nodeSize(component.kind, partitions);
  return {
    id: component.id,
    position,
    width: size.width,
    height: size.height,
    ariaLabel: `${KIND_LABEL[component.kind]} ${component.id}`,
  };
}

export function toFlowNode(component: Component, position: { x: number; y: number }): FlowNode {
  const base = baseNode(component, position);
  switch (component.kind) {
    case 'producer':
      return { ...base, type: 'producer', data: { component } };
    case 'topic':
      return { ...base, type: 'topic', data: { component } };
    case 'broker':
      return { ...base, type: 'broker', data: { component } };
    case 'consumer':
      return {
        ...base,
        type: 'consumerGroup',
        ariaLabel: `Consumer ${component.id} in group ${component.groupId}`,
        data: { groupId: component.groupId, members: [component] },
      };
  }
}

/**
 * Nodes for the current canvas. React Flow's own state on the previous nodes (selection,
 * dragging, measured size) is kept, so re-deriving after a store change doesn't drop a selection.
 */
export function toFlowNodes(state: TopologyState, previous: readonly FlowNode[] = []): FlowNode[] {
  const before = new Map(previous.map((n) => [n.id, n]));
  return state.components.map((component) => {
    const position = state.positions[component.id] ?? { x: 0, y: 0 };
    const node = toFlowNode(component, position);
    const prev = before.get(component.id);
    if (prev === undefined) return node;
    return {
      ...node,
      // While a node is dragged React Flow owns its position; the store catches up on drag stop.
      position: prev.dragging === true ? prev.position : node.position,
      ...(prev.selected === undefined ? {} : { selected: prev.selected }),
      ...(prev.dragging === undefined ? {} : { dragging: prev.dragging }),
      ...(prev.measured === undefined ? {} : { measured: prev.measured }),
    };
  });
}

/** Edges for the current canvas, keeping React Flow's selection from the previous edges. */
export function toFlowEdges(state: TopologyState, previous: readonly Edge[] = []): Edge[] {
  const selected = new Set(previous.filter((e) => e.selected === true).map((e) => e.id));
  return state.connections.map((connection) => {
    const id = edgeId(connection);
    const target = state.components.find((c) => c.id === connection.to);
    return {
      id,
      source: connection.from,
      target: connection.to,
      sourceHandle: HANDLE_OUT,
      targetHandle: target?.kind === 'consumer' ? consumerRowHandleId(target.id) : HANDLE_IN,
      ariaLabel: `Connection ${connection.from} to ${connection.to}`,
      selected: selected.has(id),
    };
  });
}

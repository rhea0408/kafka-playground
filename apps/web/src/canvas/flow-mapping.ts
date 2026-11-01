/**
 * topologyStore → React Flow nodes and edges. Pure, so the mapping is unit-tested without a DOM.
 *
 * Producers, topics and brokers are one node each, with the component id as node id. Consumers
 * are drawn by **group** (M3-T8): one node per `group.id`, id `group:<groupId>`, with one row per
 * member, in the order the consumers were added. The group node is a view of consumers that share
 * a group.id, not an engine component (M3 Q4): it appears with its first member and goes with its
 * last. Its position is stored under its node id; until it is moved, it sits where its first
 * member was dropped.
 *
 * Connections stay per consumer in the store and the engine (subscriptions are per consumer, Q6);
 * the canvas draws one edge per (topic, group) when any member reads the topic.
 * Brokers have no edges: a topic's partitions are placed on brokers by the cluster, not wired by
 * hand (`canConnect`); leader chips on the partition rows show the placement.
 */

import type { Broker, Component, Consumer, Connection, Producer, Topic } from '@kp/schema';
import type { Edge, Node } from '@xyflow/react';
import type { TopologyState } from '../stores/topology-store.ts';
import { HANDLE_IN, HANDLE_OUT, nodeSize } from './geometry.ts';

export type ProducerNodeData = { readonly component: Producer };
export type TopicNodeData = { readonly component: Topic };
export type BrokerNodeData = { readonly component: Broker };
/** One consumer group on the canvas: its members are the consumers with this `group.id`. */
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
const GROUP_PREFIX = 'group:';

/** Node id of a group: `group:orders-app`. Component ids can't contain ":", so it never clashes. */
export function groupNodeId(groupId: string): string {
  return `${GROUP_PREFIX}${groupId}`;
}

/** The group id of a group node id, or undefined for any other id. */
export function groupIdOfNode(nodeId: string): string | undefined {
  return nodeId.startsWith(GROUP_PREFIX) ? nodeId.slice(GROUP_PREFIX.length) : undefined;
}

/** The consumers of a group, in the order they were added. */
export function membersOf(state: TopologyState, groupId: string): Consumer[] {
  return state.components.filter(
    (c): c is Consumer => c.kind === 'consumer' && c.groupId === groupId,
  );
}

/** The canvas node a component is drawn in: its own, or its group's for a consumer. */
export function nodeIdOf(component: Component): string {
  return component.kind === 'consumer' ? groupNodeId(component.groupId) : component.id;
}

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

/** A component's node; a consumer on its own is drawn as a one-member group. */
export function toFlowNode(component: Component, position: { x: number; y: number }): FlowNode {
  if (component.kind === 'consumer') return toGroupNode(component.groupId, [component], position);
  const base = baseNode(component, position);
  switch (component.kind) {
    case 'producer':
      return { ...base, type: 'producer', data: { component } };
    case 'topic':
      return { ...base, type: 'topic', data: { component } };
    case 'broker':
      return { ...base, type: 'broker', data: { component } };
  }
}

/** The node of one consumer group with its members as rows. */
export function toGroupNode(
  groupId: string,
  members: readonly Consumer[],
  position: { x: number; y: number },
): ConsumerGroupFlowNode {
  const size = nodeSize('consumer', 1, members.length);
  const count = `${String(members.length)} ${members.length === 1 ? 'member' : 'members'}`;
  return {
    id: groupNodeId(groupId),
    type: 'consumerGroup',
    position,
    width: size.width,
    height: size.height,
    ariaLabel: `Consumer group ${groupId}, ${count}: ${members.map((m) => m.id).join(', ')}`,
    data: { groupId, members },
  };
}

/** Where a group node is: its own stored position, else where its first member was dropped. */
function groupPosition(state: TopologyState, groupId: string, members: readonly Consumer[]) {
  const first = members[0];
  return (
    state.positions[groupNodeId(groupId)] ??
    (first === undefined ? undefined : state.positions[first.id]) ?? { x: 0, y: 0 }
  );
}

/** Every node: components in the order they were added, each group where its first member is. */
function freshNodes(state: TopologyState): FlowNode[] {
  const nodes: FlowNode[] = [];
  const groups = new Set<string>();
  for (const component of state.components) {
    if (component.kind !== 'consumer') {
      nodes.push(toFlowNode(component, state.positions[component.id] ?? { x: 0, y: 0 }));
      continue;
    }
    if (groups.has(component.groupId)) continue;
    groups.add(component.groupId);
    const members = membersOf(state, component.groupId);
    nodes.push(
      toGroupNode(component.groupId, members, groupPosition(state, component.groupId, members)),
    );
  }
  return nodes;
}

/**
 * Nodes for the current canvas. React Flow's own state on the previous nodes (selection,
 * dragging, measured size) is kept, so re-deriving after a store change doesn't drop a selection.
 */
export function toFlowNodes(state: TopologyState, previous: readonly FlowNode[] = []): FlowNode[] {
  const before = new Map(previous.map((n) => [n.id, n]));
  return freshNodes(state).map((node): FlowNode => {
    const prev = before.get(node.id);
    if (prev === undefined) return node;
    // A group that gained or lost a row has a new height: React Flow must measure it again.
    const resized = prev.height !== node.height;
    return {
      ...node,
      // While a node is dragged React Flow owns its position; the store catches up on drag stop.
      position: prev.dragging === true ? prev.position : node.position,
      ...(prev.selected === undefined ? {} : { selected: prev.selected }),
      ...(prev.dragging === undefined ? {} : { dragging: prev.dragging }),
      ...(prev.measured === undefined || resized ? {} : { measured: prev.measured }),
    };
  });
}

/**
 * Edges for the current canvas, keeping React Flow's selection from the previous edges. A topic
 * read by several members of a group gets one edge to the group node (`t1→group:g`).
 */
export function toFlowEdges(state: TopologyState, previous: readonly Edge[] = []): Edge[] {
  const selected = new Set(previous.filter((e) => e.selected === true).map((e) => e.id));
  const edges: Edge[] = [];
  const seen = new Set<string>();
  for (const connection of state.connections) {
    const target = state.components.find((c) => c.id === connection.to);
    const to = target === undefined ? connection.to : nodeIdOf(target);
    const id = edgeId({ from: connection.from, to });
    if (seen.has(id)) continue;
    seen.add(id);
    const group = target?.kind === 'consumer' ? target.groupId : undefined;
    edges.push({
      id,
      source: connection.from,
      target: to,
      sourceHandle: HANDLE_OUT,
      targetHandle: HANDLE_IN,
      ariaLabel:
        group === undefined
          ? `Connection ${connection.from} to ${connection.to}`
          : `Connection ${connection.from} to consumer group ${group}`,
      selected: selected.has(id),
    });
  }
  return edges;
}

/**
 * The store connections behind a canvas edge: one for a plain edge, one per member that reads the
 * topic for a topic → group edge.
 */
export function connectionsOfEdge(state: TopologyState, id: string): Connection[] {
  const edge = parseEdgeId(id);
  if (edge === undefined) return [];
  const groupId = groupIdOfNode(edge.to);
  if (groupId === undefined) return [edge];
  const members = new Set(membersOf(state, groupId).map((m) => m.id));
  return state.connections.filter((c) => c.from === edge.from && members.has(c.to));
}

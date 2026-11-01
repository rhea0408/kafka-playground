/**
 * Checks a canvas connection before it is sent to the engine (FR-1.2). The kind rules come from
 * `canConnect` in @kp/schema, which the engine uses too, so the canvas and the engine always give
 * the same answer. This adds the checks that need the whole canvas: unknown ids, a component
 * connected to itself, and a connection that already exists. A consumer group node counts as a
 * consumer; connecting a topic to it connects every member (M3-T8).
 */

import { canConnect, type ComponentKind, type Connection, type ConnectionCheck } from '@kp/schema';
import { findComponent, type TopologyState } from '../stores/topology-store.ts';
import { groupIdOfNode, membersOf } from './flow-mapping.ts';

/** What a canvas node stands for: one component, or a consumer group's members. */
interface Endpoint {
  readonly kind: ComponentKind;
  /** Component ids behind the node: the component itself, or every member of the group. */
  readonly ids: readonly string[];
}

function endpoint(state: TopologyState, nodeId: string): Endpoint | undefined {
  const groupId = groupIdOfNode(nodeId);
  if (groupId !== undefined) {
    const members = membersOf(state, groupId).map((m) => m.id);
    return members.length === 0 ? undefined : { kind: 'consumer', ids: members };
  }
  const component = findComponent(state, nodeId);
  return component === undefined ? undefined : { kind: component.kind, ids: [component.id] };
}

/**
 * The connections a drag from node `from` to node `to` makes. A topic dropped on a consumer group
 * connects to every member that does not read it yet (M3 Q6: subscriptions are per consumer).
 */
export function plannedConnections(state: TopologyState, from: string, to: string): Connection[] {
  const source = endpoint(state, from);
  const target = endpoint(state, to);
  if (source === undefined || target === undefined) return [];
  return source.ids.flatMap((f) =>
    target.ids
      .filter((t) => !state.connections.some((c) => c.from === f && c.to === t))
      .map((t) => ({ from: f, to: t })),
  );
}

export function checkConnection(state: TopologyState, from: string, to: string): ConnectionCheck {
  if (from === to) return { ok: false, reason: "A component can't be connected to itself." };
  const source = endpoint(state, from);
  const target = endpoint(state, to);
  if (source === undefined || target === undefined) {
    return { ok: false, reason: `There is no component with id "${source ? to : from}".` };
  }
  const kinds = canConnect(source.kind, target.kind);
  if (!kinds.ok) return kinds;
  if (plannedConnections(state, from, to).length === 0) {
    const name = (nodeId: string) => {
      const group = groupIdOfNode(nodeId);
      return group === undefined ? nodeId : `group ${group}`;
    };
    return { ok: false, reason: `${name(from)} → ${name(to)} is already connected.` };
  }
  return { ok: true };
}

/**
 * Which way a drag between two nodes means. React Flow reports the node the drag started on and
 * the node it ended on; a drag that started on a target (input) handle runs backwards.
 */
export function orientConnection(
  startNodeId: string,
  endNodeId: string,
  startedOnTarget: boolean,
): { readonly from: string; readonly to: string } {
  return startedOnTarget
    ? { from: endNodeId, to: startNodeId }
    : { from: startNodeId, to: endNodeId };
}

/**
 * Canvas gestures turned into actions: checked connections and multi-delete. Kept apart from the
 * React component so they are tested without React Flow.
 */

import type { DispatchResult } from '@kp/engine';
import { COMPONENT_KINDS, type ComponentKind } from '@kp/schema';
import type { Actions } from '../actions/actions.ts';
import { useTopologyStore, type TopologyState } from '../stores/topology-store.ts';
import { pushToast } from '../stores/ui-store.ts';
import { checkConnection, plannedConnections } from './connection-check.ts';
import { connectionsOfEdge, groupIdOfNode, membersOf } from './flow-mapping.ts';

/** Drag-and-drop payload type for palette items. */
export const PALETTE_MIME = 'application/x-kafka-playground-component';

export function isComponentKind(value: string): value is ComponentKind {
  return (COMPONENT_KINDS as readonly string[]).includes(value);
}

/**
 * Connects `from` → `to` if the canvas rules allow it (FR-1.2). A refused link shows its reason as
 * a warning toast and sends nothing; an engine refusal is toasted by the actions module.
 */
export async function connectChecked(
  actions: Pick<Actions, 'connect'>,
  from: string,
  to: string,
): Promise<DispatchResult> {
  const state = useTopologyStore.getState();
  const check = checkConnection(state, from, to);
  if (!check.ok) {
    pushToast('warning', check.reason);
    return check;
  }
  // A group node stands for its members: one Connect per member (M3 Q6).
  let result: DispatchResult = { ok: true };
  for (const c of plannedConnections(state, from, to)) {
    const r = await actions.connect(c.from, c.to);
    if (!r.ok) result = r;
  }
  return result;
}

/**
 * Deletes a selection: first the selected edges (`Disconnect`, one per member for a topic → group
 * edge), then the nodes (`RemoveComponent`; a group node removes its focused member, or all of
 * them). Edges attached to a removed node are left to its `RemoveComponent`, which drops them in
 * the same engine step (M2-decisions T5-5). Commands are sent one by one, in order; a refusal
 * (toasted by the actions module) doesn't stop the rest.
 */
export async function deleteSelection(
  actions: Pick<Actions, 'disconnect' | 'removeComponent'>,
  nodeIds: readonly string[],
  edgeIds: readonly string[],
  focusedMember: string | null = null,
): Promise<void> {
  const state = useTopologyStore.getState();
  const components = nodeIds.flatMap((id) => componentsOfNode(state, id, focusedMember));
  const removed = new Set(components);
  for (const id of edgeIds) {
    for (const connection of connectionsOfEdge(state, id)) {
      if (removed.has(connection.from) || removed.has(connection.to)) continue;
      await actions.disconnect(connection.from, connection.to);
    }
  }
  for (const id of components) await actions.removeComponent(id);
}

/**
 * The components a selected node stands for: itself; or, for a consumer group node, the member
 * whose row is focused (if it is in this group), otherwise every member.
 */
export function componentsOfNode(
  state: TopologyState,
  nodeId: string,
  focusedMember: string | null,
): string[] {
  const groupId = groupIdOfNode(nodeId);
  if (groupId === undefined) return [nodeId];
  const members = membersOf(state, groupId).map((m) => m.id);
  return focusedMember !== null && members.includes(focusedMember) ? [focusedMember] : members;
}

/**
 * Canvas gestures turned into actions: checked connections and multi-delete. Kept apart from the
 * React component so they are tested without React Flow.
 */

import type { DispatchResult } from '@kp/engine';
import { COMPONENT_KINDS, type ComponentKind } from '@kp/schema';
import type { Actions } from '../actions/actions.ts';
import { useTopologyStore } from '../stores/topology-store.ts';
import { pushToast } from '../stores/ui-store.ts';
import { checkConnection } from './connection-check.ts';
import { parseEdgeId } from './flow-mapping.ts';

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
  const check = checkConnection(useTopologyStore.getState(), from, to);
  if (!check.ok) {
    pushToast('warning', check.reason);
    return check;
  }
  return actions.connect(from, to);
}

/**
 * Deletes a selection: first the selected edges (`Disconnect`), then the nodes (`RemoveComponent`).
 * Edges attached to a removed node are left to its `RemoveComponent`, which drops them in the same
 * engine step (M2-decisions T5-5). Commands are sent one by one, in order; a refusal (toasted by
 * the actions module) doesn't stop the rest.
 */
export async function deleteSelection(
  actions: Pick<Actions, 'disconnect' | 'removeComponent'>,
  nodeIds: readonly string[],
  edgeIds: readonly string[],
): Promise<void> {
  const removed = new Set(nodeIds);
  for (const id of edgeIds) {
    const connection = parseEdgeId(id);
    if (connection === undefined) continue;
    if (removed.has(connection.from) || removed.has(connection.to)) continue;
    await actions.disconnect(connection.from, connection.to);
  }
  for (const id of nodeIds) await actions.removeComponent(id);
}

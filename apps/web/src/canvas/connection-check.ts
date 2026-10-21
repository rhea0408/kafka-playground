/**
 * Checks a canvas connection before it is sent to the engine (FR-1.2). The kind rules come from
 * `canConnect` in @kp/schema, which the engine uses too, so the canvas and the engine always give
 * the same answer. This adds the checks that need the whole canvas: unknown ids, a component
 * connected to itself, and a connection that already exists.
 */

import { canConnect, type ConnectionCheck } from '@kp/schema';
import { findComponent, type TopologyState } from '../stores/topology-store.ts';

export function checkConnection(state: TopologyState, from: string, to: string): ConnectionCheck {
  if (from === to) return { ok: false, reason: "A component can't be connected to itself." };
  const source = findComponent(state, from);
  const target = findComponent(state, to);
  if (source === undefined || target === undefined) {
    return { ok: false, reason: `There is no component with id "${source ? to : from}".` };
  }
  const kinds = canConnect(source.kind, target.kind);
  if (!kinds.ok) return kinds;
  if (state.connections.some((c) => c.from === from && c.to === to)) {
    return { ok: false, reason: `${from} → ${to} is already connected.` };
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

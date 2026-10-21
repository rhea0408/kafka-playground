/**
 * topologyStore: what is on the canvas (architecture §5). Component configs and connections are
 * mirrored to the engine as commands by the actions module, which changes this store only after
 * the engine accepted the command. Node positions are UI-only and never reach the engine.
 */

import type { Component, ComponentKind, Connection, TopologyInput } from '@kp/schema';
import { create } from 'zustand';

export interface XYPosition {
  readonly x: number;
  readonly y: number;
}

export interface TopologyState {
  /** Every component, in the order it was added (the topology lists keep that order). */
  readonly components: readonly Component[];
  readonly connections: readonly Connection[];
  /** Canvas positions by component id. UI only. */
  readonly positions: Readonly<Record<string, XYPosition>>;
}

/** The one broker every Sandbox starts with: M2 runs a single-broker cluster. */
export const INITIAL_BROKER_ID = 'b1';

export const INITIAL_TOPOLOGY_STATE: TopologyState = {
  components: [{ kind: 'broker', id: INITIAL_BROKER_ID, rack: null }],
  connections: [],
  positions: { [INITIAL_BROKER_ID]: { x: 40, y: 40 } },
};

export const useTopologyStore = create<TopologyState>()(() => INITIAL_TOPOLOGY_STATE);

/** Restores the initial canvas (tests, and later "new sandbox"). */
export function resetTopologyStore(state: TopologyState = INITIAL_TOPOLOGY_STATE): void {
  useTopologyStore.setState(state, true);
}

// ── Pure updates (used by the actions module) ───────────────────────────────────────────────

export function findComponent(state: TopologyState, id: string): Component | undefined {
  return state.components.find((c) => c.id === id);
}

export function withComponentAdded(
  state: TopologyState,
  component: Component,
  position: XYPosition,
): TopologyState {
  return {
    ...state,
    components: [...state.components, component],
    positions: { ...state.positions, [component.id]: position },
  };
}

/** Drops the component, its connections and its position (the engine drops the connections too). */
export function withComponentRemoved(state: TopologyState, id: string): TopologyState {
  const { [id]: _removed, ...positions } = state.positions;
  return {
    components: state.components.filter((c) => c.id !== id),
    connections: state.connections.filter((c) => c.from !== id && c.to !== id),
    positions,
  };
}

export function withComponentReplaced(state: TopologyState, component: Component): TopologyState {
  return {
    ...state,
    components: state.components.map((c) => (c.id === component.id ? component : c)),
  };
}

export function withConnection(state: TopologyState, connection: Connection): TopologyState {
  return { ...state, connections: [...state.connections, connection] };
}

export function withoutConnection(state: TopologyState, connection: Connection): TopologyState {
  return {
    ...state,
    connections: state.connections.filter(
      (c) => !(c.from === connection.from && c.to === connection.to),
    ),
  };
}

export function withPosition(
  state: TopologyState,
  id: string,
  position: XYPosition,
): TopologyState {
  return { ...state, positions: { ...state.positions, [id]: position } };
}

/** The canvas as an engine topology (for `init`): configs and connections, no positions. */
export function toTopologyInput(state: TopologyState): TopologyInput {
  const of = <K extends ComponentKind>(kind: K) =>
    state.components.filter((c): c is Extract<Component, { kind: K }> => c.kind === kind);
  return {
    brokers: of('broker'),
    topics: of('topic'),
    producers: of('producer'),
    consumers: of('consumer'),
    connections: state.connections.map((c) => ({ ...c })),
  };
}

/** Id prefix per kind: `b1`, `t1`, `p1`, `c1`, as in the engine's tests and demo. */
export const ID_PREFIX: Readonly<Record<ComponentKind, string>> = {
  broker: 'b',
  topic: 't',
  producer: 'p',
  consumer: 'c',
};

/**
 * Next free id for a kind: prefix + (highest number in use for that prefix + 1). Never reuses a
 * lower number while a higher one exists, so ids keep increasing within a session's canvas.
 */
export function nextComponentId(state: TopologyState, kind: ComponentKind): string {
  const prefix = ID_PREFIX[kind];
  const pattern = new RegExp(`^${prefix}(\\d+)$`);
  let max = 0;
  for (const component of state.components) {
    const match = pattern.exec(component.id);
    if (match?.[1] !== undefined) max = Math.max(max, Number(match[1]));
  }
  return `${prefix}${String(max + 1)}`;
}

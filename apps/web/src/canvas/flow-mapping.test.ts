import { parseComponent, type Component } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { INITIAL_TOPOLOGY_STATE, type TopologyState } from '../stores/topology-store.ts';
import { checkConnection, orientConnection, plannedConnections } from './connection-check.ts';
import {
  connectionsOfEdge,
  edgeId,
  groupIdOfNode,
  parseEdgeId,
  toFlowEdges,
  toFlowNodes,
} from './flow-mapping.ts';

function component(input: Record<string, unknown>): Component {
  const parsed = parseComponent(input);
  if (!parsed.ok) throw new Error(parsed.errors.join(' '));
  return parsed.value;
}

const state: TopologyState = {
  components: [
    ...INITIAL_TOPOLOGY_STATE.components,
    component({ kind: 'producer', id: 'p1' }),
    component({ kind: 'topic', id: 'orders', partitions: 3 }),
    component({ kind: 'consumer', id: 'c1' }),
  ],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'orders', to: 'c1' },
  ],
  positions: { b1: { x: 40, y: 40 }, p1: { x: 0, y: 160 }, orders: { x: 240, y: 160 } },
};

describe('edge ids', () => {
  it('round-trip, including ids with dashes and dots', () => {
    for (const c of [
      { from: 'p1', to: 't1' },
      { from: 'a-', to: '-b' },
      { from: 'orders.v2', to: 'c_1' },
    ]) {
      expect(parseEdgeId(edgeId(c))).toEqual(c);
    }
    expect(parseEdgeId('nonsense')).toBeUndefined();
  });
});

describe('toFlowNodes / toFlowEdges', () => {
  it('maps each component to a node of its kind at its size', () => {
    const nodes = toFlowNodes(state);
    expect(nodes.map((n) => [n.id, n.type, n.width, n.height])).toEqual([
      ['b1', 'broker', 226, 84],
      ['p1', 'producer', 150, 72],
      ['orders', 'topic', 230, 36 + 3 * 48],
      ['group:c1-group', 'consumerGroup', 240, 92],
    ]);
    // A missing position falls back to the origin.
    expect(nodes[3]?.position).toEqual({ x: 0, y: 0 });
  });

  it('draws a consumer as a group with itself as the only row', () => {
    const group = toFlowNodes(state).find((n) => n.id === 'group:c1-group');
    expect(group?.type === 'consumerGroup' && group.data.groupId).toBe('c1-group');
    expect(group?.type === 'consumerGroup' && group.data.members.map((m) => m.id)).toEqual(['c1']);
  });

  it('keeps React Flow selection and the position of a node being dragged', () => {
    const first = toFlowNodes(state);
    const prev = first.map((n) =>
      n.id === 'p1' ? { ...n, selected: true, dragging: true, position: { x: 99, y: 99 } } : n,
    );
    const next = toFlowNodes(state, prev);
    const p1 = next.find((n) => n.id === 'p1');
    expect(p1?.selected).toBe(true);
    expect(p1?.position).toEqual({ x: 99, y: 99 });
  });

  it('edges use the fixed handles: producer out → topic in, topic out → group in', () => {
    expect(
      toFlowEdges(state).map((e) => [e.id, e.target, e.sourceHandle, e.targetHandle, e.selected]),
    ).toEqual([
      ['p1→orders', 'orders', 'out', 'in', false],
      ['orders→group:c1-group', 'group:c1-group', 'out', 'in', false],
    ]);
    const prev = toFlowEdges(state).map((e) => ({
      ...e,
      selected: e.id === 'orders→group:c1-group',
    }));
    expect(toFlowEdges(state, prev).map((e) => e.selected)).toEqual([false, true]);
  });
});

describe('consumer groups on the canvas (M3-T8)', () => {
  const grouped: TopologyState = {
    ...state,
    components: [
      ...state.components,
      component({ kind: 'consumer', id: 'c2', groupId: 'c1-group' }),
      component({ kind: 'consumer', id: 'c3', groupId: 'billing' }),
    ],
    connections: [...state.connections, { from: 'orders', to: 'c2' }],
    positions: { ...state.positions, c1: { x: 500, y: 100 }, 'group:billing': { x: 9, y: 9 } },
  };

  it('one node per group with a row per member, placed where its first member was dropped', () => {
    const nodes = toFlowNodes(grouped);
    const g = nodes.find((n) => n.id === 'group:c1-group');
    expect(g?.type === 'consumerGroup' && g.data.members.map((m) => m.id)).toEqual(['c1', 'c2']);
    expect([g?.height, g?.position]).toEqual([140, { x: 500, y: 100 }]);
    expect(nodes.find((n) => n.id === 'group:billing')?.position).toEqual({ x: 9, y: 9 });
    expect(nodes.filter((n) => n.type === 'consumerGroup')).toHaveLength(2);
  });

  it('one edge per topic and group; the edge stands for every member connection', () => {
    expect(toFlowEdges(grouped).map((e) => e.id)).toEqual(['p1→orders', 'orders→group:c1-group']);
    expect(connectionsOfEdge(grouped, 'orders→group:c1-group')).toEqual([
      { from: 'orders', to: 'c1' },
      { from: 'orders', to: 'c2' },
    ]);
    expect(connectionsOfEdge(grouped, 'p1→orders')).toEqual([{ from: 'p1', to: 'orders' }]);
  });

  it('connecting a topic to a group connects the members that do not read it yet (Q6)', () => {
    expect(plannedConnections(grouped, 'orders', 'group:billing')).toEqual([
      { from: 'orders', to: 'c3' },
    ]);
    expect(checkConnection(grouped, 'orders', 'group:billing')).toEqual({ ok: true });
    expect(checkConnection(grouped, 'orders', 'group:c1-group')).toEqual({
      ok: false,
      reason: 'orders → group c1-group is already connected.',
    });
    expect(checkConnection(grouped, 'p1', 'group:billing')).toMatchObject({ ok: false });
    expect(checkConnection(grouped, 'orders', 'group:nobody')).toMatchObject({ ok: false });
    expect(groupIdOfNode('group:billing')).toBe('billing');
    expect(groupIdOfNode('orders')).toBeUndefined();
  });
});

describe('checkConnection (FR-1.2)', () => {
  it('allows producer → topic and topic → consumer once', () => {
    const empty = { ...state, connections: [] };
    expect(checkConnection(empty, 'p1', 'orders')).toEqual({ ok: true });
    expect(checkConnection(empty, 'orders', 'c1')).toEqual({ ok: true });
  });

  it("refuses with the schema's reason, a duplicate, a self-link and unknown ids", () => {
    const refused = (from: string, to: string) => {
      const check = checkConnection(state, from, to);
      return check.ok ? '' : check.reason;
    };
    expect(refused('p1', 'c1')).toContain("A producer can't send straight to a consumer.");
    expect(refused('orders', 'b1')).toContain("Brokers aren't connected by hand.");
    expect(refused('p1', 'orders')).toBe('p1 → orders is already connected.');
    expect(refused('p1', 'p1')).toBe("A component can't be connected to itself.");
    expect(refused('p1', 'nope')).toBe('There is no component with id "nope".');
  });

  it('a drag that started on an input handle runs backwards', () => {
    expect(orientConnection('a', 'b', false)).toEqual({ from: 'a', to: 'b' });
    expect(orientConnection('a', 'b', true)).toEqual({ from: 'b', to: 'a' });
  });
});

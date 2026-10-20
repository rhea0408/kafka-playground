import { parseComponent, type Component } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { INITIAL_TOPOLOGY_STATE, type TopologyState } from '../stores/topology-store.ts';
import { checkConnection, orientConnection } from './connection-check.ts';
import { edgeId, parseEdgeId, toFlowEdges, toFlowNodes } from './flow-mapping.ts';

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
      ['c1', 'consumerGroup', 220, 92],
    ]);
    // A missing position falls back to the origin.
    expect(nodes[3]?.position).toEqual({ x: 0, y: 0 });
  });

  it('draws a consumer as a group with itself as the only row', () => {
    const group = toFlowNodes(state).find((n) => n.id === 'c1');
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

  it('edges use the fixed handles: producer out → topic in, topic out → consumer row', () => {
    expect(
      toFlowEdges(state).map((e) => [e.id, e.sourceHandle, e.targetHandle, e.selected]),
    ).toEqual([
      ['p1→orders', 'out', 'in', false],
      ['orders→c1', 'out', 'row:c1', false],
    ]);
    const prev = toFlowEdges(state).map((e) => ({ ...e, selected: e.id === 'orders→c1' }));
    expect(toFlowEdges(state, prev).map((e) => e.selected)).toEqual([false, true]);
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

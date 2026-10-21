// @vitest-environment jsdom
import type { EngineSnapshot } from '@kp/engine';
import { diffSnapshot } from '@kp/engine';
import { parseComponent, type Component } from '@kp/schema';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { ReactFlow, ReactFlowProvider } from '@xyflow/react';
import { Profiler, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ActionsProvider } from '../actions/actions-context.tsx';
import type { Actions } from '../actions/actions.ts';
import { applySimFrame, resetSimStore } from '../stores/sim-store.ts';
import { resetUiStore, useUiStore } from '../stores/ui-store.ts';
import {
  consumer,
  consumerPartition,
  fixtureSnapshot,
  partition,
  producer,
} from '../test/snapshot-fixture.ts';
import { toFlowNode, type FlowNode } from './flow-mapping.ts';
import { ConsumerLive, LagBadge, PartitionLive, ProducerCounters } from './live/live-parts.tsx';
import { NODE_TYPES } from './nodes.tsx';

function component(input: Record<string, unknown>): Component {
  const parsed = parseComponent(input);
  if (!parsed.ok) throw new Error(parsed.errors.join(' '));
  return parsed.value;
}

const P1 = component({ kind: 'producer', id: 'p1' });
const T1 = component({ kind: 'topic', id: 't1', partitions: 2 });
const C1 = component({ kind: 'consumer', id: 'c1' });

let frame = 0;

/** Sends `snapshot` the way the host does: a full frame first, diffs after that. */
function show(snapshot: EngineSnapshot, prev?: EngineSnapshot): void {
  frame += 1;
  const base = { frame, t: snapshot.t, playing: true, speed: 1, events: [] };
  act(() => {
    applySimFrame(
      prev === undefined
        ? { ...base, kind: 'full', snapshot, history: [] }
        : { ...base, kind: 'diff', diff: diffSnapshot(prev, snapshot) },
    );
  });
}

function fakeActions() {
  const ok = { ok: true } as const;
  return {
    killConsumer: vi.fn<(id: string) => Promise<typeof ok>>(() => Promise.resolve(ok)),
    restartConsumer: vi.fn<(id: string) => Promise<typeof ok>>(() => Promise.resolve(ok)),
  };
}

function Providers({ actions, children }: { actions: Partial<Actions>; children: ReactNode }) {
  return (
    <ActionsProvider actions={actions as Actions}>
      <ReactFlowProvider>{children}</ReactFlowProvider>
    </ActionsProvider>
  );
}

function renderCanvas(nodes: FlowNode[], actions: Partial<Actions> = fakeActions()) {
  return render(
    <Providers actions={actions}>
      <div style={{ width: 1000, height: 600 }}>
        <ReactFlow nodes={nodes} nodeTypes={NODE_TYPES} />
      </div>
    </Providers>,
  );
}

const nodeEl = (id: string): HTMLElement => {
  const el = document.querySelector<HTMLElement>(`.react-flow__node[data-id="${id}"]`);
  if (el === null) throw new Error(`no node ${id}`);
  return el;
};

const row = (i: number): HTMLElement => {
  const el = nodeEl('t1').querySelector<HTMLElement>(`[data-partition-row="${String(i)}"]`);
  if (el === null) throw new Error(`no row ${String(i)}`);
  return el;
};

const cells = (i: number) =>
  [...row(i).querySelectorAll<HTMLElement>('.kp-cell')].map((c) => ({
    offset: Number(c.dataset['offset']),
    text: c.textContent,
    committed: c.dataset['committed'] === 'true',
    className: c.className,
  }));

describe('live node state (M2-T8)', () => {
  beforeEach(() => {
    resetSimStore();
    resetUiStore();
    frame = 0;
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('producer shows sent / acked / failed from the snapshot', () => {
    show(fixtureSnapshot());
    renderCanvas([toFlowNode(P1, { x: 0, y: 0 })]);
    const stats = within(nodeEl('p1')).getByRole('group', { name: '15 sent, 14 acked, 1 failed' });
    expect(
      [...stats.querySelectorAll('[data-stat]')].map(
        (s) => `${s.getAttribute('data-stat') ?? ''}=${s.textContent}`,
      ),
    ).toEqual(['sent=15', 'acked=14', 'failed=1']);
    expect(nodeEl('p1').textContent).toContain('acks=all');
  });

  it('topic rows show LEO, HW, the last 6 log cells with key letters, and the leader', () => {
    show(fixtureSnapshot());
    renderCanvas([toFlowNode(T1, { x: 0, y: 0 })]);
    expect(row(0).querySelector('[data-slot="partition-log"]')?.textContent).toBe('LEO 12 · HW 12');
    // Keys cycle A, B, null → offsets 6…11 are A B ∅ A B ∅.
    expect(cells(0).map((c) => `${String(c.offset)}${c.text}`)).toEqual([
      '6A',
      '7B',
      '8∅',
      '9A',
      '10B',
      '11∅',
    ]);
    expect(cells(0).every((c) => c.committed)).toBe(true);
    expect(cells(1).map((c) => c.text)).toEqual(['C', 'C', 'C']);
    const leader = within(row(0)).getByLabelText('Leader: broker b1');
    expect(leader.textContent).toBe('L b1');
  });

  it('records at or above HW are drawn as not committed (dashed)', () => {
    const snapshot = fixtureSnapshot();
    const [topic] = snapshot.topics;
    if (topic === undefined) throw new Error('fixture');
    show({ ...snapshot, topics: [{ ...topic, partitions: [partition(0, 12, { hw: 10 })] }] });
    renderCanvas([toFlowNode(component({ kind: 'topic', id: 't1' }), { x: 0, y: 0 })]);
    expect(row(0).querySelector('[data-slot="partition-log"]')?.textContent).toBe('LEO 12 · HW 10');
    expect(
      cells(0).map((c) => [c.offset, c.committed, c.className.includes('border-dashed')]),
    ).toEqual([
      [6, true, false],
      [7, true, false],
      [8, true, false],
      [9, true, false],
      [10, false, true],
      [11, false, true],
    ]);
  });

  it('A4: only newly appended cells slide in; the first render does not animate', () => {
    const first = fixtureSnapshot();
    show(first);
    renderCanvas([toFlowNode(T1, { x: 0, y: 0 })]);
    expect(cells(0).some((c) => c.className.includes('kp-cell'))).toBe(true);
    expect(cells(0).some((c) => /kp-cell-(in|fade)/.test(c.className))).toBe(false);

    const [topic] = first.topics;
    if (topic === undefined) throw new Error('fixture');
    const next = {
      ...first,
      t: 1100,
      topics: [
        { ...topic, partitions: [partition(0, 14), topic.partitions[1] ?? partition(1, 0)] },
      ],
    };
    show(next, first);
    const animated = cells(0).filter((c) => c.className.includes('kp-cell-in'));
    expect(animated.map((c) => c.offset)).toEqual([12, 13]);
    // Partition 1 did not change: none of its cells animate.
    expect(cells(1).some((c) => /kp-cell-(in|fade)/.test(c.className))).toBe(false);
  });

  it('A4 under reduced motion: new cells only fade (150 ms), no slide', () => {
    useUiStore.setState({ reducedMotion: true });
    const first = fixtureSnapshot();
    show(first);
    renderCanvas([toFlowNode(T1, { x: 0, y: 0 })]);
    const [topic] = first.topics;
    if (topic === undefined) throw new Error('fixture');
    const next = {
      ...first,
      topics: [
        { ...topic, partitions: [partition(0, 13), topic.partitions[1] ?? partition(1, 0)] },
      ],
    };
    show(next, first);
    const last = cells(0).at(-1);
    expect(last?.offset).toBe(12);
    expect(last?.className).toContain('kp-cell-fade');
    expect(last?.className).not.toContain('kp-cell-in');
  });

  it('consumer row shows status, summed position / committed, and total lag', () => {
    show(fixtureSnapshot());
    renderCanvas([toFlowNode(C1, { x: 0, y: 0 })]);
    const r = nodeEl('c1').querySelector<HTMLElement>('[data-consumer-row="c1"]');
    if (r === null) throw new Error('no row');
    expect(within(r).getByText('running')).toBeTruthy();
    expect(r.querySelector('[data-slot="offsets"]')?.textContent).toBe('Σ pos 15 · Σ committed 13');
    expect(r.querySelector('[data-slot="lag"]')?.getAttribute('data-lag')).toBe('2');
    expect(within(r).getByText('Lag 2')).toBeTruthy();
    // Per-partition breakdown for screen readers and the tooltip.
    expect(within(r).getByText('P0: position 12 · committed 10 · end 12 · lag 2')).toBeTruthy();
  });

  it('a single partition shows its exact offsets; a stopped consumer says so', () => {
    show(
      fixtureSnapshot({
        consumers: [consumer('c1', [consumerPartition(0, 9, null, 4)], 'stopped')],
      }),
    );
    renderCanvas([toFlowNode(C1, { x: 0, y: 0 })]);
    const r = nodeEl('c1').querySelector<HTMLElement>('[data-consumer-row="c1"]');
    if (r === null) throw new Error('no row');
    expect(within(r).getByText('stopped')).toBeTruthy();
    expect(r.querySelector('[data-slot="offsets"]')?.textContent).toBe('pos - · committed 4');
    expect(r.querySelector('[data-slot="lag"]')?.getAttribute('data-lag')).toBe('5');
  });

  it('Kill / Restart in the toolbar send the engine commands', () => {
    const actions = fakeActions();
    show(fixtureSnapshot());
    renderCanvas([{ ...toFlowNode(C1, { x: 0, y: 200 }), selected: true }], actions);
    fireEvent.click(screen.getByRole('button', { name: 'Kill consumer c1' }));
    expect(actions.killConsumer).toHaveBeenCalledWith('c1');

    show(
      fixtureSnapshot({
        consumers: [consumer('c1', [consumerPartition(0, 1, null, 1)], 'stopped')],
      }),
    );
    fireEvent.click(screen.getByRole('button', { name: 'Restart consumer c1' }));
    expect(actions.restartConsumer).toHaveBeenCalledWith('c1');
  });

  it('the toolbar shows on hover and is reachable by keyboard from the focused node', () => {
    show(fixtureSnapshot());
    renderCanvas([toFlowNode(C1, { x: 0, y: 200 })]);
    expect(screen.queryByRole('toolbar', { name: 'Consumer actions' })).toBeNull();

    fireEvent.pointerEnter(nodeEl('c1').querySelector('[data-kind="consumer"]') as HTMLElement);
    expect(screen.getByRole('toolbar', { name: 'Consumer actions' })).toBeTruthy();
    fireEvent.pointerLeave(nodeEl('c1').querySelector('[data-kind="consumer"]') as HTMLElement);

    // Keyboard: focus the node (Tab), then Tab again jumps into its toolbar.
    act(() => {
      nodeEl('c1').focus();
    });
    const kill = screen.getByRole('button', { name: 'Kill consumer c1' });
    fireEvent.keyDown(nodeEl('c1'), { key: 'Tab' });
    expect(document.activeElement).toBe(kill);
    // Escape goes back to the node; the toolbar stays while the node has focus.
    fireEvent.keyDown(kill, { key: 'Escape' });
    expect(document.activeElement).toBe(nodeEl('c1'));
    expect(screen.getByRole('toolbar', { name: 'Consumer actions' })).toBeTruthy();
  });
});

describe('<LagBadge> (A5)', () => {
  beforeEach(() => {
    resetUiStore();
    vi.useFakeTimers({ toFake: ['requestAnimationFrame', 'cancelAnimationFrame', 'performance'] });
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  const number = () => screen.getByTestId('lag-number').textContent;
  const badge = () => document.querySelector('[data-slot="lag"]');

  it('counts to the new value over 300 ms and turns danger above 100, with a sign', () => {
    const { rerender } = render(<LagBadge lag={0} />);
    expect(number()).toBe('lag 0');
    rerender(<LagBadge lag={400} />);
    // The real value is there for screen readers (and the danger state) straight away.
    expect(screen.getByText('Lag 400, above 100')).toBeTruthy();
    expect(badge()?.getAttribute('data-danger')).toBe('true');
    expect(badge()?.textContent).toContain('⚠');

    act(() => {
      vi.advanceTimersByTime(100);
    });
    const mid = Number(number().replace('lag ', ''));
    expect(mid).toBeGreaterThan(0);
    expect(mid).toBeLessThan(400);
    act(() => {
      vi.advanceTimersByTime(250);
    });
    expect(number()).toBe('lag 400');

    rerender(<LagBadge lag={3} />);
    expect(badge()?.getAttribute('data-danger')).toBe('false');
    act(() => {
      vi.advanceTimersByTime(400);
    });
    expect(number()).toBe('lag 3');
  });

  it('jumps straight to the value under reduced motion', () => {
    useUiStore.setState({ reducedMotion: true });
    const { rerender } = render(<LagBadge lag={0} />);
    rerender(<LagBadge lag={250} />);
    expect(number()).toBe('lag 250');
  });

  it('shows "-" when the group has no committed offset', () => {
    render(<LagBadge lag={null} />);
    expect(number()).toBe('lag -');
    expect(badge()?.getAttribute('data-danger')).toBe('false');
  });
});

describe('render isolation', () => {
  beforeEach(() => {
    resetSimStore();
    resetUiStore();
    frame = 0;
  });
  afterEach(cleanup);

  it('a frame re-renders only the parts whose own data changed', () => {
    const renders = new Map<string, number>();
    const count = (id: string) => () => {
      renders.set(id, (renders.get(id) ?? 0) + 1);
    };
    const first = fixtureSnapshot();
    show(first);
    render(
      <Providers actions={fakeActions()}>
        <Profiler id="p1" onRender={count('p1')}>
          <ProducerCounters producerId="p1" />
        </Profiler>
        <Profiler id="p2" onRender={count('p2')}>
          <ProducerCounters producerId="p2" />
        </Profiler>
        <Profiler id="t1-0" onRender={count('t1-0')}>
          <PartitionLive topicId="t1" index={0} />
        </Profiler>
        <Profiler id="t1-1" onRender={count('t1-1')}>
          <PartitionLive topicId="t1" index={1} />
        </Profiler>
        <Profiler id="c1" onRender={count('c1')}>
          <ConsumerLive consumerId="c1" />
        </Profiler>
      </Providers>,
    );
    renders.clear();

    // p1 sends one record that lands in t1-0. p2, t1-1 and c1 did not change.
    const [topic] = first.topics;
    if (topic === undefined) throw new Error('fixture');
    const next: EngineSnapshot = {
      ...first,
      t: 1033,
      producers: [producer('p1', 16, 15, 1), ...first.producers.slice(1)],
      topics: [{ ...topic, partitions: [partition(0, 13), ...topic.partitions.slice(1)] }],
    };
    show(next, first);
    expect(Object.fromEntries(renders)).toEqual({ p1: 1, 't1-0': 1 });

    // A frame with only the clock moving re-renders nothing.
    renders.clear();
    show({ ...next, t: 1066 }, next);
    expect(renders.size).toBe(0);
  });
});

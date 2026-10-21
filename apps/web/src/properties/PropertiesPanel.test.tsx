// @vitest-environment jsdom
import type { Command, DispatchResult } from '@kp/engine';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ActionsProvider } from '../actions/actions-context.tsx';
import { createActions, type Actions } from '../actions/actions.ts';
import { tabOnSelect } from '../components/layout/RightPanel.tsx';
import { SimHost } from '../sim/sim-host.ts';
import { applySimFrame, resetSimStore } from '../stores/sim-store.ts';
import { findComponent, resetTopologyStore, useTopologyStore } from '../stores/topology-store.ts';
import { resetUiStore, select } from '../stores/ui-store.ts';
import { FIELD_META, type FieldMeta } from './field-meta.ts';
import { helperLine, KindChip } from './FieldFrame.tsx';
import { panelView, PropertiesPanel } from './PropertiesPanel.tsx';

/** A real in-process SimHost (no worker), paused, so every accepted dispatch sends a frame. */
async function setup() {
  resetTopologyStore();
  resetSimStore();
  resetUiStore();
  const host = new SimHost({ now: () => 0, scheduleTick: () => () => undefined });
  host.subscribe((frame) => {
    applySimFrame(frame);
  });
  const dispatch = vi.spyOn(host, 'dispatch');
  const actions = createActions({ sim: host, seed: 7 });
  expect(await actions.init()).toEqual({ ok: true });
  await actions.addComponent('topic', { x: 300, y: 100 }, { partitions: 3 });
  await actions.addComponent('producer', { x: 50, y: 100 });
  await actions.addComponent('consumer', { x: 600, y: 100 });
  await actions.connect('p1', 't1');
  await actions.connect('t1', 'c1');
  dispatch.mockClear();
  return { host, actions, dispatch };
}

function renderPanel(actions: Actions) {
  return render(
    <ActionsProvider actions={actions}>
      <PropertiesPanel />
    </ActionsProvider>,
  );
}

function selectNode(id: string): void {
  act(() => {
    select({ nodeIds: [id], edgeIds: [] });
  });
}

const config = (id: string) => findComponent(useTopologyStore.getState(), id);
const updates = (dispatch: { mock: { calls: unknown[][] } }): Command[] =>
  dispatch.mock.calls
    .map((call) => call[0] as Command)
    .filter((command) => command.type === 'UpdateConfig');

/** Lets pending promise callbacks (dispatch → store → setState) run. */
const flush = () => act(async () => Promise.resolve());

describe('<PropertiesPanel>', () => {
  let ctx: Awaited<ReturnType<typeof setup>>;
  beforeEach(async () => {
    ctx = await setup();
  });
  afterEach(() => {
    cleanup();
  });

  it('shows a short empty state, a multi-select note and a connection note', () => {
    renderPanel(ctx.actions);
    expect(screen.getByTestId('properties-panel').textContent).toContain(
      'Select a component on the canvas',
    );
    act(() => {
      select({ nodeIds: ['p1', 't1'], edgeIds: ['p1→t1'] });
    });
    expect(screen.getByTestId('properties-panel').textContent).toContain('3 items selected');
    act(() => {
      select({ nodeIds: [], edgeIds: ['p1→t1'] });
    });
    expect(screen.getByTestId('properties-panel').textContent).toContain(
      'Connections have no config',
    );
  });

  it('renders a topic: stat tiles, stepper with warnings, not-modelled fields, consumers, delete', () => {
    renderPanel(ctx.actions);
    selectNode('t1');
    const tiles = screen.getByRole('group', { name: 'Topic stats' });
    expect(within(tiles).getByText('Partitions').nextSibling?.textContent).toBe('3');
    expect(within(tiles).getByText('LEO total').nextSibling?.textContent).toBe('0');
    expect(within(tiles).getByText('Consumers').nextSibling?.textContent).toBe('1');

    expect(screen.getByRole('spinbutton', { name: 'Partitions' })).toHaveProperty('value', '3');
    expect(screen.getByRole('note').textContent).toContain(
      'Adding partitions changes which partition each key goes to',
    );
    expect(screen.getByRole('note').textContent).toContain('metadata.max.age.ms');

    // Not simulated yet: disabled, real default, the milestone that brings it.
    const rf = screen.getByLabelText('Replication factor');
    expect(rf).toHaveProperty('disabled', true);
    const retention = screen.getByLabelText('Retention');
    expect(retention).toHaveProperty('disabled', true);
    expect(retention).toHaveProperty('value', '604800000 ms (7 days)');
    expect(document.querySelector('[data-field="retentionMs"]')?.textContent).toContain(
      'coming in v0.4',
    );
    expect(document.querySelector('[data-field="replicationFactor"]')?.textContent).toContain(
      'coming in M4',
    );
    // The real Kafka config name is shown.
    expect(document.querySelector('[data-field="retentionMs"] code')?.textContent).toBe(
      'retention.ms',
    );

    expect(screen.getByText(/c1 · lag/)).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Delete topic' })).toBeTruthy();
  });

  it('renders a producer: game-setting chips, acks select, Kafka names', () => {
    renderPanel(ctx.actions);
    selectNode('p1');
    for (const field of ['ratePerSec', 'messageSizeBytes', 'keyStrategy']) {
      const row = document.querySelector(`[data-field="${field}"]`);
      const chip = row?.querySelector('[data-chip="game-setting"]');
      expect(chip?.textContent).toBe('game setting');
      // Flat chip, not a button; focusable for its tooltip.
      expect(chip?.tagName).toBe('SPAN');
      expect(chip?.getAttribute('tabindex')).toBe('0');
    }
    expect(screen.getByLabelText('Acks')).toHaveProperty('value', 'all');
    expect(document.querySelector('[data-field="batchSize"] code')?.textContent).toBe('batch.size');
    expect(screen.getByLabelText('Idempotence')).toHaveProperty('disabled', true);
  });

  it('shows the game-setting tooltip on keyboard focus', async () => {
    renderPanel(ctx.actions);
    selectNode('p1');
    const chip = document.querySelector('[data-field="ratePerSec"] [data-chip="game-setting"]');
    if (!(chip instanceof HTMLElement)) throw new Error('no chip');
    act(() => {
      chip.focus();
    });
    expect((await screen.findByRole('tooltip')).textContent).toContain(
      'Stands in for how often your application calls send()',
    );
  });

  it('renders a consumer with group.id and auto.offset.reset read-only and why', () => {
    renderPanel(ctx.actions);
    selectNode('c1');
    const group = screen.getByLabelText('Group');
    expect(group).toHaveProperty('readOnly', true);
    expect(group).toHaveProperty('value', 'c1-group');
    expect(document.querySelector('[data-field="groupId"]')?.textContent).toContain(
      'Consumer groups arrive in M3',
    );
    expect(screen.getByLabelText('Auto offset reset')).toHaveProperty('readOnly', true);
    expect(screen.getByLabelText('Session timeout')).toHaveProperty('disabled', true);
    // Help and helper lines are wired to the input.
    const describedBy = group.getAttribute('aria-describedby') ?? '';
    expect(describedBy).toContain('pp-groupId-help');
    expect(describedBy).toContain('pp-groupId-helper');
  });

  it('renders a broker with its static rack config read-only', () => {
    renderPanel(ctx.actions);
    selectNode('b1');
    expect(screen.getByLabelText('Rack')).toHaveProperty('readOnly', true);
    expect(document.querySelector('[data-field="rack"]')?.textContent).toContain(
      'static broker config',
    );
  });

  it('shows Zod errors inline and sends nothing while the value is invalid', async () => {
    renderPanel(ctx.actions);
    selectNode('c1');
    const input = screen.getByLabelText('Max poll records');
    fireEvent.change(input, { target: { value: '1.5' } });
    expect(input.getAttribute('aria-invalid')).toBe('true');
    const error = document.getElementById('pp-maxPollRecords-error');
    expect(error?.textContent).toBe('max.poll.records must be a whole number.');
    expect(input.getAttribute('aria-describedby')).toContain('pp-maxPollRecords-error');
    fireEvent.blur(input);
    await flush();
    expect(updates(ctx.dispatch)).toEqual([]);
    expect(config('c1')).toMatchObject({ maxPollRecords: 500 });
  });

  it('saves on blur with only the changed field, and Enter works too', async () => {
    renderPanel(ctx.actions);
    selectNode('c1');
    const wait = screen.getByLabelText('Fetch max wait');
    fireEvent.change(wait, { target: { value: '100' } });
    fireEvent.blur(wait);
    await flush();
    expect(updates(ctx.dispatch)).toEqual([
      { type: 'UpdateConfig', id: 'c1', patch: { fetchMaxWaitMs: 100 } },
    ]);
    expect(config('c1')).toMatchObject({ fetchMaxWaitMs: 100, fetchMinBytes: 1 });
    expect(wait).toHaveProperty('value', '100');

    const minBytes = screen.getByLabelText('Fetch min bytes');
    fireEvent.change(minBytes, { target: { value: '2048' } });
    fireEvent.keyDown(minBytes, { key: 'Enter' });
    await flush();
    expect(updates(ctx.dispatch).at(-1)).toEqual({
      type: 'UpdateConfig',
      id: 'c1',
      patch: { fetchMinBytes: 2048 },
    });

    // An unchanged blur sends nothing; a checkbox saves on change.
    fireEvent.blur(screen.getByLabelText('Max poll records'));
    fireEvent.click(screen.getByLabelText('Auto commit'));
    await flush();
    expect(updates(ctx.dispatch).map((c) => (c.type === 'UpdateConfig' ? c.patch : null))).toEqual([
      { fetchMaxWaitMs: 100 },
      { fetchMinBytes: 2048 },
      { enableAutoCommit: false },
    ]);
  });

  it('Escape puts back the committed value', () => {
    renderPanel(ctx.actions);
    selectNode('p1');
    const rate = screen.getByLabelText('Rate');
    fireEvent.change(rate, { target: { value: '-3' } });
    expect(rate.getAttribute('aria-invalid')).toBe('true');
    fireEvent.keyDown(rate, { key: 'Escape' });
    expect(rate).toHaveProperty('value', '1');
    expect(rate.getAttribute('aria-invalid')).toBe('false');
  });

  it('shows an engine rejection next to the field and keeps what was typed', async () => {
    const original = ctx.host.dispatch.bind(ctx.host);
    ctx.dispatch.mockImplementation((command: Command): DispatchResult =>
      command.type === 'UpdateConfig'
        ? { ok: false, reason: 'The engine said no.' }
        : original(command),
    );
    renderPanel(ctx.actions);
    selectNode('p1');
    const size = screen.getByLabelText('Batch size');
    fireEvent.change(size, { target: { value: '32768' } });
    fireEvent.blur(size);
    await flush();
    const error = document.getElementById('pp-batchSize-error');
    expect(error?.textContent).toBe('Not applied: The engine said no.');
    expect(error?.getAttribute('data-error-source')).toBe('engine');
    expect(size).toHaveProperty('value', '32768');
    expect(config('p1')).toMatchObject({ batchSize: 16384 });
  });

  it('changes the key strategy', async () => {
    renderPanel(ctx.actions);
    selectNode('p1');
    fireEvent.change(screen.getByLabelText('Keys'), { target: { value: 'random' } });
    await flush();
    expect(config('p1')).toMatchObject({ keyStrategy: { type: 'random', keys: ['D', 'E', 'F'] } });
    const keys = screen.getByLabelText('Keys (comma-separated)');
    fireEvent.change(keys, { target: { value: ' , ' } });
    expect(document.getElementById('pp-keyStrategy-error')?.textContent).toBe(
      'Random key strategy needs at least one key.',
    );
    fireEvent.change(keys, { target: { value: 'A, B' } });
    fireEvent.blur(keys);
    await flush();
    expect(updates(ctx.dispatch).at(-1)).toEqual({
      type: 'UpdateConfig',
      id: 'p1',
      patch: { keyStrategy: { type: 'random', keys: ['A', 'B'] } },
    });
  });

  describe('partitions stepper', () => {
    it('minus is disabled at the current count, with the reason', () => {
      renderPanel(ctx.actions);
      selectNode('t1');
      const minus = screen.getByRole('button', { name: 'One partition fewer' });
      expect(minus).toHaveProperty('disabled', true);
      const reason = document.getElementById(minus.getAttribute('aria-describedby') ?? '');
      expect(reason?.textContent).toContain('Kafka never removes partitions');
      expect(screen.getByRole('button', { name: 'Add partitions' })).toHaveProperty(
        'disabled',
        true,
      );
    });

    it('+ then Add sends partitions only; the keyboard works too', async () => {
      renderPanel(ctx.actions);
      selectNode('t1');
      fireEvent.click(screen.getByRole('button', { name: 'One partition more' }));
      const spin = screen.getByRole('spinbutton', { name: 'Partitions' });
      expect(spin).toHaveProperty('value', '4');
      expect(screen.getByRole('button', { name: 'One partition fewer' })).toHaveProperty(
        'disabled',
        false,
      );
      fireEvent.keyDown(spin, { key: 'ArrowUp' });
      fireEvent.keyDown(spin, { key: 'ArrowDown' });
      fireEvent.keyDown(spin, { key: 'ArrowDown' });
      fireEvent.keyDown(spin, { key: 'ArrowDown' }); // stops at the current count
      expect(spin).toHaveProperty('value', '3');
      fireEvent.keyDown(spin, { key: 'ArrowUp' });
      expect(updates(ctx.dispatch)).toEqual([]); // nothing applied yet
      fireEvent.click(screen.getByRole('button', { name: 'Add 1 partition' }));
      await flush();
      expect(updates(ctx.dispatch)).toEqual([
        { type: 'UpdateConfig', id: 't1', patch: { partitions: 4 } },
      ]);
      expect(config('t1')).toMatchObject({ partitions: 4 });
      expect(spin).toHaveProperty('value', '4');
      const tiles = screen.getByRole('group', { name: 'Topic stats' });
      expect(within(tiles).getByText('Partitions').nextSibling?.textContent).toBe('4');
    });

    it('a typed decrease is sent and the KRaft reason is shown', async () => {
      renderPanel(ctx.actions);
      selectNode('t1');
      const spin = screen.getByRole('spinbutton', { name: 'Partitions' });
      fireEvent.change(spin, { target: { value: '2' } });
      fireEvent.keyDown(spin, { key: 'Enter' });
      await flush();
      expect(document.getElementById('pp-partitions-error')?.textContent).toBe(
        'Not applied: The topic t1 currently has 3 partition(s); 2 would not be an increase. Kafka can add partitions to a topic but never remove them.',
      );
      expect(config('t1')).toMatchObject({ partitions: 3 });
    });

    it('shows Zod errors for nonsense', () => {
      renderPanel(ctx.actions);
      selectNode('t1');
      const spin = screen.getByRole('spinbutton', { name: 'Partitions' });
      fireEvent.change(spin, { target: { value: '0' } });
      expect(document.getElementById('pp-partitions-error')?.textContent).toBe(
        'Partitions must be at least 1.',
      );
    });
  });

  it('delete removes the component and clears the selection', async () => {
    renderPanel(ctx.actions);
    selectNode('p1');
    fireEvent.click(screen.getByRole('button', { name: 'Delete producer' }));
    await flush();
    expect(config('p1')).toBeUndefined();
    expect(screen.getByTestId('properties-panel').textContent).toContain(
      'Select a component on the canvas',
    );
  });
});

describe('labels and selection rules', () => {
  afterEach(() => {
    cleanup();
  });

  it('a game-tuned field shows a muted chip and the real default under it (Q5)', () => {
    // No current schema default is game-tuned (all are real), so use a made-up entry.
    const meta: FieldMeta = {
      label: 'Session timeout',
      kafkaName: 'session.timeout.ms',
      kind: 'game-tuned',
      kafkaDefault: { value: 45_000 },
      control: { type: 'int', unit: 'ms' },
      help: 'x',
    };
    expect(helperLine(meta)).toBe('Kafka default: 45000 ms (45 s)');
    render(<KindChip meta={meta} />);
    const chip = screen.getByText('game-tuned');
    expect(chip.tagName).toBe('SPAN');
    expect(chip.getAttribute('data-chip')).toBe('game-tuned');
  });

  it('plain Kafka fields get no chip', () => {
    const { container } = render(<KindChip meta={FIELD_META.consumer.fetchMaxWaitMs} />);
    expect(container.textContent).toBe('');
  });

  it('panelView picks the view for a selection', () => {
    expect(panelView({ nodeIds: [], edgeIds: [] })).toEqual({ type: 'empty' });
    expect(panelView({ nodeIds: ['t1'], edgeIds: [] })).toEqual({ type: 'component', id: 't1' });
    expect(panelView({ nodeIds: [], edgeIds: ['a→b'] })).toEqual({
      type: 'connection',
      edgeId: 'a→b',
    });
    expect(panelView({ nodeIds: ['a', 'b'], edgeIds: [] })).toEqual({ type: 'many', count: 2 });
  });

  it('selecting a component opens Properties, but a topic keeps the Partition log', () => {
    expect(tabOnSelect('partition-log', 'producer')).toBe('properties');
    expect(tabOnSelect('partition-log', 'topic')).toBeNull();
    expect(tabOnSelect('properties', 'topic')).toBeNull();
    expect(tabOnSelect('partition-log', undefined)).toBeNull();
  });
});

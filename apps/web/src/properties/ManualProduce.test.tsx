// @vitest-environment jsdom
import type { Command, EngineEvent } from '@kp/engine';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ActionsProvider } from '../actions/actions-context.tsx';
import { createActions, type Actions } from '../actions/actions.ts';
import { SimHost } from '../sim/sim-host.ts';
import { applySimFrame, resetSimStore, useSimStore } from '../stores/sim-store.ts';
import { resetTopologyStore } from '../stores/topology-store.ts';
import { resetUiStore, select } from '../stores/ui-store.ts';
import { findDeliveries, MAX_KEY_LENGTH, parseManualRecord } from './ManualProduce.tsx';
import { PropertiesPanel } from './PropertiesPanel.tsx';

describe('parseManualRecord', () => {
  it('empty key = null key; the key is kept exactly as typed', () => {
    expect(parseManualRecord('', 'v')).toEqual({ ok: true, value: { key: null, value: 'v' } });
    expect(parseManualRecord(' D', 'v')).toEqual({ ok: true, value: { key: ' D', value: 'v' } });
    expect(parseManualRecord('D', '')).toEqual({ ok: true, value: { key: 'D', value: '' } });
  });

  it('rejects over-long keys and values', () => {
    expect(parseManualRecord('k'.repeat(MAX_KEY_LENGTH + 1), 'v').ok).toBe(false);
    expect(parseManualRecord('k', 'v'.repeat(1000)).ok).toBe(false);
  });
});

describe('findDeliveries', () => {
  const produced = (seq: number, topic: string, partition: number, key: string | null) =>
    ({
      t: seq,
      seq,
      type: 'RecordProduced',
      producerId: 'p1',
      topic,
      partition,
      key,
      value: 'v',
      traceId: `p1@1#${String(seq)}`,
      acks: 'all',
    }) as const satisfies EngineEvent;
  const appended = (seq: number, trace: number, topic: string, offset: number) =>
    ({
      t: seq,
      seq,
      type: 'RecordAppended',
      topic,
      partition: 0,
      offset,
      key: 'D',
      value: 'v',
      producerId: 'p1',
      traceId: `p1@1#${String(trace)}`,
    }) as const satisfies EngineEvent;

  it('matches the sent record per topic, then its offset by trace id', () => {
    const events: EngineEvent[] = [
      produced(1, 't1', 2, 'D'), // before the send: ignored
      produced(5, 't1', 0, 'D'),
      produced(6, 't2', 1, 'D'),
      produced(7, 't1', 1, 'E'), // other key
      appended(8, 5, 't1', 4),
    ];
    expect(findDeliveries(events, 'p1', { key: 'D', value: 'v' }, 4)).toEqual([
      { topic: 't1', partition: 0, offset: 4 },
      { topic: 't2', partition: 1, offset: null },
    ]);
    expect(findDeliveries(events, 'p2', { key: 'D', value: 'v' }, 4)).toEqual([]);
  });
});

/** A real in-process SimHost (no worker), paused; p1 → t1 (3 partitions) → c1, and p2 alone. */
async function setup(partitions = 3) {
  resetTopologyStore();
  resetSimStore();
  resetUiStore();
  const host = new SimHost({ now: () => 0, scheduleTick: () => () => undefined });
  host.subscribe((frame) => {
    applySimFrame(frame);
  });
  const actions = createActions({ sim: host, seed: 7 });
  expect(await actions.init()).toEqual({ ok: true });
  await actions.addComponent('topic', { x: 300, y: 100 }, { partitions });
  await actions.addComponent('producer', { x: 50, y: 100 });
  await actions.addComponent('producer', { x: 50, y: 300 });
  await actions.connect('p1', 't1');
  const dispatch = vi.spyOn(host, 'dispatch');
  return { host, actions, dispatch };
}

function renderFor(actions: Actions, producerId: string) {
  const view = render(
    <ActionsProvider actions={actions}>
      <PropertiesPanel />
    </ActionsProvider>,
  );
  act(() => {
    select({ nodeIds: [producerId], edgeIds: [] });
  });
  return view;
}

const flush = () => act(async () => Promise.resolve());
const produced = (dispatch: { mock: { calls: unknown[][] } }): Command[] =>
  dispatch.mock.calls
    .map((call) => call[0] as Command)
    .filter((command) => command.type === 'ProduceManual');

describe('<ManualProduce>', () => {
  afterEach(() => {
    cleanup();
  });

  describe('with a connected producer', () => {
    let ctx: Awaited<ReturnType<typeof setup>>;
    beforeEach(async () => {
      ctx = await setup();
    });

    it('sends key D: the partitioner picks t1-0, then the leader appends it', async () => {
      renderFor(ctx.actions, 'p1');
      fireEvent.change(screen.getByLabelText(/^Record key/), { target: { value: 'D' } });
      expect(screen.getByText('Same key, same partition: murmur2(key) % partitions.')).toBeTruthy();
      fireEvent.change(screen.getByLabelText('Record value'), { target: { value: 'order-1' } });
      fireEvent.click(screen.getByRole('button', { name: 'Send record' }));
      await flush();
      expect(produced(ctx.dispatch)).toEqual([
        { type: 'ProduceManual', producerId: 'p1', key: 'D', value: 'order-1' },
      ]);
      const deliveries = screen.getByTestId('mp-deliveries');
      expect(deliveries.textContent).toBe(
        'Sent (key D) → t1-0: on its way to the leader (press Play or Step if paused).',
      );
      act(() => {
        ctx.host.stepBy(20);
      });
      // p1 also produces on its own (1/s), so the offset is whatever the leader gave it.
      const offset = Number(/, offset (\d+)\.$/.exec(deliveries.textContent)?.[1]);
      const log = useSimStore.getState().snapshot?.topics[0]?.partitions[0]?.records ?? [];
      expect(log.find((r) => r.offset === offset)).toMatchObject({ key: 'D', value: 'order-1' });
    });

    it('an empty key sends a null key and explains the sticky partitioner', async () => {
      renderFor(ctx.actions, 'p1');
      expect(screen.getByLabelText(/^Record key/).getAttribute('placeholder')).toBe(
        'empty = null key',
      );
      expect(screen.getByText(/No key: the sticky partitioner picks a partition/)).toBeTruthy();
      fireEvent.click(screen.getByRole('button', { name: 'Send record' }));
      await flush();
      expect(produced(ctx.dispatch)).toEqual([
        { type: 'ProduceManual', producerId: 'p1', key: null, value: 'hello' },
      ]);
      expect(screen.getByTestId('mp-deliveries').textContent).toMatch(/^Sent \(null key\) → t1-\d/);
    });

    it('shows a validation error and sends nothing', async () => {
      renderFor(ctx.actions, 'p1');
      const key = screen.getByLabelText(/^Record key/);
      fireEvent.change(key, { target: { value: 'k'.repeat(MAX_KEY_LENGTH + 1) } });
      fireEvent.click(screen.getByRole('button', { name: 'Send record' }));
      await flush();
      expect(produced(ctx.dispatch)).toEqual([]);
      expect(
        screen.getByText(`Keep the key to ${String(MAX_KEY_LENGTH)} characters.`),
      ).toBeTruthy();
      expect(key.getAttribute('aria-invalid')).toBe('true');
    });

    it('a producer without a topic: the button is disabled with the reason', () => {
      renderFor(ctx.actions, 'p2');
      const button = screen.getByRole('button', { name: 'Send record' });
      expect(button).toHaveProperty('disabled', true);
      expect(screen.getByText('Connect this producer to a topic first.')).toBeTruthy();
    });
  });

  it('shows the engine rejection', async () => {
    const ctx = await setup();
    const actions: Actions = {
      ...ctx.actions,
      produceManual: () => Promise.resolve({ ok: false, reason: 'Producer p1 is not connected.' }),
    };
    renderFor(actions, 'p1');
    fireEvent.click(screen.getByRole('button', { name: 'Send record' }));
    await flush();
    expect(screen.getByText('Not sent: Producer p1 is not connected.')).toBeTruthy();
  });
});

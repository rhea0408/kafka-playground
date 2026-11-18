// @vitest-environment jsdom
import type { BrokerSnapshot, EngineSnapshot, PartitionSnapshot } from '@kp/engine';
import { act, cleanup, render, screen } from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ActionsProvider } from '../../actions/actions-context.tsx';
import type { Actions } from '../../actions/actions.ts';
import { applySimFrame, resetSimStore } from '../../stores/sim-store.ts';
import { resetUiStore } from '../../stores/ui-store.ts';
import { fixtureSnapshot, partition } from '../../test/snapshot-fixture.ts';
import { BrokerLive, ControllerChip, isDown } from './broker-parts.tsx';
import { PartitionLive, PartitionStatus } from './live-parts.tsx';

let frame = 0;
function show(snapshot: EngineSnapshot): void {
  frame += 1;
  act(() => {
    applySimFrame({
      frame,
      t: snapshot.t,
      playing: true,
      speed: 1,
      events: [],
      kind: 'full',
      snapshot,
      history: [],
    });
  });
}

function Providers({ children }: { children: ReactNode }) {
  const ok = { ok: true } as const;
  const actions = {
    killBroker: vi.fn(() => Promise.resolve(ok)),
    stopBroker: vi.fn(() => Promise.resolve(ok)),
    restartBroker: vi.fn(() => Promise.resolve(ok)),
  } as unknown as Actions;
  return (
    <ActionsProvider actions={actions}>
      <ReactFlowProvider>{children}</ReactFlowProvider>
    </ActionsProvider>
  );
}

const base = fixtureSnapshot();
const brokerWith = (patch: Partial<BrokerSnapshot>): EngineSnapshot => ({
  ...base,
  brokers: [{ ...(base.brokers[0] as BrokerSnapshot), ...patch }],
});

beforeEach(() => {
  resetSimStore();
  resetUiStore();
});
afterEach(cleanup);

describe('<BrokerLive>', () => {
  it.each([
    ['running', '●', 'Running'],
    ['fenced', '⊘', 'Fenced'],
    ['shutting-down', '◐', 'Shutting down'],
    ['stopped', '■', 'Stopped'],
    ['crashed', '✕', 'Crashed'],
  ] as const)('shows %s with an icon and a word, never color alone', (state, icon, word) => {
    show(brokerWith({ state }));
    render(<BrokerLive brokerId="b1" />, { wrapper: Providers });
    const chip = document.querySelector('[data-slot="broker-state"]');
    expect(chip?.textContent).toBe(`${icon}${word}`);
    expect(chip?.getAttribute('title')).toBeTruthy();
  });

  it('shows leads / follows and the groups it coordinates', () => {
    show(brokerWith({ partitionsLed: 2, partitionsFollowed: 4, coordinatorFor: ['orders-app'] }));
    render(<BrokerLive brokerId="b1" />, { wrapper: Providers });
    expect(screen.getByText('Leads 2 · follows 4')).toBeTruthy();
    expect(screen.getByText('Coordinator: orders-app')).toBeTruthy();
  });

  it('counts crashed and stopped brokers as down (A6)', () => {
    expect(isDown('crashed')).toBe(true);
    expect(isDown('stopped')).toBe(true);
    expect(isDown('fenced')).toBe(false);
  });
});

describe('<ControllerChip>', () => {
  it('is hidden in a single-broker Sandbox', () => {
    show(base);
    render(<ControllerChip />, { wrapper: Providers });
    expect(screen.queryByTestId('controller-chip')).toBeNull();
  });

  it('shows fenced brokers, offline and under-replicated counts', () => {
    const b1 = base.brokers[0] as BrokerSnapshot;
    show({
      ...base,
      brokers: [
        b1,
        { ...b1, config: { ...b1.config, id: 'b2' }, state: 'crashed', status: 'crashed' },
      ],
      controller: {
        ...base.controller,
        fencedBrokers: ['b2'],
        offlinePartitions: 1,
        underReplicatedPartitions: 3,
      },
    });
    render(<ControllerChip />, { wrapper: Providers });
    expect(screen.getByText('Fenced: b2')).toBeTruthy();
    expect(screen.getByText('⚠ 1 offline')).toBeTruthy();
    expect(screen.getByText('3 under-replicated')).toBeTruthy();
  });
});

describe('replica chips on a partition row (M4-T11)', () => {
  const replicated = (patch: Partial<PartitionSnapshot>): EngineSnapshot => {
    const p = partition(0, 5);
    const topic = base.topics[0];
    if (topic === undefined) throw new Error('fixture');
    return {
      ...base,
      topics: [
        {
          ...topic,
          config: { ...topic.config, replicationFactor: 3, minInsyncReplicas: 2 },
          partitions: [
            {
              ...p,
              replicas: ['b1', 'b2', 'b3'].map((broker) => ({
                broker,
                logEndOffset: 5,
                highWatermark: 5,
              })),
              isr: ['b1', 'b2', 'b3'],
              ...patch,
            },
          ],
        },
      ],
    };
  };

  it('marks a replica out of the ISR with a dashed chip and the word "out" (A9)', () => {
    show(replicated({ isr: ['b1', 'b3'] }));
    render(<PartitionLive topicId="t1" index={0} />, { wrapper: Providers });
    const chips = document.querySelectorAll('[data-slot="replicas"] > span');
    expect([...chips].map((c) => c.textContent)).toEqual(['b1', 'b2·out', 'b3']);
    expect(chips[1]?.className).toContain('kp-out-of-isr');
  });

  it('shows an offline partition and acknowledged records lost to truncation (A8)', () => {
    show(
      replicated({ leader: null, lostAcked: 3, isr: [], underReplicated: true, underMinIsr: true }),
    );
    render(
      <>
        <PartitionLive topicId="t1" index={0} />
        <PartitionStatus topicId="t1" index={0} />
      </>,
      { wrapper: Providers },
    );
    expect(document.querySelector('[data-slot="leader"]')?.textContent).toBe('⚠ offline');
    expect(document.querySelector('[data-slot="lost"]')?.textContent).toBe('⚠ 3 lost');
    // "⚠ offline" already says it all: no ISR chip on top (G-3).
    expect(document.querySelector('[data-slot="isr-state"]')).toBeNull();
  });

  // M4 follow-up G-3: the status line under a replicated row.
  const status = () => document.querySelector<HTMLElement>('[data-slot="isr-state"]');

  it('shows nothing on the status line while every replica is in sync', () => {
    show(replicated({}));
    render(<PartitionStatus topicId="t1" index={0} />, { wrapper: Providers });
    expect(status()).toBeNull();
  });

  it('marks an under-replicated partition with a muted chip: "n of m replicas are in sync"', () => {
    show(replicated({ isr: ['b1', 'b2'], underReplicated: true }));
    render(<PartitionStatus topicId="t1" index={0} />, { wrapper: Providers });
    expect(status()?.textContent).toBe('under-replicated');
    expect(status()?.dataset['state']).toBe('under-replicated');
    expect(status()?.title).toBe('2 of 3 replicas are in sync.');
    expect(status()?.className).toContain('bg-muted');
  });

  it('marks a partition under min ISR as "⚠ under min ISR · HW frozen", with icon and words', () => {
    show(replicated({ isr: ['b1'], underReplicated: true, underMinIsr: true }));
    render(<PartitionStatus topicId="t1" index={0} />, { wrapper: Providers });
    expect(status()?.textContent).toBe('⚠ under min ISR · HW frozen');
    expect(status()?.dataset['state']).toBe('under-min-isr');
    expect(status()?.title).toContain(
      'The in-sync replicas (1) are fewer than min.insync.replicas (2).',
    );
    expect(status()?.title).toContain('even ones written with acks=1');
    expect(status()?.title).toContain('NOT_ENOUGH_REPLICAS');
  });

  it('keeps the M3 look with one replica (no chips)', () => {
    show(base);
    render(<PartitionLive topicId="t1" index={0} />, { wrapper: Providers });
    expect(document.querySelector('[data-slot="replicas"]')).toBeNull();
    expect(document.querySelector('[data-slot="leader"]')?.textContent).toBe('L b1');
    // RF 1 can't be under-replicated: even a stale flag shows no chip (G-3).
    const [topic] = base.topics;
    const [p0] = topic?.partitions ?? [];
    if (topic === undefined || p0 === undefined) throw new Error('fixture');
    show({
      ...base,
      topics: [{ ...topic, partitions: [{ ...p0, underReplicated: true, underMinIsr: true }] }],
    });
    render(<PartitionStatus topicId="t1" index={0} />, { wrapper: Providers });
    expect(status()).toBeNull();
  });
});

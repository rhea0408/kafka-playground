// @vitest-environment jsdom
import type { EngineSnapshot, GroupSnapshot, PartitionSnapshot } from '@kp/engine';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { resetSimStore, useSimStore } from '../stores/sim-store.ts';
import { resetUiStore, setLogTarget, useUiStore } from '../stores/ui-store.ts';
import {
  consumer,
  consumerPartition,
  fixtureSnapshot,
  partition,
} from '../test/snapshot-fixture.ts';
import { LOG_PAGE, PartitionLog } from './PartitionLog.tsx';

function snapshotWith(p0: PartitionSnapshot, groups: GroupSnapshot[] = []): EngineSnapshot {
  const base = fixtureSnapshot();
  const topic = base.topics[0];
  if (topic === undefined) throw new Error('fixture without a topic');
  return {
    ...base,
    topics: [{ ...topic, partitions: [p0, partition(1, 3, { keys: ['C'] })] }],
    consumers: [
      consumer('c1', [
        consumerPartition(0, p0.highWatermark, p0.highWatermark, null),
        consumerPartition(1, 3, 3, 3),
      ]),
    ],
    groups,
  };
}

const committed = (offset: number): GroupSnapshot[] => [
  { groupId: 'c1-group', committed: [{ topic: 't1', partition: 0, offset }] },
];

function show(snapshot: EngineSnapshot): void {
  act(() => {
    useSimStore.setState({ snapshot });
  });
}

/** The table body in order: record offsets and markers, e.g. "0 1 C@2 2 HW@3 LEO@3". */
function picture(): string {
  const body = screen.getByRole('table').querySelector('tbody');
  return [...(body?.querySelectorAll('tr') ?? [])]
    .map((tr) => {
      const marker = tr.getAttribute('data-marker');
      if (marker === null) {
        const offset = tr.getAttribute('data-offset') ?? '?';
        return tr.textContent.includes('leader only') ? `${offset}*` : offset;
      }
      const n = /(\d[\d,]*)/.exec(tr.textContent.replace(/^c1-group/, ''))?.[1] ?? '?';
      const names: Readonly<Record<string, string>> = { hw: 'HW', committed: 'C', leo: 'LEO' };
      return `${names[marker] ?? marker}@${n}`;
    })
    .join(' ');
}

describe('<PartitionLog>', () => {
  beforeEach(() => {
    resetSimStore();
    resetUiStore();
  });
  afterEach(() => {
    cleanup();
  });

  it('asks for a topic when there is none', () => {
    render(<PartitionLog />);
    expect(screen.getByTestId('partition-log').textContent).toContain('Add a topic');
  });

  it('committed in the middle: the line sits just above the next record to consume', () => {
    show(snapshotWith(partition(0, 5), committed(3)));
    render(<PartitionLog />);
    expect(picture()).toBe('0 1 2 C@3 3 4 HW@5 LEO@5');
    const line = screen.getByRole('table').querySelector('[data-marker="committed"]');
    expect(line?.textContent).toContain('c1-group committed 3');
    expect(line?.textContent).toContain('next read is 3');
  });

  it('committed = LEO: everything consumed, the line is at the end', () => {
    show(snapshotWith(partition(0, 4), committed(4)));
    render(<PartitionLog />);
    expect(picture()).toBe('0 1 2 3 C@4 HW@4 LEO@4');
  });

  it('committed = 0: the line is above offset 0', () => {
    show(snapshotWith(partition(0, 3), committed(0)));
    render(<PartitionLog />);
    expect(picture()).toBe('C@0 0 1 2 HW@3 LEO@3');
  });

  it('no committed offset: no line, and the summary says so', () => {
    show(snapshotWith(partition(0, 3)));
    render(<PartitionLog />);
    expect(picture()).toBe('0 1 2 HW@3 LEO@3');
    const groups = screen.getByTestId('pl-groups');
    expect(groups.textContent).toContain('c1-group committed - (no commit yet)');
    expect(groups.textContent).toContain('c1 position 3');
  });

  it('HW below LEO (M4 replication): records past the HW line are on the leader only', () => {
    show(snapshotWith(partition(0, 6, { hw: 4 }), committed(2)));
    render(<PartitionLog />);
    expect(picture()).toBe('0 1 C@2 2 3 HW@4 4* 5* LEO@6');
  });

  it('summary: leader, offsets with explanations, groups with lag', () => {
    show(snapshotWith(partition(0, 12), committed(10)));
    render(<PartitionLog />);
    const offsets = screen.getByRole('group', { name: 'Offsets' });
    expect(within(offsets).getByLabelText(/^Log end \(LEO\) 12\./)).toBeTruthy();
    expect(within(offsets).getByLabelText(/^High watermark 12\./)).toBeTruthy();
    expect(within(offsets).getByLabelText(/^Log start 0\./)).toBeTruthy();
    expect(screen.getByTestId('pl-groups').textContent).toContain('c1-group committed 10 · lag 2');
    expect(screen.getByTestId('partition-log').textContent).toContain('Leader b1');
    // Marker explanations are focusable, with the plain-language text as their name.
    expect(
      screen.getByLabelText(
        /^Group c1-group committed offset 10: the next record the group will consume/,
      ),
    ).toBeTruthy();
  });

  it('the picker switches partitions', () => {
    show(snapshotWith(partition(0, 5), committed(3)));
    render(<PartitionLog />);
    fireEvent.change(screen.getByLabelText('Partition'), { target: { value: '1' } });
    expect(useUiStore.getState().logTarget).toEqual({ topicId: 't1', partition: 1 });
    expect(picture()).toBe('0 1 2 HW@3 LEO@3');
    expect(screen.getByRole('region', { name: 'Records of t1-1' })).toBeTruthy();
  });

  it('a chosen partition that no longer exists falls back to partition 0', () => {
    show(snapshotWith(partition(0, 2)));
    act(() => {
      setLogTarget({ topicId: 't1', partition: 9 });
    });
    render(<PartitionLog />);
    expect(screen.getByRole('region', { name: 'Records of t1-0' })).toBeTruthy();
  });

  it('long logs: the newest page, then "Show older"; offsets beyond the snapshot are named', () => {
    show(snapshotWith(partition(0, 400, { window: 200 }), committed(390)));
    render(<PartitionLog />);
    const records = () => screen.getByRole('table').querySelectorAll('tr[data-offset]');
    expect(records()).toHaveLength(LOG_PAGE);
    expect(records()[0]?.getAttribute('data-offset')).toBe(String(400 - LOG_PAGE));
    for (let i = 0; i < 3; i++) {
      fireEvent.click(screen.getByRole('button', { name: /^Show 50 older records$/ }));
    }
    expect(records()).toHaveLength(200);
    expect(screen.queryByRole('button', { name: /older records/ })).toBeNull();
    expect(screen.getByTestId('partition-log').textContent).toContain(
      'Offsets 0–199 are older than the 200 records this panel keeps per partition.',
    );
  });

  it('the key filter keeps only matching records; markers stay', () => {
    show(snapshotWith(partition(0, 6, { keys: ['A', 'B'] }), committed(2)));
    render(<PartitionLog />);
    fireEvent.change(screen.getByLabelText('Filter key'), { target: { value: 'B' } });
    expect(picture()).toBe('1 C@2 3 5 HW@6 LEO@6');
  });

  it('has a Follow toggle', () => {
    show(snapshotWith(partition(0, 3)));
    render(<PartitionLog />);
    const follow = screen.getByRole('button', { name: 'Follow' });
    expect(follow.getAttribute('aria-pressed')).toBe('true');
    fireEvent.click(follow);
    expect(follow.getAttribute('aria-pressed')).toBe('false');
  });
});

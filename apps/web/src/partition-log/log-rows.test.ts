import type { LogRecord } from '@kp/engine';
import { describe, expect, it } from 'vitest';
import { record } from '../test/snapshot-fixture.ts';
import { buildLogRows, type LogRow, type LogRowsInput } from './log-rows.ts';

const log = (from: number, to: number, keys: readonly (string | null)[] = ['A']): LogRecord[] =>
  Array.from({ length: to - from }, (_, i) =>
    record(from + i, keys[(from + i) % keys.length] ?? null),
  );

/** Compact picture of the rows: "0 1 C:g1@2 2 HW@3 LEO@3". */
function picture(rows: readonly LogRow[]): string {
  return rows
    .map((r) => {
      switch (r.kind) {
        case 'record':
          return r.visible ? String(r.record.offset) : `${String(r.record.offset)}*`;
        case 'hw':
          return `HW@${String(r.offset)}${r.beforeShown ? '^' : ''}`;
        case 'committed':
          return `C:${r.groupId}@${String(r.offset)}${r.beforeShown ? '^' : ''}`;
        case 'leo':
          return `LEO@${String(r.offset)}`;
      }
    })
    .join(' ');
}

function build(overrides: Partial<LogRowsInput> & Pick<LogRowsInput, 'records'>) {
  const leo =
    overrides.records.at(-1) === undefined ? 0 : (overrides.records.at(-1)?.offset ?? 0) + 1;
  return buildLogRows({
    logStartOffset: 0,
    highWatermark: leo,
    logEndOffset: leo,
    committed: [],
    limit: 50,
    ...overrides,
  });
}

describe('buildLogRows: marker placement', () => {
  it('puts the HW line after record HW−1 and the committed line before the committed offset', () => {
    const { rows } = build({ records: log(0, 5), committed: [{ groupId: 'g1', offset: 3 }] });
    expect(picture(rows)).toBe('0 1 2 C:g1@3 3 4 HW@5 LEO@5');
  });

  it('committed = LEO: the group has read everything; its line sits at the end', () => {
    const { rows } = build({ records: log(0, 3), committed: [{ groupId: 'g1', offset: 3 }] });
    expect(picture(rows)).toBe('0 1 2 C:g1@3 HW@3 LEO@3');
  });

  it('committed = 0: nothing consumed yet; its line sits above offset 0', () => {
    const { rows } = build({ records: log(0, 3), committed: [{ groupId: 'g1', offset: 0 }] });
    expect(picture(rows)).toBe('C:g1@0 0 1 2 HW@3 LEO@3');
  });

  it('no committed offset: no committed line', () => {
    const { rows } = build({ records: log(0, 3) });
    expect(picture(rows)).toBe('0 1 2 HW@3 LEO@3');
  });

  it('HW below the LEO: records at HW and above are on the leader only (M4)', () => {
    const { rows } = build({
      records: log(0, 5),
      highWatermark: 3,
      logEndOffset: 5,
      committed: [{ groupId: 'g1', offset: 2 }],
    });
    expect(picture(rows)).toBe('0 1 C:g1@2 2 HW@3 3* 4* LEO@5');
  });

  it('committed = HW: committed comes first, then the HW line', () => {
    const { rows } = build({
      records: log(0, 5),
      highWatermark: 3,
      logEndOffset: 5,
      committed: [{ groupId: 'g1', offset: 3 }],
    });
    expect(picture(rows)).toBe('0 1 2 C:g1@3 HW@3 3* 4* LEO@5');
  });

  it('several groups are sorted by offset, then group id', () => {
    const { rows } = build({
      records: log(0, 4),
      committed: [
        { groupId: 'zeta', offset: 1 },
        { groupId: 'beta', offset: 3 },
        { groupId: 'alpha', offset: 1 },
      ],
    });
    expect(picture(rows)).toBe('0 C:alpha@1 C:zeta@1 1 2 C:beta@3 3 HW@4 LEO@4');
  });

  it('an empty partition: committed 0, HW 0 and LEO 0 together', () => {
    const { rows } = build({ records: [], committed: [{ groupId: 'g1', offset: 0 }] });
    expect(picture(rows)).toBe('C:g1@0 HW@0 LEO@0');
  });
});

describe('buildLogRows: windowing and filter', () => {
  it('shows the newest `limit` records and says how many older ones exist', () => {
    const result = build({
      records: log(0, 10),
      limit: 4,
      committed: [{ groupId: 'g1', offset: 2 }],
    });
    expect(picture(result.rows)).toBe('C:g1@2^ 6 7 8 9 HW@10 LEO@10');
    expect(result.olderAvailable).toBe(6);
    expect(result.notLoaded).toBeNull();
  });

  it('reports offsets older than the snapshot window', () => {
    const result = build({ records: log(150, 160), logStartOffset: 0 });
    expect(result.notLoaded).toEqual({ from: 0, to: 150 });
    expect(build({ records: log(0, 3) }).notLoaded).toBeNull();
  });

  it('a key filter keeps markers in place, before the next shown record', () => {
    const { rows } = build({
      records: log(0, 6, ['A', 'B']),
      keyFilter: 'B',
      committed: [{ groupId: 'g1', offset: 2 }],
    });
    // Shown: 1, 3, 5 (key B). Committed 2 sits before 3.
    expect(picture(rows)).toBe('1 C:g1@2 3 5 HW@6 LEO@6');
  });
});

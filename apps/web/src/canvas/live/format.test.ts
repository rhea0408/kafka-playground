import { describe, expect, it } from 'vitest';
import { consumer, consumerPartition, record } from '../../test/snapshot-fixture.ts';
import {
  compactCount,
  formatCount,
  keyLetter,
  LOG_CELLS,
  summarizeConsumer,
  visibleCells,
} from './format.ts';

describe('formatCount / compactCount', () => {
  it('groups thousands with commas', () => {
    expect(formatCount(0)).toBe('0');
    expect(formatCount(1204)).toBe('1,204');
    expect(formatCount(1234567)).toBe('1,234,567');
  });

  it('shortens big numbers without rounding up', () => {
    expect(compactCount(9999)).toBe('9,999');
    expect(compactCount(12_399)).toBe('12.3k');
    expect(compactCount(123_999)).toBe('123k');
    expect(compactCount(1_299_999)).toBe('1.2M');
  });
});

describe('keyLetter', () => {
  it('is the first character of the key, or ∅ for a null key', () => {
    expect(keyLetter('A')).toBe('A');
    expect(keyLetter('orderId')).toBe('o');
    expect(keyLetter('😀x')).toBe('😀');
    expect(keyLetter(null)).toBe('∅');
  });
});

describe('visibleCells', () => {
  it('keeps the last N records and marks those at or above HW as not committed', () => {
    const records = [0, 1, 2, 3, 4, 5, 6, 7].map((o) => record(o, 'A'));
    const cells = visibleCells(records, 6, LOG_CELLS);
    expect(cells.map((c) => c.record.offset)).toEqual([2, 3, 4, 5, 6, 7]);
    expect(cells.map((c) => c.committed)).toEqual([true, true, true, true, false, false]);
  });
});

describe('summarizeConsumer', () => {
  it('sums positions, committed offsets and lag over partitions, with a per-partition breakdown', () => {
    const s = summarizeConsumer(
      consumer('c1', [consumerPartition(0, 120, 118, 100), consumerPartition(1, 50, 50, 50)]),
    );
    expect(s).toMatchObject({ partitions: 2, position: 168, committed: 150, lag: 20 });
    expect(s.breakdown).toEqual([
      'P0: position 118 · committed 100 · end 120 · lag 20',
      'P1: position 50 · committed 50 · end 50 · lag 0',
    ]);
  });

  it('leaves unknown values out, like kafka-consumer-groups.sh prints "-"', () => {
    // P1 is not known to the consumer yet (metadata refresh): position and committed are null.
    const s = summarizeConsumer(
      consumer('c1', [consumerPartition(0, 10, 7, 5), consumerPartition(1, 4, null, null)]),
    );
    expect(s).toMatchObject({ position: 7, committed: 5, lag: 5 });
    expect(s.breakdown[1]).toBe('P1: position - · committed - · end 4 · lag -');
  });

  it('is null throughout before anything is known', () => {
    const s = summarizeConsumer(consumer('c1', [consumerPartition(0, 10, null, null)]));
    expect(s).toMatchObject({ position: null, committed: null, lag: null });
  });

  it('names the topic in the breakdown when the consumer reads several', () => {
    const s = summarizeConsumer(
      consumer('c1', [consumerPartition(0, 1, 1, 1, 'a'), consumerPartition(0, 2, 2, 2, 'b')]),
    );
    expect(s.breakdown.map((l) => l.split(':')[0])).toEqual(['a P0', 'b P0']);
  });
});

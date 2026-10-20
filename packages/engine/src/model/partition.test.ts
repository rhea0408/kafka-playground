import { describe, expect, it } from 'vitest';
import { appendRecord, createPartition, partitionName, readRecords } from './partition.ts';

const rec = (n: number, key: string | null = null) => ({
  key,
  value: `v${String(n)}`,
  timestamp: n * 100,
  producerId: 'p1',
  traceId: `p1#${String(n)}`,
});

describe('partition log', () => {
  it('starts empty with every offset at 0', () => {
    expect(createPartition('orders', 0, 'b1')).toMatchObject({
      log: [],
      logStartOffset: 0,
      logEndOffset: 0,
      highWatermark: 0,
    });
  });

  it('assigns contiguous offsets from 0 and returns the assigned offset', () => {
    const p = createPartition('orders', 0, 'b1');
    const offsets = [0, 1, 2, 3, 4].map((n) => appendRecord(p, rec(n)).offset);
    expect(offsets).toEqual([0, 1, 2, 3, 4]);
    expect(p.log.map((r) => r.offset)).toEqual([0, 1, 2, 3, 4]);
  });

  it('keeps LEO = last offset + 1, and HW = LEO with a single replica', () => {
    const p = createPartition('orders', 0, 'b1');
    appendRecord(p, rec(0));
    appendRecord(p, rec(1));
    expect(p.logEndOffset).toBe(2);
    expect(p.highWatermark).toBe(2);
  });

  it('keeps the record fields and adds the offset', () => {
    const p = createPartition('orders', 1, 'b1');
    expect(appendRecord(p, rec(7, 'A'))).toEqual({ offset: 0, ...rec(7, 'A') });
  });

  it('reads from an offset, at most max records, never at or above the HW', () => {
    const p = createPartition('orders', 0, 'b1');
    for (let n = 0; n < 5; n++) appendRecord(p, rec(n));
    expect(readRecords(p, 1, 2).map((r) => r.offset)).toEqual([1, 2]);
    expect(readRecords(p, 3, 100).map((r) => r.offset)).toEqual([3, 4]);
    expect(readRecords(p, 5, 100)).toEqual([]);
    p.highWatermark = 3; // as if offsets 3-4 were not yet replicated (M4)
    expect(readRecords(p, 2, 100).map((r) => r.offset)).toEqual([2]);
  });

  it('names partitions like Kafka tools do', () => {
    expect(partitionName('orders', 2)).toBe('orders-2');
  });
});

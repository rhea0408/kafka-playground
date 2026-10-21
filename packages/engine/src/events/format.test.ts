import { describe, expect, it } from 'vitest';
import type { EngineEvent } from './index.ts';
import { formatEvent, formatSimTime } from './format.ts';

describe('formatSimTime', () => {
  it.each([
    [0, '00:00.000'],
    [1500, '00:01.500'],
    [61_007, '01:01.007'],
  ])('%i ms → %s', (t, text) => {
    expect(formatSimTime(t)).toBe(text);
  });
});

describe('formatEvent', () => {
  it('formats records like the milestone example', () => {
    const e: EngineEvent = {
      t: 1500,
      seq: 3,
      type: 'RecordAppended',
      topic: 'orders',
      partition: 1,
      offset: 2,
      key: 'B',
      value: 'p1-3',
      producerId: 'p1',
      traceId: 'p1@1#3',
    };
    expect(formatEvent(e)).toBe('00:01.500  RecordAppended   orders-1  offset=2  key=B');
  });

  it('shows acks=0 acknowledgements with offset -1, as Kafka reports them', () => {
    const e: EngineEvent = {
      t: 500,
      seq: 1,
      type: 'ProduceAcked',
      producerId: 'p1',
      topic: 'orders',
      partition: 0,
      traceId: 'p1@1#1',
      offset: null,
    };
    expect(formatEvent(e)).toBe(
      '00:00.500  ProduceAcked     p1  orders-0  offset=-1 (acks=0: not known)',
    );
  });

  it('formats consumption and commits', () => {
    expect(
      formatEvent({
        t: 5000,
        seq: 9,
        type: 'OffsetCommitted',
        groupId: 'g',
        consumerId: 'c1',
        topic: 'orders',
        partition: 2,
        offset: 4,
      }),
    ).toBe('00:05.000  OffsetCommitted  g  orders-2  offset=4');
  });

  it('lists the fields a ConfigUpdated event changed', () => {
    const before = {
      kind: 'topic',
      id: 'orders',
      partitions: 3,
      replicationFactor: 1,
      minInsyncReplicas: 1,
    } as const;
    expect(
      formatEvent({
        t: 2000,
        seq: 4,
        type: 'ConfigUpdated',
        id: 'orders',
        before,
        after: { ...before, partitions: 4 },
      }),
    ).toBe('00:02.000  ConfigUpdated    topic orders  partitions: 3 → 4');
  });
});

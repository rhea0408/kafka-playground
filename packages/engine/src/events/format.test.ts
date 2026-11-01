import { describe, expect, it } from 'vitest';
import type { EngineEvent } from './index.ts';
import { describeEvent, formatEvent, formatSimTime } from './format.ts';

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
        memberId: null,
        generation: -1,
        topic: 'orders',
        partition: 2,
        offset: 4,
      }),
    ).toBe('00:05.000  OffsetCommitted  g  orders-2  offset=4');
  });

  it('formats group membership and rebalances', () => {
    const at = { t: 3000, seq: 1 } as const;
    expect(
      formatEvent({
        ...at,
        type: 'RebalanceStarted',
        groupId: 'g',
        generation: 2,
        reason: 'member-left',
      }),
    ).toBe('00:03.000  RebalanceStarted g  gen=2  reason=member-left');
    expect(
      formatEvent({
        ...at,
        type: 'RebalanceCompleted',
        groupId: 'g',
        generation: 3,
        protocol: 'range',
        leader: 'c1-0a',
        members: [
          { memberId: 'c1-0a', consumerId: 'c1', partitions: [{ topic: 'o', partition: 0 }] },
          { memberId: 'c2-0b', consumerId: 'c2', partitions: [] },
        ],
      }),
    ).toBe('00:03.000  RebalanceCompleted g  gen=3  range  c1: o-0, c2: none');
    expect(
      describeEvent({
        ...at,
        type: 'RebalanceCompleted',
        groupId: 'g',
        generation: 4,
        protocol: null,
        leader: null,
        members: [],
      }),
    ).toBe('g  gen=4  no members (Empty)');
    expect(
      describeEvent({
        ...at,
        type: 'MemberRemoved',
        groupId: 'g',
        memberId: 'c1-0a',
        consumerId: 'c1',
        reason: 'session-timeout',
      }),
    ).toBe('g  c1 (c1-0a) removed: session-timeout');
    expect(
      describeEvent({
        ...at,
        type: 'MemberJoined',
        groupId: 'g',
        memberId: 'c1-0a',
        consumerId: 'c1',
      }),
    ).toBe('g  c1 joined as c1-0a');
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

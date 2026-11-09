import type { EngineEvent } from '@kp/engine';
import { describe, expect, it } from 'vitest';
import {
  ALL_EVENT_TYPES,
  EVENT_TYPE_GROUPS,
  filterEvents,
  filterSummary,
  isInternalTopicEvent,
  NO_HIDDEN_TYPES,
  setGroup,
  toggleType,
} from './event-filter.ts';

const events: EngineEvent[] = [
  { t: 0, seq: 0, type: 'Connected', from: 'p1', to: 't1' },
  {
    t: 1,
    seq: 1,
    type: 'RecordAppended',
    topic: 't1',
    partition: 0,
    offset: 0,
    key: 'A',
    value: 'v',
    producerId: 'p1',
    traceId: 'p1@1#1',
  },
  {
    t: 2,
    seq: 2,
    type: 'OffsetCommitted',
    groupId: 'g1',
    consumerId: 'c1',
    memberId: null,
    generation: -1,
    topic: 't1',
    partition: 0,
    offset: 1,
    acceptedAt: 2,
  },
];

describe('event types', () => {
  it('lists every type once', () => {
    expect(new Set(ALL_EVENT_TYPES).size).toBe(ALL_EVENT_TYPES.length);
    expect(ALL_EVENT_TYPES).toHaveLength(36);
    expect(EVENT_TYPE_GROUPS.map((g) => g.label)).toEqual([
      'Records',
      'Commit',
      'Group',
      'Consumers',
      'Broker',
      'Replication',
      'Topology',
    ]);
  });
});

// A 1-broker restart (M4 follow-up G-1): events about __consumer_offsets mixed with the player's
// topic and with events that have no topic at all.
const OFFSETS = '__consumer_offsets';
const restart: EngineEvent[] = [
  { t: 0, seq: 0, type: 'BrokerStopped', brokerId: 'b1', reason: 'crash' },
  { t: 1, seq: 1, type: 'BrokerFenced', brokerId: 'b1', reason: 'session-timeout' },
  {
    t: 1,
    seq: 2,
    type: 'PartitionOffline',
    topic: 'orders',
    partition: 0,
    previousLeader: 'b1',
    isr: ['b1'],
    elr: [],
  },
  {
    t: 1,
    seq: 3,
    type: 'PartitionOffline',
    topic: OFFSETS,
    partition: 7,
    previousLeader: 'b1',
    isr: ['b1'],
    elr: [],
  },
  { t: 2, seq: 4, type: 'BrokerStarted', brokerId: 'b1' },
  { t: 3, seq: 5, type: 'BrokerUnfenced', brokerId: 'b1' },
  {
    t: 3,
    seq: 6,
    type: 'LeaderElected',
    topic: OFFSETS,
    partition: 7,
    leader: 'b1',
    previousLeader: null,
    leaderEpoch: 2,
    reason: 'unclean',
  },
  {
    t: 3,
    seq: 7,
    type: 'CoordinatorMoved',
    groupId: 'g1',
    from: null,
    to: 'b1',
    state: 'Stable',
    generation: 1,
  },
  {
    t: 4,
    seq: 8,
    type: 'ReplicaFetched',
    broker: 'b2',
    leader: 'b1',
    topic: OFFSETS,
    partition: 7,
    fromOffset: 0,
    toOffset: 1,
  },
];

describe('filterEvents', () => {
  it('returns the same array when nothing is hidden', () => {
    expect(filterEvents(events, NO_HIDDEN_TYPES, true)).toBe(events);
    // Internal topics hidden, but none of these events is about one: still the same array.
    expect(filterEvents(events, NO_HIDDEN_TYPES, false)).toBe(events);
  });

  it('drops hidden types and keeps the order', () => {
    const shown = filterEvents(events, new Set(['Connected']), true);
    expect(shown.map((e) => e.seq)).toEqual([1, 2]);
    expect(filterEvents(events, new Set(ALL_EVENT_TYPES), true)).toEqual([]);
  });

  it('hides events about internal topics by default and shows them with the toggle', () => {
    expect(filterEvents(restart, NO_HIDDEN_TYPES, false).map((e) => e.seq)).toEqual([
      0, 1, 2, 4, 5, 7,
    ]);
    expect(filterEvents(restart, NO_HIDDEN_TYPES, true)).toBe(restart);
  });

  it('never hides CoordinatorMoved or broker events: they have no topic', () => {
    const always = [
      'CoordinatorMoved',
      'BrokerFenced',
      'BrokerUnfenced',
      'BrokerStarted',
      'BrokerStopped',
    ];
    const shown = filterEvents(restart, NO_HIDDEN_TYPES, false).map((e) => e.type);
    for (const type of always) expect(shown).toContain(type);
    for (const event of restart.filter((e) => always.includes(e.type))) {
      expect(isInternalTopicEvent(event)).toBe(false);
    }
  });

  it('applies both filters together', () => {
    const shown = filterEvents(restart, new Set(['PartitionOffline']), false);
    expect(shown.map((e) => e.seq)).toEqual([0, 1, 4, 5, 7]);
  });
});

describe('toggleType / setGroup', () => {
  it('toggles one type without changing the input', () => {
    const hidden = toggleType(NO_HIDDEN_TYPES, 'RecordAppended');
    expect([...hidden]).toEqual(['RecordAppended']);
    expect(NO_HIDDEN_TYPES.size).toBe(0);
    expect(toggleType(hidden, 'RecordAppended').size).toBe(0);
  });

  it('hides and shows a whole group', () => {
    const records = EVENT_TYPE_GROUPS[0]?.types ?? [];
    const hidden = setGroup(new Set(['Connected']), records, false);
    expect(hidden.size).toBe(1 + records.length);
    expect([...setGroup(hidden, records, true)]).toEqual(['Connected']);
  });
});

describe('filterSummary', () => {
  it('says how many types are shown', () => {
    expect(filterSummary(NO_HIDDEN_TYPES, true)).toBe('All types');
    expect(filterSummary(new Set(['Connected']), true)).toBe(
      `${String(ALL_EVENT_TYPES.length - 1)} of ${String(ALL_EVENT_TYPES.length)} types`,
    );
    expect(filterSummary(new Set(ALL_EVENT_TYPES), true)).toBe('No types');
  });

  it('says when internal topics are hidden', () => {
    expect(filterSummary(NO_HIDDEN_TYPES, false)).toBe('All types · internal topics hidden');
    expect(filterSummary(new Set(ALL_EVENT_TYPES), false)).toBe(
      'No types · internal topics hidden',
    );
  });
});

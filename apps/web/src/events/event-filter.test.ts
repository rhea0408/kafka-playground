import type { EngineEvent } from '@kp/engine';
import { describe, expect, it } from 'vitest';
import {
  ALL_EVENT_TYPES,
  EVENT_TYPE_GROUPS,
  filterEvents,
  filterSummary,
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
    topic: 't1',
    partition: 0,
    offset: 1,
  },
];

describe('event types', () => {
  it('lists every type once', () => {
    expect(new Set(ALL_EVENT_TYPES).size).toBe(ALL_EVENT_TYPES.length);
    expect(ALL_EVENT_TYPES).toHaveLength(13);
    expect(EVENT_TYPE_GROUPS.map((g) => g.label)).toEqual([
      'Records',
      'Offsets',
      'Consumers',
      'Topology',
    ]);
  });
});

describe('filterEvents', () => {
  it('returns the same array when nothing is hidden', () => {
    expect(filterEvents(events, NO_HIDDEN_TYPES)).toBe(events);
  });

  it('drops hidden types and keeps the order', () => {
    const shown = filterEvents(events, new Set(['Connected']));
    expect(shown.map((e) => e.seq)).toEqual([1, 2]);
    expect(filterEvents(events, new Set(ALL_EVENT_TYPES))).toEqual([]);
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
    expect(filterSummary(NO_HIDDEN_TYPES)).toBe('All types');
    expect(filterSummary(new Set(['Connected']))).toBe('12 of 13 types');
    expect(filterSummary(new Set(ALL_EVENT_TYPES))).toBe('No types');
  });
});

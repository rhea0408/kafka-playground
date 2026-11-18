import { describe, expect, it } from 'vitest';
import {
  consumer,
  consumerPartition,
  fixtureSnapshot,
  groupWithCommits,
} from '../test/snapshot-fixture.ts';
import { groupOffsets, resolveTarget } from './partition-info.ts';

describe('resolveTarget', () => {
  const snapshot = fixtureSnapshot();

  it('defaults to the first topic, partition 0', () => {
    expect(resolveTarget(snapshot, null)?.partition.index).toBe(0);
    expect(resolveTarget(snapshot, null)?.topic.config.id).toBe('t1');
  });

  it('uses the chosen partition, and falls back when it is gone', () => {
    expect(resolveTarget(snapshot, { topicId: 't1', partition: 1 })?.partition.index).toBe(1);
    expect(resolveTarget(snapshot, { topicId: 't1', partition: 7 })?.partition.index).toBe(0);
    expect(resolveTarget(snapshot, { topicId: 'gone', partition: 1 })?.partition.index).toBe(0);
  });

  it('picks __consumer_offsets only while internal topics are shown, never by default (G-1)', () => {
    const [first] = snapshot.topics;
    if (first === undefined) throw new Error('fixture has a topic');
    const internal = { ...first, config: { ...first.config, id: '__consumer_offsets' } };
    const withOffsets = fixtureSnapshot({ topics: [internal, ...snapshot.topics] });
    const target = { topicId: '__consumer_offsets', partition: 1 };
    expect(resolveTarget(withOffsets, null, true)?.topic.config.id).toBe('t1');
    expect(resolveTarget(withOffsets, target, true)?.topic.config.id).toBe('__consumer_offsets');
    expect(resolveTarget(withOffsets, target, false)?.topic.config.id).toBe('t1');
    expect(resolveTarget(withOffsets, target)?.partition.index).toBe(0);
  });

  it('is null without topics or a snapshot', () => {
    expect(resolveTarget(null, null)).toBeNull();
    expect(resolveTarget(fixtureSnapshot({ topics: [] }), null)).toBeNull();
  });
});

describe('groupOffsets', () => {
  it('joins the group committed offsets with member positions', () => {
    // The fixture consumer c1 is in group "c1-group" (the schema default).
    const snapshot = fixtureSnapshot({
      groups: [groupWithCommits('c1-group', [{ topic: 't1', partition: 0, offset: 10 }])],
    });
    expect(groupOffsets(snapshot, 't1', 0, 12)).toEqual([
      {
        groupId: 'c1-group',
        committed: 10,
        lag: 2,
        members: [{ consumerId: 'c1', position: 12, status: 'stable' }],
      },
    ]);
  });

  it('lists a group with no commit yet, and a group with a commit but no member', () => {
    const snapshot = fixtureSnapshot({
      consumers: [consumer('c1', [consumerPartition(0, 12, 4, null)], 'stopped')],
      groups: [groupWithCommits('old', [{ topic: 't1', partition: 0, offset: 3 }])],
    });
    const groups = groupOffsets(snapshot, 't1', 0, 12);
    expect(groups.map((g) => [g.groupId, g.committed, g.lag, g.members.length])).toEqual([
      ['c1-group', null, null, 1],
      ['old', 3, 9, 0],
    ]);
    expect(groups[0]?.members[0]?.status).toBe('stopped');
    // Another partition: c1 is assigned there too, "old" has no commit there.
    expect(groupOffsets(snapshot, 't1', 1, 3)).toEqual([]);
  });
});

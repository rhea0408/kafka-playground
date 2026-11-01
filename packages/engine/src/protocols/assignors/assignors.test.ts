import { describe, expect, it } from 'vitest';
import { ASSIGNORS, rebalanceProtocol } from './index.ts';
import type { Assignment, AssignorMember, TopicPartition } from './types.ts';

const tp = (topic: string, partition: number): TopicPartition => ({ topic, partition });

function member(
  memberId: string,
  topics: readonly string[],
  ownedPartitions: readonly TopicPartition[] = [],
  generation = -1,
): AssignorMember {
  return { memberId, topics, ownedPartitions, generation };
}

/** "t0-1 t1-0" per member, for readable expectations. */
function show(assignment: Assignment): Record<string, string> {
  return Object.fromEntries(
    [...assignment].map(([id, list]) => [
      id,
      list.map((p) => `${p.topic}-${String(p.partition)}`).join(' '),
    ]),
  );
}

const counts = (entries: Record<string, number>) => new Map(Object.entries(entries));

describe('range (ported from RangeAssignorTest)', () => {
  const range = ASSIGNORS.range;

  it('one consumer gets every partition', () => {
    expect(show(range([member('c1', ['t'])], counts({ t: 3 })))).toEqual({ c1: 't-0 t-1 t-2' });
  });

  it('only subscribed topics are assigned, and a missing topic is ignored', () => {
    expect(show(range([member('c1', ['t', 'missing'])], counts({ t: 1, other: 2 })))).toEqual({
      c1: 't-0',
    });
  });

  it('two consumers, one topic with three partitions: the first gets two', () => {
    expect(
      show(range([member('consumer2', ['t']), member('consumer1', ['t'])], counts({ t: 3 }))),
    ).toEqual({ consumer2: 't-2', consumer1: 't-0 t-1' });
  });

  it('two topics: the same consumer gets the extra partition of each (testMultipleConsumersMixedTopics shape)', () => {
    expect(
      show(
        range([member('c1', ['t1', 't2']), member('c2', ['t1', 't2'])], counts({ t1: 3, t2: 3 })),
      ),
    ).toEqual({ c1: 't1-0 t1-1 t2-0 t2-1', c2: 't1-2 t2-2' });
  });

  it('learning checkpoint: 2 topics × 3 partitions and 4 consumers leaves c4 idle', () => {
    const members = ['c1', 'c2', 'c3', 'c4'].map((id) => member(id, ['t1', 't2']));
    expect(show(range(members, counts({ t1: 3, t2: 3 })))).toEqual({
      c1: 't1-0 t2-0',
      c2: 't1-1 t2-1',
      c3: 't1-2 t2-2',
      c4: '',
    });
  });

  it('members are ordered by member id as strings (c10 before c2)', () => {
    expect(show(range([member('c2', ['t']), member('c10', ['t'])], counts({ t: 1 })))).toEqual({
      c2: '',
      c10: 't-0',
    });
  });
});

describe('roundrobin (ported from RoundRobinAssignorTest)', () => {
  const rr = ASSIGNORS.roundrobin;

  it('two consumers, two topics with three partitions each', () => {
    expect(
      show(rr([member('c1', ['t1', 't2']), member('c2', ['t1', 't2'])], counts({ t1: 3, t2: 3 }))),
    ).toEqual({ c1: 't1-0 t1-2 t2-1', c2: 't1-1 t2-0 t2-2' });
  });

  it('learning checkpoint: 2 topics × 3 partitions and 4 consumers uses all four', () => {
    const members = ['c1', 'c2', 'c3', 'c4'].map((id) => member(id, ['t1', 't2']));
    expect(show(rr(members, counts({ t1: 3, t2: 3 })))).toEqual({
      c1: 't1-0 t2-1',
      c2: 't1-1 t2-2',
      c3: 't1-2',
      c4: 't2-0',
    });
  });

  it('skips members not subscribed to a topic (testTwoConsumersOneTopicOnePartition with mixed subscriptions)', () => {
    expect(
      show(rr([member('c1', ['t1']), member('c2', ['t1', 't2'])], counts({ t1: 2, t2: 2 }))),
    ).toEqual({ c1: 't1-0', c2: 't1-1 t2-0 t2-1' });
  });
});

describe('sticky (ported from StickyAssignorTest / AbstractStickyAssignorTest)', () => {
  const sticky = ASSIGNORS.sticky;

  it('testAddRemoveConsumerOneTopic: a new member takes only what balance needs', () => {
    const first = sticky([member('consumer1', ['t'])], counts({ t: 3 }));
    expect(show(first)).toEqual({ consumer1: 't-0 t-1 t-2' });
    const second = sticky(
      [member('consumer1', ['t'], first.get('consumer1'), 1), member('consumer2', ['t'])],
      counts({ t: 3 }),
    );
    expect(show(second)).toEqual({ consumer1: 't-0 t-1', consumer2: 't-2' });
    const third = sticky(
      [member('consumer2', ['t'], second.get('consumer2'), 2)],
      counts({ t: 3 }),
    );
    expect(show(third)).toEqual({ consumer2: 't-0 t-1 t-2' });
  });

  it('keeps every partition of members that stay when one leaves', () => {
    const owned = {
      c1: [tp('t', 0), tp('t', 3)],
      c2: [tp('t', 1), tp('t', 4)],
      c3: [tp('t', 2), tp('t', 5)],
    };
    const result = sticky(
      [member('c1', ['t'], owned.c1, 3), member('c3', ['t'], owned.c3, 3)],
      counts({ t: 6 }),
    );
    expect(show(result)).toEqual({ c1: 't-0 t-1 t-3', c3: 't-2 t-4 t-5' });
  });

  it('two members claiming one partition: the higher generation keeps it', () => {
    const result = sticky(
      [member('c1', ['t'], [tp('t', 0)], 1), member('c2', ['t'], [tp('t', 0), tp('t', 1)], 2)],
      counts({ t: 2 }),
    );
    // c2 (generation 2) wins t-0 over c1 (generation 1), then gives t-1 up for balance.
    expect(show(result)).toEqual({ c1: 't-1', c2: 't-0' });
  });

  it('two members claiming one partition in the same generation: neither keeps it', () => {
    const owners = [
      member('c1', ['t'], [tp('t', 0), tp('t', 1)], 2),
      member('c2', ['t'], [tp('t', 1), tp('t', 2)], 2),
    ];
    // t-1 is contested, so it is dealt as unassigned; with cooperative it must wait a round.
    expect(show(sticky(owners, counts({ t: 4 })))).toEqual({ c1: 't-0 t-1', c2: 't-2 t-3' });
    expect(show(ASSIGNORS['cooperative-sticky'](owners, counts({ t: 4 })))).toEqual({
      c1: 't-0',
      c2: 't-2 t-3',
    });
  });

  it('different subscriptions: balanced over what each member may take', () => {
    const result = sticky(
      [member('c1', ['t1']), member('c2', ['t1', 't2'])],
      counts({ t1: 2, t2: 2 }),
    );
    expect(show(result)).toEqual({ c1: 't1-0 t1-1', c2: 't2-0 t2-1' });
  });
});

describe('cooperative-sticky (KIP-429)', () => {
  const coop = ASSIGNORS['cooperative-sticky'];

  it('a partition that changes owner is left unassigned for one round', () => {
    const first = coop(
      [
        member('consumer1', ['t'], [tp('t', 0), tp('t', 1), tp('t', 2)], 1),
        member('consumer2', ['t']),
      ],
      counts({ t: 3 }),
    );
    expect(show(first)).toEqual({ consumer1: 't-0 t-1', consumer2: '' });
    // consumer1 revoked t-2 and rejoined: the follow-up rebalance gives it to consumer2.
    const second = coop(
      [member('consumer1', ['t'], first.get('consumer1'), 2), member('consumer2', ['t'], [], 2)],
      counts({ t: 3 }),
    );
    expect(show(second)).toEqual({ consumer1: 't-0 t-1', consumer2: 't-2' });
  });

  it('partitions of a member that left are assigned at once (no owner to revoke them)', () => {
    expect(show(coop([member('c2', ['t'], [tp('t', 1)], 4)], counts({ t: 2 })))).toEqual({
      c2: 't-0 t-1',
    });
  });
});

describe('rebalanceProtocol', () => {
  it('is cooperative only when every listed assignor supports it', () => {
    expect(rebalanceProtocol(['range', 'cooperative-sticky'])).toBe('eager');
    expect(rebalanceProtocol(['cooperative-sticky'])).toBe('cooperative');
    expect(rebalanceProtocol(['sticky'])).toBe('eager');
    expect(rebalanceProtocol([])).toBe('eager');
  });
});

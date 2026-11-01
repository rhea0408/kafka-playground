/**
 * Assignor properties (M3-T2) over random groups: every subscribed partition is assigned exactly
 * once, balance, stickiness, and idle members when members outnumber partitions.
 */

import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { ASSIGNORS } from './index.ts';
import { stickyAssignment } from './sticky.ts';
import {
  partitionsOf,
  tpKey,
  type Assignment,
  type AssignorMember,
  type AssignorName,
  type PartitionCounts,
} from './types.ts';

const NAMES = Object.keys(ASSIGNORS) as AssignorName[];
const TOPICS = ['a', 'b', 'c'];

interface Case {
  readonly members: AssignorMember[];
  readonly counts: PartitionCounts;
}

const countsArb = fc
  .tuple(...TOPICS.map(() => fc.integer({ min: 0, max: 7 })))
  .map((ns) => new Map(TOPICS.map((t, i) => [t, ns[i] ?? 0])));

/** Members with ids that sort oddly ("m10" < "m2") and random subscriptions. */
function caseArb(sameSubscriptions: boolean): fc.Arbitrary<Case> {
  const subscription = fc.subarray(TOPICS, { minLength: 1 });
  return fc
    .record({
      counts: countsArb,
      ids: fc.uniqueArray(fc.integer({ min: 1, max: 20 }), { minLength: 1, maxLength: 8 }),
      shared: subscription,
      own: fc.array(subscription, { minLength: 8, maxLength: 8 }),
    })
    .map(({ counts, ids, shared, own }) => ({
      counts,
      members: ids.map((n, i) => ({
        memberId: `m${String(n)}`,
        topics: sameSubscriptions ? shared : (own[i] ?? shared),
        ownedPartitions: [],
        generation: -1,
      })),
    }));
}

const owners = (assignment: Assignment): Map<string, string[]> => {
  const out = new Map<string, string[]>();
  for (const [id, list] of assignment) {
    for (const tp of list) out.set(tpKey(tp), [...(out.get(tpKey(tp)) ?? []), id]);
  }
  return out;
};

const subscribedPartitions = ({ members, counts }: Case): string[] =>
  partitionsOf(
    members.flatMap((m) => m.topics),
    counts,
  ).map(tpKey);

/** The assignment fed back as owned partitions of the next generation. */
const asOwned = (members: readonly AssignorMember[], a: Assignment, generation: number) =>
  members.map((m) => ({ ...m, ownedPartitions: a.get(m.memberId) ?? [], generation }));

describe('assignor properties (fast-check)', () => {
  it.each(NAMES.filter((n) => n !== 'cooperative-sticky'))(
    '%s: every subscribed partition is assigned exactly once, only to a subscriber',
    (name) => {
      fc.assert(
        fc.property(caseArb(false), (c) => {
          const assignment = ASSIGNORS[name](c.members, c.counts);
          expect([...assignment.keys()].sort()).toEqual(c.members.map((m) => m.memberId).sort());
          const who = owners(assignment);
          expect([...who.keys()].sort()).toEqual(subscribedPartitions(c).sort());
          for (const [key, ids] of who) {
            expect(ids).toHaveLength(1);
            const topic = key.slice(0, key.lastIndexOf('-'));
            const m = c.members.find((x) => x.memberId === ids[0]);
            expect(m?.topics).toContain(topic);
          }
        }),
        { numRuns: 300 },
      );
    },
  );

  it.each(['roundrobin', 'sticky', 'cooperative-sticky'] as const)(
    '%s: with equal subscriptions, counts differ by at most 1',
    (name) => {
      fc.assert(
        fc.property(caseArb(true), (c) => {
          const sizes = [...ASSIGNORS[name](c.members, c.counts).values()].map((l) => l.length);
          expect(Math.max(...sizes) - Math.min(...sizes)).toBeLessThanOrEqual(1);
        }),
        { numRuns: 300 },
      );
    },
  );

  it.each(NAMES)('%s: with more members than partitions, the extra members get nothing', (name) => {
    fc.assert(
      fc.property(caseArb(true), (c) => {
        const total = subscribedPartitions(c).length;
        fc.pre(c.members.length > total);
        const sizes = [...ASSIGNORS[name](c.members, c.counts).values()].map((l) => l.length);
        // Range splits each topic on its own, so the same first members get a partition of
        // every topic: only the largest topic decides how many members get anything.
        const busy =
          name === 'range'
            ? Math.max(...(c.members[0]?.topics ?? []).map((t) => c.counts.get(t) ?? 0))
            : total;
        expect(sizes.filter((n) => n === 0).length).toBe(c.members.length - busy);
      }),
      { numRuns: 200 },
    );
  });

  it('sticky: a balanced assignment is kept as it is (stable across generations)', () => {
    fc.assert(
      fc.property(caseArb(false), (c) => {
        const first = ASSIGNORS.sticky(c.members, c.counts);
        const again = ASSIGNORS.sticky(asOwned(c.members, first, 1), c.counts);
        expect(again).toEqual(first);
      }),
      { numRuns: 300 },
    );
  });

  it('sticky: when a member leaves, every remaining member keeps all its partitions', () => {
    fc.assert(
      fc.property(caseArb(true), fc.nat(), (c, pick) => {
        fc.pre(c.members.length > 1);
        const first = ASSIGNORS.sticky(c.members, c.counts);
        const leaving = c.members[pick % c.members.length]?.memberId;
        const rest = asOwned(c.members, first, 1).filter((m) => m.memberId !== leaving);
        const after = ASSIGNORS.sticky(rest, c.counts);
        for (const m of rest) {
          const kept = new Set((after.get(m.memberId) ?? []).map(tpKey));
          for (const tp of m.ownedPartitions) expect(kept.has(tpKey(tp))).toBe(true);
        }
      }),
      { numRuns: 300 },
    );
  });

  it('sticky: when a member joins, existing members only give partitions up', () => {
    fc.assert(
      fc.property(caseArb(true), (c) => {
        const [joining, ...stay] = c.members;
        fc.pre(joining !== undefined && stay.length > 0);
        const first = ASSIGNORS.sticky(stay, c.counts);
        const after = ASSIGNORS.sticky([...asOwned(stay, first, 1), joining], c.counts);
        for (const m of stay) {
          const before = new Set((first.get(m.memberId) ?? []).map(tpKey));
          for (const tp of after.get(m.memberId) ?? []) expect(before.has(tpKey(tp))).toBe(true);
        }
      }),
      { numRuns: 300 },
    );
  });

  it('cooperative-sticky = sticky minus the partitions that change owner, and converges in two rounds', () => {
    fc.assert(
      fc.property(caseArb(true), (c) => {
        const [joining, ...stay] = c.members;
        fc.pre(joining !== undefined && stay.length > 0);
        const owned = asOwned(stay, ASSIGNORS.sticky(stay, c.counts), 1);
        const members = [...owned, joining];
        const { transferring } = stickyAssignment(members, c.counts);
        const coop = ASSIGNORS['cooperative-sticky'](members, c.counts);
        const sticky = ASSIGNORS.sticky(members, c.counts);
        for (const [id, list] of sticky) {
          expect(coop.get(id)).toEqual(list.filter((tp) => !transferring.has(tpKey(tp))));
        }
        // No partition moves directly from one member to another in the first round.
        for (const m of owned) {
          for (const tp of coop.get(m.memberId) ?? []) {
            const prev = owned.find((o) => o.ownedPartitions.some((p) => tpKey(p) === tpKey(tp)));
            if (prev !== undefined) expect(prev.memberId).toBe(m.memberId);
          }
        }
        // Round two (revoked partitions dropped from owned): every partition is assigned.
        const second = ASSIGNORS['cooperative-sticky'](asOwned(members, coop, 2), c.counts);
        expect([...owners(second).keys()].sort()).toEqual(subscribedPartitions(c).sort());
      }),
      { numRuns: 300 },
    );
  });
});

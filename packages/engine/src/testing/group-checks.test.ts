/**
 * The group checks must reject what they claim to reject, or the properties that use them would
 * pass vacuously. Synthetic event logs, one rule each.
 */

import { describe, expect, it } from 'vitest';
import type { EngineEvent } from '../events/index.ts';
import { commitViolations, cooperativeCoverageViolations } from './group-checks.ts';

let seq = 0;
/** Stamps a payload with time `t` and an increasing seq, as the kernel does. */
const at = (t: number, payload: Record<string, unknown>): EngineEvent =>
  ({ t, seq: ++seq, groupId: 'g', ...payload }) as unknown as EngineEvent;
const p = (partition: number) => ({ topic: 't', partition });
/** One-way network delay of these logs (the engine default). */
const DELAY = 1;

const completed = (t: number, generation: number, members: [string, number[]][]) =>
  at(t, {
    type: 'RebalanceCompleted',
    generation,
    protocol: 'cooperative-sticky',
    leader: 'm1',
    members: members.map(([id, parts]) => ({
      memberId: `m${id.slice(1)}`,
      consumerId: id,
      partitions: parts.map(p),
    })),
  });
const assigned = (t: number, consumerId: string, generation: number, parts: number[]) =>
  at(t, { type: 'PartitionsAssigned', consumerId, generation, partitions: parts.map(p) });
const revoked = (t: number, consumerId: string, parts: number[]) =>
  at(t, { type: 'PartitionsRevoked', consumerId, partitions: parts.map(p) });
const commit = (t: number, consumerId: string, generation: number, partition: number) =>
  at(t, {
    type: 'OffsetCommitted',
    consumerId,
    memberId: `m${consumerId.slice(1)}`,
    generation,
    topic: 't',
    partition,
    offset: 1,
  });

// Generation 1: c1 owns P0 and P1. Generation 2 takes P1 from c1 (pending revocation).
const base = [
  completed(10, 1, [['c1', [0, 1]]]),
  assigned(11, 'c1', 1, [0, 1]),
  completed(20, 2, [
    ['c1', [0]],
    ['c2', []],
  ]),
];

describe('commitViolations', () => {
  it('accepts the owner, and the previous owner until it revokes (judged at send time)', () => {
    expect(
      commitViolations([...base, commit(21, 'c1', 2, 0), commit(21, 'c1', 2, 1)], DELAY),
    ).toEqual([]);
    // Sent at 24, revoked at 24, accepted at 25: still allowed.
    expect(
      commitViolations([...base, revoked(24, 'c1', [1]), commit(25, 'c1', 2, 1)], DELAY),
    ).toEqual([]);
  });

  it('rejects the previous owner once it revoked before the commit was sent', () => {
    const log = [...base, revoked(24, 'c1', [1]), commit(30, 'c1', 2, 1)];
    expect(commitViolations(log, DELAY)).toEqual([
      't=30 c1 t-1 gen 2: neither owns nor still holds it',
    ]);
  });

  it('allows exactly one network delay between the revocation and the arrival, no more', () => {
    // Accepted at 26 means sent at 25, one ms after the revocation: the position was already gone.
    const late = [...base, revoked(24, 'c1', [1]), commit(26, 'c1', 2, 1)];
    expect(commitViolations(late, DELAY)).toEqual([
      't=26 c1 t-1 gen 2: neither owns nor still holds it',
    ]);
    // With a 2 ms network the same log is the in-flight case again, and allowed.
    expect(commitViolations(late, 2)).toEqual([]);
  });

  it('rejects a commit for a generation that is not the last completed one', () => {
    expect(commitViolations([...base, commit(21, 'c1', 1, 0)], DELAY)).toHaveLength(1);
  });

  it('rejects a member that never owned the partition, and two committers in one generation', () => {
    expect(commitViolations([...base, commit(21, 'c2', 2, 1)], DELAY)).toHaveLength(1);
    // A broken assignment: P0 moves straight from c1 to c2, and both commit it in generation 2.
    const log = [
      completed(10, 1, [['c1', [0]]]),
      assigned(11, 'c1', 1, [0]),
      completed(20, 2, [['c2', [0]]]),
      assigned(21, 'c2', 2, [0]),
      commit(22, 'c1', 2, 0),
      commit(23, 'c2', 2, 0),
    ];
    expect(commitViolations(log, DELAY)).toEqual(['t=23 c2 t-0 gen 2: m1 committed it too']);
  });
});

describe('cooperativeCoverageViolations', () => {
  it('accepts an unowned partition while a member still holds it', () => {
    expect(cooperativeCoverageViolations(base)).toEqual([]);
  });

  it('rejects an unowned partition no member holds', () => {
    const log = [
      ...base,
      revoked(24, 'c1', [1]),
      completed(30, 3, [
        ['c1', [0]],
        ['c2', [2]],
      ]),
    ];
    expect(cooperativeCoverageViolations(log)).toEqual([
      't=30 gen 3: t-1 has no owner and no one holds it',
    ]);
  });
});

/**
 * StickyAssignor ("sticky") and CooperativeStickyAssignor ("cooperative-sticky"), ported from
 * org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor. Both aim for a balanced
 * assignment (partition counts differ by at most one when subscriptions are equal) that moves as
 * few partitions as possible from their previous owners.
 *
 * Previous ownership comes from each member's owned partitions and their generation. If two
 * members claim the same partition, the higher generation wins; with the same generation neither
 * keeps it (AbstractStickyAssignor: "assigned to the member with the higher generation" / "claiming
 * the same TopicPartition ... in the same generation ... will be invalidated").
 *
 * When every member subscribes to the same topics, `constrainedAssign` is followed closely:
 * - minQuota = floor(P / N), maxQuota = ceil(P / N), and P % N members may have maxQuota;
 * - each member keeps its owned partitions (sorted by partition, then topic) up to its quota:
 *   all of them if fewer than minQuota, maxQuota while members above the minimum are still
 *   allowed, otherwise minQuota; the rest are given up;
 * - the unassigned partitions (topic, then partition order) are dealt round-robin to the members
 *   below minQuota, then one each to members at exactly minQuota until P % N members are above it.
 * Kafka iterates members in HashMap order in the keep step; we use member id order (deterministic).
 *
 * With different subscriptions Kafka runs `generalAssign`, a long balancing search. We keep its
 * idea in a short form: keep valid owned partitions, give each unassigned partition to the
 * subscribed member with the fewest partitions, then move partitions from members with at least
 * two more than an eligible member until no such move exists (simplifications: "Sticky assignor
 * with different subscriptions").
 *
 * Cooperative-sticky computes the same assignment and then removes every partition that would
 * change owner while its previous owner is still a member (CooperativeStickyAssignor
 * #adjustAssignment). The previous owner revokes it and rejoins; the follow-up rebalance gives it
 * to the new owner (KIP-429).
 */

import {
  compareIds,
  comparePartitions,
  finish,
  partitionsOf,
  sortedMembers,
  tpKey,
  type AssignFn,
  type AssignorMember,
  type PartitionCounts,
  type TopicPartition,
} from './types.ts';

export interface StickyResult {
  /** Member id → partitions (working lists; `finish` sorts them). */
  readonly assignment: Map<string, TopicPartition[]>;
  /** Partitions given to a member other than a previous owner that is still in the group. */
  readonly transferring: ReadonlySet<string>;
}

export const stickyAssign: AssignFn = (members, counts) =>
  finish(members, stickyAssignment(members, counts).assignment);

export const cooperativeStickyAssign: AssignFn = (members, counts) => {
  const { assignment, transferring } = stickyAssignment(members, counts);
  for (const [memberId, list] of assignment) {
    assignment.set(
      memberId,
      list.filter((tp) => !transferring.has(tpKey(tp))),
    );
  }
  return finish(members, assignment);
};

export function stickyAssignment(
  members: readonly AssignorMember[],
  counts: PartitionCounts,
): StickyResult {
  const sorted = sortedMembers(members);
  const owned = validOwnedPartitions(sorted, counts);
  return allSubscriptionsEqual(sorted)
    ? constrainedAssign(sorted, counts, owned)
    : generalAssign(sorted, counts, owned);
}

function allSubscriptionsEqual(members: readonly AssignorMember[]): boolean {
  const first = members[0];
  if (first === undefined) return true;
  const topics = [...new Set(first.topics)].sort().join('\u0000');
  return members.every((m) => [...new Set(m.topics)].sort().join('\u0000') === topics);
}

interface OwnedPartitions {
  /** Member id → the partitions it may keep (existing, subscribed, uncontested), sorted. */
  readonly byMember: Map<string, TopicPartition[]>;
  /** Partition → the member that owned it last (after resolving conflicts by generation). */
  readonly previousOwner: Map<string, string>;
  /** Partitions claimed by two members of the same generation: no previous owner keeps them. */
  readonly contested: Set<string>;
}

/**
 * Owned partitions that still exist and are still subscribed, with conflicting claims resolved
 * by generation. Partitions are sorted by partition number, then topic, as Kafka's keep step
 * sorts them (so a member keeps its lowest-numbered partitions across topics first).
 */
function validOwnedPartitions(
  members: readonly AssignorMember[],
  counts: PartitionCounts,
): OwnedPartitions {
  const claim = new Map<string, { memberId: string; generation: number; tp: TopicPartition }>();
  const contested = new Set<string>();
  const contestedGeneration = new Map<string, number>();
  for (const member of members) {
    const subscribed = new Set(member.topics);
    for (const tp of member.ownedPartitions) {
      if (!subscribed.has(tp.topic) || tp.partition >= (counts.get(tp.topic) ?? 0)) continue;
      const key = tpKey(tp);
      const blockedAt = contestedGeneration.get(key);
      if (blockedAt !== undefined && member.generation <= blockedAt) continue;
      const current = claim.get(key);
      if (current === undefined || member.generation > current.generation) {
        claim.set(key, { memberId: member.memberId, generation: member.generation, tp });
        contested.delete(key);
      } else if (member.generation === current.generation && current.memberId !== member.memberId) {
        claim.delete(key);
        contested.add(key);
        contestedGeneration.set(key, member.generation);
      }
    }
  }
  const byMember = new Map<string, TopicPartition[]>(members.map((m) => [m.memberId, []]));
  const previousOwner = new Map<string, string>();
  for (const [key, { memberId, tp }] of claim) {
    byMember.get(memberId)?.push(tp);
    previousOwner.set(key, memberId);
  }
  for (const list of byMember.values()) {
    list.sort((a, b) => a.partition - b.partition || comparePartitions(a, b));
  }
  return { byMember, previousOwner, contested };
}

function constrainedAssign(
  members: readonly AssignorMember[],
  counts: PartitionCounts,
  owned: OwnedPartitions,
): StickyResult {
  const assignment = new Map<string, TopicPartition[]>(members.map((m) => [m.memberId, []]));
  const transferring = new Set<string>();
  const first = members[0];
  if (first === undefined) return { assignment, transferring };

  const all = partitionsOf(first.topics, counts);
  const total = all.length;
  const minQuota = Math.floor(total / members.length);
  const maxQuota = Math.ceil(total / members.length);
  const expectedOverMin = total % members.length;
  let overMin = 0;

  const assigned = new Set<string>();
  const underMin: string[] = [];
  let exactlyMin: string[] = [];

  // Step 1: keep owned partitions up to the member's quota (assignOwnedPartitions).
  for (const member of members) {
    const mine = owned.byMember.get(member.memberId) ?? [];
    const target = assignment.get(member.memberId) ?? [];
    let keep: number;
    if (mine.length < minQuota) {
      keep = mine.length;
      underMin.push(member.memberId);
    } else if (mine.length >= maxQuota && overMin < expectedOverMin) {
      keep = maxQuota;
      overMin++;
      if (overMin === expectedOverMin) exactlyMin = [];
    } else {
      keep = minQuota;
      if (overMin !== expectedOverMin && maxQuota > minQuota) exactlyMin.push(member.memberId);
    }
    for (const tp of mine.slice(0, keep)) {
      target.push(tp);
      assigned.add(tpKey(tp));
    }
  }

  // Steps 2 and 4: deal the rest round-robin, first to members below minQuota, then one each to
  // members at exactly minQuota until expectedOverMin members are above it (assignRoundRobin).
  underMin.sort(compareIds);
  exactlyMin.sort(compareIds);
  let cursor = 0;
  for (const tp of all) {
    const key = tpKey(tp);
    if (assigned.has(key)) continue;
    let memberId: string | undefined;
    if (underMin.length > 0) {
      cursor %= underMin.length;
      memberId = underMin[cursor];
    } else {
      memberId = exactlyMin.shift();
      if (memberId !== undefined) {
        overMin++;
        if (overMin === expectedOverMin) exactlyMin = [];
      }
    }
    if (memberId === undefined) break; // cannot happen: the quotas cover every partition
    const list = assignment.get(memberId) ?? [];
    list.push(tp);
    assigned.add(key);
    markTransfer(owned, key, memberId, transferring);
    if (underMin.length > 0) {
      if (list.length === minQuota) {
        underMin.splice(cursor, 1);
        if (maxQuota > minQuota && overMin < expectedOverMin) exactlyMin.push(memberId);
      } else {
        cursor++;
      }
    }
  }
  return { assignment, transferring };
}

function generalAssign(
  members: readonly AssignorMember[],
  counts: PartitionCounts,
  owned: OwnedPartitions,
): StickyResult {
  const assignment = new Map<string, TopicPartition[]>(
    members.map((m) => [m.memberId, [...(owned.byMember.get(m.memberId) ?? [])]]),
  );
  const transferring = new Set<string>();
  const subscribed = new Map(members.map((m) => [m.memberId, new Set(m.topics)]));
  const size = (id: string): number => assignment.get(id)?.length ?? 0;
  const eligible = (tp: TopicPartition): string[] =>
    members
      .filter((m) => subscribed.get(m.memberId)?.has(tp.topic) === true)
      .map((m) => m.memberId);
  const fewest = (ids: readonly string[]): string | undefined =>
    [...ids].sort((a, b) => size(a) - size(b) || compareIds(a, b))[0];

  const assigned = new Set([...assignment.values()].flatMap((list) => list.map((tp) => tpKey(tp))));
  for (const tp of partitionsOf(
    members.flatMap((m) => m.topics),
    counts,
  )) {
    const key = tpKey(tp);
    if (assigned.has(key)) continue;
    const memberId = fewest(eligible(tp));
    if (memberId === undefined) continue;
    assignment.get(memberId)?.push(tp);
    markTransfer(owned, key, memberId, transferring);
  }

  // Balance: move a partition from a member to an eligible one with at least two fewer.
  for (let moved = true; moved;) {
    moved = false;
    const bySizeDesc = [...members]
      .map((m) => m.memberId)
      .sort((a, b) => size(b) - size(a) || compareIds(a, b));
    for (const from of bySizeDesc) {
      const list = assignment.get(from) ?? [];
      for (let i = list.length - 1; i >= 0 && !moved; i--) {
        const tp = list[i];
        if (tp === undefined) continue;
        const to = fewest(eligible(tp).filter((id) => size(id) <= list.length - 2));
        if (to === undefined) continue;
        list.splice(i, 1);
        assignment.get(to)?.push(tp);
        markTransfer(owned, tpKey(tp), to, transferring);
        moved = true;
      }
      if (moved) break;
    }
  }
  return { assignment, transferring };
}

/** Records that `key` moves away from a previous owner that is still a member. */
function markTransfer(
  owned: OwnedPartitions,
  key: string,
  newOwner: string,
  transferring: Set<string>,
): void {
  const previous = owned.previousOwner.get(key);
  if ((previous !== undefined && previous !== newOwner) || owned.contested.has(key)) {
    transferring.add(key);
  } else {
    transferring.delete(key);
  }
}

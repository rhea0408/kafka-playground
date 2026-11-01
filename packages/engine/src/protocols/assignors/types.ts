/**
 * Shared types for the partition assignors (M3-T2). An assignor runs on the group *leader* (a
 * client) in the classic protocol: it gets every member's subscription and owned partitions from
 * the JoinGroup response and returns each member's partitions (ConsumerPartitionAssignor#assign).
 * Assignors are pure functions: same input, same output, no engine state.
 */

import type { Assignor as AssignorName } from '@kp/schema';

export interface TopicPartition {
  readonly topic: string;
  readonly partition: number;
}

/** One member as the leader sees it (ConsumerPartitionAssignor.Subscription). */
export interface AssignorMember {
  readonly memberId: string;
  /** Topics this member subscribes to. */
  readonly topics: readonly string[];
  /**
   * Partitions the member owned before this rebalance (Subscription#ownedPartitions; the eager
   * StickyAssignor sends the same information in its user data). Ignored by range and round-robin.
   */
  readonly ownedPartitions: readonly TopicPartition[];
  /** The generation those partitions were owned in; -1 when unknown (DEFAULT_GENERATION). */
  readonly generation: number;
}

/** Partition count of every topic the leader's metadata knows, by topic name. */
export type PartitionCounts = ReadonlyMap<string, number>;

/** Member id → assigned partitions, sorted by topic then partition. Every member has an entry. */
export type Assignment = ReadonlyMap<string, readonly TopicPartition[]>;

export type AssignFn = (members: readonly AssignorMember[], counts: PartitionCounts) => Assignment;

export type { AssignorName };

/** Topic then partition: the order Kafka's assignors sort partitions in. */
export function comparePartitions(a: TopicPartition, b: TopicPartition): number {
  if (a.topic !== b.topic) return a.topic < b.topic ? -1 : 1;
  return a.partition - b.partition;
}

/** Java String#compareTo order for member ids (UTF-16 code units, as `<` compares). */
export function compareIds(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

export const tpKey = (tp: TopicPartition): string => `${tp.topic}-${String(tp.partition)}`;

/** Every partition of the given topics that exist in `counts`, sorted (topic, partition). */
export function partitionsOf(topics: Iterable<string>, counts: PartitionCounts): TopicPartition[] {
  const out: TopicPartition[] = [];
  for (const topic of [...new Set(topics)].sort()) {
    const n = counts.get(topic) ?? 0;
    for (let partition = 0; partition < n; partition++) out.push({ topic, partition });
  }
  return out;
}

/** Members sorted by member id (AbstractPartitionAssignor.MemberInfo#compareTo, no static ids). */
export const sortedMembers = (members: readonly AssignorMember[]): AssignorMember[] =>
  [...members].sort((a, b) => compareIds(a.memberId, b.memberId));

/** Freezes a working assignment into the result shape, partitions sorted, members in input order. */
export function finish(
  members: readonly AssignorMember[],
  working: ReadonlyMap<string, TopicPartition[]>,
): Assignment {
  return new Map(
    members.map((m) => [m.memberId, [...(working.get(m.memberId) ?? [])].sort(comparePartitions)]),
  );
}

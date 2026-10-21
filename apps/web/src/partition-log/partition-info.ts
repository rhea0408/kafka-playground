/**
 * What the Partition log tab needs from a snapshot (screen 05c, FR-6.1): the partition itself and,
 * per consumer group, its committed offset and each member's position. Pure: unit-tested in Node.
 */

import type { EngineSnapshot, PartitionSnapshot, TopicSnapshot } from '@kp/engine';
import type { LogTarget } from '../stores/ui-store.ts';

export interface MemberPosition {
  readonly consumerId: string;
  /** Next offset this consumer will fetch; null before its first poll or while stopped. */
  readonly position: number | null;
  readonly status: 'running' | 'stopped';
}

export interface GroupOffsets {
  readonly groupId: string;
  /** The group's committed offset (next record to consume); null if it never committed here. */
  readonly committed: number | null;
  /** HW − committed, as kafka-consumer-groups.sh prints LAG; null without a committed offset. */
  readonly lag: number | null;
  readonly members: readonly MemberPosition[];
}

export interface ResolvedTarget {
  readonly topic: TopicSnapshot;
  readonly partition: PartitionSnapshot;
}

/**
 * The partition to show: the chosen one if it still exists, else the chosen topic's partition 0,
 * else the first topic's partition 0. Null when there are no topics.
 */
export function resolveTarget(
  snapshot: EngineSnapshot | null,
  target: LogTarget | null,
): ResolvedTarget | null {
  const topics = snapshot?.topics ?? [];
  const topic = topics.find((t) => t.config.id === target?.topicId) ?? topics[0];
  if (topic === undefined) return null;
  const wanted = topic.config.id === target?.topicId ? target.partition : 0;
  const partition = topic.partitions[wanted] ?? topic.partitions[0];
  if (partition === undefined) return null;
  return { topic, partition };
}

/**
 * Every group that has a committed offset for this partition or a member assigned to it, sorted
 * by group id. In M2 every consumer reads all partitions of its topics, so each member of a
 * group shows a position here (one consumer per group until M3).
 */
export function groupOffsets(
  snapshot: EngineSnapshot,
  topicId: string,
  partition: number,
  highWatermark: number,
): GroupOffsets[] {
  const committed = new Map<string, number>();
  for (const group of snapshot.groups) {
    const entry = group.committed.find((c) => c.topic === topicId && c.partition === partition);
    if (entry !== undefined) committed.set(group.groupId, entry.offset);
  }
  const members = new Map<string, MemberPosition[]>();
  for (const consumer of snapshot.consumers) {
    const assigned = consumer.partitions.find(
      (p) => p.topic === topicId && p.partition === partition,
    );
    if (assigned === undefined) continue;
    const list = members.get(consumer.config.groupId) ?? [];
    list.push({
      consumerId: consumer.config.id,
      position: assigned.position,
      status: consumer.status,
    });
    members.set(consumer.config.groupId, list);
  }
  const ids = [...new Set([...committed.keys(), ...members.keys()])].sort();
  return ids.map((groupId) => {
    const offset = committed.get(groupId) ?? null;
    return {
      groupId,
      committed: offset,
      lag: offset === null ? null : Math.max(0, highWatermark - offset),
      members: members.get(groupId) ?? [],
    };
  });
}

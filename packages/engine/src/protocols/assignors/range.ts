/**
 * RangeAssignor (org.apache.kafka.clients.consumer.RangeAssignor, name "range"): for each topic
 * separately, the subscribed members are sorted by member id and the partitions are split into
 * contiguous ranges. With P partitions and C members, each gets floor(P / C) and the first
 * P % C members get one more. Because every topic is split the same way, the first members get
 * the extra partition of every topic, and with more members than partitions the last members of
 * a topic get none of it. Rack-aware assignment is not modelled (no client.rack).
 */

import {
  finish,
  sortedMembers,
  type AssignFn,
  type AssignorMember,
  type TopicPartition,
} from './types.ts';

export const rangeAssign: AssignFn = (members, counts) => {
  const working = new Map<string, TopicPartition[]>(members.map((m) => [m.memberId, []]));
  const byTopic = new Map<string, AssignorMember[]>();
  for (const member of sortedMembers(members)) {
    for (const topic of new Set(member.topics)) {
      if (!counts.has(topic)) continue;
      byTopic.set(topic, [...(byTopic.get(topic) ?? []), member]);
    }
  }
  for (const [topic, consumers] of byTopic) {
    const numPartitions = counts.get(topic) ?? 0;
    const perConsumer = Math.floor(numPartitions / consumers.length);
    const withExtra = numPartitions % consumers.length;
    consumers.forEach((member, i) => {
      const start = perConsumer * i + Math.min(i, withExtra);
      const length = perConsumer + (i < withExtra ? 1 : 0);
      const list = working.get(member.memberId) ?? [];
      for (let p = start; p < start + length; p++) list.push({ topic, partition: p });
    });
  }
  return finish(members, working);
};

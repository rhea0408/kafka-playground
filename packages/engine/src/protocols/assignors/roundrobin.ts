/**
 * RoundRobinAssignor (org.apache.kafka.clients.consumer.RoundRobinAssignor, name "roundrobin"):
 * lists every partition of every subscribed topic, topics in name order and partitions in number
 * order, and deals them out to the members sorted by member id, in a circle. A member that does
 * not subscribe to a partition's topic is skipped for it:
 *
 *   while (!subscriptions.get(assigner.peek().memberId).topics().contains(topic)) assigner.next();
 *   assignment.get(assigner.next().memberId).add(partition);
 */

import {
  finish,
  partitionsOf,
  sortedMembers,
  type AssignFn,
  type TopicPartition,
} from './types.ts';

export const roundRobinAssign: AssignFn = (members, counts) => {
  const working = new Map<string, TopicPartition[]>(members.map((m) => [m.memberId, []]));
  const circle = sortedMembers(members);
  if (circle.length === 0) return finish(members, working);
  const subscribed = circle.map((m) => new Set(m.topics));
  let next = 0;
  for (const tp of partitionsOf(
    circle.flatMap((m) => m.topics),
    counts,
  )) {
    // Some member subscribes to tp.topic (that is how it got into the list), so this ends.
    while (!(subscribed[next % circle.length]?.has(tp.topic) ?? false)) next++;
    const member = circle[next % circle.length];
    if (member !== undefined) working.get(member.memberId)?.push(tp);
    next++;
  }
  return finish(members, working);
};

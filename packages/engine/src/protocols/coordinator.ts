/**
 * The internal topic __consumer_offsets and finding a group's coordinator (FindCoordinator),
 * M4-T8 (M4 Q8, Q9; M4-decisions K10).
 *
 * Committed offsets and group metadata live in __consumer_offsets, a real, replicated topic with
 * offsets.topic.num.partitions (50) partitions. The coordinator of a group is the **leader** of
 * partition `Utils.abs(groupId.hashCode()) % 50` (GroupCoordinatorService#partitionFor), so it
 * moves when that partition's leadership moves. The topic is created on the first FindCoordinator
 * (KafkaApis#getCoordinator), placed by the same StripedReplicaPlacer as other topics.
 *
 * Its replication factor: offsets.topic.replication.factor (3) in Kafka, which can't be created
 * with fewer registered brokers (FindCoordinator answers COORDINATOR_NOT_AVAILABLE for ever).
 * The game uses min(offsets.topic.replication.factor, registered brokers) when it creates the
 * topic (game-tuned, Q9), so single-broker scenes still have coordinators.
 */

import { TopicSchema } from '@kp/schema';
import type { EngineContext } from '../model/context.ts';
import { createPartition } from '../model/partition.ts';
import { placeReplicas } from '../model/placement.ts';
import { isAlive, placementBrokers, type TopicState } from '../model/state.ts';
import { startFollowers } from './replication.ts';

/** offsets.topic.num.partitions default. */
export const OFFSETS_TOPIC_PARTITIONS = 50;

/** The internal topic's name (Topic.GROUP_METADATA_TOPIC_NAME). */
export const OFFSETS_TOPIC = '__consumer_offsets';

/** Whether a topic is internal (hidden on the canvas, M4 Q8). */
export const isInternalTopic = (topic: string): boolean => topic === OFFSETS_TOPIC;

/** java.lang.String#hashCode: s[0]*31^(n-1) + … + s[n-1], in 32-bit int arithmetic (UTF-16). */
export function javaStringHashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return h;
}

/** Kafka's Utils.abs: Math.abs, except Integer.MIN_VALUE maps to 0. */
const kafkaAbs = (n: number): number => (n === -2147483648 ? 0 : Math.abs(n));

/** The __consumer_offsets partition that holds `groupId`. */
export function partitionFor(groupId: string, partitions = OFFSETS_TOPIC_PARTITIONS): number {
  return kafkaAbs(javaStringHashCode(groupId)) % partitions;
}

/**
 * The internal topic, created on first use (AutoTopicCreationManager). Null while no broker can
 * take a replica (none registered, or all shutting down).
 */
export function ensureOffsetsTopic(ctx: EngineContext): TopicState | null {
  const { state } = ctx;
  const existing = state.topics.get(OFFSETS_TOPIC);
  if (existing !== undefined) return existing;
  const brokers = placementBrokers(state);
  if (brokers.length === 0 || !brokers.some((b) => !b.fenced)) return null;
  const configured = [...state.brokers.values()].sort((a, b) =>
    a.config.id < b.config.id ? -1 : a.config.id > b.config.id ? 1 : 0,
  )[0]?.config.offsetsTopicReplicationFactor;
  const replicationFactor = Math.min(configured ?? 3, brokers.length);
  const assignments = placeReplicas(
    OFFSETS_TOPIC_PARTITIONS,
    replicationFactor,
    brokers,
    ctx.kernel.rng.fork(`placement:${OFFSETS_TOPIC}`),
  );
  const topic: TopicState = {
    topicId: ++state.counters.topic,
    config: TopicSchema.parse({
      kind: 'topic',
      id: OFFSETS_TOPIC,
      partitions: OFFSETS_TOPIC_PARTITIONS,
      replicationFactor,
    }),
    partitions: assignments.map((replicas, index) =>
      createPartition(OFFSETS_TOPIC, index, replicas),
    ),
  };
  state.topics.set(OFFSETS_TOPIC, topic);
  for (const partition of topic.partitions) startFollowers(ctx, partition);
  return topic;
}

/**
 * The broker that coordinates `groupId` (FindCoordinator's answer), creating __consumer_offsets
 * on first use: the leader of its partition. Null while that partition has no leader or the topic
 * can't be created (COORDINATOR_NOT_AVAILABLE).
 */
export function coordinatorFor(ctx: EngineContext, groupId: string): string | null {
  const topic = ensureOffsetsTopic(ctx);
  const leader = topic?.partitions[partitionFor(groupId)]?.leader ?? null;
  if (leader === null) return null;
  const broker = ctx.state.brokers.get(leader);
  return broker !== undefined && isAlive(broker.status) ? leader : null;
}

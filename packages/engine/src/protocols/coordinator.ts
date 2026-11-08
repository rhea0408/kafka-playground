/**
 * Finding a group's coordinator (FindCoordinator). Committed offsets and group metadata live in
 * the internal topic __consumer_offsets; the coordinator of a group is the leader of partition
 * `Utils.abs(groupId.hashCode()) % offsets.topic.num.partitions` (GroupCoordinatorService
 * #partitionFor; default 50 partitions). The topic is created on the first FindCoordinator
 * (KafkaApis#getCoordinator); its partitions never move afterwards unless reassigned (M4).
 *
 * Simplified: the lookup has no round trip, and the internal topic's replication factor is not
 * modelled. Real Kafka uses offsets.topic.replication.factor = 3 and cannot create the topic on a
 * cluster with fewer brokers (FindCoordinator keeps answering COORDINATOR_NOT_AVAILABLE), which
 * is why single-broker setups set it to 1 (simplifications: "Group coordinator lookup").
 */

import type { EngineContext } from '../model/context.ts';
import { placeReplicas } from '../model/placement.ts';
import { placementBrokers } from '../model/state.ts';

/** offsets.topic.num.partitions default. */
export const OFFSETS_TOPIC_PARTITIONS = 50;

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
 * The broker that coordinates `groupId`, creating __consumer_offsets on first use. Null while the
 * cluster has no broker (the client gets COORDINATOR_NOT_AVAILABLE and retries).
 */
export function coordinatorFor(ctx: EngineContext, groupId: string): string | null {
  const { state } = ctx;
  if (state.offsetsTopicLeaders === null) {
    // No broker can take a replica (none registered, or all shutting down): the topic can't be
    // created yet, so FindCoordinator answers COORDINATOR_NOT_AVAILABLE (K10).
    if (placementBrokers(state).length === 0) return null;
    // TODO(M4-T8): a real replicated internal topic; for now only the leaders are kept.
    state.offsetsTopicLeaders = placeReplicas(
      OFFSETS_TOPIC_PARTITIONS,
      1,
      placementBrokers(state).map((b) => ({ ...b, fenced: false })),
      ctx.kernel.rng.fork('placement:__consumer_offsets'),
    ).map((replicas) => replicas[0] as string);
  }
  const leader = state.offsetsTopicLeaders[partitionFor(groupId)];
  return leader !== undefined && state.brokers.has(leader) ? leader : null;
}

/** Whether `brokerId` leads a partition of __consumer_offsets (so it coordinates groups). */
export function hostsOffsetsTopic(ctx: EngineContext, brokerId: string): boolean {
  return ctx.state.offsetsTopicLeaders?.includes(brokerId) ?? false;
}

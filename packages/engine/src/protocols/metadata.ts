/**
 * Client-side cluster metadata: how many partitions a producer or consumer *thinks* a topic has.
 *
 * Kafka clients cache metadata and do not hear about new partitions by themselves. They refresh
 * it every `metadata.max.age.ms` (default 300000 = 5 min: "The period of time in milliseconds
 * after which we force a refresh of metadata even if we haven't seen any partition leadership
 * changes to proactively discover any new brokers or partitions", CommonClientConfigs), or sooner
 * when a request fails with a metadata error such as UNKNOWN_TOPIC_OR_PARTITION
 * (`Metadata#requestUpdate`). The first send to / subscription of a topic fetches that topic's
 * metadata at once (KIP-526: new topics are fetched on their own, without a full refresh).
 *
 * So after partitions are added, a producer keeps hashing keys over the old count, and a
 * subscribed consumer keeps its old assignment, until its next refresh.
 *
 * Simplified: the refresh is applied instantly when the client next needs metadata (a send or a
 * poll() after the age has passed), instead of after a MetadataRequest round trip.
 */

import type { EngineContext } from '../model/context.ts';
import type { TopicState } from '../model/state.ts';

export interface ClientMetadata {
  /** Partition count per topic, as of this client's last refresh. */
  readonly partitionCounts: Map<string, number>;
  /** Sim time of the last full refresh; the next is due metadata.max.age.ms later. */
  refreshedAt: number;
  /** Set after a metadata error: refresh at the next opportunity, whatever the age. */
  updateRequested: boolean;
}

export function createClientMetadata(now: number): ClientMetadata {
  return { partitionCounts: new Map(), refreshedAt: now, updateRequested: false };
}

/** Forgets everything, as a freshly started client process would. */
export function resetClientMetadata(metadata: ClientMetadata, now: number): void {
  metadata.partitionCounts.clear();
  metadata.refreshedAt = now;
  metadata.updateRequested = false;
}

/**
 * Refreshes every cached topic if the metadata is older than `maxAgeMs` or an update was
 * requested. Returns the topics whose partition count changed (or that disappeared).
 */
export function refreshIfStale(
  ctx: EngineContext,
  metadata: ClientMetadata,
  maxAgeMs: number,
): string[] {
  const now = ctx.kernel.now();
  if (!metadata.updateRequested && now - metadata.refreshedAt < maxAgeMs) return [];
  metadata.refreshedAt = now;
  metadata.updateRequested = false;
  const changed: string[] = [];
  for (const [topic, count] of metadata.partitionCounts) {
    const actual = ctx.state.topics.get(topic)?.partitions.length;
    if (actual === count) continue;
    if (actual === undefined) metadata.partitionCounts.delete(topic);
    else metadata.partitionCounts.set(topic, actual);
    changed.push(topic);
  }
  return changed;
}

/** The partition count this client uses for `topic`, fetching it on first use. */
export function knownPartitionCount(metadata: ClientMetadata, topic: TopicState): number {
  const known = metadata.partitionCounts.get(topic.config.id);
  if (known !== undefined) return known;
  const actual = topic.partitions.length;
  metadata.partitionCounts.set(topic.config.id, actual);
  return actual;
}

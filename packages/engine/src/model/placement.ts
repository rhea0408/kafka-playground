/**
 * Where a new topic's partitions live. In KRaft mode the controller places replicas with
 * org.apache.kafka.metadata.placement.StripedReplicaPlacer, which stripes partitions across
 * brokers (and racks) from a randomised starting point, so topics do not all put partition 0 on
 * the same broker. Simplified: plain round-robin over brokers sorted by id, from a seeded random
 * start, one replica each (replication factor 1 until M4). See docs/simplifications.md.
 */

import type { Rng } from '../core/rng.ts';

/** Leader broker id for each partition index. `brokerIds` must be non-empty. */
export function placePartitions(
  partitionCount: number,
  brokerIds: readonly string[],
  rng: Rng,
): string[] {
  if (brokerIds.length === 0) throw new Error('placePartitions needs at least one broker');
  const sorted = [...brokerIds].sort();
  const start = rng.int(0, sorted.length - 1);
  return Array.from(
    { length: partitionCount },
    (_, p) => sorted[(start + p) % sorted.length] as string,
  );
}

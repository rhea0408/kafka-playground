/**
 * Where a new partition's replicas live. In KRaft mode the controller places replicas with
 * org.apache.kafka.metadata.placement.StripedReplicaPlacer (Kafka 4.4, M4-decisions K13). This is
 * a port of its rack-unaware path (every broker in one rack; racks arrive in v0.5):
 *
 * - The unfenced and the fenced brokers each form a list sorted by id, with a random start offset
 *   (fenced list first, then unfenced; Kafka also draws a rack offset from a one-rack list).
 * - For each partition the leader is the next **unfenced** broker of the stripe; followers are the
 *   following unfenced brokers, then fenced ones. Each partition starts one broker further along
 *   (`BrokerList#next` moves its offset by one on a new "epoch", i.e. a new partition).
 * - After as many partitions as there are unfenced brokers, the lists are shuffled, so the pattern
 *   does not repeat forever (when there is more than one unfenced broker).
 *
 * So partition p of a topic gets consecutive brokers starting at the random offset + p, which
 * spreads both the replicas and the preferred leaders (replicas[0]) over the brokers.
 *
 * Simplified: Kafka uses an unseeded java.util.Random; we use the engine's seeded RNG
 * (determinism, docs/simplifications.md). Brokers in controlled shutdown are excluded by the
 * caller. RF > registered brokers is rejected by the caller (INVALID_REPLICATION_FACTOR).
 */

import type { Rng } from '../core/rng.ts';

/** BrokerList from StripedReplicaPlacer: a list with a moving offset, read once per epoch. */
class BrokerList {
  private epoch = 0;
  private index = 0;
  private offset = 0;
  private readonly brokers: string[];
  constructor(brokers: string[]) {
    this.brokers = brokers;
  }

  get size(): number {
    return this.brokers.length;
  }

  initialize(rng: Rng): void {
    if (this.brokers.length > 0) this.offset = rng.int(0, this.brokers.length - 1);
  }

  /** The next broker for this epoch, or undefined once every broker was returned. */
  next(epoch: number): string | undefined {
    if (this.epoch !== epoch) {
      this.epoch = epoch;
      this.index = 0;
      this.offset = this.brokers.length === 0 ? 0 : (this.offset + 1) % this.brokers.length;
    }
    if (this.index >= this.brokers.length) return undefined;
    const broker = this.brokers[(this.index + this.offset) % this.brokers.length];
    this.index++;
    return broker;
  }

  /** Collections.shuffle (Fisher–Yates from the end), with the seeded RNG. */
  shuffle(rng: Rng): void {
    for (let i = this.brokers.length - 1; i > 0; i--) {
      const j = rng.int(0, i);
      const a = this.brokers[i] as string;
      this.brokers[i] = this.brokers[j] as string;
      this.brokers[j] = a;
    }
  }
}

export interface PlacementBroker {
  readonly id: string;
  readonly fenced: boolean;
}

/**
 * Replica lists (preferred leader first) for `partitionCount` partitions with `replicationFactor`
 * replicas each. Needs at least one unfenced broker and `replicationFactor` ≤ brokers.
 */
export function placeReplicas(
  partitionCount: number,
  replicationFactor: number,
  brokers: readonly PlacementBroker[],
  rng: Rng,
): string[][] {
  const unfencedIds = brokers.filter((b) => !b.fenced).map((b) => b.id);
  const fencedIds = brokers.filter((b) => b.fenced).map((b) => b.id);
  if (unfencedIds.length === 0) throw new Error('placeReplicas needs an unfenced broker');
  if (replicationFactor > brokers.length) {
    throw new Error('placeReplicas: replication factor larger than the number of brokers');
  }
  const unfenced = new BrokerList([...unfencedIds].sort());
  const fenced = new BrokerList([...fencedIds].sort());
  // RackList's constructor initialises the fenced list, then the unfenced one, then picks a rack
  // offset from the (single) rack: random.nextInt(1), always 0.
  fenced.initialize(rng);
  unfenced.initialize(rng);
  rng.int(0, 0);

  const result: string[][] = [];
  let epoch = 0;
  for (let p = 0; p < partitionCount; p++) {
    const replicas: string[] = [];
    const leader = unfenced.next(epoch) as string;
    replicas.push(leader);
    while (replicas.length < replicationFactor) {
      const next = unfenced.next(epoch) ?? fenced.next(epoch);
      if (next === undefined) break;
      replicas.push(next);
    }
    result.push(replicas);
    epoch++;
    if (epoch === unfenced.size && unfenced.size > 1) {
      unfenced.shuffle(rng);
      fenced.shuffle(rng);
      epoch = 0;
    }
  }
  return result;
}

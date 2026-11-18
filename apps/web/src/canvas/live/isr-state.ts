/**
 * A replicated partition's health, for the partition row's chip and the partition log's replica
 * summary (M4 follow-up G-3). Pure: unit-tested in Node.
 *
 * Under min ISR is the state players miss: with fewer in-sync replicas than min.insync.replicas
 * the leader stops moving the high watermark (strict min ISR, `Partition.maybeIncrementLeaderHW`,
 * M4-decisions K1b), so acks=1 writes are still acknowledged but consumers get nothing new, and
 * acks=all writes fail with NOT_ENOUGH_REPLICAS.
 */

import type { PartitionSnapshot } from '@kp/engine';

export type IsrState = 'in-sync' | 'under-replicated' | 'under-min-isr';

/**
 * In sync, under-replicated, or under min ISR. Always "in-sync" for RF 1, which can't be
 * under-replicated, and for an offline partition, whose "⚠ offline" badge already says more.
 */
export function isrState(
  partition: Pick<PartitionSnapshot, 'replicas' | 'leader' | 'underMinIsr' | 'underReplicated'>,
): IsrState {
  if (partition.replicas.length <= 1 || partition.leader === null) return 'in-sync';
  if (partition.underMinIsr) return 'under-min-isr';
  if (partition.underReplicated) return 'under-replicated';
  return 'in-sync';
}

/**
 * Tooltip for "under min ISR". `minIsr` is min(min.insync.replicas, RF): Kafka applies the RF when
 * min.insync.replicas is above it (M4-decisions T3-4).
 */
export const underMinIsrText = (inSync: number, minIsr: number): string =>
  `The in-sync replicas (${String(inSync)}) are fewer than min.insync.replicas (${String(minIsr)}). Kafka stops moving the high watermark, so consumers see no new records, even ones written with acks=1, and acks=all writes are refused with NOT_ENOUGH_REPLICAS.`;

/** Tooltip for "under-replicated". */
export const underReplicatedText = (inSync: number, replicas: number): string =>
  `${String(inSync)} of ${String(replicas)} replicas are in sync.`;

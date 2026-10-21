/**
 * A partition is an ordered, append-only log of records (Kafka docs, "Topics and Logs" in the
 * Introduction, and Design §"Replication"). Each record gets the next offset when the leader
 * appends it; offsets are never reused or reordered.
 *
 * Offsets tracked per partition (FR-2.4):
 * - logStartOffset: offset of the first record still in the log (moves up with retention, v0.4).
 * - logEndOffset (LEO): offset the *next* record will get, i.e. last offset + 1.
 * - highWatermark (HW): records below it are replicated to every in-sync replica and visible to
 *   consumers. With a single replica (M1) the leader is the whole ISR, so HW = LEO after every
 *   append. From M4 the HW only advances once followers have fetched the record.
 */

import type { SimTime } from '../core/clock.ts';

export interface LogRecord {
  readonly offset: number;
  readonly key: string | null;
  readonly value: string;
  /** CreateTime: when the producer created the record (sim ms). */
  readonly timestamp: SimTime;
  readonly producerId: string;
  /**
   * Id for following one record through the system, unique across producer lives:
   * "<producerId>@<incarnation>#<n>", e.g. "p1@3#7" (see produce.ts).
   */
  readonly traceId: string;
  /**
   * Serialized size in bytes (the producer's messageSizeBytes game setting). Used where Kafka
   * counts bytes: fetch.min.bytes and the sticky partitioner's batch.size.
   */
  readonly sizeBytes: number;
}

export interface PartitionState {
  readonly topic: string;
  readonly index: number;
  /** Broker id of the partition leader. Fixed in M1; elections arrive in M4. */
  readonly leader: string;
  readonly log: LogRecord[];
  logStartOffset: number;
  logEndOffset: number;
  highWatermark: number;
}

export function createPartition(topic: string, index: number, leader: string): PartitionState {
  return { topic, index, leader, log: [], logStartOffset: 0, logEndOffset: 0, highWatermark: 0 };
}

/** Appends a record at the leader and returns it with its assigned offset. */
export function appendRecord(
  partition: PartitionState,
  record: Omit<LogRecord, 'offset'>,
): LogRecord {
  const appended: LogRecord = { offset: partition.logEndOffset, ...record };
  partition.log.push(appended);
  partition.logEndOffset += 1;
  // Single replica: the leader alone is the ISR, so everything appended is committed.
  partition.highWatermark = partition.logEndOffset;
  return appended;
}

/**
 * Records a consumer may read starting at `fromOffset`: at most `max` (default: all), and only
 * below the high watermark (consumers never see records that are not yet fully replicated).
 */
export function readRecords(
  partition: PartitionState,
  fromOffset: number,
  max = Number.POSITIVE_INFINITY,
): LogRecord[] {
  const start = Math.max(fromOffset, partition.logStartOffset);
  const end = Math.min(partition.highWatermark, start + max);
  if (end <= start) return [];
  const base = partition.logStartOffset;
  return partition.log.slice(start - base, end - base);
}

/** Bytes a fetch from `fromOffset` could return now: the records from there up to the HW. */
export function bytesAvailable(partition: PartitionState, fromOffset: number): number {
  let bytes = 0;
  for (const r of readRecords(partition, fromOffset)) bytes += r.sizeBytes;
  return bytes;
}

export const partitionName = (topic: string, index: number): string => `${topic}-${String(index)}`;

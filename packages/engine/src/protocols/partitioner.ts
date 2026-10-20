/**
 * The producer's default partitioner, ported from the Apache Kafka Java client (4.x):
 *
 * - Keyed records: `Utils.toPositive(Utils.murmur2(serializedKey)) % numPartitions`
 *   (org.apache.kafka.clients.producer.internals.BuiltInPartitioner#partitionForKey). The same key
 *   always lands on the same partition, as long as the partition count does not change.
 * - Records without a key: the "uniform sticky" partitioner (KIP-480, KIP-794). The producer
 *   stays on one partition until about batch.size bytes have been produced to it, then switches
 *   to a randomly chosen partition (which may be the same one again).
 *
 * Keys are serialised the way StringSerializer does it: String#getBytes(UTF_8).
 */

import type { Rng } from '../core/rng.ts';

/**
 * murmur2, ported line by line from org.apache.kafka.common.utils.Utils#murmur2 (Kafka trunk,
 * clients/src/main/java/org/apache/kafka/common/utils/Utils.java). Java `int` arithmetic is
 * reproduced with Math.imul and `| 0`; Java's `>>>` is JavaScript's `>>>`. Returns a signed int32.
 */
export function murmur2(data: Uint8Array): number {
  const length = data.length;
  const seed = 0x9747b28c;
  // 'm' and 'r' are mixing constants generated offline.
  const m = 0x5bd1e995;
  const r = 24;

  // Initialize the hash to a random value
  let h = (seed ^ length) | 0;
  const length4 = length >> 2;

  for (let i = 0; i < length4; i++) {
    const i4 = i << 2;
    // Java reads these four bytes as one little-endian int.
    let k =
      (byteAt(data, i4) & 0xff) |
      ((byteAt(data, i4 + 1) & 0xff) << 8) |
      ((byteAt(data, i4 + 2) & 0xff) << 16) |
      ((byteAt(data, i4 + 3) & 0xff) << 24);
    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);
    h = Math.imul(h, m);
    h ^= k;
  }

  // Handle the last few bytes of the input array (Java's switch falls through 3 → 2 → 1).
  const index = length4 << 2;
  const rest = length - index;
  if (rest >= 3) h ^= (byteAt(data, index + 2) & 0xff) << 16;
  if (rest >= 2) h ^= (byteAt(data, index + 1) & 0xff) << 8;
  if (rest >= 1) {
    h ^= byteAt(data, index) & 0xff;
    h = Math.imul(h, m);
  }

  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;

  return h | 0;
}

/** org.apache.kafka.common.utils.Utils#toPositive: clears the sign bit (not Math.abs). */
export function toPositive(n: number): number {
  return n & 0x7fffffff;
}

/** BuiltInPartitioner#partitionForKey for a String key. */
export function partitionForKey(key: string, numPartitions: number): number {
  return toPositive(murmur2(utf8Bytes(key))) % numPartitions;
}

/**
 * UTF-8 bytes of a string, matching Java's String#getBytes(StandardCharsets.UTF_8). An unpaired
 * surrogate is encoded as "?" (0x3f), Java's replacement byte; TextEncoder would emit U+FFFD
 * instead (and is a browser global the engine must not use).
 */
export function utf8Bytes(s: string): Uint8Array {
  const out: number[] = [];
  for (let i = 0; i < s.length; i++) {
    let cp = s.charCodeAt(i);
    if (cp >= 0xd800 && cp <= 0xdbff) {
      const next = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (next >= 0xdc00 && next <= 0xdfff) {
        cp = 0x10000 + ((cp - 0xd800) << 10) + (next - 0xdc00);
        i++;
      } else {
        out.push(0x3f);
        continue;
      }
    } else if (cp >= 0xdc00 && cp <= 0xdfff) {
      out.push(0x3f);
      continue;
    }
    if (cp < 0x80) out.push(cp);
    else if (cp < 0x800) out.push(0xc0 | (cp >> 6), 0x80 | (cp & 0x3f));
    else if (cp < 0x10000)
      out.push(0xe0 | (cp >> 12), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
    else {
      out.push(
        0xf0 | (cp >> 18),
        0x80 | ((cp >> 12) & 0x3f),
        0x80 | ((cp >> 6) & 0x3f),
        0x80 | (cp & 0x3f),
      );
    }
  }
  return Uint8Array.from(out);
}

function byteAt(data: Uint8Array, i: number): number {
  return data[i] ?? 0;
}

// ── Sticky partitioning for records without a key ─────────────────────────────────────────────

/** Per producer and topic: the current sticky partition and bytes produced to it. */
export interface StickyState {
  partition: number | null;
  producedBytes: number;
}

export function createStickyState(): StickyState {
  return { partition: null, producedBytes: 0 };
}

/** The partition the next keyless record goes to. Picks one at random the first time. */
export function stickyPartition(state: StickyState, numPartitions: number, rng: Rng): number {
  if (state.partition === null || state.partition >= numPartitions) {
    state.partition = rng.int(0, numPartitions - 1);
    state.producedBytes = 0;
  }
  return state.partition;
}

/**
 * Records that `bytes` were appended to the sticky partition, and switches once batch.size bytes
 * have been produced to it (BuiltInPartitioner#updatePartitionInfo). The next partition is uniform
 * over all partitions and may repeat (BuiltInPartitioner#nextPartition without load stats).
 *
 * Simplified: the real client may wait until 2 × batch.size when its batches are not yet full
 * (`enableSwitch`), and with adaptive partitioning it weights the choice by broker queue sizes.
 */
export function recordStickyAppend(
  state: StickyState,
  bytes: number,
  batchSize: number,
  numPartitions: number,
  rng: Rng,
): void {
  state.producedBytes += bytes;
  if (state.producedBytes >= Math.max(1, batchSize)) {
    state.partition = rng.int(0, numPartitions - 1);
    state.producedBytes = 0;
  }
}

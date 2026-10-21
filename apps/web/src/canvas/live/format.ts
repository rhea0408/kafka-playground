/**
 * Pure helpers for the live state on canvas nodes (M2-T8, FR-1.4): number formatting, the key
 * letter shown in log cells (and, in T9, on message dots), and the consumer summary that fits a
 * 196 × 40 row. No React, no DOM: unit-tested in Node.
 */

import type { ConsumerPartitionSnapshot, ConsumerSnapshot, LogRecord } from '@kp/engine';

/** Log cells per partition row: 6 × 18 px + 5 × 3 px gaps = 123 px of the row's ~140 free px. */
export const LOG_CELLS = 6;

/**
 * Lag (in records) above which the badge uses the danger color (A5). Wireframe 05 shows lag 3 as
 * normal and lag 412 as danger; the docs give no number, so this is ours (M2-decisions T8).
 */
export const LAG_DANGER_THRESHOLD = 100;

/** A5: the lag number counts to its new value over this long. */
export const LAG_COUNT_MS = 300;

/** 1234567 → "1,234,567". Fixed format, not the browser locale, so tests are stable. */
export function formatCount(n: number): string {
  return String(Math.trunc(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

/** Short form for tight spots: up to 9,999 in full, then 12.3k, 123k, 1.2M. */
export function compactCount(n: number): string {
  if (n < 10_000) return formatCount(n);
  if (n < 100_000) return `${(Math.floor(n / 100) / 10).toFixed(1)}k`;
  if (n < 1_000_000) return `${String(Math.floor(n / 1000))}k`;
  return `${(Math.floor(n / 100_000) / 10).toFixed(1)}M`;
}

/** Null key → "∅" (not "no key" text: it must fit an 18 px cell). */
export const NULL_KEY_LETTER = '∅';

/** The letter shown for a record key: its first character, or ∅ for a null key. */
export function keyLetter(key: string | null): string {
  if (key === null) return NULL_KEY_LETTER;
  const first = key.codePointAt(0);
  return first === undefined ? '""' : String.fromCodePoint(first);
}

/**
 * The last `n` records of a partition window, and whether each is committed. Records at offsets
 * ≥ HW are on the leader but not yet replicated to the whole ISR, so consumers can't read them yet
 * (Kafka docs, Design §"Replication"). With one replica (M2) HW = LEO after every append, so all
 * records are below HW; M4 makes the difference visible.
 */
export function visibleCells(
  records: readonly LogRecord[],
  highWatermark: number,
  n: number = LOG_CELLS,
): { readonly record: LogRecord; readonly committed: boolean }[] {
  return records.slice(-n).map((record) => ({ record, committed: record.offset < highWatermark }));
}

/** Sum of the non-null values, or null when every value is null (nothing known yet). */
function sumKnown(values: readonly (number | null)[]): number | null {
  let sum: number | null = null;
  for (const v of values) if (v !== null) sum = (sum ?? 0) + v;
  return sum;
}

export interface ConsumerSummary {
  /** Number of partitions the consumer reads (all partitions of its topics in M2). */
  readonly partitions: number;
  /** Next offset to fetch, summed over partitions; null when no partition has one. */
  readonly position: number | null;
  /** Committed offset, summed over partitions; null when the group never committed any. */
  readonly committed: number | null;
  /** Total lag (HW − committed) over the partitions with a committed offset; null if none. */
  readonly lag: number | null;
  /** One line per partition, kafka-consumer-groups.sh style, for the row's tooltip. */
  readonly breakdown: readonly string[];
}

const dash = (n: number | null): string => (n === null ? '-' : formatCount(n));

function partitionLine(p: ConsumerPartitionSnapshot, topics: number): string {
  const name = topics > 1 ? `${p.topic} P${String(p.partition)}` : `P${String(p.partition)}`;
  return `${name}: position ${dash(p.position)} · committed ${dash(p.committed)} · end ${formatCount(p.highWatermark)} · lag ${dash(p.lag)}`;
}

/**
 * Totals for the consumer row. kafka-consumer-groups.sh --describe prints one line per partition
 * (CURRENT-OFFSET, LOG-END-OFFSET, LAG) and tools sum LAG per group; the row shows the sums and the
 * per-partition lines go into its tooltip (M2-decisions T8). Lag comes from the engine (HW −
 * committed, M1 deviation); partitions without a committed offset are left out of the total, as
 * the tool prints "-" for them.
 */
export function summarizeConsumer(consumer: ConsumerSnapshot): ConsumerSummary {
  const ps = consumer.partitions;
  const topics = new Set(ps.map((p) => p.topic)).size;
  return {
    partitions: ps.length,
    position: sumKnown(ps.map((p) => p.position)),
    committed: sumKnown(ps.map((p) => p.committed)),
    lag: sumKnown(ps.map((p) => p.lag)),
    breakdown: ps.map((p) => partitionLine(p, topics)),
  };
}

/**
 * Snapshot diffs: what changed between two snapshots, small enough to send to the UI every
 * frame (~33 ms) across the Web Worker boundary (architecture §5, M2-T3).
 *
 * - Brokers, producers, consumers and groups: changed entries are sent whole, by id.
 * - Topics: only changed partition summaries; for a partition's records, only the records with
 *   offsets above the newest record `prev` held, plus how many old records drop off the front of
 *   the window. This relies on a partition being an append-only log in which offsets are never
 *   reused (Kafka docs, Introduction "Topics and Logs").
 * - Append-only does not hold across a topic being deleted and re-created with the same name: the
 *   new topic starts again at offset 0. Kafka tells the two apart by topic ID (KIP-516); so do we
 *   (`TopicSnapshot.topicId`), and a new topic ID sends the whole topic. As a second guard, the
 *   records `prev` and `next` share are compared field by field; any mismatch (offsets going
 *   backwards, a window that does not line up) sends the whole partition instead.
 * - Removed ids are listed explicitly. The order of ids is sent only when it is not the obvious
 *   one (e.g. a component removed and re-added with the same id moves to the end).
 *
 * A diff is plain objects and arrays only (no Map, class or function), so it survives
 * `postMessage`'s structured clone and JSON. `applyDiff` never mutates `prev`: unchanged entries
 * are shared between `prev` and the result (structural sharing).
 */

import type { Connection, Topic } from '@kp/schema';
import type { SimTime } from '../core/clock.ts';
import type { LogRecord } from '../model/partition.ts';
import type {
  BrokerSnapshot,
  ConsumerSnapshot,
  EngineSnapshot,
  GroupSnapshot,
  PartitionSnapshot,
  ProducerSnapshot,
  TopicSnapshot,
} from './index.ts';

/** Changes to a list of entries identified by id. Empty fields are left out. */
export interface KeyedDiff<C> {
  /** New or changed entries (or entry diffs), in their order in `next`. */
  readonly changed?: readonly C[];
  /** Ids present in `prev` but not in `next`. */
  readonly removed?: readonly string[];
  /**
   * Every id of `next`, in order. Only sent when it differs from the default order: the ids of
   * `prev` without the removed ones, followed by the new ids in `changed` order.
   */
  readonly order?: readonly string[];
}

/** A partition summary that changed. */
export type PartitionDiff =
  /** The whole partition: a new partition, or its records do not continue `prev`'s. */
  | { readonly index: number; readonly full: PartitionSnapshot }
  /** Offsets and leader as in `next`; records = prev.records.slice(drop) + append. */
  | {
      readonly index: number;
      readonly leader: string;
      readonly logStartOffset: number;
      readonly logEndOffset: number;
      readonly highWatermark: number;
      readonly drop: number;
      readonly append: readonly LogRecord[];
    };

/** A topic that changed, by name. */
export type TopicDiff =
  /** The whole topic: new, or re-created (new topic ID), or its partitions do not line up. */
  | { readonly id: string; readonly full: TopicSnapshot }
  /** Same topic ID: the new config if it changed, and the changed partitions by index. */
  | {
      readonly id: string;
      readonly config?: Topic;
      readonly partitions?: readonly PartitionDiff[];
    };

/** The diff type of each snapshot field. Adding a field to EngineSnapshot without one fails to compile. */
interface FieldDiffs {
  readonly t: SimTime;
  readonly brokers: KeyedDiff<BrokerSnapshot>;
  readonly topics: KeyedDiff<TopicDiff>;
  readonly producers: KeyedDiff<ProducerSnapshot>;
  readonly consumers: KeyedDiff<ConsumerSnapshot>;
  readonly groups: KeyedDiff<GroupSnapshot>;
  /** Sent whole when anything about the connections changed (they are few and small). */
  readonly connections: readonly Connection[];
}

/** What changed from one snapshot to the next. `{}` when nothing changed. */
export type SnapshotDiff = { readonly [K in keyof EngineSnapshot]?: FieldDiffs[K] };

/** Diff from `prev` to `next`. Neither snapshot is modified. */
export function diffSnapshot(prev: EngineSnapshot, next: EngineSnapshot): SnapshotDiff {
  const diff: Partial<Record<keyof EngineSnapshot, unknown>> = {};
  for (const field of FIELDS) {
    const differ = DIFFERS[field] as (p: unknown, n: unknown) => unknown;
    const d = differ(prev[field], next[field]);
    if (d !== undefined) diff[field] = d;
  }
  return diff as SnapshotDiff;
}

/**
 * Applies `diff` (made by `diffSnapshot(prev, next)`) to `prev` and returns a snapshot equal to
 * `next`. `prev` is not modified; unchanged parts are shared with it. Throws if the diff does not
 * fit `prev` (it was made from a different snapshot).
 */
export function applyDiff(prev: EngineSnapshot, diff: SnapshotDiff): EngineSnapshot {
  if (isEmptyDiff(diff)) return prev;
  const next: Record<string, unknown> = { ...prev };
  for (const field of FIELDS) {
    const d = diff[field];
    if (d === undefined) continue;
    const applier = APPLIERS[field] as (p: unknown, d: unknown) => unknown;
    next[field] = applier(prev[field], d);
  }
  return next as unknown as EngineSnapshot;
}

/** True when the diff carries no change at all. */
export const isEmptyDiff = (diff: SnapshotDiff): boolean => Object.keys(diff).length === 0;

// ---------------------------------------------------------------------------------------------
// Per-field diff and apply

type Differs = {
  readonly [K in keyof EngineSnapshot]: (
    prev: EngineSnapshot[K],
    next: EngineSnapshot[K],
  ) => FieldDiffs[K] | undefined;
};

type Appliers = {
  readonly [K in keyof EngineSnapshot]: (
    prev: EngineSnapshot[K],
    diff: FieldDiffs[K],
  ) => EngineSnapshot[K];
};

const brokerId = (b: BrokerSnapshot): string => b.config.id;
const topicId = (t: TopicSnapshot): string => t.config.id;
const producerId = (p: ProducerSnapshot): string => p.config.id;
const consumerId = (c: ConsumerSnapshot): string => c.config.id;
const groupId = (g: GroupSnapshot): string => g.groupId;

/** Whole-entry change: the entry itself when it differs (or is new). */
const wholeEntry = <T>(prev: T | undefined, next: T): T | undefined =>
  prev !== undefined && deepEqual(prev, next) ? undefined : next;
const takeEntry = <T>(_prev: T | undefined, change: T): T => change;

const DIFFERS: Differs = {
  t: (prev, next) => (prev === next ? undefined : next),
  brokers: (prev, next) => diffKeyed(prev, next, brokerId, wholeEntry),
  topics: (prev, next) => diffKeyed(prev, next, topicId, diffTopic),
  producers: (prev, next) => diffKeyed(prev, next, producerId, wholeEntry),
  consumers: (prev, next) => diffKeyed(prev, next, consumerId, wholeEntry),
  groups: (prev, next) => diffKeyed(prev, next, groupId, wholeEntry),
  connections: (prev, next) => (deepEqual(prev, next) ? undefined : next),
};

const APPLIERS: Appliers = {
  t: (_prev, t) => t,
  brokers: (prev, d) => applyKeyed(prev, d, brokerId, brokerId, takeEntry),
  topics: (prev, d) => applyKeyed(prev, d, topicId, (c) => c.id, applyTopic),
  producers: (prev, d) => applyKeyed(prev, d, producerId, producerId, takeEntry),
  consumers: (prev, d) => applyKeyed(prev, d, consumerId, consumerId, takeEntry),
  groups: (prev, d) => applyKeyed(prev, d, groupId, groupId, takeEntry),
  connections: (_prev, next) => next,
};

const FIELDS = Object.keys(DIFFERS) as (keyof EngineSnapshot)[];

// ---------------------------------------------------------------------------------------------
// Keyed lists

/**
 * `diffItem(undefined, n)` must return a change: a new id is always sent. Returns undefined when
 * nothing changed.
 */
function diffKeyed<T, C>(
  prev: readonly T[],
  next: readonly T[],
  key: (item: T) => string,
  diffItem: (prev: T | undefined, next: T) => C | undefined,
): KeyedDiff<C> | undefined {
  const prevById = new Map(prev.map((item) => [key(item), item]));
  const nextIds = next.map(key);
  const nextIdSet = new Set(nextIds);
  const removed = prev.map(key).filter((id) => !nextIdSet.has(id));
  const changed: C[] = [];
  for (const item of next) {
    const change = diffItem(prevById.get(key(item)), item);
    if (change !== undefined) changed.push(change);
  }
  const defaultOrder = [
    ...prev.map(key).filter((id) => nextIdSet.has(id)),
    ...nextIds.filter((id) => !prevById.has(id)),
  ];
  const reordered = !sameStrings(defaultOrder, nextIds);
  if (changed.length === 0 && removed.length === 0 && !reordered) return undefined;
  return {
    ...(changed.length > 0 ? { changed } : {}),
    ...(removed.length > 0 ? { removed } : {}),
    ...(reordered ? { order: nextIds } : {}),
  };
}

function applyKeyed<T, C>(
  prev: readonly T[],
  diff: KeyedDiff<C>,
  key: (item: T) => string,
  keyOfChange: (change: C) => string,
  applyItem: (prev: T | undefined, change: C) => T,
): readonly T[] {
  const prevById = new Map(prev.map((item) => [key(item), item]));
  const changes = new Map((diff.changed ?? []).map((c) => [keyOfChange(c), c]));
  const removed = new Set(diff.removed ?? []);
  const order = diff.order ?? [
    ...prev.map(key).filter((id) => !removed.has(id)),
    ...[...changes.keys()].filter((id) => !prevById.has(id)),
  ];
  return order.map((id) => {
    const change = changes.get(id);
    const before = prevById.get(id);
    if (change !== undefined) return applyItem(before, change);
    if (before === undefined) throw new Error(`Snapshot diff does not fit: no entry "${id}".`);
    return before;
  });
}

// ---------------------------------------------------------------------------------------------
// Topics and partitions

function diffTopic(prev: TopicSnapshot | undefined, next: TopicSnapshot): TopicDiff | undefined {
  const id = next.config.id;
  // Partitions only ever grow within one topic ID (CreatePartitions cannot remove any).
  if (
    prev === undefined ||
    prev.topicId !== next.topicId ||
    next.partitions.length < prev.partitions.length ||
    !next.partitions.every((p, i) => p.index === i) ||
    !prev.partitions.every((p, i) => p.index === i)
  ) {
    return { id, full: next };
  }
  const partitions: PartitionDiff[] = [];
  next.partitions.forEach((p, i) => {
    const before = prev.partitions[i];
    const change = before === undefined ? { index: i, full: p } : diffPartition(before, p);
    if (change !== undefined) partitions.push(change);
  });
  const configChanged = !deepEqual(prev.config, next.config);
  if (!configChanged && partitions.length === 0) return undefined;
  return {
    id,
    ...(configChanged ? { config: next.config } : {}),
    ...(partitions.length > 0 ? { partitions } : {}),
  };
}

function diffPartition(
  prev: PartitionSnapshot,
  next: PartitionSnapshot,
): PartitionDiff | undefined {
  const lastPrevOffset = prev.records.at(-1)?.offset ?? -1;
  // next.records = (records `prev` also holds) + (records with higher offsets).
  let kept = 0;
  while (kept < next.records.length && (next.records[kept]?.offset ?? 0) <= lastPrevOffset) kept++;
  const drop = prev.records.length - kept;
  let continues = drop >= 0;
  for (let i = 0; continues && i < kept; i++) {
    continues = sameRecord(prev.records[drop + i], next.records[i]);
  }
  if (!continues) return { index: next.index, full: next };

  const unchanged =
    kept === next.records.length &&
    drop === 0 &&
    prev.leader === next.leader &&
    prev.logStartOffset === next.logStartOffset &&
    prev.logEndOffset === next.logEndOffset &&
    prev.highWatermark === next.highWatermark;
  if (unchanged) return undefined;
  return {
    index: next.index,
    leader: next.leader,
    logStartOffset: next.logStartOffset,
    logEndOffset: next.logEndOffset,
    highWatermark: next.highWatermark,
    drop,
    append: next.records.slice(kept),
  };
}

function applyTopic(prev: TopicSnapshot | undefined, change: TopicDiff): TopicSnapshot {
  if ('full' in change) return change.full;
  if (prev === undefined) throw new Error(`Snapshot diff does not fit: no topic "${change.id}".`);
  const partitions = [...prev.partitions];
  for (const p of change.partitions ?? []) {
    partitions[p.index] = applyPartition(partitions[p.index], p);
  }
  return {
    topicId: prev.topicId,
    config: change.config ?? prev.config,
    partitions: change.partitions === undefined ? prev.partitions : partitions,
  };
}

function applyPartition(
  prev: PartitionSnapshot | undefined,
  change: PartitionDiff,
): PartitionSnapshot {
  if ('full' in change) return change.full;
  if (prev === undefined) {
    throw new Error(`Snapshot diff does not fit: no partition ${String(change.index)}.`);
  }
  return {
    index: prev.index,
    leader: change.leader,
    logStartOffset: change.logStartOffset,
    logEndOffset: change.logEndOffset,
    highWatermark: change.highWatermark,
    records:
      change.drop === 0 && change.append.length === 0
        ? prev.records
        : [...prev.records.slice(change.drop), ...change.append],
  };
}

/** Records are immutable, so equal fields mean the same record. */
function sameRecord(a: LogRecord | undefined, b: LogRecord | undefined): boolean {
  if (a === b) return true;
  if (a === undefined || b === undefined) return false;
  return (
    a.offset === b.offset &&
    a.traceId === b.traceId &&
    a.key === b.key &&
    a.value === b.value &&
    a.timestamp === b.timestamp &&
    a.producerId === b.producerId &&
    a.sizeBytes === b.sizeBytes
  );
}

// ---------------------------------------------------------------------------------------------
// Helpers

function sameStrings(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((s, i) => s === b[i]);
}

/** Structural equality for plain JSON-like data (snapshots hold nothing else). */
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false;
    return true;
  }
  const ra = a as Record<string, unknown>;
  const rb = b as Record<string, unknown>;
  const keys = Object.keys(ra);
  if (keys.length !== Object.keys(rb).length) return false;
  return keys.every((k) => Object.hasOwn(rb, k) && deepEqual(ra[k], rb[k]));
}

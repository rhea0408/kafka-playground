/**
 * A partition is an ordered, append-only log of records (Kafka docs, "Topics and Logs" in the
 * Introduction, and Design §"Replication"). Each record gets the next offset when the leader
 * appends it; offsets are never reused or reordered.
 *
 * Since M4 a partition has `replication.factor` replicas, each on its own broker with its **own
 * copy of the log** (ReplicaLog). One replica is the leader: producers append to it and consumers
 * read from it. Followers copy the leader's log by fetching (protocols/replication.ts, M4-T3).
 * Keeping one log per replica is what makes divergence and truncation real (M4-T6). Replica logs
 * share the frozen record objects, so a copy costs one array slot per record.
 *
 * Offsets tracked per replica (FR-2.4):
 * - logStartOffset: offset of the first record still in the log (moves up with retention, v0.4).
 * - logEndOffset (LEO): offset the *next* record will get, i.e. last offset + 1.
 * - highWatermark (HW): records below it are replicated to every in-sync replica and visible to
 *   consumers. The leader computes it; followers learn it from fetch responses.
 *
 * Partition metadata, as the KRaft controller keeps it (PartitionRegistration):
 * - replicas: the assigned brokers, preferred leader first;
 * - isr: the in-sync replicas; elr / lastKnownElr: Eligible Leader Replicas (KIP-966, on by
 *   default in Kafka 4.4, M4-decisions K1);
 * - leader (null while the partition is offline), leaderEpoch and partitionEpoch.
 *
 * `log`, `logStartOffset`, `logEndOffset` and `highWatermark` on the partition read the **view
 * replica**: the leader's, or the last leader's while the partition has none. That is the log
 * consumers read and the one most views show.
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
  /**
   * The leader epoch of the leader that appended it (the batch header's partitionLeaderEpoch).
   * Followers build their leader-epoch cache from it; truncation compares epochs (KIP-101).
   */
  readonly leaderEpoch: number;
}

/** One entry of a replica's leader-epoch cache (LeaderEpochFileCache): epoch → first offset. */
export interface EpochEntry {
  readonly epoch: number;
  readonly startOffset: number;
}

/** One replica's copy of the partition, on broker `broker`. */
export interface ReplicaLog {
  readonly broker: string;
  readonly log: LogRecord[];
  logStartOffset: number;
  logEndOffset: number;
  highWatermark: number;
  /** Leader-epoch cache, ascending by epoch: where each leader epoch's records start. */
  readonly epochs: EpochEntry[];
}

/**
 * The leader's view of one follower (Replica.ReplicaState in Partition.scala). `logEndOffset` is
 * -1 while unknown (a new leader has not heard from the follower yet).
 */
export interface FollowerState {
  logEndOffset: number;
  lastCaughtUpTimeMs: number;
  lastFetchTimeMs: number;
  lastFetchLeaderLogEndOffset: number;
}

export interface PartitionState {
  readonly topic: string;
  readonly index: number;
  /** Assigned brokers, preferred leader first. Fixed (reassignment is v0.2). */
  readonly replicas: readonly string[];
  /** In-sync replicas, as the controller last committed them. */
  isr: string[];
  /**
   * The leader's maximal ISR while an AlterPartition is in flight (K4): the committed ISR plus a
   * replica being added, or the old ISR during a shrink. Null when nothing is pending (M4-T4).
   */
  pendingIsr: string[] | null;
  /** Eligible Leader Replicas (KIP-966): may lead when the ISR is empty. */
  elr: string[];
  /** The last leader before the partition went leaderless (KIP-966 lastKnownElr). */
  lastKnownElr: string[];
  /** Broker id of the leader, or null while the partition is offline. */
  leader: string | null;
  /** Bumped on every leader change. */
  leaderEpoch: number;
  /** Bumped on every change of the partition's metadata (leader, ISR, ELR). */
  partitionEpoch: number;
  /** One log per replica, in replica order. */
  readonly replicaLogs: ReadonlyMap<string, ReplicaLog>;
  /** The leader's view of each follower; reset when the leader changes. */
  readonly followers: Map<string, FollowerState>;
  /** The replica whose log `log` / offsets below read: the leader, or the last one. */
  view: string;
  /** The view replica's log and offsets (see the file comment). */
  readonly log: LogRecord[];
  readonly logStartOffset: number;
  logEndOffset: number;
  highWatermark: number;
}

function createReplicaLog(broker: string): ReplicaLog {
  return { broker, log: [], logStartOffset: 0, logEndOffset: 0, highWatermark: 0, epochs: [] };
}

/**
 * A new, empty partition on `replicas` (or one broker). The first replica leads at epoch 0, and
 * every replica starts in sync: the controller creates a partition with ISR = replicas.
 */
export function createPartition(
  topic: string,
  index: number,
  replicas: string | readonly string[],
): PartitionState {
  const list = typeof replicas === 'string' ? [replicas] : [...replicas];
  const first = list[0];
  if (first === undefined) throw new Error('createPartition needs at least one replica');
  const replicaLogs = new Map(list.map((b) => [b, createReplicaLog(b)]));
  const viewLog = (): ReplicaLog => replicaLogs.get(partition.view) as ReplicaLog;
  const partition: PartitionState = {
    topic,
    index,
    replicas: list,
    isr: [...list],
    pendingIsr: null,
    elr: [],
    lastKnownElr: [],
    leader: first,
    leaderEpoch: 0,
    partitionEpoch: 0,
    replicaLogs,
    followers: new Map(),
    view: first,
    get log() {
      return viewLog().log;
    },
    get logStartOffset() {
      return viewLog().logStartOffset;
    },
    get logEndOffset() {
      return viewLog().logEndOffset;
    },
    set logEndOffset(value: number) {
      viewLog().logEndOffset = value;
    },
    get highWatermark() {
      return viewLog().highWatermark;
    },
    set highWatermark(value: number) {
      viewLog().highWatermark = value;
    },
  };
  return partition;
}

/** The leader's replica log, or undefined while the partition is offline. */
export function leaderLog(partition: PartitionState): ReplicaLog | undefined {
  return partition.leader === null ? undefined : partition.replicaLogs.get(partition.leader);
}

/** Appends a record to a replica's log, keeping its leader-epoch cache in step. */
export function appendToReplica(replica: ReplicaLog, record: LogRecord): void {
  replica.log.push(record);
  replica.logEndOffset = record.offset + 1;
  const last = replica.epochs[replica.epochs.length - 1];
  if (last === undefined || last.epoch < record.leaderEpoch) {
    replica.epochs.push({ epoch: record.leaderEpoch, startOffset: record.offset });
  }
}

/**
 * Appends a record at the leader and returns it with its assigned offset and the leader epoch.
 * Throws if the partition has no leader (callers check first).
 */
export function appendRecord(
  partition: PartitionState,
  record: Omit<LogRecord, 'offset' | 'leaderEpoch'>,
): LogRecord {
  const leader = leaderLog(partition);
  if (leader === undefined)
    throw new Error(`${partitionName(partition.topic, partition.index)} has no leader`);
  // Frozen: a record never changes once it is in the log, so snapshots and other replicas can
  // share it instead of copying it, and nobody can rewrite it through them.
  const appended: LogRecord = Object.freeze({
    offset: leader.logEndOffset,
    ...record,
    leaderEpoch: partition.leaderEpoch,
  });
  appendToReplica(leader, appended);
  // The HW moves only once the in-sync followers have the record (protocols/replication.ts).
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

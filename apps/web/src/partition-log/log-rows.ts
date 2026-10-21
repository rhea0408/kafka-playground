/**
 * Rows of the Partition log tab (screen 05c, FR-6.1, B9): records interleaved with marker rows
 * for the high watermark and each group's committed offset, plus the log end. Pure: unit-tested
 * in Node.
 *
 * Every offset marker is a *boundary*, not a record. A marker for offset X sits between record
 * X−1 and record X:
 * - High watermark (HW): "the offset of the next record to be committed" — records below it are
 *   on every in-sync replica and readable by consumers (Kafka docs, Design §"Replication";
 *   ListOffsets with read_uncommitted returns it as the end offset). So the HW line sits after
 *   record HW−1; records at HW and above are on the leader only.
 * - Committed offset: the group stores the offset of the *next* record to consume, not the last
 *   one processed (KafkaConsumer#commitSync javadoc: "the committed offset should be the next
 *   message your application will consume, i.e. lastProcessedMessageOffset + 1"). A restarted
 *   consumer resumes exactly at the record below the marker.
 * - Log end offset (LEO): the offset the next appended record will get, so it is always the end.
 */

import type { LogRecord } from '@kp/engine';

export interface CommittedMark {
  readonly groupId: string;
  readonly offset: number;
}

export interface LogRowsInput {
  /** The records available (the snapshot's tail of the partition), oldest first. */
  readonly records: readonly LogRecord[];
  readonly logStartOffset: number;
  readonly highWatermark: number;
  readonly logEndOffset: number;
  /** One per group with a committed offset for this partition. */
  readonly committed: readonly CommittedMark[];
  /** Show at most this many records (the newest). */
  readonly limit: number;
  /** Exact key to keep (null = every record). Marker placement does not change. */
  readonly keyFilter?: string | null;
}

export type LogRow =
  | { readonly kind: 'record'; readonly record: LogRecord; readonly visible: boolean }
  | {
      readonly kind: 'hw';
      readonly offset: number;
      /** The boundary lies above the first row shown (the records just before it are hidden). */
      readonly beforeShown: boolean;
    }
  | {
      readonly kind: 'committed';
      readonly groupId: string;
      readonly offset: number;
      readonly beforeShown: boolean;
    }
  | { readonly kind: 'leo'; readonly offset: number };

export interface LogRows {
  readonly rows: readonly LogRow[];
  /** Records the snapshot has that `limit` leaves out (offered by "Show older"). */
  readonly olderAvailable: number;
  /** Offsets in the log older than anything the snapshot carries: [logStartOffset, firstKept). */
  readonly notLoaded: { readonly from: number; readonly to: number } | null;
}

type Marker = Exclude<LogRow, { kind: 'record' } | { kind: 'leo' }>;

/**
 * Records (filtered and limited), with each marker placed just before the first shown record
 * whose offset is ≥ the marker's offset, and after the last record when no such record is shown
 * (marker = LEO). Committed markers come before the HW marker at the same place (committed ≤ HW).
 * The LEO row always ends the list.
 */
export function buildLogRows(input: LogRowsInput): LogRows {
  const { records, highWatermark, logEndOffset, limit } = input;
  const keyFilter = input.keyFilter ?? null;
  const kept = Math.max(0, limit);
  const tail = kept === 0 ? [] : records.slice(-kept);
  const olderAvailable = records.length - tail.length;
  const firstLoaded = records[0]?.offset ?? logEndOffset;
  const notLoaded =
    firstLoaded > input.logStartOffset ? { from: input.logStartOffset, to: firstLoaded } : null;

  const shown = keyFilter === null ? tail : tail.filter((r) => r.key === keyFilter);
  const firstShown = shown[0]?.offset ?? logEndOffset;

  const markers: Marker[] = [
    ...[...input.committed]
      .sort((a, b) => a.offset - b.offset || a.groupId.localeCompare(b.groupId))
      .map((c): Marker => ({
        kind: 'committed',
        groupId: c.groupId,
        offset: c.offset,
        beforeShown: c.offset < firstShown,
      })),
    { kind: 'hw', offset: highWatermark, beforeShown: highWatermark < firstShown },
  ];
  // Stable sort: same offset keeps committed before HW.
  markers.sort((a, b) => a.offset - b.offset);

  const rows: LogRow[] = [];
  let m = 0;
  for (const record of shown) {
    while (m < markers.length && (markers[m]?.offset ?? Infinity) <= record.offset) {
      rows.push(markers[m] as Marker);
      m++;
    }
    rows.push({ kind: 'record', record, visible: record.offset < highWatermark });
  }
  while (m < markers.length) rows.push(markers[m++] as Marker);
  rows.push({ kind: 'leo', offset: logEndOffset });
  return { rows, olderAvailable, notLoaded };
}

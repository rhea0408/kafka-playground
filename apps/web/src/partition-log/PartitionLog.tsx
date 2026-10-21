import { formatSimTime, partitionName, type LogRecord } from '@kp/engine';
import * as Tooltip from '@radix-ui/react-tooltip';
import { memo, useEffect, useId, useMemo, useRef, useState, type ReactNode } from 'react';
import { formatCount } from '../canvas/live/format.ts';
import { FollowToggle } from '../events/FollowToggle.tsx';
import { useFollow } from '../events/use-follow.ts';
import { INPUT } from '../properties/controls.tsx';
import { Tip } from '../properties/FieldFrame.tsx';
import { useSimStore } from '../stores/sim-store.ts';
import { setLogTarget, useUiStore } from '../stores/ui-store.ts';
import { buildLogRows, type LogRow } from './log-rows.ts';
import { groupOffsets, resolveTarget, type GroupOffsets } from './partition-info.ts';

/** Records shown at first, and how many more each "Show older" adds (M2-decisions T11). */
export const LOG_PAGE = 50;

/** INPUT without its full width, for the two pickers that share a row. */
const SELECT = INPUT.replace('w-full ', '');

const SECTION_TITLE = 'text-[11px] font-bold text-status/70';
const FOCUSABLE_TIP =
  'cursor-help rounded-sm focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-primary';

/** Plain-language explanations (tooltips on the summary and the marker rows). */
export const EXPLAIN = {
  leo: (leo: number) =>
    `Log end offset (LEO) ${formatCount(leo)}: the offset the next record appended to this partition will get (last offset + 1). The log always ends here.`,
  hw: (hw: number) =>
    `High watermark (HW) ${formatCount(hw)}: the offset of the next record to be fully replicated. Records above this line (offsets below ${formatCount(hw)}) are on every in-sync replica, so consumers can read them; anything below the line is on the leader only. With one replica (until M4) the HW moves to the LEO on every append.`,
  committed: (groupId: string, offset: number) =>
    `Group ${groupId} committed offset ${formatCount(offset)}: the next record the group will consume, not the last one it read. A consumer of this group that restarts resumes at offset ${formatCount(offset)}. With auto-commit this marker trails the consumer's position and jumps forward when the next commit lands (every auto.commit.interval.ms, 5 s by default).`,
  start: (start: number) =>
    `Log start offset ${formatCount(start)}: the oldest offset still in the log. It moves up when retention deletes old segments (arrives in v0.4).`,
} as const;

/**
 * Partition log tab (screen 05c, FR-6.1, B9): a partition picker, a summary of the partition's
 * offsets and each group's committed offset, and the records with marker rows for the high
 * watermark, each committed offset and the log end (see log-rows.ts for where markers sit).
 */
export function PartitionLog() {
  const snapshot = useSimStore((s) => s.snapshot);
  const target = useUiStore((s) => s.logTarget);
  const resolved = resolveTarget(snapshot, target);

  if (snapshot === null || resolved === null) {
    return (
      <p className="p-4 text-[13px] text-status/70" data-testid="partition-log">
        Add a topic to see its partition log.
      </p>
    );
  }
  const { topic, partition } = resolved;
  const groups = groupOffsets(snapshot, topic.config.id, partition.index, partition.highWatermark);
  return (
    <Tooltip.Provider delayDuration={300}>
      {/* Keyed by topic life and partition: the row limit, filter and Follow start over. */}
      <PartitionLogView
        key={`${String(topic.topicId)}:${String(partition.index)}`}
        topicId={topic.config.id}
        topicIds={snapshot.topics.map((t) => t.config.id)}
        partitionCount={topic.partitions.length}
        partition={partition.index}
        leader={partition.leader}
        logStartOffset={partition.logStartOffset}
        highWatermark={partition.highWatermark}
        logEndOffset={partition.logEndOffset}
        records={partition.records}
        groups={groups}
      />
    </Tooltip.Provider>
  );
}

interface ViewProps {
  readonly topicId: string;
  readonly topicIds: readonly string[];
  readonly partitionCount: number;
  readonly partition: number;
  readonly leader: string;
  readonly logStartOffset: number;
  readonly highWatermark: number;
  readonly logEndOffset: number;
  readonly records: readonly LogRecord[];
  readonly groups: readonly GroupOffsets[];
}

function PartitionLogView(props: ViewProps) {
  const { topicId, partition, highWatermark, logEndOffset, records, groups } = props;
  const [limit, setLimit] = useState(LOG_PAGE);
  const [keyFilter, setKeyFilter] = useState('');
  const reducedMotion = useUiStore((s) => s.reducedMotion);
  const listId = useId();

  const committedKey = groups.map((g) => `${g.groupId}=${String(g.committed)}`).join(',');
  const log = useMemo(
    () =>
      buildLogRows({
        records,
        logStartOffset: props.logStartOffset,
        highWatermark,
        logEndOffset,
        committed: groups.flatMap((g) =>
          g.committed === null ? [] : [{ groupId: g.groupId, offset: g.committed }],
        ),
        limit,
        keyFilter: keyFilter === '' ? null : keyFilter,
      }),
    // `groups` is rebuilt every frame; `committedKey` holds what matters from it.
    [records, props.logStartOffset, highWatermark, logEndOffset, committedKey, limit, keyFilter],
  );

  const scroller = useRef<HTMLDivElement>(null);
  const follow = useFollow(scroller, log);

  // B9: records newer than the last render slide up into place.
  const seen = useRef<number | null>(null);
  const previousEnd = seen.current ?? logEndOffset;
  useEffect(() => {
    seen.current = logEndOffset;
  }, [logEndOffset]);
  const enterClass = reducedMotion ? 'kp-cell-fade' : 'kp-row-in';

  const name = partitionName(topicId, partition);
  return (
    <div className="flex h-full min-h-0 flex-col" data-testid="partition-log">
      <div className="flex shrink-0 flex-col gap-3 px-4 pt-4 pb-2">
        <Picker {...props} />
        <Summary {...props} />
        <div className="flex h-8 w-[328px] items-center gap-2">
          <label htmlFor="pl-key" className="shrink-0 text-[12px]">
            Filter key
          </label>
          <input
            id="pl-key"
            className={`${INPUT} h-7 min-w-0 flex-1 font-mono`}
            value={keyFilter}
            placeholder="any key"
            aria-describedby="pl-key-help"
            onChange={(event) => {
              setKeyFilter(event.target.value);
            }}
          />
          <FollowToggle
            following={follow.following}
            onChange={follow.setFollowing}
            controls={listId}
          />
        </div>
        <p id="pl-key-help" className="sr-only">
          Shows only records with exactly this key. Markers stay in place.
        </p>
      </div>
      <div
        ref={scroller}
        id={listId}
        tabIndex={0}
        role="region"
        aria-label={`Records of ${name}`}
        className="min-h-0 flex-1 overflow-y-auto px-4 pb-4 focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-primary"
        {...follow.handlers}
      >
        {log.olderAvailable > 0 && (
          <button
            type="button"
            className="mb-1 h-7 w-[328px] rounded-sm border border-border bg-surface text-[12px] hover:bg-muted focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-primary"
            onClick={() => {
              follow.setFollowing(false);
              setLimit(limit + LOG_PAGE);
            }}
          >
            Show {formatCount(Math.min(LOG_PAGE, log.olderAvailable))} older records
          </button>
        )}
        {log.olderAvailable === 0 && log.notLoaded !== null && (
          <p className="mb-1 w-[328px] text-[11px] text-status/70">
            Offsets {formatCount(log.notLoaded.from)}–{formatCount(log.notLoaded.to - 1)} are older
            than the {formatCount(records.length)} records this panel keeps per partition.
          </p>
        )}
        <table className="w-[328px] table-fixed border-collapse text-[12px]">
          <caption className="sr-only">
            Records of {name}, oldest first, with lines for the committed offsets, the high
            watermark and the log end.
          </caption>
          <thead>
            <tr className="h-6 text-left text-[11px] text-status/70">
              <th scope="col" className="w-[52px] font-normal">
                offset
              </th>
              <th scope="col" className="w-10 font-normal">
                key
              </th>
              <th scope="col" className="font-normal">
                value
              </th>
              <th scope="col" className="w-[66px] font-normal">
                time
              </th>
              <th scope="col" className="w-[58px] font-normal">
                status
              </th>
            </tr>
          </thead>
          <tbody>
            {log.rows.map((row) => (
              <LogRowView
                key={rowKey(row)}
                row={row}
                enter={
                  follow.following && row.kind === 'record' && row.record.offset >= previousEnd
                    ? enterClass
                    : null
                }
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function rowKey(row: LogRow): string {
  switch (row.kind) {
    case 'record':
      return `r${String(row.record.offset)}`;
    case 'committed':
      return `c:${row.groupId}`;
    case 'hw':
      return 'hw';
    case 'leo':
      return 'leo';
  }
}

/** Topic and partition pickers in one 328 × 32 row (ui-spec "partition picker"). */
function Picker({ topicId, topicIds, partitionCount, partition }: ViewProps) {
  return (
    <div className="flex h-8 w-[328px] items-center gap-2 text-[12px]">
      <label htmlFor="pl-topic">Topic</label>
      <select
        id="pl-topic"
        className={`${SELECT} h-8 min-w-0 flex-1 font-mono`}
        value={topicId}
        onChange={(event) => {
          setLogTarget({ topicId: event.target.value, partition: 0 });
        }}
      >
        {topicIds.map((id) => (
          <option key={id} value={id}>
            {id}
          </option>
        ))}
      </select>
      <label htmlFor="pl-partition">Partition</label>
      <select
        id="pl-partition"
        className={`${SELECT} h-8 w-[76px] font-mono`}
        value={partition}
        onChange={(event) => {
          setLogTarget({ topicId, partition: Number(event.target.value) });
        }}
      >
        {Array.from({ length: partitionCount }, (_, i) => (
          <option key={i} value={i}>
            P{i}
          </option>
        ))}
      </select>
    </div>
  );
}

function Offset({ label, value, tip }: { label: string; value: number; tip: string }) {
  return (
    <Tip text={tip}>
      <div
        tabIndex={0}
        className={`flex h-12 w-[104px] flex-col justify-center rounded-md bg-muted px-2 ${FOCUSABLE_TIP}`}
        aria-label={`${label} ${formatCount(value)}. ${tip}`}
      >
        <div className="text-[10px] text-status/70" aria-hidden="true">
          {label}
        </div>
        <div className="font-mono text-[13px] font-semibold" aria-hidden="true">
          {formatCount(value)}
        </div>
      </div>
    </Tip>
  );
}

const dash = (n: number | null): string => (n === null ? '-' : formatCount(n));

/** Leader, offsets and each group's committed offset (05c summary). */
function Summary(props: ViewProps) {
  const { leader, logStartOffset, highWatermark, logEndOffset, groups } = props;
  return (
    <section aria-labelledby="pl-summary" className="flex flex-col gap-2">
      <h3 id="pl-summary" className="sr-only">
        Summary
      </h3>
      <p className="text-[12px]">
        Leader <span className="font-mono">{leader}</span> · replicas{' '}
        <span className="font-mono">{leader}</span> · ISR{' '}
        <span className="font-mono">{leader}</span>{' '}
        <span className="text-status/70">(one replica until M4)</span>
      </p>
      <div className="grid grid-cols-[repeat(3,104px)] gap-2" role="group" aria-label="Offsets">
        <Offset label="Log end (LEO)" value={logEndOffset} tip={EXPLAIN.leo(logEndOffset)} />
        <Offset label="High watermark" value={highWatermark} tip={EXPLAIN.hw(highWatermark)} />
        <Offset label="Log start" value={logStartOffset} tip={EXPLAIN.start(logStartOffset)} />
      </div>
      <div className="flex flex-col gap-0.5">
        <h4 className={SECTION_TITLE}>CONSUMER GROUPS</h4>
        {groups.length === 0 ? (
          <p className="text-[12px] text-status/70">No group reads this partition yet.</p>
        ) : (
          <ul className="flex flex-col gap-0.5 text-[12px]" data-testid="pl-groups">
            {groups.map((g) => (
              <li key={g.groupId} data-group={g.groupId}>
                <span className="font-mono font-semibold">{g.groupId}</span> committed{' '}
                <span className="font-mono">{dash(g.committed)}</span>
                {g.committed === null ? (
                  <span className="text-status/70"> (no commit yet)</span>
                ) : (
                  <>
                    {' '}
                    · lag <span className="font-mono">{formatCount(g.lag ?? 0)}</span>
                  </>
                )}
                {g.members.map((m) => (
                  <span key={m.consumerId} className="text-status/70">
                    {' '}
                    · <span className="font-mono">{m.consumerId}</span> position{' '}
                    <span className="font-mono">{dash(m.position)}</span>
                    {m.status === 'stopped' && ' (stopped)'}
                  </span>
                ))}
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}

function MarkerRow(props: {
  kind: 'hw' | 'committed' | 'leo';
  tip: string;
  line: string;
  children: ReactNode;
}) {
  return (
    <tr className="h-[22px]" data-marker={props.kind}>
      <td colSpan={5} className="p-0">
        <Tip text={props.tip}>
          <div
            tabIndex={0}
            aria-label={props.tip}
            className={`flex h-[22px] items-center gap-1.5 border-t-2 px-1 text-[11px] ${props.line} ${FOCUSABLE_TIP}`}
          >
            {props.children}
          </div>
        </Tip>
      </td>
    </tr>
  );
}

/** Same row content: records by their frozen record object, markers by what they show. */
function sameRow(a: LogRow, b: LogRow): boolean {
  if (a.kind === 'record' || b.kind === 'record') {
    return (
      a.kind === 'record' && b.kind === 'record' && a.record === b.record && a.visible === b.visible
    );
  }
  if (a.kind === 'leo' || b.kind === 'leo') return a.kind === b.kind && a.offset === b.offset;
  return (
    a.kind === b.kind &&
    a.offset === b.offset &&
    a.beforeShown === b.beforeShown &&
    (a.kind !== 'committed' || (b.kind === 'committed' && a.groupId === b.groupId))
  );
}

/**
 * One row: a record (328 × 28) or a marker (328 × 22). Memoised with `sameRow`, so a frame only
 * renders the rows that are new or whose marker moved.
 */
const LogRowView = memo(
  function LogRowView(props: { row: LogRow; enter: string | null }) {
    const { row } = props;
    // Decided at mount, so the next frame's render can't cut the slide-in short.
    const [enter] = useState(props.enter);
    switch (row.kind) {
      case 'record': {
        const { record, visible } = row;
        return (
          <tr
            className={`h-7 border-b border-divider font-mono ${visible ? '' : 'text-status/70 italic'} ${enter ?? ''}`}
            data-offset={record.offset}
            title={
              visible
                ? undefined
                : 'On the leader only: not yet on every in-sync replica, so consumers can’t read it.'
            }
          >
            <td className="truncate">{record.offset}</td>
            <td className="truncate">
              {record.key ?? (
                <span className="text-status/70" title="null key">
                  ∅<span className="sr-only">null key</span>
                </span>
              )}
            </td>
            <td className="truncate" title={record.value}>
              {record.value}
            </td>
            <td className="truncate text-[11px] text-status/70">
              {formatSimTime(record.timestamp)}
            </td>
            <td className="truncate font-sans text-[11px]">
              {visible ? 'readable' : 'leader only'}
            </td>
          </tr>
        );
      }
      case 'hw':
        return (
          <MarkerRow kind="hw" tip={EXPLAIN.hw(row.offset)} line="border-dashed border-primary">
            <span className="shrink-0 font-semibold whitespace-nowrap">
              high watermark {formatCount(row.offset)}
            </span>
            <span className="truncate text-status/70">
              {row.beforeShown ? '· above the rows shown' : '· consumers read above this line'}
            </span>
          </MarkerRow>
        );
      case 'committed':
        return (
          <MarkerRow
            kind="committed"
            tip={EXPLAIN.committed(row.groupId, row.offset)}
            line="border-solid border-status bg-muted"
          >
            <span className="truncate font-semibold whitespace-nowrap">
              {row.groupId} committed {formatCount(row.offset)}
            </span>
            <span className="truncate text-status/70">
              {row.beforeShown
                ? '· older than the rows shown'
                : `· next read is ${formatCount(row.offset)}`}
            </span>
          </MarkerRow>
        );
      case 'leo':
        return (
          <MarkerRow kind="leo" tip={EXPLAIN.leo(row.offset)} line="border-dotted border-border">
            <span className="shrink-0 font-semibold whitespace-nowrap">
              log end {formatCount(row.offset)}
            </span>
            <span className="truncate text-status/70">· the next record gets this offset</span>
          </MarkerRow>
        );
    }
  },
  (prev, next) => sameRow(prev.row, next.row),
);

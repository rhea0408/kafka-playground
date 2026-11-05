/**
 * Live state inside the canvas nodes (M2-T8, FR-1.4): producer counters, partition rows (LEO, HW,
 * log cells, leader chip), the consumer row (status, offsets, lag badge) and the consumer's
 * Kill / Restart toolbar. Each part subscribes to its own slice of the snapshot (use-live.ts), so
 * a frame re-renders only the parts whose data changed. Everything updates straight from
 * snapshots and never waits for message dots (animations.md, ADR 0008).
 */

import type { ConsumerSnapshot, GroupSnapshot, LogRecord } from '@kp/engine';
import { NodeToolbar, Position } from '@xyflow/react';
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
  type FocusEvent as ReactFocusEvent,
  type RefObject,
} from 'react';
import { useActions } from '../../actions/actions-context.tsx';
import { useUiStore } from '../../stores/ui-store.ts';
import {
  compactCount,
  formatCount,
  keyLetter,
  LAG_COUNT_MS,
  LAG_DANGER_THRESHOLD,
  LOG_CELLS,
  summarizeConsumer,
  visibleCells,
} from './format.ts';
import {
  useConsumer,
  useConsumerStatus,
  useCountUp,
  useGroup,
  usePartition,
  useProducerStats,
} from './use-live.ts';

const useReducedMotion = (): boolean => useUiStore((s) => s.reducedMotion);

// ── Producer ──────────────────────────────────────────────────────────────────────────────────

function Stat({ label, value, danger }: { label: string; value: number; danger?: boolean }) {
  return (
    <div className="min-w-0 leading-none" title={`${formatCount(value)} ${label}`}>
      <div
        className={`truncate font-mono text-[10px] font-semibold ${danger === true ? 'text-danger-text' : ''}`}
        data-stat={label}
      >
        {compactCount(value)}
      </div>
      <div className="mt-0.5 text-[9px] text-status/70">{label}</div>
    </div>
  );
}

/**
 * Sent / acked / failed (ProducerStats): records handed to the producer, records the broker
 * acknowledged (per `acks`), and records the producer was told failed.
 */
export function ProducerCounters({ producerId }: { producerId: string }) {
  const stats = useProducerStats(producerId);
  const sent = stats?.sent ?? 0;
  const acked = stats?.acked ?? 0;
  const failed = stats?.failed ?? 0;
  return (
    <div
      className="mt-1 grid grid-cols-3 gap-1"
      data-slot="producer-stats"
      aria-label={`${formatCount(sent)} sent, ${formatCount(acked)} acked, ${formatCount(failed)} failed`}
      role="group"
    >
      <Stat label="sent" value={sent} />
      <Stat label="acked" value={acked} />
      <Stat label="failed" value={failed} danger={failed > 0} />
    </div>
  );
}

// ── Topic partition row ───────────────────────────────────────────────────────────────────────

function LogCell({
  record,
  committed,
  animate,
}: {
  record: LogRecord;
  committed: boolean;
  animate: boolean;
}) {
  const reduced = useReducedMotion();
  // A4 runs once, when the cell first appears; later renders must not restart or cut it.
  const [entering] = useState(animate);
  const motion = entering ? (reduced ? 'kp-cell-fade' : 'kp-cell-in') : '';
  const key = record.key === null ? 'no key' : `key ${record.key}`;
  return (
    <span
      className={`kp-cell ${committed ? 'border-solid' : 'border-dashed'} ${motion}`}
      data-offset={record.offset}
      data-committed={committed}
      title={`Offset ${String(record.offset)} · ${key}${committed ? '' : ' · above the high watermark: not readable by consumers yet'}`}
    >
      {keyLetter(record.key)}
    </span>
  );
}

/**
 * The live part of a partition row: the last log cells (newest on the right, A4), LEO and HW, and
 * the leader chip. LEO is the offset the next record gets; HW is the end of what consumers can
 * read (Kafka docs, Design §"Replication").
 */
export function PartitionLive({ topicId, index }: { topicId: string; index: number }) {
  const partition = usePartition(topicId, index);
  // Highest offset shown in the previous render: cells above it are new and slide in (A4). Null on
  // the first render, so a node that appears with records (or after a full frame) doesn't animate.
  const seen = useRef<number | null>(null);
  const leo = partition?.logEndOffset ?? 0;
  const hw = partition?.highWatermark ?? 0;
  const cells = partition === undefined ? [] : visibleCells(partition.records, hw, LOG_CELLS);
  const prev = seen.current;
  // A log that went backwards is a new log (Reset): nothing in it counts as "just appended".
  const continues = prev !== null && leo - 1 >= prev;
  useEffect(() => {
    seen.current = leo - 1;
  }, [leo]);

  return (
    <>
      <div className="flex min-w-0 flex-1 flex-col justify-center gap-0.5">
        <div className="flex h-[18px] gap-[3px]" data-slot="log-cells" aria-hidden="true">
          {cells.map(({ record, committed }) => (
            <LogCell
              key={record.offset}
              record={record}
              committed={committed}
              animate={continues && record.offset > prev}
            />
          ))}
        </div>
        <span
          className="truncate font-mono text-[10px] text-status/70"
          data-slot="partition-log"
          title="LEO (log end offset): the offset the next record gets. HW (high watermark): consumers can read records below it."
        >
          LEO {formatCount(leo)} · HW {formatCount(hw)}
        </span>
      </div>
      {partition !== undefined && (
        <span
          className="shrink-0 rounded-sm bg-muted px-1 py-0.5 font-mono text-[10px] font-semibold"
          data-slot="leader"
          title={`Leader: broker ${partition.leader}. Producers write to and consumers read from the leader.`}
          aria-label={`Leader: broker ${partition.leader}`}
        >
          L {partition.leader}
        </span>
      )}
    </>
  );
}

// ── Consumer row ──────────────────────────────────────────────────────────────────────────────

/**
 * A5: total lag. The number counts to its new value over 300 ms (instant under reduced motion);
 * above `LAG_DANGER_THRESHOLD` the badge turns danger and shows a warning sign, and the number is
 * always there, so the color is never the only signal (NFR-7.1). Screen readers get the real
 * value, not the counting one.
 */
export function LagBadge({ lag }: { lag: number | null }) {
  const reduced = useReducedMotion();
  const shown = useCountUp(lag ?? 0, LAG_COUNT_MS, reduced);
  const danger = lag !== null && lag > LAG_DANGER_THRESHOLD;
  const label =
    lag === null
      ? 'Lag unknown: the group has no committed offset yet'
      : `Lag ${formatCount(lag)}${danger ? `, above ${String(LAG_DANGER_THRESHOLD)}` : ''}`;
  return (
    <span
      className={`ml-auto inline-flex shrink-0 items-center gap-0.5 rounded-lg px-1.5 py-0.5 font-mono text-[10px] font-semibold whitespace-nowrap ${
        danger ? 'bg-danger-bg text-danger-text' : 'bg-muted'
      }`}
      data-slot="lag"
      data-lag={lag ?? ''}
      data-danger={danger}
      title={`${label}. Lag = end offset (high watermark) − committed offset, summed over partitions.`}
    >
      {danger && <span aria-hidden="true">⚠&#xFE0E;</span>}
      <span aria-hidden="true" data-testid="lag-number">
        lag {lag === null ? '-' : formatCount(shown)}
      </span>
      <span className="sr-only">{label}</span>
    </span>
  );
}

/** Icon per consumer status; the status word is always shown next to it (never icon alone). */
const STATUS_ICON: { readonly [S in ConsumerSnapshot['status']]: string } = {
  joining: '↻',
  'left-group': '⚠',
  stable: '▶',
  idle: '◇',
  stopped: '■',
  crashed: '✕',
};

function ConsumerStatus({ status }: { status: ConsumerSnapshot['status'] }) {
  return (
    <span className="inline-flex shrink-0 items-center gap-0.5 text-[11px]" data-slot="status">
      <span aria-hidden="true">{STATUS_ICON[status]}</span>
      {status}
    </span>
  );
}

/** Whether the consumer process is running (any status but stopped / crashed). */
export const isRunning = (status: ConsumerSnapshot['status']): boolean =>
  status !== 'stopped' && status !== 'crashed';

const dash = (n: number | null): string => (n === null ? '-' : formatCount(n));

/** A12: how long a partition chip takes to fade out (revoked) or in (assigned). */
export const CHIP_FADE_MS = 200;

interface ChipState {
  readonly label: string;
  readonly phase: 'in' | 'out' | 'steady';
}

/**
 * A12 (animations.md): a member's partition chips. A chip that disappears from the snapshot
 * (revoked or lost) stays for `CHIP_FADE_MS` while it fades out; a new one fades in. Eager
 * rebalances revoke everything, so every chip fades; cooperative ones only the partitions that
 * move. Driven by snapshots (which follow PartitionsRevoked / PartitionsAssigned at once), not by
 * dots. Instant under reduced motion. UI timing, not scaled by sim speed.
 */
export function PartitionChips({ labels }: { labels: readonly string[] }) {
  const reduced = useReducedMotion();
  const [chips, setChips] = useState<readonly ChipState[]>(() =>
    labels.map((label) => ({ label, phase: 'steady' })),
  );
  const key = labels.join(' ');
  useEffect(() => {
    const now = new Set(key === '' ? [] : key.split(' '));
    setChips((prev) => {
      const prevLabels = new Set(prev.map((c) => c.label));
      const kept = prev
        .filter((c) => !reduced || now.has(c.label))
        .map((c): ChipState => {
          if (now.has(c.label)) return c.phase === 'out' ? { ...c, phase: 'in' } : c;
          return { ...c, phase: 'out' };
        });
      const added = [...now]
        .filter((l) => !prevLabels.has(l))
        .map((label): ChipState => ({ label, phase: reduced ? 'steady' : 'in' }));
      return [...kept, ...added];
    });
    if (reduced) return undefined;
    const timer = setTimeout(() => {
      setChips((prev) => prev.filter((c) => c.phase !== 'out'));
    }, CHIP_FADE_MS);
    return () => {
      clearTimeout(timer);
    };
  }, [key, reduced]);
  return (
    <span className="flex min-w-0 shrink-0 gap-0.5" data-slot="partition-chips">
      {chips.map((c) => (
        <span
          key={c.label}
          className={`rounded-sm bg-muted px-1 font-mono text-[10px] font-semibold ${
            c.phase === 'in' ? 'kp-chip-in' : c.phase === 'out' ? 'kp-chip-out' : ''
          }`}
          data-chip={c.label}
          data-phase={c.phase}
          aria-hidden={c.phase === 'out'}
        >
          {c.label}
        </span>
      ))}
    </span>
  );
}

/** "P0", or "orders-0" when the member reads more than one topic. */
export function chipLabels(consumer: ConsumerSnapshot): string[] {
  const topics = new Set(consumer.partitions.map((p) => p.topic)).size;
  return consumer.partitions.map((p) =>
    topics > 1 ? `${p.topic}-${String(p.partition)}` : `P${String(p.partition)}`,
  );
}

/** The flat muted chip of an idle member (icon + words, never color alone). */
function IdleChip() {
  return (
    <span
      className="inline-flex shrink-0 items-center gap-0.5 rounded-sm bg-muted px-1 text-[10px]"
      data-slot="idle"
      title="This member got no partition in the current generation: the group has more members than partitions it can share out. It stays idle until a rebalance gives it one."
    >
      <span aria-hidden="true">◇</span>Idle: no partition
    </span>
  );
}

/** Live part of a member row: status, owned partitions, position, committed offset, lag. */
export function ConsumerLive({ consumerId }: { consumerId: string }) {
  const consumer = useConsumer(consumerId);
  const s = consumer === undefined ? undefined : summarizeConsumer(consumer);
  const many = s !== undefined && s.partitions > 1;
  const total = many ? 'Σ ' : '';
  const status =
    consumer === undefined
      ? ''
      : `${consumer.status}${consumer.stopping ? ' (stopping)' : ''}${
          consumer.generation === null ? '' : ` in generation ${String(consumer.generation)}`
        }`;
  const title =
    consumer === undefined || s === undefined
      ? `${consumerId}: not in the simulation yet`
      : [
          `${consumerId} · ${status} · ${formatCount(consumer.consumed)} records consumed`,
          `Position: next offset to read. Committed: saved for the group.${
            many ? ` Totals over ${String(s.partitions)} partitions:` : ''
          }`,
          ...s.breakdown,
        ].join('\n');
  return (
    <div
      className="flex min-w-0 flex-1 flex-col justify-center gap-0.5"
      title={title}
      data-status={consumer?.status ?? 'unknown'}
    >
      <div className="flex min-w-0 items-center gap-1.5">
        <span className="truncate font-medium">{consumerId}</span>
        {consumer !== undefined && <ConsumerStatus status={consumer.status} />}
        {s !== undefined && <LagBadge lag={s.lag} />}
      </div>
      <div className="flex min-w-0 items-center gap-1 font-mono text-[10px] text-status/70">
        {consumer?.status === 'idle' ? (
          <IdleChip />
        ) : (
          consumer !== undefined && <PartitionChips labels={chipLabels(consumer)} />
        )}
        <span className="min-w-0 truncate" data-slot="offsets">
          {s === undefined || s.partitions === 0
            ? ''
            : `${total}pos ${dash(s.position)} · ${total}committed ${dash(s.committed)}`}
        </span>
      </div>
      {s !== undefined && s.breakdown.length > 0 && (
        <ul className="sr-only">
          {s.breakdown.map((line) => (
            <li key={line}>{line}</li>
          ))}
        </ul>
      )}
    </div>
  );
}

/** The coordinator's state, as the header chip says it (icon + words, never color alone). */
export function groupStateLabel(group: GroupSnapshot | undefined): {
  readonly icon: string;
  readonly text: string;
} {
  switch (group?.state) {
    case undefined:
      return { icon: '…', text: 'Joining' };
    case 'Stable':
      return { icon: '✓', text: 'Stable' };
    case 'PreparingRebalance':
    case 'CompletingRebalance':
      return { icon: '↻', text: 'Rebalancing…' };
    case 'Empty':
      return { icon: '∅', text: 'Empty' };
    case 'Dead':
      return { icon: '✕', text: 'Dead' };
  }
}

/**
 * Live part of the group header: the state chip ("Rebalancing…" from RebalanceStarted until the
 * group is Stable again, A12) and the generation.
 */
export function GroupHeaderLive({ groupId }: { groupId: string }) {
  const group = useGroup(groupId);
  const state = groupStateLabel(group);
  const detail =
    group === undefined
      ? 'The group coordinator creates the group when its first member joins.'
      : `Coordinator state ${group.state}, generation ${String(group.generation)}${
          group.protocol === null ? '' : `, assignor ${group.protocol}`
        }. Lag over the group: ${formatCount(group.totalLag)}.`;
  return (
    <span
      className="ml-auto inline-flex shrink-0 items-center gap-0.5 rounded-sm bg-muted px-1 py-0.5 text-[10px] font-semibold"
      data-slot="group-state"
      data-state={group?.state ?? 'none'}
      title={detail}
    >
      <span aria-hidden="true">{state.icon}</span>
      {state.text}
    </span>
  );
}

/** Second header line: generation and the group's total lag (M3-T11). */
export function GroupHeaderStats({ groupId }: { groupId: string }) {
  const group = useGroup(groupId);
  if (group === undefined) return null;
  const danger = group.totalLag > LAG_DANGER_THRESHOLD;
  return (
    <span className="font-mono text-[10px] font-normal text-status/70">
      <span data-slot="generation">gen {group.generation}</span>
      {' · '}
      <span
        data-slot="group-lag"
        data-lag={group.totalLag}
        className={danger ? 'font-semibold text-danger-text' : ''}
        title="Lag over the whole group: sum of HW − committed offset per partition."
      >
        {danger ? '⚠\uFE0E ' : ''}group lag {compactCount(group.totalLag)}
      </span>
    </span>
  );
}

// ── Consumer toolbar (Kill / Restart) ─────────────────────────────────────────────────────────

/** Short delay before a hover toolbar hides, so the pointer can travel from node to toolbar. */
const HOVER_HIDE_MS = 150;

export interface HoverToolbar {
  readonly visible: boolean;
  readonly rootRef: RefObject<HTMLDivElement>;
  readonly toolbarRef: RefObject<HTMLDivElement>;
  readonly onPointerEnter: () => void;
  readonly onPointerLeave: () => void;
  readonly onToolbarFocus: () => void;
  readonly onToolbarBlur: (event: ReactFocusEvent) => void;
  readonly onToolbarKeyDown: (event: ReactKeyboardEvent) => void;
}

/**
 * Visibility and keyboard access for a node's hover toolbar. Shown while the node is hovered,
 * selected or focused (keyboard). Tab on the focused node jumps into the toolbar (React Flow
 * renders it in a portal after all nodes, so normal Tab order would reach it late); Escape or
 * Shift+Tab from its first button goes back to the node.
 */
export function useHoverToolbar(selected: boolean): HoverToolbar {
  const rootRef = useRef<HTMLDivElement>(null);
  const toolbarRef = useRef<HTMLDivElement>(null);
  const [hovered, setHovered] = useState(false);
  const [focused, setFocused] = useState(false);
  const hideTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const wrapper = useCallback(
    (): HTMLElement | null => rootRef.current?.closest<HTMLElement>('.react-flow__node') ?? null,
    [],
  );
  const inside = useCallback(
    (target: EventTarget | null): boolean =>
      target instanceof Node &&
      (wrapper()?.contains(target) === true || toolbarRef.current?.contains(target) === true),
    [wrapper],
  );

  useEffect(() => {
    const el = wrapper();
    if (el === null) return undefined;
    const onFocusIn = (): void => {
      setFocused(true);
    };
    const onFocusOut = (event: FocusEvent): void => {
      if (!inside(event.relatedTarget)) setFocused(false);
    };
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key !== 'Tab' || event.shiftKey || event.target !== el) return;
      const first = toolbarRef.current?.querySelector('button');
      if (first === null || first === undefined) return;
      event.preventDefault();
      first.focus();
    };
    el.addEventListener('focusin', onFocusIn);
    el.addEventListener('focusout', onFocusOut);
    el.addEventListener('keydown', onKeyDown);
    return () => {
      el.removeEventListener('focusin', onFocusIn);
      el.removeEventListener('focusout', onFocusOut);
      el.removeEventListener('keydown', onKeyDown);
    };
  }, [wrapper, inside]);

  useEffect(
    () => () => {
      clearTimeout(hideTimer.current);
    },
    [],
  );

  return {
    visible: hovered || focused || selected,
    rootRef,
    toolbarRef,
    onPointerEnter: () => {
      clearTimeout(hideTimer.current);
      setHovered(true);
    },
    onPointerLeave: () => {
      clearTimeout(hideTimer.current);
      hideTimer.current = setTimeout(() => {
        setHovered(false);
      }, HOVER_HIDE_MS);
    },
    onToolbarFocus: () => {
      setFocused(true);
    },
    onToolbarBlur: (event) => {
      if (!inside(event.relatedTarget)) setFocused(false);
    },
    onToolbarKeyDown: (event) => {
      // Keys in the toolbar are the toolbar's: React would otherwise bubble them (through the
      // portal) to the node wrapper, where Enter / Escape change the selection.
      event.stopPropagation();
      const first = toolbarRef.current?.querySelector('button');
      const shiftTabOut = event.key === 'Tab' && event.shiftKey && event.target === first;
      if (event.key === 'Escape' || shiftTabOut) {
        event.preventDefault();
        wrapper()?.focus();
      }
    },
  };
}

const TOOL_BUTTON =
  'flex h-7 items-center gap-1.5 rounded px-2 text-xs text-surface hover:bg-surface/15 focus-visible:outline-2 focus-visible:outline-surface';

/** What each action does, for its tooltip (M3 Q8): the difference matters for the lesson. */
export const MEMBER_ACTION_HELP = {
  kill: 'Crash the process: no commit, no LeaveGroup. The group only notices after session.timeout.ms, then rebalances; records since the last commit are read again.',
  stop: 'Graceful close(): finish the current batch, commit (auto-commit), send LeaveGroup. The group rebalances at once.',
  restart:
    'Start a new consumer process. It joins as a new member and resumes from the committed offsets.',
} as const;

/** Kill / Stop / Restart for one member (engine commands KillConsumer, StopConsumer, RestartConsumer). */
function MemberActions({ consumerId }: { consumerId: string }) {
  const actions = useActions();
  const status = useConsumerStatus(consumerId);
  if (status === undefined) return null;
  const label = (
    <span className="px-1 font-mono text-[11px] text-surface/80" aria-hidden="true">
      {consumerId}
    </span>
  );
  if (!isRunning(status)) {
    return (
      <>
        {label}
        <button
          type="button"
          className={TOOL_BUTTON}
          title={MEMBER_ACTION_HELP.restart}
          aria-label={`Restart consumer ${consumerId}`}
          onClick={() => {
            void actions.restartConsumer(consumerId);
          }}
        >
          <span aria-hidden="true">▶</span>Restart
        </button>
      </>
    );
  }
  return (
    <>
      {label}
      <button
        type="button"
        className={TOOL_BUTTON}
        title={MEMBER_ACTION_HELP.kill}
        aria-label={`Kill consumer ${consumerId}`}
        onClick={() => {
          void actions.killConsumer(consumerId);
        }}
      >
        <span aria-hidden="true">✕</span>Kill (crash)
      </button>
      <button
        type="button"
        className={TOOL_BUTTON}
        title={MEMBER_ACTION_HELP.stop}
        aria-label={`Stop consumer ${consumerId}`}
        onClick={() => {
          void actions.stopConsumer(consumerId);
        }}
      >
        <span aria-hidden="true">■</span>Stop (leave)
      </button>
    </>
  );
}

/**
 * The group node's hover toolbar (M3-T9): actions for the member whose row is under the pointer
 * (or focused, or the first member). Dark like the ui-spec hover toolbar (status color, 36 high).
 */
export function ConsumerToolbar({
  consumerId,
  toolbar,
}: {
  consumerId: string | undefined;
  toolbar: HoverToolbar;
}) {
  return (
    <NodeToolbar isVisible={toolbar.visible} position={Position.Top} offset={6}>
      <div
        ref={toolbar.toolbarRef}
        role="toolbar"
        aria-label={
          consumerId === undefined ? 'Consumer actions' : `Actions for consumer ${consumerId}`
        }
        className="nodrag nopan flex h-9 items-center gap-1 rounded-md bg-status px-1"
        onPointerEnter={toolbar.onPointerEnter}
        onPointerLeave={toolbar.onPointerLeave}
        onFocus={toolbar.onToolbarFocus}
        onBlur={toolbar.onToolbarBlur}
        onKeyDown={toolbar.onToolbarKeyDown}
        onClick={(event) => {
          event.stopPropagation();
        }}
      >
        {consumerId !== undefined && <MemberActions consumerId={consumerId} />}
      </div>
    </NodeToolbar>
  );
}

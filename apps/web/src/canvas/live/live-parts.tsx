/**
 * Live state inside the canvas nodes (M2-T8, FR-1.4): producer counters, partition rows (LEO, HW,
 * log cells, leader chip), the consumer row (status, offsets, lag badge) and the consumer's
 * Kill / Restart toolbar. Each part subscribes to its own slice of the snapshot (use-live.ts), so
 * a frame re-renders only the parts whose data changed. Everything updates straight from
 * snapshots and never waits for message dots (animations.md, ADR 0008).
 */

import type { ConsumerSnapshot, GroupSnapshot, LogRecord, PartitionSnapshot } from '@kp/engine';
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
  useHeldByCoordinator,
  usePartition,
  useProducerStats,
  useSimTime,
  type HeldByCoordinator,
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
      {partition !== undefined && <ReplicaColumn partition={partition} />}
    </>
  );
}

/**
 * The right column of a partition row (M4-T11): the leader badge (A7 flip on change) and, with
 * RF > 1, one chip per replica: solid while in sync, dashed and marked "out" when not (A9), never
 * color alone. Under-replicated, offline and lost-records chips sit with them (A8).
 */
function ReplicaColumn({ partition }: { partition: PartitionSnapshot }) {
  const reduced = useReducedMotion();
  const leaderRef = useRef<string | null | undefined>(undefined);
  const lostRef = useRef<number | undefined>(undefined);
  const flip = leaderRef.current !== undefined && leaderRef.current !== partition.leader;
  const lostNow = lostRef.current !== undefined && partition.lostAcked > lostRef.current;
  useEffect(() => {
    leaderRef.current = partition.leader;
    lostRef.current = partition.lostAcked;
  }, [partition.leader, partition.lostAcked]);
  const offline = partition.leader === null;
  const replicated = partition.replicas.length > 1;
  return (
    <div className="flex shrink-0 flex-col items-end gap-0.5">
      <span
        key={partition.leader ?? 'offline'}
        className={`rounded-sm px-1 py-0.5 font-mono text-[10px] font-semibold ${
          offline ? 'bg-danger-bg text-danger-text' : 'bg-muted'
        } ${flip && !reduced ? 'kp-flip' : ''}`}
        data-slot="leader"
        title={
          offline
            ? 'Offline: no leader. Producers and consumers wait until a replica can lead again.'
            : `Leader: broker ${partition.leader} (epoch ${String(partition.leaderEpoch)}). Producers write to and consumers read from the leader.`
        }
        aria-label={offline ? 'Offline: no leader' : `Leader: broker ${partition.leader}`}
      >
        {offline ? '⚠ offline' : `L ${partition.leader}`}
      </span>
      {replicated && (
        <span className="flex gap-[2px]" data-slot="replicas">
          {partition.replicas.map((r) => {
            const inSync = partition.isr.includes(r.broker);
            const eligible = partition.elr.includes(r.broker);
            return (
              <span
                key={r.broker}
                className={`rounded-sm border px-[3px] font-mono text-[9px] leading-[12px] ${
                  inSync ? 'border-status/60' : 'kp-out-of-isr border-danger text-danger-text'
                }`}
                data-in-sync={inSync}
                title={`${r.broker}: ${
                  inSync ? 'in sync' : eligible ? 'out of ISR (eligible leader, ELR)' : 'out of ISR'
                } · LEO ${formatCount(r.logEndOffset)} · HW ${formatCount(r.highWatermark)}`}
                aria-label={`Replica ${r.broker} ${inSync ? 'in sync' : 'out of ISR'}`}
              >
                {r.broker}
                {!inSync && <span aria-hidden="true">·out</span>}
              </span>
            );
          })}
        </span>
      )}
      {partition.lostAcked > 0 && (
        <span
          className={`rounded-sm border border-dashed border-danger bg-danger-bg px-1 text-[9px] text-danger-text ${
            lostNow && !reduced ? 'kp-lost' : ''
          }`}
          data-slot="lost"
          title="Acknowledged records deleted by truncation when an old leader came back (see the partition log)."
        >
          ⚠ {formatCount(partition.lostAcked)} lost
        </span>
      )}
    </div>
  );
}

// ── Consumer row ──────────────────────────────────────────────────────────────────────────────

/**
 * A5: total lag. The number counts to its new value over 300 ms (instant under reduced motion);
 * above `LAG_DANGER_THRESHOLD` the badge turns danger and shows a warning sign, and the number is
 * always there, so the color is never the only signal (NFR-7.1). Screen readers get the real
 * value, not the counting one.
 */
export function LagBadge({ lag, uncommitted = 0 }: { lag: number | null; uncommitted?: number }) {
  const reduced = useReducedMotion();
  const shown = useCountUp(lag ?? 0, LAG_COUNT_MS, reduced);
  const danger = lag !== null && lag > LAG_DANGER_THRESHOLD;
  // Some partitions without a committed offset: their lag is unknown, so the sum is a lower
  // bound (review F-5), as kafka-consumer-groups prints "-" for them rather than 0.
  const atLeast = lag !== null && uncommitted > 0;
  const label =
    lag === null
      ? 'Lag unknown: the group has no committed offset yet'
      : `Lag ${atLeast ? 'at least ' : ''}${formatCount(lag)}${
          danger ? `, above ${String(LAG_DANGER_THRESHOLD)}` : ''
        }${atLeast ? `; ${partitionsText(uncommitted)} no committed offset yet` : ''}`;
  return (
    <span
      className={`ml-auto inline-flex shrink-0 items-center gap-0.5 rounded-lg px-1.5 py-0.5 font-mono text-[10px] font-semibold whitespace-nowrap ${
        danger ? 'bg-danger-bg text-danger-text' : 'bg-muted'
      }`}
      data-slot="lag"
      data-lag={lag ?? ''}
      data-uncommitted={uncommitted}
      data-danger={danger}
      title={`${label}. Lag = end offset (high watermark) − committed offset, summed over partitions.`}
    >
      {danger && <span aria-hidden="true">⚠&#xFE0E;</span>}
      <span aria-hidden="true" data-testid="lag-number">
        lag {lag === null ? '-' : `${atLeast ? '≥ ' : ''}${formatCount(shown)}`}
      </span>
      <span className="sr-only">{label}</span>
    </span>
  );
}

/** "1 partition has" / "2 partitions have". */
const partitionsText = (n: number): string =>
  n === 1 ? '1 partition has' : `${String(n)} partitions have`;

/** The words shown for each consumer status. */
const STATUS_LABEL: { readonly [S in ConsumerSnapshot['status']]: string } = {
  joining: 'joining',
  'left-group': 'left group',
  stable: 'stable',
  idle: 'idle',
  stopped: 'stopped',
  crashed: 'crashed',
};

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
      {STATUS_LABEL[status]}
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

/** Seconds left, one decimal: 6234 ms → "6.2 s". */
const secondsLeft = (ms: number): string => `${(Math.max(0, ms) / 1000).toFixed(1)} s`;

/**
 * The partitions the coordinator still assigns to a crashed member (review F-6): greyed, dashed
 * chips (not color alone: the words say "held"), and how long until its session times out. The
 * coordinator only notices the crash when no heartbeat arrives for session.timeout.ms
 * (GroupMetadataManager#expireClassicGroupMemberHeartbeat); until then nobody reads these
 * partitions. A member whose JoinGroup (or SyncGroup) was parked when it died has no deadline
 * yet: the parked request counts as a heartbeat, the join phase does not wait for it, and its
 * session timer starts when that wait ends (completeJoin re-arms it; K4). The countdown appears
 * as soon as the snapshot carries the deadline.
 */
function HeldChips({ held }: { held: HeldByCoordinator }) {
  const t = useSimTime();
  const labels = held.labels === '' ? [] : held.labels.split(' ');
  const until =
    held.expiresAt === null
      ? 'waiting for rebalance'
      : `timeout in ${secondsLeft(held.expiresAt - t)}`;
  return (
    <span className="flex min-w-0 items-center gap-1" data-slot="held">
      {labels.map((label) => (
        <span
          key={label}
          className="shrink-0 rounded-sm border border-dashed border-border px-1 font-mono text-[10px] text-status/70"
          data-chip-held={label}
        >
          {label}
        </span>
      ))}
      <span className="min-w-0 truncate" data-slot="held-until" aria-hidden="true">
        held · {until}
      </span>
      <span className="sr-only">
        {held.expiresAt === null
          ? 'Held by the coordinator; its session timer starts when this rebalance ends'
          : `Held by the coordinator until its session times out, in ${secondsLeft(held.expiresAt - t)}`}
      </span>
    </span>
  );
}

const heldTitle = (consumerId: string, held: HeldByCoordinator): string => {
  const parts = held.labels === '' ? 'no partitions yet' : held.labels.split(' ').join(', ');
  return held.expiresAt === null
    ? `The coordinator still counts an earlier ${consumerId} process as a member (${parts}). It was waiting in JoinGroup when it died, which counts as a heartbeat. Its session timer starts when this rebalance's join phase ends, and it's removed one session timeout after that.`
    : `The coordinator still assigns ${parts} to an earlier ${consumerId} process that died: it only notices when no heartbeat arrives for session.timeout.ms. Until then nobody reads these partitions; then the group rebalances.`;
};

/** Live part of a member row: status, owned partitions, position, committed offset, lag. */
export function ConsumerLive({ consumerId }: { consumerId: string }) {
  const consumer = useConsumer(consumerId);
  const held = useHeldByCoordinator(consumerId);
  const s = consumer === undefined ? undefined : summarizeConsumer(consumer);
  const many = s !== undefined && s.partitions > 1;
  const total = many ? 'Σ ' : '';
  const zombie = consumer?.status === 'left-group';
  const status =
    consumer === undefined
      ? ''
      : `${STATUS_LABEL[consumer.status]}${consumer.stopping ? ' (stopping)' : ''}${
          consumer.generation === null ? '' : ` in generation ${String(consumer.generation)}`
        }`;
  const title =
    consumer === undefined || s === undefined
      ? `${consumerId}: not in the simulation yet`
      : [
          `${consumerId} · ${status} · ${formatCount(consumer.consumed)} records consumed`,
          ...(zombie
            ? [
                `${consumerId} is out of the group (it missed max.poll.interval.ms, or the coordinator removed it) but still processes the batch it has. The coordinator may already have given these partitions to another member, and its commits fail. At its next poll() it loses them and rejoins.`,
              ]
            : []),
          ...(held === undefined ? [] : [heldTitle(consumerId, held)]),
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
        {s !== undefined && <LagBadge lag={s.lag} uncommitted={consumer?.uncommitted ?? 0} />}
      </div>
      <div className="flex min-w-0 items-center gap-1 font-mono text-[10px] text-status/70">
        {/* A restarted process can own partitions of its own while its earlier life is held. */}
        {consumer?.status === 'idle' && held === undefined ? (
          <IdleChip />
        ) : (
          consumer !== undefined && <PartitionChips labels={chipLabels(consumer)} />
        )}
        {held !== undefined && <HeldChips held={held} />}
        <span className="min-w-0 truncate" data-slot="offsets">
          {s === undefined || s.partitions === 0
            ? ''
            : zombie
              ? `still processing · ${total}pos ${dash(s.position)}`
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
        }. Lag over the group: ${groupLagWords(group)}.`;
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

/**
 * The group lag as text (review F-5). A partition without a committed offset has unknown lag
 * (kafka-consumer-groups prints "-"), so with any of those the sum is only a lower bound: "≥ N";
 * with no committed offset at all, "-". Counting them as 0 would show "group lag 0" in exactly the
 * moment records pile up unread (a crashed member before its first commit).
 */
export function groupLagText(group: GroupSnapshot): string {
  if (group.uncommitted === 0) return compactCount(group.totalLag);
  if (group.uncommitted === group.partitions.length) return '-';
  return `≥ ${compactCount(group.totalLag)}`;
}

function groupLagWords(group: GroupSnapshot): string {
  if (group.uncommitted === 0) return formatCount(group.totalLag);
  const unknown = `${partitionsText(group.uncommitted)} no committed offset, so their lag is unknown`;
  return group.uncommitted === group.partitions.length
    ? `unknown (${unknown})`
    : `at least ${formatCount(group.totalLag)} (${unknown})`;
}

/** Second header line: generation and the group's total lag (M3-T11). */
export function GroupHeaderStats({ groupId }: { groupId: string }) {
  const group = useGroup(groupId);
  if (group === undefined) return null;
  const danger = group.totalLag > LAG_DANGER_THRESHOLD;
  return (
    <span className="block truncate font-mono text-[10px] font-normal text-status/70">
      <span data-slot="generation">gen {group.generation}</span>
      {' · '}
      <span
        data-slot="group-lag"
        data-lag={group.totalLag}
        data-uncommitted={group.uncommitted}
        className={danger ? 'font-semibold text-danger-text' : ''}
        title={`Lag over the whole group: sum of HW − committed offset per partition. Now: ${groupLagWords(group)}.`}
      >
        {danger ? '⚠\uFE0E ' : ''}group lag {groupLagText(group)}
        {group.uncommitted > 0 && (
          <>
            <span aria-hidden="true"> · {group.uncommitted} uncommitted</span>
            <span className="sr-only">, {groupLagWords(group)}</span>
          </>
        )}
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

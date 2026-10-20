/**
 * The message-dot model behind the Canvas2D overlay (M2-T9, animations.md A1 / A2, ADR 0008).
 * Pure and DOM-free: the overlay component feeds it engine events and wall-clock times and draws
 * what it returns, so the whole lifecycle is unit-tested in Node.
 *
 * Dots are decorative: the engine keeps real Kafka timing and a record is in the log and with its
 * consumer long before its dot arrives. The rules from ADR 0008:
 * - A1 starts on `RecordProduced` (it already carries the partition the client's partitioner
 *   chose, so no need to wait for `RecordAppended`) and runs producer → partition row.
 * - A2 starts on `RecordConsumed` but only after the A1 dot with the same trace id has arrived:
 *   while A1 is moving, A2 waits in a queue keyed by trace id. If the record had no A1 dot
 *   (skipped, or from before a reset), A2 is skipped too.
 * - Skip, don't delay: at most `MAX_DOTS` dots (moving + queued A2) exist; beyond that new dots are
 *   skipped and `capChipVisible` turns on for a moment.
 * - Speed: `DOT_SPEED_PX_PER_S` flow px per wall-clock second × sim speed. Paused = frozen.
 * - Reduced motion: no movement; the dot fades in at its destination over `FADE_MS`, then is done.
 */

import type { EngineEvent } from '@kp/engine';
import { keyLetter } from '../live/format.ts';
import {
  consumerRow,
  partitionIn,
  partitionOut,
  producerOut,
  type AnchorResolver,
  type EndpointRef,
} from './anchors.ts';
import { makeEdgePath, pointAt, type EdgePath } from './path.ts';

/** animations.md A1/A2: 120 px/s × sim speed (flow px: dots and edges scale with zoom alike). */
export const DOT_SPEED_PX_PER_S = 120;
/** ui-spec: 16 px circle with the key letter. */
export const DOT_SIZE = 16;
/** NFR-2.5: at most ~500 dots on screen. */
export const MAX_DOTS = 500;
/** Reduced motion: fade in at the destination (animations.md "Rules for all animations"). */
export const FADE_MS = 150;
/** A frame gap longer than this (tab in the background, a long GC) is treated as this long. */
export const MAX_STEP_MS = 100;
/** The "too many to animate" chip stays this long after the last dot skipped for the cap. */
export const CAP_CHIP_HOLD_MS = 1000;
/** How many arrived A1 trace ids are remembered, so a later `RecordConsumed` can start its A2. */
export const ARRIVED_MEMORY = 4096;

export type DotKind = 'A1' | 'A2';

/** 0 = dark, 1 = mid, 2 = light grey (ui-spec "Key colors: greys + letter labels"). */
export type DotShade = 0 | 1 | 2;

export interface DotSpec {
  readonly kind: DotKind;
  readonly traceId: string;
  readonly key: string | null;
  readonly from: EndpointRef;
  readonly to: EndpointRef;
}

/** A dot as the drawing code sees it. Positions are flow coordinates. */
export interface Dot {
  readonly id: number;
  readonly kind: DotKind;
  readonly traceId: string;
  readonly letter: string;
  readonly shade: DotShade;
  readonly from: EndpointRef;
  readonly to: EndpointRef;
  /** 'move' runs along the path; 'fade' (reduced motion) sits at the destination. */
  mode: 'move' | 'fade';
  /** Fraction of the path covered (move). */
  progress: number;
  /** Time faded in so far (fade). */
  fadeMs: number;
  x: number;
  y: number;
  alpha: number;
  /** False until the first step gave it a position. */
  placed: boolean;
}

export interface StepOptions {
  readonly playing: boolean;
  readonly speed: number;
  readonly reducedMotion: boolean;
}

export interface DotStats {
  /** Dots started (cumulative, kept across resets). */
  started: number;
  /** Dots that reached their destination (cumulative). */
  arrived: number;
  /** New dots skipped because `MAX_DOTS` was reached (cumulative). */
  skippedCap: number;
  /** A2 dots skipped because their record had no A1 dot (cumulative). */
  skippedNoA1: number;
  /** Dots dropped because an end of their path disappeared (node removed, …) (cumulative). */
  droppedNoPath: number;
}

/** Same letter as the log cells (T8). */
export function dotLetter(key: string | null): string {
  return keyLetter(key);
}

/**
 * Grey shade for a key, stable per key. Single letters cycle A → dark, B → mid, C → light like the
 * wireframes; color is never the only signal, the letter is always drawn.
 */
export function keyShade(key: string | null): DotShade {
  if (key === null) return 1;
  let sum = 0;
  for (let i = 0; i < key.length; i++) sum += key.charCodeAt(i);
  return ((sum + 1) % 3) as DotShade;
}

export function a1Spec(e: Extract<EngineEvent, { type: 'RecordProduced' }>): DotSpec {
  return {
    kind: 'A1',
    traceId: e.traceId,
    key: e.key,
    from: producerOut(e.producerId),
    to: partitionIn(e.topic, e.partition),
  };
}

export function a2Spec(e: Extract<EngineEvent, { type: 'RecordConsumed' }>): DotSpec {
  return {
    kind: 'A2',
    traceId: e.traceId,
    key: e.key,
    from: partitionOut(e.topic, e.partition),
    to: consumerRow(e.consumerId),
  };
}

export class DotModel {
  readonly cap: number;
  readonly stats: DotStats = {
    started: 0,
    arrived: 0,
    skippedCap: 0,
    skippedNoA1: 0,
    droppedNoPath: 0,
  };

  private list: Dot[] = [];
  /** Moving A1 dots by trace id. */
  private readonly activeA1 = new Map<string, Dot>();
  /** A2 specs waiting for their A1 dot, by trace id. */
  private readonly pending = new Map<string, DotSpec[]>();
  private pendingCount = 0;
  /** Trace ids whose A1 dot arrived (insertion-ordered, oldest evicted). */
  private readonly arrivedA1 = new Set<string>();
  private nextId = 1;
  private lastNow: number | null = null;
  private lastCapSkipAt = Number.NEGATIVE_INFINITY;

  constructor(cap: number = MAX_DOTS) {
    this.cap = cap;
  }

  /** Dots to draw (only the ones that have a position). */
  get dots(): readonly Dot[] {
    return this.list;
  }

  /** Dots that exist (moving / fading) plus A2 dots queued behind their A1. */
  get load(): number {
    return this.list.length + this.pendingCount;
  }

  get queued(): number {
    return this.pendingCount;
  }

  /** True while there is anything to animate. */
  get busy(): boolean {
    return this.load > 0;
  }

  /** Full frame (init / reset / new subscriber): drop everything on screen and queued. */
  reset(): void {
    this.list = [];
    this.activeA1.clear();
    this.pending.clear();
    this.pendingCount = 0;
    this.arrivedA1.clear();
    this.lastNow = null;
  }

  /** The animation loop stopped: the next step starts a fresh clock instead of a long gap. */
  pauseClock(): void {
    this.lastNow = null;
  }

  /** Whether the "too many to animate" chip should show at wall time `now`. */
  capChipVisible(now: number): boolean {
    return now - this.lastCapSkipAt < CAP_CHIP_HOLD_MS;
  }

  /** New engine events from one frame (never a full frame's `history`). */
  ingest(events: readonly EngineEvent[], now: number): void {
    for (const e of events) {
      if (e.type === 'RecordProduced') {
        this.spawn(a1Spec(e), now);
      } else if (e.type === 'RecordConsumed') {
        this.consume(a2Spec(e), now);
      }
    }
  }

  /** Records skipped without being ingested (e.g. the tab was hidden): counted as cap skips. */
  skip(count: number, now: number): void {
    if (count <= 0) return;
    this.stats.skippedCap += count;
    this.lastCapSkipAt = now;
  }

  /**
   * Starts a dot now unless the cap is reached. `progress` (0…1) starts it part-way along its path;
   * used by the stress hook, normal dots start at 0. Returns false when the dot was skipped.
   */
  spawn(spec: DotSpec, now: number, progress = 0): boolean {
    if (this.load >= this.cap) {
      this.skipForCap(now);
      return false;
    }
    this.start(spec, progress);
    return true;
  }

  private consume(spec: DotSpec, now: number): void {
    const traceId = spec.traceId;
    if (this.activeA1.has(traceId)) {
      if (this.load >= this.cap) {
        this.skipForCap(now);
        return;
      }
      const queue = this.pending.get(traceId);
      if (queue === undefined) this.pending.set(traceId, [spec]);
      else queue.push(spec);
      this.pendingCount++;
    } else if (this.arrivedA1.has(traceId)) {
      this.spawn(spec, now);
    } else {
      this.stats.skippedNoA1++;
    }
  }

  private skipForCap(now: number): void {
    this.stats.skippedCap++;
    this.lastCapSkipAt = now;
  }

  private start(spec: DotSpec, progress: number): Dot {
    const dot: Dot = {
      id: this.nextId++,
      kind: spec.kind,
      traceId: spec.traceId,
      letter: dotLetter(spec.key),
      shade: keyShade(spec.key),
      from: spec.from,
      to: spec.to,
      mode: 'move',
      progress: Math.min(Math.max(progress, 0), 1),
      fadeMs: 0,
      x: 0,
      y: 0,
      alpha: 1,
      placed: false,
    };
    this.list.push(dot);
    if (spec.kind === 'A1') this.activeA1.set(spec.traceId, dot);
    this.stats.started++;
    return dot;
  }

  private rememberArrived(traceId: string): void {
    this.arrivedA1.add(traceId);
    if (this.arrivedA1.size > ARRIVED_MEMORY) {
      const oldest = this.arrivedA1.values().next();
      if (oldest.done !== true) this.arrivedA1.delete(oldest.value);
    }
  }

  /** Drops the A2 dots queued behind an A1 dot that will never arrive. */
  private dropPending(traceId: string): void {
    const queue = this.pending.get(traceId);
    if (queue === undefined) return;
    this.pending.delete(traceId);
    this.pendingCount -= queue.length;
    this.stats.droppedNoPath += queue.length;
  }

  /**
   * Advances every dot to wall time `now` (ms) and updates its position from the current node
   * positions (`resolve`), so dots stay on their path while nodes move. Dots that arrive are
   * removed; an arriving A1 releases the A2 dots queued behind it, which start from the partition
   * row in the same step.
   */
  step(now: number, options: StepOptions, resolve: AnchorResolver): void {
    const dt = this.lastNow === null ? 0 : Math.min(Math.max(now - this.lastNow, 0), MAX_STEP_MS);
    this.lastNow = now;
    const advance = options.playing ? dt : 0;
    const distance = (DOT_SPEED_PX_PER_S * options.speed * advance) / 1000;
    const paths = new Map<string, EdgePath | null>();
    const pathFor = (dot: Dot): EdgePath | null => {
      const key = `${dot.from.nodeId}\u0000${dot.from.handleId}\u0000${dot.to.nodeId}\u0000${dot.to.handleId}`;
      let path = paths.get(key);
      if (path === undefined) {
        const a = resolve(dot.from);
        const b = resolve(dot.to);
        path = a === undefined || b === undefined ? null : makeEdgePath(a, b);
        paths.set(key, path);
      }
      return path;
    };

    const list = this.list;
    let write = 0;
    // `list` can grow while we walk it (released A2 dots); those start this step without moving.
    const existing = list.length;
    for (let read = 0; read < list.length; read++) {
      const dot = list[read];
      if (dot === undefined) continue;
      const fresh = read >= existing;
      const path = pathFor(dot);
      if (path === null) {
        this.stats.droppedNoPath++;
        if (dot.kind === 'A1') {
          this.activeA1.delete(dot.traceId);
          this.dropPending(dot.traceId);
        }
        continue;
      }
      if (options.reducedMotion && dot.mode === 'move') {
        dot.mode = 'fade';
        dot.fadeMs = 0;
      }
      let done: boolean;
      if (dot.mode === 'move') {
        if (!fresh && path.length > 0) dot.progress += distance / path.length;
        else if (!fresh && advance > 0) dot.progress = 1;
        done = dot.progress >= 1;
        const p = pointAt(path, dot.progress);
        dot.x = p.x;
        dot.y = p.y;
        dot.alpha = 1;
      } else {
        // The fade is a UI transition (150 ms), not message travel: not scaled by sim speed, but
        // frozen while paused like everything else on the overlay.
        if (!fresh) dot.fadeMs += advance;
        done = dot.fadeMs >= FADE_MS;
        const p = pointAt(path, 1);
        dot.x = p.x;
        dot.y = p.y;
        dot.alpha = Math.min(dot.fadeMs / FADE_MS, 1);
      }
      dot.placed = true;
      if (done) {
        this.stats.arrived++;
        if (dot.kind === 'A1') this.arrive(dot.traceId);
        continue;
      }
      list[write++] = dot;
    }
    list.length = write;
  }

  private arrive(traceId: string): void {
    this.activeA1.delete(traceId);
    this.rememberArrived(traceId);
    const queue = this.pending.get(traceId);
    if (queue === undefined) return;
    this.pending.delete(traceId);
    this.pendingCount -= queue.length;
    // Their room was reserved while queued, so they start whatever the load is now.
    for (const spec of queue) this.start(spec, 0);
  }
}

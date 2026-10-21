/**
 * SimHost: drives the engine in real time and pushes frames to the UI (architecture §4.1,
 * milestone M2 Q4/T4). It is a plain class with an injected wall clock and tick scheduler, so it
 * runs in Node tests with a fake clock; `sim.worker.ts` gives it `performance.now` and
 * `setTimeout` and exposes it over Comlink.
 *
 * Speed, pause and step live here and never reach the engine: they change how fast sim time is
 * driven, not what happens in it, so they are not commands and are not in the command log
 * (commands/index.ts). The engine only ever sees `runUntil(t)` / `step()` and real commands.
 */

import {
  createEngine,
  createEventHasher,
  diffSnapshot,
  isEmptyDiff,
  replayCommandLog,
  type Command,
  type CommandLogEntry,
  type DispatchResult,
  type Engine,
  type EngineEvent,
  type EngineSnapshot,
} from '@kp/engine';
import {
  EVENT_HISTORY_SIZE,
  MAX_SPEED,
  MIN_SPEED,
  SNAPSHOT_RECORDS_PER_PARTITION,
  type DiffFrame,
  type FrameListener,
  type FullFrame,
  type HostResult,
  type HostStatus,
  type InitOptions,
  type SimApi,
  type StepResult,
} from './protocol.ts';
import { RingBuffer } from './ring-buffer.ts';

/** Minimum wall time between two run-loop frames: ~30 frames per second (architecture §4.1). */
export const FRAME_INTERVAL_MS = 33;

/**
 * Most wall time one tick may turn into sim time. A backgrounded tab throttles worker timers to
 * once a second or less; without a cap the next tick would jump minutes of sim time at once (and
 * a slow tick would make the next one slower). With the cap the sim just runs slower while the
 * tab is hidden.
 */
export const MAX_TICK_DELTA_MS = 250;

export interface SimHostOptions {
  /** Wall clock in ms (`performance.now` in the worker). Fractions are fine. */
  readonly now: () => number;
  /** Calls `tick` once, soon (`setTimeout(tick, 16)` in the worker). Returns a cancel function. */
  readonly scheduleTick: (tick: () => void) => () => void;
  readonly frameIntervalMs?: number;
  readonly maxTickDeltaMs?: number;
  readonly historySize?: number;
}

export class SimHost implements SimApi {
  readonly #now: () => number;
  readonly #scheduleTick: (tick: () => void) => () => void;
  readonly #frameIntervalMs: number;
  readonly #maxTickDeltaMs: number;

  #engine: Engine | null = null;
  #initial: InitOptions | null = null;
  #unlisten: (() => void) | null = null;

  /** Events since the last frame. Bounded by one tick's work (MAX_TICK_DELTA_MS × speed). */
  #pending: EngineEvent[] = [];
  readonly #history: RingBuffer<EngineEvent>;

  readonly #subscribers = new Map<number, FrameListener>();
  #nextToken = 1;
  #frame = 0;
  /** What every subscriber holds (the base of the next diff); null without subscribers. */
  #lastSent: EngineSnapshot | null = null;
  #lastFrameWall = Number.NEGATIVE_INFINITY;

  #playing = false;
  #speed = 1;
  #lastWall = 0;
  /** Sub-millisecond sim time not yet run: the sim clock is an integer, wall deltas are not. */
  #carry = 0;
  #cancelTick: (() => void) | null = null;

  constructor(options: SimHostOptions) {
    this.#now = options.now;
    this.#scheduleTick = options.scheduleTick;
    this.#frameIntervalMs = options.frameIntervalMs ?? FRAME_INTERVAL_MS;
    this.#maxTickDeltaMs = options.maxTickDeltaMs ?? MAX_TICK_DELTA_MS;
    this.#history = new RingBuffer(options.historySize ?? EVENT_HISTORY_SIZE);
  }

  init(options: InitOptions): HostResult {
    // Keep our own copy: reset() rebuilds from it, and the caller may change its object later.
    const initial = structuredClone(options);
    let engine: Engine;
    try {
      engine = createEngine({
        seed: initial.seed,
        topology: initial.topology,
        snapshotRecordsPerPartition: SNAPSHOT_RECORDS_PER_PARTITION,
      });
    } catch (error) {
      return { ok: false, reason: error instanceof Error ? error.message : String(error) };
    }
    this.#initial = initial;
    this.#install(engine);
    return { ok: true };
  }

  reset(): void {
    const initial = this.#initial;
    if (initial === null) throw new Error('SimHost: call init() first');
    this.#install(
      createEngine({
        seed: initial.seed,
        topology: initial.topology,
        snapshotRecordsPerPartition: SNAPSHOT_RECORDS_PER_PARTITION,
      }),
    );
  }

  dispatch(command: Command): DispatchResult {
    const result = this.#requireEngine().dispatch(command);
    // While playing, the next run-loop frame carries the change. While paused there is no next
    // frame, so send one now: the user must see the topology change.
    if (result.ok && !this.#playing) this.#flush(false);
    return result;
  }

  play(): void {
    this.#requireEngine();
    if (this.#playing) return;
    this.#playing = true;
    this.#lastWall = this.#now();
    this.#flush(true);
    this.#scheduleNext();
  }

  pause(): void {
    this.#requireEngine();
    if (!this.#playing) return;
    this.#stop();
    this.#flush(true);
  }

  step(): StepResult {
    const engine = this.#requireEngine();
    this.#stop();
    const stepped = engine.step();
    this.#flush(true);
    return { stepped, t: engine.now() };
  }

  stepBy(ms: number): HostResult {
    const engine = this.#requireEngine();
    if (!Number.isInteger(ms) || ms < 1) {
      return { ok: false, reason: `stepBy needs a whole number of ms ≥ 1, got ${String(ms)}` };
    }
    this.#stop();
    engine.runUntil(engine.now() + ms);
    this.#flush(true);
    return { ok: true };
  }

  setSpeed(speed: number): HostResult {
    if (!Number.isFinite(speed) || speed < MIN_SPEED || speed > MAX_SPEED) {
      return {
        ok: false,
        reason: `Speed must be between ${String(MIN_SPEED)}× and ${String(MAX_SPEED)}×, got ${String(speed)}`,
      };
    }
    if (speed === this.#speed) return { ok: true };
    // Time up to now ran at the old speed.
    if (this.#playing) this.#advance();
    this.#speed = speed;
    if (this.#engine !== null) this.#flush(true);
    return { ok: true };
  }

  subscribe(onFrame: FrameListener): number {
    const token = this.#nextToken++;
    const engine = this.#engine;
    if (engine !== null) {
      // Deliver what is pending to the existing subscribers first, so the new one's history
      // holds only delivered events and the next frame repeats none of them.
      this.#flush(false);
      this.#lastSent ??= engine.getSnapshot();
      onFrame(this.#fullFrame(this.#lastSent, this.#history.toArray()));
    }
    this.#subscribers.set(token, onFrame);
    return token;
  }

  unsubscribe(token: number): void {
    this.#subscribers.delete(token);
    if (this.#subscribers.size === 0) this.#lastSent = null;
  }

  status(): HostStatus {
    return {
      initialised: this.#engine !== null,
      playing: this.#playing,
      speed: this.#speed,
      t: this.#engine?.now() ?? 0,
    };
  }

  commandLog(): readonly CommandLogEntry[] {
    return [...this.#requireEngine().commandLog];
  }

  /**
   * Replays the command log on a fresh engine to the same number of processed tasks and hashes
   * every event. Costs a full re-run, but only when called: hashing live would add ~60% to the
   * engine's CPU time at high rates (M2-decisions T4). The replay runs the same bundled engine,
   * which is what the browser-vs-Node determinism test (T12) needs.
   */
  eventLogHash(): string {
    const engine = this.#requireEngine();
    const initial = this.#initial;
    if (initial === null) throw new Error('SimHost: call init() first');
    const hasher = createEventHasher();
    const replay = replayCommandLog(initial, engine.commandLog, (event) => {
      hasher.add(event);
    });
    // Count tasks, not time: after step() some tasks due at the current instant may still wait.
    while (replay.stepsProcessed() < engine.stepsProcessed() && replay.step()) {
      // step() does the work
    }
    return hasher.digest();
  }

  eventHistory(): readonly EngineEvent[] {
    return this.#history.toArray();
  }

  // ---------------------------------------------------------------------------------------------

  #requireEngine(): Engine {
    if (this.#engine === null) throw new Error('SimHost: call init() first');
    return this.#engine;
  }

  /** Swaps in a fresh engine, paused, with empty logs, and sends every subscriber a full frame. */
  #install(engine: Engine): void {
    this.#stop();
    this.#carry = 0;
    this.#unlisten?.();
    this.#engine = engine;
    this.#pending = [];
    this.#history.clear();
    this.#unlisten = engine.on((event) => {
      this.#pending.push(event);
      this.#history.push(event);
    });
    this.#lastSent = null;
    if (this.#subscribers.size === 0) return;
    const snapshot = engine.getSnapshot();
    this.#lastSent = snapshot;
    this.#send(this.#fullFrame(snapshot, []));
  }

  readonly #tick = (): void => {
    this.#cancelTick = null;
    if (!this.#playing) return;
    this.#advance();
    if (this.#now() - this.#lastFrameWall >= this.#frameIntervalMs) this.#flush(false);
    this.#scheduleNext();
  };

  #scheduleNext(): void {
    this.#cancelTick ??= this.#scheduleTick(this.#tick);
  }

  /** Runs the engine for the wall time since the last advance × speed. */
  #advance(): void {
    const engine = this.#requireEngine();
    const wall = this.#now();
    const delta = Math.min(Math.max(wall - this.#lastWall, 0), this.#maxTickDeltaMs);
    this.#lastWall = wall;
    const simMs = delta * this.#speed + this.#carry;
    const whole = Math.floor(simMs);
    this.#carry = simMs - whole;
    if (whole > 0) engine.runUntil(engine.now() + whole);
  }

  /** Stops the run loop (after running the time up to now). No frame. */
  #stop(): void {
    if (this.#playing) this.#advance();
    this.#playing = false;
    this.#cancelTick?.();
    this.#cancelTick = null;
  }

  /**
   * Sends a diff frame with the pending events. Skipped when nothing changed, unless `force`
   * (play state or speed changed, or the user stepped and expects an answer).
   */
  #flush(force: boolean): void {
    const engine = this.#requireEngine();
    if (this.#subscribers.size === 0) {
      this.#pending = [];
      return;
    }
    const prev = this.#lastSent;
    if (prev === null) throw new Error('SimHost: subscribers without a base snapshot');
    const snapshot = engine.getSnapshot();
    const diff = diffSnapshot(prev, snapshot);
    if (!force && this.#pending.length === 0 && isEmptyDiff(diff)) return;
    const frame: DiffFrame = { kind: 'diff', ...this.#frameBase(), diff };
    this.#lastSent = isEmptyDiff(diff) ? prev : snapshot;
    this.#send(frame);
  }

  #fullFrame(snapshot: EngineSnapshot, history: readonly EngineEvent[]): FullFrame {
    return { kind: 'full', ...this.#frameBase(), snapshot, history };
  }

  /** Common frame fields; takes the pending events, so each one is sent exactly once. */
  #frameBase(): Omit<DiffFrame, 'kind' | 'diff'> {
    const events = this.#pending;
    this.#pending = [];
    return {
      frame: ++this.#frame,
      t: this.#engine?.now() ?? 0,
      playing: this.#playing,
      speed: this.#speed,
      events,
    };
  }

  #send(frame: DiffFrame | FullFrame): void {
    this.#lastFrameWall = this.#now();
    for (const listener of this.#subscribers.values()) listener(frame);
  }
}

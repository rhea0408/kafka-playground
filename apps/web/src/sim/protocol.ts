/**
 * The worker frame protocol (milestone M2 Q4, architecture §4.1): types shared by the sim host in
 * the worker and the main-thread client. Everything here is plain data, so it crosses
 * `postMessage` by structured clone. No DOM or worker globals: this file is type-checked under
 * both the DOM and the WebWorker lib.
 */

import {
  applyDiff,
  type Command,
  type CommandLogEntry,
  type DispatchResult,
  type EngineEvent,
  type EngineSnapshot,
  type SimTime,
  type SnapshotDiff,
} from '@kp/engine';
import type { TopologyInput } from '@kp/schema';

/** Sim speed limits (architecture §4.1: 0.25×–10×). */
export const MIN_SPEED = 0.25;
export const MAX_SPEED = 10;

/** Result of a host call that can be refused. Same shape as the engine's `DispatchResult`. */
export type HostResult = DispatchResult;

export interface InitOptions {
  readonly seed: number;
  readonly topology: TopologyInput;
}

interface FrameBase {
  /** Host-wide frame counter: increases with every frame sent, never reset. */
  readonly frame: number;
  /** Sim time when the frame was built. */
  readonly t: SimTime;
  readonly playing: boolean;
  readonly speed: number;
  /** Every event emitted since the previous frame, in order. Never dropped or repeated. */
  readonly events: readonly EngineEvent[];
}

/**
 * A full snapshot: sent after `init` / `reset`, and as the first frame of a new subscriber.
 * `history` holds the recent, already-delivered events from the host's ring buffer, for the event
 * list only (don't animate them). It is empty after `init` / `reset`.
 */
export interface FullFrame extends FrameBase {
  readonly kind: 'full';
  readonly snapshot: EngineSnapshot;
  readonly history: readonly EngineEvent[];
}

/** Changes since the previous frame: `applyDiff(previousSnapshot, diff)`. */
export interface DiffFrame extends FrameBase {
  readonly kind: 'diff';
  readonly diff: SnapshotDiff;
}

export type SimFrame = FullFrame | DiffFrame;

/** The snapshot a subscriber holds after `frame`, given the one it held before (null at first). */
export function applyFrame(prev: EngineSnapshot | null, frame: SimFrame): EngineSnapshot {
  if (frame.kind === 'full') return frame.snapshot;
  if (prev === null) throw new Error('Diff frame before a full frame');
  return applyDiff(prev, frame.diff);
}

export type FrameListener = (frame: SimFrame) => void;

export interface HostStatus {
  readonly initialised: boolean;
  readonly playing: boolean;
  readonly speed: number;
  readonly t: SimTime;
}

/** Result of `step()`: whether a task was processed, and the sim time after it. */
export interface StepResult {
  readonly stepped: boolean;
  readonly t: SimTime;
}

/**
 * What the worker exposes over Comlink. `SimHost` implements it directly; the worker only adds
 * proxy bookkeeping to `subscribe` / `unsubscribe`.
 */
export interface SimApi {
  init: (options: InitOptions) => HostResult;
  dispatch: (command: Command) => DispatchResult;
  play: () => void;
  pause: () => void;
  /** Pauses, then processes exactly one scheduled task. */
  step: () => StepResult;
  /** Pauses, then advances sim time by `ms` (a positive integer). */
  stepBy: (ms: number) => HostResult;
  setSpeed: (speed: number) => HostResult;
  /** Fresh engine from the same seed and initial topology; clears the command log. Pauses. */
  reset: () => void;
  /** Registers a frame listener and sends it a full frame at once. Returns a token. */
  subscribe: (onFrame: FrameListener) => number;
  unsubscribe: (token: number) => void;
  status: () => HostStatus;
  /** Accepted commands since `init` / `reset`, for replay and the T12 determinism test. */
  commandLog: () => readonly CommandLogEntry[];
  /**
   * FNV-1a hash of every event since `init` / `reset` (engine `hashEvents`), computed by replaying
   * the command log. Slow for long runs; meant for tests and diagnostics.
   */
  eventLogHash: () => string;
  /** The ring buffer's events, oldest first. */
  eventHistory: () => readonly EngineEvent[];
}

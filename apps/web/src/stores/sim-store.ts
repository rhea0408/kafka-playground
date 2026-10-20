/**
 * simStore: the main thread's copy of the running simulation (architecture §5): the latest
 * snapshot, a bounded event list, sim time, play state and speed. It is fed only by frames from
 * the sim host (`applySimFrame`); the UI never writes runtime state itself (CLAUDE.md rule 3).
 *
 * Per-frame consumers that must not cause React renders (the Canvas2D overlay, T9) use
 * `onSimFrame` instead of subscribing to the store.
 */

import type { EngineEvent, EngineSnapshot, SimTime } from '@kp/engine';
import { create } from 'zustand';
import { applyFrame, EVENT_HISTORY_SIZE, type SimFrame } from '../sim/protocol.ts';

/** Same size as the host's ring buffer, so a late subscriber's history fills the list exactly. */
export const EVENT_LIST_SIZE = EVENT_HISTORY_SIZE;

export interface SimState {
  /** Null until the first full frame arrives. */
  readonly snapshot: EngineSnapshot | null;
  /** Recent events, oldest first, at most `EVENT_LIST_SIZE`. */
  readonly events: readonly EngineEvent[];
  readonly t: SimTime;
  readonly playing: boolean;
  readonly speed: number;
  /** Number of the last frame applied (0 before the first). */
  readonly frame: number;
}

export const INITIAL_SIM_STATE: SimState = {
  snapshot: null,
  events: [],
  t: 0,
  playing: false,
  speed: 1,
  frame: 0,
};

export const useSimStore = create<SimState>()(() => INITIAL_SIM_STATE);

export function resetSimStore(): void {
  useSimStore.setState(INITIAL_SIM_STATE, true);
  frameListeners.clear();
}

/** Appends `added` to `list` and keeps the newest `limit` entries. */
export function appendBounded<T>(
  list: readonly T[],
  added: readonly T[],
  limit: number,
): readonly T[] {
  const all = added.length === 0 ? list : list.length === 0 ? added : list.concat(added);
  return all.length > limit ? all.slice(all.length - limit) : all;
}

/**
 * Applies one frame from the host. A full frame replaces the snapshot and the event list (its
 * `history` was already delivered: listed, never animated); a diff frame patches the snapshot and
 * appends its events. Frame listeners run after the store is updated.
 */
export function applySimFrame(frame: SimFrame, limit: number = EVENT_LIST_SIZE): void {
  const prev = useSimStore.getState();
  const snapshot = applyFrame(prev.snapshot, frame);
  const base = frame.kind === 'full' ? frame.history : prev.events;
  useSimStore.setState({
    snapshot,
    events: appendBounded(base, frame.events, limit),
    t: frame.t,
    playing: frame.playing,
    speed: frame.speed,
    frame: frame.frame,
  });
  for (const listener of [...frameListeners]) {
    try {
      listener(frame);
    } catch (error) {
      // One broken listener must not stop the others or the store; report it asynchronously.
      queueMicrotask(() => {
        throw error;
      });
    }
  }
}

/**
 * Called with every frame after it is applied, outside React. `frame.events` are the new events
 * to animate; a full frame means "start over" (init / reset / new subscriber): drop what is on
 * screen, and don't animate `frame.history`.
 */
export type SimFrameListener = (frame: SimFrame) => void;

const frameListeners = new Set<SimFrameListener>();

/** Registers a per-frame listener. Returns a function that removes it. */
export function onSimFrame(listener: SimFrameListener): () => void {
  frameListeners.add(listener);
  return () => {
    frameListeners.delete(listener);
  };
}

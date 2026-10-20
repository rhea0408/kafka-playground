/**
 * Discrete-event loop (architecture §4.1): a sim clock, a (time, seq) queue of scheduled tasks,
 * a seeded RNG and an ordered event stream. It knows nothing about Kafka; the engine plugs in the
 * task handler and the event types.
 *
 * Tasks are plain data (a discriminated union chosen by the engine), not closures, so pending work
 * stays inspectable and can later be serialised for rewind checkpoints (architecture §4.7).
 */

import { createClock, type SimTime } from './clock.ts';
import { createEventQueue } from './queue.ts';
import { createRng, type Rng } from './rng.ts';

/** An event as delivered to listeners: sim time and emission order first, then the payload. */
export type Stamped<E> = { readonly t: SimTime; readonly seq: number } & E;

export type Listener<E> = (event: Stamped<E>) => void;

export interface KernelOptions<Task> {
  readonly seed: number;
  /** Runs one task at its scheduled time. May schedule further tasks and emit events. */
  readonly handleTask: (task: Task) => void;
}

export interface Kernel<Task, E extends { readonly type: string }> {
  readonly rng: Rng;
  readonly now: () => SimTime;
  /** Number of tasks processed so far. Used to place commands exactly on replay. */
  readonly stepsProcessed: () => number;
  readonly pendingTasks: () => number;
  /** Schedules `task` at absolute sim time `at` (must not be in the past). */
  readonly scheduleAt: (at: SimTime, task: Task) => void;
  /** Schedules `task` `delayMs` after now (0 = later in the current instant, after pending work). */
  readonly scheduleIn: (delayMs: number, task: Task) => void;
  /** Stamps the event with the current time and the next event seq, then notifies listeners. */
  readonly emit: (event: E) => Stamped<E>;
  /** Processes the next task, moving the clock to its time. Returns false when nothing is queued. */
  readonly step: () => boolean;
  /** Processes every task scheduled at or before `t`, then sets the clock to exactly `t`. */
  readonly runUntil: (t: SimTime) => void;
  /**
   * Moves the clock to `t` without processing anything. Tasks due at exactly `t` stay queued.
   * Throws if a task is due before `t`, since skipping it would break causality. Used by replay.
   */
  readonly advanceTo: (t: SimTime) => void;
  /** Subscribes to events; returns an unsubscribe function. */
  readonly on: (listener: Listener<E>) => () => void;
}

export function createKernel<Task, E extends { readonly type: string }>(
  options: KernelOptions<Task>,
): Kernel<Task, E> {
  const clock = createClock();
  const queue = createEventQueue<Task>();
  const listeners = new Set<Listener<E>>();
  let eventSeq = 0;
  let steps = 0;

  const scheduleAt = (at: SimTime, task: Task): void => {
    if (at < clock.now()) {
      throw new RangeError(
        `cannot schedule in the past: now=${String(clock.now())}, at=${String(at)}`,
      );
    }
    queue.push(at, task);
  };

  const step = (): boolean => {
    const next = queue.pop();
    if (next === undefined) return false;
    clock.advanceTo(next.time);
    steps++;
    options.handleTask(next.item);
    return true;
  };

  return {
    rng: createRng(options.seed),
    now: clock.now,
    stepsProcessed: () => steps,
    pendingTasks: () => queue.size,
    scheduleAt,
    scheduleIn: (delayMs, task) => {
      scheduleAt(clock.now() + delayMs, task);
    },
    emit: (event) => {
      const stamped = { t: clock.now(), seq: eventSeq++, ...event } as Stamped<E>;
      // Copy so a listener that unsubscribes during delivery cannot skip another listener.
      for (const listener of [...listeners]) listener(stamped);
      return stamped;
    },
    step,
    runUntil: (t) => {
      if (t < clock.now()) {
        throw new RangeError(
          `runUntil cannot go backwards: now=${String(clock.now())}, t=${String(t)}`,
        );
      }
      for (let next = queue.peek(); next !== undefined && next.time <= t; next = queue.peek()) {
        step();
      }
      clock.advanceTo(t);
    },
    advanceTo: (t) => {
      const next = queue.peek();
      if (next !== undefined && next.time < t) {
        throw new RangeError(
          `cannot advance to ${String(t)}: a task is due at ${String(next.time)}`,
        );
      }
      clock.advanceTo(t);
    },
    on: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}

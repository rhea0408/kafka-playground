/**
 * Simulated time in integer milliseconds since the start of the run. This is the only notion of
 * "now" inside the engine: wall-clock time (Date.now, performance.now) would make two runs with
 * the same seed and commands diverge (NFR-4.1). The UI maps wall-clock to sim-clock for speed
 * control (architecture §4.1); the engine never sees wall-clock time.
 */

/** Simulated milliseconds. Always a non-negative integer. */
export type SimTime = number;

export interface Clock {
  readonly now: () => SimTime;
  /** Moves time forward to `t`. Time never goes backwards; staying at the same time is allowed. */
  readonly advanceTo: (t: SimTime) => void;
}

export function createClock(start: SimTime = 0): Clock {
  assertSimTime(start);
  let current = start;
  return {
    now: () => current,
    advanceTo: (t) => {
      assertSimTime(t);
      if (t < current) {
        throw new RangeError(
          `clock cannot go backwards: now=${String(current)}, requested=${String(t)}`,
        );
      }
      current = t;
    },
  };
}

export function assertSimTime(t: number): asserts t is SimTime {
  if (!Number.isSafeInteger(t) || t < 0) {
    throw new RangeError(`sim time must be a non-negative integer (ms), got ${String(t)}`);
  }
}

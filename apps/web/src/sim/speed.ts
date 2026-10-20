/**
 * Sim speed presets for the speed control (FR-2.1: 0.25×–10×). Speed is a host concern, never an
 * engine command (M1 deviation, M2-decisions T4).
 */

import { MAX_SPEED, MIN_SPEED } from './protocol.ts';

export const SPEED_PRESETS = [0.25, 0.5, 1, 2, 5, 10] as const;

/** "0.25×", "1×", "10×". */
export function formatSpeed(speed: number): string {
  return `${String(speed)}×`;
}

/**
 * The preset after `speed` in the given direction, wrapping around at the ends. A speed that is
 * not a preset moves to the nearest preset in that direction.
 */
export function cycleSpeed(speed: number, direction: 1 | -1 = 1): number {
  const presets: readonly number[] = SPEED_PRESETS;
  if (direction === 1) {
    return presets.find((p) => p > speed) ?? MIN_SPEED;
  }
  return [...presets].reverse().find((p) => p < speed) ?? MAX_SPEED;
}

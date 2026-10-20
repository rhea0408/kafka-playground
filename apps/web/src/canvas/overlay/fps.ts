/**
 * Frames-per-second counter for the overlay's animation loop (M2-T9, NFR-2.1). Pure: callers pass
 * the rAF timestamps. FPS = frames in the last second.
 */

export const FPS_WINDOW_MS = 1000;

export class FpsMeter {
  private readonly times: number[] = [];

  /** Records one frame at `now` (ms) and returns the current FPS. */
  frame(now: number): number {
    this.times.push(now);
    return this.fps(now);
  }

  /** Frames in the `FPS_WINDOW_MS` before `now`. */
  fps(now: number): number {
    const from = now - FPS_WINDOW_MS;
    let drop = 0;
    while (drop < this.times.length && (this.times[drop] ?? 0) <= from) drop++;
    if (drop > 0) this.times.splice(0, drop);
    return this.times.length;
  }

  reset(): void {
    this.times.length = 0;
  }
}

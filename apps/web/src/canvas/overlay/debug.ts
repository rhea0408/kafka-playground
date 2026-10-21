/**
 * Overlay counters for tests and the FPS meter (M2-T9). `window.__kp.overlay` is always present:
 * updating a few numbers on a plain object costs nothing, and the Playwright suite runs against
 * the production build (T12 reads `dotsDrawn`). The visible FPS meter and the stress hook are only
 * enabled in dev builds or with `?fps` in the URL.
 */

import type { TopologyState } from '../../stores/topology-store.ts';
import { partitionIn, producerOut } from './anchors.ts';
import type { DotSpec } from './dots.ts';

export interface OverlayDebug {
  /** Dot draws so far (one per dot per frame drawn), cumulative. > 0 once a dot was on screen. */
  dotsDrawn: number;
  /** Dots on screen in the last frame. */
  dotsOnScreen: number;
  /** Dots that exist now (moving or fading). */
  dotsActive: number;
  /** A2 dots waiting for their A1 dot. */
  dotsQueued: number;
  /** Dots started, cumulative. */
  dotsStarted: number;
  /** Dots skipped for the 500 cap, cumulative. */
  skippedCap: number;
  /** A2 dots skipped because their record had no A1 dot, cumulative. */
  skippedNoA1: number;
  /** Frames in the last second of the overlay's animation loop (0 while the loop is stopped). */
  fps: number;
  /** Whether the "too many to animate" chip is showing. */
  capChip: boolean;
  /**
   * Stress hook (dev / `?fps` only): starts `count` A1 dots spread along the first producer → topic
   * connection, without involving the engine. Returns how many started (the cap still applies).
   */
  spawnTestDots?: (count: number) => number;
}

declare global {
  /** Test and debug hooks on `window.__kp`; other modules add their own fields (sim/debug-hook.ts). */
  interface KpDebugHooks {
    overlay?: OverlayDebug;
  }
  interface Window {
    __kp?: KpDebugHooks;
  }
}

export function newOverlayDebug(): OverlayDebug {
  return {
    dotsDrawn: 0,
    dotsOnScreen: 0,
    dotsActive: 0,
    dotsQueued: 0,
    dotsStarted: 0,
    skippedCap: 0,
    skippedNoA1: 0,
    fps: 0,
    capChip: false,
  };
}

/** True in `vite dev`, or with `?fps` (also `?fps=1`) in any build. */
export function debugEnabled(search: string, dev: boolean): boolean {
  return dev || new URLSearchParams(search).has('fps');
}

/**
 * Specs for the stress hook: `count` A1 dots on the first producer → topic connection, spread over
 * the topic's partitions and evenly along the path (`progress`), so a frame has dots everywhere.
 * Empty when there is no such connection. `batch` keeps trace ids unique across calls.
 */
export function testDotSpecs(
  state: TopologyState,
  count: number,
  batch = 0,
): { readonly spec: DotSpec; readonly progress: number }[] {
  for (const c of state.connections) {
    const from = state.components.find((x) => x.id === c.from);
    const to = state.components.find((x) => x.id === c.to);
    if (from?.kind !== 'producer' || to?.kind !== 'topic') continue;
    const keys = ['A', 'B', 'C', null] as const;
    return Array.from({ length: Math.max(0, Math.floor(count)) }, (_, i) => ({
      spec: {
        kind: 'A1' as const,
        traceId: `stress${String(batch)}#${String(i)}`,
        key: keys[i % keys.length] ?? null,
        from: producerOut(from.id),
        to: partitionIn(to.id, i % to.partitions),
      },
      progress: count <= 1 ? 0 : i / count,
    }));
  }
  return [];
}

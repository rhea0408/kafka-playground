/**
 * Per-node subscriptions to the sim snapshot (M2-T8). Frames arrive at ~30 Hz, so every node reads
 * only its own slice: `applyDiff` shares unchanged entries between snapshots (structural sharing,
 * snapshot/diff.ts), so a selector that returns an entry by id gives the same object until that
 * entry changes, and the shallow comparison skips the render. A node therefore re-renders only
 * when its own producer / partition / consumer changed, and the canvas itself never subscribes.
 */

import type { ConsumerSnapshot, PartitionSnapshot, ProducerStats } from '@kp/engine';
import { useEffect, useRef, useState } from 'react';
import { useShallow } from 'zustand/react/shallow';
import { useSimStore, type SimState } from '../../stores/sim-store.ts';

export const selectProducerStats =
  (id: string) =>
  (s: SimState): ProducerStats | undefined =>
    s.snapshot?.producers.find((p) => p.config.id === id)?.stats;

export const selectPartition =
  (topicId: string, index: number) =>
  (s: SimState): PartitionSnapshot | undefined =>
    s.snapshot?.topics.find((t) => t.config.id === topicId)?.partitions[index];

export const selectConsumer =
  (id: string) =>
  (s: SimState): ConsumerSnapshot | undefined =>
    s.snapshot?.consumers.find((c) => c.config.id === id);

// `useShallow` also skips renders when an entry was rebuilt with the same fields (e.g. a producer
// whose config changed gets a new but equal `stats` object).
export function useProducerStats(id: string): ProducerStats | undefined {
  return useSimStore(useShallow(selectProducerStats(id)));
}

export function usePartition(topicId: string, index: number): PartitionSnapshot | undefined {
  return useSimStore(useShallow(selectPartition(topicId, index)));
}

export function useConsumer(id: string): ConsumerSnapshot | undefined {
  return useSimStore(useShallow(selectConsumer(id)));
}

/** Only the status, so the Kill / Restart toolbar doesn't re-render on every offset change. */
export function useConsumerStatus(id: string): ConsumerSnapshot['status'] | undefined {
  return useSimStore((s) => selectConsumer(id)(s)?.status);
}

const easeOutCubic = (p: number): number => 1 - (1 - p) ** 3;

/**
 * A5: a number that counts from what is on screen to `target` over `durationMs`
 * (requestAnimationFrame, ease-out). A new target mid-count starts a new count from the number
 * shown, so a lag that changes every frame is followed smoothly. `instant` (reduced motion) jumps
 * straight to the target. The count is a pure view of the snapshot value: it never delays or
 * changes the value itself (animations.md: "never waits for dots").
 */
export function useCountUp(target: number, durationMs: number, instant: boolean): number {
  const [shown, setShown] = useState(target);
  const shownRef = useRef(target);

  useEffect(() => {
    const from = shownRef.current;
    if (instant || from === target || durationMs <= 0) {
      shownRef.current = target;
      setShown(target);
      return undefined;
    }
    let start: number | undefined;
    let frame = requestAnimationFrame(function tick(now) {
      start ??= now;
      const p = Math.min(1, (now - start) / durationMs);
      const value = p >= 1 ? target : Math.round(from + (target - from) * easeOutCubic(p));
      shownRef.current = value;
      setShown(value);
      if (p < 1) frame = requestAnimationFrame(tick);
    });
    return () => {
      cancelAnimationFrame(frame);
    };
  }, [target, durationMs, instant]);

  return shown;
}

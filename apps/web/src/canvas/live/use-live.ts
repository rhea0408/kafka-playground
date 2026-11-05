/**
 * Per-node subscriptions to the sim snapshot (M2-T8). Frames arrive at ~30 Hz, so every node reads
 * only its own slice: `applyDiff` shares unchanged entries between snapshots (structural sharing,
 * snapshot/diff.ts), so a selector that returns an entry by id gives the same object until that
 * entry changes, and the shallow comparison skips the render. A node therefore re-renders only
 * when its own producer / partition / consumer changed, and the canvas itself never subscribes.
 */

import type { ConsumerSnapshot, GroupSnapshot, PartitionSnapshot, ProducerStats } from '@kp/engine';
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

export const selectGroup =
  (groupId: string) =>
  (s: SimState): GroupSnapshot | undefined =>
    s.snapshot?.groups.find((g) => g.groupId === groupId);

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

export function useGroup(groupId: string): GroupSnapshot | undefined {
  return useSimStore(useShallow(selectGroup(groupId)));
}

/**
 * What the coordinator still holds for a consumer that is not running (review F-6): a crashed
 * member stays in the group, with its partitions, until its session times out. Primitives only,
 * so the row re-renders when the assignment or the deadline changes, not on every group frame.
 * Undefined while the consumer runs or once the coordinator has removed it.
 */
export interface HeldByCoordinator {
  /** Partition chip labels, space-separated ("P0 P1", or "orders-0 …" across topics). */
  readonly labels: string;
  /** Sim time the session runs out; null while its JoinGroup / SyncGroup is parked. */
  readonly expiresAt: number | null;
}

export const selectHeld =
  (consumerId: string) =>
  (s: SimState): HeldByCoordinator | undefined => {
    const consumer = selectConsumer(consumerId)(s);
    if (consumer === undefined) return undefined;
    if (consumer.status !== 'crashed' && consumer.status !== 'stopped') return undefined;
    const member = selectGroup(consumer.config.groupId)(s)?.members.find(
      (m) => m.consumerId === consumerId,
    );
    if (member === undefined) return undefined;
    const topics = new Set(member.partitions.map((p) => p.topic)).size;
    const labels = member.partitions.map((p) =>
      topics > 1 ? `${p.topic}-${String(p.partition)}` : `P${String(p.partition)}`,
    );
    return { labels: labels.join(' '), expiresAt: member.sessionExpiresAt };
  };

export function useHeldByCoordinator(consumerId: string): HeldByCoordinator | undefined {
  return useSimStore(useShallow(selectHeld(consumerId)));
}

/** The sim clock. Only for parts that must tick (a countdown): every frame re-renders them. */
export function useSimTime(): number {
  return useSimStore((s) => s.snapshot?.t ?? 0);
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

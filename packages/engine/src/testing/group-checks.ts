/**
 * Consumer group safety checks over an event log, shared by the invariant properties (general
 * random runs) and the liveness property (busy groups, where rebalances overlap processing).
 * Each returns the violations it found, as readable strings; an empty list means it holds.
 * Test-only; not exported from the package.
 */

import type { EngineEvent } from '../events/index.ts';

const tp = (e: { topic: string; partition: number }) => `${e.topic}-${String(e.partition)}`;

/**
 * Partitions each consumer holds according to its rebalance listener: assigned and not yet
 * revoked or lost. A stop, start, removal or re-add starts it from nothing.
 */
class Held {
  private readonly byConsumer = new Map<string, Set<string>>();
  /** When each consumer last gave each partition up (revoked or lost). */
  private readonly gaveUpAt = new Map<string, Map<string, number>>();

  /** Applies one event (only listener and lifecycle events change anything). */
  apply(e: EngineEvent): void {
    if (e.type === 'PartitionsAssigned') {
      const set = this.byConsumer.get(e.consumerId) ?? new Set<string>();
      for (const p of e.partitions) set.add(tp(p));
      this.byConsumer.set(e.consumerId, set);
    } else if (e.type === 'PartitionsRevoked' || e.type === 'PartitionsLost') {
      const at = this.gaveUpAt.get(e.consumerId) ?? new Map<string, number>();
      for (const p of e.partitions) {
        this.byConsumer.get(e.consumerId)?.delete(tp(p));
        at.set(tp(p), e.t);
      }
      this.gaveUpAt.set(e.consumerId, at);
    } else if (e.type === 'ConsumerStopped' || e.type === 'ConsumerStarted') {
      this.byConsumer.delete(e.consumerId);
    } else if (
      (e.type === 'ComponentRemoved' || e.type === 'ComponentAdded') &&
      e.component.kind === 'consumer'
    ) {
      this.byConsumer.delete(e.component.id);
    }
  }

  of(consumerId: string): ReadonlySet<string> {
    return this.byConsumer.get(consumerId) ?? new Set();
  }

  /** Whether the consumer held the partition at time `t` or later (holds it now, or gave it up at ≥ t). */
  heldSince(consumerId: string, key: string, t: number): boolean {
    return this.of(consumerId).has(key) || (this.gaveUpAt.get(consumerId)?.get(key) ?? -1) >= t;
  }
}

/**
 * An accepted commit comes from a member of the current generation that owns the partition, or
 * that owned it in the previous completed generation and has not emitted PartitionsRevoked /
 * PartitionsLost for it yet; and within one generation only one member commits a partition.
 *
 * ClassicGroup#validateOffsetCommit checks only the member id and the generation, never who owns
 * the partition. A cooperative member is STABLE in generation N+1 as soon as SyncGroup is
 * answered, but revokes the partitions it gives up only at its next poll() (onJoinComplete), so
 * until then it may still commit them with N+1. Nobody else can own them in N+1: they move only
 * in the follow-up rebalance (KIP-429). M4-decisions D3.
 *
 * "Not yet" is judged when the commit was sent, one network delay before the coordinator accepted
 * it: a commitAsync() sent just before poll() revokes the partition arrives after the
 * PartitionsRevoked event, and Kafka accepts it. That is still safe, because the member's next
 * JoinGroup leaves after the commit and requests arrive in order, so the partition cannot move
 * first. (A commit sent after the revocation cannot name the partition: its position is dropped.)
 *
 * The window is exactly one network delay, so `networkDelayMs` must be the run's own (Run has
 * it); there is no default, so a scenario with a slower network cannot silently use a wrong one.
 */
export function commitViolations(events: readonly EngineEvent[], networkDelayMs: number): string[] {
  type Gen = { generation: number; owner: Map<string, string> };
  const current = new Map<string, Gen>();
  const previous = new Map<string, Gen>();
  const held = new Held();
  const committer = new Map<string, string>();
  const out: string[] = [];
  for (const e of events) {
    held.apply(e);
    if (e.type === 'RebalanceCompleted') {
      const owner = new Map<string, string>();
      for (const m of e.members) for (const p of m.partitions) owner.set(tp(p), m.memberId);
      const last = current.get(e.groupId);
      if (last !== undefined) previous.set(e.groupId, last);
      current.set(e.groupId, { generation: e.generation, owner });
    } else if (e.type === 'OffsetCommitted') {
      const at = `t=${String(e.t)} ${e.consumerId} ${tp(e)} gen ${String(e.generation)}`;
      const g = current.get(e.groupId);
      if (e.memberId === null) {
        out.push(`${at}: accepted without a member id`);
        continue;
      }
      if (e.generation !== g?.generation) {
        out.push(`${at}: not the last completed generation (${String(g?.generation)})`);
        continue;
      }
      const ownsNow = g.owner.get(tp(e)) === e.memberId;
      const stillHolds =
        previous.get(e.groupId)?.owner.get(tp(e)) === e.memberId &&
        held.heldSince(e.consumerId, tp(e), e.t - networkDelayMs);
      if (!ownsNow && !stillHolds) out.push(`${at}: neither owns nor still holds it`);
      const key = `${e.groupId}|${String(e.generation)}|${tp(e)}`;
      const first = committer.get(key);
      if (first !== undefined && first !== e.memberId) out.push(`${at}: ${first} committed it too`);
      committer.set(key, e.memberId);
    }
  }
  return out;
}

/**
 * Cooperative assignments (review F-4): a partition the assignor leaves without an owner must be
 * pending revocation, i.e. still held by a member of that generation (CooperativeStickyAssignor
 * takes a partition that changes owner away from both members for one round, KIP-429). So once
 * no member holds a partition it was not assigned, every partition has an owner. Partitions are
 * checked from 0 up to the highest one the generation assigns or its members hold, because the
 * leader may not know the newest partitions yet (as in the eager check).
 */
export function cooperativeCoverageViolations(events: readonly EngineEvent[]): string[] {
  const held = new Held();
  const out: string[] = [];
  for (const e of events) {
    held.apply(e);
    if (e.type !== 'RebalanceCompleted' || e.protocol !== 'cooperative-sticky') continue;
    const owned = new Set(e.members.flatMap((m) => m.partitions.map(tp)));
    const pending = new Set(e.members.flatMap((m) => [...held.of(m.consumerId)]));
    const highest = new Map<string, number>();
    const note = (key: string) => {
      const dash = key.lastIndexOf('-');
      const topic = key.slice(0, dash);
      highest.set(topic, Math.max(highest.get(topic) ?? -1, Number(key.slice(dash + 1))));
    };
    for (const key of owned) note(key);
    for (const key of pending) note(key);
    for (const [topic, top] of highest) {
      for (let p = 0; p <= top; p++) {
        const key = `${topic}-${String(p)}`;
        if (!owned.has(key) && !pending.has(key)) {
          out.push(
            `t=${String(e.t)} gen ${String(e.generation)}: ${key} has no owner and no one holds it`,
          );
        }
      }
    }
  }
  return out;
}

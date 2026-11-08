/**
 * Engine invariants (architecture §4.6) checked over random topologies and command sequences,
 * including consumer groups with several members, crashes, stops, restarts, group.id and
 * assignor changes, partition increases and slow consumers (M3-T6). The ISR invariants join in M4.
 */

import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { replayCommandLog } from './engine.ts';
import type { EngineEvent } from './events/index.ts';
import { stepsArb, topologyArb } from './testing/arbitraries.ts';
import { commitViolations, cooperativeCoverageViolations } from './testing/group-checks.ts';
import { runScenario } from './testing/run.ts';
import { runsFromEnv } from './testing/runs.ts';

const seedArb = fc.integer({ min: 0, max: 0xffffffff });
/** KP_INVARIANT_RUNS, default 150; the nightly job runs 1000 (M4-T0). */
const NUM_RUNS = runsFromEnv('KP_INVARIANT_RUNS', 150);

type Ev<T extends EngineEvent['type']> = Extract<EngineEvent, { type: T }>;
const tp = (e: { topic: string; partition: number }) => `${e.topic}-${String(e.partition)}`;

describe('engine invariants (fast-check)', { timeout: Math.max(30_000, NUM_RUNS * 400) }, () => {
  it('offsets in every partition are contiguous from 0, in append order', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { engine, events } = runScenario(seed, topology, steps);
        const next = new Map<string, number>();
        for (const e of events) {
          if (e.type !== 'RecordAppended') continue;
          expect(e.offset).toBe(next.get(tp(e)) ?? 0);
          next.set(tp(e), e.offset + 1);
        }
        for (const topic of engine.getSnapshot().topics) {
          for (const p of topic.partitions) {
            expect(p.records.map((r) => r.offset)).toEqual(
              Array.from(
                { length: p.records.length },
                (_, i) => p.logEndOffset - p.records.length + i,
              ),
            );
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('records with the same key from the same producer keep their produce order in a partition', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        const producedSeq = new Map<string, number>();
        const last = new Map<string, number>();
        for (const e of events) {
          if (e.type === 'RecordProduced') producedSeq.set(e.traceId, e.seq);
          if (e.type !== 'RecordAppended' || e.key === null) continue;
          const sentAt = producedSeq.get(e.traceId);
          expect(sentAt).toBeDefined();
          const k = `${tp(e)}|${e.producerId}|${e.key}`;
          const prev = last.get(k);
          if (prev !== undefined) expect(sentAt).toBeGreaterThan(prev);
          last.set(k, sentAt ?? -1);
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('HW ≤ LEO, and a committed offset never exceeds the HW at the time of the commit', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { engine, events } = runScenario(seed, topology, steps);
        const hw = new Map<string, number>();
        for (const e of events) {
          // A deleted topic's HW goes with it: a topic re-created with the same name is a new
          // topic (new topic ID, KIP-516) whose log starts again at offset 0.
          if (e.type === 'ComponentRemoved' && e.component.kind === 'topic') {
            for (const key of [...hw.keys()]) {
              if (key.slice(0, key.lastIndexOf('-')) === e.component.id) hw.delete(key);
            }
          }
          if (e.type === 'RecordAppended') hw.set(tp(e), e.offset + 1);
          if (e.type === 'OffsetCommitted')
            expect(e.offset).toBeLessThanOrEqual(hw.get(tp(e)) ?? 0);
        }
        const snap = engine.getSnapshot();
        const partitions = new Map<string, { highWatermark: number; logEndOffset: number }>();
        for (const t of snap.topics) {
          for (const p of t.partitions) {
            expect(p.highWatermark).toBeLessThanOrEqual(p.logEndOffset);
            partitions.set(`${t.config.id}-${String(p.index)}`, p);
          }
        }
        // Not against the current HW: after a leader change the new leader's HW may lag behind
        // the old one's until its followers fetch (K5, M4-T5), so a commit made under the old
        // leader can be above it for a while. It never exceeds what was ever appended.
        for (const g of snap.groups) {
          for (const c of g.committed) {
            if (partitions.has(tp(c))) expect(c.offset).toBeLessThanOrEqual(hw.get(tp(c)) ?? 0);
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('no record is consumed before it was appended, and consumers see exactly what was appended', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        const appended = new Map<string, Ev<'RecordAppended'>>();
        for (const e of events) {
          if (e.type === 'RecordAppended') appended.set(`${tp(e)}@${String(e.offset)}`, e);
          if (e.type === 'RecordConsumed') {
            const a = appended.get(`${tp(e)}@${String(e.offset)}`);
            expect(a).toBeDefined();
            expect([e.key, e.value, e.traceId]).toEqual([a?.key, a?.value, a?.traceId]);
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('within one poll, a consumer gets each partition’s records in offset order without gaps', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        let previous: Ev<'RecordConsumed'> | undefined;
        for (const e of events) {
          if (e.type !== 'RecordConsumed') {
            previous = undefined;
            continue;
          }
          if (previous && previous.consumerId === e.consumerId && tp(previous) === tp(e)) {
            expect(e.offset).toBe(previous.offset + 1);
          }
          previous = e;
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('trace ids are unique across producer lives and rate changes', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        const ids = events.filter((e) => e.type === 'RecordProduced').map((e) => e.traceId);
        expect(new Set(ids).size).toBe(ids.length);
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('a record reaches a consumer no sooner than one network delay after it was appended', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        const appendedAt = new Map<string, number>();
        for (const e of events) {
          if (e.type === 'RecordAppended') appendedAt.set(`${tp(e)}@${String(e.offset)}`, e.t);
          if (e.type === 'RecordConsumed') {
            const t = appendedAt.get(`${tp(e)}@${String(e.offset)}`) ?? Infinity;
            expect(e.t).toBeGreaterThanOrEqual(t + 1); // default networkDelayMs
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('a stopped or removed consumer reads nothing until it is started or added again', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        const alive = new Set((topology.consumers ?? []).map((c) => c.id));
        /** When each consumer last stopped: a commit it sent just before may still land. */
        const stoppedAt = new Map<string, number>();
        const stop = (id: string, t: number) => {
          alive.delete(id);
          stoppedAt.set(id, t);
        };
        for (const e of events) {
          if (e.type === 'ComponentAdded' && e.component.kind === 'consumer') {
            alive.add(e.component.id);
          }
          if (e.type === 'ComponentRemoved' && e.component.kind === 'consumer') {
            stop(e.component.id, e.t);
          }
          if (e.type === 'ConfigUpdated' && e.after.kind === 'consumer') {
            // A group.id change closes the client and starts a new one.
            if (e.before.kind === 'consumer' && e.before.groupId !== e.after.groupId) {
              stoppedAt.set(e.id, e.t);
            }
          }
          if (e.type === 'ConsumerStarted') alive.add(e.consumerId);
          if (e.type === 'ConsumerStopped') stop(e.consumerId, e.t);
          if (e.type === 'RecordConsumed') expect(alive.has(e.consumerId)).toBe(true);
          if (e.type === 'OffsetCommitted' && !alive.has(e.consumerId)) {
            // A commit already on the wire lands one network delay later.
            expect(e.t).toBeLessThanOrEqual((stoppedAt.get(e.consumerId) ?? -Infinity) + 1);
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('rejected commands change nothing: replaying only the accepted log gives the same events', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { engine, events, endTime } = runScenario(seed, topology, steps);
        // Finish the current instant on the original too: if the last step was a command, tasks
        // it scheduled at `endTime` are still queued, and the replay's runUntil would run them.
        engine.runUntil(endTime);
        const replayed: EngineEvent[] = [];
        const again = replayCommandLog({ seed, topology }, engine.commandLog, (e) =>
          replayed.push(e),
        );
        again.runUntil(endTime);
        expect(JSON.stringify(replayed)).toBe(JSON.stringify(events));
        expect(again.getSnapshot()).toEqual(engine.getSnapshot());
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('snapshots are always JSON-serialisable', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const snap = runScenario(seed, topology, steps).engine.getSnapshot();
        expect(JSON.parse(JSON.stringify(snap))).toEqual(snap);
      }),
      { numRuns: 50 },
    );
  });

  // ── Consumer groups (M3-T6) ─────────────────────────────────────────────────────────────────

  it('within one generation, each partition is assigned to at most one member', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        for (const e of events) {
          if (e.type !== 'RebalanceCompleted') continue;
          const all = e.members.flatMap((m) => m.partitions.map(tp));
          expect(new Set(all).size).toBe(all.length);
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('an eager assignment gives every partition of an assigned topic an owner (no gaps)', () => {
    // The leader assigns from its own metadata, which may not know the newest partitions yet, so
    // each assigned topic must be covered from partition 0 up without holes. Cooperative
    // assignments leave moving partitions unowned for one round on purpose (KIP-429).
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        for (const e of events) {
          if (e.type !== 'RebalanceCompleted' || e.protocol === 'cooperative-sticky') continue;
          const byTopic = new Map<string, number[]>();
          for (const m of e.members) {
            for (const p of m.partitions) {
              byTopic.set(p.topic, [...(byTopic.get(p.topic) ?? []), p.partition]);
            }
          }
          for (const parts of byTopic.values()) {
            expect(parts.sort((a, b) => a - b)).toEqual(parts.map((_, i) => i));
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('an accepted commit comes from a member of the current generation that owns the partition, or still holds it from the previous one', () => {
    // See commitViolations (M4-decisions D3).
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events, networkDelayMs } = runScenario(seed, topology, steps);
        expect(commitViolations(events, networkDelayMs)).toEqual([]);
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('a cooperative assignment leaves a partition unowned only while a member still holds it', () => {
    // See cooperativeCoverageViolations (review F-4).
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        expect(cooperativeCoverageViolations(runScenario(seed, topology, steps).events)).toEqual(
          [],
        );
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('generations only increase', () => {
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        // A generation can also end without RebalanceCompleted (CompletingRebalance → a member
        // never syncs → a new rebalance), so check order only: never backwards, and every
        // completion is newer than the generation its rebalance started from.
        const lastStarted = new Map<string, number>();
        const lastCompleted = new Map<string, number>();
        for (const e of events) {
          if (e.type === 'RebalanceStarted') {
            expect(e.generation).toBeGreaterThanOrEqual(lastStarted.get(e.groupId) ?? 0);
            expect(e.generation).toBeGreaterThanOrEqual(lastCompleted.get(e.groupId) ?? 0);
            lastStarted.set(e.groupId, e.generation);
          }
          if (e.type === 'RebalanceCompleted') {
            expect(e.generation).toBeGreaterThan(lastCompleted.get(e.groupId) ?? 0);
            expect(e.generation).toBeGreaterThan(lastStarted.get(e.groupId) ?? -1);
            lastCompleted.set(e.groupId, e.generation);
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('a member only reads partitions it owns in its current generation', () => {
    // Not "two consumers never read one partition at the same time": a zombie can (that is why
    // commits are checked by generation). What holds is that every record poll() returns comes
    // from a partition the rebalance listener gave this consumer and has not taken back.
    fc.assert(
      fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
        const { events } = runScenario(seed, topology, steps);
        const owned = new Map<string, Set<string>>();
        const reset = (id: string) => owned.set(id, new Set());
        for (const e of events) {
          if (e.type === 'PartitionsAssigned') {
            const set = owned.get(e.consumerId) ?? new Set<string>();
            for (const p of e.partitions) set.add(tp(p));
            owned.set(e.consumerId, set);
          } else if (e.type === 'PartitionsRevoked' || e.type === 'PartitionsLost') {
            for (const p of e.partitions) owned.get(e.consumerId)?.delete(tp(p));
          } else if (e.type === 'ConsumerStopped' || e.type === 'ConsumerStarted') {
            reset(e.consumerId);
          } else if (
            (e.type === 'ComponentRemoved' || e.type === 'ComponentAdded') &&
            e.component.kind === 'consumer'
          ) {
            reset(e.component.id);
          } else if (e.type === 'RecordConsumed') {
            expect(owned.get(e.consumerId)?.has(tp(e))).toBe(true);
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });
});

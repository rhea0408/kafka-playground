/**
 * Engine invariants (architecture §4.6) checked over random topologies and command sequences.
 * M1 has one replica per partition and no groups with several members, so the ISR and
 * group-assignment invariants join in M3/M4.
 */

import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { replayCommandLog } from './engine.ts';
import type { EngineEvent } from './events/index.ts';
import { stepsArb, topologyArb } from './testing/arbitraries.ts';
import { runScenario } from './testing/run.ts';

const seedArb = fc.integer({ min: 0, max: 0xffffffff });
const NUM_RUNS = 150;

type Ev<T extends EngineEvent['type']> = Extract<EngineEvent, { type: T }>;
const tp = (e: { topic: string; partition: number }) => `${e.topic}-${String(e.partition)}`;

describe('engine invariants (fast-check)', () => {
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
        const last = new Map<string, number>();
        for (const e of events) {
          if (e.type !== 'RecordAppended' || e.key === null) continue;
          const n = Number(e.traceId.slice(e.traceId.lastIndexOf('#') + 1));
          const k = `${tp(e)}|${e.producerId}|${e.key}`;
          // Re-adding a producer restarts its numbering, so only compare within increasing runs.
          const prev = last.get(k);
          if (prev !== undefined && n !== 1) expect(n).toBeGreaterThan(prev);
          last.set(k, n);
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
        for (const g of snap.groups) {
          for (const c of g.committed) {
            const p = partitions.get(tp(c));
            if (p !== undefined) expect(c.offset).toBeLessThanOrEqual(p.highWatermark);
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
});

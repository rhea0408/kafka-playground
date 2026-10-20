import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { createEngine } from '../engine.ts';
import { stepsArb, topologyArb } from '../testing/arbitraries.ts';
import { applyDiff, diffSnapshot } from './diff.ts';
import type { EngineSnapshot } from './index.ts';

/**
 * Random runs (including UpdateConfig, deletes and re-adds of producers, consumers and topics),
 * with a snapshot after every step. Small record windows make records drop off the front.
 */
const runArb = fc.record({
  seed: fc.integer({ min: 0, max: 0xffffffff }),
  topology: topologyArb,
  steps: stepsArb,
  recordsPerPartition: fc.integer({ min: 0, max: 5 }),
});

type Run = typeof runArb extends fc.Arbitrary<infer R> ? R : never;

function snapshotsOf(run: Run): EngineSnapshot[] {
  const engine = createEngine({
    seed: run.seed,
    topology: run.topology,
    snapshotRecordsPerPartition: run.recordsPerPartition,
  });
  const snapshots = [engine.getSnapshot()];
  const frozen = [JSON.stringify(snapshots[0])];
  for (const step of run.steps) {
    if (step.kind === 'advance') engine.runUntil(engine.now() + step.ms);
    else engine.dispatch(step.command);
    const snapshot = engine.getSnapshot();
    snapshots.push(snapshot);
    frozen.push(JSON.stringify(snapshot));
  }
  // Old snapshots never change while the engine keeps running (no aliasing of engine state).
  expect(snapshots.map((s) => JSON.stringify(s))).toEqual(frozen);
  return snapshots;
}

describe('snapshot diff properties', () => {
  it('applyDiff(prev, diffSnapshot(prev, next)) equals next for any two snapshots of a run', () => {
    fc.assert(
      fc.property(runArb, (run) => {
        const snapshots = snapshotsOf(run);
        // Every pair, so prev/next span any number of steps (and also go backwards in time).
        for (const prev of snapshots) {
          for (const next of snapshots) {
            const before = JSON.stringify(prev);
            const diff = diffSnapshot(prev, next);
            expect(applyDiff(prev, diff)).toEqual(next);
            expect(JSON.stringify(prev)).toBe(before); // prev untouched
            expect(JSON.parse(JSON.stringify(diff))).toEqual(diff); // plain data
          }
        }
      }),
      { numRuns: 60 },
    );
  });

  it('a chain of frame diffs rebuilds the final snapshot (what the UI does)', () => {
    fc.assert(
      fc.property(runArb, (run) => {
        const snapshots = snapshotsOf(run);
        let ui = snapshots[0] as EngineSnapshot;
        for (let i = 1; i < snapshots.length; i++) {
          const next = snapshots[i] as EngineSnapshot;
          // The diff crosses the worker boundary as a copy.
          const wire = JSON.parse(
            JSON.stringify(diffSnapshot(snapshots[i - 1] as EngineSnapshot, next)),
          ) as ReturnType<typeof diffSnapshot>;
          ui = applyDiff(ui, wire);
          expect(ui).toEqual(next);
        }
      }),
      { numRuns: 100 },
    );
  });
});

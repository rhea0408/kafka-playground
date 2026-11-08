/**
 * Liveness of consumer groups (M4-T0, review F-3). The invariants in invariants.prop.test.ts are
 * safety properties (no double owners, no stale commits): a group that never becomes Stable passes
 * them all. This property checks progress under load: busy members that can finish a poll()'s
 * batch in time must end up in a Stable group, and none of them may be removed for missing the
 * rebalance timeout.
 *
 * The arbitrary is its own: one group whose members are instances of one application (same
 * settings), fed fast enough that members are usually busy when a rebalance starts, so the join
 * phase overlaps processing (which the general arbitraries rarely produce).
 */

import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import type { Command } from './commands/index.ts';
import type { EngineEvent } from './events/index.ts';
import { rebalanceProtocol } from './protocols/assignors/index.ts';
import type { Step } from './testing/arbitraries.ts';
import { commitViolations, cooperativeCoverageViolations } from './testing/group-checks.ts';
import { runScenario } from './testing/run.ts';
import { runsFromEnv } from './testing/runs.ts';

const seedArb = fc.integer({ min: 0, max: 0xffffffff });

const settingsArb = fc.record({
  partitionAssignmentStrategy: fc
    .constantFrom(
      ['range', 'cooperative-sticky'] as const,
      ['range'] as const,
      ['roundrobin'] as const,
      ['sticky'] as const,
      ['cooperative-sticky'] as const,
    )
    .map((list) => [...list]),
  commitMode: fc.constantFrom(
    'auto' as const,
    'sync-after-processing' as const,
    'async-after-processing' as const,
    'sync-before-processing' as const,
  ),
  enableAutoCommit: fc.boolean(),
  autoCommitIntervalMs: fc.integer({ min: 100, max: 5000 }),
  maxPollRecords: fc.integer({ min: 1, max: 500 }),
  processingTimeMs: fc.integer({ min: 0, max: 60 }),
  maxPollIntervalMs: fc.integer({ min: 5000, max: 40_000 }),
  sessionTimeoutMs: fc.integer({ min: 6000, max: 20_000 }),
  heartbeatIntervalMs: fc.integer({ min: 500, max: 3000 }),
});
type Settings = typeof settingsArb extends fc.Arbitrary<infer T> ? T : never;

const consumer = (id: string, settings: Settings) => ({
  kind: 'consumer' as const,
  id,
  groupId: 'g',
  autoOffsetReset: 'earliest' as const,
  ...settings,
});

const scenarioArb = fc
  .record({
    settings: settingsArb,
    partitions: fc.integer({ min: 1, max: 6 }),
    ratePerSec: fc.integer({ min: 20, max: 300 }),
    initial: fc.integer({ min: 1, max: 4 }),
    initialDelay: fc.constantFrom(0, 3000),
  })
  .chain((s) => {
    const ids = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5'];
    const command = fc.oneof(
      // A new instance joins (added, then subscribed).
      fc.constantFrom(...ids).map((id): Command[] => [
        { type: 'AddComponent', component: consumer(id, s.settings) },
        { type: 'Connect', from: 't0', to: id },
      ]),
      fc.constantFrom(...ids).map((id): Command[] => [{ type: 'KillConsumer', id }]),
      fc.constantFrom(...ids).map((id): Command[] => [{ type: 'StopConsumer', id }]),
      fc.constantFrom(...ids).map((id): Command[] => [{ type: 'RestartConsumer', id }]),
      fc
        .integer({ min: 1, max: 8 })
        .map((p): Command[] => [{ type: 'UpdateConfig', id: 't0', patch: { partitions: p } }]),
      fc
        .integer({ min: 20, max: 300 })
        .map((r): Command[] => [{ type: 'UpdateConfig', id: 'p0', patch: { ratePerSec: r } }]),
    );
    const step = fc.oneof(
      fc.integer({ min: 100, max: 15_000 }).map((ms): Step[] => [{ kind: 'advance', ms }]),
      command.map((cs): Step[] => cs.map((c) => ({ kind: 'command', command: c }))),
    );
    return fc.record({
      s: fc.constant(s),
      steps: fc.array(step, { minLength: 1, maxLength: 12 }).map((l) => l.flat()),
    });
  });

type Scenario = typeof scenarioArb extends fc.Arbitrary<infer T> ? T : never;

/** Runs a scenario, then a quiet period long enough for the group to settle. */
function run(seed: number, { s, steps }: Scenario) {
  const ids = Array.from({ length: s.initial }, (_, i) => `c${String(i)}`);
  const topology = {
    brokers: [{ kind: 'broker' as const, id: 'b1', groupInitialRebalanceDelayMs: s.initialDelay }],
    topics: [{ kind: 'topic' as const, id: 't0', partitions: s.partitions }],
    producers: [
      {
        kind: 'producer' as const,
        id: 'p0',
        ratePerSec: s.ratePerSec,
        keyStrategy: { type: 'null' as const },
      },
    ],
    consumers: ids.map((id) => consumer(id, s.settings)),
    connections: [{ from: 'p0', to: 't0' }, ...ids.map((id) => ({ from: 't0', to: id }))],
  };
  // Then no command for long enough to settle. A cooperative move takes two rebalances (KIP-429),
  // each up to the rebalance timeout R; in between, the revoking member needs up to two batches to
  // rejoin, and afterwards each member one batch before poll() applies the assignment. With the
  // precondition below (2 batches < R), that is under 4 R. Plus a session timeout for crashed
  // members to be noticed and the initial delay for a group that emptied.
  const { maxPollIntervalMs, sessionTimeoutMs } = s.settings;
  const settle = 4 * maxPollIntervalMs + sessionTimeoutMs + 2 * s.initialDelay + 2000;
  return runScenario(seed, topology, [...steps, { kind: 'advance', ms: settle }]);
}

/**
 * Cases per run: KP_LIVENESS_RUNS, default 50. Each case simulates minutes of a busy group, and
 * coverage instrumentation makes that about 4× slower, so the PR job runs 50; the nightly job
 * (.github/workflows/nightly.yml) runs 1000 without coverage. That deep search is how L1–L3 were
 * found: 1500 and 2000 cases take about 4 and 6 minutes locally.
 */
const RUNS = runsFromEnv('KP_LIVENESS_RUNS', 50);

/** About 0.3 s per case without coverage, 1.2 s with; generous so slow CI runners pass too. */
const TIMEOUT_MS = Math.max(300_000, RUNS * 2_000);

describe('consumer group liveness (fast-check)', { timeout: TIMEOUT_MS }, () => {
  it('busy members that can join within the rebalance timeout end in a Stable group, never removed', () => {
    fc.assert(
      fc.property(seedArb, scenarioArb, (seed, scenario) => {
        const { settings } = scenario.s;
        // How long a member may take to send JoinGroup once a rebalance starts (M4-decisions
        // L1/L2). It learns of the rebalance from a Heartbeat answer (REBALANCE_IN_PROGRESS), up
        // to heartbeat.interval.ms later, and may have just started a batch it must finish first;
        // then it polls. An eager member (or one without auto-commit) joins at that poll(). A
        // cooperative member with auto-commit sends onJoinPrepare's commit there, and that same
        // poll() returns the next batch (it keeps fetching what it owns), so its JoinGroup only
        // goes out one batch later (ConsumerCoordinator#onJoinPrepare, K12). One more second
        // covers network round trips and commits.
        const batchMs = settings.maxPollRecords * settings.processingTimeMs;
        const batches =
          settings.enableAutoCommit &&
          rebalanceProtocol(settings.partitionAssignmentStrategy) === 'cooperative'
            ? 2
            : 1;
        fc.pre(
          batches * batchMs + settings.heartbeatIntervalMs + 1000 < settings.maxPollIntervalMs,
        );
        const { engine, events } = run(seed, scenario);

        const snapshot = engine.getSnapshot();
        const group = snapshot.groups.find((g) => g.groupId === 'g');
        const running = snapshot.consumers.filter(
          (c) => c.status !== 'stopped' && c.status !== 'crashed' && c.config.groupId === 'g',
        );
        // Consumers that ran the whole time (never killed, stopped or restarted) must never be
        // removed, for any reason: the precondition rules out the poll and join/sync timeouts,
        // and their heartbeat threads keep the session alive (this also catches heartbeat bugs).
        const steady = new Set(running.map((c) => c.config.id));
        for (const e of events) {
          if (e.type === 'ConsumerStopped' || e.type === 'ConsumerStarted') {
            steady.delete(e.consumerId);
          }
        }
        const removed = events.filter(
          (e): e is Extract<EngineEvent, { type: 'MemberRemoved' }> => e.type === 'MemberRemoved',
        );
        for (const e of removed) expect(steady.has(e.consumerId), JSON.stringify(e)).toBe(false);
        if (running.length === 0) return;
        expect(group?.state).toBe('Stable');
        for (const c of running) {
          expect(['stable', 'idle'], c.config.id).toContain(c.status);
          expect(group?.members.map((m) => m.memberId)).toContain(c.memberId);
        }
      }),
      { numRuns: RUNS },
    );
  });

  it('busy groups keep the commit and cooperative-coverage invariants', () => {
    // The general arbitraries rarely make a rebalance overlap processing; these runs do, so the
    // windows D3 allows (commits before onJoinComplete) and cooperative moves happen often.
    fc.assert(
      fc.property(seedArb, scenarioArb, (seed, scenario) => {
        const { events, networkDelayMs } = run(seed, scenario);
        expect(commitViolations(events, networkDelayMs)).toEqual([]);
        expect(cooperativeCoverageViolations(events)).toEqual([]);
      }),
      { numRuns: RUNS },
    );
  });
});

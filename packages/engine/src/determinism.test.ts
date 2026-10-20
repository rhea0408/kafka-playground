/**
 * Determinism (NFR-4.1, FR-2.15, CLAUDE.md rule 2): the same seed + topology + commands must give
 * a byte-identical event log. Runs in CI with every `pnpm test`.
 */

import type { TopologyInput } from '@kp/schema';
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import type { Command } from './commands/index.ts';
import { replayCommandLog } from './engine.ts';
import type { EngineEvent } from './events/index.ts';
import { stepsArb, topologyArb, type Step } from './testing/arbitraries.ts';
import { hashEvents, runScenario } from './testing/run.ts';

const topology: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1' }],
  topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
  producers: [
    {
      kind: 'producer',
      id: 'p1',
      ratePerSec: 7,
      keyStrategy: { type: 'random', keys: ['A', 'B', 'C'] },
    },
    { kind: 'producer', id: 'p2', ratePerSec: 3, acks: '0' },
  ],
  consumers: [
    {
      kind: 'consumer',
      id: 'c1',
      autoOffsetReset: 'earliest',
      autoCommitIntervalMs: 1500,
      processingTimeMs: 30,
    },
  ],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'p2', to: 'orders' },
    { from: 'orders', to: 'c1' },
  ],
};

const cmd = (command: Command): Step => ({ kind: 'command', command });
const advance = (ms: number): Step => ({ kind: 'advance', ms });

const steps: Step[] = [
  advance(2500),
  cmd({ type: 'ProduceManual', producerId: 'p1', key: 'Z', value: 'manual' }),
  advance(1500),
  cmd({ type: 'KillConsumer', id: 'c1' }),
  advance(3000),
  cmd({ type: 'RestartConsumer', id: 'c1' }),
  advance(2000),
  cmd({ type: 'Disconnect', from: 'p2', to: 'orders' }),
  advance(3000),
];

describe('determinism', () => {
  it('two runs with the same seed and commands give byte-identical event logs', () => {
    const a = runScenario(42, topology, steps);
    const b = runScenario(42, topology, steps);
    expect(a.events.length).toBeGreaterThan(200);
    expect(JSON.stringify(b.events)).toBe(JSON.stringify(a.events));
    expect(hashEvents(b.events)).toBe(hashEvents(a.events));
  });

  it('a different seed gives a different run', () => {
    expect(hashEvents(runScenario(43, topology, steps).events)).not.toBe(
      hashEvents(runScenario(42, topology, steps).events),
    );
  });

  it('replaying the recorded command log reproduces the event-log hash', () => {
    const original = runScenario(42, topology, steps);
    original.engine.runUntil(original.endTime);
    const replayed: EngineEvent[] = [];
    replayCommandLog({ seed: 42, topology }, original.engine.commandLog, (e) =>
      replayed.push(e),
    ).runUntil(original.endTime);
    expect(hashEvents(replayed)).toBe(hashEvents(original.events));
  });

  it('holds for random topologies and command sequences', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 0xffffffff }),
        topologyArb,
        stepsArb,
        (seed, topo, s) => {
          expect(hashEvents(runScenario(seed, topo, s).events)).toBe(
            hashEvents(runScenario(seed, topo, s).events),
          );
        },
      ),
      { numRuns: 100 },
    );
  });
});

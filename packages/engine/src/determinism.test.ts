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
import { clusterSteps, clusterTopology } from './testing/scenes.ts';

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
      groupId: 'g',
      autoOffsetReset: 'earliest',
      autoCommitIntervalMs: 1500,
      processingTimeMs: 30,
    },
    // A second member of the same group (M3): rebalances, crashes and commits by generation.
    {
      kind: 'consumer',
      id: 'c2',
      groupId: 'g',
      autoOffsetReset: 'earliest',
      processingTimeMs: 10,
      commitMode: 'sync-after-processing',
      enableAutoCommit: false,
    },
  ],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'p2', to: 'orders' },
    { from: 'orders', to: 'c1' },
    { from: 'orders', to: 'c2' },
  ],
};

const cmd = (command: Command): Step => ({ kind: 'command', command });
const advance = (ms: number): Step => ({ kind: 'advance', ms });

const steps: Step[] = [
  advance(8000), // the first rebalance (two initial-delay windows) and some consuming
  cmd({ type: 'ProduceManual', producerId: 'p1', key: 'Z', value: 'manual' }),
  advance(1500),
  cmd({ type: 'KillConsumer', id: 'c1' }),
  advance(3000),
  cmd({ type: 'RestartConsumer', id: 'c1' }),
  advance(2000),
  cmd({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 5 } }),
  cmd({ type: 'UpdateConfig', id: 'p1', patch: { ratePerSec: 11, metadataMaxAgeMs: 1000 } }),
  cmd({ type: 'UpdateConfig', id: 'c1', patch: { fetchMinBytes: 4096, maxPollRecords: 3 } }),
  advance(2000),
  cmd({ type: 'Disconnect', from: 'p2', to: 'orders' }),
  advance(3000),
  // Group steps (M3-T6): a crash detected by session timeout, a graceful stop, a third member
  // with another assignor list, and a group.id change.
  cmd({ type: 'KillConsumer', id: 'c2' }),
  advance(12_000),
  cmd({ type: 'RestartConsumer', id: 'c2' }),
  advance(4000),
  cmd({
    type: 'AddComponent',
    component: {
      kind: 'consumer',
      id: 'c3',
      groupId: 'g',
      partitionAssignmentStrategy: ['cooperative-sticky', 'range'],
    },
  }),
  cmd({ type: 'Connect', from: 'orders', to: 'c3' }),
  advance(5000),
  cmd({ type: 'StopConsumer', id: 'c1' }),
  advance(3000),
  cmd({ type: 'UpdateConfig', id: 'c3', patch: { groupId: 'other' } }),
  advance(5000),
];

// Generous: the random-topology property takes ~12 s under coverage (PR CI), more on slow runners.
describe('determinism', { timeout: 90_000 }, () => {
  it('two runs with the same seed and commands give byte-identical event logs', () => {
    const a = runScenario(42, topology, steps);
    const b = runScenario(42, topology, steps);
    expect(a.events.length).toBeGreaterThan(200);
    // The group steps really happen: a crash found by session timeout, a graceful leave, a
    // group.id change, and several generations.
    const removed = a.events.flatMap((e) => (e.type === 'MemberRemoved' ? [e.reason] : []));
    expect(removed).toEqual(expect.arrayContaining(['session-timeout', 'leave']));
    expect(a.events.filter((e) => e.type === 'RebalanceCompleted').length).toBeGreaterThan(4);
    expect(a.events.some((e) => e.type === 'MemberJoined' && e.groupId === 'other')).toBe(true);
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

describe('determinism with broker failures (M4-T9)', { timeout: 90_000 }, () => {
  it('same seed and commands give the same event log, with elections, truncation and moves', () => {
    const a = runScenario(7, clusterTopology, clusterSteps);
    const b = runScenario(7, clusterTopology, clusterSteps);
    const types = new Set(a.events.map((e) => e.type));
    for (const t of [
      'BrokerFenced',
      'BrokerUnfenced',
      'LeaderElected',
      'IsrShrunk',
      'IsrExpanded',
      'ProduceRetried',
      'ReplicaFetched',
    ]) {
      expect(types.has(t as EngineEvent['type'])).toBe(true);
    }
    expect(hashEvents(b.events)).toBe(hashEvents(a.events));
    const replayed: EngineEvent[] = [];
    replayCommandLog({ seed: 7, topology: clusterTopology }, a.engine.commandLog, (e) =>
      replayed.push(e),
    ).runUntil(a.endTime);
    expect(hashEvents(replayed)).toBe(hashEvents(a.events));
  });
});

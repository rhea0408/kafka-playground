/** Scenes shared by tests and benchmarks (test-only). */

import type { TopologyInput } from '@kp/schema';
import type { Command } from '../commands/index.ts';
import type { Step } from './arbitraries.ts';

const cmd = (command: Command): Step => ({ kind: 'command', command });
const advance = (ms: number): Step => ({ kind: 'advance', ms });

/**
 * M4-T9: a three-broker cluster with replication, broker crashes, controlled shutdowns, restarts,
 * coordinator moves and idempotent retries. The worker hash e2e uses the same idea in the browser.
 */
export const clusterTopology: TopologyInput = {
  brokers: ['b1', 'b2', 'b3'].map((id) => ({ kind: 'broker' as const, id })),
  topics: [
    { kind: 'topic', id: 'orders', partitions: 3, replicationFactor: 3, minInsyncReplicas: 2 },
    {
      kind: 'topic',
      id: 'audit',
      partitions: 1,
      replicationFactor: 2,
      uncleanLeaderElectionEnable: true,
    },
  ],
  producers: [
    {
      kind: 'producer',
      id: 'p1',
      ratePerSec: 20,
      keyStrategy: { type: 'random', keys: ['D', 'E', 'F'] },
    },
    { kind: 'producer', id: 'p2', ratePerSec: 5, acks: '1' },
  ],
  consumers: [
    { kind: 'consumer', id: 'c1', groupId: 'orders-app', autoOffsetReset: 'earliest' },
    { kind: 'consumer', id: 'c2', groupId: 'orders-app', autoOffsetReset: 'earliest' },
  ],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'p2', to: 'audit' },
    { from: 'orders', to: 'c1' },
    { from: 'orders', to: 'c2' },
  ],
};

export const clusterSteps: Step[] = [
  advance(8000),
  cmd({ type: 'KillBroker', id: 'b2' }),
  advance(12_000),
  cmd({ type: 'StopBroker', id: 'b1' }),
  advance(3000),
  cmd({ type: 'RestartBroker', id: 'b2' }),
  advance(5000),
  cmd({ type: 'RestartBroker', id: 'b1' }),
  advance(4000),
  cmd({ type: 'KillBroker', id: 'b3' }),
  advance(2000),
  cmd({ type: 'RestartBroker', id: 'b3' }),
  advance(35_000),
];

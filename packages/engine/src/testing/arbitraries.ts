/**
 * fast-check generators for random engine runs: a valid topology plus a sequence of steps that
 * either advance sim time or dispatch a command (sometimes an invalid one, which must be rejected
 * without side effects). Test-only; not exported from the package.
 */

import type { TopologyInput } from '@kp/schema';
import fc from 'fast-check';
import type { Command } from '../commands/index.ts';

export type Step =
  | { readonly kind: 'advance'; readonly ms: number }
  | { readonly kind: 'command'; readonly command: Command };

const keyStrategy = fc.oneof(
  fc.constant({ type: 'null' as const }),
  fc.record({ type: fc.constant('fixed' as const), key: fc.constantFrom('A', 'B', 'é') }),
  fc.record({
    type: fc.constant('random' as const),
    keys: fc.uniqueArray(fc.constantFrom('A', 'B', 'C', 'D', 'customer-7'), {
      minLength: 1,
      maxLength: 4,
    }),
  }),
  fc.record({
    type: fc.constant('skewed' as const),
    hotKey: fc.constant('HOT'),
    hotShare: fc.double({ min: 0, max: 1, noNaN: true }),
    otherKeys: fc.constant<string[]>(['x', 'y']),
  }),
);

export const topologyArb: fc.Arbitrary<TopologyInput> = fc
  .record({
    topics: fc.array(fc.integer({ min: 1, max: 4 }), { minLength: 1, maxLength: 2 }),
    producers: fc.array(
      fc.record({
        ratePerSec: fc.integer({ min: 1, max: 20 }),
        keyStrategy,
        acks: fc.constantFrom('0' as const, '1' as const, 'all' as const),
        batchSize: fc.integer({ min: 1, max: 8192 }),
        messageSizeBytes: fc.integer({ min: 100, max: 2048 }),
        metadataMaxAgeMs: fc.oneof(fc.constant(300_000), fc.integer({ min: 0, max: 3000 })),
        topicMask: fc.integer({ min: 1, max: 3 }),
      }),
      { minLength: 1, maxLength: 2 },
    ),
    consumers: fc.array(
      fc.record({
        autoOffsetReset: fc.constantFrom('earliest' as const, 'latest' as const),
        enableAutoCommit: fc.boolean(),
        autoCommitIntervalMs: fc.integer({ min: 100, max: 3000 }),
        maxPollRecords: fc.integer({ min: 1, max: 50 }),
        fetchMinBytes: fc.oneof(fc.constant(1), fc.integer({ min: 0, max: 5000 })),
        fetchMaxWaitMs: fc.oneof(fc.constant(500), fc.integer({ min: 0, max: 1000 })),
        metadataMaxAgeMs: fc.oneof(fc.constant(300_000), fc.integer({ min: 0, max: 3000 })),
        processingTimeMs: fc.integer({ min: 0, max: 200 }),
        topicMask: fc.integer({ min: 1, max: 3 }),
      }),
      { maxLength: 2 },
    ),
  })
  .map(({ topics, producers, consumers }) => {
    const topicIds = topics.map((_, i) => `t${String(i)}`);
    const picked = (mask: number) => topicIds.filter((_, i) => (mask & (1 << i)) !== 0);
    return {
      brokers: [{ kind: 'broker' as const, id: 'b1' }],
      topics: topics.map((partitions, i) => ({
        kind: 'topic' as const,
        id: `t${String(i)}`,
        partitions,
      })),
      producers: producers.map(({ topicMask, ...p }, i) => ({
        kind: 'producer' as const,
        id: `p${String(i)}`,
        ...p,
      })),
      consumers: consumers.map(({ topicMask, ...c }, i) => ({
        kind: 'consumer' as const,
        id: `c${String(i)}`,
        ...c,
      })),
      connections: [
        ...producers.flatMap((p, i) =>
          picked(p.topicMask).map((to) => ({ from: `p${String(i)}`, to })),
        ),
        ...consumers.flatMap((c, i) =>
          picked(c.topicMask).map((from) => ({ from, to: `c${String(i)}` })),
        ),
      ],
    };
  });

const ids = fc.constantFrom('p0', 'p1', 'c0', 'c1', 't0', 't1', 'b1', 'ghost');

const commandArb: fc.Arbitrary<Command> = fc.oneof(
  ids.map((id): Command => ({ type: 'KillConsumer', id })),
  ids.map((id): Command => ({ type: 'RestartConsumer', id })),
  fc
    .record({ producerId: ids, key: fc.option(fc.constantFrom('A', 'B', 'Z'), { nil: null }) })
    .map(({ producerId, key }): Command => ({
      type: 'ProduceManual',
      producerId,
      key,
      value: 'manual',
    })),
  fc.record({ from: ids, to: ids }).map(({ from, to }): Command => ({ type: 'Connect', from, to })),
  fc
    .record({ from: ids, to: ids })
    .map(({ from, to }): Command => ({ type: 'Disconnect', from, to })),
  ids.map((id): Command => ({ type: 'RemoveComponent', id })),
  fc.constantFrom('c0', 'c1').map((id): Command => ({
    type: 'AddComponent',
    component: { kind: 'consumer', id, autoOffsetReset: 'earliest' },
  })),
  // UpdateConfig: growing (or, rejected, shrinking) topics, producer and consumer settings,
  // and changes that must be rejected (group.id, broker.rack, unknown ids).
  fc
    .record({ id: fc.constantFrom('t0', 't1'), partitions: fc.integer({ min: 1, max: 6 }) })
    .map(({ id, partitions }): Command => ({ type: 'UpdateConfig', id, patch: { partitions } })),
  fc
    .record({
      id: fc.constantFrom('p0', 'p1', 'ghost'),
      patch: fc.record(
        {
          ratePerSec: fc.integer({ min: 1, max: 30 }),
          acks: fc.constantFrom('0' as const, '1' as const, 'all' as const),
          keyStrategy,
          batchSize: fc.integer({ min: 1, max: 8192 }),
          messageSizeBytes: fc.integer({ min: 100, max: 2048 }),
          metadataMaxAgeMs: fc.integer({ min: 0, max: 3000 }),
        },
        { requiredKeys: [] },
      ),
    })
    .map(({ id, patch }): Command => ({ type: 'UpdateConfig', id, patch })),
  fc
    .record({
      id: fc.constantFrom('c0', 'c1'),
      patch: fc.record(
        {
          maxPollRecords: fc.integer({ min: 1, max: 50 }),
          fetchMinBytes: fc.integer({ min: 0, max: 5000 }),
          fetchMaxWaitMs: fc.integer({ min: 0, max: 1000 }),
          processingTimeMs: fc.integer({ min: 0, max: 200 }),
          enableAutoCommit: fc.boolean(),
          autoCommitIntervalMs: fc.integer({ min: 100, max: 3000 }),
          metadataMaxAgeMs: fc.integer({ min: 0, max: 3000 }),
          groupId: fc.constant('other-group'),
        },
        { requiredKeys: [] },
      ),
    })
    .map(({ id, patch }): Command => ({ type: 'UpdateConfig', id, patch })),
  fc.constant<Command>({ type: 'UpdateConfig', id: 'b1', patch: { rack: 'az-1' } }),
  // Re-adding a removed producer or topic with the same id (a new life / a re-created topic).
  fc.constantFrom('p0', 'p1').map((id): Command => ({
    type: 'AddComponent',
    component: {
      kind: 'producer',
      id,
      ratePerSec: 5,
      keyStrategy: { type: 'random', keys: ['A', 'B'] },
    },
  })),
  fc
    .record({ id: fc.constantFrom('t0', 't1'), partitions: fc.integer({ min: 1, max: 4 }) })
    .map(({ id, partitions }): Command => ({
      type: 'AddComponent',
      component: { kind: 'topic', id, partitions },
    })),
  // Invalid on purpose: must be rejected without changing anything.
  fc.constant<Command>({ type: 'AddComponent', component: { kind: 'topic', id: 'bad name' } }),
);

export const stepsArb: fc.Arbitrary<Step[]> = fc.array(
  fc.oneof(
    {
      weight: 3,
      arbitrary: fc.integer({ min: 0, max: 2000 }).map((ms): Step => ({ kind: 'advance', ms })),
    },
    { weight: 2, arbitrary: commandArb.map((command): Step => ({ kind: 'command', command })) },
  ),
  { maxLength: 25 },
);

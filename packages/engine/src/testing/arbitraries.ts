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

const assignors = fc.constantFrom(
  ['range', 'cooperative-sticky'] as const,
  ['range'] as const,
  ['roundrobin'] as const,
  ['sticky'] as const,
  ['cooperative-sticky'] as const,
  ['cooperative-sticky', 'range'] as const,
);

const commitMode = fc.constantFrom(
  'auto' as const,
  'sync-after-processing' as const,
  'async-after-processing' as const,
  'sync-before-processing' as const,
);

/** Consumer group settings (M3). Always valid: heartbeat < 6000 ≤ session timeout. */
const groupFields = {
  sessionTimeoutMs: fc.integer({ min: 6000, max: 20_000 }),
  heartbeatIntervalMs: fc.integer({ min: 500, max: 5000 }),
  maxPollIntervalMs: fc.integer({ min: 1000, max: 40_000 }),
  partitionAssignmentStrategy: assignors.map((list) => [...list]),
  commitMode,
};

/** Producer retry settings (M4-T1). Always valid: delivery.timeout.ms ≥ request.timeout.ms. */
const producerRetryFields = {
  requestTimeoutMs: fc.integer({ min: 500, max: 10_000 }),
  deliveryTimeoutMs: fc.integer({ min: 10_000, max: 40_000 }),
  retryBackoffMs: fc.integer({ min: 0, max: 500 }),
  retryBackoffMaxMs: fc.integer({ min: 500, max: 2000 }),
};

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
    topics: fc.array(
      fc.record({
        partitions: fc.integer({ min: 1, max: 4 }),
        unclean: fc.boolean(),
        rf: fc.integer({ min: 1, max: 3 }),
        minIsr: fc.constantFrom(1, 1, 2, 3),
      }),
      { minLength: 1, maxLength: 2 },
    ),
    producers: fc.array(
      fc.record({
        ratePerSec: fc.integer({ min: 1, max: 20 }),
        keyStrategy,
        acks: fc.constantFrom('0' as const, '1' as const, 'all' as const),
        batchSize: fc.integer({ min: 1, max: 8192 }),
        messageSizeBytes: fc.integer({ min: 100, max: 2048 }),
        metadataMaxAgeMs: fc.oneof(fc.constant(300_000), fc.integer({ min: 0, max: 3000 })),
        ...producerRetryFields,
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
        // Sometimes slow enough to exceed a short max.poll.interval.ms (a zombie).
        processingTimeMs: fc.oneof(
          { weight: 4, arbitrary: fc.integer({ min: 0, max: 200 }) },
          { weight: 1, arbitrary: fc.integer({ min: 500, max: 3000 }) },
        ),
        ...groupFields,
        // Most consumers share group "g", so groups with several members are common.
        groupId: fc.constantFrom('g', 'g', 'h'),
        /** Use the topology's shared group settings (most of the time) or this consumer's own. */
        ownGroupSettings: fc.oneof(
          { weight: 1, arbitrary: fc.constant(true) },
          { weight: 4, arbitrary: fc.constant(false) },
        ),
        topicMask: fc.integer({ min: 1, max: 3 }),
      }),
      { maxLength: 3 },
    ),
    // Members of one group usually share the assignor list and timeouts, as instances of one
    // application do; otherwise most groups end in INCONSISTENT_GROUP_PROTOCOL.
    shared: fc.record(groupFields),
    initialDelay: fc.constantFrom(0, 0, 3000),
    // M4-T2: mostly one broker as before; sometimes two or three with replicated topics.
    brokerCount: fc.constantFrom(1, 1, 2, 3),
  })
  .map(({ topics, producers, consumers, shared, initialDelay, brokerCount }) => {
    const topicIds = topics.map((_, i) => `t${String(i)}`);
    const picked = (mask: number) => topicIds.filter((_, i) => (mask & (1 << i)) !== 0);
    return {
      brokers: Array.from({ length: brokerCount }, (_, i) => ({
        kind: 'broker' as const,
        id: `b${String(i + 1)}`,
        groupInitialRebalanceDelayMs: initialDelay,
      })),
      topics: topics.map(({ partitions, unclean, rf, minIsr }, i) => ({
        kind: 'topic' as const,
        id: `t${String(i)}`,
        partitions,
        replicationFactor: Math.min(rf, brokerCount),
        minInsyncReplicas: minIsr,
        uncleanLeaderElectionEnable: unclean,
      })),
      producers: producers.map(({ topicMask, ...p }, i) => ({
        kind: 'producer' as const,
        id: `p${String(i)}`,
        ...p,
      })),
      consumers: consumers.map(({ topicMask, ownGroupSettings, ...c }, i) => ({
        kind: 'consumer' as const,
        id: `c${String(i)}`,
        ...c,
        ...(ownGroupSettings ? {} : shared),
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

const ids = fc.constantFrom('p0', 'p1', 'c0', 'c1', 'c2', 't0', 't1', 'b1', 'b2', 'ghost');

const commandArb: fc.Arbitrary<Command> = fc.oneof(
  ids.map((id): Command => ({ type: 'KillConsumer', id })),
  ids.map((id): Command => ({ type: 'RestartConsumer', id })),
  ids.map((id): Command => ({ type: 'StopConsumer', id })),
  // Aimed at consumers, so crashes, stops and restarts of group members are common.
  fc
    .record({
      type: fc.constantFrom(
        'KillConsumer' as const,
        'KillConsumer' as const,
        'StopConsumer' as const,
        'RestartConsumer' as const,
      ),
      id: fc.constantFrom('c0', 'c1', 'c2'),
    })
    .map(({ type, id }): Command => ({ type, id })),
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
  fc
    .record({
      id: fc.constantFrom('c0', 'c1', 'c2', 'c3'),
      groupId: fc.constantFrom('g', 'h'),
      partitionAssignmentStrategy: assignors.map((list) => [...list]),
    })
    .map(({ id, groupId, partitionAssignmentStrategy }): Command => ({
      type: 'AddComponent',
      component: {
        kind: 'consumer',
        id,
        groupId,
        autoOffsetReset: 'earliest',
        partitionAssignmentStrategy,
      },
    })),
  // Joining a member to a topic (a subscription change).
  fc
    .record({ from: fc.constantFrom('t0', 't1'), to: fc.constantFrom('c0', 'c1', 'c2', 'c3') })
    .map(({ from, to }): Command => ({ type: 'Connect', from, to })),
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
          ...producerRetryFields,
        },
        { requiredKeys: [] },
      ),
    })
    .map(({ id, patch }): Command => ({ type: 'UpdateConfig', id, patch })),
  fc
    .record({
      id: fc.constantFrom('c0', 'c1', 'c2'),
      patch: fc.record(
        {
          maxPollRecords: fc.integer({ min: 1, max: 50 }),
          fetchMinBytes: fc.integer({ min: 0, max: 5000 }),
          fetchMaxWaitMs: fc.integer({ min: 0, max: 1000 }),
          processingTimeMs: fc.integer({ min: 0, max: 200 }),
          enableAutoCommit: fc.boolean(),
          autoCommitIntervalMs: fc.integer({ min: 100, max: 3000 }),
          metadataMaxAgeMs: fc.integer({ min: 0, max: 3000 }),
          groupId: fc.constantFrom('g', 'h', 'other-group'),
          autoOffsetReset: fc.constantFrom('earliest' as const, 'latest' as const),
          ...groupFields,
        },
        { requiredKeys: [] },
      ),
    })
    .map(({ id, patch }): Command => ({ type: 'UpdateConfig', id, patch })),
  fc.constant<Command>({ type: 'UpdateConfig', id: 'b1', patch: { rack: 'az-1' } }),
  // Broker crashes, controlled shutdowns and restarts (M4-T1; effects from M4-T5).
  fc
    .record({
      type: fc.constantFrom('KillBroker' as const, 'StopBroker' as const, 'RestartBroker' as const),
      id: fc.constantFrom('b1', 'b2', 'b3', 'ghost'),
    })
    .map(({ type, id }): Command => ({ type, id })),
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
    // Now and then long enough for a session or poll timeout to expire.
    {
      weight: 1,
      arbitrary: fc
        .integer({ min: 5000, max: 12_000 })
        .map((ms): Step => ({ kind: 'advance', ms })),
    },
    { weight: 2, arbitrary: commandArb.map((command): Step => ({ kind: 'command', command })) },
  ),
  // fast-check's default "small" size keeps arrays at about ten steps; groups need longer runs.
  { minLength: 5, maxLength: 30, size: 'medium' },
);

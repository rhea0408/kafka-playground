/**
 * Zod schemas for canvas components and their config. Defaults are the real Kafka 4.x defaults
 * unless marked "game setting": those have no Kafka equivalent and exist only for the simulation.
 * Field names are camelCase versions of the Kafka config names so the code panel (M7) can map them.
 */

import { z } from 'zod';

/** Kafka topic name rules (org.apache.kafka.common.internals.Topic#validate). */
export const TOPIC_NAME_MAX_LENGTH = 249;
const TOPIC_NAME_LEGAL = /^[a-zA-Z0-9._-]+$/;

export const TopicNameSchema = z
  .string()
  .min(1, { error: 'Topic name must not be empty.' })
  .max(TOPIC_NAME_MAX_LENGTH, {
    error: `Topic name must be at most ${String(TOPIC_NAME_MAX_LENGTH)} characters.`,
  })
  .regex(TOPIC_NAME_LEGAL, {
    error: 'Topic name may only contain letters, digits, ".", "_" and "-".',
  })
  .refine((name) => name !== '.' && name !== '..', {
    error: 'Topic name cannot be "." or "..".',
  });

/** Ids for brokers, producers and consumers. Same character set as topic names, shorter. */
export const ComponentIdSchema = z.string().regex(/^[a-zA-Z0-9._-]{1,64}$/, {
  error: 'Id must be 1-64 characters: letters, digits, ".", "_" or "-".',
});

const positiveInt = (what: string) =>
  z
    .int({ error: `${what} must be a whole number.` })
    .min(1, { error: `${what} must be at least 1.` });

/** Integer.MAX_VALUE, the producer's default `retries`. */
export const INT_MAX = 2_147_483_647;

const nonNegativeInt = (what: string) =>
  z
    .int({ error: `${what} must be a whole number.` })
    .min(0, { error: `${what} cannot be negative.` });

// ── Broker ────────────────────────────────────────────────────────────────────────────────────

export const BrokerSchema = z.object({
  kind: z.literal('broker'),
  id: ComponentIdSchema,
  /** broker.rack. Unused until rack-aware placement (v0.5). */
  rack: z.string().nullable().default(null),
  /**
   * group.initial.rebalance.delay.ms, default 3000: "The amount of time the group coordinator will
   * wait for more consumers to join a new group before performing the first rebalance. A longer
   * delay means potentially fewer rebalances, but increases the time until processing begins."
   * A static broker config (read-only in M3).
   */
  groupInitialRebalanceDelayMs: nonNegativeInt('group.initial.rebalance.delay.ms').default(3000),
  /**
   * replica.lag.time.max.ms. Kafka's default is 30000: "If a follower hasn't sent any fetch
   * requests or hasn't consumed up to the leader's log end offset for at least this time, the
   * leader will remove the follower from isr". The game default is 10000 (game-tuned, M4 Q5).
   */
  replicaLagTimeMaxMs: positiveInt('replica.lag.time.max.ms').default(10_000),
  /**
   * replica.fetch.wait.max.ms, default 500: "The maximum wait time for each fetcher request issued
   * by follower replicas. This value should always be less than the replica.lag.time.max.ms at all
   * times to prevent frequent shrinking of ISR for low throughput topics." Followers long-poll the
   * leader with it (M4 Q4).
   */
  replicaFetchWaitMaxMs: nonNegativeInt('replica.fetch.wait.max.ms').default(500),
  /**
   * offsets.topic.replication.factor, default 3: the replication factor of __consumer_offsets.
   * Real Kafka cannot create the topic while fewer brokers are alive; the engine uses
   * min(this, live brokers) when it creates the topic (game-tuned, M4 Q9). Read-only.
   */
  offsetsTopicReplicationFactor: positiveInt('offsets.topic.replication.factor').default(3),
  /**
   * auto.leader.rebalance.enable, default true: the controller moves leadership back to the
   * preferred replica (the first in the replica list) when it is in sync (M4 Q10).
   */
  autoLeaderRebalanceEnable: z.boolean().default(true),
  /**
   * leader.imbalance.check.interval.seconds. Kafka's default is 300; the game default is 30
   * (game-tuned, M4 Q10): how often the controller checks for partitions led by a replica other
   * than the preferred one.
   */
  leaderImbalanceCheckIntervalSeconds: positiveInt(
    'leader.imbalance.check.interval.seconds',
  ).default(30),
});

/**
 * The broker's group.min.session.timeout.ms (6000) and group.max.session.timeout.ms (1800000)
 * defaults. A JoinGroup whose session.timeout.ms is outside them fails with
 * INVALID_SESSION_TIMEOUT (GroupMetadataManager#classicGroupJoin).
 */
export const GROUP_MIN_SESSION_TIMEOUT_MS = 6000;
export const GROUP_MAX_SESSION_TIMEOUT_MS = 1_800_000;

// ── Topic ─────────────────────────────────────────────────────────────────────────────────────

export const TopicSchema = z.object({
  kind: z.literal('topic'),
  /** The topic name doubles as its component id. */
  id: TopicNameSchema,
  /** num.partitions, default 1. */
  partitions: positiveInt('Partitions').default(1),
  /** default.replication.factor, default 1. */
  replicationFactor: positiveInt('Replication factor').default(1),
  /**
   * min.insync.replicas, default 1: "When a producer sets acks to "all" (or "-1"),
   * min.insync.replicas specifies the minimum number of replicas that must acknowledge a write for
   * the write to be considered successful." Kafka accepts a value above the replication factor;
   * every acks=all write then fails with NOT_ENOUGH_REPLICAS (the panel warns, M4 T1).
   */
  minInsyncReplicas: positiveInt('min.insync.replicas').default(1),
  /**
   * unclean.leader.election.enable, default false: "Indicates whether to enable replicas not in
   * the ISR set to be elected as leader as a last resort, even though doing so may result in data
   * loss." (M4 Q6)
   */
  uncleanLeaderElectionEnable: z.boolean().default(false),
});

// ── Producer ──────────────────────────────────────────────────────────────────────────────────

/** How the simulated producer picks record keys (FR-1 palette: null / fixed / random / skewed). */
export const KeyStrategySchema = z.discriminatedUnion('type', [
  /** No key: the sticky partitioner spreads records over partitions. */
  z.object({ type: z.literal('null') }),
  /** Every record uses the same key. */
  z.object({ type: z.literal('fixed'), key: z.string() }),
  /** Each record picks a key uniformly from `keys`. */
  z.object({
    type: z.literal('random'),
    keys: z.array(z.string()).min(1, { error: 'Random key strategy needs at least one key.' }),
  }),
  /** `hotKey` is used for a `hotShare` fraction of records; the rest pick from `otherKeys`. */
  z.object({
    type: z.literal('skewed'),
    hotKey: z.string(),
    hotShare: z
      .number()
      .min(0, { error: 'Hot-key share must be between 0 and 1.' })
      .max(1, { error: 'Hot-key share must be between 0 and 1.' }),
    otherKeys: z.array(z.string()).min(1, { error: 'Skewed key strategy needs other keys.' }),
  }),
]);

export const AcksSchema = z.enum(['0', '1', 'all'], { error: 'acks must be "0", "1" or "all".' });

export const ProducerSchema = z
  .object({
    kind: z.literal('producer'),
    id: ComponentIdSchema,
    /** Game setting: records per second of sim time. */
    ratePerSec: z
      .number()
      .positive({ error: 'Rate must be greater than 0.' })
      .max(1000, { error: 'Rate is limited to 1000 msg/s per producer in per-message mode.' })
      .default(1),
    keyStrategy: KeyStrategySchema.default({ type: 'null' }),
    /** acks, default "all" since Kafka 3.0 (KIP-679). */
    acks: AcksSchema.default('all'),
    /** batch.size in bytes, default 16384. Drives when the sticky partitioner switches. */
    batchSize: positiveInt('batch.size').default(16384),
    /** Game setting: serialized size of each record in bytes. */
    messageSizeBytes: positiveInt('Message size').default(1024),
    /**
     * metadata.max.age.ms, default 300000 (5 min): how often the client refreshes its cluster
     * metadata even without errors, which is when it notices partitions added to a topic
     * (CommonClientConfigs METADATA_MAX_AGE_DOC).
     */
    metadataMaxAgeMs: nonNegativeInt('metadata.max.age.ms').default(300_000),
    /**
     * retries, default Integer.MAX_VALUE (shown as "∞"): how often a send is retried after a
     * retriable error. In practice delivery.timeout.ms ends the retries first.
     */
    retries: nonNegativeInt('retries').max(INT_MAX).default(INT_MAX),
    /**
     * delivery.timeout.ms. Kafka's default is 120000: "An upper bound on the time to report success
     * or failure after a call to send() returns." The game default is 30000 (game-tuned, M4 Q7).
     */
    deliveryTimeoutMs: positiveInt('delivery.timeout.ms').default(30_000),
    /**
     * request.timeout.ms. Kafka's default is 30000: how long the client waits for the response to
     * one request before it retries or fails it. The game default is 10000 (game-tuned, M4 Q7).
     */
    requestTimeoutMs: positiveInt('request.timeout.ms').default(10_000),
    /** retry.backoff.ms, default 100: the wait before the first retry of a failed request. */
    retryBackoffMs: nonNegativeInt('retry.backoff.ms').default(100),
    /**
     * retry.backoff.max.ms, default 1000 (KIP-580): retries back off exponentially from
     * retry.backoff.ms up to this, with ±20 % jitter.
     */
    retryBackoffMaxMs: nonNegativeInt('retry.backoff.max.ms').default(1000),
    /**
     * enable.idempotence, default true since Kafka 3.0 (KIP-679): the broker drops a retried record
     * it already has. Fixed to true here; turning it off arrives with the v0.4 patterns (M4 Q7).
     */
    enableIdempotence: z
      .literal(true, { error: 'enable.idempotence is fixed to true here (turning it off: v0.4).' })
      .default(true),
  })
  // KafkaProducer#configureDeliveryTimeout throws a ConfigException when delivery.timeout.ms is
  // set below linger.ms + request.timeout.ms (linger.ms is not modelled: 0 here).
  .refine((p) => p.deliveryTimeoutMs >= p.requestTimeoutMs, {
    error: 'delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms.',
    path: ['deliveryTimeoutMs'],
  });

// ── Consumer ──────────────────────────────────────────────────────────────────────────────────

export const AutoOffsetResetSchema = z.enum(['earliest', 'latest'], {
  error: 'auto.offset.reset must be "earliest" or "latest".',
});

/**
 * partition.assignment.strategy values, by the names the assignors register under
 * (ConsumerPartitionAssignor#name): RangeAssignor "range", RoundRobinAssignor "roundrobin",
 * StickyAssignor "sticky", CooperativeStickyAssignor "cooperative-sticky".
 */
export const AssignorSchema = z.enum(['range', 'roundrobin', 'sticky', 'cooperative-sticky'], {
  error: 'Assignor must be "range", "roundrobin", "sticky" or "cooperative-sticky".',
});

/**
 * Game setting (M3 Q7): how the simulated application commits, standing in for its code.
 * - auto: no commit calls; enable.auto.commit decides.
 * - sync-after-processing: commitSync() after processing each poll()'s records (at-least-once).
 * - async-after-processing: commitAsync() after processing; a failure is only logged.
 * - sync-before-processing: commitSync() right after poll(), before processing (at-most-once).
 */
export const CommitModeSchema = z.enum(
  ['auto', 'sync-after-processing', 'async-after-processing', 'sync-before-processing'],
  {
    error:
      'Commit mode must be "auto", "sync-after-processing", "async-after-processing" or "sync-before-processing".',
  },
);

export const ConsumerSchema = z
  .object({
    kind: z.literal('consumer'),
    id: ComponentIdSchema,
    /** group.id. Defaults to "<id>-group" so each consumer gets its own group. */
    groupId: ComponentIdSchema.optional(),
    /** auto.offset.reset, default "latest". */
    autoOffsetReset: AutoOffsetResetSchema.default('latest'),
    /** enable.auto.commit, default true. */
    enableAutoCommit: z.boolean().default(true),
    /** auto.commit.interval.ms, default 5000. */
    autoCommitIntervalMs: positiveInt('auto.commit.interval.ms').default(5000),
    /** max.poll.records, default 500: the most records one poll() returns, over all partitions. */
    maxPollRecords: positiveInt('max.poll.records').default(500),
    /**
     * fetch.min.bytes, default 1: "The minimum amount of data the server should return for a
     * fetch request. If insufficient data is available the request will wait for that much data
     * to accumulate before answering the request." (ConsumerConfig FETCH_MIN_BYTES_DOC)
     */
    fetchMinBytes: nonNegativeInt('fetch.min.bytes').default(1),
    /**
     * fetch.max.wait.ms, default 500: "The maximum amount of time the server will block before
     * answering the fetch request there isn't sufficient data to immediately satisfy the
     * requirement given by fetch.min.bytes." (ConsumerConfig FETCH_MAX_WAIT_MS_DOC)
     */
    fetchMaxWaitMs: nonNegativeInt('fetch.max.wait.ms').default(500),
    /** metadata.max.age.ms, default 300000 (5 min). See the producer field. */
    metadataMaxAgeMs: nonNegativeInt('metadata.max.age.ms').default(300_000),
    /** Game setting: time the application spends on each record before polling again. */
    processingTimeMs: nonNegativeInt('Processing time').default(0),
    /**
     * session.timeout.ms. Kafka's default is 45000 (KIP-735); the game default is 10000
     * (game-tuned, M3 Q2), still above the broker's group.min.session.timeout.ms.
     */
    sessionTimeoutMs: z
      .int({ error: 'session.timeout.ms must be a whole number.' })
      .min(GROUP_MIN_SESSION_TIMEOUT_MS, {
        error: `INVALID_SESSION_TIMEOUT: session.timeout.ms must be at least the broker's group.min.session.timeout.ms (${String(GROUP_MIN_SESSION_TIMEOUT_MS)}).`,
      })
      .max(GROUP_MAX_SESSION_TIMEOUT_MS, {
        error: `INVALID_SESSION_TIMEOUT: session.timeout.ms must be at most the broker's group.max.session.timeout.ms (${String(GROUP_MAX_SESSION_TIMEOUT_MS)}).`,
      })
      .default(10_000),
    /** heartbeat.interval.ms, default 3000 (the real default). */
    heartbeatIntervalMs: positiveInt('heartbeat.interval.ms').default(3000),
    /**
     * max.poll.interval.ms. Kafka's default is 300000; the game default is 30000 (game-tuned).
     * Also the group's rebalance timeout.
     */
    maxPollIntervalMs: positiveInt('max.poll.interval.ms').default(30_000),
    /**
     * partition.assignment.strategy, in preference order. Kafka 4.x default: RangeAssignor then
     * CooperativeStickyAssignor (ConsumerConfig).
     */
    partitionAssignmentStrategy: z
      .array(AssignorSchema)
      .min(1, { error: 'partition.assignment.strategy needs at least one assignor.' })
      .refine((list) => new Set(list).size === list.length, {
        error: 'partition.assignment.strategy lists an assignor twice.',
      })
      .default(['range', 'cooperative-sticky']),
    /** Game setting: how the application commits (see CommitModeSchema). */
    commitMode: CommitModeSchema.default('auto'),
  })
  // ConsumerConfig / GroupRebalanceConfig: "Heartbeat must be set lower than the session timeout".
  .refine((c) => c.heartbeatIntervalMs < c.sessionTimeoutMs, {
    error: 'heartbeat.interval.ms must be lower than session.timeout.ms.',
    path: ['heartbeatIntervalMs'],
  })
  .transform(({ groupId, ...rest }) => ({ ...rest, groupId: groupId ?? `${rest.id}-group` }));

// ── Unions and types ──────────────────────────────────────────────────────────────────────────

export const ComponentSchema = z.union([BrokerSchema, TopicSchema, ProducerSchema, ConsumerSchema]);

export type Broker = z.output<typeof BrokerSchema>;
export type Topic = z.output<typeof TopicSchema>;
export type KeyStrategy = z.output<typeof KeyStrategySchema>;
export type Acks = z.output<typeof AcksSchema>;
export type Producer = z.output<typeof ProducerSchema>;
export type AutoOffsetReset = z.output<typeof AutoOffsetResetSchema>;
export type Assignor = z.output<typeof AssignorSchema>;
export type CommitMode = z.output<typeof CommitModeSchema>;
export type Consumer = z.output<typeof ConsumerSchema>;
export type Component = z.output<typeof ComponentSchema>;
/** What a level file or the UI may write: optional fields fall back to their defaults. */
export type ComponentInput = z.input<typeof ComponentSchema>;
export type ComponentKind = Component['kind'];

export const COMPONENT_KINDS = [
  'broker',
  'topic',
  'producer',
  'consumer',
] as const satisfies readonly ComponentKind[];

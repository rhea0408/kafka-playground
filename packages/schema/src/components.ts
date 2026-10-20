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
});

// ── Topic ─────────────────────────────────────────────────────────────────────────────────────

export const TopicSchema = z.object({
  kind: z.literal('topic'),
  /** The topic name doubles as its component id. */
  id: TopicNameSchema,
  /** num.partitions, default 1. */
  partitions: positiveInt('Partitions').default(1),
  /** default.replication.factor, default 1. */
  replicationFactor: positiveInt('Replication factor').default(1),
  /** min.insync.replicas, default 1. */
  minInsyncReplicas: positiveInt('min.insync.replicas').default(1),
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

export const ProducerSchema = z.object({
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
});

// ── Consumer ──────────────────────────────────────────────────────────────────────────────────

export const AutoOffsetResetSchema = z.enum(['earliest', 'latest'], {
  error: 'auto.offset.reset must be "earliest" or "latest".',
});

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

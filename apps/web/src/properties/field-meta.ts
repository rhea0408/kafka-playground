/**
 * What the properties panel (M2-T10, FR-1.3) shows for each config field: its label, the real
 * Kafka config name, the real Kafka default, the kind of field and a one-line explanation. The
 * values and their validation come from the Zod schemas in `@kp/schema`; this table only adds the
 * words around them.
 *
 * The table is typed against the schema's output types (`FIELD_META` uses `satisfies` with a
 * mapped type), so adding a schema field without an entry here fails typecheck; `field-meta.test.ts`
 * also checks the keys against the schema shapes at run time and that every `kafka` field's schema
 * default equals the real Kafka default written here.
 *
 * Kinds (M2-web-shell Q5, CLAUDE.md rule 6, NFR-3.3):
 * - `kafka`: a real Kafka config with its real default.
 * - `game-tuned`: a real Kafka config whose game default is shortened; shows a `game-tuned` chip
 *   and "Kafka default: …" under the field.
 * - `game-setting`: no Kafka config at all; shows a `game setting` chip with a tooltip saying
 *   what it stands in for.
 * - `not-modelled`: a real Kafka config the engine does not simulate yet. Shown disabled with its
 *   real default and the milestone that brings it (M2-decisions T10).
 *
 * Kafka defaults are from the Kafka 4.x configuration reference (docs "Broker / Topic / Producer /
 * Consumer Configs"); KIPs are cited where a default changed.
 */

import {
  AcksSchema,
  AutoOffsetResetSchema,
  BrokerSchema,
  ConsumerSchema,
  ProducerSchema,
  TopicSchema,
  type Component,
  type ComponentKind,
} from '@kp/schema';

export type ConfigOf<K extends ComponentKind> = Extract<Component, { kind: K }>;

/** Editable-looking config fields of a kind: everything but `kind` and `id` (ids are fixed). */
export type ConfigField<K extends ComponentKind> = Exclude<keyof ConfigOf<K>, 'kind' | 'id'>;

export type FieldKind = 'kafka' | 'game-tuned' | 'game-setting' | 'not-modelled';

/** Units the panel knows how to print ("300000 ms (5 min)", "16384 bytes (16 KiB)"). */
export type Unit = 'ms' | 'bytes' | 'records' | 'msg/s' | 'replicas';

/** Where a not-modelled feature arrives (architecture §11 and the roadmap). */
export type Milestone = 'M3' | 'M4' | 'v0.4';

export type Control =
  | { readonly type: 'int'; readonly unit?: Unit }
  | { readonly type: 'number'; readonly unit?: Unit }
  | { readonly type: 'enum'; readonly options: readonly string[] }
  | { readonly type: 'boolean' }
  | { readonly type: 'text' }
  /** Topic partitions: a stepper that can only grow, applied with an explicit button. */
  | { readonly type: 'partitions' }
  /** Producer key strategy: a strategy picker plus its keys. */
  | { readonly type: 'keyStrategy' };

/** A Kafka default: a value (compared with the schema default in tests) or "no default". */
export type KafkaDefault =
  { readonly value: string | number | boolean | null } | { readonly none: string };

interface BaseMeta {
  /** Plain-language label. */
  readonly label: string;
  /** One or two sentences: what the field does in Kafka and here. Shown in the (i) tooltip. */
  readonly help: string;
  readonly control: Control;
  /**
   * Set when the field is shown read-only, with the reason (immutable in M2, static config).
   * Not-modelled fields are always disabled and don't need this.
   */
  readonly readOnly?: string;
}

export type FieldMeta = BaseMeta &
  (
    | { readonly kind: 'kafka'; readonly kafkaName: string; readonly kafkaDefault: KafkaDefault }
    | {
        readonly kind: 'game-tuned';
        readonly kafkaName: string;
        readonly kafkaDefault: KafkaDefault;
      }
    | {
        readonly kind: 'game-setting';
        /** One line for the chip's tooltip: what this stands in for in a real application. */
        readonly standsFor: string;
      }
    | {
        readonly kind: 'not-modelled';
        readonly kafkaName: string;
        readonly kafkaDefault: KafkaDefault;
        readonly arrives: Milestone;
      }
  );

/** A not-modelled config that isn't in the schema at all: shown disabled with its default. */
export type ExtraFieldMeta = Extract<FieldMeta, { kind: 'not-modelled' }> & {
  readonly key: string;
};

type MetaTable = { readonly [K in ComponentKind]: { readonly [F in ConfigField<K>]: FieldMeta } };

const METADATA_NOTE =
  'Producers and consumers notice new partitions only at their next metadata refresh (metadata.max.age.ms).';

export const FIELD_META = {
  broker: {
    rack: {
      label: 'Rack',
      kafkaName: 'broker.rack',
      kind: 'kafka',
      kafkaDefault: { value: null },
      control: { type: 'text' },
      help: 'Which rack or availability zone the broker is in. Kafka uses it to spread replicas over racks.',
      readOnly:
        'broker.rack is a static broker config: Kafka reads it only when the broker starts. Broker restarts arrive in M4.',
    },
  },
  topic: {
    partitions: {
      label: 'Partitions',
      kafkaName: '--partitions',
      kind: 'kafka',
      kafkaDefault: { value: 1 },
      control: { type: 'partitions' },
      help: `Set when the topic is created and grown with kafka-topics --alter; the broker default for new topics is num.partitions = 1. Kafka can add partitions but never remove them. ${METADATA_NOTE}`,
    },
    replicationFactor: {
      label: 'Replication factor',
      kafkaName: '--replication-factor',
      kind: 'not-modelled',
      kafkaDefault: { value: 1 },
      arrives: 'M4',
      control: { type: 'int', unit: 'replicas' },
      help: 'How many brokers keep a copy of each partition (broker default default.replication.factor = 1). Replication needs several brokers, which arrive in M4.',
    },
    minInsyncReplicas: {
      label: 'Min in-sync replicas',
      kafkaName: 'min.insync.replicas',
      kind: 'not-modelled',
      kafkaDefault: { value: 1 },
      arrives: 'M4',
      control: { type: 'int', unit: 'replicas' },
      help: 'With acks=all, a write fails (NotEnoughReplicas) unless at least this many replicas are in sync. It only has an effect once replication arrives in M4.',
    },
  },
  producer: {
    ratePerSec: {
      label: 'Rate',
      kind: 'game-setting',
      standsFor:
        'Stands in for how often your application calls send(). Kafka has no rate setting.',
      control: { type: 'number', unit: 'msg/s' },
      help: 'Records this producer sends per second of sim time. Changing it starts a new send schedule from now.',
    },
    keyStrategy: {
      label: 'Keys',
      kind: 'game-setting',
      standsFor: 'Stands in for the keys your code puts on each ProducerRecord.',
      control: { type: 'keyStrategy' },
      help: 'Records with the same key always go to the same partition (murmur2 hash of the key). Records without a key are spread by the sticky partitioner.',
    },
    acks: {
      label: 'Acks',
      kafkaName: 'acks',
      kind: 'kafka',
      // Default "all" since Kafka 3.0 (KIP-679).
      kafkaDefault: { value: 'all' },
      control: { type: 'enum', options: AcksSchema.options },
      help: 'How many replicas must have a record before the send counts as done: 0 = do not wait, 1 = the leader, all = every in-sync replica. With one broker, 1 and all behave the same until M4.',
    },
    batchSize: {
      label: 'Batch size',
      kafkaName: 'batch.size',
      kind: 'kafka',
      kafkaDefault: { value: 16_384 },
      control: { type: 'int', unit: 'bytes' },
      help: 'Upper size of one batch per partition. Here it only decides when the sticky partitioner moves on for records without a key (batching itself is simplified).',
    },
    messageSizeBytes: {
      label: 'Message size',
      kind: 'game-setting',
      standsFor:
        'Stands in for the size of each serialized record (key + value). Kafka has no such setting.',
      control: { type: 'int', unit: 'bytes' },
      help: 'Bytes per record. It counts towards batch.size and the consumer’s fetch.min.bytes.',
    },
    metadataMaxAgeMs: {
      label: 'Metadata max age',
      kafkaName: 'metadata.max.age.ms',
      kind: 'kafka',
      kafkaDefault: { value: 300_000 },
      control: { type: 'int', unit: 'ms' },
      help: 'How often the producer refreshes its view of the cluster even without errors. That is when it notices partitions added to a topic.',
    },
  },
  consumer: {
    groupId: {
      label: 'Group',
      kafkaName: 'group.id',
      kind: 'kafka',
      kafkaDefault: { none: 'none (required to subscribe)' },
      control: { type: 'text' },
      help: 'The consumer group this consumer belongs to. The group shares the partitions and stores committed offsets. Here it defaults to "<id>-group".',
      readOnly:
        'Changing group.id means leaving one group and joining another. Consumer groups arrive in M3; until then remove the consumer and add a new one.',
    },
    autoOffsetReset: {
      label: 'Auto offset reset',
      kafkaName: 'auto.offset.reset',
      kind: 'kafka',
      kafkaDefault: { value: 'latest' },
      control: { type: 'enum', options: AutoOffsetResetSchema.options },
      help: 'Where to start when the group has no committed offset: earliest = the oldest record, latest = only new records.',
      readOnly:
        'auto.offset.reset is read when the consumer joins its group. Changing it on a running consumer arrives with consumer groups in M3.',
    },
    enableAutoCommit: {
      label: 'Auto commit',
      kafkaName: 'enable.auto.commit',
      kind: 'kafka',
      kafkaDefault: { value: true },
      control: { type: 'boolean' },
      help: 'Commit the consumed offsets automatically from inside poll(), every auto.commit.interval.ms.',
    },
    autoCommitIntervalMs: {
      label: 'Auto commit interval',
      kafkaName: 'auto.commit.interval.ms',
      kind: 'kafka',
      kafkaDefault: { value: 5000 },
      control: { type: 'int', unit: 'ms' },
      help: 'How often auto commit runs. Ignored when auto commit is off.',
    },
    maxPollRecords: {
      label: 'Max poll records',
      kafkaName: 'max.poll.records',
      kind: 'kafka',
      kafkaDefault: { value: 500 },
      control: { type: 'int', unit: 'records' },
      help: 'The most records one poll() hands to your code, over all partitions. The rest stay buffered for the next poll().',
    },
    fetchMinBytes: {
      label: 'Fetch min bytes',
      kafkaName: 'fetch.min.bytes',
      kind: 'kafka',
      kafkaDefault: { value: 1 },
      control: { type: 'int', unit: 'bytes' },
      help: 'The broker holds a fetch until at least this much data is available, or fetch.max.wait.ms passes.',
    },
    fetchMaxWaitMs: {
      label: 'Fetch max wait',
      kafkaName: 'fetch.max.wait.ms',
      kind: 'kafka',
      kafkaDefault: { value: 500 },
      control: { type: 'int', unit: 'ms' },
      help: 'The longest the broker holds a fetch when there is less than fetch.min.bytes of data.',
    },
    metadataMaxAgeMs: {
      label: 'Metadata max age',
      kafkaName: 'metadata.max.age.ms',
      kind: 'kafka',
      kafkaDefault: { value: 300_000 },
      control: { type: 'int', unit: 'ms' },
      help: 'How often the consumer refreshes its view of the cluster even without errors. That is when it notices partitions added to a topic.',
    },
    processingTimeMs: {
      label: 'Processing time',
      kind: 'game-setting',
      standsFor: 'Stands in for the time your code spends on each record between poll() calls.',
      control: { type: 'int', unit: 'ms' },
      help: 'Time the application spends on each record before it calls poll() again. Raise it to make a slow consumer.',
    },
  },
} as const satisfies MetaTable;

/**
 * Real Kafka configs the playground doesn't simulate at all yet (not in the schema). Shown
 * disabled under the editable fields with their real default and the milestone that brings them.
 * Only configs whose milestone is planned are listed.
 */
export const EXTRA_FIELDS: { readonly [K in ComponentKind]: readonly ExtraFieldMeta[] } = {
  broker: [],
  topic: [
    {
      key: 'retentionMs',
      label: 'Retention',
      kafkaName: 'retention.ms',
      kind: 'not-modelled',
      kafkaDefault: { value: 604_800_000 },
      arrives: 'v0.4',
      control: { type: 'int', unit: 'ms' },
      help: 'How long records are kept before old log segments are deleted. Here records are kept forever.',
    },
    {
      key: 'cleanupPolicy',
      label: 'Cleanup policy',
      kafkaName: 'cleanup.policy',
      kind: 'not-modelled',
      kafkaDefault: { value: 'delete' },
      arrives: 'v0.4',
      control: { type: 'enum', options: ['delete', 'compact'] },
      help: 'delete = drop old segments after retention; compact = keep the latest record per key.',
    },
  ],
  producer: [
    {
      key: 'enableIdempotence',
      label: 'Idempotence',
      kafkaName: 'enable.idempotence',
      kind: 'not-modelled',
      // Default true since Kafka 3.0 (KIP-679).
      kafkaDefault: { value: true },
      arrives: 'v0.4',
      control: { type: 'boolean' },
      help: 'The broker drops duplicates when the producer retries a send, so each record is written once per partition.',
    },
  ],
  consumer: [
    {
      key: 'sessionTimeoutMs',
      label: 'Session timeout',
      kafkaName: 'session.timeout.ms',
      kind: 'not-modelled',
      // 45 s since Kafka 3.0 (KIP-735).
      kafkaDefault: { value: 45_000 },
      arrives: 'M3',
      control: { type: 'int', unit: 'ms' },
      help: 'If the group coordinator hears no heartbeat for this long, it removes the consumer from the group and rebalances.',
    },
    {
      key: 'heartbeatIntervalMs',
      label: 'Heartbeat interval',
      kafkaName: 'heartbeat.interval.ms',
      kind: 'not-modelled',
      kafkaDefault: { value: 3000 },
      arrives: 'M3',
      control: { type: 'int', unit: 'ms' },
      help: 'How often the consumer tells the group coordinator it is alive.',
    },
  ],
};

/** The order fields appear in: the table's order (schema order, by meaning). */
export function fieldsOf<K extends ComponentKind>(kind: K): readonly ConfigField<K>[] {
  return Object.keys(FIELD_META[kind]) as ConfigField<K>[];
}

export function metaOf<K extends ComponentKind>(kind: K, field: ConfigField<K>): FieldMeta {
  const table: Readonly<Record<string, FieldMeta>> = FIELD_META[kind];
  const meta = table[field as string];
  if (meta === undefined) throw new Error(`No metadata for ${kind}.${String(field)}`);
  return meta;
}

/** The Zod object shape of a kind (the consumer schema is a pipe because of its transform). */
export const SHAPES = {
  broker: BrokerSchema.shape,
  topic: TopicSchema.shape,
  producer: ProducerSchema.shape,
  consumer: ConsumerSchema.in.shape,
} as const;

/** The part of a Zod schema the panel uses (the web app doesn't depend on zod directly). */
export interface FieldValidator {
  safeParse(value: unknown):
    | { readonly success: true; readonly data: unknown }
    | {
        readonly success: false;
        readonly error: { readonly issues: readonly { message: string }[] };
      };
}

/** The Zod schema that validates one field (inline validation, before anything is sent). */
export function fieldSchema(kind: ComponentKind, field: string): FieldValidator | undefined {
  const shape: Readonly<Record<string, FieldValidator>> = SHAPES[kind];
  return shape[field];
}

/** Whether the field can be changed in the panel. */
export function isEditable(meta: FieldMeta): boolean {
  return meta.kind !== 'not-modelled' && meta.readOnly === undefined;
}

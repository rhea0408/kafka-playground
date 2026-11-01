import { describe, expect, it } from 'vitest';
import {
  BrokerSchema,
  ConsumerSchema,
  ProducerSchema,
  TopicNameSchema,
  TopicSchema,
} from './components.ts';
import { parseComponent } from './topology.ts';

describe('component defaults', () => {
  it('fill a topic with Kafka defaults', () => {
    expect(TopicSchema.parse({ kind: 'topic', id: 'orders' })).toEqual({
      kind: 'topic',
      id: 'orders',
      partitions: 1,
      replicationFactor: 1,
      minInsyncReplicas: 1,
    });
  });

  it('fill a producer with Kafka defaults (acks=all, batch.size=16384)', () => {
    expect(ProducerSchema.parse({ kind: 'producer', id: 'p1' })).toEqual({
      kind: 'producer',
      id: 'p1',
      ratePerSec: 1,
      keyStrategy: { type: 'null' },
      acks: 'all',
      batchSize: 16384,
      messageSizeBytes: 1024,
      metadataMaxAgeMs: 300_000,
    });
  });

  it('drop the removed pollIntervalMs game setting (fetching is a long poll since M2)', () => {
    expect(
      ConsumerSchema.parse({ kind: 'consumer', id: 'c1', pollIntervalMs: 100 }),
    ).not.toHaveProperty('pollIntervalMs');
  });

  it('accept fetch.min.bytes and fetch.max.wait.ms of 0, but not negative values', () => {
    expect(
      parseComponent({ kind: 'consumer', id: 'c1', fetchMinBytes: 0, fetchMaxWaitMs: 0 }).ok,
    ).toBe(true);
    expect(parseComponent({ kind: 'consumer', id: 'c1', fetchMaxWaitMs: -1 })).toEqual({
      ok: false,
      errors: ['fetchMaxWaitMs: fetch.max.wait.ms cannot be negative.'],
    });
  });

  it('fill a consumer with Kafka defaults and its own group', () => {
    expect(ConsumerSchema.parse({ kind: 'consumer', id: 'c1' })).toEqual({
      kind: 'consumer',
      id: 'c1',
      groupId: 'c1-group',
      autoOffsetReset: 'latest',
      enableAutoCommit: true,
      autoCommitIntervalMs: 5000,
      maxPollRecords: 500,
      fetchMinBytes: 1,
      fetchMaxWaitMs: 500,
      metadataMaxAgeMs: 300_000,
      processingTimeMs: 0,
      sessionTimeoutMs: 10_000, // game-tuned; Kafka 45000
      heartbeatIntervalMs: 3000,
      maxPollIntervalMs: 30_000, // game-tuned; Kafka 300000
      partitionAssignmentStrategy: ['range', 'cooperative-sticky'],
      commitMode: 'auto',
    });
    expect(ConsumerSchema.parse({ kind: 'consumer', id: 'c1', groupId: 'billing' }).groupId).toBe(
      'billing',
    );
  });
});

describe('topic names follow Kafka rules', () => {
  it.each(['orders', 'orders.v2', 'my_topic-1', 'a'.repeat(249)])('accepts %s', (name) => {
    expect(TopicNameSchema.safeParse(name).success).toBe(true);
  });

  it.each([
    ['', /empty/],
    ['.', /"\." or "\.\."/],
    ['..', /"\." or "\.\."/],
    ['has space', /letters, digits/],
    ['ünïcode', /letters, digits/],
    ['a'.repeat(250), /at most 249/],
  ])('rejects %j', (name, message) => {
    const result = TopicNameSchema.safeParse(name);
    expect(result.success).toBe(false);
    expect(result.error?.issues[0]?.message).toMatch(message);
  });
});

describe('parseComponent', () => {
  it('parses by kind and applies defaults', () => {
    expect(parseComponent({ kind: 'broker', id: 'b1' })).toEqual({
      ok: true,
      value: { kind: 'broker', id: 'b1', rack: null, groupInitialRebalanceDelayMs: 3000 },
    });
  });

  it('reports errors for the chosen kind only, in plain words', () => {
    expect(parseComponent({ kind: 'topic', id: 'orders', partitions: 0 })).toEqual({
      ok: false,
      errors: ['partitions: Partitions must be at least 1.'],
    });
    expect(parseComponent({ kind: 'producer', id: 'p1', acks: 2 })).toEqual({
      ok: false,
      errors: ['acks: acks must be "0", "1" or "all".'],
    });
    expect(parseComponent({ kind: 'queue', id: 'x' })).toEqual({
      ok: false,
      errors: ['kind: must be one of "broker", "topic", "producer" or "consumer".'],
    });
  });

  it('validates key strategies', () => {
    const skewed = (hotShare: number) =>
      parseComponent({
        kind: 'producer',
        id: 'p1',
        keyStrategy: { type: 'skewed', hotKey: 'A', hotShare, otherKeys: ['B', 'C'] },
      });
    expect(skewed(0.8).ok).toBe(true);
    expect(skewed(1.5)).toEqual({
      ok: false,
      errors: ['keyStrategy.hotShare: Hot-key share must be between 0 and 1.'],
    });
    expect(
      parseComponent({ kind: 'producer', id: 'p1', keyStrategy: { type: 'random', keys: [] } }),
    ).toEqual({
      ok: false,
      errors: ['keyStrategy.keys: Random key strategy needs at least one key.'],
    });
  });
});

describe('group and commit configs (M3)', () => {
  const consumer = (extra: Record<string, unknown>) =>
    parseComponent({ kind: 'consumer', id: 'c1', ...extra });

  it('give the broker the real group.initial.rebalance.delay.ms', () => {
    expect(BrokerSchema.parse({ kind: 'broker', id: 'b1' })).toEqual({
      kind: 'broker',
      id: 'b1',
      rack: null,
      groupInitialRebalanceDelayMs: 3000,
    });
  });

  it('keep session.timeout.ms within the broker’s group.min/max.session.timeout.ms', () => {
    expect(consumer({ sessionTimeoutMs: 6000, heartbeatIntervalMs: 2000 }).ok).toBe(true);
    expect(consumer({ sessionTimeoutMs: 1_800_000 }).ok).toBe(true);
    expect(consumer({ sessionTimeoutMs: 5999 })).toEqual({
      ok: false,
      errors: [
        "sessionTimeoutMs: INVALID_SESSION_TIMEOUT: session.timeout.ms must be at least the broker's group.min.session.timeout.ms (6000).",
      ],
    });
    expect(consumer({ sessionTimeoutMs: 1_800_001 }).ok).toBe(false);
  });

  it('need heartbeat.interval.ms below session.timeout.ms', () => {
    expect(consumer({ sessionTimeoutMs: 6000, heartbeatIntervalMs: 5999 }).ok).toBe(true);
    expect(consumer({ sessionTimeoutMs: 6000, heartbeatIntervalMs: 6000 })).toEqual({
      ok: false,
      errors: ['heartbeatIntervalMs: heartbeat.interval.ms must be lower than session.timeout.ms.'],
    });
  });

  it('take an ordered, non-empty list of known assignors without repeats', () => {
    expect(consumer({ partitionAssignmentStrategy: ['cooperative-sticky'] }).ok).toBe(true);
    expect(consumer({ partitionAssignmentStrategy: ['roundrobin', 'range'] }).ok).toBe(true);
    expect(consumer({ partitionAssignmentStrategy: [] }).ok).toBe(false);
    expect(consumer({ partitionAssignmentStrategy: ['range', 'range'] }).ok).toBe(false);
    expect(consumer({ partitionAssignmentStrategy: ['uniform'] }).ok).toBe(false);
  });

  it('take a commit mode and a positive max.poll.interval.ms', () => {
    for (const commitMode of [
      'auto',
      'sync-after-processing',
      'async-after-processing',
      'sync-before-processing',
    ]) {
      expect(consumer({ commitMode }).ok).toBe(true);
    }
    expect(consumer({ commitMode: 'manual' }).ok).toBe(false);
    expect(consumer({ maxPollIntervalMs: 0 }).ok).toBe(false);
  });
});

import { describe, expect, it } from 'vitest';
import { ConsumerSchema, ProducerSchema, TopicNameSchema, TopicSchema } from './components.ts';
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
      pollIntervalMs: 100,
      processingTimeMs: 0,
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
      value: { kind: 'broker', id: 'b1', rack: null },
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

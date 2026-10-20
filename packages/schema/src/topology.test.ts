import { describe, expect, it } from 'vitest';
import { parseTopology, type TopologyInput } from './topology.ts';

const valid: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1' }],
  topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
  producers: [
    { kind: 'producer', id: 'p1', keyStrategy: { type: 'random', keys: ['A', 'B', 'C'] } },
  ],
  consumers: [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest' }],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'orders', to: 'c1' },
  ],
};

const errorsOf = (input: unknown): readonly string[] => {
  const result = parseTopology(input);
  return result.ok ? [] : result.errors;
};

describe('parseTopology', () => {
  it('accepts a producer → topic → consumer topology and fills defaults', () => {
    const result = parseTopology(valid);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.value.topics[0]).toMatchObject({ partitions: 3, replicationFactor: 1 });
    expect(result.value.consumers[0]?.groupId).toBe('c1-group');
  });

  it('accepts an empty topology', () => {
    expect(parseTopology({})).toEqual({
      ok: true,
      value: { brokers: [], topics: [], producers: [], consumers: [], connections: [] },
    });
  });

  it('names the component in config errors', () => {
    expect(
      errorsOf({ ...valid, topics: [{ kind: 'topic', id: 'orders', partitions: 1.5 }] }),
    ).toEqual(['topic "orders" → partitions: Partitions must be a whole number.']);
  });

  it('rejects duplicate ids across kinds', () => {
    expect(
      errorsOf({ ...valid, consumers: [{ kind: 'consumer', id: 'p1' }], connections: [] }),
    ).toEqual(['consumer "p1" → id: Id "p1" is already used by another component.']);
  });

  it('rejects connections to missing components and against the rules', () => {
    expect(
      errorsOf({
        ...valid,
        connections: [
          { from: 'p1', to: 'nope' },
          { from: 'p1', to: 'c1' },
          { from: 'p1', to: 'orders' },
          { from: 'p1', to: 'orders' },
        ],
      }),
    ).toEqual([
      'connection p1 → nope: There is no component with id "nope".',
      "connection p1 → c1: A producer can't send straight to a consumer. Producers write to a topic, and consumers read from it.",
      'connection p1 → orders: p1 → orders is connected twice.',
    ]);
  });

  it('rejects a replication factor larger than the broker count, like Kafka does', () => {
    expect(
      errorsOf({ ...valid, topics: [{ kind: 'topic', id: 'orders', replicationFactor: 3 }] }),
    ).toEqual([
      'topic "orders" → replicationFactor: Replication factor 3 is larger than the number of brokers (1).',
    ]);
    expect(errorsOf({ topics: [{ kind: 'topic', id: 't' }] })).toEqual([
      'topic "t" → replicationFactor: Replication factor 1 is larger than the number of brokers (0).',
    ]);
  });

  it('reports a missing kind or wrong type readably', () => {
    const errors = errorsOf({ brokers: [{ id: 'b1' }] });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/^broker "b1" → kind: /);
  });
});

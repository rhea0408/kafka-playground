/**
 * The topology: everything on the canvas (brokers, topics, producers, consumers, connections).
 * Parsing applies config defaults and checks the cross-component rules a single component schema
 * cannot see: unique ids, connections between existing components, connection rules, and
 * replication factor against the number of brokers.
 */

import { z } from 'zod';
import {
  BrokerSchema,
  ConsumerSchema,
  ProducerSchema,
  TopicSchema,
  type Component,
  type ComponentKind,
} from './components.ts';
import { canConnect } from './connections.ts';
import { formatIssues } from './errors.ts';

export const ConnectionSchema = z.object({
  from: z.string().min(1, { error: 'Connection needs a "from" id.' }),
  to: z.string().min(1, { error: 'Connection needs a "to" id.' }),
});

export type Connection = z.output<typeof ConnectionSchema>;

const TopologyShape = z.object({
  brokers: z.array(BrokerSchema).default([]),
  topics: z.array(TopicSchema).default([]),
  producers: z.array(ProducerSchema).default([]),
  consumers: z.array(ConsumerSchema).default([]),
  connections: z.array(ConnectionSchema).default([]),
});

export const TopologySchema = TopologyShape.superRefine((topology, ctx) => {
  const kinds = new Map<string, ComponentKind>();
  const groups: [keyof typeof topology, readonly { id: string; kind: ComponentKind }[]][] = [
    ['brokers', topology.brokers],
    ['topics', topology.topics],
    ['producers', topology.producers],
    ['consumers', topology.consumers],
  ];
  for (const [key, list] of groups) {
    list.forEach((component, index) => {
      if (kinds.has(component.id)) {
        ctx.addIssue({
          code: 'custom',
          path: [key, index, 'id'],
          message: `Id "${component.id}" is already used by another component.`,
        });
      }
      kinds.set(component.id, component.kind);
    });
  }

  const seen = new Set<string>();
  topology.connections.forEach(({ from, to }, index) => {
    const path = ['connections', index];
    const fromKind = kinds.get(from);
    const toKind = kinds.get(to);
    if (fromKind === undefined || toKind === undefined) {
      const missing = fromKind === undefined ? from : to;
      ctx.addIssue({
        code: 'custom',
        path,
        message: `There is no component with id "${missing}".`,
      });
      return;
    }
    const check = canConnect(fromKind, toKind);
    if (!check.ok) {
      ctx.addIssue({ code: 'custom', path, message: check.reason });
      return;
    }
    const key = `${from}\u0000${to}`;
    if (seen.has(key)) {
      ctx.addIssue({ code: 'custom', path, message: `${from} → ${to} is connected twice.` });
    }
    seen.add(key);
  });

  // Topic creation fails with InvalidReplicationFactorException when RF exceeds the live brokers
  // (Kafka docs, "Adding and removing topics"; CreateTopics error INVALID_REPLICATION_FACTOR).
  topology.topics.forEach((topic, index) => {
    if (topic.replicationFactor > topology.brokers.length) {
      ctx.addIssue({
        code: 'custom',
        path: ['topics', index, 'replicationFactor'],
        message: `Replication factor ${String(topic.replicationFactor)} is larger than the number of brokers (${String(topology.brokers.length)}).`,
      });
    }
  });
});

export type Topology = z.output<typeof TopologySchema>;
export type TopologyInput = z.input<typeof TopologySchema>;

export type ParseResult<T> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly errors: readonly string[] };

/** Parses and validates a topology, returning readable error lines on failure. */
export function parseTopology(input: unknown): ParseResult<Topology> {
  const result = TopologySchema.safeParse(input);
  return result.success
    ? { ok: true, value: result.data }
    : { ok: false, errors: formatIssues(result.error.issues, input) };
}

const SCHEMA_BY_KIND = {
  broker: BrokerSchema,
  topic: TopicSchema,
  producer: ProducerSchema,
  consumer: ConsumerSchema,
} as const;

/**
 * Parses one component, choosing the schema by `kind` first so errors talk about that component
 * instead of listing why every kind of component failed.
 */
export function parseComponent(input: unknown): ParseResult<Component> {
  const kind =
    typeof input === 'object' && input !== null && 'kind' in input ? input.kind : undefined;
  if (typeof kind !== 'string' || !(kind in SCHEMA_BY_KIND)) {
    return {
      ok: false,
      errors: ['kind: must be one of "broker", "topic", "producer" or "consumer".'],
    };
  }
  const result = SCHEMA_BY_KIND[kind as ComponentKind].safeParse(input);
  return result.success
    ? { ok: true, value: result.data }
    : { ok: false, errors: formatIssues(result.error.issues, input) };
}

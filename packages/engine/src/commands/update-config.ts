/**
 * UpdateConfig: changes one component's config at the current sim time (properties panel,
 * FR-1.3). The patch is merged into the current config and validated with the same schema as
 * AddComponent. A rejected update changes nothing.
 *
 * Rules (M2):
 * - `kind` and `id` are immutable (the id is how Kafka and the canvas know the component).
 * - Topic: partitions can only grow. Kafka's CreatePartitions rejects a decrease with
 *   InvalidPartitionsException; the reason below is the KRaft controller's wording
 *   (ReplicationControlManager#createPartitions). New partitions start empty; existing records
 *   stay where they are. Producers and consumers see the new partitions after their next
 *   metadata refresh (metadata.max.age.ms, see protocols/metadata.ts). The replication factor
 *   cannot be changed through config (it takes a partition reassignment, M4).
 *   min.insync.replicas is a dynamic topic config and is accepted.
 * - Broker: the only field, `rack` (broker.rack), is a static broker config that needs a broker
 *   restart, so changing it is rejected.
 * - Producer: every field. A rate change starts a new send schedule from now; stats are kept.
 *   Requests already in flight keep the acks they were sent with; other fields apply from the
 *   next record.
 * - Consumer: max.poll.records, fetch.min.bytes, fetch.max.wait.ms, processing time, auto-commit
 *   settings and metadata.max.age.ms apply from the next poll() (a fetch already parked at the
 *   broker keeps the values it was sent with). group.id and auto.offset.reset are rejected in M2:
 *   changing them means leaving one group and joining another (M3).
 * - A patch that changes nothing is accepted, emits no event and is not logged.
 *
 * Real clients cannot change their config at run time at all; the application would close the
 * client and create a new one. The playground applies changes in place (docs/simplifications.md).
 */

import { parseComponent, type Component } from '@kp/schema';
import { assertNever } from '../core/assert-never.ts';
import type { EngineContext } from '../model/context.ts';
import { createPartition } from '../model/partition.ts';
import type { TopicState } from '../model/state.ts';
import { restartSchedule } from '../protocols/produce.ts';
import { kindOf, reject, type ApplyResult } from './apply.ts';
import type { Command } from './index.ts';

type UpdateConfigCommand = Extract<Command, { type: 'UpdateConfig' }>;

export function updateConfig(ctx: EngineContext, command: UpdateConfigCommand): ApplyResult {
  const { id } = command;
  const patch: unknown = command.patch;
  if (typeof patch !== 'object' || patch === null || Array.isArray(patch)) {
    return reject('UpdateConfig needs a patch object with the fields to change.');
  }
  const before = currentConfig(ctx, id);
  if (before === undefined) return reject(`There is no component with id "${id}".`);
  if ('id' in patch && patch.id !== id) {
    return reject(`Ids cannot be changed (${id}). Remove the component and add a new one instead.`);
  }
  if ('kind' in patch && patch.kind !== before.kind) {
    return reject(`${id} is a ${before.kind}; its kind cannot be changed.`);
  }

  const parsed = parseComponent({ ...before, ...patch, id, kind: before.kind });
  if (!parsed.ok) return reject(parsed.errors.join(' '));
  const after = parsed.value;
  const unknown = Object.keys(patch).filter((k) => !(k in after));
  if (unknown.length > 0) {
    return reject(
      `A ${before.kind} has no config field ${unknown.map((k) => `"${k}"`).join(', ')}.`,
    );
  }
  if (JSON.stringify(after) === JSON.stringify(before)) return { ok: true, logged: null };

  const problem = apply(ctx, before, after);
  if (problem !== undefined) return reject(problem);
  ctx.emit({ type: 'ConfigUpdated', id, before, after });
  return { ok: true, logged: command };
}

function currentConfig(ctx: EngineContext, id: string): Component | undefined {
  const { state } = ctx;
  switch (kindOf(ctx, id)) {
    case 'broker':
      return state.brokers.get(id)?.config;
    case 'topic':
      return state.topics.get(id)?.config;
    case 'producer':
      return state.producers.get(id)?.config;
    case 'consumer':
      return state.consumers.get(id)?.config;
    case undefined:
      return undefined;
  }
}

/** Checks the per-kind rules, then applies. Returns a reason (and changes nothing) if not allowed. */
function apply(ctx: EngineContext, before: Component, after: Component): string | undefined {
  const { state } = ctx;
  switch (after.kind) {
    case 'broker':
      return 'broker.rack is a static broker config: Kafka only reads it when the broker starts. Restarting brokers arrives in M4.';
    case 'topic': {
      const topic = state.topics.get(after.id);
      if (before.kind !== 'topic' || topic === undefined) return 'Internal error: not a topic.';
      const current = topic.partitions.length;
      if (after.partitions < current) {
        return `The topic ${after.id} currently has ${String(current)} partition(s); ${String(after.partitions)} would not be an increase. Kafka can add partitions to a topic but never remove them.`;
      }
      if (after.replicationFactor !== before.replicationFactor) {
        return 'The replication factor of an existing topic is changed by reassigning partitions, not through its config. That arrives in M4.';
      }
      addPartitions(topic, after.partitions, [...state.brokers.keys()]);
      topic.config = after;
      return undefined;
    }
    case 'producer': {
      const producer = state.producers.get(after.id);
      if (before.kind !== 'producer' || producer === undefined) {
        return 'Internal error: not a producer.';
      }
      producer.config = after;
      if (after.ratePerSec !== before.ratePerSec) restartSchedule(ctx, producer);
      return undefined;
    }
    case 'consumer': {
      const consumer = state.consumers.get(after.id);
      if (before.kind !== 'consumer' || consumer === undefined) {
        return 'Internal error: not a consumer.';
      }
      if (after.groupId !== before.groupId) {
        return 'Changing group.id means leaving one consumer group and joining another. Consumer groups arrive in M3; remove the consumer and add a new one instead.';
      }
      if (after.autoOffsetReset !== before.autoOffsetReset) {
        return 'auto.offset.reset is read when the consumer joins its group. Changing it on a running consumer arrives with consumer groups in M3; remove the consumer and add a new one instead.';
      }
      consumer.config = after;
      return undefined;
    }
    default:
      return assertNever(after);
  }
}

/**
 * Adds empty partitions up to `count` (CreatePartitions). Leaders continue the topic's stripe
 * over the current brokers sorted by id, from partition 0's leader (placement.ts stripes the same
 * way). With one broker (M2) every new partition lands on it.
 */
function addPartitions(topic: TopicState, count: number, brokerIds: readonly string[]): void {
  const first = topic.partitions[0]?.leader;
  const sorted = [...brokerIds].sort();
  const start = Math.max(0, first === undefined ? 0 : sorted.indexOf(first));
  for (let index = topic.partitions.length; index < count; index++) {
    const leader = sorted[(start + index) % sorted.length] ?? first ?? '';
    topic.partitions.push(createPartition(topic.config.id, index, leader));
  }
}

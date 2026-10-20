/**
 * Command handlers: validate a command against the current state, apply it, emit events.
 * A rejected command changes nothing (no state, no RNG draws, no events), so the command log,
 * which holds only accepted commands, replays exactly.
 */

import { canConnect, parseComponent, type Component, type ComponentKind } from '@kp/schema';
import { assertNever } from '../core/assert-never.ts';
import type { EngineContext } from '../model/context.ts';
import { createPartition } from '../model/partition.ts';
import { placePartitions } from '../model/placement.ts';
import { topicsOfProducer } from '../model/state.ts';
import {
  closeConsumer,
  killConsumer,
  runConsumer,
  startConsumer,
  wakeConsumer,
} from '../protocols/consume.ts';
import { onTopicRemoved } from '../protocols/fetch.ts';
import { sendRecord, startProducer } from '../protocols/produce.ts';
import { updateConfig } from './update-config.ts';
import type { Command, LoggedCommand } from './index.ts';

/** `logged` is null for an accepted command that changed nothing (it is not logged). */
export type ApplyResult =
  | { readonly ok: true; readonly logged: LoggedCommand | null }
  | { readonly ok: false; readonly reason: string };

export const reject = (reason: string): ApplyResult => ({ ok: false, reason });

export function applyCommand(ctx: EngineContext, command: Command): ApplyResult {
  switch (command.type) {
    case 'AddComponent':
      return addComponent(ctx, command.component);
    case 'RemoveComponent':
      return removeComponent(ctx, command.id);
    case 'Connect':
      return connect(ctx, command.from, command.to);
    case 'Disconnect':
      return disconnect(ctx, command.from, command.to);
    case 'ProduceManual':
      return produceManual(ctx, command);
    case 'KillConsumer':
    case 'RestartConsumer':
      return setConsumerRunning(ctx, command.id, command.type === 'RestartConsumer');
    case 'UpdateConfig':
      return updateConfig(ctx, command);
    default:
      return assertNever(command);
  }
}

export function kindOf(ctx: EngineContext, id: string): ComponentKind | undefined {
  const { state } = ctx;
  if (state.brokers.has(id)) return 'broker';
  if (state.topics.has(id)) return 'topic';
  if (state.producers.has(id)) return 'producer';
  if (state.consumers.has(id)) return 'consumer';
  return undefined;
}

function addComponent(ctx: EngineContext, input: unknown): ApplyResult {
  const parsed = parseComponent(input);
  if (!parsed.ok) return reject(parsed.errors.join(' '));
  const component = parsed.value;
  if (kindOf(ctx, component.id) !== undefined) {
    return reject(`A component with id "${component.id}" already exists.`);
  }
  const problem = checkAdd(ctx, component);
  if (problem !== undefined) return reject(problem);

  const { state } = ctx;
  switch (component.kind) {
    case 'broker':
      state.brokers.set(component.id, { config: component });
      break;
    case 'topic': {
      const leaders = placePartitions(
        component.partitions,
        [...state.brokers.keys()],
        ctx.kernel.rng.fork(`placement:${component.id}`),
      );
      state.topics.set(component.id, {
        config: component,
        partitions: leaders.map((leader, index) => createPartition(component.id, index, leader)),
      });
      break;
    }
    case 'producer':
      startProducer(ctx, component);
      break;
    case 'consumer':
      startConsumer(ctx, component);
      break;
    default:
      return assertNever(component);
  }
  ctx.emit({ type: 'ComponentAdded', component });
  return { ok: true, logged: { type: 'AddComponent', component } };
}

/** Rules that depend on the rest of the cluster. Returns a reason, or undefined if allowed. */
function checkAdd(ctx: EngineContext, component: Component): string | undefined {
  const { state } = ctx;
  if (component.kind === 'topic') {
    const brokers = state.brokers.size;
    // CreateTopics fails with INVALID_REPLICATION_FACTOR when RF exceeds the available brokers.
    if (component.replicationFactor > brokers) {
      return `Replication factor ${String(component.replicationFactor)} is larger than the number of brokers (${String(brokers)}). Add a broker first.`;
    }
    if (component.replicationFactor > 1) {
      return 'This version keeps one copy of each partition, so replication factor must be 1. Replication arrives in a later milestone (M4).';
    }
  }
  if (component.kind === 'consumer') {
    for (const other of state.consumers.values()) {
      if (other.config.groupId === component.groupId) {
        return `Consumer group "${component.groupId}" already has a member (${other.config.id}). Groups with several consumers arrive in M3; give this consumer its own group.id for now.`;
      }
    }
  }
  return undefined;
}

function removeComponent(ctx: EngineContext, id: string): ApplyResult {
  const { state } = ctx;
  const kind = kindOf(ctx, id);
  if (kind === undefined) return reject(`There is no component with id "${id}".`);

  let component: Component;
  switch (kind) {
    case 'broker': {
      for (const topic of state.topics.values()) {
        if (topic.partitions.some((p) => p.leader === id)) {
          return reject(
            `Broker ${id} leads partitions of topic "${topic.config.id}". Remove the topic first; moving partitions to other brokers arrives in M4.`,
          );
        }
      }
      component = (state.brokers.get(id) as { config: Component }).config;
      state.brokers.delete(id);
      break;
    }
    case 'topic':
      component = (state.topics.get(id) as { config: Component }).config;
      state.topics.delete(id);
      onTopicRemoved(ctx, id);
      break;
    case 'producer':
      component = (state.producers.get(id) as { config: Component }).config;
      state.producers.delete(id);
      break;
    case 'consumer': {
      const consumer = state.consumers.get(id);
      if (consumer === undefined) return reject(`There is no component with id "${id}".`);
      closeConsumer(ctx, consumer);
      component = consumer.config;
      state.consumers.delete(id);
      break;
    }
    default:
      return assertNever(kind);
  }

  for (let i = state.connections.length - 1; i >= 0; i--) {
    const c = state.connections[i];
    if (c !== undefined && (c.from === id || c.to === id)) state.connections.splice(i, 1);
  }
  ctx.emit({ type: 'ComponentRemoved', component });
  return { ok: true, logged: { type: 'RemoveComponent', id } };
}

function connect(ctx: EngineContext, from: string, to: string): ApplyResult {
  const fromKind = kindOf(ctx, from);
  const toKind = kindOf(ctx, to);
  if (fromKind === undefined) return reject(`There is no component with id "${from}".`);
  if (toKind === undefined) return reject(`There is no component with id "${to}".`);
  const check = canConnect(fromKind, toKind);
  if (!check.ok) return reject(check.reason);
  if (ctx.state.connections.some((c) => c.from === from && c.to === to)) {
    return reject(`${from} → ${to} is already connected.`);
  }
  ctx.state.connections.push({ from, to });
  ctx.emit({ type: 'Connected', from, to });
  // A consumer blocked in poll() with nothing to fetch starts fetching the new topic now.
  const consumer = ctx.state.consumers.get(to);
  if (consumer !== undefined) wakeConsumer(ctx, consumer);
  return { ok: true, logged: { type: 'Connect', from, to } };
}

function disconnect(ctx: EngineContext, from: string, to: string): ApplyResult {
  const index = ctx.state.connections.findIndex((c) => c.from === from && c.to === to);
  if (index === -1) return reject(`${from} → ${to} is not connected.`);
  ctx.state.connections.splice(index, 1);
  ctx.emit({ type: 'Disconnected', from, to });
  return { ok: true, logged: { type: 'Disconnect', from, to } };
}

function produceManual(
  ctx: EngineContext,
  command: Extract<Command, { type: 'ProduceManual' }>,
): ApplyResult {
  const producer = ctx.state.producers.get(command.producerId);
  if (producer === undefined)
    return reject(`There is no producer with id "${command.producerId}".`);
  const topics = topicsOfProducer(ctx.state, producer.config.id);
  if (topics.length === 0) {
    return reject(`Producer ${producer.config.id} is not connected to a topic.`);
  }
  for (const topic of topics) sendRecord(ctx, producer, topic, command.key, command.value);
  return { ok: true, logged: command };
}

function setConsumerRunning(ctx: EngineContext, id: string, running: boolean): ApplyResult {
  const consumer = ctx.state.consumers.get(id);
  if (consumer === undefined) return reject(`There is no consumer with id "${id}".`);
  if (running) {
    if (consumer.status === 'running') return reject(`Consumer ${id} is already running.`);
    runConsumer(ctx, consumer);
    ctx.emit({ type: 'ConsumerStarted', consumerId: id });
    return { ok: true, logged: { type: 'RestartConsumer', id } };
  }
  if (consumer.status === 'stopped') return reject(`Consumer ${id} is already stopped.`);
  killConsumer(ctx, consumer);
  ctx.emit({ type: 'ConsumerStopped', consumerId: id });
  return { ok: true, logged: { type: 'KillConsumer', id } };
}

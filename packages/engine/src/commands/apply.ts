/**
 * Command handlers: validate a command against the current state, apply it, emit events.
 * A rejected command changes nothing (no state, no RNG draws, no events), so the command log,
 * which holds only accepted commands, replays exactly.
 */

import { canConnect, parseComponent, type Component, type ComponentKind } from '@kp/schema';
import { assertNever } from '../core/assert-never.ts';
import type { EngineContext } from '../model/context.ts';
import { createPartition } from '../model/partition.ts';
import { placeReplicas } from '../model/placement.ts';
import { placementBrokers } from '../model/state.ts';
import { topicsOfProducer } from '../model/state.ts';
import {
  closeConsumer,
  deleteCommittedOffsets,
  killConsumer,
  runConsumer,
  startConsumer,
  stopConsumer,
  wakeConsumer,
} from '../protocols/consume.ts';
import { isInternalTopic } from '../protocols/coordinator.ts';
import { onTopicRemoved } from '../protocols/fetch.ts';
import { sendRecord, startProducer } from '../protocols/produce.ts';
import { registerNewBroker } from '../protocols/controller.ts';
import {
  onTopicRemovedReplication,
  scheduleIsrExpiration,
  startFollowers,
} from '../protocols/replication.ts';
import { brokerLifecycle } from './broker-lifecycle.ts';
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
    case 'StopConsumer':
    case 'RestartConsumer':
      return consumerLifecycle(ctx, command);
    case 'KillBroker':
    case 'StopBroker':
    case 'RestartBroker':
      return brokerLifecycle(ctx, command);
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
      state.brokers.set(component.id, {
        config: component,
        status: 'running',
        incarnation: ++state.counters.brokerIncarnation,
        brokerEpoch: -1,
        cleanShutdownEpoch: -1,
      });
      registerNewBroker(ctx, component.id);
      scheduleIsrExpiration(ctx, component.id);
      break;
    case 'topic': {
      const assignments = placeReplicas(
        component.partitions,
        component.replicationFactor,
        placementBrokers(state),
        ctx.kernel.rng.fork(`placement:${component.id}`),
      );
      const topic = {
        topicId: ++state.counters.topic,
        config: component,
        partitions: assignments.map((replicas, index) =>
          createPartition(component.id, index, replicas),
        ),
      };
      state.topics.set(component.id, topic);
      // The followers' replica fetchers start copying from the leaders.
      for (const partition of topic.partitions) startFollowers(ctx, partition);
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
    if (isInternalTopic(component.id)) {
      return `Creation of internal topic ${component.id} is prohibited.`;
    }
    const brokers = state.brokers.size;
    // CreateTopics fails with INVALID_REPLICATION_FACTOR when RF exceeds the available brokers.
    if (component.replicationFactor > brokers) {
      return `Replication factor ${String(component.replicationFactor)} is larger than the number of brokers (${String(brokers)}). Add a broker first.`;
    }
    const usable = placementBrokers(state);
    if (component.replicationFactor > usable.length) {
      return placementTooSmall(component.replicationFactor, usable.length);
    }
    // StripedReplicaPlacer needs an unfenced broker for each partition's leader.
    if (!usable.some((b) => !b.fenced)) {
      return 'INVALID_REPLICATION_FACTOR: no broker is running, so the partitions would have no leader. Restart a broker first.';
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
        if (
          isInternalTopic(topic.config.id) &&
          topic.partitions.some((p) => p.replicas.includes(id))
        ) {
          return reject(
            `Broker ${id} holds replicas of __consumer_offsets, the internal topic where consumer groups keep their committed offsets. Moving replicas to other brokers (partition reassignment) arrives in v0.2.`,
          );
        }
        if (topic.partitions.some((p) => p.replicas.includes(id))) {
          return reject(
            `Broker ${id} holds replicas of topic "${topic.config.id}". Remove the topic first: moving replicas to other brokers (partition reassignment) arrives in v0.2.`,
          );
        }
      }
      component = (state.brokers.get(id) as { config: Component }).config;
      state.brokers.delete(id);
      state.controller.brokers.delete(id); // unregistered
      break;
    }
    case 'topic':
      if (isInternalTopic(id)) return reject(`The internal topic ${id} can't be deleted.`);
      component = (state.topics.get(id) as { config: Component }).config;
      state.topics.delete(id);
      onTopicRemoved(ctx, id);
      onTopicRemovedReplication(ctx, id);
      deleteCommittedOffsets(ctx, id);
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

  const affected: string[] = [];
  for (let i = state.connections.length - 1; i >= 0; i--) {
    const c = state.connections[i];
    if (c !== undefined && (c.from === id || c.to === id)) {
      state.connections.splice(i, 1);
      affected.push(c.to);
    }
  }
  ctx.emit({ type: 'ComponentRemoved', component });
  // Consumers that read a removed topic lose it from their subscription.
  for (const consumerId of affected) {
    const consumer = state.consumers.get(consumerId);
    if (consumer !== undefined) wakeConsumer(ctx, consumer);
  }
  return { ok: true, logged: { type: 'RemoveComponent', id } };
}

function connect(ctx: EngineContext, from: string, to: string): ApplyResult {
  const fromKind = kindOf(ctx, from);
  const toKind = kindOf(ctx, to);
  if (fromKind === undefined) return reject(`There is no component with id "${from}".`);
  if (toKind === undefined) return reject(`There is no component with id "${to}".`);
  const check = canConnect(fromKind, toKind);
  if (!check.ok) return reject(check.reason);
  // Clients can't produce to or subscribe to an internal topic here (Kafka refuses appends to
  // internal topics from producers; the coordinator owns it).
  for (const id of [from, to]) {
    if (isInternalTopic(id))
      return reject(`${id} is an internal topic: only the group coordinator writes to it.`);
  }
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
  // The subscription changed (unsubscribing a topic): a consumer blocked in poll() rejoins now.
  const consumer = ctx.state.consumers.get(to);
  if (consumer !== undefined) wakeConsumer(ctx, consumer);
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

/** KillConsumer (crash), StopConsumer (graceful close) and RestartConsumer (a new process). */
function consumerLifecycle(
  ctx: EngineContext,
  command: Extract<Command, { type: 'KillConsumer' | 'StopConsumer' | 'RestartConsumer' }>,
): ApplyResult {
  const { id } = command;
  const consumer = ctx.state.consumers.get(id);
  if (consumer === undefined) return reject(`There is no consumer with id "${id}".`);
  const notRunning = `Consumer ${id} is not running (${consumer.status}).`;
  switch (command.type) {
    case 'RestartConsumer':
      if (consumer.status === 'running') return reject(`Consumer ${id} is already running.`);
      runConsumer(ctx, consumer);
      ctx.emit({ type: 'ConsumerStarted', consumerId: id });
      break;
    case 'KillConsumer':
      if (consumer.status !== 'running') return reject(notRunning);
      killConsumer(ctx, consumer);
      ctx.emit({ type: 'ConsumerStopped', consumerId: id, reason: 'crash' });
      break;
    case 'StopConsumer':
      if (consumer.status !== 'running') return reject(notRunning);
      if (consumer.stopRequested) {
        return reject(`Consumer ${id} is already stopping: it finishes its current batch first.`);
      }
      stopConsumer(ctx, consumer);
      break;
  }
  return { ok: true, logged: command };
}

/**
 * StripedReplicaPlacer's InvalidReplicationFactorException: brokers in controlled shutdown don't
 * count as usable for placement (K13).
 */
export function placementTooSmall(replicationFactor: number, usable: number): string {
  return `INVALID_REPLICATION_FACTOR: The target replication factor of ${String(replicationFactor)} cannot be reached because only ${String(usable)} broker(s) are registered and not shutting down.`;
}

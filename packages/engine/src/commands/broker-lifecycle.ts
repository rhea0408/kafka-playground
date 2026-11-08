/**
 * KillBroker (crash), StopBroker (controlled shutdown) and RestartBroker (the process comes back
 * with its log on disk), M4 Q3 / K11. What the controller does about each is in
 * protocols/controller.ts.
 *
 * - Crash: the process stops at once. Requests in flight to it are lost; consumers' connections
 *   to it drop (they refresh metadata and retry); producers time out (M4-T7). The controller only
 *   notices when broker.session.timeout.ms passes without a heartbeat.
 * - Controlled shutdown: the broker asks to shut down in a heartbeat (wantShutDown). The
 *   controller moves its leaderships away and removes it from every ISR, then lets it stop.
 * - Restart: the broker registers with its previous broker epoch (unclean if it crashed), starts
 *   fenced, heartbeats, gets unfenced and starts fetching as a follower.
 */

import type { EngineContext } from '../model/context.ts';
import { isAlive, type BrokerState } from '../model/state.ts';
import type { EngineTask } from '../model/tasks.ts';
import { wakeConsumer } from '../protocols/consume.ts';
import {
  BROKER_HEARTBEAT_INTERVAL_MS,
  scheduleBrokerHeartbeat,
  sendBrokerHeartbeat,
} from '../protocols/controller.ts';
import { onBrokerStoppedForConsumers } from '../protocols/fetch.ts';
import {
  ensureReplicaFetcher,
  onBrokerStopped,
  scheduleIsrExpiration,
} from '../protocols/replication.ts';
import { reject, type ApplyResult } from './apply.ts';
import type { Command } from './index.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

export function brokerLifecycle(
  ctx: EngineContext,
  command: Extract<Command, { type: 'KillBroker' | 'StopBroker' | 'RestartBroker' }>,
): ApplyResult {
  const { id } = command;
  const broker = ctx.state.brokers.get(id);
  if (broker === undefined) return reject(`There is no broker with id "${id}".`);
  switch (command.type) {
    case 'RestartBroker':
      if (isAlive(broker.status)) return reject(`Broker ${id} is already running.`);
      broker.status = 'running';
      broker.incarnation = ++ctx.state.counters.brokerIncarnation;
      ctx.emit({ type: 'BrokerStarted', brokerId: id });
      ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
        type: 'BrokerRegistrationRequest',
        broker: id,
        incarnation: broker.incarnation,
        previousBrokerEpoch: broker.brokerEpoch,
      });
      break;
    case 'KillBroker':
      if (!isAlive(broker.status)) return reject(`Broker ${id} is not running (${broker.status}).`);
      stopProcess(ctx, broker, 'crashed');
      ctx.emit({ type: 'BrokerStopped', brokerId: id, reason: 'crash' });
      break;
    case 'StopBroker':
      if (broker.status === 'shutting-down') {
        return reject(`Broker ${id} is already shutting down: it waits for its leaders to move.`);
      }
      if (!isAlive(broker.status)) return reject(`Broker ${id} is not running (${broker.status}).`);
      broker.status = 'shutting-down';
      sendBrokerHeartbeat(ctx, id); // BrokerLifecycleManager#beginControlledShutdown
      break;
  }
  return { ok: true, logged: command };
}

/** The process is gone: its memory (purgatories, fetchers, connections) goes with it. */
function stopProcess(ctx: EngineContext, broker: BrokerState, status: 'crashed' | 'stopped'): void {
  const id = broker.config.id;
  broker.status = status;
  broker.incarnation = ++ctx.state.counters.brokerIncarnation;
  onBrokerStopped(ctx, id);
  onBrokerStoppedForConsumers(ctx, id);
  // Consumers with a fetch in flight to it see the connection drop: they ask for fresh
  // metadata and fetch again (NetworkClient#handleDisconnections).
  for (const consumer of ctx.state.consumers.values()) {
    if (!consumer.fetchesInFlight.delete(id)) continue;
    consumer.metadata.updateRequested = true;
    wakeConsumer(ctx, consumer);
  }
}

/** The controller let a broker in controlled shutdown stop (shouldShutDown). */
export function handleBrokerShutdownAllowed(
  ctx: EngineContext,
  task: Task<'BrokerShutdownAllowed'>,
): void {
  const broker = ctx.state.brokers.get(task.broker);
  if (broker?.incarnation !== task.incarnation || broker.status !== 'shutting-down') return;
  stopProcess(ctx, broker, 'stopped');
  ctx.emit({ type: 'BrokerStopped', brokerId: task.broker, reason: 'shutdown' });
}

/** The registration answer: the broker heartbeats (to be unfenced) and starts its fetchers. */
export function handleBrokerRegistrationResponse(
  ctx: EngineContext,
  task: Task<'BrokerRegistrationResponse'>,
): void {
  const broker = ctx.state.brokers.get(task.broker);
  if (broker?.incarnation !== task.incarnation || !isAlive(broker.status)) return;
  broker.brokerEpoch = task.brokerEpoch;
  sendBrokerHeartbeat(ctx, task.broker);
  scheduleBrokerHeartbeat(ctx, task.broker, BROKER_HEARTBEAT_INTERVAL_MS);
  scheduleIsrExpiration(ctx, task.broker);
  for (const topic of ctx.state.topics.values()) {
    for (const partition of topic.partitions) {
      const { leader } = partition;
      if (leader !== null && leader !== task.broker && partition.replicas.includes(task.broker)) {
        ensureReplicaFetcher(ctx, task.broker, leader);
      }
    }
  }
}

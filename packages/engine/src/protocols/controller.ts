/**
 * The KRaft controller quorum (ADR 0005, M4 Q2): one abstract controller, always up. It keeps
 * the partition metadata (replicas, ISR, ELR, leader, epochs) and changes it on request:
 *
 * - AlterPartition from a leader that wants to shrink or expand the ISR (M4-T4, K4): the
 *   controller checks the leader epoch and partition epoch, refuses ineligible replicas
 *   (KIP-841), applies the change, bumps the partition epoch and answers.
 * - Broker liveness and elections (M4-T5).
 *
 * Every change goes through `applyPartitionChange`, a port of the parts of PartitionChangeBuilder
 * the playground needs, including Eligible Leader Replicas (KIP-966, on by default in Kafka 4.4,
 * K1): when the new ISR is below min.insync.replicas, the replicas leaving it become ELR members
 * (they have every committed record, since the HW froze); at or above min.insync.replicas the ELR
 * is cleared.
 */

import type { EngineEventPayload } from '../events/index.ts';
import type { EngineContext } from '../model/context.ts';
import type { PartitionState } from '../model/partition.ts';
import { isAlive, type TopicState } from '../model/state.ts';
import { onLeaderChanged, onLeaderStateChanged } from './replication.ts';
import type { EngineTask } from '../model/tasks.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/** Whether a broker may be in an ISR (KIP-841): registered, unfenced, not shutting down. */
export function isrEligible(ctx: EngineContext, brokerId: string): boolean {
  return isActive(ctx, brokerId);
}

export interface PartitionChange {
  readonly isr?: readonly string[];
  readonly leader?: string | null;
  /** Replicas that shut down uncleanly: they never enter the ELR (K1). */
  readonly uncleanShutdown?: readonly string[];
}

/** What changed, for the caller's events. */
export interface PartitionChangeResult {
  readonly removed: readonly string[];
  readonly added: readonly string[];
  readonly leaderChanged: boolean;
}

/**
 * Applies a metadata change to a partition (PartitionChangeBuilder): ISR, ELR, leader, epochs.
 * The ISR keeps replica order. A leader change bumps the leader epoch; every change bumps the
 * partition epoch.
 */
export function applyPartitionChange(
  topic: TopicState,
  partition: PartitionState,
  change: PartitionChange,
): PartitionChangeResult {
  const before = partition.isr;
  const targetIsr =
    change.isr === undefined
      ? before
      : partition.replicas.filter((b) => (change.isr as readonly string[]).includes(b));
  const removed = before.filter((b) => !targetIsr.includes(b));
  const added = targetIsr.filter((b) => !before.includes(b));
  // PartitionChangeBuilder#maybePopulateTargetElr (K1).
  const minIsr = Math.min(topic.config.minInsyncReplicas, partition.replicas.length);
  if (targetIsr.length >= minIsr) {
    partition.elr = [];
    partition.lastKnownElr = [];
  } else {
    const unclean = new Set(change.uncleanShutdown ?? []);
    const candidates = new Set([...partition.elr, ...before]);
    partition.elr = partition.replicas.filter(
      (b) => candidates.has(b) && !targetIsr.includes(b) && !unclean.has(b),
    );
  }
  partition.isr = targetIsr;
  let leaderChanged = false;
  if (change.leader !== undefined && change.leader !== partition.leader) {
    partition.leader = change.leader;
    partition.leaderEpoch += 1;
    leaderChanged = true;
  }
  partition.partitionEpoch += 1;
  return { removed, added, leaderChanged };
}

/**
 * The leader sends AlterPartition (+ network delay). It keeps the maximal ISR in `pendingIsr`
 * until the answer arrives (K4); only one request per partition is in flight.
 */
export function sendAlterPartition(
  ctx: EngineContext,
  topic: TopicState,
  partition: PartitionState,
  newIsr: readonly string[],
): void {
  const leader = partition.leader;
  if (leader === null) return;
  // The maximal ISR: the union of the committed ISR and the proposed one (a shrink keeps the old
  // members, an expansion adds the new one, until the controller confirms).
  partition.pendingIsr = partition.replicas.filter(
    (b) => partition.isr.includes(b) || newIsr.includes(b),
  );
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'AlterPartitionRequest',
    leader,
    leaderIncarnation: ctx.state.brokers.get(leader)?.incarnation ?? -1,
    topic: topic.config.id,
    topicId: topic.topicId,
    partition: partition.index,
    leaderEpoch: partition.leaderEpoch,
    partitionEpoch: partition.partitionEpoch,
    newIsr: [...newIsr],
  });
}

/** AlterPartition reaches the controller: validate, apply, answer (ReplicationControlManager). */
export function handleAlterPartitionRequest(
  ctx: EngineContext,
  task: Task<'AlterPartitionRequest'>,
): void {
  const topic = ctx.state.topics.get(task.topic);
  const partition = topic?.partitions[task.partition];
  const answer = (error: Task<'AlterPartitionResponse'>['error']): void => {
    ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
      type: 'AlterPartitionResponse',
      leader: task.leader,
      leaderIncarnation: task.leaderIncarnation,
      topic: task.topic,
      topicId: task.topicId,
      partition: task.partition,
      error,
    });
  };
  if (topic === undefined || partition === undefined || topic.topicId !== task.topicId) {
    answer('UNKNOWN_TOPIC_OR_PARTITION');
    return;
  }
  if (partition.leader !== task.leader || partition.leaderEpoch !== task.leaderEpoch) {
    answer('FENCED_LEADER_EPOCH');
    return;
  }
  if (partition.partitionEpoch !== task.partitionEpoch) {
    answer('INVALID_UPDATE_VERSION');
    return;
  }
  // KIP-841: only registered, unfenced brokers not shutting down may join the ISR.
  const joining = task.newIsr.filter((b) => !partition.isr.includes(b));
  if (joining.some((b) => !isrEligible(ctx, b))) {
    answer('INELIGIBLE_REPLICA');
    return;
  }
  const { removed, added } = applyPartitionChange(topic, partition, { isr: task.newIsr });
  if (removed.length > 0) {
    ctx.emit({
      type: 'IsrShrunk',
      topic: task.topic,
      partition: task.partition,
      removed,
      isr: [...partition.isr],
      elr: [...partition.elr],
      partitionEpoch: partition.partitionEpoch,
    });
  }
  if (added.length > 0) {
    ctx.emit({
      type: 'IsrExpanded',
      topic: task.topic,
      partition: task.partition,
      added,
      isr: [...partition.isr],
      partitionEpoch: partition.partitionEpoch,
    });
  }
  answer(null);
}

// ── Broker liveness, fencing and elections (M4-T5) ─────────────────────────────────────────────

/** broker.heartbeat.interval.ms (KRaftConfigs default 2000; read-only, K2). */
export const BROKER_HEARTBEAT_INTERVAL_MS = 2000;
/** broker.session.timeout.ms (KRaftConfigs default 9000; read-only, K2). */
export const BROKER_SESSION_TIMEOUT_MS = 9000;

type ElectionReason = Extract<EngineEventPayload, { type: 'LeaderElected' }>['reason'];

/** Whether the controller would accept `brokerId` as a leader (LeaderAcceptor, K12). */
export function isActive(ctx: EngineContext, brokerId: string): boolean {
  const entry = ctx.state.controller.brokers.get(brokerId);
  const broker = ctx.state.brokers.get(brokerId);
  return (
    entry !== undefined &&
    broker !== undefined &&
    isAlive(broker.status) &&
    !entry.fenced &&
    !entry.inControlledShutdown
  );
}

/**
 * Registers a broker added to the cluster (AddComponent, or the initial topology): it gets a
 * broker epoch, starts unfenced (the game skips the catch-up wait of a brand-new broker) and
 * starts heartbeating.
 */
export function registerNewBroker(ctx: EngineContext, brokerId: string): void {
  const { controller } = ctx.state;
  const broker = ctx.state.brokers.get(brokerId);
  if (broker === undefined) return;
  const epoch = controller.nextBrokerEpoch++;
  controller.brokers.set(brokerId, {
    brokerEpoch: epoch,
    fenced: false,
    inControlledShutdown: false,
    sessionToken: 0,
    cleanShutdownEpoch: null,
  });
  broker.brokerEpoch = epoch;
  armSession(ctx, brokerId);
  scheduleBrokerHeartbeat(ctx, brokerId, BROKER_HEARTBEAT_INTERVAL_MS);
  if (!controller.preferredCheckScheduled) schedulePreferredLeaderCheck(ctx);
}

/** The broker's next heartbeat, `delayMs` from now. */
export function scheduleBrokerHeartbeat(
  ctx: EngineContext,
  brokerId: string,
  delayMs: number,
): void {
  const broker = ctx.state.brokers.get(brokerId);
  if (broker === undefined) return;
  ctx.kernel.scheduleIn(delayMs, {
    type: 'BrokerHeartbeatTimer',
    broker: brokerId,
    incarnation: broker.incarnation,
  });
}

/** (Re)starts the controller's session timer for a broker (BrokerHeartbeatManager). */
function armSession(ctx: EngineContext, brokerId: string): void {
  const entry = ctx.state.controller.brokers.get(brokerId);
  if (entry === undefined) return;
  entry.sessionToken += 1;
  ctx.kernel.scheduleIn(BROKER_SESSION_TIMEOUT_MS, {
    type: 'ControllerSessionExpire',
    broker: brokerId,
    token: entry.sessionToken,
  });
}

/** Sends one heartbeat now (BrokerLifecycleManager). */
export function sendBrokerHeartbeat(ctx: EngineContext, brokerId: string): void {
  const broker = ctx.state.brokers.get(brokerId);
  if (broker === undefined || !isAlive(broker.status)) return;
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'BrokerHeartbeatRequest',
    broker: brokerId,
    incarnation: broker.incarnation,
    brokerEpoch: broker.brokerEpoch,
    wantShutDown: broker.status === 'shutting-down',
  });
}

export function handleBrokerHeartbeatTimer(
  ctx: EngineContext,
  task: Task<'BrokerHeartbeatTimer'>,
): void {
  const broker = ctx.state.brokers.get(task.broker);
  if (broker === undefined || broker.incarnation !== task.incarnation || !isAlive(broker.status)) {
    return; // the process is gone: no more heartbeats
  }
  sendBrokerHeartbeat(ctx, task.broker);
  scheduleBrokerHeartbeat(ctx, task.broker, BROKER_HEARTBEAT_INTERVAL_MS);
}

/**
 * A heartbeat reaches the controller: the session is renewed. A fenced broker that is not
 * shutting down is unfenced (it has caught up with the metadata log). `wantShutDown` starts or
 * continues a controlled shutdown (K11).
 */
export function handleBrokerHeartbeatRequest(
  ctx: EngineContext,
  task: Task<'BrokerHeartbeatRequest'>,
): void {
  const entry = ctx.state.controller.brokers.get(task.broker);
  if (entry === undefined || entry.brokerEpoch !== task.brokerEpoch) return; // stale epoch
  armSession(ctx, task.broker);
  if (task.wantShutDown) {
    controlledShutdown(ctx, task.broker, task.incarnation);
    return;
  }
  if (entry.fenced && !entry.inControlledShutdown) {
    entry.fenced = false;
    ctx.emit({ type: 'BrokerUnfenced', brokerId: task.broker });
    // handleBrokerUnfenced: elections only for partitions that have no leader (K2).
    electLeaderless(ctx);
  }
}

/**
 * Controlled shutdown (K11): the controller removes the broker from every ISR and moves its
 * leaderships to other in-sync replicas. Once it leads nothing, the broker is fenced and told it
 * may shut down.
 */
function controlledShutdown(ctx: EngineContext, brokerId: string, incarnation: number): void {
  const entry = ctx.state.controller.brokers.get(brokerId);
  if (entry === undefined) return;
  entry.inControlledShutdown = true;
  removeBrokerFromPartitions(ctx, brokerId, 'controlled-shutdown', false);
  const leadsSomething = [...ctx.state.topics.values()].some((t) =>
    t.partitions.some((p) => p.leader === brokerId),
  );
  if (leadsSomething) return; // it asks again with its next heartbeat
  if (!entry.fenced) {
    entry.fenced = true;
    entry.cleanShutdownEpoch = entry.brokerEpoch;
    ctx.emit({ type: 'BrokerFenced', brokerId, reason: 'controlled-shutdown' });
  }
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'BrokerShutdownAllowed',
    broker: brokerId,
    incarnation,
  });
}

/** broker.session.timeout.ms without a heartbeat: the controller fences the broker (K2). */
export function handleControllerSessionExpire(
  ctx: EngineContext,
  task: Task<'ControllerSessionExpire'>,
): void {
  const entry = ctx.state.controller.brokers.get(task.broker);
  if (entry === undefined || entry.sessionToken !== task.token || entry.fenced) return;
  entry.fenced = true;
  ctx.emit({ type: 'BrokerFenced', brokerId: task.broker, reason: 'session-timeout' });
  removeBrokerFromPartitions(ctx, task.broker, 'failover', false);
}

/**
 * A restarted broker registers (ClusterControlManager#registerBroker). Its previous broker epoch
 * matches the stored one only after a controlled shutdown; otherwise the shutdown was unclean and
 * it is removed from every ISR **and ELR** (K1). It starts fenced, with a new broker epoch.
 */
export function handleBrokerRegistrationRequest(
  ctx: EngineContext,
  task: Task<'BrokerRegistrationRequest'>,
): void {
  const { controller } = ctx.state;
  const entry = controller.brokers.get(task.broker);
  if (entry === undefined) return;
  const clean =
    entry.cleanShutdownEpoch !== null && entry.cleanShutdownEpoch === task.previousBrokerEpoch;
  if (!clean) removeBrokerFromPartitions(ctx, task.broker, 'failover', true);
  entry.brokerEpoch = controller.nextBrokerEpoch++;
  entry.fenced = true;
  entry.inControlledShutdown = false;
  entry.cleanShutdownEpoch = null;
  armSession(ctx, task.broker);
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'BrokerRegistrationResponse',
    broker: task.broker,
    incarnation: task.incarnation,
    brokerEpoch: entry.brokerEpoch,
  });
}

/**
 * Removes a broker from every ISR (and, after an unclean shutdown, every ELR) and moves its
 * leaderships (ReplicationControlManager#generateLeaderAndIsrUpdates).
 */
function removeBrokerFromPartitions(
  ctx: EngineContext,
  brokerId: string,
  reason: ElectionReason,
  unclean: boolean,
): void {
  for (const topic of ctx.state.topics.values()) {
    for (const partition of topic.partitions) {
      const inIsr = partition.isr.includes(brokerId);
      const inElr = partition.elr.includes(brokerId);
      const leads = partition.leader === brokerId;
      if (!inIsr && !leads && !(unclean && inElr)) continue;
      if (unclean) partition.elr = partition.elr.filter((b) => b !== brokerId);
      const targetIsr = partition.isr.filter((b) => b !== brokerId);
      changePartition(ctx, topic, partition, targetIsr, reason, unclean ? [brokerId] : []);
    }
  }
}

/** Elections for every leaderless partition (after an unfence, or unclean election enabled). */
export function electLeaderless(ctx: EngineContext, onlyTopic?: string): void {
  for (const topic of ctx.state.topics.values()) {
    if (onlyTopic !== undefined && topic.config.id !== onlyTopic) continue;
    for (const partition of topic.partitions) {
      if (partition.leader !== null) continue;
      changePartition(ctx, topic, partition, partition.isr, 'failover', []);
    }
  }
}

/**
 * Applies a new ISR and elects a leader if the current one is no longer valid
 * (PartitionChangeBuilder#electLeader, K1, K12, K14), then emits the events and tells the
 * brokers.
 */
function changePartition(
  ctx: EngineContext,
  topic: TopicState,
  partition: PartitionState,
  targetIsr: readonly string[],
  reason: ElectionReason,
  uncleanShutdown: readonly string[],
): void {
  const previousLeader = partition.leader;
  const election = electLeader(ctx, topic, partition, targetIsr);
  let isr = election?.isr ?? targetIsr;
  if (election === null && isr.length === 0 && partition.isr.length > 0 && !elrEnabled()) {
    isr = partition.isr; // ELR off: the last member stays in the ISR (unreachable while ELR is on)
  }
  const newLeader = election?.leader ?? null;
  const isrBefore = [...partition.isr];
  if (newLeader === null && previousLeader !== null && partition.lastKnownElr.length === 0) {
    partition.lastKnownElr = [previousLeader]; // maybeUpdateLastKnownLeader
  }
  const { removed, leaderChanged } = applyPartitionChange(topic, partition, {
    isr,
    leader: newLeader,
    uncleanShutdown,
  });
  if (election?.dropElr === true) partition.elr = partition.elr.filter((b) => b !== newLeader);
  if (election?.unclean === true) partition.elr = [];
  if (newLeader !== null) partition.lastKnownElr = [];
  if (removed.length > 0) {
    ctx.emit({
      type: 'IsrShrunk',
      topic: topic.config.id,
      partition: partition.index,
      removed: removed.filter((b) => isrBefore.includes(b)),
      isr: [...partition.isr],
      elr: [...partition.elr],
      partitionEpoch: partition.partitionEpoch,
    });
  }
  if (!leaderChanged) {
    // The leader learns the new ISR from the metadata (instantly here) and may move its HW.
    onLeaderStateChanged(ctx, topic, partition);
    return;
  }
  if (newLeader === null) {
    ctx.emit({
      type: 'PartitionOffline',
      topic: topic.config.id,
      partition: partition.index,
      previousLeader: previousLeader ?? '',
      isr: [...partition.isr],
      elr: [...partition.elr],
    });
  } else {
    ctx.emit({
      type: 'LeaderElected',
      topic: topic.config.id,
      partition: partition.index,
      leader: newLeader,
      previousLeader,
      leaderEpoch: partition.leaderEpoch,
      reason: election?.unclean === true ? 'unclean' : reason,
    });
  }
  onLeaderChanged(ctx, topic, partition, previousLeader);
}

/** Eligible Leader Replicas are on, as in a fresh Kafka 4.4 cluster (K1). */
const elrEnabled = (): boolean => true;

interface Election {
  readonly leader: string;
  /** The new ISR when the election changes it (ELR / last known leader / unclean: [leader]). */
  readonly isr?: readonly string[];
  readonly dropElr?: boolean;
  readonly unclean?: boolean;
}

/** PartitionChangeBuilder#electLeader / electAnyLeader (K1, K12, K14). */
function electLeader(
  ctx: EngineContext,
  topic: TopicState,
  partition: PartitionState,
  targetIsr: readonly string[],
): Election | null {
  const active = (b: string) => isActive(ctx, b);
  const current = partition.leader;
  if (current !== null && targetIsr.includes(current) && active(current))
    return { leader: current };
  if (targetIsr.length > 0) {
    const fromIsr = partition.replicas.find((b) => targetIsr.includes(b) && active(b));
    if (fromIsr !== undefined) return { leader: fromIsr };
  } else {
    const fromElr = partition.replicas.find((b) => partition.elr.includes(b) && active(b));
    if (fromElr !== undefined) return { leader: fromElr, isr: [fromElr], dropElr: true };
    const [lastKnown] = partition.lastKnownElr;
    if (
      partition.elr.length === 0 &&
      partition.lastKnownElr.length === 1 &&
      lastKnown !== undefined &&
      active(lastKnown)
    ) {
      // canElectLastKnownLeader: no ELR, the last leader is back; Kafka flags it unclean.
      return { leader: lastKnown, isr: [lastKnown], unclean: true };
    }
  }
  if (topic.config.uncleanLeaderElectionEnable) {
    const any = partition.replicas.find(active);
    if (any !== undefined) return { leader: any, isr: [any], unclean: true };
  }
  return null;
}

/** Starts the controller's periodic preferred-leader check (once). */
function schedulePreferredLeaderCheck(ctx: EngineContext): void {
  const first = [...ctx.state.brokers.values()].sort((a, b) =>
    a.config.id < b.config.id ? -1 : a.config.id > b.config.id ? 1 : 0,
  )[0];
  if (first === undefined) return;
  ctx.state.controller.preferredCheckScheduled = true;
  ctx.kernel.scheduleIn(first.config.leaderImbalanceCheckIntervalSeconds * 1000, {
    type: 'PreferredLeaderCheck',
  });
}

/**
 * auto.leader.rebalance.enable: every leader.imbalance.check.interval.seconds the controller
 * moves leadership back to each partition's preferred replica (replicas[0]) when it is in the
 * ISR and active (PREFERRED election, Q10). The settings are read from the broker with the lowest
 * id (the controller's own config in Kafka).
 */
export function handlePreferredLeaderCheck(ctx: EngineContext): void {
  ctx.state.controller.preferredCheckScheduled = false;
  const first = [...ctx.state.brokers.values()].sort((a, b) =>
    a.config.id < b.config.id ? -1 : a.config.id > b.config.id ? 1 : 0,
  )[0];
  if (first === undefined) return;
  if (first.config.autoLeaderRebalanceEnable) {
    for (const topic of ctx.state.topics.values()) {
      for (const partition of topic.partitions) {
        const [preferred] = partition.replicas;
        if (preferred === undefined || partition.leader === preferred) continue;
        if (!partition.isr.includes(preferred) || !isActive(ctx, preferred)) continue;
        const previousLeader = partition.leader;
        applyPartitionChange(topic, partition, { leader: preferred });
        ctx.emit({
          type: 'LeaderElected',
          topic: topic.config.id,
          partition: partition.index,
          leader: preferred,
          previousLeader,
          leaderEpoch: partition.leaderEpoch,
          reason: 'preferred',
        });
        onLeaderChanged(ctx, topic, partition, previousLeader);
      }
    }
  }
  schedulePreferredLeaderCheck(ctx);
}

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

import type { EngineContext } from '../model/context.ts';
import type { PartitionState } from '../model/partition.ts';
import type { TopicState } from '../model/state.ts';
import type { EngineTask } from '../model/tasks.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/** Whether a broker may be in an ISR (KIP-841): running, unfenced, not shutting down. */
export function isrEligible(ctx: EngineContext, brokerId: string): boolean {
  return ctx.state.brokers.get(brokerId)?.status === 'running';
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

/**
 * One readable line per event, for the headless demo and later the Event list panel (FR-6.3):
 *   00:01.500  RecordAppended   orders-1  offset=2  key=B
 */

import { assertNever } from '../core/assert-never.ts';
import { partitionName } from '../model/partition.ts';
import type { EngineEvent } from './index.ts';

/** Sim time as mm:ss.mmm. */
export function formatSimTime(t: number): string {
  const minutes = Math.floor(t / 60_000);
  const seconds = Math.floor((t % 60_000) / 1000);
  const ms = t % 1000;
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}.${String(ms).padStart(3, '0')}`;
}

const keyText = (key: string | null): string => (key === null ? 'key=null' : `key=${key}`);

/** What happened, without the time: "orders-1  offset=2  key=B". */
export function describeEvent(e: EngineEvent): string {
  switch (e.type) {
    case 'ComponentAdded':
    case 'ComponentRemoved':
      return `${e.component.kind} ${e.component.id}`;
    case 'Connected':
    case 'Disconnected':
      return `${e.from} → ${e.to}`;
    case 'RecordProduced':
      return `${e.producerId} → ${partitionName(e.topic, e.partition)}  ${keyText(e.key)}  value=${e.value}  acks=${e.acks}`;
    case 'RecordAppended':
      return `${partitionName(e.topic, e.partition)}  offset=${String(e.offset)}  ${keyText(e.key)}`;
    case 'ProduceAcked':
      return `${e.producerId}  ${partitionName(e.topic, e.partition)}  ${e.offset === null ? 'offset=-1 (acks=0: not known)' : `offset=${String(e.offset)}`}`;
    case 'ProduceFailed':
      return `${e.producerId}  ${partitionName(e.topic, e.partition)}  ${e.error}`;
    case 'ProduceRetried':
      return `${e.producerId}  ${partitionName(e.topic, e.partition)}  ${e.error}  retry ${String(e.attempt)} in ${String(e.backoffMs)} ms`;
    case 'RecordConsumed':
      return `${e.consumerId} ← ${partitionName(e.topic, e.partition)}  offset=${String(e.offset)}  ${keyText(e.key)}`;
    case 'OffsetCommitted':
      return `${e.groupId}  ${partitionName(e.topic, e.partition)}  offset=${String(e.offset)}${e.generation >= 0 ? `  gen=${String(e.generation)}` : ''}`;
    case 'MemberJoined':
      return `${e.groupId}  ${e.consumerId} joined as ${e.memberId}`;
    case 'MemberRemoved':
      return `${e.groupId}  ${e.consumerId} (${e.memberId}) removed: ${e.reason}`;
    case 'RebalanceStarted':
      return `${e.groupId}  gen=${String(e.generation)}  reason=${e.reason}`;
    case 'RebalanceCompleted':
      return e.members.length === 0
        ? `${e.groupId}  gen=${String(e.generation)}  no members (Empty)`
        : `${e.groupId}  gen=${String(e.generation)}  ${e.protocol ?? ''}  ${e.members
            .map((m) => `${m.consumerId}: ${partitionsText(m.partitions)}`)
            .join(', ')}`;
    case 'PartitionsAssigned':
      return `${e.consumerId}  ${e.groupId}  gen=${String(e.generation)}  ${partitionsText(e.partitions)}`;
    case 'PartitionsRevoked':
    case 'PartitionsLost':
      return `${e.consumerId}  ${e.groupId}  ${partitionsText(e.partitions)}`;
    case 'ConsumerIdle':
      return `${e.consumerId}  ${e.groupId}  gen=${String(e.generation)}  no partition`;
    case 'CommitFailed':
      return `${e.consumerId}  ${e.groupId}  ${e.error}  ${partitionsText(e.partitions)}`;
    case 'ConsumerStopped':
      return `${e.consumerId}  ${e.reason}`;
    case 'CoordinatorMoved':
      return `${e.groupId}  coordinator ${e.from ?? 'none'} → ${e.to === '' ? 'none' : e.to}  ${e.state} gen=${String(e.generation)}`;
    case 'ConsumerStarted':
      return e.consumerId;
    case 'DuplicateProcessed':
      return `${e.consumerId} ← ${partitionName(e.topic, e.partition)}  offset=${String(e.offset)}  already processed by ${e.groupId}`;
    case 'RecordsSkipped':
      return `${e.consumerId}  ${partitionName(e.topic, e.partition)}  offsets ${String(e.from)}–${String(e.to)} never processed by ${e.groupId}`;
    case 'ConfigUpdated':
      return `${e.after.kind} ${e.id}  ${configChanges(e.before, e.after)}`;
    case 'ReplicaFetched':
      return `${e.broker} ← ${e.leader}  ${partitionName(e.topic, e.partition)}  offsets ${String(e.fromOffset)}–${String(e.toOffset - 1)}`;
    case 'IsrShrunk':
      return `${partitionName(e.topic, e.partition)}  removed ${e.removed.join(' ')}  isr=[${e.isr.join(' ')}]${e.elr.length > 0 ? `  elr=[${e.elr.join(' ')}]` : ''}`;
    case 'IsrExpanded':
      return `${partitionName(e.topic, e.partition)}  added ${e.added.join(' ')}  isr=[${e.isr.join(' ')}]`;
    case 'BrokerFenced':
      return `${e.brokerId}  ${e.reason}`;
    case 'BrokerUnfenced':
      return e.brokerId;
    case 'LeaderElected':
      return `${partitionName(e.topic, e.partition)}  leader ${e.previousLeader ?? 'none'} → ${e.leader}  epoch=${String(e.leaderEpoch)}  ${electionReason(e)}`;
    case 'PartitionOffline':
      return `${partitionName(e.topic, e.partition)}  no leader (was ${e.previousLeader})  isr=[${e.isr.join(' ')}]  elr=[${e.elr.join(' ')}]`;
    case 'RecordsTruncated':
      return `${e.broker}  ${partitionName(e.topic, e.partition)}  offsets ${String(e.from)}–${String(e.to - 1)} deleted${e.acked > 0 ? `  ${String(e.acked)} acknowledged record(s) lost` : ''}`;
    case 'BrokerStarted':
      return e.brokerId;
    case 'BrokerStopped':
      return `${e.brokerId}  ${e.reason}`;
    default:
      return assertNever(e);
  }
}

/**
 * The election's reason, with the two unclean kinds told apart (M4 follow-up G-2): Kafka flags
 * both unclean, but only an out-of-sync replica taking over can lose records. The last known
 * leader held the only copy (KIP-966).
 */
function electionReason(e: Extract<EngineEvent, { type: 'LeaderElected' }>): string {
  if (e.reason !== 'unclean') return e.reason;
  return e.lastKnownLeader
    ? 'unclean (last known leader: it held the only copy, nothing lost)'
    : 'unclean (out-of-sync replica: records may be lost)';
}

/** "orders-0 orders-2", or "none". */
function partitionsText(partitions: readonly { topic: string; partition: number }[]): string {
  return partitions.length === 0
    ? 'none'
    : partitions.map((p) => partitionName(p.topic, p.partition)).join(' ');
}

/** "partitions: 3 → 4, acks: all → 1" for the fields that differ. */
function configChanges(before: object, after: object): string {
  const b = before as Record<string, unknown>;
  const a = after as Record<string, unknown>;
  return Object.keys(a)
    .filter((k) => JSON.stringify(b[k]) !== JSON.stringify(a[k]))
    .map((k) => `${k}: ${valueText(b[k])} → ${valueText(a[k])}`)
    .join(', ');
}

function valueText(v: unknown): string {
  return typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean'
    ? String(v)
    : JSON.stringify(v);
}

export function formatEvent(e: EngineEvent): string {
  return `${formatSimTime(e.t)}  ${e.type.padEnd(16)} ${describeEvent(e)}`;
}

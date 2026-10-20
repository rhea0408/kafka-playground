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
    case 'RecordConsumed':
      return `${e.consumerId} ← ${partitionName(e.topic, e.partition)}  offset=${String(e.offset)}  ${keyText(e.key)}`;
    case 'OffsetCommitted':
      return `${e.groupId}  ${partitionName(e.topic, e.partition)}  offset=${String(e.offset)}`;
    case 'ConsumerStopped':
    case 'ConsumerStarted':
      return e.consumerId;
    case 'ConfigUpdated':
      return `${e.after.kind} ${e.id}  ${configChanges(e.before, e.after)}`;
    default:
      return assertNever(e);
  }
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

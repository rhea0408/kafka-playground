/**
 * Domain events: the single stream the engine emits (architecture §4.5, FR-2.16, NFR-8.4). The UI,
 * animations, tips, objectives and level runtime all consume it. A discriminated union on `type`.
 * The kernel stamps each event with `{ t, seq }` when it is emitted.
 */

import type { Acks, Component } from '@kp/schema';
import type { Stamped } from '../core/kernel.ts';
import type { ProduceError } from '../model/tasks.ts';

export type EngineEventPayload =
  | { readonly type: 'ComponentAdded'; readonly component: Component }
  | { readonly type: 'ComponentRemoved'; readonly component: Component }
  | { readonly type: 'Connected'; readonly from: string; readonly to: string }
  | { readonly type: 'Disconnected'; readonly from: string; readonly to: string }
  /** The producer created a record and chose its partition (client side). */
  | {
      readonly type: 'RecordProduced';
      readonly producerId: string;
      readonly topic: string;
      readonly partition: number;
      readonly key: string | null;
      readonly value: string;
      readonly traceId: string;
      readonly acks: Acks;
    }
  /** The partition leader appended the record and assigned its offset. */
  | {
      readonly type: 'RecordAppended';
      readonly topic: string;
      readonly partition: number;
      readonly offset: number;
      readonly key: string | null;
      readonly value: string;
      readonly producerId: string;
      readonly traceId: string;
    }
  /**
   * The producer considers the record delivered. `offset` is null for acks=0, where Kafka reports
   * -1 because the producer never hears back from the broker.
   */
  | {
      readonly type: 'ProduceAcked';
      readonly producerId: string;
      readonly topic: string;
      readonly partition: number;
      readonly traceId: string;
      readonly offset: number | null;
    }
  /** The broker answered the produce request with an error. */
  | {
      readonly type: 'ProduceFailed';
      readonly producerId: string;
      readonly topic: string;
      readonly partition: number;
      readonly traceId: string;
      readonly error: ProduceError;
    }
  /** poll() returned this record to the consumer application. */
  | {
      readonly type: 'RecordConsumed';
      readonly consumerId: string;
      readonly groupId: string;
      readonly topic: string;
      readonly partition: number;
      readonly offset: number;
      readonly key: string | null;
      readonly value: string;
      readonly traceId: string;
    }
  /** The group saved `offset` (the next offset to read) for this partition. */
  | {
      readonly type: 'OffsetCommitted';
      readonly groupId: string;
      readonly consumerId: string;
      readonly topic: string;
      readonly partition: number;
      readonly offset: number;
    }
  /** The consumer process crashed or was stopped (KillConsumer). Nothing was committed. */
  | { readonly type: 'ConsumerStopped'; readonly consumerId: string }
  /** The consumer process started again (RestartConsumer); it resumes from committed offsets. */
  | { readonly type: 'ConsumerStarted'; readonly consumerId: string }
  /** UpdateConfig changed a component's config. Not emitted for a patch that changes nothing. */
  | {
      readonly type: 'ConfigUpdated';
      readonly id: string;
      readonly before: Component;
      readonly after: Component;
    };

export type EngineEvent = Stamped<EngineEventPayload>;

export type EngineEventType = EngineEvent['type'];

/**
 * Domain events: the single stream the engine emits (architecture §4.5, FR-2.16, NFR-8.4). The UI,
 * animations, tips, objectives and level runtime all consume it. A discriminated union on `type`.
 * The kernel stamps each event with `{ t, seq }` when it is emitted.
 */

import type { Acks, Assignor, Component } from '@kp/schema';
import type { Stamped } from '../core/kernel.ts';
import type { MemberRemovedReason, RebalanceReason } from '../model/group.ts';
import type { GroupError, ProduceError } from '../model/tasks.ts';
import type { TopicPartition } from '../protocols/assignors/index.ts';

/** One member's share of a generation's assignment (RebalanceCompleted). */
export interface MemberAssignment {
  readonly memberId: string;
  readonly consumerId: string;
  readonly partitions: readonly TopicPartition[];
}

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
  /**
   * The group coordinator saved `offset` (the next offset to read) for this partition. `memberId`
   * and `generation` are what the commit carried (null / -1 for a consumer outside a group).
   */
  | {
      readonly type: 'OffsetCommitted';
      readonly groupId: string;
      readonly consumerId: string;
      readonly memberId: string | null;
      readonly generation: number;
      readonly topic: string;
      readonly partition: number;
      readonly offset: number;
    }
  /** The coordinator added a new member to the group (its first JoinGroup). */
  | {
      readonly type: 'MemberJoined';
      readonly groupId: string;
      readonly memberId: string;
      readonly consumerId: string;
    }
  /** The coordinator removed a member from the group. */
  | {
      readonly type: 'MemberRemoved';
      readonly groupId: string;
      readonly memberId: string;
      readonly consumerId: string;
      readonly reason: MemberRemovedReason;
    }
  /**
   * The group went to PreparingRebalance. `generation` is the current one; the rebalance ends
   * with generation + 1.
   */
  | {
      readonly type: 'RebalanceStarted';
      readonly groupId: string;
      readonly generation: number;
      readonly reason: RebalanceReason;
    }
  /**
   * The rebalance ended: the leader's assignment for `generation` is stored and the group is
   * Stable, or the group has no members left and is Empty (`protocol` null, no members).
   */
  | {
      readonly type: 'RebalanceCompleted';
      readonly groupId: string;
      readonly generation: number;
      readonly protocol: Assignor | null;
      readonly leader: string | null;
      readonly members: readonly MemberAssignment[];
    }
  /**
   * ConsumerRebalanceListener#onPartitionsAssigned: the member's new partitions in `generation`
   * (eager: its whole assignment; cooperative: only the added ones; may be empty).
   */
  | {
      readonly type: 'PartitionsAssigned';
      readonly consumerId: string;
      readonly groupId: string;
      readonly generation: number;
      readonly partitions: readonly TopicPartition[];
    }
  /** ConsumerRebalanceListener#onPartitionsRevoked: the member gives these partitions up. */
  | {
      readonly type: 'PartitionsRevoked';
      readonly consumerId: string;
      readonly groupId: string;
      readonly partitions: readonly TopicPartition[];
    }
  /**
   * ConsumerRebalanceListener#onPartitionsLost: the member learned it is no longer in the group
   * (session or poll timeout), so its partitions may already belong to others. Nothing is committed.
   */
  | {
      readonly type: 'PartitionsLost';
      readonly consumerId: string;
      readonly groupId: string;
      readonly partitions: readonly TopicPartition[];
    }
  /** A completed rebalance gave this member no partitions: it is idle until the next one. */
  | {
      readonly type: 'ConsumerIdle';
      readonly consumerId: string;
      readonly groupId: string;
      readonly generation: number;
    }
  /**
   * A commit failed: the coordinator refused it, or the client refused it locally
   * (`COMMIT_FAILED`: not part of an active group, CommitFailedException; `REBALANCE_IN_PROGRESS`:
   * the member is rejoining). Nothing was stored.
   */
  | {
      readonly type: 'CommitFailed';
      readonly consumerId: string;
      readonly groupId: string;
      readonly memberId: string;
      readonly generation: number;
      readonly error: GroupError | 'COMMIT_FAILED';
      readonly partitions: readonly TopicPartition[];
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

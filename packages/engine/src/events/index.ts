/**
 * Domain events: the single stream the engine emits (architecture §4.5, FR-2.16, NFR-8.4). The UI,
 * animations, tips, objectives and level runtime all consume it. A discriminated union on `type`.
 * The kernel stamps each event with `{ t, seq }` when it is emitted.
 */

import type { Acks, Assignor, Component } from '@kp/schema';
import type { Stamped } from '../core/kernel.ts';
import type { GroupStateName, MemberRemovedReason, RebalanceReason } from '../model/group.ts';
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
  /**
   * A send attempt failed with a retriable error and the producer will send the record again
   * (after `backoffMs`), until delivery.timeout.ms (M4-T7).
   */
  | {
      readonly type: 'ProduceRetried';
      readonly producerId: string;
      readonly topic: string;
      readonly partition: number;
      readonly traceId: string;
      readonly attempt: number;
      readonly error: ProduceError;
      readonly backoffMs: number;
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
      /**
       * When the coordinator accepted (validated and wrote) the commit. The event itself comes
       * when the commit is replicated to the HW of its __consumer_offsets partition (M4-T8).
       */
      readonly acceptedAt: number;
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
  /**
   * The consumer process ended: `crash` (KillConsumer: no commit, no LeaveGroup), `close` (a
   * graceful close(): commit and LeaveGroup) or `error` (poll() threw, e.g.
   * INCONSISTENT_GROUP_PROTOCOL, and the application died).
   */
  | {
      readonly type: 'ConsumerStopped';
      readonly consumerId: string;
      readonly reason: 'crash' | 'close' | 'error';
    }
  /**
   * Teaching aid (no Kafka equivalent, M3 Q9): poll() handed the application a record its group
   * had already processed (or is processing), e.g. after a crash replayed records past the last
   * commit (at-least-once).
   */
  | {
      readonly type: 'DuplicateProcessed';
      readonly groupId: string;
      readonly consumerId: string;
      readonly topic: string;
      readonly partition: number;
      readonly offset: number;
      readonly traceId: string;
    }
  /**
   * Teaching aid (M3 Q9): the group's next record for this partition jumped past records it never
   * processed (offsets `from`–`to`), e.g. after a commit before processing and a crash
   * (at-most-once), or a reset to "latest".
   */
  | {
      readonly type: 'RecordsSkipped';
      readonly groupId: string;
      readonly consumerId: string;
      readonly topic: string;
      readonly partition: number;
      readonly from: number;
      readonly to: number;
    }
  /**
   * A group's coordinator moved: the leader of its __consumer_offsets partition changed and the
   * new leader loaded the group from its replica (M4-T8). `state` and `generation` are what it
   * loaded; a Stable group stays Stable (no rebalance). While the partition has no leader nothing
   * is emitted: the group is unavailable until a leader loads it.
   */
  | {
      readonly type: 'CoordinatorMoved';
      readonly groupId: string;
      readonly from: string | null;
      readonly to: string;
      readonly state: GroupStateName;
      readonly generation: number;
    }
  /** The consumer process started again (RestartConsumer); it resumes from committed offsets. */
  | { readonly type: 'ConsumerStarted'; readonly consumerId: string }
  /**
   * A broker process started again (RestartBroker) with the log it had on disk. It registers with
   * the KRaft controller, which unfences it.
   */
  | { readonly type: 'BrokerStarted'; readonly brokerId: string }
  /**
   * A broker process stopped: `crash` (KillBroker: at once, requests in flight are lost) or
   * `shutdown` (StopBroker: a controlled shutdown, after leadership moved away).
   */
  | {
      readonly type: 'BrokerStopped';
      readonly brokerId: string;
      readonly reason: 'crash' | 'shutdown';
    }
  /**
   * A follower copied records from the leader: offsets `fromOffset` up to (not including)
   * `toOffset` are now in its log. One per follower fetch that returned records (M4 Q11; there is
   * no per-record replication event).
   */
  | {
      readonly type: 'ReplicaFetched';
      readonly broker: string;
      readonly leader: string;
      readonly topic: string;
      readonly partition: number;
      readonly fromOffset: number;
      readonly toOffset: number;
    }
  /**
   * The controller removed replicas from a partition's ISR (the leader asked: they fell behind
   * for replica.lag.time.max.ms). `isr` and `elr` are the results (M4-T4).
   */
  | {
      readonly type: 'IsrShrunk';
      readonly topic: string;
      readonly partition: number;
      readonly removed: readonly string[];
      readonly isr: readonly string[];
      readonly elr: readonly string[];
      readonly partitionEpoch: number;
    }
  /** The controller added caught-up replicas back to a partition's ISR (M4-T4). */
  | {
      readonly type: 'IsrExpanded';
      readonly topic: string;
      readonly partition: number;
      readonly added: readonly string[];
      readonly isr: readonly string[];
      readonly partitionEpoch: number;
    }
  /**
   * The controller fenced a broker: it heard no heartbeat for broker.session.timeout.ms
   * (`session-timeout`, after a crash), or the broker finished a controlled shutdown
   * (`controlled-shutdown`). A fenced broker can't lead or be in an ISR (M4-T5).
   */
  | {
      readonly type: 'BrokerFenced';
      readonly brokerId: string;
      readonly reason: 'session-timeout' | 'controlled-shutdown';
    }
  /** A registered broker heartbeated and the controller unfenced it (M4-T5). */
  | { readonly type: 'BrokerUnfenced'; readonly brokerId: string }
  /**
   * The controller elected a new leader for a partition (M4-T5). `initial`: a new partition;
   * `failover`: the old leader was fenced; `controlled-shutdown`: it was shutting down;
   * `preferred`: back to the preferred replica; `unclean`: an out-of-sync replica (data may be
   * lost) or KIP-966's last known leader after an unclean shutdown.
   */
  | {
      readonly type: 'LeaderElected';
      readonly topic: string;
      readonly partition: number;
      readonly leader: string;
      readonly previousLeader: string | null;
      readonly leaderEpoch: number;
      readonly reason: 'initial' | 'failover' | 'controlled-shutdown' | 'preferred' | 'unclean';
    }
  /** A partition lost its leader and no replica may lead: it is offline (M4-T5). */
  | {
      readonly type: 'PartitionOffline';
      readonly topic: string;
      readonly partition: number;
      readonly previousLeader: string;
      readonly isr: readonly string[];
      readonly elr: readonly string[];
    }
  /**
   * A follower deleted records the leader doesn't have (its log diverged after a leader change):
   * offsets `from` up to (not including) `to`. `acked` counts the deleted records whose producer
   * had been told they were delivered: data loss (M4-T6). Leaders never truncate.
   */
  | {
      readonly type: 'RecordsTruncated';
      readonly broker: string;
      readonly topic: string;
      readonly partition: number;
      readonly from: number;
      readonly to: number;
      readonly acked: number;
      /**
       * How many of them had been committed (below a leader's HW). Only an unclean election can
       * make this non-zero (M4-T9 invariant 3).
       */
      readonly committed: number;
      readonly traceIds: readonly string[];
    }
  /** UpdateConfig changed a component's config. Not emitted for a patch that changes nothing. */
  | {
      readonly type: 'ConfigUpdated';
      readonly id: string;
      readonly before: Component;
      readonly after: Component;
    };

export type EngineEvent = Stamped<EngineEventPayload>;

export type EngineEventType = EngineEvent['type'];

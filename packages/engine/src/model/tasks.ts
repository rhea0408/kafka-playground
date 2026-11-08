/**
 * Scheduled engine work, as plain data so pending work stays inspectable and serialisable
 * (architecture §4.7). `incarnation` ties a task to one life of a producer or consumer: if the
 * component was removed (and maybe re-added with the same id) the stale task is ignored.
 */

import type { Acks } from '@kp/schema';
import type { AssignorMember, TopicPartition } from '../protocols/assignors/index.ts';
import type { JoinReason, MemberMetadata, MemberRemovedReason } from './group.ts';
import type { LogRecord } from './partition.ts';

/**
 * One partition in a fetch request: read from `fetchOffset` (the consumer's position). Fetch v13+
 * names topics by topic ID (KIP-516): `topicId` is the topic life the position belongs to, and the
 * broker answers UNKNOWN_TOPIC_ID when the topic with that name now has another ID.
 */
export interface FetchPartition {
  readonly topic: string;
  readonly partition: number;
  readonly topicId: number;
  readonly fetchOffset: number;
}

/** A consumer's FetchRequest reaches the partition leader (Kafka protocol API key 1). */
export interface FetchRequestTask {
  readonly type: 'FetchRequest';
  readonly consumerId: string;
  readonly incarnation: number;
  readonly fetchId: number;
  readonly broker: string;
  readonly partitions: readonly FetchPartition[];
  /** fetch.min.bytes and fetch.max.wait.ms as they were when the request was sent. */
  readonly minBytes: number;
  readonly maxWaitMs: number;
}

/** Per-partition result in a fetch response; records only when there is no error. */
export type FetchPartitionResult = FetchPartition &
  (
    | { readonly error: null; readonly records: readonly LogRecord[] }
    | { readonly error: FetchError; readonly records: readonly [] }
  );

/** One partition in a follower's fetch: from its own LEO, with its last epoch (K9). */
export interface ReplicaFetchPartition {
  readonly topic: string;
  readonly partition: number;
  readonly topicId: number;
  /** The follower's LEO: the leader learns the follower's progress from it (K3). */
  readonly fetchOffset: number;
  /** The epoch of the follower's last record (-1 for an empty log); used to find divergence. */
  readonly lastFetchedEpoch: number;
  /** The leader epoch the follower knows; a mismatch gets FENCED_LEADER_EPOCH. */
  readonly currentLeaderEpoch: number;
}

/** A follower broker's fetch reaches the leader broker (replica fetcher, one per pair). */
export interface ReplicaFetchRequestTask {
  readonly type: 'ReplicaFetchRequest';
  readonly fetchId: number;
  readonly follower: string;
  readonly followerIncarnation: number;
  readonly leader: string;
  readonly partitions: readonly ReplicaFetchPartition[];
  /** replica.fetch.wait.max.ms of the follower. */
  readonly maxWaitMs: number;
}

/** Per-partition result of a follower fetch. */
export type ReplicaFetchResult = ReplicaFetchPartition &
  (
    | {
        readonly error: null;
        /** The leader's records from the fetch offset to its LEO. */
        readonly records: readonly LogRecord[];
        readonly highWatermark: number;
      }
    | {
        readonly error:
          'NOT_LEADER_OR_FOLLOWER' | 'FENCED_LEADER_EPOCH' | 'UNKNOWN_TOPIC_OR_PARTITION';
      }
  );

/**
 * An acks=all produce waiting at the leader (the DelayedProduce purgatory) until the HW passes
 * `offset`, or its timeout: who to answer, and where the record went.
 */
export interface ParkedProduce {
  readonly produceId: number;
  readonly producerId: string;
  readonly incarnation: number;
  readonly topic: string;
  readonly partition: number;
  readonly traceId: string;
  readonly offset: number;
}

export type EngineTask =
  /**
   * The consumer application calls poll(), or wakes up inside a blocked poll() for an
   * auto-commit. Only the task whose `token` matches the consumer's pollToken is current.
   */
  | {
      readonly type: 'ConsumerPoll';
      readonly consumerId: string;
      readonly incarnation: number;
      readonly token: number;
    }
  /** The producer creates its n-th scheduled record(s) of schedule `schedule`. */
  | {
      readonly type: 'ProducerTick';
      readonly producerId: string;
      readonly incarnation: number;
      readonly schedule: number;
      readonly n: number;
    }
  /** A produce request reaches the broker the producer believes leads the partition. */
  | {
      readonly type: 'LeaderAppend';
      readonly producerId: string;
      readonly incarnation: number;
      /** The broker the request was sent to. */
      readonly broker: string;
      readonly topic: string;
      readonly partition: number;
      readonly acks: Acks;
      /** The request's timeout (request.timeout.ms): how long an acks=all write may wait. */
      readonly timeoutMs: number;
      readonly record: Omit<LogRecord, 'offset' | 'leaderEpoch'>;
    }
  /** The leader's produce response reaches the producer (acks = 1 / all). */
  | {
      readonly type: 'ProduceResponse';
      readonly producerId: string;
      readonly incarnation: number;
      readonly topic: string;
      readonly partition: number;
      readonly traceId: string;
      readonly result:
        | { readonly ok: true; readonly offset: number }
        | { readonly ok: false; readonly error: ProduceError };
    }
  | FetchRequestTask
  | ReplicaFetchRequestTask
  /** replica.fetch.wait.max.ms passed for a parked follower fetch. */
  | { readonly type: 'ReplicaFetchExpire'; readonly fetchId: number }
  /** The leader's answer reaches the follower broker. */
  | {
      readonly type: 'ReplicaFetchResponse';
      readonly fetchId: number;
      readonly follower: string;
      readonly followerIncarnation: number;
      readonly leader: string;
      readonly partitions: readonly ReplicaFetchResult[];
    }
  /** A parked acks=all produce reached its request timeout. */
  | { readonly type: 'DelayedProduceExpire'; readonly produceId: number }
  /** fetch.max.wait.ms has passed for a parked fetch; answer with whatever is there. */
  | { readonly type: 'FetchExpire'; readonly fetchId: number }
  /** A consumer's request reaches the coordinator broker of its group. */
  | {
      readonly type: 'CoordinatorRequest';
      readonly groupId: string;
      readonly consumerId: string;
      readonly incarnation: number;
      readonly request: CoordinatorRequest;
    }
  /** The coordinator's response reaches the consumer. */
  | {
      readonly type: 'CoordinatorResponse';
      readonly groupId: string;
      readonly consumerId: string;
      readonly incarnation: number;
      readonly response: CoordinatorResponse;
    }
  /**
   * The join phase timer: group.initial.rebalance.delay.ms windows for a new group (`initial`),
   * or the rebalance timeout otherwise.
   */
  | {
      readonly type: 'GroupJoinTimer';
      readonly groupId: string;
      readonly token: number;
      readonly initial: { readonly delayMs: number; readonly remainingMs: number } | null;
    }
  /** The pending-sync timer: members that did not send SyncGroup in time are removed. */
  | { readonly type: 'GroupSyncTimer'; readonly groupId: string; readonly token: number }
  /** session.timeout.ms passed since this member's last heartbeat (if the token is current). */
  | {
      readonly type: 'MemberSessionExpire';
      readonly groupId: string;
      readonly memberId: string;
      readonly token: number;
    }
  /** The consumer's heartbeat thread wakes up to send a Heartbeat (every heartbeat.interval.ms). */
  | {
      readonly type: 'ConsumerHeartbeat';
      readonly consumerId: string;
      readonly incarnation: number;
      readonly token: number;
    }
  /** max.poll.interval.ms passed since the application last called poll() (heartbeat thread). */
  | {
      readonly type: 'ConsumerPollTimeout';
      readonly consumerId: string;
      readonly incarnation: number;
      readonly token: number;
    }
  /**
   * rebalance.timeout.ms passed while onJoinPrepare waited for its auto-commit (joinPrepareTimer):
   * the member stops waiting and joins anyway.
   */
  | {
      readonly type: 'ConsumerJoinPrepareTimeout';
      readonly consumerId: string;
      readonly incarnation: number;
      readonly token: number;
    }
  /** The broker's fetch response reaches the consumer. */
  | {
      readonly type: 'FetchResponse';
      readonly consumerId: string;
      readonly incarnation: number;
      readonly fetchId: number;
      readonly broker: string;
      readonly partitions: readonly FetchPartitionResult[];
    };

/** Group coordinator errors (Kafka protocol error codes) a client can get back. */
export type GroupError =
  | 'UNKNOWN_MEMBER_ID'
  | 'ILLEGAL_GENERATION'
  | 'REBALANCE_IN_PROGRESS'
  | 'INCONSISTENT_GROUP_PROTOCOL'
  | 'COORDINATOR_NOT_AVAILABLE';

/** An offset a client asks to commit; `topicId` is the topic life its position belongs to. */
export interface CommitOffset {
  readonly topic: string;
  readonly partition: number;
  readonly offset: number;
  readonly topicId: number;
}

/** Requests from a consumer to its group coordinator (classic protocol). */
export type CoordinatorRequest =
  | {
      readonly api: 'JoinGroup';
      /** '' for a member joining for the first time. */
      readonly memberId: string;
      readonly metadata: MemberMetadata;
      readonly reason: JoinReason;
    }
  | {
      readonly api: 'SyncGroup';
      readonly memberId: string;
      readonly generation: number;
      /** Only from the leader: every member's partitions. */
      readonly assignment: readonly (readonly [string, readonly TopicPartition[]])[] | null;
    }
  | { readonly api: 'Heartbeat'; readonly memberId: string; readonly generation: number }
  | {
      readonly api: 'LeaveGroup';
      readonly memberId: string;
      readonly reason: Extract<MemberRemovedReason, 'leave' | 'poll-timeout'>;
    }
  | {
      readonly api: 'OffsetCommit';
      readonly commitId: number;
      readonly memberId: string;
      readonly generation: number;
      readonly offsets: readonly CommitOffset[];
    };

/** The coordinator's answers (LeaveGroup gets none here: the client does not wait for it). */
export type CoordinatorResponse =
  | {
      readonly api: 'JoinGroup';
      readonly error: null;
      readonly memberId: string;
      readonly generation: number;
      readonly protocol: string;
      readonly leader: string;
      /** Every member's metadata, only in the leader's response (empty for followers). */
      readonly members: readonly AssignorMember[];
    }
  | { readonly api: 'JoinGroup'; readonly error: GroupError }
  | {
      readonly api: 'SyncGroup';
      readonly error: null;
      readonly generation: number;
      readonly assignment: readonly TopicPartition[];
    }
  | { readonly api: 'SyncGroup'; readonly error: GroupError }
  | { readonly api: 'Heartbeat'; readonly error: GroupError | null }
  | {
      readonly api: 'OffsetCommit';
      readonly commitId: number;
      /** A whole-request error; per-partition UNKNOWN_TOPIC_OR_PARTITION is silent (not stored). */
      readonly error: GroupError | null;
    };

/** Kafka protocol error names, as the client reports them. */
export type ProduceError =
  | 'UNKNOWN_TOPIC_OR_PARTITION'
  /** The broker does not lead the partition (any more). */
  | 'NOT_LEADER_OR_FOLLOWER'
  /** acks=all and the ISR is below min.insync.replicas before the append (K6). */
  | 'NOT_ENOUGH_REPLICAS'
  /** The record reached the HW, but the ISR had shrunk below min.insync.replicas (K6). */
  | 'NOT_ENOUGH_REPLICAS_AFTER_APPEND'
  /** The acks=all write waited longer than the request's timeout (DelayedProduce). */
  | 'REQUEST_TIMED_OUT';

/** Per-partition fetch errors (Kafka protocol error codes). */
export type FetchError = 'UNKNOWN_TOPIC_OR_PARTITION' | 'UNKNOWN_TOPIC_ID' | 'OFFSET_OUT_OF_RANGE';

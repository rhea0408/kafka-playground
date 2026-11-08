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
  /** A produce request reaches the partition leader. */
  | {
      readonly type: 'LeaderAppend';
      readonly producerId: string;
      readonly incarnation: number;
      readonly topic: string;
      readonly partition: number;
      readonly acks: Acks;
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
export type ProduceError = 'UNKNOWN_TOPIC_OR_PARTITION';

/** Per-partition fetch errors (Kafka protocol error codes). */
export type FetchError = 'UNKNOWN_TOPIC_OR_PARTITION' | 'UNKNOWN_TOPIC_ID' | 'OFFSET_OUT_OF_RANGE';

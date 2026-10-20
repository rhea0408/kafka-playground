/**
 * Scheduled engine work, as plain data so pending work stays inspectable and serialisable
 * (architecture §4.7). `incarnation` ties a task to one life of a producer or consumer: if the
 * component was removed (and maybe re-added with the same id) the stale task is ignored.
 */

import type { Acks } from '@kp/schema';
import type { LogRecord } from './partition.ts';

/** One partition in a fetch request: read from `fetchOffset` (the consumer's position). */
export interface FetchPartition {
  readonly topic: string;
  readonly partition: number;
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

/**
 * Per-partition result in a fetch response. `topicId` is the ID of the topic the broker read
 * from (Fetch v13+ responses carry topic IDs, KIP-516); null when the topic does not exist.
 */
export type FetchPartitionResult = FetchPartition & { readonly topicId: number | null } & (
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
      readonly record: Omit<LogRecord, 'offset'>;
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
  /** The broker's fetch response reaches the consumer. */
  | {
      readonly type: 'FetchResponse';
      readonly consumerId: string;
      readonly incarnation: number;
      readonly fetchId: number;
      readonly broker: string;
      readonly partitions: readonly FetchPartitionResult[];
    };

/** Kafka protocol error names, as the client reports them. */
export type ProduceError = 'UNKNOWN_TOPIC_OR_PARTITION';

/** Per-partition fetch errors (Kafka protocol error codes). */
export type FetchError = 'UNKNOWN_TOPIC_OR_PARTITION' | 'OFFSET_OUT_OF_RANGE';

/**
 * Scheduled engine work, as plain data so pending work stays inspectable and serialisable
 * (architecture §4.7). `incarnation` ties a task to one life of a producer or consumer: if the
 * component was removed (and maybe re-added with the same id) the stale task is ignored.
 */

import type { Acks } from '@kp/schema';
import type { LogRecord } from './partition.ts';

export type EngineTask =
  /** The producer creates its n-th scheduled record(s). */
  | {
      readonly type: 'ProducerTick';
      readonly producerId: string;
      readonly incarnation: number;
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
    };

/** Kafka protocol error names, as the client reports them. */
export type ProduceError = 'UNKNOWN_TOPIC_OR_PARTITION';

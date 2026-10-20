/**
 * Mutable engine state. Only command handlers and protocol tasks inside the engine touch it; the
 * UI sees it through snapshots (CLAUDE.md rule 3). Maps keep insertion order, so iteration is
 * deterministic for a given command log.
 */

import type { Broker, Connection, Consumer, Producer, Topic } from '@kp/schema';
import type { Rng } from '../core/rng.ts';
import type { StickyState } from '../protocols/partitioner.ts';
import type { PartitionState } from './partition.ts';

export interface BrokerState {
  readonly config: Broker;
}

export interface TopicState {
  readonly config: Topic;
  readonly partitions: readonly PartitionState[];
}

export interface ProducerState {
  readonly config: Producer;
  /** Distinguishes this life of the producer from earlier ones with the same id. */
  readonly incarnation: number;
  /** Sim time the producer started; record n is due at startedAt + floor(n * 1000 / rate). */
  readonly startedAt: number;
  /** Seeded streams: one for key choice, one for the sticky partitioner. */
  readonly keyRng: Rng;
  readonly partitionRng: Rng;
  /** Sticky partition per topic, for records without a key. */
  readonly sticky: Map<string, StickyState>;
  stats: ProducerStats;
}

export interface ProducerStats {
  /** Records handed to the producer (RecordProduced). */
  sent: number;
  /** Records the producer considers delivered (ProduceAcked). */
  acked: number;
  /** Records the producer was told failed (ProduceFailed). */
  failed: number;
}

export interface ConsumerState {
  readonly config: Consumer;
}

export interface EngineState {
  readonly brokers: Map<string, BrokerState>;
  readonly topics: Map<string, TopicState>;
  readonly producers: Map<string, ProducerState>;
  readonly consumers: Map<string, ConsumerState>;
  /** In the order they were made. */
  readonly connections: Connection[];
  /** Source of incarnation numbers for producers and consumers. */
  readonly counters: { incarnation: number };
}

export function createState(): EngineState {
  return {
    brokers: new Map(),
    topics: new Map(),
    producers: new Map(),
    consumers: new Map(),
    connections: [],
    counters: { incarnation: 0 },
  };
}

/** Topics a producer writes to (producer → topic connections), in connection order. */
export function topicsOfProducer(state: EngineState, producerId: string): TopicState[] {
  return state.connections
    .filter((c) => c.from === producerId)
    .map((c) => state.topics.get(c.to))
    .filter((t): t is TopicState => t !== undefined);
}

/** Topics a consumer reads (topic → consumer connections), in connection order. */
export function topicsOfConsumer(state: EngineState, consumerId: string): TopicState[] {
  return state.connections
    .filter((c) => c.to === consumerId)
    .map((c) => state.topics.get(c.from))
    .filter((t): t is TopicState => t !== undefined);
}

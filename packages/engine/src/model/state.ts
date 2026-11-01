/**
 * Mutable engine state. Only command handlers and protocol tasks inside the engine touch it; the
 * UI sees it through snapshots (CLAUDE.md rule 3). Maps keep insertion order, so iteration is
 * deterministic for a given command log.
 */

import type { Broker, Connection, Consumer, Producer, Topic } from '@kp/schema';
import type { Rng } from '../core/rng.ts';
import type { StickyState } from '../protocols/partitioner.ts';
import type { ClientMetadata } from '../protocols/metadata.ts';
import type { LogRecord, PartitionState } from './partition.ts';
import type { Assignor } from '@kp/schema';
import type { TopicPartition } from '../protocols/assignors/index.ts';
import type { GroupState, JoinReason } from './group.ts';
import type { FetchRequestTask } from './tasks.ts';

export type { GroupState } from './group.ts';

export interface BrokerState {
  readonly config: Broker;
}

export interface TopicState {
  /**
   * Identifies this life of the topic. Kafka gives every topic a topic ID when it is created
   * (KIP-516), so a topic deleted and re-created with the same name is a different topic with a
   * new ID. Ours is a sequence number rather than a random UUID (see simplifications).
   */
  readonly topicId: number;
  /** Replaced by UpdateConfig. */
  config: Topic;
  /** Grows when partitions are added (UpdateConfig); never shrinks. */
  readonly partitions: PartitionState[];
}

export interface ProducerState {
  /** Replaced by UpdateConfig. */
  config: Producer;
  /**
   * Distinguishes this life of the producer (AddComponent → RemoveComponent) from earlier ones
   * with the same id. Part of every trace id; produce responses for another life are dropped.
   */
  readonly incarnation: number;
  /**
   * The current send schedule. A rate change (UpdateConfig) starts a new schedule from now;
   * ticks of an older schedule are ignored. In-flight requests are not affected.
   */
  schedule: number;
  /** Sim time the schedule started; record n is due at startedAt + floor(n * 1000 / rate). */
  startedAt: number;
  /** Seeded streams: one for key choice, one for the sticky partitioner. */
  readonly keyRng: Rng;
  readonly partitionRng: Rng;
  /** Sticky partition per topic, for records without a key. */
  readonly sticky: Map<string, StickyState>;
  /** The producer's cached cluster metadata (partition counts), refreshed every metadata.max.age.ms. */
  readonly metadata: ClientMetadata;
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

/** Records from one partition that a fetch returned and poll() has not handed out yet. */
export interface BufferedFetch {
  /** "topic-partition". */
  readonly key: string;
  readonly topic: string;
  readonly partition: number;
  /** The topic ID (KIP-516) of the topic life these records were read from. */
  readonly topicId: number;
  /** Not yet returned, oldest first. poll() takes from the front. */
  readonly records: LogRecord[];
}

/**
 * A consumer's position in one partition, and the topic ID (KIP-516) of the topic life it belongs
 * to, set when the position is initialised or reset. Fetch requests carry it, and the broker
 * answers UNKNOWN_TOPIC_ID when the topic was re-created since; a commit only lands on that same
 * topic life (see commitPositions in consume.ts).
 */
export interface ConsumerPosition {
  readonly offset: number;
  readonly topicId: number;
}

/**
 * The consumer's side of its group membership (AbstractCoordinator / ConsumerCoordinator state).
 * See protocols/consumer-coordinator.ts.
 */
export interface ClientGroupState {
  /** The coordinator's member id for this client; '' until the first JoinGroup is answered. */
  memberId: string;
  /** Generation of the current assignment; -1 (NO_GENERATION) before joining or after a reset. */
  generation: number;
  /** AbstractCoordinator.MemberState (`preparing` = JoinGroup sent, `completing` = SyncGroup sent). */
  state: 'unjoined' | 'preparing' | 'completing' | 'stable';
  /** Set by a heartbeat, a metadata or subscription change: rejoin at the next poll(). */
  rejoinNeeded: boolean;
  rejoinReason: JoinReason;
  /** Partitions this member owns now (its assignment), sorted. */
  owned: TopicPartition[];
  /** The subscription sent with the last JoinGroup; a different one means rejoining. */
  joinedSubscription: readonly string[] | null;
  /** The assignor the group chose; and whether this member is the leader. */
  protocol: Assignor | null;
  isLeader: boolean;
  /** Leader only: the partition counts its last assignment was based on (assignmentSnapshot). */
  leaderCounts: Map<string, number> | null;
  /** A SyncGroup answer waiting for the application's next poll() to take effect. */
  pendingAssignment: { readonly generation: number; readonly partitions: TopicPartition[] } | null;
  /** onJoinPrepare: JoinGroup waits until this commit is answered. */
  prepareCommitId: number | null;
  /** Owned partitions and generation sent in JoinGroup (eager members have revoked theirs). */
  lastOwned: TopicPartition[];
  lastOwnedGeneration: number;
  /** Only the heartbeat / poll-timeout task carrying the current token runs. */
  heartbeatToken: number;
  pollTimeoutToken: number;
}

export function createClientGroupState(): ClientGroupState {
  return {
    memberId: '',
    generation: -1,
    state: 'unjoined',
    rejoinNeeded: false,
    rejoinReason: 'new-member',
    owned: [],
    joinedSubscription: null,
    protocol: null,
    isLeader: false,
    leaderCounts: null,
    pendingAssignment: null,
    prepareCommitId: null,
    lastOwned: [],
    lastOwnedGeneration: -1,
    heartbeatToken: 0,
    pollTimeoutToken: 0,
  };
}

export interface ConsumerState {
  /** Replaced by UpdateConfig; read at each poll(). */
  config: Consumer;
  /** Changes on every (re)start, so poll tasks from an earlier life are ignored. */
  incarnation: number;
  /** "stopped" after KillConsumer, until RestartConsumer. */
  status: 'running' | 'stopped';
  /** Next offset to fetch, per assigned partition ("topic-partition" → position). */
  readonly positions: Map<string, ConsumerPosition>;
  /** Sim time of the last auto-commit (or of the start); the interval counts from here. */
  lastAutoCommitAt: number;
  /** Rotates which assigned partition comes first in the next fetch request. */
  nextPartitionIndex: number;
  /**
   * The client's fetch buffer (completed fetches), oldest first. poll() returns these before it
   * waits for anything new; leftovers beyond max.poll.records stay for the next poll().
   */
  readonly buffer: BufferedFetch[];
  /** Brokers this consumer has a fetch request in flight to (at most one per broker). */
  readonly fetchesInFlight: Set<string>;
  /**
   * True while the application is blocked inside poll() waiting for data; false while it is
   * processing the records the last poll() returned.
   */
  inPoll: boolean;
  /** Only the ConsumerPoll task carrying this token is current; older ones are ignored. */
  pollToken: number;
  /** The consumer's cached cluster metadata (partition counts per topic). */
  readonly metadata: ClientMetadata;
  /** Group membership as this client sees it; replaced on every (re)start. */
  group: ClientGroupState;
  stats: { consumed: number };
}

export interface EngineState {
  readonly brokers: Map<string, BrokerState>;
  readonly topics: Map<string, TopicState>;
  readonly producers: Map<string, ProducerState>;
  readonly consumers: Map<string, ConsumerState>;
  /**
   * Consumer groups as their group coordinator holds them, with their committed offsets. They
   * survive their consumers: a restarted or re-added consumer resumes from here.
   */
  readonly groups: Map<string, GroupState>;
  /**
   * Leader broker of each __consumer_offsets partition (offsets.topic.num.partitions = 50), or
   * null until the first FindCoordinator creates the internal topic (protocols/coordinator.ts).
   */
  offsetsTopicLeaders: readonly string[] | null;
  /** In the order they were made. */
  readonly connections: Connection[];
  /**
   * Fetch requests the broker is holding (the "fetch purgatory") until fetch.min.bytes are
   * available or fetch.max.wait.ms passes, by fetch id, oldest first.
   */
  readonly parkedFetches: Map<number, FetchRequestTask>;
  /** Sources of incarnation numbers (producers and consumers), request ids and topic IDs. */
  readonly counters: { incarnation: number; fetch: number; topic: number; commit: number };
}

export function createState(): EngineState {
  return {
    brokers: new Map(),
    topics: new Map(),
    producers: new Map(),
    consumers: new Map(),
    groups: new Map(),
    offsetsTopicLeaders: null,
    connections: [],
    parkedFetches: new Map(),
    counters: { incarnation: 0, fetch: 0, topic: 0, commit: 0 },
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

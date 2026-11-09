/**
 * The default Sandbox scene (M2-T12): producer → topic `orders` (3 partitions) → consumer, on the
 * one broker. The Sandbox opens with it, paused; `?scene=empty` opens the bare canvas (broker
 * only) instead, which the e2e tests that build a flow by hand use.
 *
 * Keys: the producer picks D, E or F at random (seeded). With 3 partitions the Java client's
 * default partitioner sends them to partitions 0, 1 and 2 (`murmur2(key) % 3`, Kafka
 * DefaultPartitioner / BuiltInPartitioner#partitionForKey), so every partition gets traffic and
 * each key stays on its own partition. A, B and C would all land on partition 1 (see the unit
 * test and M2-decisions T12).
 */

import { parseComponent, type Component } from '@kp/schema';
import {
  INITIAL_BROKER_ID,
  INITIAL_TOPOLOGY_STATE,
  type TopologyState,
  type XYPosition,
} from './topology-store.ts';

export const DEFAULT_TOPIC = 'orders';
export const DEFAULT_PRODUCER = 'p1';
export const DEFAULT_CONSUMER = 'c1';
export const DEFAULT_GROUP = 'orders-app';
export const DEFAULT_PARTITIONS = 3;
/** One key per partition with 3 partitions: D → 0, E → 1, F → 2 (checked in the unit test). */
export const DEFAULT_KEYS: readonly string[] = ['D', 'E', 'F'];
export const DEFAULT_RATE_PER_SEC = 2;

/** Raw inputs; the schema fills in every other field with its (Kafka) default. */
const INPUTS: readonly Record<string, unknown>[] = [
  { kind: 'broker', id: INITIAL_BROKER_ID },
  { kind: 'topic', id: DEFAULT_TOPIC, partitions: DEFAULT_PARTITIONS },
  {
    kind: 'producer',
    id: DEFAULT_PRODUCER,
    ratePerSec: DEFAULT_RATE_PER_SEC,
    keyStrategy: { type: 'random', keys: [...DEFAULT_KEYS] },
  },
  { kind: 'consumer', id: DEFAULT_CONSUMER, groupId: DEFAULT_GROUP },
];

/**
 * Positions (flow px, on the 16 px grid) after wireframe 05: producer, topic and consumer in a
 * row with ~74 px edges, the broker under the topic. Everything fits the smallest canvas
 * (1366 × 768 → 766 × 512) at zoom 1, so the minimap stays hidden.
 */
const POSITIONS: Readonly<Record<string, XYPosition>> = {
  [DEFAULT_PRODUCER]: { x: 16, y: 112 },
  [DEFAULT_TOPIC]: { x: 240, y: 64 },
  [DEFAULT_CONSUMER]: { x: 512, y: 112 },
  [INITIAL_BROKER_ID]: { x: 240, y: 288 },
};

/** The default scene as topology-store state (parsed configs, as the actions module stores them). */
export function defaultSceneState(): TopologyState {
  const components = INPUTS.map((input): Component => {
    const parsed = parseComponent(input);
    if (!parsed.ok) throw new Error(`Default scene is invalid: ${parsed.errors.join(' ')}`);
    return parsed.value;
  });
  return {
    components,
    connections: [
      { from: DEFAULT_PRODUCER, to: DEFAULT_TOPIC },
      { from: DEFAULT_TOPIC, to: DEFAULT_CONSUMER },
    ],
    positions: POSITIONS,
  };
}

// ── M3 scene presets (M3-T12) ───────────────────────────────────────────────────────────────
// The same producer → orders (3 partitions, keys D/E/F) flow, with a consumer group set up for
// one lesson each. Members of a group share their config, as instances of one application do.

function parsed(inputs: readonly Record<string, unknown>[]): Component[] {
  return inputs.map((input): Component => {
    const result = parseComponent(input);
    if (!result.ok) throw new Error(`Scene is invalid: ${result.errors.join(' ')}`);
    return result.value;
  });
}

function groupScene(
  members: readonly Record<string, unknown>[],
  producer: Record<string, unknown> = {},
): TopologyState {
  const ids = members.map((m) => String(m.id));
  return {
    components: parsed([
      { kind: 'broker', id: INITIAL_BROKER_ID },
      { kind: 'topic', id: DEFAULT_TOPIC, partitions: DEFAULT_PARTITIONS },
      {
        kind: 'producer',
        id: DEFAULT_PRODUCER,
        ratePerSec: DEFAULT_RATE_PER_SEC,
        keyStrategy: { type: 'random', keys: [...DEFAULT_KEYS] },
        ...producer,
      },
      ...members.map((m) => ({ kind: 'consumer', groupId: DEFAULT_GROUP, ...m })),
    ]),
    connections: [
      { from: DEFAULT_PRODUCER, to: DEFAULT_TOPIC },
      ...ids.map((id) => ({ from: DEFAULT_TOPIC, to: id })),
    ],
    positions: { ...POSITIONS, [`group:${DEFAULT_GROUP}`]: { x: 512, y: 112 } },
  };
}

export type SceneId =
  | 'default'
  | 'idle'
  | 'crash'
  | 'slow'
  | 'failover'
  | 'acks1-loss'
  | 'not-enough-replicas'
  | 'coordinator-failover'
  | 'empty';

// ── M4 scene presets (M4-T12) ───────────────────────────────────────────────────────────────
// Three brokers, orders with 3 partitions and RF 3, keys D/E/F as before.

const BROKERS = ['b1', 'b2', 'b3'] as const;

function clusterScene(options: {
  readonly topic?: Record<string, unknown>;
  readonly producer?: Record<string, unknown>;
  readonly members?: number;
  readonly networkDelayMs?: number;
}): TopologyState {
  const members = Array.from({ length: options.members ?? 1 }, (_, i) => `c${String(i + 1)}`);
  return {
    components: parsed([
      ...BROKERS.map((id) => ({ kind: 'broker', id })),
      {
        kind: 'topic',
        id: DEFAULT_TOPIC,
        partitions: DEFAULT_PARTITIONS,
        replicationFactor: 3,
        minInsyncReplicas: 2,
        ...options.topic,
      },
      {
        kind: 'producer',
        id: DEFAULT_PRODUCER,
        ratePerSec: 5,
        keyStrategy: { type: 'random', keys: [...DEFAULT_KEYS] },
        ...options.producer,
      },
      ...members.map((id) => ({
        kind: 'consumer',
        id,
        groupId: DEFAULT_GROUP,
        autoOffsetReset: 'earliest',
      })),
    ]),
    connections: [
      { from: DEFAULT_PRODUCER, to: DEFAULT_TOPIC },
      ...members.map((id) => ({ from: DEFAULT_TOPIC, to: id })),
    ],
    positions: {
      [DEFAULT_PRODUCER]: { x: 16, y: 112 },
      [DEFAULT_TOPIC]: { x: 240, y: 64 },
      [`group:${DEFAULT_GROUP}`]: { x: 512, y: 112 },
      b1: { x: 16, y: 320 },
      b2: { x: 256, y: 320 },
      b3: { x: 496, y: 320 },
    },
    ...(options.networkDelayMs === undefined ? {} : { networkDelayMs: options.networkDelayMs }),
  };
}

export interface Scene {
  readonly id: SceneId;
  readonly label: string;
  /** One sentence: what to do and what to look for. */
  readonly hint: string;
  readonly state: () => TopologyState;
}

/** The scenes the Sandbox offers (top bar "Scene", or `?scene=<id>`). */
export const SCENES: readonly Scene[] = [
  {
    id: 'default',
    label: 'Default',
    hint: 'One producer, one topic, one consumer.',
    state: defaultSceneState,
  },
  {
    id: 'idle',
    label: 'Idle consumer',
    hint: 'Four members share three partitions: one member gets none and stays idle.',
    state: () => groupScene(['c1', 'c2', 'c3', 'c4'].map((id) => ({ id }))),
  },
  {
    id: 'crash',
    label: 'Crash and duplicates',
    hint: 'Auto-commit every 5 s. Kill a member while it processes: after session.timeout.ms its partitions move and the new owner reads some records again (DuplicateProcessed).',
    state: () =>
      groupScene(
        // earliest: a partition whose owner crashed before its first commit is read from the
        // start (duplicates) instead of from the end (skips).
        ['c1', 'c2'].map((id) => ({
          id,
          autoOffsetReset: 'earliest',
          processingTimeMs: 100,
          maxPollRecords: 10,
        })),
        { ratePerSec: 5 },
      ),
  },
  {
    id: 'slow',
    label: 'Slow consumer',
    hint: 'c2 needs 20 × 2 s = 40 s per poll(), more than max.poll.interval.ms (30 s): it is removed, its partitions move, and it keeps processing as a zombie.',
    state: () =>
      groupScene(
        [
          { id: 'c1', autoOffsetReset: 'earliest' },
          { id: 'c2', autoOffsetReset: 'earliest', processingTimeMs: 2000, maxPollRecords: 20 },
        ],
        { ratePerSec: 5 },
      ),
  },
  {
    id: 'failover',
    label: 'Leader failover',
    hint: 'RF 3, min.insync.replicas 2, acks=all. Kill the broker that leads P0 (hover it): about 9 s later the controller fences it and an in-sync follower takes over. No acknowledged record is lost; restart the old leader and it rejoins the ISR.',
    state: () => clusterScene({}),
  },
  {
    id: 'acks1-loss',
    label: 'acks=1 data loss',
    hint: 'acks=1, every record keyed D (so all go to P0), 50 msg/s, on a cross-zone network (20 ms between machines): the leader acknowledges before its followers have copied. Kill the leader of P0 while records flow, then restart it once it is fenced: it truncates records that were acknowledged (see "⚠ lost" and the partition log).',
    state: () =>
      clusterScene({
        topic: { minInsyncReplicas: 1 },
        producer: { acks: '1', ratePerSec: 50, keyStrategy: { type: 'fixed', key: 'D' } },
        networkDelayMs: 20,
      }),
  },
  {
    id: 'not-enough-replicas',
    label: 'Not enough replicas',
    hint: 'RF 3, min.insync.replicas 2, acks=all. Kill two brokers that follow P0: once they leave the ISR, writes are refused with NOT_ENOUGH_REPLICAS and the producer retries (shown under it). Restart one and writes go through again.',
    state: () => clusterScene({}),
  },
  {
    id: 'coordinator-failover',
    label: 'Coordinator failover',
    hint: 'Two members in orders-app. Kill the broker marked "Coordinator: orders-app": its __consumer_offsets partition moves, the new leader loads the group, and the group stays Stable with its committed offsets (no rebalance).',
    state: () => clusterScene({ members: 2 }),
  },
  {
    id: 'empty',
    label: 'Empty (broker only)',
    hint: 'Build your own flow from the palette.',
    state: () => INITIAL_TOPOLOGY_STATE,
  },
];

export function sceneById(id: string | null): Scene | undefined {
  return SCENES.find((scene) => scene.id === id);
}

/** The canvas the Sandbox opens with: `?scene=<id>` (see SCENES) or the default. */
export function initialSceneFor(search: string): TopologyState {
  return (
    (sceneById(new URLSearchParams(search).get('scene')) ?? SCENES[0])?.state() ??
    defaultSceneState()
  );
}

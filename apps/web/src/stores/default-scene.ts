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

export type SceneId = 'default' | 'idle' | 'crash' | 'slow' | 'empty';

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
    id: 'empty',
    label: 'Empty (broker only)',
    hint: 'Build your own flow from the palette.',
    state: () => INITIAL_TOPOLOGY_STATE,
  },
];

export function sceneById(id: string | null): Scene | undefined {
  return SCENES.find((scene) => scene.id === id);
}

/** The canvas the Sandbox opens with: `?scene=<id>` (idle, crash, slow, empty) or the default. */
export function initialSceneFor(search: string): TopologyState {
  return (
    (sceneById(new URLSearchParams(search).get('scene')) ?? SCENES[0])?.state() ??
    defaultSceneState()
  );
}

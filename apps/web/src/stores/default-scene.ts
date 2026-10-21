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
  [DEFAULT_CONSUMER]: { x: 544, y: 112 },
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

/** The canvas the Sandbox opens with: the default scene, or the bare canvas for `?scene=empty`. */
export function initialSceneFor(search: string): TopologyState {
  return new URLSearchParams(search).get('scene') === 'empty'
    ? INITIAL_TOPOLOGY_STATE
    : defaultSceneState();
}

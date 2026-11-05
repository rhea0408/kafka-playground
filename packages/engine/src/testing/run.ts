/** Runs a generated scenario and returns everything the property tests inspect. Test-only. */

import type { TopologyInput } from '@kp/schema';
import { createEngine, DEFAULT_NETWORK_DELAY_MS, type Engine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';
import type { Step } from './arbitraries.ts';

export interface Run {
  readonly engine: Engine;
  readonly events: EngineEvent[];
  readonly endTime: number;
  /** The run's one-way network delay, for checks that reason about send vs arrival time. */
  readonly networkDelayMs: number;
}

export function runScenario(
  seed: number,
  topology: TopologyInput,
  steps: readonly Step[],
  options: { readonly networkDelayMs?: number } = {},
): Run {
  const networkDelayMs = options.networkDelayMs ?? DEFAULT_NETWORK_DELAY_MS;
  const engine = createEngine({ seed, topology, networkDelayMs });
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  for (const step of steps) {
    if (step.kind === 'advance') engine.runUntil(engine.now() + step.ms);
    else engine.dispatch(step.command);
  }
  return { engine, events, endTime: engine.now(), networkDelayMs };
}

export { hashEvents } from '../events/hash.ts';

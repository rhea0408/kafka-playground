/** Runs a generated scenario and returns everything the property tests inspect. Test-only. */

import type { TopologyInput } from '@kp/schema';
import { createEngine, type Engine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';
import type { Step } from './arbitraries.ts';

export interface Run {
  readonly engine: Engine;
  readonly events: EngineEvent[];
  readonly endTime: number;
}

export function runScenario(seed: number, topology: TopologyInput, steps: readonly Step[]): Run {
  const engine = createEngine({ seed, topology });
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  for (const step of steps) {
    if (step.kind === 'advance') engine.runUntil(engine.now() + step.ms);
    else engine.dispatch(step.command);
  }
  return { engine, events, endTime: engine.now() };
}

export { hashEvents } from '../events/hash.ts';

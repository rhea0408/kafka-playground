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

/** 32-bit FNV-1a over the JSON event log, as a compact fingerprint for determinism checks. */
export function hashEvents(events: readonly EngineEvent[]): string {
  const text = JSON.stringify(events);
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) h = Math.imul(h ^ text.charCodeAt(i), 0x01000193);
  return (h >>> 0).toString(16).padStart(8, '0');
}

/**
 * What command handlers and protocol tasks get to work with: the kernel (time, RNG, scheduling),
 * the mutable engine state, and `emit`, which is silent while the initial topology loads.
 */

import type { Kernel } from '../core/kernel.ts';
import type { EngineEventPayload } from '../events/index.ts';
import type { EngineState } from './state.ts';
import type { EngineTask } from './tasks.ts';

export interface EngineContext {
  readonly kernel: Kernel<EngineTask, EngineEventPayload>;
  readonly state: EngineState;
  readonly emit: (event: EngineEventPayload) => void;
  readonly config: EngineConfig;
}

export interface EngineConfig {
  /** Fixed one-way network latency between clients and brokers, in sim ms. */
  readonly networkDelayMs: number;
}

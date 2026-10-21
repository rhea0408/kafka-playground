/**
 * `window.__kp.sim`: a read-only view of the sim worker for the e2e determinism test (M2-T12) and
 * for poking at the sim from the browser console. Set in every build, because Playwright runs
 * against the production bundle and the point of the test is to check that bundle. Every method
 * only reads: nothing here can change the sim, so it can't bypass the command path (CLAUDE.md
 * rule 3).
 */

import type { CommandLogEntry, EngineEvent } from '@kp/engine';
import type { HostStatus, SimApi } from './protocol.ts';

type Promisable<T> = T | Promise<T>;

export interface SimDebugHook {
  /** The seed the Sandbox engine was started with. */
  readonly seed: number;
  readonly status: () => Promisable<HostStatus>;
  /** Accepted commands since the last init / Reset. */
  readonly commandLog: () => Promisable<readonly CommandLogEntry[]>;
  /** FNV-1a hash of every event since the last init / Reset (replays the command log). */
  readonly eventLogHash: () => Promisable<string>;
  /** The worker's event ring buffer (the newest EVENT_HISTORY_SIZE events), oldest first. */
  readonly eventHistory: () => Promisable<readonly EngineEvent[]>;
}

declare global {
  interface KpDebugHooks {
    sim?: SimDebugHook;
  }
}

/** The read-only methods of the worker API (sync on a `SimHost`, async over Comlink). */
export type SimReadPort = {
  readonly [K in 'status' | 'commandLog' | 'eventLogHash' | 'eventHistory']: () => Promisable<
    ReturnType<SimApi[K]>
  >;
};

export function simDebugHook(api: SimReadPort, seed: number): SimDebugHook {
  return {
    seed,
    status: () => api.status(),
    commandLog: () => api.commandLog(),
    eventLogHash: () => api.eventLogHash(),
    eventHistory: () => api.eventHistory(),
  };
}

/** Installs the hook on `window.__kp.sim`; returns a function that removes it again. */
export function installSimDebugHook(hook: SimDebugHook): () => void {
  window.__kp = { ...window.__kp, sim: hook };
  return () => {
    if (window.__kp?.sim === hook) delete window.__kp.sim;
  };
}

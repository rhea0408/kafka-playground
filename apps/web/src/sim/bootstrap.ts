/**
 * App bootstrap: starts the sim worker, feeds its frames into simStore, keeps the reduced-motion
 * setting in sync, and builds the actions (the command path) on top of the worker API.
 */

import { createActions, type Actions } from '../actions/actions.ts';
import { applySimFrame } from '../stores/sim-store.ts';
import { watchReducedMotion } from '../stores/ui-store.ts';
import { createSimClient, type SimClient } from './sim-client.ts';

/** Seed for the Sandbox; the same one the engine demo uses (`--seed 7`). */
export const DEFAULT_SEED = 7;

export interface StartedSim {
  readonly actions: Actions;
  /** Resolves once the worker is subscribed and the engine runs the initial canvas. */
  readonly ready: Promise<void>;
  readonly dispose: () => void;
}

export interface StartSimOptions {
  readonly seed?: number;
  /** Defaults to a new worker (`createSimClient`). */
  readonly client?: SimClient;
}

export function startSim({ seed = DEFAULT_SEED, client }: StartSimOptions = {}): StartedSim {
  const sim = client ?? createSimClient();
  const actions = createActions({ sim: sim.api, seed });
  const stopWatching = watchReducedMotion();
  let stopFrames: (() => Promise<void>) | null = null;
  // An object, not a `let`: TS can't see the closure in dispose() change a local boolean.
  const life = { disposed: false };

  const ready = (async () => {
    // Subscribe first: init sends its full frame to existing subscribers only.
    stopFrames = await sim.onFrames((frame) => {
      applySimFrame(frame);
    });
    if (!life.disposed) await actions.init();
  })();

  return {
    actions,
    ready,
    dispose: () => {
      life.disposed = true;
      stopWatching();
      void stopFrames?.();
      sim.dispose();
    },
  };
}

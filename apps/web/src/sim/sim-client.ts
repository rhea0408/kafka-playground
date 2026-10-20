/**
 * Main-thread side of the sim worker: creates the worker and wraps it with Comlink. Every call is
 * async (it crosses `postMessage`); frames arrive through `subscribe`.
 */

import { proxy, releaseProxy, wrap, type Remote } from 'comlink';
import type { FrameListener, SimApi } from './protocol.ts';

export interface SimClient {
  /** The worker's API. `subscribe` / `unsubscribe` are wrapped by `onFrames` below. */
  readonly api: Remote<SimApi>;
  /** Starts receiving frames (a full one first). Resolves to a function that stops them. */
  readonly onFrames: (listener: FrameListener) => Promise<() => Promise<void>>;
  /** Terminates the worker. The client is unusable afterwards. */
  readonly dispose: () => void;
}

export function createSimClient(): SimClient {
  // Vite bundles the worker from this URL pattern (worker.format 'es' in vite.config.ts).
  const worker = new Worker(new URL('./sim.worker.ts', import.meta.url), {
    type: 'module',
    name: 'sim',
  });
  const api = wrap<SimApi>(worker);
  return {
    api,
    onFrames: async (listener) => {
      const token = await api.subscribe(proxy(listener));
      return async () => {
        await api.unsubscribe(token);
      };
    },
    dispose: () => {
      api[releaseProxy]();
      worker.terminate();
    },
  };
}

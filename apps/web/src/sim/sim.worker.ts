/**
 * Web Worker entry: one SimHost driven by the worker's wall clock, exposed over Comlink
 * (architecture §3, NFR-2.2). The host may read `performance.now()` and use timers; the engine it
 * drives never does (CLAUDE.md rule 2).
 */

import { expose, releaseProxy, type Remote } from 'comlink';
import type { FrameListener, SimApi } from './protocol.ts';
import { SimHost } from './sim-host.ts';

/** Run-loop tick: about 60 Hz. Frames go out at most every 33 ms (FRAME_INTERVAL_MS). */
const TICK_MS = 16;

const host = new SimHost({
  now: () => performance.now(),
  scheduleTick: (tick) => {
    const id = setTimeout(tick, TICK_MS);
    return () => {
      clearTimeout(id);
    };
  },
});

/** Frame callbacks are Comlink proxies to the main thread; release them on unsubscribe. */
const remotes = new Map<number, Remote<FrameListener>>();

const api: SimApi = {
  init: (options) => host.init(options),
  dispatch: (command) => host.dispatch(command),
  play: () => {
    host.play();
  },
  pause: () => {
    host.pause();
  },
  step: () => host.step(),
  stepBy: (ms) => host.stepBy(ms),
  setSpeed: (speed) => host.setSpeed(speed),
  reset: () => {
    host.reset();
  },
  subscribe: (onFrame) => {
    // `onFrame` is a Comlink proxy: calling it posts the frame to the main thread and returns a
    // promise we don't wait for, so a slow main thread never blocks the run loop.
    const remote = onFrame as unknown as Remote<FrameListener>;
    const token = host.subscribe((frame) => {
      void remote(frame);
    });
    remotes.set(token, remote);
    return token;
  },
  unsubscribe: (token) => {
    host.unsubscribe(token);
    remotes.get(token)?.[releaseProxy]();
    remotes.delete(token);
  },
  status: () => host.status(),
  commandLog: () => host.commandLog(),
  eventLogHash: () => host.eventLogHash(),
  eventHistory: () => host.eventHistory(),
};

expose(api);

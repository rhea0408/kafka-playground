// @vitest-environment jsdom
import { hashEvents } from '@kp/engine';
import { describe, expect, it } from 'vitest';
import { newOverlayDebug } from '../canvas/overlay/debug.ts';
import { defaultSceneState } from '../stores/default-scene.ts';
import { toTopologyInput } from '../stores/topology-store.ts';
import { installSimDebugHook, simDebugHook } from './debug-hook.ts';
import { SimHost } from './sim-host.ts';

function host(): SimHost {
  const h = new SimHost({ now: () => 0, scheduleTick: () => () => undefined });
  expect(h.init({ seed: 7, topology: toTopologyInput(defaultSceneState()) })).toEqual({ ok: true });
  h.dispatch({ type: 'ProduceManual', producerId: 'p1', key: 'D', value: 'x' });
  h.stepBy(3_000);
  return h;
}

describe('simDebugHook', () => {
  it('passes the host’s read-only answers through', async () => {
    const h = host();
    const hook = simDebugHook(h, 7);
    expect(hook.seed).toBe(7);
    expect(await hook.status()).toEqual(h.status());
    expect(await hook.commandLog()).toEqual(h.commandLog());
    const history = await hook.eventHistory();
    expect(history.length).toBeGreaterThan(0);
    expect(await hook.eventLogHash()).toBe(hashEvents(history));
  });

  it('is installed on window.__kp.sim next to other hooks and removed again', () => {
    const overlay = newOverlayDebug();
    window.__kp = { overlay };
    const hook = simDebugHook(host(), 7);
    const remove = installSimDebugHook(hook);
    expect(window.__kp.sim).toBe(hook);
    expect(window.__kp.overlay).toBe(overlay);
    remove();
    expect(window.__kp.sim).toBeUndefined();
  });
});

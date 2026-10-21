import type { EngineEvent } from '@kp/engine';
import type { TopologyInput } from '@kp/schema';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { DiffFrame, FullFrame, SimFrame } from '../sim/protocol.ts';
import { SimHost } from '../sim/sim-host.ts';
import {
  appendBounded,
  applySimFrame,
  EVENT_LIST_SIZE,
  onSimFrame,
  resetSimStore,
  useSimStore,
} from './sim-store.ts';

const topology: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1' }],
  topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
  producers: [
    {
      kind: 'producer',
      id: 'p1',
      ratePerSec: 50,
      keyStrategy: { type: 'random', keys: ['D', 'E', 'F'] },
    },
  ],
  consumers: [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest' }],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'orders', to: 'c1' },
  ],
};

function newHost(): SimHost {
  const host = new SimHost({ now: () => 0, scheduleTick: () => () => undefined });
  expect(host.init({ seed: 7, topology })).toEqual({ ok: true });
  return host;
}

/** The snapshot a brand-new subscriber gets: the host's current state. */
function freshSnapshot(host: SimHost) {
  let full: FullFrame | undefined;
  const token = host.subscribe((frame) => {
    if (frame.kind === 'full') full = frame;
  });
  host.unsubscribe(token);
  if (full === undefined) throw new Error('no full frame');
  return full.snapshot;
}

describe('simStore frame application', () => {
  beforeEach(() => {
    resetSimStore();
  });

  it('applies a full frame, then diffs, to the same snapshot the host holds', () => {
    const host = newHost();
    const frames: SimFrame[] = [];
    host.subscribe((frame) => {
      frames.push(frame);
      applySimFrame(frame);
    });
    for (let i = 0; i < 20; i++) host.stepBy(100);

    expect(frames[0]?.kind).toBe('full');
    expect(frames.slice(1).every((f) => f.kind === 'diff')).toBe(true);
    const state = useSimStore.getState();
    expect(state.snapshot).toEqual(freshSnapshot(host));
    expect(state.t).toBe(2000);
    expect(state.frame).toBe(frames.at(-1)?.frame);
    // Every event, exactly once, in order.
    expect(state.events).toEqual(host.eventHistory());
    expect(state.events.length).toBeGreaterThan(50);
  });

  it('takes play state and speed from frames', () => {
    const host = newHost();
    host.subscribe((frame) => {
      applySimFrame(frame);
    });
    host.setSpeed(5);
    expect(useSimStore.getState()).toMatchObject({ speed: 5, playing: false });
  });

  it('a full frame replaces the event list with its history (events are listed, not animated)', () => {
    const host = newHost();
    host.stepBy(500);
    const seen: SimFrame[] = [];
    onSimFrame((frame) => seen.push(frame));
    host.subscribe((frame) => {
      applySimFrame(frame);
    });
    expect(useSimStore.getState().events).toEqual(host.eventHistory());
    const first = seen[0];
    expect(first?.kind).toBe('full');
    // The overlay animates `events` only; a late subscriber's full frame has none.
    expect(first?.events).toEqual([]);
  });

  it('throws on a diff before any full frame (protocol bug)', () => {
    const diff = { kind: 'diff', frame: 1, t: 0, playing: false, speed: 1, events: [] };
    expect(() => {
      applySimFrame(diff as unknown as DiffFrame);
    }).toThrow(/Diff frame before a full frame/);
  });
});

describe('bounded event list', () => {
  beforeEach(() => {
    resetSimStore();
  });

  it('matches the host ring buffer size', () => {
    expect(EVENT_LIST_SIZE).toBe(10_000);
  });

  it('appendBounded keeps the newest entries', () => {
    expect(appendBounded([1, 2, 3], [4, 5], 4)).toEqual([2, 3, 4, 5]);
    expect(appendBounded([1, 2, 3, 4, 5, 6], [], 4)).toEqual([3, 4, 5, 6]);
    const list = [1, 2];
    expect(appendBounded(list, [], 4)).toBe(list);
  });

  it('keeps at most `limit` events across full and diff frames', () => {
    const host = newHost();
    host.stepBy(1000);
    const limit = 25;
    host.subscribe((frame) => {
      applySimFrame(frame, limit);
    });
    const history = host.eventHistory();
    expect(history.length).toBeGreaterThan(limit);
    expect(useSimStore.getState().events).toEqual(history.slice(-limit));
    for (let i = 0; i < 5; i++) host.stepBy(200);
    const events = useSimStore.getState().events;
    expect(events).toHaveLength(limit);
    expect(events).toEqual(host.eventHistory().slice(-limit));
  });
});

describe('onSimFrame', () => {
  beforeEach(() => {
    resetSimStore();
  });

  it('runs after the store is updated and can be removed', () => {
    const host = newHost();
    const seen: { frame: number; storeFrame: number; events: readonly EngineEvent[] }[] = [];
    const off = onSimFrame((frame) => {
      seen.push({
        frame: frame.frame,
        storeFrame: useSimStore.getState().frame,
        events: frame.events,
      });
    });
    host.subscribe((frame) => {
      applySimFrame(frame);
    });
    host.stepBy(300);
    expect(seen.length).toBe(2);
    for (const s of seen) expect(s.storeFrame).toBe(s.frame);
    off();
    host.stepBy(300);
    expect(seen.length).toBe(2);
  });

  it('a throwing listener does not stop other listeners; its error is rethrown later', () => {
    const host = newHost();
    const deferred: (() => void)[] = [];
    vi.stubGlobal('queueMicrotask', (callback: () => void) => {
      deferred.push(callback);
    });
    try {
      onSimFrame(() => {
        throw new Error('boom');
      });
      const good = vi.fn();
      onSimFrame(good);
      host.subscribe((frame) => {
        applySimFrame(frame);
      });
      expect(good).toHaveBeenCalledTimes(1);
      expect(useSimStore.getState().snapshot).not.toBeNull();
      expect(deferred).toHaveLength(1);
      expect(deferred[0]).toThrow('boom');
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

import {
  createEngine,
  hashEvents,
  replayCommandLog,
  type EngineEvent,
  type EngineSnapshot,
} from '@kp/engine';
import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { applyFrame, type SimFrame } from './protocol.ts';
import { MAX_TICK_DELTA_MS, SimHost, type SimHostOptions } from './sim-host.ts';

const topology: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1' }],
  topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
  producers: [
    {
      kind: 'producer',
      id: 'p1',
      ratePerSec: 20,
      keyStrategy: { type: 'random', keys: ['D', 'E', 'F'] },
    },
  ],
  consumers: [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest' }],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'orders', to: 'c1' },
  ],
};

const SEED = 7;

/** A fake wall clock and tick scheduler: `advance` moves time and fires due ticks in order. */
function fakeWorld(tickMs = 16) {
  let wall = 0;
  let pending: (() => void) | null = null;
  let dueAt = 0;
  const options: SimHostOptions = {
    now: () => wall,
    scheduleTick: (tick) => {
      if (pending !== null) throw new Error('two ticks scheduled at once');
      pending = tick;
      dueAt = wall + tickMs;
      return () => {
        if (pending === tick) pending = null;
      };
    },
  };
  return {
    options,
    now: () => wall,
    hasTick: () => pending !== null,
    /** Moves the wall clock by `ms`, running every tick that falls due on the way. */
    advance(ms: number): void {
      const end = wall + ms;
      while (pending !== null && dueAt <= end) {
        wall = dueAt;
        const tick = pending;
        pending = null;
        tick();
      }
      wall = end;
    },
  };
}

function setup(tickMs = 16, extra: Partial<SimHostOptions> = {}) {
  const world = fakeWorld(tickMs);
  const host = new SimHost({ ...world.options, ...extra });
  expect(host.init({ seed: SEED, topology })).toEqual({ ok: true });
  const frames: { wall: number; frame: SimFrame }[] = [];
  host.subscribe((frame) => frames.push({ wall: world.now(), frame }));
  return { world, host, frames };
}

const eventsOf = (frames: readonly { frame: SimFrame }[]): EngineEvent[] =>
  frames.flatMap(({ frame }) => frame.events);

/** The UI's view: the snapshot rebuilt from every frame. */
function rebuild(frames: readonly { frame: SimFrame }[]): EngineSnapshot | null {
  let snapshot: EngineSnapshot | null = null;
  for (const { frame } of frames) snapshot = applyFrame(snapshot, frame);
  return snapshot;
}

describe('SimHost run loop', () => {
  it('advances sim time by wall time × speed', () => {
    const { world, host } = setup();
    host.play();
    world.advance(1000);
    expect(host.status().t).toBe(992); // last tick at wall 992 (62 × 16 ms)

    expect(host.setSpeed(10)).toEqual({ ok: true });
    world.advance(1000);
    // setSpeed ran wall 992 → 1000 at 1× first; ticks then run 1000 → 2000 at 10×.
    expect(host.status().t).toBe(1000 + 10_000);
  });

  it('carries fractional ms so slow speeds and odd tick lengths lose no time', () => {
    const { world, host } = setup(1000 / 60);
    expect(host.setSpeed(0.25)).toEqual({ ok: true });
    host.play();
    world.advance(6000);
    host.pause(); // runs the time up to now
    expect(host.status().t).toBe(1500);
  });

  it('pause stops sim time and the tick loop', () => {
    const { world, host } = setup();
    host.play();
    world.advance(500);
    host.pause();
    const t = host.status().t;
    expect(t).toBe(500);
    expect(world.hasTick()).toBe(false);
    world.advance(10_000);
    expect(host.status()).toMatchObject({ t, playing: false });
  });

  it('caps one tick so a throttled (background) tab does not jump minutes ahead', () => {
    const { world, host } = setup(60_000);
    host.setSpeed(2);
    host.play();
    world.advance(60_000);
    expect(host.status().t).toBe(MAX_TICK_DELTA_MS * 2);
  });

  it('sends at most one run-loop frame per 33 ms', () => {
    const { world, host, frames } = setup(5);
    host.play();
    const start = frames.length;
    world.advance(2000);
    const loopFrames = frames.slice(start + 1); // after the frame play() sends
    expect(loopFrames.length).toBeGreaterThan(50);
    expect(loopFrames.length).toBeLessThanOrEqual(Math.ceil(2000 / 33) + 1);
    for (let i = 1; i < loopFrames.length; i++) {
      const gap = (loopFrames[i]?.wall ?? 0) - (loopFrames[i - 1]?.wall ?? 0);
      expect(gap).toBeGreaterThanOrEqual(33);
    }
  });

  it('frames carry every event exactly once, in order, and rebuild the engine snapshot', () => {
    const { world, host, frames } = setup(7);
    host.play();
    world.advance(1234);
    expect(
      host.dispatch({ type: 'ProduceManual', producerId: 'p1', key: 'Z', value: 'x' }),
    ).toEqual({ ok: true });
    world.advance(800);
    host.setSpeed(3.5);
    world.advance(999);
    host.pause();
    host.stepBy(250);
    host.step();
    host.play();
    world.advance(321);
    host.pause();

    const events = eventsOf(frames);
    expect(events.length).toBeGreaterThan(100);
    events.forEach((e, i) => {
      expect(e.seq).toBe(i);
    });

    // Replaying the host's command log on a plain engine gives the same events and snapshot.
    const t = host.status().t;
    const replayed: EngineEvent[] = [];
    const engine = replayCommandLog({ seed: SEED, topology }, host.commandLog(), (e) =>
      replayed.push(e),
    );
    engine.runUntil(t);
    expect(events).toEqual(replayed);
    expect(host.eventLogHash()).toBe(hashEvents(replayed));
    expect(rebuild(frames)).toEqual(engine.getSnapshot());
  });

  it('skips a frame when nothing changed', () => {
    const { host, frames } = setup();
    const before = frames.length;
    host.dispatch({ type: 'UpdateConfig', id: 'p1', patch: { ratePerSec: 20 } }); // no-op patch
    expect(frames.length).toBe(before);
  });
});

describe('SimHost step', () => {
  it('step processes exactly one scheduled task', () => {
    const { host, frames } = setup();
    const reference = createEngine({ seed: SEED, topology });
    const refEvents: EngineEvent[] = [];
    reference.on((e) => refEvents.push(e));

    for (let i = 0; i < 25; i++) {
      const result = host.step();
      expect(reference.step()).toBe(true);
      expect(result).toEqual({ stepped: true, t: reference.now() });
      expect(eventsOf(frames)).toEqual(refEvents);
    }
    expect(reference.stepsProcessed()).toBe(25);
    // Mid-instant: the hash replays to the same task count, not just the same time.
    expect(host.eventLogHash()).toBe(hashEvents(refEvents));
    expect(frames.length).toBe(1 + 25); // the subscribe frame, then one per step
  });

  it('step and stepBy pause a running host', () => {
    const { world, host } = setup();
    host.play();
    world.advance(100);
    host.step();
    expect(host.status().playing).toBe(false);
    host.play();
    world.advance(100);
    const t = host.status().t;
    expect(host.stepBy(40)).toEqual({ ok: true });
    expect(host.status()).toMatchObject({ playing: false });
    expect(host.status().t).toBeGreaterThanOrEqual(t + 40);
  });

  it('stepBy rejects anything but a positive whole number of ms', () => {
    const { host } = setup();
    for (const ms of [0, -5, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(host.stepBy(ms).ok).toBe(false);
    }
    expect(host.status().t).toBe(0);
  });
});

describe('SimHost commands and speed', () => {
  it('dispatch while paused sends a frame with the change', () => {
    const { host, frames } = setup();
    const before = frames.length;
    expect(
      host.dispatch({ type: 'AddComponent', component: { kind: 'producer', id: 'p2' } }),
    ).toEqual({ ok: true });
    expect(frames.length).toBe(before + 1);
    const last = frames.at(-1)?.frame;
    expect(last?.kind).toBe('diff');
    expect(last?.events.map((e) => e.type)).toEqual(['ComponentAdded']);
    expect(rebuild(frames)?.producers.map((p) => p.config.id)).toEqual(['p1', 'p2']);
  });

  it('a rejected command returns the reason, sends no frame and is not logged', () => {
    const { host, frames } = setup();
    const before = frames.length;
    const result = host.dispatch({ type: 'Connect', from: 'p1', to: 'nowhere' });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.reason).toMatch(/nowhere/);
    expect(frames.length).toBe(before);
    expect(host.commandLog()).toEqual([]);
  });

  it('setSpeed rejects values outside 0.25–10 and keeps the old speed', () => {
    const { host } = setup();
    for (const speed of [0.1, 0, -1, 10.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(host.setSpeed(speed).ok).toBe(false);
    }
    expect(host.status().speed).toBe(1);
    expect(host.setSpeed(0.25).ok).toBe(true);
    expect(host.setSpeed(10).ok).toBe(true);
    expect(host.status().speed).toBe(10);
  });

  it('play, pause and speed are not in the command log', () => {
    const { world, host } = setup();
    host.play();
    host.setSpeed(2);
    world.advance(300);
    host.pause();
    host.step();
    expect(host.commandLog()).toEqual([]);
  });

  it('frames report play state and speed', () => {
    const { host, frames } = setup();
    host.play();
    expect(frames.at(-1)?.frame).toMatchObject({ playing: true, speed: 1 });
    host.setSpeed(5);
    expect(frames.at(-1)?.frame).toMatchObject({ playing: true, speed: 5 });
    host.pause();
    expect(frames.at(-1)?.frame).toMatchObject({ playing: false, speed: 5 });
  });
});

describe('SimHost init, reset and subscribers', () => {
  it('rejects an invalid topology and keeps no engine', () => {
    const host = new SimHost(fakeWorld().options);
    const result = host.init({ seed: 1, topology: { topics: [{ kind: 'topic', id: '' }] } });
    expect(result.ok).toBe(false);
    expect(host.status().initialised).toBe(false);
    expect(() => {
      host.play();
    }).toThrow(/init/);
  });

  it('reset rebuilds from the same seed and topology and clears the logs', () => {
    const { world, host, frames } = setup();
    const fresh = host.eventLogHash();
    host.play();
    world.advance(700);
    host.dispatch({ type: 'KillConsumer', id: 'c1' });
    world.advance(300);
    expect(host.commandLog().length).toBe(1);
    const firstRun = eventsOf(frames).filter((e) => e.t <= 500);

    host.reset();
    expect(host.status()).toMatchObject({ t: 0, playing: false, speed: 1 });
    expect(host.commandLog()).toEqual([]);
    expect(host.eventHistory()).toEqual([]);
    expect(host.eventLogHash()).toBe(fresh);
    const resetFrame = frames.at(-1)?.frame;
    expect(resetFrame).toMatchObject({ kind: 'full', t: 0, events: [], history: [] });

    // Same seed, same run.
    const start = frames.length;
    host.stepBy(500);
    expect(eventsOf(frames.slice(start))).toEqual(firstRun);
  });

  it('a late subscriber gets a full frame with history, then no repeats', () => {
    const { world, host, frames } = setup();
    host.play();
    world.advance(1000);
    const late: SimFrame[] = [];
    host.subscribe((f) => late.push(f));
    world.advance(1000);
    host.pause();

    const [first] = late;
    expect(first?.kind).toBe('full');
    if (first?.kind !== 'full') return;
    expect(first.events).toEqual([]);
    expect(first.history).toEqual(eventsOf(frames).slice(0, first.history.length));
    const lateEvents = [...first.history, ...late.slice(1).flatMap((f) => f.events)];
    expect(lateEvents).toEqual(eventsOf(frames));
    expect(rebuild(late.map((frame) => ({ frame })))).toEqual(rebuild(frames));
  });

  it('unsubscribed listeners get nothing more', () => {
    const { world, host } = setup();
    const got: SimFrame[] = [];
    const token = host.subscribe((f) => got.push(f));
    host.unsubscribe(token);
    host.play();
    world.advance(500);
    expect(got.length).toBe(1);
  });

  it('keeps a bounded event history', () => {
    const { world, host, frames } = setup(16, { historySize: 50 });
    host.play();
    world.advance(3000);
    host.pause();
    const all = eventsOf(frames);
    expect(all.length).toBeGreaterThan(50);
    expect(host.eventHistory()).toEqual(all.slice(-50));
  });
});

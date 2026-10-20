import type { EngineEvent } from '@kp/engine';
import { describe, expect, it } from 'vitest';
import type { AnchorResolver } from './anchors.ts';
import {
  ARRIVED_MEMORY,
  CAP_CHIP_HOLD_MS,
  DotModel,
  FADE_MS,
  keyShade,
  MAX_DOTS,
  MAX_STEP_MS,
  type StepOptions,
} from './dots.ts';
import type { Anchor } from './path.ts';

// A straight layout: p1 → (120 px) → t1 P0 … t1 P0 → (120 px) → c1. At 120 px/s each hop is 1 s.
const ANCHORS: Record<string, Anchor> = {
  'p1|out': { x: 0, y: 0, side: 'right' },
  't1|p0:in': { x: 120, y: 0, side: 'left' },
  't1|p1:in': { x: 120, y: 48, side: 'left' },
  't1|p0:out': { x: 240, y: 0, side: 'right' },
  'c1|row:c1': { x: 360, y: 0, side: 'left' },
  'c2|row:c2': { x: 360, y: 100, side: 'left' },
};
const resolve: AnchorResolver = (ref) => ANCHORS[`${ref.nodeId}|${ref.handleId}`];

const PLAY: StepOptions = { playing: true, speed: 1, reducedMotion: false };
const PAUSED: StepOptions = { ...PLAY, playing: false };

let seq = 0;
function produced(traceId: string, key: string | null = 'A', partition = 0): EngineEvent {
  return {
    t: 0,
    seq: seq++,
    type: 'RecordProduced',
    producerId: 'p1',
    topic: 't1',
    partition,
    key,
    value: 'v',
    traceId,
    acks: 'all',
  };
}
function consumed(traceId: string, consumerId = 'c1'): EngineEvent {
  return {
    t: 0,
    seq: seq++,
    type: 'RecordConsumed',
    consumerId,
    groupId: 'g',
    topic: 't1',
    partition: 0,
    offset: 0,
    key: 'A',
    value: 'v',
    traceId,
  };
}

/**
 * Steps the model from `from` to `to` ms in 16 ms frames; returns the time reached. The first
 * step at `from` starts the model's clock (a step after a gap of nothing moves dots by 0).
 */
function run(model: DotModel, from: number, to: number, options: StepOptions = PLAY): number {
  let now = from;
  model.step(now, options, resolve);
  while (now < to) {
    now = Math.min(now + 16, to);
    model.step(now, options, resolve);
  }
  return now;
}

describe('A1 dots', () => {
  it('start on RecordProduced at the producer and travel 120 px/s to the partition row', () => {
    const model = new DotModel();
    model.ingest([produced('p1@1#0', 'B')], 0);
    model.step(0, PLAY, resolve);
    const dot = model.dots[0];
    expect(dot).toMatchObject({ kind: 'A1', letter: 'B', x: 0, y: 0, placed: true });
    run(model, 0, 500);
    expect(model.dots[0]?.x).toBeCloseTo(60, 0);
    run(model, 500, 1000);
    expect(model.dots).toHaveLength(0);
    expect(model.stats.arrived).toBe(1);
  });

  it('go to the row of the partition the producer chose', () => {
    const model = new DotModel();
    model.ingest([produced('p1@1#0', 'C', 1)], 0);
    run(model, 0, 1000 - 16);
    expect(model.dots[0]?.x).toBeGreaterThan(100);
    expect(model.dots[0]?.y).toBeGreaterThan(40);
  });

  it('use ∅ for a null key, and the same letter as the log cells otherwise', () => {
    const model = new DotModel();
    model.ingest([produced('a', null), produced('b', 'Bob')], 0);
    expect(model.dots.map((d) => d.letter)).toEqual(['∅', 'B']);
  });

  it('get a grey shade per key (A dark, B mid, C light) and keep it for the same key', () => {
    expect([keyShade('A'), keyShade('B'), keyShade('C'), keyShade('D')]).toEqual([0, 1, 2, 0]);
    expect(keyShade('order-17')).toBe(keyShade('order-17'));
    expect(keyShade(null)).toBe(1);
  });

  it('are dropped (with their queued A2) when an end of the path disappears', () => {
    const model = new DotModel();
    model.ingest([produced('x'), consumed('x')], 0);
    model.step(0, PLAY, () => undefined);
    expect(model.load).toBe(0);
    expect(model.stats.droppedNoPath).toBe(2);
  });
});

describe('A2 waits for A1 (ADR 0008 rule 1)', () => {
  it('queues A2 by trace id until the A1 dot arrives, then starts it at the partition row', () => {
    const model = new DotModel();
    model.ingest([produced('p1@1#0')], 0);
    // The engine delivers the record to the consumer ~2 ms later; the A2 dot must wait.
    model.ingest([consumed('p1@1#0')], 5);
    expect(model.dots.map((d) => d.kind)).toEqual(['A1']);
    expect(model.queued).toBe(1);
    let now = run(model, 0, 1000 - 16);
    expect(model.dots.map((d) => d.kind)).toEqual(['A1']);
    now = run(model, now, 1000);
    expect(model.queued).toBe(0);
    expect(model.dots.map((d) => d.kind)).toEqual(['A2']);
    expect(model.dots[0]).toMatchObject({ x: 240, y: 0 });
    run(model, now, 1500);
    expect(model.dots[0]?.x).toBeCloseTo(300, 0);
    run(model, 1500, 2000);
    expect(model.dots).toHaveLength(0);
  });

  it('never shows an A2 dot of a trace while its A1 dot is still moving', () => {
    const model = new DotModel();
    for (let i = 0; i < 20; i++) {
      model.ingest([produced(`t${String(i)}`)], i * 50);
      model.ingest([consumed(`t${String(i)}`)], i * 50 + 2);
      model.step(i * 50, PLAY, resolve);
      const a1 = new Set(model.dots.filter((d) => d.kind === 'A1').map((d) => d.traceId));
      for (const d of model.dots) if (d.kind === 'A2') expect(a1.has(d.traceId)).toBe(false);
    }
  });

  it('starts one A2 per consumer of the record', () => {
    const model = new DotModel();
    model.ingest([produced('r'), consumed('r', 'c1'), consumed('r', 'c2')], 0);
    run(model, 0, 1000);
    expect(model.dots.map((d) => d.to.nodeId)).toEqual(['c1', 'c2']);
  });

  it('starts A2 at once for a record whose A1 dot already arrived (consumed later)', () => {
    const model = new DotModel();
    model.ingest([produced('r')], 0);
    const now = run(model, 0, 1000);
    expect(model.load).toBe(0);
    model.ingest([consumed('r')], now);
    expect(model.dots.map((d) => d.kind)).toEqual(['A2']);
  });

  it('skips A2 when the record had no A1 dot (skipped, or produced before a reset)', () => {
    const model = new DotModel();
    model.ingest([consumed('never-seen')], 0);
    expect(model.load).toBe(0);
    expect(model.stats.skippedNoA1).toBe(1);

    const capped = new DotModel(1);
    capped.ingest([produced('a'), produced('b'), consumed('b')], 0);
    expect(capped.dots.map((d) => d.traceId)).toEqual(['a']);
    expect(capped.stats).toMatchObject({ skippedCap: 1, skippedNoA1: 1 });
  });

  it('forgets the oldest arrived trace ids beyond ARRIVED_MEMORY', () => {
    const model = new DotModel(ARRIVED_MEMORY + 10);
    const events = Array.from({ length: ARRIVED_MEMORY + 1 }, (_, i) => produced(`r${String(i)}`));
    model.ingest(events, 0);
    run(model, 0, 1000);
    model.ingest([consumed('r0'), consumed(`r${String(ARRIVED_MEMORY)}`)], 1000);
    expect(model.dots.map((d) => d.traceId)).toEqual([`r${String(ARRIVED_MEMORY)}`]);
    expect(model.stats.skippedNoA1).toBe(1);
  });
});

describe('cap (NFR-2.5) and skip, not delay', () => {
  it(`keeps at most ${String(MAX_DOTS)} dots, counting queued A2 dots, and skips the rest`, () => {
    const model = new DotModel();
    const events = Array.from({ length: MAX_DOTS + 20 }, (_, i) => produced(`r${String(i)}`));
    model.ingest(events, 0);
    expect(model.dots).toHaveLength(MAX_DOTS);
    expect(model.stats.skippedCap).toBe(20);
    // Full: a consumed record of a moving A1 can't queue either.
    model.ingest([consumed('r0')], 0);
    expect(model.queued).toBe(0);
    expect(model.stats.skippedCap).toBe(21);
  });

  it('a queued A2 keeps its reserved room when its A1 arrives', () => {
    const model = new DotModel(2);
    model.ingest([produced('a'), consumed('a')], 0);
    expect(model.load).toBe(2);
    model.ingest([produced('b')], 0);
    expect(model.stats.skippedCap).toBe(1);
    run(model, 0, 1000);
    expect(model.dots.map((d) => `${d.kind}:${d.traceId}`)).toEqual(['A2:a']);
  });

  it('shows the "too many" chip for a moment after a skip', () => {
    const model = new DotModel(1);
    model.ingest([produced('a'), produced('b')], 100);
    expect(model.capChipVisible(100)).toBe(true);
    expect(model.capChipVisible(100 + CAP_CHIP_HOLD_MS - 1)).toBe(true);
    expect(model.capChipVisible(100 + CAP_CHIP_HOLD_MS)).toBe(false);
  });

  it('counts records skipped while the tab was hidden', () => {
    const model = new DotModel();
    model.skip(3, 10);
    expect(model.stats.skippedCap).toBe(3);
    expect(model.load).toBe(0);
  });

  it('treats a long frame gap as MAX_STEP_MS instead of jumping dots ahead', () => {
    const model = new DotModel();
    model.ingest([produced('a')], 0);
    model.step(0, PLAY, resolve);
    model.step(5000, PLAY, resolve);
    expect(model.dots[0]?.x).toBeCloseTo((120 * MAX_STEP_MS) / 1000, 0);
  });
});

describe('play state, speed, reset', () => {
  it('pause freezes dots; play resumes them where they were', () => {
    const model = new DotModel();
    model.ingest([produced('a')], 0);
    let now = run(model, 0, 256);
    const x = model.dots[0]?.x ?? Number.NaN;
    now = run(model, now, 2000, PAUSED);
    expect(model.dots[0]?.x).toBe(x);
    run(model, now, now + 256);
    expect(model.dots[0]?.x).toBeCloseTo(x + (120 * 256) / 1000, 0);
  });

  it('pausing the loop clock does not count the idle time', () => {
    const model = new DotModel();
    model.ingest([produced('a')], 0);
    model.step(0, PLAY, resolve);
    model.pauseClock();
    model.step(10_000, PLAY, resolve);
    expect(model.dots[0]?.x).toBe(0);
  });

  it('speed scales the dot speed (120 px/s × sim speed), also mid-flight', () => {
    const model = new DotModel();
    model.ingest([produced('a')], 0);
    run(model, 0, 160, { ...PLAY, speed: 4 });
    expect(model.dots[0]?.x).toBeCloseTo(120 * 4 * 0.16, 0);
    run(model, 160, 320, { ...PLAY, speed: 0.25 });
    expect(model.dots[0]?.x).toBeCloseTo(120 * 4 * 0.16 + 120 * 0.25 * 0.16, 0);
  });

  it('reset clears dots, queued A2 dots and the arrived memory', () => {
    const model = new DotModel();
    model.ingest([produced('a'), consumed('a'), produced('b')], 0);
    const now = run(model, 0, 1000);
    model.ingest([produced('c'), consumed('c')], now);
    model.reset();
    expect(model.load).toBe(0);
    expect(model.busy).toBe(false);
    model.ingest([consumed('b')], now);
    expect(model.load).toBe(0);
    // a, b, a's A2 (released when a arrived) and c; c's A2 was only queued.
    expect(model.stats.started).toBe(4);
  });

  it('follows node moves: positions come from the anchors at every step', () => {
    const model = new DotModel();
    model.ingest([produced('a')], 0);
    run(model, 0, 496);
    const moved: AnchorResolver = (ref) => {
      const a = resolve(ref);
      return a === undefined ? undefined : { ...a, y: a.y + 300 };
    };
    model.step(512, PLAY, moved);
    expect(model.dots[0]?.y).toBeCloseTo(300, 6);
  });
});

describe('reduced motion', () => {
  const REDUCED: StepOptions = { ...PLAY, reducedMotion: true };

  it('shows the dot at its destination and fades it in over 150 ms, without moving', () => {
    const model = new DotModel();
    model.ingest([produced('a')], 0);
    model.step(0, REDUCED, resolve);
    expect(model.dots[0]).toMatchObject({ mode: 'fade', x: 120, y: 0, alpha: 0 });
    model.step(75, REDUCED, resolve);
    expect(model.dots[0]).toMatchObject({ x: 120, alpha: 0.5 });
    model.step(FADE_MS, REDUCED, resolve);
    expect(model.dots).toHaveLength(0);
  });

  it('A2 still waits for A1, then fades in at the consumer row', () => {
    const model = new DotModel();
    model.ingest([produced('a'), consumed('a')], 0);
    model.step(0, REDUCED, resolve);
    model.step(FADE_MS / 2, REDUCED, resolve);
    model.step(FADE_MS, REDUCED, resolve);
    expect(model.dots).toMatchObject([{ kind: 'A2', x: 360, alpha: 0 }]);
    model.step(FADE_MS * 1.5, REDUCED, resolve);
    model.step(FADE_MS * 2, REDUCED, resolve);
    expect(model.dots).toHaveLength(0);
  });

  it('turns moving dots into fades when the setting changes mid-flight', () => {
    const model = new DotModel();
    model.ingest([produced('a')], 0);
    run(model, 0, 320);
    model.step(336, REDUCED, resolve);
    expect(model.dots[0]).toMatchObject({ mode: 'fade', x: 120 });
  });

  it('the fade is not scaled by sim speed and freezes while paused', () => {
    const model = new DotModel();
    model.ingest([produced('a')], 0);
    model.step(0, { ...REDUCED, speed: 10 }, resolve);
    model.step(75, { ...REDUCED, speed: 10 }, resolve);
    expect(model.dots[0]?.alpha).toBeCloseTo(0.5, 6);
    model.step(1000, { ...REDUCED, playing: false }, resolve);
    expect(model.dots[0]?.alpha).toBeCloseTo(0.5, 6);
  });
});

describe('stress hook spawn', () => {
  it('starts part-way along the path', () => {
    const model = new DotModel();
    model.spawn(
      {
        kind: 'A1',
        traceId: 's',
        key: 'A',
        from: { nodeId: 'p1', handleId: 'out' },
        to: { nodeId: 't1', handleId: 'p0:in' },
      },
      0,
      0.5,
    );
    model.step(0, PAUSED, resolve);
    expect(model.dots[0]?.x).toBeCloseTo(60, 6);
  });
});

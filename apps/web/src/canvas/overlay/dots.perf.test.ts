import type { EngineEvent } from '@kp/engine';
import { describe, expect, it } from 'vitest';
import type { AnchorResolver } from './anchors.ts';
import { DotModel, MAX_DOTS } from './dots.ts';

/**
 * Perf sanity check (NFR-2.1 / NFR-2.5): stepping 500 dots must take a small part of a 16.7 ms
 * frame. 600 steps = 10 s of animation at 60 fps. The bound is loose on purpose (CI machines vary);
 * a typical run is well under 1 ms per step.
 */
describe('DotModel performance', () => {
  it(`steps ${String(MAX_DOTS)} dots on 6 edges quickly`, () => {
    // Six long, curved edges: one producer to three partitions, three partitions to a consumer.
    const resolve: AnchorResolver = (ref) => {
      if (ref.handleId === 'out') return { x: 0, y: 0, side: 'right' };
      const m = /^p(\d+):(in|out)$/.exec(ref.handleId);
      if (m !== null) {
        const y = 200 + Number(m[1]) * 48;
        return m[2] === 'in' ? { x: 2000, y, side: 'left' } : { x: 2230, y, side: 'right' };
      }
      return { x: 4000, y: 600, side: 'left' };
    };
    const model = new DotModel();
    const events: EngineEvent[] = [];
    for (let i = 0; i < MAX_DOTS; i++) {
      events.push({
        t: 0,
        seq: i,
        type: 'RecordProduced',
        producerId: 'p1',
        topic: 't1',
        partition: i % 3,
        key: String.fromCharCode(65 + (i % 26)),
        value: 'v',
        traceId: `r${String(i)}`,
        acks: 'all',
      });
    }
    model.ingest(events, 0);
    expect(model.dots).toHaveLength(MAX_DOTS);

    const options = { playing: true, speed: 1, reducedMotion: false };
    const steps = 600;
    const start = performance.now();
    for (let i = 1; i <= steps; i++) model.step(i * 16, options, resolve);
    const perStep = (performance.now() - start) / steps;
    expect(model.dots).toHaveLength(MAX_DOTS); // 2000 px at 120 px/s: still travelling
    expect(perStep).toBeLessThan(4);
  });
});

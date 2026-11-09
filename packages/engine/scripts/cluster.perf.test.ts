/**
 * Performance baseline for NFR-2.4 (M4-T9): engine time for 60 sim-seconds of a three-broker
 * cluster (RF 3, 3 partitions, 20 msg/s, a two-member group), headless. Printed, not asserted
 * tightly: the summary records the number.
 */

import { describe, expect, it } from 'vitest';
import { createEngine } from '../src/engine.ts';
import { clusterTopology } from '../src/testing/scenes.ts';

describe('cluster benchmark', () => {
  it('runs 60 sim-seconds of a three-broker cluster well under real time', () => {
    const times: number[] = [];
    let events = 0;
    for (let i = 0; i < 5; i++) {
      const engine = createEngine({ seed: i, topology: clusterTopology });
      let n = 0;
      engine.on(() => n++);
      const t0 = performance.now();
      engine.runUntil(60_000);
      times.push(performance.now() - t0);
      events = n;
    }
    times.sort((a, b) => a - b);
    const median = times[2] ?? 0;
    console.log(`cluster 60 s: median ${median.toFixed(1)} ms, ${String(events)} events`);
    expect(median).toBeLessThan(60_000);
  });
});

import { describe, expect, it } from 'vitest';
import { createEngine } from '../engine.ts';
import { createEventHasher, hashEvents } from './hash.ts';
import type { EngineEvent } from './index.ts';

function run(): EngineEvent[] {
  const engine = createEngine({
    seed: 7,
    topology: {
      brokers: [{ kind: 'broker', id: 'b1' }],
      topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
      producers: [{ kind: 'producer', id: 'p1', ratePerSec: 20 }],
      consumers: [{ kind: 'consumer', id: 'c1' }],
      connections: [
        { from: 'p1', to: 'orders' },
        { from: 'orders', to: 'c1' },
      ],
    },
  });
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  engine.runUntil(3000);
  return events;
}

describe('event-log hash', () => {
  it('the incremental hasher matches the one-shot hash', () => {
    const events = run();
    expect(events.length).toBeGreaterThan(50);
    const hasher = createEventHasher();
    for (const e of events) hasher.add(e);
    expect(hasher.digest()).toBe(hashEvents(events));
  });

  it('matches for an empty log and for a single event', () => {
    const [first] = run();
    expect(createEventHasher().digest()).toBe(hashEvents([]));
    const hasher = createEventHasher();
    if (first === undefined) throw new Error('no events');
    hasher.add(first);
    expect(hasher.digest()).toBe(hashEvents([first]));
  });

  it('digest does not consume the hasher', () => {
    const events = run();
    const hasher = createEventHasher();
    for (const e of events.slice(0, 10)) hasher.add(e);
    hasher.digest();
    for (const e of events.slice(10)) hasher.add(e);
    expect(hasher.digest()).toBe(hashEvents(events));
  });

  it('is a stable FNV-1a value', () => {
    // FNV-1a of "[]": guards against accidental changes to the algorithm, which would break the
    // browser-vs-Node comparison in M2-T12 silently on one side only.
    expect(hashEvents([])).toBe('741638a5');
  });
});

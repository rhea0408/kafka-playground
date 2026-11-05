/**
 * selectHeld against the real engine (review F-6): after Kill and Restart, the row must keep
 * showing the old member's partitions until the coordinator removes it.
 */

import { createEngine, type EngineEvent } from '@kp/engine';
import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { INITIAL_SIM_STATE } from '../../stores/sim-store.ts';
import { selectHeld } from './use-live.ts';

const topology: TopologyInput = {
  brokers: [{ kind: 'broker', id: 'b1', groupInitialRebalanceDelayMs: 0 }],
  topics: [{ kind: 'topic', id: 'orders', partitions: 2 }],
  producers: [{ kind: 'producer', id: 'p1', ratePerSec: 5, keyStrategy: { type: 'null' } }],
  consumers: [{ kind: 'consumer', id: 'c1', groupId: 'g', autoOffsetReset: 'earliest' }],
  connections: [
    { from: 'p1', to: 'orders' },
    { from: 'orders', to: 'c1' },
  ],
};

describe('selectHeld (real engine)', () => {
  it('Kill, then Restart: the old member stays held until MemberRemoved', () => {
    const engine = createEngine({ seed: 3, topology });
    const removed: EngineEvent[] = [];
    engine.on((e) => {
      if (e.type === 'MemberRemoved') removed.push(e);
    });
    const heldAt = () => selectHeld('c1')({ ...INITIAL_SIM_STATE, snapshot: engine.getSnapshot() });

    engine.runUntil(4000);
    expect(heldAt()).toBeUndefined(); // a running member is never "held"
    engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    engine.runUntil(4500);
    engine.dispatch({ type: 'RestartConsumer', id: 'c1' });

    const deadline = heldAt()?.expiresAt ?? -1;
    expect(heldAt()?.labels).toBe('P0 P1');
    expect(deadline).toBeGreaterThan(4500);
    // Every 100 ms until the old member is removed: still held, though the new process runs
    // (and has joined as a new member, which must not count).
    let both = false;
    const gone = () => removed.length > 0; // the listener fills `removed` during runUntil
    for (let t = 4600; !gone() && t < 30_000; t += 100) {
      engine.runUntil(t);
      if (gone()) break;
      expect(engine.getSnapshot().consumers[0]?.status, `t=${String(t)}`).not.toBe('crashed');
      expect(heldAt()?.labels, `t=${String(t)}`).toBe('P0 P1');
      const c1s = engine.getSnapshot().groups[0]?.members.filter((m) => m.consumerId === 'c1');
      both ||= c1s?.length === 2;
    }
    expect(removed).toMatchObject([{ consumerId: 'c1', reason: 'session-timeout' }]);
    expect(removed[0]?.t).toBe(deadline);
    expect(heldAt()).toBeUndefined();
    // The new process joined as a second member while the old one was held, and stays.
    expect(both).toBe(true);
    const members = engine.getSnapshot().groups[0]?.members ?? [];
    expect(members.map((m) => m.consumerId)).toEqual(['c1']);
  });
});

import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import type { Command } from './commands/index.ts';
import { createEngine, replayCommandLog, type Engine } from './engine.ts';
import type { EngineEvent } from './events/index.ts';

function record(engine: Engine): EngineEvent[] {
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return events;
}

const oneBroker: TopologyInput = { brokers: [{ kind: 'broker', id: 'b1' }] };

describe('createEngine', () => {
  it('loads the initial topology without events or log entries, with defaults applied', () => {
    const engine = createEngine({
      seed: 1,
      topology: {
        ...oneBroker,
        topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
        producers: [{ kind: 'producer', id: 'p1' }],
        connections: [{ from: 'p1', to: 'orders' }],
      },
    });
    const snap = engine.getSnapshot();
    expect(snap.t).toBe(0);
    expect(snap.brokers).toEqual([{ config: { kind: 'broker', id: 'b1', rack: null } }]);
    expect(snap.topics[0]?.partitions.map((p) => [p.index, p.leader, p.logEndOffset])).toEqual([
      [0, 'b1', 0],
      [1, 'b1', 0],
      [2, 'b1', 0],
    ]);
    expect(snap.producers[0]?.config.acks).toBe('all');
    expect(snap.connections).toEqual([{ from: 'p1', to: 'orders' }]);
    expect(engine.commandLog).toEqual([]);
  });

  it('throws a readable error for an invalid initial topology', () => {
    expect(() =>
      createEngine({ seed: 1, topology: { topics: [{ kind: 'topic', id: 'orders' }] } }),
    ).toThrow(/Replication factor 1 is larger than the number of brokers \(0\)/);
  });

  it('dispatch applies a command at the current sim time, emits events, logs it with defaults', () => {
    const engine = createEngine({ seed: 1, topology: oneBroker });
    const events = record(engine);
    engine.runUntil(1500);
    expect(
      engine.dispatch({ type: 'AddComponent', component: { kind: 'topic', id: 'orders' } }),
    ).toEqual({ ok: true });
    const topic = {
      kind: 'topic',
      id: 'orders',
      partitions: 1,
      replicationFactor: 1,
      minInsyncReplicas: 1,
    };
    expect(events).toEqual([{ t: 1500, seq: 0, type: 'ComponentAdded', component: topic }]);
    expect(engine.commandLog).toEqual([
      { t: 1500, afterStep: 0, command: { type: 'AddComponent', component: topic } },
    ]);
  });

  it('connects and disconnects, following the shared connection rules', () => {
    const engine = createEngine({
      seed: 1,
      topology: {
        ...oneBroker,
        topics: [{ kind: 'topic', id: 'orders' }],
        producers: [{ kind: 'producer', id: 'p1' }],
        consumers: [{ kind: 'consumer', id: 'c1' }],
      },
    });
    const events = record(engine);
    expect(engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' })).toEqual({ ok: true });
    expect(engine.dispatch({ type: 'Connect', from: 'p1', to: 'c1' })).toMatchObject({
      ok: false,
      reason: /straight to a consumer/,
    });
    expect(engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' })).toEqual({
      ok: false,
      reason: 'p1 → orders is already connected.',
    });
    expect(engine.dispatch({ type: 'Disconnect', from: 'p1', to: 'orders' })).toEqual({ ok: true });
    expect(engine.dispatch({ type: 'Disconnect', from: 'p1', to: 'orders' })).toEqual({
      ok: false,
      reason: 'p1 → orders is not connected.',
    });
    expect(events.map((e) => e.type)).toEqual(['Connected', 'Disconnected']);
  });

  it('removing a component removes its connections', () => {
    const engine = createEngine({
      seed: 1,
      topology: {
        ...oneBroker,
        topics: [{ kind: 'topic', id: 'orders' }],
        producers: [{ kind: 'producer', id: 'p1' }],
        connections: [{ from: 'p1', to: 'orders' }],
      },
    });
    expect(engine.dispatch({ type: 'RemoveComponent', id: 'orders' })).toEqual({ ok: true });
    expect(engine.getSnapshot().connections).toEqual([]);
    expect(engine.getSnapshot().topics).toEqual([]);
  });

  it('rejects invalid commands with a readable reason, without logging or emitting', () => {
    const engine = createEngine({ seed: 1, topology: oneBroker });
    const events = record(engine);
    const cases: [Command, RegExp][] = [
      [{ type: 'AddComponent', component: { kind: 'producer', id: 'b1' } }, /already exists/],
      [{ type: 'AddComponent', component: { kind: 'topic', id: 'bad name' } }, /letters, digits/],
      [
        { type: 'AddComponent', component: { kind: 'topic', id: 't', replicationFactor: 2 } },
        /larger than the number of brokers \(1\)/,
      ],
      [{ type: 'RemoveComponent', id: 'nope' }, /no component with id "nope"/],
    ];
    for (const [command, reason] of cases) {
      expect(engine.dispatch(command)).toMatchObject({ ok: false, reason });
    }
    expect(engine.commandLog).toEqual([]);
    expect(events).toEqual([]);
  });

  it('enforces the M1 limits with an explanation', () => {
    const engine = createEngine({
      seed: 1,
      topology: {
        brokers: [
          { kind: 'broker', id: 'b1' },
          { kind: 'broker', id: 'b2' },
        ],
        topics: [{ kind: 'topic', id: 'orders' }],
        consumers: [{ kind: 'consumer', id: 'c1', groupId: 'g' }],
      },
    });
    expect(
      engine.dispatch({
        type: 'AddComponent',
        component: { kind: 'topic', id: 'replicated', replicationFactor: 2 },
      }),
    ).toMatchObject({ ok: false, reason: /replication factor must be 1/ });
    expect(
      engine.dispatch({
        type: 'AddComponent',
        component: { kind: 'consumer', id: 'c2', groupId: 'g' },
      }),
    ).toMatchObject({ ok: false, reason: /already has a member \(c1\)/ });
    const leader = engine.getSnapshot().topics[0]?.partitions[0]?.leader ?? '';
    expect(engine.dispatch({ type: 'RemoveComponent', id: leader })).toMatchObject({
      ok: false,
      reason: /leads partitions of topic "orders"/,
    });
  });

  it('getSnapshot() is plain data that survives a JSON round trip and does not alias state', () => {
    const engine = createEngine({ seed: 1, topology: oneBroker });
    const snap = engine.getSnapshot();
    expect(JSON.parse(JSON.stringify(snap))).toEqual(snap);
    engine.dispatch({ type: 'AddComponent', component: { kind: 'broker', id: 'b2' } });
    expect(snap.brokers).toHaveLength(1);
  });

  it('runUntil stops exactly at the boundary', () => {
    const engine = createEngine({ seed: 1 });
    engine.runUntil(999);
    expect(engine.now()).toBe(999);
    expect(engine.step()).toBe(false);
    expect(engine.now()).toBe(999);
  });
});

describe('command log and replay', () => {
  it('replaying the command log on a fresh engine reproduces the same event log', () => {
    const options = { seed: 42, topology: oneBroker };
    const original = createEngine(options);
    const originalEvents = record(original);
    original.dispatch({
      type: 'AddComponent',
      component: { kind: 'topic', id: 'orders', partitions: 3 },
    });
    original.runUntil(500);
    original.dispatch({ type: 'AddComponent', component: { kind: 'producer', id: 'p1' } });
    original.dispatch({ type: 'Connect', from: 'p1', to: 'orders' });
    original.dispatch({ type: 'Connect', from: 'p1', to: 'nowhere' }); // rejected, not logged
    original.runUntil(1200);
    original.dispatch({ type: 'RemoveComponent', id: 'p1' });
    original.runUntil(5000);

    const replayedEvents: EngineEvent[] = [];
    const replayed = replayCommandLog(options, original.commandLog, (e) => replayedEvents.push(e));
    replayed.runUntil(5000);

    expect(JSON.stringify(replayedEvents)).toBe(JSON.stringify(originalEvents));
    expect(replayed.commandLog).toEqual(original.commandLog);
    expect(replayed.getSnapshot()).toEqual(original.getSnapshot());
  });

  it('replay fails loudly when the log does not fit the engine', () => {
    expect(() =>
      replayCommandLog({ seed: 1 }, [
        { t: 0, afterStep: 3, command: { type: 'RemoveComponent', id: 'x' } },
      ]),
    ).toThrow(/diverged/);
    expect(() =>
      replayCommandLog({ seed: 1 }, [
        { t: 0, afterStep: 0, command: { type: 'RemoveComponent', id: 'ghost' } },
      ]),
    ).toThrow(/diverged/);
  });

  it('stores a copy, so mutating the dispatched object later does not change history', () => {
    const engine = createEngine({ seed: 1 });
    const component = { kind: 'broker' as const, id: 'b1' };
    engine.dispatch({ type: 'AddComponent', component });
    component.id = 'changed';
    expect(engine.commandLog[0]?.command).toEqual({
      type: 'AddComponent',
      component: { kind: 'broker', id: 'b1', rack: null },
    });
  });
});

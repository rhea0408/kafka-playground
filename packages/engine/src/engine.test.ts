import { describe, expect, it } from 'vitest';
import type { Command } from './commands';
import { createEngine, replayCommandLog, type Engine } from './engine';
import type { EngineEvent } from './events';

function record(engine: Engine): EngineEvent[] {
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return events;
}

const add = (kind: 'broker' | 'topic' | 'producer' | 'consumer', id: string): Command => ({
  type: 'AddComponent',
  component: { kind, id },
});

describe('createEngine', () => {
  it('dispatch applies a command at the current sim time and emits events', () => {
    const engine = createEngine({ seed: 1 });
    const events = record(engine);
    engine.runUntil(1500);
    expect(engine.dispatch(add('broker', 'b1'))).toEqual({ ok: true });
    expect(events).toEqual([
      { t: 1500, seq: 0, type: 'ComponentAdded', component: { kind: 'broker', id: 'b1' } },
    ]);
    expect(engine.getSnapshot().components).toEqual([{ kind: 'broker', id: 'b1' }]);
  });

  it('stamps accepted commands into the command log', () => {
    const engine = createEngine({ seed: 1 });
    engine.dispatch(add('topic', 'orders'));
    engine.runUntil(2000);
    engine.dispatch({ type: 'RemoveComponent', id: 'orders' });
    expect(engine.commandLog).toEqual([
      { t: 0, afterStep: 0, command: add('topic', 'orders') },
      { t: 2000, afterStep: 0, command: { type: 'RemoveComponent', id: 'orders' } },
    ]);
  });

  it('rejects invalid commands with a readable reason, without logging or emitting', () => {
    const engine = createEngine({
      seed: 1,
      topology: { components: [{ kind: 'broker', id: 'b1' }] },
    });
    const events = record(engine);
    expect(engine.dispatch(add('producer', 'b1'))).toEqual({
      ok: false,
      reason: 'A component with id "b1" already exists.',
    });
    expect(engine.dispatch({ type: 'RemoveComponent', id: 'nope' })).toEqual({
      ok: false,
      reason: 'There is no component with id "nope".',
    });
    expect(engine.commandLog).toEqual([]);
    expect(events).toEqual([]);
  });

  it('loads the initial topology without events or log entries, and rejects duplicate ids', () => {
    const engine = createEngine({
      seed: 1,
      topology: {
        components: [
          { kind: 'broker', id: 'b1' },
          { kind: 'topic', id: 'orders' },
        ],
      },
    });
    expect(engine.getSnapshot()).toEqual({
      t: 0,
      components: [
        { kind: 'broker', id: 'b1' },
        { kind: 'topic', id: 'orders' },
      ],
    });
    expect(() =>
      createEngine({
        seed: 1,
        topology: {
          components: [
            { kind: 'broker', id: 'x' },
            { kind: 'topic', id: 'x' },
          ],
        },
      }),
    ).toThrow(/duplicate/);
  });

  it('getSnapshot() is plain data that survives a JSON round trip and does not alias state', () => {
    const engine = createEngine({ seed: 1 });
    engine.dispatch(add('consumer', 'c1'));
    const snap = engine.getSnapshot();
    expect(JSON.parse(JSON.stringify(snap))).toEqual(snap);
    engine.dispatch({ type: 'RemoveComponent', id: 'c1' });
    expect(snap.components).toHaveLength(1);
  });

  it('runUntil stops exactly at the boundary', () => {
    const engine = createEngine({ seed: 1 });
    engine.runUntil(999);
    expect(engine.now()).toBe(999);
    expect(engine.step()).toBe(false);
    expect(engine.now()).toBe(999);
  });

  it('replaying the command log on a fresh engine reproduces the same event log', () => {
    const options = { seed: 42, topology: { components: [{ kind: 'broker' as const, id: 'b1' }] } };
    const original = createEngine(options);
    const originalEvents = record(original);
    original.dispatch(add('topic', 'orders'));
    original.runUntil(500);
    original.dispatch(add('producer', 'p1'));
    original.dispatch(add('consumer', 'c1'));
    original.runUntil(1200);
    original.dispatch({ type: 'RemoveComponent', id: 'p1' });
    original.dispatch(add('producer', 'p1'));
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
      replayCommandLog({ seed: 1 }, [{ t: 0, afterStep: 3, command: add('broker', 'b1') }]),
    ).toThrow(/diverged/);
    expect(() =>
      replayCommandLog({ seed: 1 }, [
        { t: 0, afterStep: 0, command: { type: 'RemoveComponent', id: 'ghost' } },
      ]),
    ).toThrow(/diverged/);
  });
});

describe('command log', () => {
  it('stores a copy, so mutating the dispatched object later does not change history', () => {
    const engine = createEngine({ seed: 1 });
    const command = {
      type: 'AddComponent' as const,
      component: { kind: 'broker' as const, id: 'b1' },
    };
    engine.dispatch(command);
    (command.component as { id: string }).id = 'changed';
    expect(engine.commandLog[0]?.command).toEqual(add('broker', 'b1'));
  });
});

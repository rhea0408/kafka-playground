import type { EngineSnapshot } from '@kp/engine';
import type { Component } from '@kp/schema';
import { beforeEach, describe, expect, it } from 'vitest';
import { SimHost } from '../sim/sim-host.ts';
import { applySimFrame, resetSimStore, useSimStore } from '../stores/sim-store.ts';
import {
  INITIAL_BROKER_ID,
  nextComponentId,
  resetTopologyStore,
  useTopologyStore,
  type TopologyState,
} from '../stores/topology-store.ts';
import { resetUiStore, select, useUiStore } from '../stores/ui-store.ts';
import { createActions, type SimPort } from './actions.ts';

const SEED = 7;

/** A real in-process SimHost (no Worker). It stays paused, so every accepted dispatch sends a frame. */
function setup() {
  resetTopologyStore();
  resetSimStore();
  resetUiStore();
  const host = new SimHost({ now: () => 0, scheduleTick: () => () => undefined });
  host.subscribe((frame) => {
    applySimFrame(frame);
  });
  const actions = createActions({ sim: host, seed: SEED });
  return { host, actions };
}

const topology = (): TopologyState => useTopologyStore.getState();
const toasts = () => useUiStore.getState().toasts;
const snapshot = (): EngineSnapshot => {
  const s = useSimStore.getState().snapshot;
  if (s === null) throw new Error('no snapshot yet');
  return s;
};

/** Every config the engine holds, by id (from the main thread's snapshot). */
function engineConfigs(s: EngineSnapshot): Component[] {
  return [
    ...s.brokers.map((b) => b.config),
    ...s.topics.map((t) => t.config),
    ...s.producers.map((p) => p.config),
    ...s.consumers.map((c) => c.config),
  ].sort((a, b) => a.id.localeCompare(b.id));
}

/** The canvas and the engine agree on components and connections. */
function expectInSync(): void {
  const canvas = [...topology().components].sort((a, b) => a.id.localeCompare(b.id));
  expect(canvas).toEqual(engineConfigs(snapshot()));
  expect(new Set(topology().connections.map((c) => `${c.from}>${c.to}`))).toEqual(
    new Set(snapshot().connections.map((c) => `${c.from}>${c.to}`)),
  );
}

/** producer p1 → topic t1 → consumer c1. */
async function buildFlow(actions: ReturnType<typeof setup>['actions']): Promise<void> {
  expect(await actions.addComponent('topic', { x: 300, y: 100 }, { partitions: 3 })).toEqual({
    ok: true,
    id: 't1',
  });
  expect(await actions.addComponent('producer', { x: 50, y: 100 })).toEqual({ ok: true, id: 'p1' });
  expect(await actions.addComponent('consumer', { x: 600, y: 100 })).toEqual({
    ok: true,
    id: 'c1',
  });
  expect((await actions.connect('p1', 't1')).ok).toBe(true);
  expect((await actions.connect('t1', 'c1')).ok).toBe(true);
}

describe('actions → SimHost round trips', () => {
  let ctx: ReturnType<typeof setup>;
  beforeEach(async () => {
    ctx = setup();
    expect(await ctx.actions.init()).toEqual({ ok: true });
  });

  it('init starts the engine from the canvas and feeds simStore a full frame', () => {
    expect(snapshot().brokers.map((b) => b.config.id)).toEqual([INITIAL_BROKER_ID]);
    expectInSync();
  });

  it('accepted add stores the defaulted config and the position', async () => {
    const result = await ctx.actions.addComponent('consumer', { x: 10, y: 20 });
    expect(result).toEqual({ ok: true, id: 'c1' });
    const consumer = topology().components.find((c) => c.id === 'c1');
    // Schema defaults are filled in, as the engine has them.
    expect(consumer).toMatchObject({ groupId: 'c1-group', autoOffsetReset: 'latest' });
    expect(topology().positions['c1']).toEqual({ x: 10, y: 20 });
    expectInSync();
    expect(toasts()).toEqual([]);
  });

  it('builds producer → topic → consumer with every store in sync', async () => {
    await buildFlow(ctx.actions);
    expect(topology().connections).toEqual([
      { from: 'p1', to: 't1' },
      { from: 't1', to: 'c1' },
    ]);
    expectInSync();
    expect(ctx.host.commandLog().map((e) => e.command.type)).toEqual([
      'AddComponent',
      'AddComponent',
      'AddComponent',
      'Connect',
      'Connect',
    ]);
  });

  it('a rejected connect leaves the canvas unchanged and shows the engine reason', async () => {
    await buildFlow(ctx.actions);
    const before = topology();
    const result = await ctx.actions.connect('c1', 't1');
    expect(result.ok).toBe(false);
    expect(topology()).toBe(before);
    expect(toasts()).toEqual([
      {
        id: expect.any(Number) as number,
        kind: 'warning',
        message: 'Consumers read from topics. Connect topic → consumer instead.',
      },
    ]);
  });

  it('a rejected add leaves the canvas unchanged and shows the reason', async () => {
    const before = topology();
    const result = await ctx.actions.addComponent(
      'topic',
      { x: 0, y: 0 },
      { replicationFactor: 3 },
    );
    expect(result.ok).toBe(false);
    expect(topology()).toBe(before);
    expect(toasts()[0]?.message).toMatch(
      /Replication factor 3 is larger than the number of brokers/,
    );
  });

  it('updateConfig: accepted increase is stored, a decrease is refused with Kafka’s reason', async () => {
    await buildFlow(ctx.actions);
    expect(await ctx.actions.updateConfig('t1', { partitions: 4 })).toEqual({ ok: true });
    expect(topology().components.find((c) => c.id === 't1')).toMatchObject({ partitions: 4 });
    expectInSync();

    const before = topology();
    const result = await ctx.actions.updateConfig('t1', { partitions: 2 });
    expect(result.ok).toBe(false);
    expect(topology()).toBe(before);
    expect(toasts()).toHaveLength(1);
    expect(toasts()[0]?.message).toMatch(/never remove them/);
  });

  it('removing a connected component sends one RemoveComponent and drops its edges', async () => {
    await buildFlow(ctx.actions);
    select({ nodeIds: ['t1', 'p1'], edgeIds: [] });
    expect(await ctx.actions.removeComponent('t1')).toEqual({ ok: true });
    expect(topology().components.map((c) => c.id)).toEqual(['b1', 'p1', 'c1']);
    expect(topology().connections).toEqual([]);
    expect(topology().positions['t1']).toBeUndefined();
    expect(useUiStore.getState().selection.nodeIds).toEqual(['p1']);
    expect(ctx.host.commandLog().at(-1)?.command).toEqual({ type: 'RemoveComponent', id: 't1' });
    expectInSync();
  });

  it('moveNode changes positions only and sends nothing', async () => {
    await buildFlow(ctx.actions);
    const commands = ctx.host.commandLog().length;
    const frame = useSimStore.getState().frame;
    ctx.actions.moveNode('p1', { x: 1, y: 2 });
    expect(topology().positions['p1']).toEqual({ x: 1, y: 2 });
    expect(ctx.host.commandLog()).toHaveLength(commands);
    expect(useSimStore.getState().frame).toBe(frame);
  });

  it('produce manual, kill and restart reach the engine; a second kill is refused', async () => {
    await buildFlow(ctx.actions);
    expect(await ctx.actions.produceManual('p1', 'D', 'hello')).toEqual({ ok: true });
    expect(useSimStore.getState().events.some((e) => e.type === 'RecordProduced')).toBe(true);

    expect(await ctx.actions.killConsumer('c1')).toEqual({ ok: true });
    expect(snapshot().consumers[0]?.status).toBe('stopped');
    expect((await ctx.actions.killConsumer('c1')).ok).toBe(false);
    expect(toasts()[0]?.message).toBe('Consumer c1 is already stopped.');
    expect(await ctx.actions.restartConsumer('c1')).toEqual({ ok: true });
    expect(snapshot().consumers[0]?.status).toBe('running');
  });

  it('sim controls: play/pause/step/speed show up in frames; a bad speed is refused', async () => {
    await buildFlow(ctx.actions);
    await ctx.actions.setSpeed(2);
    expect(useSimStore.getState().speed).toBe(2);
    await ctx.actions.play();
    expect(useSimStore.getState().playing).toBe(true);
    await ctx.actions.pause();
    expect(useSimStore.getState().playing).toBe(false);
    const stepped = await ctx.actions.step();
    expect(stepped?.stepped).toBe(true);
    expect(useSimStore.getState().t).toBe(stepped?.t);

    expect((await ctx.actions.setSpeed(20)).ok).toBe(false);
    expect(useSimStore.getState().speed).toBe(2);
    expect(toasts()[0]?.message).toMatch(/Speed must be between/);
  });

  it('reset restarts the sim at t = 0 with the canvas as it is now', async () => {
    await buildFlow(ctx.actions);
    await ctx.actions.produceManual('p1', 'D', 'x');
    ctx.host.stepBy(1000);
    expect(snapshot().t).toBe(1000);

    expect(await ctx.actions.reset()).toEqual({ ok: true });
    expect(snapshot().t).toBe(0);
    expect(useSimStore.getState().events).toEqual([]);
    expect(ctx.host.commandLog()).toEqual([]);
    expectInSync(); // the topic, producer, consumer and both connections are still there
    expect(snapshot().topics[0]?.partitions.every((p) => p.logEndOffset === 0)).toBe(true);
  });
});

describe('actions with a failing port', () => {
  it('turns a thrown error (e.g. a dead worker) into a refusal and a toast', async () => {
    resetTopologyStore();
    resetUiStore();
    const fail = (): never => {
      throw new Error('worker terminated');
    };
    const port: SimPort = {
      init: fail,
      dispatch: () => Promise.reject(new Error('worker terminated')),
      play: fail,
      pause: fail,
      step: fail,
      setSpeed: fail,
    };
    const actions = createActions({ sim: port, seed: SEED });
    const before = topology();
    expect(await actions.addComponent('topic', { x: 0, y: 0 })).toEqual({
      ok: false,
      reason: 'worker terminated',
    });
    expect(await actions.step()).toBeNull();
    await actions.play();
    expect(topology()).toBe(before);
    expect(toasts().map((t) => t.message)).toEqual([
      'worker terminated',
      'worker terminated',
      'worker terminated',
    ]);
  });
});

describe('nextComponentId', () => {
  it('uses the kind prefix and the next number after the highest in use', () => {
    resetTopologyStore();
    const state = topology();
    expect(nextComponentId(state, 'broker')).toBe('b2');
    expect(nextComponentId(state, 'topic')).toBe('t1');
    const withGaps: TopologyState = {
      ...state,
      components: [
        ...state.components,
        { kind: 'broker', id: 'b7', rack: null },
        { kind: 'topic', id: 'orders', partitions: 1, replicationFactor: 1, minInsyncReplicas: 1 },
      ],
    };
    expect(nextComponentId(withGaps, 'broker')).toBe('b8');
    expect(nextComponentId(withGaps, 'topic')).toBe('t1');
  });
});

/**
 * The command path (architecture §5, CLAUDE.md rule 3): every canvas edit and sim control goes
 * through here. Edits become engine commands; `topologyStore` changes only after the engine
 * accepted one, so the canvas never shows something the engine refused. A refusal shows a warning
 * toast with the engine's reason.
 *
 * The module talks to a `SimPort`: the worker client in the app, an in-process `SimHost` in tests.
 */

import type { Command, ConfigPatch, DispatchResult } from '@kp/engine';
import { parseComponent, type Component, type ComponentKind } from '@kp/schema';
import type { SimApi, StepResult } from '../sim/protocol.ts';
import {
  findComponent,
  nextComponentId,
  toTopologyInput,
  useTopologyStore,
  withComponentAdded,
  withComponentRemoved,
  withComponentReplaced,
  withConnection,
  withoutConnection,
  withPosition,
  type TopologyState,
  type XYPosition,
} from '../stores/topology-store.ts';
import { deselect, pushToast } from '../stores/ui-store.ts';

type Promisable<T> = T | Promise<T>;
type PortMethod<K extends keyof SimApi> = (
  ...args: Parameters<SimApi[K]>
) => Promisable<ReturnType<SimApi[K]>>;

/** The part of the sim API the actions use, sync (SimHost) or async (Comlink `Remote<SimApi>`). */
export interface SimPort {
  readonly init: PortMethod<'init'>;
  readonly dispatch: PortMethod<'dispatch'>;
  readonly play: PortMethod<'play'>;
  readonly pause: PortMethod<'pause'>;
  readonly step: PortMethod<'step'>;
  readonly setSpeed: PortMethod<'setSpeed'>;
}

export type AddResult =
  { readonly ok: true; readonly id: string } | { readonly ok: false; readonly reason: string };

/** Config for a new component: anything but `kind`; `id` is generated when left out. */
export type NewComponentConfig = Readonly<Record<string, unknown>> & { readonly id?: string };

export interface Actions {
  /** Starts the engine from the current canvas. Also what Reset does. */
  readonly init: () => Promise<DispatchResult>;
  readonly addComponent: (
    kind: ComponentKind,
    position: XYPosition,
    config?: NewComponentConfig,
  ) => Promise<AddResult>;
  readonly removeComponent: (id: string) => Promise<DispatchResult>;
  readonly connect: (from: string, to: string) => Promise<DispatchResult>;
  readonly disconnect: (from: string, to: string) => Promise<DispatchResult>;
  readonly updateConfig: (id: string, patch: ConfigPatch) => Promise<DispatchResult>;
  /** UI only: positions never reach the engine. */
  readonly moveNode: (id: string, position: XYPosition) => void;
  readonly produceManual: (
    producerId: string,
    key: string | null,
    value: string,
  ) => Promise<DispatchResult>;
  readonly killConsumer: (id: string) => Promise<DispatchResult>;
  readonly restartConsumer: (id: string) => Promise<DispatchResult>;
  readonly play: () => Promise<void>;
  readonly pause: () => Promise<void>;
  readonly step: () => Promise<StepResult | null>;
  readonly setSpeed: (speed: number) => Promise<DispatchResult>;
  /** Sim state back to t = 0 with the canvas as it is now (see M2-decisions T5). */
  readonly reset: () => Promise<DispatchResult>;
}

export interface ActionsOptions {
  readonly sim: SimPort;
  readonly seed: number;
}

const errorReason = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

export function createActions({ sim, seed }: ActionsOptions): Actions {
  const topology = (): TopologyState => useTopologyStore.getState();
  const setTopology = (state: TopologyState): void => {
    useTopologyStore.setState(state, true);
  };

  /** Shows a refusal (or a failed call) as a warning toast and passes the result on. */
  const report = (result: DispatchResult): DispatchResult => {
    if (!result.ok) pushToast('warning', result.reason);
    return result;
  };

  /** Runs a host call; an exception (e.g. the worker died) becomes a refusal. */
  const call = async <T>(run: () => Promisable<T>): Promise<T | { ok: false; reason: string }> => {
    try {
      return await run();
    } catch (error) {
      return { ok: false, reason: errorReason(error) };
    }
  };

  /** Sends a command; on success applies `onOk` to the topology store. */
  const send = async (
    command: Command,
    onOk?: (state: TopologyState) => TopologyState,
  ): Promise<DispatchResult> => {
    const result = report(await call(() => sim.dispatch(command)));
    if (result.ok && onOk !== undefined) setTopology(onOk(topology()));
    return result;
  };

  const init = async (): Promise<DispatchResult> =>
    report(await call(() => sim.init({ seed, topology: toTopologyInput(topology()) })));

  return {
    init,
    reset: init,

    addComponent: async (kind, position, config = {}) => {
      const id = config.id ?? nextComponentId(topology(), kind);
      const input = { ...config, kind, id };
      const result = await send({ type: 'AddComponent', component: input as Component });
      if (!result.ok) return result;
      // The engine accepted the same input, so parsing (to fill in defaults) cannot fail.
      const parsed = parseComponent(input);
      if (!parsed.ok)
        throw new Error(`Accepted component failed to parse: ${parsed.errors.join(' ')}`);
      setTopology(withComponentAdded(topology(), parsed.value, position));
      return { ok: true, id };
    },

    // One RemoveComponent: the engine drops the component's connections in the same step
    // (commands/apply.ts), so the UI drops its edges locally instead of sending Disconnects.
    removeComponent: async (id) => {
      const result = await send({ type: 'RemoveComponent', id }, (s) =>
        withComponentRemoved(s, id),
      );
      if (result.ok) deselect(id);
      return result;
    },

    connect: (from, to) =>
      send({ type: 'Connect', from, to }, (s) => withConnection(s, { from, to })),

    disconnect: (from, to) =>
      send({ type: 'Disconnect', from, to }, (s) => withoutConnection(s, { from, to })),

    updateConfig: async (id, patch) => {
      const result = await send({ type: 'UpdateConfig', id, patch });
      if (!result.ok) return result;
      const current = findComponent(topology(), id);
      if (current === undefined) return result;
      // Same merge as the engine (commands/update-config.ts): current config + patch, re-parsed.
      const parsed = parseComponent({ ...current, ...patch, id, kind: current.kind });
      if (!parsed.ok)
        throw new Error(`Accepted update failed to parse: ${parsed.errors.join(' ')}`);
      setTopology(withComponentReplaced(topology(), parsed.value));
      return result;
    },

    moveNode: (id, position) => {
      if (findComponent(topology(), id) === undefined) return;
      setTopology(withPosition(topology(), id, position));
    },

    produceManual: (producerId, key, value) =>
      send({ type: 'ProduceManual', producerId, key, value }),
    killConsumer: (id) => send({ type: 'KillConsumer', id }),
    restartConsumer: (id) => send({ type: 'RestartConsumer', id }),

    play: async () => {
      report(
        await call(async () => {
          await sim.play();
          return { ok: true } as const;
        }),
      );
    },
    pause: async () => {
      report(
        await call(async () => {
          await sim.pause();
          return { ok: true } as const;
        }),
      );
    },
    step: async () => {
      const result = await call(() => sim.step());
      if ('ok' in result) {
        report(result);
        return null;
      }
      return result;
    },
    setSpeed: async (speed) => report(await call(() => sim.setSpeed(speed))),
  };
}

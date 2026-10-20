/**
 * createEngine: the public entry point. Wraps the kernel (clock, queue, RNG, event stream) with
 * engine state, command handling and the command log (architecture §4.1, §4.4).
 */

import type { Command, CommandLogEntry, ComponentRef, DispatchResult } from './commands';
import { assertNever } from './core/assert-never';
import type { SimTime } from './core/clock';
import { createKernel, type Kernel } from './core/kernel';
import type { EngineEvent, EngineEventPayload } from './events';

/** Scheduled engine work. Empty until producers and consumers arrive (M1-T6, T7). */
type EngineTask = never;

export interface EngineOptions {
  readonly seed: number;
  /** Initial components. Loaded at t=0 without events or command-log entries. */
  readonly topology?: { readonly components: readonly ComponentRef[] };
}

/** Minimal snapshot; expanded in M1-T8. Plain data, safe to JSON-serialise or postMessage. */
export interface EngineSnapshot {
  readonly t: SimTime;
  readonly components: readonly ComponentRef[];
}

export interface Engine {
  /** Validates and applies a command at the current sim time. Accepted commands are logged. */
  readonly dispatch: (command: Command) => DispatchResult;
  /** Processes the next scheduled task. Returns false when nothing is queued. */
  readonly step: () => boolean;
  /** Processes every task scheduled at or before `t`, then sets the clock to exactly `t`. */
  readonly runUntil: (t: SimTime) => void;
  readonly now: () => SimTime;
  /** Number of scheduled tasks processed so far (recorded in the command log as `afterStep`). */
  readonly stepsProcessed: () => number;
  readonly getSnapshot: () => EngineSnapshot;
  readonly on: (listener: (event: EngineEvent) => void) => () => void;
  readonly commandLog: readonly CommandLogEntry[];
}

export function createEngine(options: EngineOptions): Engine {
  return createEngineInternal(options).engine;
}

function createEngineInternal(options: EngineOptions): {
  engine: Engine;
  kernel: Kernel<EngineTask, EngineEventPayload>;
} {
  const kernel = createKernel<EngineTask, EngineEventPayload>({
    seed: options.seed,
    handleTask: (task) => {
      assertNever(task);
    },
  });

  // Map iteration follows insertion order, so it is deterministic for a given command log.
  const components = new Map<string, ComponentRef>();
  for (const component of options.topology?.components ?? []) {
    if (components.has(component.id)) {
      throw new Error(`duplicate component id in topology: "${component.id}"`);
    }
    components.set(component.id, component);
  }

  const commandLog: CommandLogEntry[] = [];

  const apply = (command: Command): DispatchResult => {
    switch (command.type) {
      case 'AddComponent': {
        const { component } = command;
        if (components.has(component.id)) {
          return { ok: false, reason: `A component with id "${component.id}" already exists.` };
        }
        components.set(component.id, { ...component });
        kernel.emit({ type: 'ComponentAdded', component: { ...component } });
        return { ok: true };
      }
      case 'RemoveComponent': {
        const component = components.get(command.id);
        if (component === undefined) {
          return { ok: false, reason: `There is no component with id "${command.id}".` };
        }
        components.delete(command.id);
        kernel.emit({ type: 'ComponentRemoved', component });
        return { ok: true };
      }
      default:
        return assertNever(command);
    }
  };

  const engine: Engine = {
    dispatch: (command) => {
      const entry: CommandLogEntry = {
        t: kernel.now(),
        afterStep: kernel.stepsProcessed(),
        // Commands are plain JSON data; copy so a caller mutating its object cannot rewrite history.
        command: JSON.parse(JSON.stringify(command)) as Command,
      };
      const result = apply(command);
      if (result.ok) commandLog.push(entry);
      return result;
    },
    step: kernel.step,
    runUntil: kernel.runUntil,
    now: kernel.now,
    stepsProcessed: kernel.stepsProcessed,
    getSnapshot: () => ({
      t: kernel.now(),
      components: [...components.values()].map((c) => ({ ...c })),
    }),
    on: kernel.on,
    commandLog,
  };
  return { engine, kernel };
}

/**
 * Builds a fresh engine and replays `log` on it. For each entry it processes exactly `afterStep`
 * tasks, moves the clock to `t` without running tasks due at `t`, then dispatches the command.
 * With the same options this reproduces the original run event for event (NFR-4.1). Listeners
 * passed in `onEvent` see every replayed event, including those emitted before the first command.
 */
export function replayCommandLog(
  options: EngineOptions,
  log: readonly CommandLogEntry[],
  onEvent?: (event: EngineEvent) => void,
): Engine {
  const { engine, kernel } = createEngineInternal(options);
  if (onEvent !== undefined) engine.on(onEvent);
  for (const [index, entry] of log.entries()) {
    while (kernel.stepsProcessed() < entry.afterStep && kernel.step()) {
      // step() does the work
    }
    if (kernel.stepsProcessed() !== entry.afterStep) {
      throw new Error(
        `replay diverged at entry ${String(index)}: expected ${String(entry.afterStep)} steps, ran out at ${String(kernel.stepsProcessed())}`,
      );
    }
    kernel.advanceTo(entry.t);
    const result = engine.dispatch(entry.command);
    if (!result.ok) {
      throw new Error(`replay diverged at entry ${String(index)}: ${result.reason}`);
    }
  }
  return engine;
}

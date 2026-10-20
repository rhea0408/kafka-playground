/**
 * createEngine: the public entry point. Wraps the kernel (clock, queue, RNG, event stream) with
 * engine state, command handling and the command log (architecture §4.1, §4.4).
 */

import { parseTopology, type TopologyInput } from '@kp/schema';
import { applyCommand } from './commands/apply.ts';
import type { Command, CommandLogEntry, DispatchResult, LoggedCommand } from './commands/index.ts';
import { assertSimTime, type SimTime } from './core/clock.ts';
import { createKernel, type Kernel } from './core/kernel.ts';
import type { EngineEvent, EngineEventPayload } from './events/index.ts';
import type { EngineContext } from './model/context.ts';
import { createState } from './model/state.ts';
import type { EngineTask } from './model/tasks.ts';
import { runTask } from './protocols/index.ts';
import { buildSnapshot, type EngineSnapshot } from './snapshot/index.ts';

export interface EngineOptions {
  readonly seed: number;
  /** Initial components and connections. Loaded at t=0 without events or command-log entries. */
  readonly topology?: TopologyInput;
  /** How many recent records per partition snapshots include. Default 20. */
  readonly snapshotRecordsPerPartition?: number;
  /** Fixed one-way client↔broker network latency in sim ms. Default 1. */
  readonly networkDelayMs?: number;
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
  const networkDelayMs = options.networkDelayMs ?? 1;
  assertSimTime(networkDelayMs);
  let loading = true;
  const kernel = createKernel<EngineTask, EngineEventPayload>({
    seed: options.seed,
    // Tasks only run from step()/runUntil(), after `context` below is initialised.
    handleTask: (task) => {
      runTask(context, task);
    },
  });
  const context: EngineContext = {
    kernel,
    state: createState(),
    emit: (event) => {
      if (!loading) kernel.emit(event);
    },
    config: { networkDelayMs },
  };

  loadTopology(context, options.topology ?? {});
  loading = false;

  const commandLog: CommandLogEntry[] = [];
  const recordsPerPartition = options.snapshotRecordsPerPartition ?? 20;

  const engine: Engine = {
    dispatch: (command) => {
      const t = kernel.now();
      const afterStep = kernel.stepsProcessed();
      const result = applyCommand(context, command);
      if (!result.ok) return result;
      // Plain JSON copy, so a caller mutating its object later cannot rewrite history.
      const logged = JSON.parse(JSON.stringify(result.logged)) as LoggedCommand;
      commandLog.push({ t, afterStep, command: logged });
      return { ok: true };
    },
    step: kernel.step,
    runUntil: kernel.runUntil,
    now: kernel.now,
    stepsProcessed: kernel.stepsProcessed,
    getSnapshot: () => buildSnapshot(context.state, kernel.now(), { recordsPerPartition }),
    on: kernel.on,
    commandLog,
  };
  return { engine, kernel };
}

/** Builds the initial topology through the same handlers as commands, so the same rules apply. */
function loadTopology(ctx: EngineContext, input: TopologyInput): void {
  const parsed = parseTopology(input);
  if (!parsed.ok) throw new Error(`Invalid topology:\n${parsed.errors.join('\n')}`);
  const { brokers, topics, producers, consumers, connections } = parsed.value;
  const commands: Command[] = [
    ...[...brokers, ...topics, ...producers, ...consumers].map((component): Command => ({
      type: 'AddComponent',
      component,
    })),
    ...connections.map(({ from, to }): Command => ({ type: 'Connect', from, to })),
  ];
  for (const command of commands) {
    const result = applyCommand(ctx, command);
    if (!result.ok) throw new Error(`Invalid topology: ${result.reason}`);
  }
}

/**
 * Builds a fresh engine and replays `log` on it. For each entry it processes exactly `afterStep`
 * tasks, moves the clock to `t` without running tasks due at `t`, then dispatches the command.
 * With the same options this reproduces the original run event for event (NFR-4.1). `onEvent`
 * sees every replayed event, including those emitted before the first command.
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

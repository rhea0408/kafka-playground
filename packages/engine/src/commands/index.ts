/**
 * Commands: the only way the UI (or a level script, or a replay) changes engine state
 * (CLAUDE.md rule 3, architecture §4.4). A discriminated union on `type`.
 *
 * Pause / SetSpeed / Step are not engine commands: they only change how fast the worker drives
 * the loop, never the simulated outcome, so they are not logged.
 */

import type { Component, ComponentInput } from '@kp/schema';
import type { SimTime } from '../core/clock.ts';

export type Command =
  /** Adds a component. Optional config fields take their defaults. */
  | { readonly type: 'AddComponent'; readonly component: ComponentInput }
  /** Removes a component and its connections. */
  | { readonly type: 'RemoveComponent'; readonly id: string }
  /** Connects two components (producer → topic or topic → consumer). */
  | { readonly type: 'Connect'; readonly from: string; readonly to: string }
  | { readonly type: 'Disconnect'; readonly from: string; readonly to: string };

export type CommandType = Command['type'];

/**
 * A command as stored in the log: AddComponent carries the fully-defaulted component, so a saved
 * log replays the same way even if a default changes later.
 */
export type LoggedCommand =
  | { readonly type: 'AddComponent'; readonly component: Component }
  | Exclude<Command, { readonly type: 'AddComponent' }>;

/**
 * An accepted command, stamped for replay. `afterStep` is the number of queue tasks processed
 * before the command was applied. `t` alone is not enough: several tasks can share one sim time,
 * and a command dispatched between two of them must be replayed between the same two.
 */
export interface CommandLogEntry {
  readonly t: SimTime;
  readonly afterStep: number;
  readonly command: LoggedCommand;
}

export type DispatchResult =
  { readonly ok: true } | { readonly ok: false; readonly reason: string };

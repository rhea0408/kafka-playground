/**
 * Commands: the only way the UI (or a level script, or a replay) changes engine state
 * (CLAUDE.md rule 3, architecture §4.4). A discriminated union on `type`.
 *
 * M1-T2 starts with adding and removing components; later tasks add UpdateConfig, Connect,
 * ProduceManual, KillBroker and so on. Pause / SetSpeed / Step are not engine commands: they only
 * change how fast the worker drives the loop, never the simulated outcome, so they are not logged.
 */

import type { ComponentKind } from '@kp/schema';
import type { SimTime } from '../core/clock.ts';

export interface ComponentRef {
  readonly kind: ComponentKind;
  readonly id: string;
}

export type Command =
  | { readonly type: 'AddComponent'; readonly component: ComponentRef }
  | { readonly type: 'RemoveComponent'; readonly id: string };

export type CommandType = Command['type'];

/**
 * An accepted command, stamped for replay. `afterStep` is the number of queue tasks processed
 * before the command was applied. `t` alone is not enough: several tasks can share one sim time,
 * and a command dispatched between two of them must be replayed between the same two.
 */
export interface CommandLogEntry {
  readonly t: SimTime;
  readonly afterStep: number;
  readonly command: Command;
}

export type DispatchResult =
  { readonly ok: true } | { readonly ok: false; readonly reason: string };

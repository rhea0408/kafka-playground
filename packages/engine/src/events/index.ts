/**
 * Domain events: the single stream the engine emits (architecture §4.5, FR-2.16, NFR-8.4). The UI,
 * animations, tips, objectives and level runtime all consume it. A discriminated union on `type`.
 * The kernel stamps each event with `{ t, seq }` when it is emitted.
 */

import type { Component } from '@kp/schema';
import type { Stamped } from '../core/kernel.ts';

export type EngineEventPayload =
  | { readonly type: 'ComponentAdded'; readonly component: Component }
  | { readonly type: 'ComponentRemoved'; readonly component: Component }
  | { readonly type: 'Connected'; readonly from: string; readonly to: string }
  | { readonly type: 'Disconnected'; readonly from: string; readonly to: string };

export type EngineEvent = Stamped<EngineEventPayload>;

export type EngineEventType = EngineEvent['type'];

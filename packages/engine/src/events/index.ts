/**
 * Domain events: the single stream the engine emits (architecture §4.5, FR-2.16, NFR-8.4). The UI,
 * animations, tips, objectives and level runtime all consume it. A discriminated union on `type`.
 * The kernel stamps each event with `{ t, seq }` when it is emitted.
 *
 * M1-T2 starts with component lifecycle events; RecordProduced, RecordAppended and the rest are
 * added by the tasks that model them.
 */

import type { ComponentRef } from '../commands/index.ts';
import type { Stamped } from '../core/kernel.ts';

export type EngineEventPayload =
  | { readonly type: 'ComponentAdded'; readonly component: ComponentRef }
  | { readonly type: 'ComponentRemoved'; readonly component: ComponentRef };

export type EngineEvent = Stamped<EngineEventPayload>;

export type EngineEventType = EngineEvent['type'];

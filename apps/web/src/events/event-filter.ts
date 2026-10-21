/**
 * Event list filter (B7, FR-6.3): which engine event types to show. Pure: unit-tested in Node.
 *
 * The filter stores the *hidden* types, so an event type added to the engine later shows up by
 * default instead of being silently filtered out.
 */

import type { EngineEvent, EngineEventType } from '@kp/engine';

export interface EventTypeGroup {
  readonly label: string;
  readonly types: readonly EngineEventType[];
}

/** Every engine event type, grouped the way a learner thinks about them. */
const GROUPS = [
  {
    label: 'Records',
    types: ['RecordProduced', 'RecordAppended', 'ProduceAcked', 'ProduceFailed', 'RecordConsumed'],
  },
  { label: 'Offsets', types: ['OffsetCommitted'] },
  { label: 'Consumers', types: ['ConsumerStarted', 'ConsumerStopped'] },
  {
    label: 'Topology',
    types: ['ComponentAdded', 'ComponentRemoved', 'Connected', 'Disconnected', 'ConfigUpdated'],
  },
] as const satisfies readonly EventTypeGroup[];

export const EVENT_TYPE_GROUPS: readonly EventTypeGroup[] = GROUPS;

export const ALL_EVENT_TYPES: readonly EngineEventType[] = EVENT_TYPE_GROUPS.flatMap(
  (g) => g.types,
);

// Compile-time check: a new engine event type must be added to a group above.
type Missing = Exclude<EngineEventType, (typeof GROUPS)[number]['types'][number]>;
export const EVERY_TYPE_GROUPED: [Missing] extends [never] ? true : Missing = true;

export type HiddenTypes = ReadonlySet<EngineEventType>;

export const NO_HIDDEN_TYPES: HiddenTypes = new Set();

/** The events to show, oldest first. Returns `events` itself when nothing is hidden. */
export function filterEvents(
  events: readonly EngineEvent[],
  hidden: HiddenTypes,
): readonly EngineEvent[] {
  if (hidden.size === 0) return events;
  return events.filter((e) => !hidden.has(e.type));
}

/** Shows or hides one type. */
export function toggleType(hidden: HiddenTypes, type: EngineEventType): HiddenTypes {
  const next = new Set(hidden);
  if (next.has(type)) next.delete(type);
  else next.add(type);
  return next;
}

/** Shows or hides every type of a group at once. */
export function setGroup(
  hidden: HiddenTypes,
  types: readonly EngineEventType[],
  show: boolean,
): HiddenTypes {
  const next = new Set(hidden);
  for (const type of types) {
    if (show) next.delete(type);
    else next.add(type);
  }
  return next;
}

/** "All types", "12 of 13 types", "No types" for the filter button. */
export function filterSummary(hidden: HiddenTypes): string {
  const shown = ALL_EVENT_TYPES.filter((t) => !hidden.has(t)).length;
  if (shown === ALL_EVENT_TYPES.length) return 'All types';
  if (shown === 0) return 'No types';
  return `${String(shown)} of ${String(ALL_EVENT_TYPES.length)} types`;
}

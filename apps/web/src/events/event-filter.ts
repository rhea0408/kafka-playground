/**
 * Event list filter (B7, FR-6.3): which engine event types to show. Pure: unit-tested in Node.
 *
 * The filter stores the *hidden* types, so an event type added to the engine later shows up by
 * default instead of being silently filtered out.
 *
 * Separately, events about internal topics (__consumer_offsets, M4-T8) are hidden unless the
 * "Show internal topics" toggle is on (M4 follow-up G-1): a single broker restart elects a leader
 * for each of its 50 partitions, which would bury the player's own topic. Events without a topic
 * (CoordinatorMoved, Broker*) always pass this check.
 */

import { isInternalTopic, type EngineEvent, type EngineEventType } from '@kp/engine';

export interface EventTypeGroup {
  readonly label: string;
  readonly types: readonly EngineEventType[];
}

/** Every engine event type, grouped the way a learner thinks about them. */
const GROUPS = [
  {
    label: 'Records',
    types: [
      'RecordProduced',
      'RecordAppended',
      'ProduceAcked',
      'ProduceRetried',
      'ProduceFailed',
      'RecordConsumed',
    ],
  },
  // Commits and what they mean for delivery (FR-2.8): duplicates and skipped records.
  {
    label: 'Commit',
    types: ['OffsetCommitted', 'CommitFailed', 'DuplicateProcessed', 'RecordsSkipped'],
  },
  // Group membership and rebalancing (FR-2.7), named after the rebalance listener callbacks.
  {
    label: 'Group',
    types: [
      'MemberJoined',
      'MemberRemoved',
      'RebalanceStarted',
      'RebalanceCompleted',
      'PartitionsAssigned',
      'PartitionsRevoked',
      'PartitionsLost',
      'ConsumerIdle',
      'CoordinatorMoved',
    ],
  },
  { label: 'Consumers', types: ['ConsumerStarted', 'ConsumerStopped'] },
  // Broker processes (FR-3.1, M4): crashes, controlled shutdowns and restarts.
  { label: 'Broker', types: ['BrokerStarted', 'BrokerStopped', 'BrokerFenced', 'BrokerUnfenced'] },
  // Copying records between replicas (FR-2.5, M4).
  {
    label: 'Replication',
    types: [
      'ReplicaFetched',
      'IsrShrunk',
      'IsrExpanded',
      'LeaderElected',
      'PartitionOffline',
      'RecordsTruncated',
    ],
  },
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

/** Whether an event is about an internal topic (it has a `topic` field naming one). */
export const isInternalTopicEvent = (event: EngineEvent): boolean =>
  'topic' in event && isInternalTopic(event.topic);

/**
 * The events to show, oldest first. Returns `events` itself when nothing is hidden: at once when
 * no type is hidden and internal topics are shown, else after a pass that removed nothing (so a
 * memoised consumer sees the same array).
 */
export function filterEvents(
  events: readonly EngineEvent[],
  hidden: HiddenTypes,
  showInternal: boolean,
): readonly EngineEvent[] {
  if (hidden.size === 0 && showInternal) return events;
  const shown = events.filter(
    (e) => !hidden.has(e.type) && (showInternal || !isInternalTopicEvent(e)),
  );
  return shown.length === events.length ? events : shown;
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

/**
 * "All types", "12 of 13 types", "No types" for the filter button, followed by
 * " · internal topics hidden" while the internal-topics toggle is off.
 */
export function filterSummary(hidden: HiddenTypes, showInternal: boolean): string {
  const count = ALL_EVENT_TYPES.filter((t) => !hidden.has(t)).length;
  const types =
    count === ALL_EVENT_TYPES.length
      ? 'All types'
      : count === 0
        ? 'No types'
        : `${String(count)} of ${String(ALL_EVENT_TYPES.length)} types`;
  return showInternal ? types : `${types} · internal topics hidden`;
}

import { describeEvent, formatSimTime, type EngineEvent } from '@kp/engine';
import { memo, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { formatCount } from '../canvas/live/format.ts';
import { EVENT_LIST_SIZE, useSimStore } from '../stores/sim-store.ts';
import { setHiddenEventTypes, useUiStore } from '../stores/ui-store.ts';
import { EventFilter } from './EventFilter.tsx';
import { filterEvents } from './event-filter.ts';
import { FollowToggle } from './FollowToggle.tsx';
import { useFollow } from './use-follow.ts';
import { tailRange, visibleRange } from './windowing.ts';

/** Row height of the event list, px. Fixed, so windowing is plain arithmetic. */
export const EVENT_ROW_HEIGHT = 20;

/** Rows rendered beyond the visible ones on each side. */
const OVERSCAN = 6;

/** Viewport height before the first measurement (and in jsdom, which has no layout). */
const FALLBACK_VIEWPORT = 128;

const LIST_ID = 'event-list-rows';

/**
 * One event: sim time, type and what happened, the same three parts as the engine's
 * `formatEvent` line ("00:01.500  RecordAppended   orders-1  offset=2  key=B") in columns.
 * Memoised on the event object: frames append new objects and never change old ones, so a
 * row on screen re-renders only when it moves.
 */
const EventRow = memo(function EventRow(props: {
  event: EngineEvent;
  index: number;
  total: number;
  /** Entry animation class for a row that just arrived (B7), or null. */
  enter: string | null;
}) {
  const { event, index, total } = props;
  // Decided once, at mount: the slide-in must not be cut short by the next frame's render.
  const [enter] = useState(props.enter);
  return (
    <li
      aria-setsize={total}
      aria-posinset={index + 1}
      data-seq={event.seq}
      className={`absolute inset-x-0 flex h-5 items-center gap-3 px-4 font-mono text-[12px] leading-5 whitespace-pre ${enter ?? ''}`}
      style={{ top: index * EVENT_ROW_HEIGHT }}
    >
      <span className="w-[76px] shrink-0 text-status/70">{formatSimTime(event.t)}</span>
      <span className="w-[124px] shrink-0 truncate font-semibold">{event.type}</span>
      <span className="min-w-0 truncate">{describeEvent(event)}</span>
    </li>
  );
});

/**
 * Event list (B7, FR-6.3): every engine event, newest at the bottom, with a sim-time column, a
 * type filter and a Follow toggle. Bounded to the host's ring buffer (simStore keeps the same
 * 10,000). Only the rows in view (plus a few) are mounted, so frames at 30 Hz cost a filter pass
 * and a handful of row renders, however long the list is.
 */
export function EventList() {
  const events = useSimStore((s) => s.events);
  const hidden = useUiStore((s) => s.hiddenEventTypes);
  const reducedMotion = useUiStore((s) => s.reducedMotion);
  const shown = useMemo(() => filterEvents(events, hidden), [events, hidden]);

  const scroller = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewport, setViewport] = useState(FALLBACK_VIEWPORT);
  const follow = useFollow(scroller, shown, setScrollTop);

  useEffect(() => {
    const el = scroller.current;
    if (el === null) return;
    const measure = (): void => {
      if (el.clientHeight > 0) setViewport(el.clientHeight);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => {
      observer.disconnect();
    };
  }, []);

  const range = follow.following
    ? tailRange(shown.length, EVENT_ROW_HEIGHT, viewport, OVERSCAN)
    : visibleRange({
        count: shown.length,
        rowHeight: EVENT_ROW_HEIGHT,
        scrollTop,
        viewportHeight: viewport,
        overscan: OVERSCAN,
      });

  // B7: rows newer than the last render slide in. Rows scrolled into view are not new.
  const newestSeq = shown.at(-1)?.seq ?? -1;
  const seenSeq = useRef<number | null>(null);
  const previousSeq = seenSeq.current ?? newestSeq;
  useEffect(() => {
    seenSeq.current = newestSeq;
  }, [newestSeq]);

  // Reduced motion: a 150 ms fade instead of the slide (animations.md).
  const enterClass = reducedMotion ? 'kp-cell-fade' : 'kp-row-in';
  const rows: ReactNode[] = [];
  for (let i = range.start; i < range.end; i++) {
    const event = shown[i];
    if (event === undefined) continue;
    rows.push(
      <EventRow
        key={event.seq}
        event={event}
        index={i}
        total={shown.length}
        enter={
          follow.following && event.seq > previousSeq && previousSeq <= newestSeq
            ? enterClass
            : null
        }
      />,
    );
  }

  const atCap = events.length >= EVENT_LIST_SIZE;
  return (
    <div className="flex h-full min-h-0 flex-col" data-testid="event-list">
      <div className="flex h-8 shrink-0 items-center gap-3 border-b border-divider px-4 text-[12px]">
        <span className="text-status/70" data-testid="event-count">
          {formatCount(events.length)} {events.length === 1 ? 'event' : 'events'}
          {atCap && ` (newest ${formatCount(EVENT_LIST_SIZE)} kept)`}
          {hidden.size > 0 && ` · ${formatCount(shown.length)} shown`}
        </span>
        <span className="flex-1" />
        <EventFilter hidden={hidden} onChange={setHiddenEventTypes} />
        <FollowToggle
          following={follow.following}
          onChange={follow.setFollowing}
          controls={LIST_ID}
        />
      </div>
      <div
        ref={scroller}
        id={LIST_ID}
        tabIndex={0}
        role="region"
        aria-label="Event list"
        className="relative min-h-0 flex-1 overflow-y-auto focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-primary"
        {...follow.handlers}
      >
        {shown.length === 0 ? (
          <p className="px-4 py-2 text-[13px] text-status/70">
            {events.length === 0
              ? 'No events yet. Build something on the canvas and press Play.'
              : 'No events of the selected types.'}
          </p>
        ) : (
          // overflow-clip: an entering row starts 6 px low (kp-row-in's translateY). Unclipped,
          // that transform adds 6 px to the scroller's scrollHeight, so Follow scrolled past the
          // last row and the view crept back up as each animation ended (M2-decisions F3).
          <ol
            aria-label="Events, oldest first"
            className="relative overflow-clip"
            style={{ height: shown.length * EVENT_ROW_HEIGHT }}
          >
            {rows}
          </ol>
        )}
      </div>
    </div>
  );
}

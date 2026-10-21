// @vitest-environment jsdom
import { describeEvent, formatSimTime, type EngineEvent } from '@kp/engine';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { resetSimStore, useSimStore } from '../stores/sim-store.ts';
import { resetUiStore } from '../stores/ui-store.ts';
import { EVENT_ROW_HEIGHT, EventList } from './EventList.tsx';

/** Alternating Connected / RecordAppended events with seq from..to-1. */
function makeEvents(from: number, to: number): EngineEvent[] {
  const out: EngineEvent[] = [];
  for (let seq = from; seq < to; seq++) {
    out.push(
      seq % 2 === 0
        ? { t: seq * 10, seq, type: 'Connected', from: 'p1', to: `t${String(seq)}` }
        : {
            t: seq * 10,
            seq,
            type: 'RecordAppended',
            topic: 't1',
            partition: 0,
            offset: seq,
            key: 'A',
            value: 'v',
            producerId: 'p1',
            traceId: `p1@1#${String(seq)}`,
          },
    );
  }
  return out;
}

function setEvents(events: readonly EngineEvent[]): void {
  act(() => {
    useSimStore.setState({ events });
  });
}

const VIEWPORT = 100;

/**
 * jsdom has no layout: give the scroller a 100 px viewport, a scrollHeight from the list's
 * height, and a scrollTop clamped like a browser's.
 */
function fakeLayout(el: HTMLElement): { scrollTop: () => number } {
  let top = 0;
  const content = (): number => {
    const list = el.querySelector('ol');
    return list === null ? 0 : Number.parseFloat(list.style.height);
  };
  Object.defineProperty(el, 'clientHeight', { configurable: true, get: () => VIEWPORT });
  Object.defineProperty(el, 'scrollHeight', {
    configurable: true,
    get: () => Math.max(VIEWPORT, content()),
  });
  Object.defineProperty(el, 'scrollTop', {
    configurable: true,
    get: () => top,
    set: (v: number) => {
      top = Math.max(0, Math.min(v, Math.max(VIEWPORT, content()) - VIEWPORT));
    },
  });
  return { scrollTop: () => top };
}

const scroller = () => screen.getByRole('region', { name: 'Event list' });
const follow = () => screen.getByRole('button', { name: 'Follow' });
const rows = () => within(scroller()).queryAllByRole('listitem');

describe('<EventList>', () => {
  beforeEach(() => {
    resetSimStore();
    resetUiStore();
  });
  afterEach(() => {
    cleanup();
  });

  it('shows an empty state', () => {
    render(<EventList />);
    expect(scroller().textContent).toContain('No events yet');
    expect(screen.getByTestId('event-count').textContent).toBe('0 events');
  });

  it('formats rows like formatEvent: sim time, type, description; newest at the bottom', () => {
    const events = makeEvents(0, 3);
    setEvents(events);
    render(<EventList />);
    const items = rows();
    expect(items).toHaveLength(3);
    events.forEach((e, i) => {
      const text = items[i]?.textContent ?? '';
      expect(text).toBe(`${formatSimTime(e.t)}${e.type}${describeEvent(e)}`);
      expect(items[i]?.style.top).toBe(`${String(i * EVENT_ROW_HEIGHT)}px`);
    });
  });

  it('renders only the visible window of 10,000 events', () => {
    setEvents(makeEvents(0, 10_000));
    render(<EventList />);
    const items = rows();
    expect(items.length).toBeGreaterThan(0);
    expect(items.length).toBeLessThan(25);
    // Following: the window is at the end.
    expect(items.at(-1)?.getAttribute('data-seq')).toBe('9999');
    expect(items.at(-1)?.getAttribute('aria-posinset')).toBe('10000');
    expect(screen.getByTestId('event-count').textContent).toBe(
      '10,000 events (newest 10,000 kept)',
    );
  });

  it('follows new events, pauses on scrolling up, and resumes with the toggle', () => {
    setEvents(makeEvents(0, 50));
    render(<EventList />);
    const layout = fakeLayout(scroller());
    expect(follow().getAttribute('aria-pressed')).toBe('true');

    setEvents(makeEvents(0, 60));
    const bottom = 60 * EVENT_ROW_HEIGHT - VIEWPORT;
    expect(layout.scrollTop()).toBe(bottom);

    // Wheel up: following stops at once; new events no longer move the view.
    fireEvent.wheel(scroller(), { deltaY: -120 });
    expect(follow().getAttribute('aria-pressed')).toBe('false');
    scroller().scrollTop = 200;
    fireEvent.scroll(scroller());
    setEvents(makeEvents(0, 80));
    expect(layout.scrollTop()).toBe(200);
    // The window follows the scroll position, not the tail.
    expect(rows()[0]?.getAttribute('data-seq')).toBe(String(Math.max(0, 200 / 20 - 6)));

    // Follow again: jumps to the newest row.
    fireEvent.click(follow());
    expect(follow().getAttribute('aria-pressed')).toBe('true');
    expect(layout.scrollTop()).toBe(80 * EVENT_ROW_HEIGHT - VIEWPORT);
    expect(rows().at(-1)?.getAttribute('data-seq')).toBe('79');
  });

  it('a scrollbar drag (scroll event away from the bottom) pauses following too', () => {
    setEvents(makeEvents(0, 50));
    render(<EventList />);
    fakeLayout(scroller());
    setEvents(makeEvents(0, 51));
    scroller().scrollTop = 0;
    fireEvent.scroll(scroller());
    expect(follow().getAttribute('aria-pressed')).toBe('false');
  });

  it('keys that scroll up pause following', () => {
    setEvents(makeEvents(0, 50));
    render(<EventList />);
    fireEvent.keyDown(scroller(), { key: 'PageUp' });
    expect(follow().getAttribute('aria-pressed')).toBe('false');
  });

  it('filters by event type with labelled checkboxes', () => {
    setEvents(makeEvents(0, 6));
    render(<EventList />);
    const button = screen.getByRole('button', { name: 'Filter: All types' });
    expect(button.getAttribute('aria-expanded')).toBe('false');
    fireEvent.click(button);
    expect(button.getAttribute('aria-expanded')).toBe('true');
    const panel = screen.getByRole('group', { name: 'Event types to show' });
    const connected = within(panel).getByRole('checkbox', { name: 'Connected' });
    expect(connected).toHaveProperty('checked', true);
    fireEvent.click(connected);
    expect(connected).toHaveProperty('checked', false);
    expect(rows().map((r) => r.getAttribute('data-seq'))).toEqual(['1', '3', '5']);
    expect(screen.getByTestId('event-count').textContent).toBe('6 events · 3 shown');
    expect(screen.getByRole('button', { name: 'Filter: 12 of 13 types' })).toBeTruthy();

    fireEvent.click(within(panel).getByRole('button', { name: 'Hide all' }));
    expect(scroller().textContent).toContain('No events of the selected types.');
    fireEvent.click(within(panel).getByRole('button', { name: 'Show all' }));
    expect(rows()).toHaveLength(6);

    // Escape closes the panel and returns focus to the button.
    fireEvent.keyDown(within(panel).getByRole('checkbox', { name: 'Connected' }), {
      key: 'Escape',
    });
    expect(screen.queryByRole('group', { name: 'Event types to show' })).toBeNull();
    expect(document.activeElement).toBe(button);
  });

  it('a full frame (reset) replaces the list', () => {
    setEvents(makeEvents(0, 40));
    render(<EventList />);
    setEvents(makeEvents(0, 2));
    expect(rows()).toHaveLength(2);
  });
});

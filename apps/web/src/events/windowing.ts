/**
 * Fixed-row-height windowing for long lists (M2-T11): which rows to render for a scroll position,
 * so a 10,000-event list only ever mounts the few rows on screen. Pure: unit-tested in Node.
 */

export interface WindowInput {
  /** Items in the list. */
  readonly count: number;
  /** Height of every row, px. */
  readonly rowHeight: number;
  /** Scroll offset of the viewport, px. */
  readonly scrollTop: number;
  /** Height of the viewport, px. */
  readonly viewportHeight: number;
  /** Extra rows rendered above and below the visible ones, so fast scrolling shows no gaps. */
  readonly overscan?: number;
}

/** Rows `start` (inclusive) to `end` (exclusive) to render. Empty when `count` is 0. */
export interface RowWindow {
  readonly start: number;
  readonly end: number;
}

/** Rows to render for a scroll position. Out-of-range scroll offsets are clamped. */
export function visibleRange(input: WindowInput): RowWindow {
  const { count, rowHeight, viewportHeight } = input;
  const overscan = input.overscan ?? 0;
  if (count <= 0 || rowHeight <= 0) return { start: 0, end: 0 };
  const maxScroll = Math.max(0, count * rowHeight - viewportHeight);
  const scrollTop = Math.min(Math.max(0, input.scrollTop), maxScroll);
  const first = Math.floor(scrollTop / rowHeight);
  const last = Math.ceil((scrollTop + Math.max(0, viewportHeight)) / rowHeight);
  return {
    start: Math.max(0, first - overscan),
    end: Math.min(count, Math.max(last, first + 1) + overscan),
  };
}

/** The window when the list is scrolled to its end (Follow on): the last rows that fit. */
export function tailRange(
  count: number,
  rowHeight: number,
  viewportHeight: number,
  overscan = 0,
): RowWindow {
  return visibleRange({
    count,
    rowHeight,
    viewportHeight,
    overscan,
    scrollTop: Math.max(0, count * rowHeight - viewportHeight),
  });
}

/**
 * Whether a scroll container shows its last pixel. `tolerance` absorbs sub-pixel scroll offsets
 * (zoomed pages report fractions).
 */
export function isAtBottom(
  scrollTop: number,
  viewportHeight: number,
  scrollHeight: number,
  tolerance = 2,
): boolean {
  return scrollTop + viewportHeight >= scrollHeight - tolerance;
}

import {
  useCallback,
  useLayoutEffect,
  useState,
  type KeyboardEvent,
  type RefObject,
  type UIEvent,
  type WheelEvent,
} from 'react';
import { isAtBottom } from './windowing.ts';

/** Keys that scroll a focused list up: pressing one means "stop following, I'm reading". */
const SCROLL_UP_KEYS = new Set(['ArrowUp', 'PageUp', 'Home']);

export interface Follow {
  readonly following: boolean;
  readonly setFollowing: (following: boolean) => void;
  /** Spread on the scroll container. */
  readonly handlers: {
    readonly onScroll: (event: UIEvent<HTMLElement>) => void;
    readonly onWheel: (event: WheelEvent<HTMLElement>) => void;
    readonly onKeyDown: (event: KeyboardEvent<HTMLElement>) => void;
  };
}

/**
 * "Follow" for a list whose newest row is at the bottom (B7, B9): while following, the container
 * is kept scrolled to the end after every change of `contentKey`; scrolling up (wheel, keys or
 * the scrollbar) pauses following, and only the Follow toggle turns it back on.
 *
 * Wheel and key events pause at once, before the scroll event: at 30 frames a second a new frame
 * could otherwise scroll back to the bottom before the scroll event is seen.
 */
export function useFollow(
  ref: RefObject<HTMLElement | null>,
  contentKey: unknown,
  onScrollTop?: (scrollTop: number) => void,
): Follow {
  const [following, setFollowing] = useState(true);

  useLayoutEffect(() => {
    const el = ref.current;
    if (!following || el === null) return;
    el.scrollTop = el.scrollHeight;
    onScrollTop?.(el.scrollTop);
  }, [ref, following, contentKey, onScrollTop]);

  const onScroll = useCallback(
    (event: UIEvent<HTMLElement>) => {
      const el = event.currentTarget;
      onScrollTop?.(el.scrollTop);
      if (following && !isAtBottom(el.scrollTop, el.clientHeight, el.scrollHeight)) {
        setFollowing(false);
      }
    },
    [following, onScrollTop],
  );
  const onWheel = useCallback((event: WheelEvent<HTMLElement>) => {
    if (event.deltaY < 0) setFollowing(false);
  }, []);
  const onKeyDown = useCallback((event: KeyboardEvent<HTMLElement>) => {
    if (SCROLL_UP_KEYS.has(event.key)) setFollowing(false);
  }, []);

  return { following, setFollowing, handlers: { onScroll, onWheel, onKeyDown } };
}

import { beforeEach, describe, expect, it } from 'vitest';
import {
  deselect,
  dismissToast,
  MAX_TOASTS,
  pushToast,
  readReducedMotion,
  REDUCED_MOTION_QUERY,
  resetUiStore,
  select,
  setBottomTab,
  setRightTab,
  useUiStore,
  watchReducedMotion,
} from './ui-store.ts';

/** A minimal MediaQueryList stand-in whose `matches` can be flipped. */
function fakeMatchMedia(initial: boolean) {
  let matches = initial;
  const listeners = new Set<(event: MediaQueryListEvent) => void>();
  const queries: string[] = [];
  const matchMedia = (query: string): MediaQueryList => {
    queries.push(query);
    return {
      get matches() {
        return matches;
      },
      media: query,
      addEventListener: (_type: string, listener: (event: MediaQueryListEvent) => void) => {
        listeners.add(listener);
      },
      removeEventListener: (_type: string, listener: (event: MediaQueryListEvent) => void) => {
        listeners.delete(listener);
      },
    } as unknown as MediaQueryList;
  };
  return {
    matchMedia,
    queries,
    listenerCount: () => listeners.size,
    set(value: boolean) {
      matches = value;
      for (const listener of listeners) listener({ matches: value } as MediaQueryListEvent);
    },
  };
}

describe('reduced motion', () => {
  beforeEach(() => {
    resetUiStore();
  });

  it('defaults to false where there is no matchMedia (Node)', () => {
    expect(readReducedMotion(undefined)).toBe(false);
    expect(useUiStore.getState().reducedMotion).toBe(false);
  });

  it('is initialised from prefers-reduced-motion', () => {
    const media = fakeMatchMedia(true);
    expect(readReducedMotion(media.matchMedia)).toBe(true);
    const stop = watchReducedMotion(media.matchMedia);
    expect(media.queries).toContain(REDUCED_MOTION_QUERY);
    expect(useUiStore.getState().reducedMotion).toBe(true);
    stop();
  });

  it('follows changes until stopped', () => {
    const media = fakeMatchMedia(false);
    const stop = watchReducedMotion(media.matchMedia);
    expect(useUiStore.getState().reducedMotion).toBe(false);
    media.set(true);
    expect(useUiStore.getState().reducedMotion).toBe(true);
    stop();
    expect(media.listenerCount()).toBe(0);
    media.set(false);
    expect(useUiStore.getState().reducedMotion).toBe(true);
  });
});

describe('toasts, tabs and selection', () => {
  beforeEach(() => {
    resetUiStore();
  });

  it(`keeps at most ${String(MAX_TOASTS)} toasts, newest last`, () => {
    for (let i = 1; i <= 5; i++) pushToast('warning', `m${String(i)}`);
    expect(useUiStore.getState().toasts.map((t) => t.message)).toEqual(['m3', 'm4', 'm5']);
    const id = useUiStore.getState().toasts[1]?.id ?? -1;
    dismissToast(id);
    expect(useUiStore.getState().toasts.map((t) => t.message)).toEqual(['m3', 'm5']);
  });

  it('switches tabs and removes deleted ids from the selection', () => {
    expect(useUiStore.getState()).toMatchObject({ rightTab: 'properties', bottomTab: 'events' });
    setRightTab('partition-log');
    setBottomTab('lag');
    expect(useUiStore.getState()).toMatchObject({ rightTab: 'partition-log', bottomTab: 'lag' });
    select({ nodeIds: ['p1', 't1'], edgeIds: ['p1->t1'] });
    deselect('t1');
    expect(useUiStore.getState().selection).toEqual({ nodeIds: ['p1'], edgeIds: ['p1->t1'] });
  });
});

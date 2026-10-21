/**
 * uiStore: view state that is neither topology nor simulation (architecture §5): selection, the
 * active panel tabs, toasts and the reduced-motion setting.
 */

import type { EngineEventType } from '@kp/engine';
import { create } from 'zustand';

export type RightTab = 'properties' | 'partition-log';
export type BottomTab = 'events' | 'lag' | 'timeline';

export interface Selection {
  readonly nodeIds: readonly string[];
  /** Edge ids as React Flow knows them (T7). */
  readonly edgeIds: readonly string[];
}

export type ToastKind = 'warning' | 'info';

export interface Toast {
  readonly id: number;
  readonly kind: ToastKind;
  readonly message: string;
}

/** The partition the Partition log tab shows (M2-T11). */
export interface LogTarget {
  readonly topicId: string;
  readonly partition: number;
}

export interface UiState {
  readonly selection: Selection;
  readonly rightTab: RightTab;
  readonly bottomTab: BottomTab;
  /** Oldest first, at most `MAX_TOASTS`. */
  readonly toasts: readonly Toast[];
  /** `prefers-reduced-motion: reduce` (animations.md). */
  readonly reducedMotion: boolean;
  /** Event types the event list hides (B7). Kept here so the filter survives a tab switch. */
  readonly hiddenEventTypes: ReadonlySet<EngineEventType>;
  /** Partition log target; null = the first topic's partition 0. */
  readonly logTarget: LogTarget | null;
}

/** Toasts on screen at once; a new one pushes out the oldest. */
export const MAX_TOASTS = 3;

/** How long a toast stays before it dismisses itself (UI chrome: not scaled by sim speed). */
export const TOAST_DURATION_MS = 5000;

export const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

export const EMPTY_SELECTION: Selection = { nodeIds: [], edgeIds: [] };

type MatchMedia = (query: string) => MediaQueryList;

function browserMatchMedia(): MatchMedia | undefined {
  return typeof window !== 'undefined' && typeof window.matchMedia === 'function'
    ? window.matchMedia.bind(window)
    : undefined;
}

/** The current `prefers-reduced-motion` value; false where there is no `matchMedia` (Node). */
export function readReducedMotion(
  matchMedia: MatchMedia | undefined = browserMatchMedia(),
): boolean {
  return matchMedia?.(REDUCED_MOTION_QUERY).matches ?? false;
}

export function initialUiState(): UiState {
  return {
    selection: EMPTY_SELECTION,
    rightTab: 'properties',
    bottomTab: 'events',
    toasts: [],
    reducedMotion: readReducedMotion(),
    hiddenEventTypes: new Set(),
    logTarget: null,
  };
}

export const useUiStore = create<UiState>()(() => initialUiState());

export function resetUiStore(): void {
  useUiStore.setState(initialUiState(), true);
}

// ── Updates ───────────────────────────────────────────────────────────────────────────────────

export function select(selection: Selection): void {
  useUiStore.setState({ selection });
}

/** Removes ids that no longer exist (e.g. after a component was removed). */
export function deselect(id: string): void {
  const { selection } = useUiStore.getState();
  if (!selection.nodeIds.includes(id) && !selection.edgeIds.includes(id)) return;
  useUiStore.setState({
    selection: {
      nodeIds: selection.nodeIds.filter((n) => n !== id),
      edgeIds: selection.edgeIds.filter((e) => e !== id),
    },
  });
}

export function setRightTab(rightTab: RightTab): void {
  useUiStore.setState({ rightTab });
}

export function setBottomTab(bottomTab: BottomTab): void {
  useUiStore.setState({ bottomTab });
}

export function setHiddenEventTypes(hiddenEventTypes: ReadonlySet<EngineEventType>): void {
  useUiStore.setState({ hiddenEventTypes });
}

export function setLogTarget(logTarget: LogTarget | null): void {
  useUiStore.setState({ logTarget });
}

let nextToastId = 1;

/** Shows a toast and returns its id. Dismissal is the Toasts component's job (it owns the timer). */
export function pushToast(kind: ToastKind, message: string): number {
  const id = nextToastId++;
  const { toasts } = useUiStore.getState();
  useUiStore.setState({ toasts: [...toasts, { id, kind, message }].slice(-MAX_TOASTS) });
  return id;
}

export function dismissToast(id: number): void {
  const { toasts } = useUiStore.getState();
  if (!toasts.some((t) => t.id === id)) return;
  useUiStore.setState({ toasts: toasts.filter((t) => t.id !== id) });
}

/**
 * Keeps `reducedMotion` in sync with the OS setting. Returns a function that stops watching.
 * Called once by the app bootstrap.
 */
export function watchReducedMotion(
  matchMedia: MatchMedia | undefined = browserMatchMedia(),
): () => void {
  if (matchMedia === undefined) return () => undefined;
  const query = matchMedia(REDUCED_MOTION_QUERY);
  useUiStore.setState({ reducedMotion: query.matches });
  const onChange = (event: MediaQueryListEvent): void => {
    useUiStore.setState({ reducedMotion: event.matches });
  };
  query.addEventListener('change', onChange);
  return () => {
    query.removeEventListener('change', onChange);
  };
}

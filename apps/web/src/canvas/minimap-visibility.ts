/**
 * When the minimap shows (FR-1.7, ui-spec "Minimap", animations.md B11): automatically only when
 * part of the setup is off-screen, because it doesn't fit or the view is zoomed in. The map toggle
 * overrides the automatic choice until the page is reloaded.
 */

import type { Rect } from './geometry.ts';

/** Content closer than this to an edge still counts as visible (handles stick out by ~5 px). */
export const OFFSCREEN_TOLERANCE_PX = 1;

/**
 * True when some content lies outside the visible area. `content` and `visible` are both in flow
 * coordinates. An empty canvas, or a canvas that has not been measured yet, has nothing off-screen.
 */
export function isContentOffscreen(
  content: Rect | null,
  visible: Rect,
  tolerance = OFFSCREEN_TOLERANCE_PX,
): boolean {
  if (content === null || visible.width <= 0 || visible.height <= 0) return false;
  return (
    content.x < visible.x - tolerance ||
    content.y < visible.y - tolerance ||
    content.x + content.width > visible.x + visible.width + tolerance ||
    content.y + content.height > visible.y + visible.height + tolerance
  );
}

/** `null` = automatic; `true` / `false` = the user showed / hid the map with the toggle. */
export type MinimapOverride = boolean | null;

export function isMinimapVisible(contentOffscreen: boolean, override: MinimapOverride): boolean {
  return override ?? contentOffscreen;
}

/** The override after pressing the map toggle: the opposite of what is on screen now. */
export function toggleMinimap(currentlyVisible: boolean): MinimapOverride {
  return !currentlyVisible;
}

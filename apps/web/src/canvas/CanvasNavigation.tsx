import { MiniMap, Panel, useReactFlow, useStore, type ReactFlowState } from '@xyflow/react';
import { useState } from 'react';
import { useUiStore } from '../stores/ui-store.ts';
import { boundsOf, visibleFlowRect } from './geometry.ts';
import { nodeRects } from './ConnectionHint.tsx';
import {
  isContentOffscreen,
  isMinimapVisible,
  toggleMinimap,
  type MinimapOverride,
} from './minimap-visibility.ts';

/** Zoom / fit animation (UI chrome, not sim time); none under reduced motion. */
export const VIEWPORT_ANIMATION_MS = 200;

/** Fit never zooms in past 100 %: a lone node would otherwise fill the canvas. */
export const FIT_VIEW_OPTIONS = { padding: 0.15, maxZoom: 1 } as const;

function contentOffscreen(s: ReactFlowState): boolean {
  const viewport = { x: s.transform[0], y: s.transform[1], zoom: s.transform[2] };
  return isContentOffscreen(
    boundsOf(nodeRects(s.nodeLookup)),
    visibleFlowRect(viewport, { width: s.width, height: s.height }),
  );
}

const BUTTON =
  'flex h-full items-center justify-center px-2 text-[13px] text-status hover:bg-muted focus-visible:outline-2 focus-visible:outline-primary';

/**
 * Zoom controls (160 × 40: −, fit, +, map toggle) bottom-left and the minimap (160 × 84)
 * bottom-right (ui-spec, wireframe 05, FR-1.7). The minimap shows only while some content is
 * off-screen and fades in / out over 200 ms (animations.md B11); the map toggle overrides that.
 */
export function CanvasNavigation() {
  const flow = useReactFlow();
  const reducedMotion = useUiStore((s) => s.reducedMotion);
  const offscreen = useStore(contentOffscreen);
  const [override, setOverride] = useState<MinimapOverride>(null);
  const visible = isMinimapVisible(offscreen, override);
  const duration = reducedMotion ? 0 : VIEWPORT_ANIMATION_MS;

  return (
    <>
      <Panel position="bottom-left" className="!m-4">
        <div
          role="group"
          aria-label="Canvas navigation"
          className="flex h-10 w-[160px] items-stretch justify-around overflow-hidden rounded-md border-[1.5px] border-border bg-surface"
        >
          <button
            type="button"
            aria-label="Zoom out"
            className={BUTTON}
            onClick={() => void flow.zoomOut({ duration })}
          >
            −
          </button>
          <button
            type="button"
            aria-label="Fit view"
            className={BUTTON}
            onClick={() => void flow.fitView({ ...FIT_VIEW_OPTIONS, duration })}
          >
            fit
          </button>
          <button
            type="button"
            aria-label="Zoom in"
            className={BUTTON}
            onClick={() => void flow.zoomIn({ duration })}
          >
            +
          </button>
          <button
            type="button"
            aria-label="Minimap"
            aria-pressed={visible}
            title={override === null ? 'Minimap: automatic' : 'Minimap: set by hand'}
            className={`${BUTTON} border-l border-divider ${visible ? 'font-semibold underline' : ''}`}
            onClick={() => {
              setOverride(toggleMinimap(visible));
            }}
          >
            map
          </button>
        </div>
      </Panel>
      {/* A static wrapper: the minimap's panel still positions itself in the canvas corner. */}
      <div data-testid="minimap" data-visible={visible} aria-hidden={!visible}>
        <MiniMap
          position="bottom-right"
          ariaLabel="Minimap"
          pannable
          zoomable
          style={{ width: 160, height: 84, margin: 16 }}
          className={`overflow-hidden rounded-md border-[1.5px] border-border ${
            visible ? 'opacity-100' : 'pointer-events-none opacity-0'
          } ${reducedMotion ? '' : 'transition-opacity duration-200'}`}
          nodeBorderRadius={4}
        />
      </div>
    </>
  );
}

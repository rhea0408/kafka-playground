import { Panel, useStoreApi } from '@xyflow/react';
import { useEffect, useRef, useState } from 'react';
import { onSimFrame, useSimStore } from '../../stores/sim-store.ts';
import { useTopologyStore } from '../../stores/topology-store.ts';
import { useUiStore } from '../../stores/ui-store.ts';
import type { FlowNode } from '../flow-mapping.ts';
import { resolverFor } from './anchors.ts';
import { debugEnabled, newOverlayDebug, testDotSpecs, type OverlayDebug } from './debug.ts';
import { DotModel } from './dots.ts';
import { drawDots, readPalette, type DotPalette } from './draw.ts';
import { FpsMeter } from './fps.ts';

/** NFR-3.3 / ADR 0008: dots are a visual aid, not a timing claim. */
export const OVERLAY_LEGEND =
  'Dots are slowed down so you can see them. Real delivery takes a few milliseconds.';
export const CAP_CHIP_TEXT = 'Too many to animate';

/** How often the visible FPS number is refreshed (it is text, not a per-frame redraw). */
const FPS_TEXT_INTERVAL_MS = 250;

function readTokens(): DotPalette {
  const style = getComputedStyle(document.documentElement);
  return readPalette((token) => style.getPropertyValue(token));
}

/**
 * The Canvas2D message overlay (M2-T9, animations.md A1 / A2, ADR 0008). One `<canvas>` above
 * React Flow's nodes and edges, under its panels; `pointer-events: none`, so the canvas below
 * stays fully interactive. Engine events come in through `onSimFrame` (no React renders), the
 * pure `DotModel` moves the dots, and one `requestAnimationFrame` loop draws them with React
 * Flow's viewport transform. The loop runs only while there are dots or the sim is playing.
 */
export function MessageOverlay() {
  const store = useStoreApi<FlowNode>();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const fpsRef = useRef<HTMLSpanElement>(null);
  const [capChip, setCapChip] = useState(false);
  const [showFps] = useState(() => debugEnabled(window.location.search, import.meta.env.DEV));

  useEffect(() => {
    const canvas = canvasRef.current;
    const ctx = canvas?.getContext('2d') ?? null;
    if (canvas === null || ctx === null) return;

    const model = new DotModel();
    const meter = new FpsMeter();
    const debug: OverlayDebug = newOverlayDebug();
    window.__kp = { ...window.__kp, overlay: debug };

    let palette = readTokens();
    let playing = useSimStore.getState().playing;
    let speed = useSimStore.getState().speed;
    let raf: number | null = null;
    let chip = false;
    let lastFpsText = 0;
    let stressBatch = 0;

    const sizeCanvas = (width: number, height: number, dpr: number): void => {
      const w = Math.max(1, Math.round(width * dpr));
      const h = Math.max(1, Math.round(height * dpr));
      if (canvas.width !== w || canvas.height !== h) {
        canvas.width = w;
        canvas.height = h;
      }
    };

    const tick = (now: number): void => {
      raf = null;
      const rf = store.getState();
      model.step(
        now,
        { playing, speed, reducedMotion: useUiStore.getState().reducedMotion },
        resolverFor(rf.nodeLookup),
      );
      const dpr = window.devicePixelRatio || 1;
      sizeCanvas(rf.width, rf.height, dpr);
      const [x, y, zoom] = rf.transform;
      const drawn = drawDots(
        ctx,
        model.dots,
        { width: rf.width, height: rf.height, dpr, viewport: { x, y, zoom } },
        palette,
      );
      debug.fps = meter.frame(now);
      debug.dotsDrawn += drawn;
      debug.dotsOnScreen = drawn;
      debug.dotsActive = model.dots.length;
      debug.dotsQueued = model.queued;
      debug.dotsStarted = model.stats.started;
      debug.skippedCap = model.stats.skippedCap;
      debug.skippedNoA1 = model.stats.skippedNoA1;
      const nextChip = model.capChipVisible(now);
      if (nextChip !== chip) {
        chip = nextChip;
        debug.capChip = chip;
        setCapChip(chip);
      }
      if (fpsRef.current !== null && now - lastFpsText >= FPS_TEXT_INTERVAL_MS) {
        lastFpsText = now;
        fpsRef.current.textContent = `${String(debug.fps)} fps · ${String(drawn)} dots`;
      }
      if (model.busy || playing || chip) {
        raf = requestAnimationFrame(tick);
      } else {
        // Idle: stop the loop. The canvas was just cleared by drawDots (no dots).
        model.pauseClock();
        meter.reset();
        debug.fps = 0;
        if (fpsRef.current !== null) fpsRef.current.textContent = '0 fps · idle';
      }
    };

    const ensureLoop = (): void => {
      if (raf === null) raf = requestAnimationFrame(tick);
    };

    const stopFrames = onSimFrame((frame) => {
      playing = frame.playing;
      speed = frame.speed;
      // A full frame means "start over" (init / reset / new subscriber); its history is never animated.
      if (frame.kind === 'full') model.reset();
      const now = performance.now();
      if (document.hidden) {
        // No animation frames run in a background tab: skip these dots rather than replay them
        // all when the tab comes back (ADR 0008: skip, don't delay).
        model.skip(
          frame.events.filter((e) => e.type === 'RecordProduced' || e.type === 'RecordConsumed')
            .length,
          now,
        );
      } else {
        model.ingest(frame.events, now);
      }
      ensureLoop();
    });

    if (debugEnabled(window.location.search, import.meta.env.DEV)) {
      debug.spawnTestDots = (count: number): number => {
        const now = performance.now();
        let started = 0;
        for (const { spec, progress } of testDotSpecs(
          useTopologyStore.getState(),
          count,
          stressBatch++,
        )) {
          if (model.spawn(spec, now, progress)) started++;
        }
        ensureLoop();
        return started;
      };
    }

    // Colors come from the CSS tokens; read them again if the theme can have changed.
    const onTheme = (): void => {
      palette = readTokens();
      ensureLoop();
    };
    const scheme = window.matchMedia('(prefers-color-scheme: dark)');
    scheme.addEventListener('change', onTheme);
    const observer = new MutationObserver(onTheme);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class', 'style', 'data-theme'],
    });
    // Dots stay put while paused, but nodes and the viewport can still move: redraw on any
    // React Flow change while there is something to draw.
    const stopRf = store.subscribe(() => {
      if (model.busy) ensureLoop();
    });

    ensureLoop();
    return () => {
      stopFrames();
      stopRf();
      scheme.removeEventListener('change', onTheme);
      observer.disconnect();
      if (raf !== null) cancelAnimationFrame(raf);
      if (window.__kp?.overlay === debug) delete window.__kp.overlay;
    };
  }, [store]);

  return (
    <>
      <canvas
        ref={canvasRef}
        className="pointer-events-none absolute inset-0 z-[4] h-full w-full"
        aria-hidden="true"
        data-testid="message-overlay"
      />
      <Panel
        position="top-left"
        className="pointer-events-none !m-4 flex flex-col items-start gap-1"
      >
        <p
          className="flex items-center gap-1.5 rounded-sm bg-muted px-1.5 py-0.5 text-[11px] text-status"
          data-testid="overlay-legend"
        >
          <span
            aria-hidden="true"
            className="inline-flex size-4 items-center justify-center rounded-full bg-status text-[9px] font-bold text-surface"
          >
            A
          </span>
          {OVERLAY_LEGEND}
        </p>
        <div aria-live="polite" className="empty:hidden">
          {capChip ? (
            <p
              className="rounded-sm bg-muted px-1.5 py-0.5 text-[11px] font-medium text-status"
              data-testid="overlay-cap-chip"
            >
              {CAP_CHIP_TEXT}: some records get no dot.
            </p>
          ) : null}
        </div>
        {showFps ? (
          <span
            ref={fpsRef}
            aria-hidden="true"
            className="rounded-sm bg-surface px-1 font-mono text-[10px] text-status/70"
            data-testid="overlay-fps"
          >
            idle
          </span>
        ) : null}
      </Panel>
    </>
  );
}

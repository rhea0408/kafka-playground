import { formatSimTime } from '@kp/engine';
import type { MouseEvent } from 'react';
import { useActions } from '../actions/actions-context.tsx';
import { cycleSpeed, formatSpeed } from '../sim/speed.ts';
import { useSimStore } from '../stores/sim-store.ts';

/** Outline button: primary border and text on the surface color (wireframes 02, 03b, 05). */
const OUTLINE =
  'border-[1.5px] border-primary bg-surface text-primary hover:bg-muted active:bg-muted';
/** Filled primary: the paused state's Play button (wireframe 03b, ui-spec "active Play"). */
const FILLED = 'border-2 border-primary bg-primary font-bold text-surface';
const BASE =
  'flex h-10 shrink-0 items-center justify-center rounded-md text-[11px] leading-none ' +
  'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary';

/**
 * Play/Pause · Step · Speed · Reset (ui-spec "Sim controls"): buttons 40 × 40, speed 56 × 40,
 * gap 8, absolutely positioned 8 px from the right edge and vertically centred on the 56 px bar
 * (including its 1.5 px bottom border, so the centre is at y = 28).
 * Same order and spot on every screen (wireframes 02, 03b, 05).
 */
export function SimControls() {
  const actions = useActions();
  const playing = useSimStore((s) => s.playing);
  const speed = useSimStore((s) => s.speed);

  const next = cycleSpeed(speed, 1);
  const onSpeed = (event: MouseEvent<HTMLButtonElement>): void => {
    void actions.setSpeed(event.shiftKey ? cycleSpeed(speed, -1) : next);
  };

  return (
    <div
      role="group"
      aria-label="Simulation controls"
      className="absolute top-0 right-2 flex h-14 items-center gap-2"
    >
      {/* One toggle button whose label says what it will do (Play when paused, Pause when
          running), so the state is in the text, never only in the fill color. */}
      <button
        type="button"
        aria-label={playing ? 'Pause' : 'Play'}
        className={`${BASE} w-10 ${playing ? OUTLINE : FILLED}`}
        onClick={() => {
          void (playing ? actions.pause() : actions.play());
        }}
      >
        {playing ? 'Pause' : 'Play'}
      </button>
      <button
        type="button"
        aria-label="Step to the next event"
        className={`${BASE} w-10 ${OUTLINE}`}
        onClick={() => {
          void actions.step();
        }}
      >
        Step
      </button>
      <button
        type="button"
        aria-label={`Speed ${formatSpeed(speed)}. Activate for ${formatSpeed(next)}`}
        title="Click: faster · Shift+click: slower"
        className={`${BASE} w-14 text-xs ${OUTLINE}`}
        onClick={onSpeed}
      >
        {formatSpeed(speed)}
      </button>
      <button
        type="button"
        aria-label="Reset to time zero"
        className={`${BASE} w-10 ${OUTLINE}`}
        onClick={() => {
          void actions.reset();
        }}
      >
        Reset
      </button>
    </div>
  );
}

/**
 * The sim clock, `mm:ss.mmm` (engine `formatSimTime`). Not part of the wireframe; it sits where
 * wireframe 05 keeps free space left of the controls (16 px gap).
 */
export function SimClock() {
  const t = useSimStore((s) => s.t);
  return (
    <div className="absolute top-0 right-[224px] flex h-14 items-center gap-2 text-sm">
      <span id="sim-clock-label" className="text-xs text-status/70">
        Sim time
      </span>
      <span
        role="timer"
        aria-labelledby="sim-clock-label"
        className="font-mono tabular-nums text-status"
      >
        {formatSimTime(t)}
      </span>
    </div>
  );
}

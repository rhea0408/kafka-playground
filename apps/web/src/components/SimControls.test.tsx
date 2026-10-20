// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ActionsProvider } from '../actions/actions-context.tsx';
import type { Actions } from '../actions/actions.ts';
import { resetSimStore, useSimStore } from '../stores/sim-store.ts';
import { SimClock, SimControls } from './SimControls.tsx';

/** Only the sim-control actions; any other call fails the test. */
function fakeActions() {
  const ok = { ok: true } as const;
  const sim = {
    play: vi.fn(() => Promise.resolve()),
    pause: vi.fn(() => Promise.resolve()),
    step: vi.fn(() => Promise.resolve(null)),
    reset: vi.fn(() => Promise.resolve(ok)),
    setSpeed: vi.fn<(speed: number) => Promise<typeof ok>>(() => Promise.resolve(ok)),
  };
  const actions = new Proxy(sim, {
    get(target, prop) {
      if (prop in target) return target[prop as keyof typeof target];
      throw new Error(`Unexpected action ${String(prop)}`);
    },
  }) as unknown as Actions;
  return { sim, actions };
}

function renderControls() {
  const fake = fakeActions();
  render(
    <ActionsProvider actions={fake.actions}>
      <SimControls />
      <SimClock />
    </ActionsProvider>,
  );
  return fake.sim;
}

describe('<SimControls>', () => {
  beforeEach(() => {
    resetSimStore();
  });
  afterEach(() => {
    cleanup();
  });

  it('renders four labelled buttons in wireframe order: Play, Step, Speed, Reset', () => {
    renderControls();
    const group = screen.getByRole('group', { name: 'Simulation controls' });
    const buttons = within(group).getAllByRole('button');
    expect(buttons.map((b) => b.tagName)).toEqual(['BUTTON', 'BUTTON', 'BUTTON', 'BUTTON']);
    expect(buttons.map((b) => b.getAttribute('aria-label'))).toEqual([
      'Play',
      'Step to the next event',
      'Speed 1×. Activate for 2×',
      'Reset to time zero',
    ]);
    // Visible text is part of each accessible name (WCAG 2.5.3 label in name).
    for (const b of buttons) {
      expect(b.getAttribute('aria-label')).toContain(b.textContent);
    }
  });

  it('shows Play (filled primary) while paused and Pause (outline) while playing', () => {
    const sim = renderControls();
    const play = screen.getByRole('button', { name: 'Play' });
    expect(play.className).toContain('bg-primary');
    fireEvent.click(play);
    expect(sim.play).toHaveBeenCalledTimes(1);

    act(() => {
      useSimStore.setState({ playing: true });
    });
    const pause = screen.getByRole('button', { name: 'Pause' });
    expect(pause.textContent).toBe('Pause');
    expect(pause.className).not.toContain('bg-primary');
    fireEvent.click(pause);
    expect(sim.pause).toHaveBeenCalledTimes(1);
    expect(sim.play).toHaveBeenCalledTimes(1);
  });

  it('step and reset call their actions', () => {
    const sim = renderControls();
    fireEvent.click(screen.getByRole('button', { name: /^Step/ }));
    fireEvent.click(screen.getByRole('button', { name: /^Reset/ }));
    expect(sim.step).toHaveBeenCalledTimes(1);
    expect(sim.reset).toHaveBeenCalledTimes(1);
  });

  it('the speed button offers every preset: click goes faster, shift+click slower, both wrap', () => {
    const sim = renderControls();
    const seen: string[] = [];
    for (const speed of [0.25, 0.5, 1, 2, 5, 10]) {
      act(() => {
        useSimStore.setState({ speed });
      });
      const button = screen.getByRole('button', { name: /^Speed/ });
      seen.push(button.textContent);
      fireEvent.click(button);
    }
    expect(seen).toEqual(['0.25×', '0.5×', '1×', '2×', '5×', '10×']);
    expect(sim.setSpeed.mock.calls.map(([s]) => s)).toEqual([0.5, 1, 2, 5, 10, 0.25]);

    sim.setSpeed.mockClear();
    act(() => {
      useSimStore.setState({ speed: 0.25 });
    });
    fireEvent.click(screen.getByRole('button', { name: /^Speed/ }), { shiftKey: true });
    expect(sim.setSpeed).toHaveBeenCalledWith(10);
  });

  it('shows the sim clock as mm:ss.mmm', () => {
    renderControls();
    const clock = screen.getByRole('timer', { name: 'Sim time' });
    expect(clock.textContent).toBe('00:00.000');
    act(() => {
      useSimStore.setState({ t: 83_045 });
    });
    expect(clock.textContent).toBe('01:23.045');
  });
});

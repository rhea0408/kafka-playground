// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ActionsProvider } from '../../actions/actions-context.tsx';
import type { Actions } from '../../actions/actions.ts';
import { PALETTE_MIME } from '../../canvas/canvas-commands.ts';
import { resetTopologyStore } from '../../stores/topology-store.ts';
import { Palette } from './Palette.tsx';

function renderPalette(addComponent: Actions['addComponent']) {
  render(
    <ActionsProvider actions={{ addComponent } as Actions}>
      <ReactFlowProvider>
        <Palette />
      </ReactFlowProvider>
    </ActionsProvider>,
  );
}

describe('<Palette>', () => {
  beforeEach(() => {
    resetTopologyStore();
  });
  afterEach(() => {
    cleanup();
  });

  it('items are buttons that add at the canvas centre (keyboard access)', () => {
    const add = vi.fn<Actions['addComponent']>(() => Promise.resolve({ ok: true, id: 'p1' }));
    renderPalette(add);
    const button = screen.getByRole('button', { name: 'Producer' });
    expect(button.getAttribute('aria-describedby')).toBe('palette-hint');
    // Enter / Space on a <button> fire click.
    fireEvent.click(button);
    // jsdom has no layout: canvas 0 × 0 at zoom 1, so the centre is (0, 0) and the 150 × 72
    // producer is centred on it and snapped to the 16 px grid.
    expect(add).toHaveBeenCalledWith('producer', { x: -80, y: -32 });
  });

  it('puts the component kind in the drag payload', () => {
    renderPalette(vi.fn<Actions['addComponent']>());
    const data = new Map<string, string>();
    const dataTransfer = {
      setData: (type: string, value: string) => data.set(type, value),
      effectAllowed: 'none',
    };
    fireEvent.dragStart(screen.getByRole('button', { name: 'Topic' }), { dataTransfer });
    expect(data.get(PALETTE_MIME)).toBe('topic');
    expect(dataTransfer.effectAllowed).toBe('copy');
  });
});

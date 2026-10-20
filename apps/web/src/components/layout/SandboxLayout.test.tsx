// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ActionsProvider } from '../../actions/actions-context.tsx';
import type { Actions } from '../../actions/actions.ts';
import { resetSimStore } from '../../stores/sim-store.ts';
import { pushToast, resetUiStore, setBottomTab, useUiStore } from '../../stores/ui-store.ts';
import { SandboxLayout } from './SandboxLayout.tsx';

function renderLayout() {
  render(
    <ActionsProvider actions={{} as Actions}>
      <SandboxLayout />
    </ActionsProvider>,
  );
}

describe('<SandboxLayout>', () => {
  beforeEach(() => {
    resetUiStore();
    resetSimStore();
  });
  afterEach(() => {
    cleanup();
  });

  it('has the screen 05 regions and the four palette shells', () => {
    renderLayout();
    expect(screen.getByRole('banner')).toBeTruthy();
    expect(screen.getByRole('main', { name: 'Canvas' })).toBeTruthy();
    const palette = screen.getByRole('complementary', { name: 'Palette' });
    expect(
      within(palette)
        .getAllByRole('listitem')
        .map((li) => li.textContent),
    ).toEqual(['Producer', 'Topic', 'Broker', 'Consumer']);
  });

  it('right panel tabs follow and set uiStore.rightTab', () => {
    renderLayout();
    const tabs = within(screen.getByRole('tablist', { name: 'Right panel' })).getAllByRole('tab');
    expect(tabs.map((t) => t.textContent)).toEqual(['Properties', 'Partition log']);
    expect(tabs[0]?.getAttribute('aria-selected')).toBe('true');
    // Radix activates a tab on mousedown.
    fireEvent.mouseDown(tabs[1] as HTMLElement);
    expect(useUiStore.getState().rightTab).toBe('partition-log');
    expect(tabs[1]?.getAttribute('aria-selected')).toBe('true');
  });

  it('bottom panel tabs are in ui-spec order and follow uiStore.bottomTab', () => {
    renderLayout();
    const list = screen.getByRole('tablist', { name: 'Bottom panel' });
    const tabs = within(list).getAllByRole('tab');
    expect(tabs.map((t) => t.textContent)).toEqual(['Event timeline', 'Event list', 'Lag chart']);
    expect(within(list).getByRole('tab', { selected: true }).textContent).toBe('Event list');
    act(() => {
      setBottomTab('lag');
    });
    expect(within(list).getByRole('tab', { selected: true }).textContent).toBe('Lag chart');
    fireEvent.mouseDown(tabs[0] as HTMLElement);
    expect(useUiStore.getState().bottomTab).toBe('timeline');
  });

  it('shows toasts inside the canvas region', () => {
    renderLayout();
    act(() => {
      pushToast('warning', 'Consumers read from topics.');
    });
    const canvas = screen.getByRole('main', { name: 'Canvas' });
    expect(within(canvas).getByRole('status').textContent).toContain('Consumers read from topics.');
  });
});

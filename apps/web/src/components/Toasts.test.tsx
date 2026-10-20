// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { pushToast, resetUiStore, TOAST_DURATION_MS, useUiStore } from '../stores/ui-store.ts';
import { Toasts } from './Toasts.tsx';

describe('<Toasts>', () => {
  beforeEach(() => {
    resetUiStore();
    vi.useFakeTimers();
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('renders a polite live region even when empty', () => {
    render(<Toasts />);
    const region = screen.getByRole('status');
    expect(region.getAttribute('aria-live')).toBe('polite');
    expect(region.textContent).toBe('');
  });

  it('shows a warning with a text label, not color alone, and dismisses it after the timeout', () => {
    render(<Toasts />);
    act(() => {
      pushToast('warning', 'Consumers read from topics.');
    });
    const region = screen.getByRole('status');
    expect(region.textContent).toContain('Warning:');
    expect(region.textContent).toContain('Consumers read from topics.');

    act(() => {
      vi.advanceTimersByTime(TOAST_DURATION_MS - 1);
    });
    expect(useUiStore.getState().toasts).toHaveLength(1);
    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(useUiStore.getState().toasts).toHaveLength(0);
    expect(region.textContent).toBe('');
  });

  it('closes on the dismiss button', () => {
    render(<Toasts />);
    act(() => {
      pushToast('warning', 'Nope.');
    });
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(useUiStore.getState().toasts).toHaveLength(0);
  });
});

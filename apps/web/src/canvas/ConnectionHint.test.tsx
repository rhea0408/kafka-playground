// @vitest-environment jsdom
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { ConnectionReason } from './ConnectionHint.tsx';

describe('<ConnectionReason>', () => {
  afterEach(() => {
    cleanup();
  });

  it('shows the reason in words next to the pointer, with an icon (not color alone)', () => {
    render(
      <ConnectionReason reason="A producer can't send straight to a consumer." x={100} y={40} />,
    );
    const hint = screen.getByTestId('connection-reason');
    expect(hint.textContent).toBe("⚠Can't connect. A producer can't send straight to a consumer.");
    expect(hint.style.left).toBe('116px');
    expect(hint.style.top).toBe('56px');
    // The toast on drop announces the reason; the moving hint stays out of the a11y tree.
    expect(hint.getAttribute('aria-hidden')).toBe('true');
  });
});

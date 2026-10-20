import { useEffect } from 'react';
import { dismissToast, TOAST_DURATION_MS, useUiStore, type Toast } from '../stores/ui-store.ts';

/**
 * Warning toasts (ui-spec: 340 × 40). The live region is always rendered so screen readers
 * announce toasts added later (role="status" is polite). Each toast dismisses itself after
 * `TOAST_DURATION_MS` and has a close button. Placement is provisional until the layout (T6).
 */
export function Toasts() {
  const toasts = useUiStore((s) => s.toasts);
  return (
    <div
      role="status"
      aria-live="polite"
      className="pointer-events-none fixed bottom-4 left-1/2 z-50 flex -translate-x-1/2 flex-col gap-2"
    >
      {toasts.map((toast) => (
        <ToastItem key={toast.id} toast={toast} />
      ))}
    </div>
  );
}

function ToastItem({ toast }: { toast: Toast }) {
  useEffect(() => {
    const timer = setTimeout(() => {
      dismissToast(toast.id);
    }, TOAST_DURATION_MS);
    return () => {
      clearTimeout(timer);
    };
  }, [toast.id]);

  const warning = toast.kind === 'warning';
  return (
    // Width 340, height 40 for one line; longer engine reasons wrap rather than being cut off.
    <div
      className={`pointer-events-auto flex min-h-10 w-[340px] items-center gap-2 rounded border px-3 py-1.5 text-sm ${
        warning
          ? 'border-danger bg-danger-bg text-danger-text'
          : 'border-border bg-surface text-status'
      }`}
    >
      {/* Icon + word, never color alone (ui-spec accessibility rules). */}
      <span aria-hidden="true">{warning ? '⚠' : 'ℹ'}</span>
      <span className="sr-only">{warning ? 'Warning: ' : 'Note: '}</span>
      <span className="flex-1">{toast.message}</span>
      <button
        type="button"
        aria-label="Dismiss"
        className="shrink-0 rounded px-1 leading-none"
        onClick={() => {
          dismissToast(toast.id);
        }}
      >
        ×
      </button>
    </div>
  );
}

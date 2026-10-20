/**
 * Browser APIs React Flow needs that jsdom does not have (M2-T7). Only installed in jsdom test
 * files; Node-environment tests have no `window` and skip this.
 */

if (typeof window !== 'undefined') {
  if (!('ResizeObserver' in window)) {
    class ResizeObserverStub {
      observe(): void {
        /* jsdom has no layout, so nothing ever resizes. */
      }
      unobserve(): void {
        /* nothing observed */
      }
      disconnect(): void {
        /* nothing observed */
      }
    }
    Object.defineProperty(window, 'ResizeObserver', { value: ResizeObserverStub, writable: true });
    Object.defineProperty(globalThis, 'ResizeObserver', {
      value: ResizeObserverStub,
      writable: true,
    });
  }
  if (!('DOMMatrixReadOnly' in window)) {
    // React Flow reads the viewport transform's scale (m22) from computed styles.
    class DOMMatrixReadOnlyStub {
      readonly m22: number = 1;
    }
    Object.defineProperty(window, 'DOMMatrixReadOnly', {
      value: DOMMatrixReadOnlyStub,
      writable: true,
    });
  }
}

export {};

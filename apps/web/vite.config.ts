import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // Engine and level code run in a Web Worker as ES modules (architecture §3).
  worker: { format: 'es' },
  test: {
    // Playwright specs live in e2e/ and are run by `pnpm e2e`, not Vitest.
    include: ['src/**/*.test.{ts,tsx}'],
    // ResizeObserver & co. for React Flow in jsdom component tests (no-op in Node tests).
    setupFiles: ['src/test/jsdom-setup.ts'],
  },
});

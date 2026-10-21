# @kp/web

The React app: canvas, panels and the worker bootstrap (M2). Vite + React 18 + Tailwind v4.

```
pnpm dev                                   # from the repo root, or `pnpm --filter @kp/web dev`
pnpm --filter @kp/web build                # production build into dist/
pnpm --filter @kp/web e2e                  # Playwright tests (Chromium), via vite preview; layout screenshots go to e2e-screenshots/ (gitignored)
pnpm --filter @kp/web exec playwright install chromium   # once, locally
```

- **Theme:** `src/index.css` defines the `docs/ui/ui-spec.md` color tokens in a Tailwind `@theme` block, which gives both CSS variables (`var(--color-primary)`) and utilities (`bg-primary`, `border-border`, …). The default Tailwind palette is switched off and ESLint rejects color literals in `src/`, so every color comes from a token.
- **TypeScript:** `tsconfig.app.json` covers `src/` (DOM + JSX), `tsconfig.node.json` covers the Vite/Playwright configs and `e2e/`.
- **Workers:** files named `*.worker.ts` get worker globals in ESLint and may not use `window` or `document`.

The store layout, command path and worker frame protocol are documented here in M2-T13.

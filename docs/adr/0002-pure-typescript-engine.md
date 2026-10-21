# ADR 0002: Pure-TypeScript engine shared by browser, Node and harness

- **Status:** Accepted
- **Date:** 2026-10-21 (written down in M2-T13; the decision is from the architecture draft, §1 "Key principles" and §12 item 2, and was built in M1)
- **Milestone:** M1 (engine core), M2 (web worker host)

## Context

The same simulation has to run in three places: the browser's Web Worker (ADR 0001), Node for unit, property and determinism tests and for headless level verification (`pnpm levels:verify`, M6), and the fidelity harness's TS runner, which compares the engine with real Kafka (M5). If each place had its own copy or its own adapter, they could drift apart, and a green test in Node would say nothing about the browser.

## Decision

1. **`packages/engine` (and later `packages/level-runtime`) are pure TypeScript with no DOM, browser or Node APIs and no React.** The engine's only dependency is `@kp/schema` (Zod schemas and connection rules shared with the UI).
2. **The API is plain data in, plain data out:** `createEngine({ seed, topology })`, `dispatch(command)`, `step`, `runUntil(t)`, `on(listener)`, `getSnapshot()`, `commandLog()`, `replayCommandLog(...)`, `diffSnapshot` / `applyDiff`, `formatEvent`, `hashEvents`. Commands and events are discriminated unions; snapshots are structured-clone friendly, so they cross `postMessage` unchanged.
3. **Rules enforced by tooling**, not just convention (root `eslint.config.js`): headless packages (`engine`, `level-runtime`, `schema`) may not use browser globals (`window`, `document`, `setTimeout`, `performance`, `console`, …) or import React; deterministic packages (`engine`, `level-runtime`) may not use `Math.random`, `Date.now`, `new Date()`, `performance.now` or `crypto` randomness. Code that needs wall time or Node (the demo, the snapshot benchmark) lives in `packages/engine/scripts/`, outside `src/`.
4. Workspace packages export their TypeScript source (`"exports": { ".": "./src/index.ts" }`). Vite bundles it for the worker; Node 22 and Playwright's loader run it directly.

## Consequences

- One engine everywhere. The M2-T12 e2e tests run the default scene in the bundled worker and in Node from source, and the event-log hashes match.
- Tests are fast and need no browser: 179 engine tests and the fast-check invariants run in Node in seconds.
- Kafka behaviour lives in one place (`protocols/`), with the Kafka doc or KIP cited in comments, so the fidelity harness checks the same code the player sees.
- The UI can't take shortcuts into engine state: it gets copies (targeted copy, frozen log records, M2-T3) and sends commands.
- Anything browser-specific (timers, workers, Comlink, rendering) stays in `apps/web`.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Engine in another language** (Go or Rust compiled to WebAssembly, sharing code with `lab/`) | One more toolchain and a serialisation boundary for every event; the team works in TypeScript and the UI types would have to be generated. |
| **Engine inside the React app** | Couples simulation to rendering, can't run headless in CI or in the harness, and makes determinism hard to guarantee. |
| **Separate engines for browser and tests** | They would drift; a passing Node test would prove nothing about the browser. |

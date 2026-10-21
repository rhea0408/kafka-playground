# @kp/web

The React app: canvas, panels and the worker bootstrap (M2). Vite + React 18 + Tailwind v4, React Flow for the canvas, a Canvas2D overlay for message dots, Zustand stores and a Comlink worker running `@kp/engine`.

## Scripts

```
pnpm dev                                   # from the repo root, or `pnpm --filter @kp/web dev`
pnpm --filter @kp/web build                # production build into dist/
pnpm --filter @kp/web preview              # serve dist/ (vite preview)
pnpm --filter @kp/web test                 # Vitest: Node tests + jsdom component tests (src/**/*.test.ts(x))
pnpm --filter @kp/web typecheck            # app, node (configs + e2e) and worker tsconfigs
pnpm --filter @kp/web e2e                  # Playwright (Chromium) against `build` + `preview` on 127.0.0.1:4173
pnpm --filter @kp/web exec playwright install chromium   # once, locally
pnpm test:coverage                         # from the root: coverage for every package into <package>/coverage/
```

Playwright screenshots go to `e2e-screenshots/` (gitignored). The first local e2e run builds the app; locally under WSL the web server start sometimes takes ~2 min instead of ~2 s (M2-decisions T0-10).

## Layout of `src/`

| Folder           | What                                                                                                                                                                                                                                                                                           |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sim/`           | Worker side and its client: `sim-host.ts` (run loop, `SimHost`), `sim.worker.ts` (Comlink expose), `sim-client.ts` (creates the worker, `Comlink.wrap`), `protocol.ts` (frame and API types), `bootstrap.ts` (`startSim()`), `debug-hook.ts` (`window.__kp.sim`), `speed.ts`, `ring-buffer.ts` |
| `stores/`        | Zustand stores (below) and `default-scene.ts`                                                                                                                                                                                                                                                  |
| `actions/`       | The command path (`createActions`) and its React context (`useActions()`)                                                                                                                                                                                                                      |
| `canvas/`        | React Flow canvas, nodes, connection checks, minimap, `live/` (per-node snapshot slices) and `overlay/` (message dots)                                                                                                                                                                         |
| `properties/`    | Schema-driven properties panel, partitions stepper, manual produce form                                                                                                                                                                                                                        |
| `events/`        | Event list: windowing, Follow, type filter                                                                                                                                                                                                                                                     |
| `partition-log/` | Partition log tab (records, HW / committed / LEO markers)                                                                                                                                                                                                                                      |
| `components/`    | Layout shell (top bar, palette, panels), sim controls, toasts                                                                                                                                                                                                                                  |

## Stores

Three stores in `src/stores/`. State objects are plain data; updates are exported functions (`applySimFrame`, `pushToast`, `select`, …), so they can be called outside React.

| Store              | Holds                                                                                                                                                                  | Written by                                                                                                           |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `useTopologyStore` | What is on the canvas: components (parsed configs, schema defaults filled in, insertion order), connections, node positions                                            | The actions module, **only after the engine accepted the command**. Positions are UI-only and never reach the engine |
| `useSimStore`      | The running simulation: latest snapshot (diffs applied), the newest 10,000 events (`EVENT_LIST_SIZE` = the host's ring buffer), sim time, playing, speed, frame number | Only `applySimFrame`, fed by worker frames. The UI never writes runtime state                                        |
| `useUiStore`       | Selection, right / bottom tab, toasts, reduced motion, event-type filter, partition-log target                                                                         | UI components                                                                                                        |

Topology (what the user built) lives in the UI; runtime state (offsets, lag, logs) comes only from engine snapshots (architecture §5). Nodes subscribe to their own snapshot slice (`canvas/live/use-live.ts`), so a frame re-renders only what changed. Per-frame consumers that must not cause React renders (the overlay) use `onSimFrame(listener)`, which gets every frame's raw events.

## The command path

```
canvas / panel edit → actions (actions/actions.ts) → SimPort.dispatch(command) ── Comlink ──▶ SimHost → engine.dispatch
                                                    ◀─ { ok: true } → topologyStore updated
                                                    ◀─ { ok: false, reason } → warning toast with the engine's reason, store unchanged
```

- Every canvas edit becomes an engine command (`AddComponent`, `RemoveComponent`, `Connect`, `Disconnect`, `UpdateConfig`, `ProduceManual`, `KillConsumer`, `RestartConsumer`). `moveNode` is UI-only and sends nothing.
- The actions depend on a `SimPort` (the subset of `SimApi` they use, sync or async), so the app uses the Comlink `Remote<SimApi>` and the tests use an in-process `SimHost` through the same code. A thrown error (dead worker) becomes `{ ok: false, reason }` and a toast.
- **Play, Pause, Step and Speed are host controls, not commands.** They change how fast the worker drives the engine, never the outcome, so they are not in the command log.
- **Reset** calls `init({ seed, topology: <current canvas> })`: the sim restarts at t = 0 with empty logs and a cleared command log, and the canvas the user built stays (M2-decisions T5-6).

## Worker frame protocol

`src/sim/protocol.ts` defines it; the host is `SimHost` in `sim-host.ts`, a plain class with an injected clock, so it runs in Node tests.

- **Run loop:** the worker ticks about every 16 ms (`setTimeout`), runs the engine to `now + wall delta × speed` with `runUntil` (each tick capped at 250 ms of wall time), and buffers the events.
- **Frames** are pushed through a `Comlink.proxy` callback registered with `subscribe` (it returns a token for `unsubscribe`):
  - `{ kind: 'full', snapshot, history }` after `init` / `reset` and as a new subscriber's first frame. `history` is the ring buffer's already-delivered events, for the event list only: never animate them.
  - `{ kind: 'diff', diff }` otherwise; `applyFrame(prev, frame)` rebuilds the snapshot with the engine's `applyDiff`.
  - Every frame also carries `frame` (host-wide counter), `t`, `playing`, `speed` and `events` (every event since the previous frame, in order, never dropped or repeated).
- **Rate:** at most one loop frame per 33 ms, and only when something changed. User actions (`play`, `pause`, `step`, `stepBy`, `setSpeed`, a `dispatch` while paused) send a frame at once.
- **Ring buffer:** the host keeps the newest 10,000 events (`EVENT_HISTORY_SIZE`, NFR-2.9) for late subscribers and `eventHistory()`. Snapshots carry the newest 200 records per partition (`SNAPSHOT_RECORDS_PER_PARTITION`).
- **No formatted text in frames:** the main thread formats events itself with the engine's `formatSimTime` / `describeEvent`.
- `eventLogHash()` replays the command log on a fresh engine and hashes the events (FNV-1a, engine `hashEvents`). It is for tests and diagnostics.

## How the overlay gets edge geometry

`canvas/overlay/` draws message dots on one `<canvas>` inside `<ReactFlow>` (above nodes and edges, `pointer-events: none`). Everything except `draw.ts` and `MessageOverlay.tsx` is pure and tested in Node.

- **Endpoints** come from React Flow's internal node lookup, **every frame**: `resolverFor(store.getState().nodeLookup)` reads `internals.positionAbsolute` and `internals.handleBounds` and finds the handle by id (`anchors.ts`, same rule as React Flow's `getHandlePosition`). So dots stay on their path while nodes are dragged.
  - A1: producer `out` handle → the partition row's `p<i>:in` anchor. A2: `p<i>:out` → the consumer row's `row:<consumerId>` handle.
  - The `p<i>:in` / `p<i>:out` anchors are invisible, non-connectable handles on each partition row (`canvas/geometry.ts`), so React Flow measures them with the node.
  - **Fallback:** if a node has no measured handle bounds yet, `anchors.ts` computes a partition anchor from the node's position and width and `partitionRowCenterY` (the layout constants).
- **Path:** React Flow's default bezier (`path.ts`, same control points as `getBezierPath`), walked by arc length. Dots are in **flow coordinates** (16 flow px, 120 flow px per wall second × sim speed) and drawn with React Flow's viewport transform (`[x, y, zoom]`) and `devicePixelRatio`.
- **Lifecycle** (`dots.ts`): A1 starts on `RecordProduced`; A2 starts on `RecordConsumed` only after the A1 dot with the same trace id has arrived; cap 500 dots; pause freezes; a full frame (Reset) clears. Dots are decorative (ADR 0008).

## Default scene

The Sandbox opens **paused** with the default scene (`stores/default-scene.ts`): broker `b1`, topic `orders` with 3 partitions, producer `p1` at 2 msg/s with random keys **D, E, F** (they hash to partitions 0, 1, 2; A, B, C would all land on partition 1), and consumer `c1` in group `orders-app`. Every other field is the schema's Kafka default. Seed 7. Reload the page to get it back; Reset keeps whatever is on the canvas.

## Test hooks

| Hook                  | What                                                                                                                                                                                     | Where                                  |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `window.__kp.sim`     | Read-only view of the worker: `seed`, `status()`, `commandLog()`, `eventLogHash()`, `eventHistory()`. Used by the e2e determinism tests. It can't change the sim                         | `sim/debug-hook.ts`, every build       |
| `window.__kp.overlay` | Overlay counters: `dotsDrawn` (cumulative), `dotsOnScreen`, `dotsActive`, `dotsQueued`, `dotsStarted`, `skippedCap`, `skippedNoA1`, `fps`, `capChip`                                     | `canvas/overlay/debug.ts`, every build |
| `?fps`                | Shows the FPS meter (`N fps · M dots`) and enables `window.__kp.overlay.spawnTestDots(n)` (n A1 dots along the first producer → topic edge, no engine involved). Always on in `vite dev` | `canvas/overlay/debug.ts`              |
| `?scene=empty`        | Opens the bare canvas (broker `b1` only) instead of the default scene                                                                                                                    | `stores/default-scene.ts`              |

Both `window.__kp` hooks are in production builds on purpose: Playwright runs against the production bundle. `window.__kp` is typed through the global `KpDebugHooks` interface, which each module extends.

## Conventions

- **Theme:** `src/index.css` defines the `docs/ui/ui-spec.md` color tokens in a Tailwind `@theme static` block, which gives both CSS variables (`var(--color-primary)`) and utilities (`bg-primary`, `border-border`, …). The default Tailwind palette is switched off and ESLint rejects color literals in `src/`, so every color comes from a token. React Flow's colors are set through its `--xy-*` variables.
- **TypeScript:** `tsconfig.app.json` covers `src/` (DOM + JSX), `tsconfig.worker.json` covers `*.worker.ts` and what they import (WebWorker lib), `tsconfig.node.json` covers the Vite/Playwright configs and `e2e/`.
- **Workers:** files named `*.worker.ts` get worker globals in ESLint and may not use `window` or `document`.
- **Component tests** opt into jsdom with `// @vitest-environment jsdom`; `src/test/jsdom-setup.ts` stubs what React Flow needs.

## Credits

The canvas is built with [React Flow](https://reactflow.dev) (`@xyflow/react`, MIT licence, © webkid GmbH). Its attribution badge is hidden with `proOptions.hideAttribution` (M2-decisions T7-12), which the MIT licence allows; this credit stands in for it.

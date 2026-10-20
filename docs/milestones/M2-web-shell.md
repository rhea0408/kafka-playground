# Milestone 2: Web app shell

**Goal:** the engine from M1 running in a Web Worker behind a React Flow canvas. You can build producer → topic → consumer on the canvas, edit config in a properties panel, press Play and watch messages move, with live offsets and lag on the nodes.
**Done when:** a producer → topic → consumer flow animates (architecture §11); `pnpm lint && pnpm typecheck && pnpm test` and the Playwright smoke test are green in CI; the browser worker gives the same event-log hash as Node for the same seed and commands.

**In scope:** Sandbox layout (screen 05) only, one broker, one consumer per group (M1 engine limits).
**Out of scope:** consumer groups and rebalancing (M3), multi-broker, kill broker, rewind and the timeline scrubber (M4), levels, home screen and dialogues (M6), code panel (M7), persistence and autosave (M8), flow mode (v0.2).

Work through the tasks in order, one Claude Code session or PR per task. T1 records the message-timing decision (realistic engine timing, decorative dots).

---

## Decisions to make before starting (answers to M1-summary §6)

| # | Question | Recommendation |
|---|---|---|
| Q1 | `UpdateConfig` semantics | See T2. Partitions can only grow (Kafka rejects a decrease); a producer rate change restarts its schedule from now; consumer changes apply from the next poll; ids are immutable. |
| Q2 | `LagChanged`: event or derived? | **Derive it in the UI from snapshots** for A5. Lag moves on every append, so one event per append would double the event stream. Add a `LagThresholdCrossed`-style event later when the level runtime (M6) needs a trigger. Amend architecture §4.5 to match. |
| Q3 | Does `RecordConsumed` drive A2? | Yes, as a decorative dot that starts only after the record's A1 dot has arrived (T1). After T2, `RecordConsumed` is emitted when the fetch response reaches the consumer. |
| Q4 | Worker API | Agree: Comlink, with the worker **pushing** frames (`{ t, events[], diff }`) at ~30 Hz through a `Comlink.proxy` callback. The event list reuses `formatEvent`. |
| Q5 | Game-tuned labels | A field with a real Kafka equivalent but a shortened value shows a muted `game-tuned` chip and a helper line "Kafka default: 45 s". A pure game setting with no Kafka config (`ratePerSec`, `processingTimeMs`, `messageSizeBytes`) shows a `game setting` chip and a one-line tooltip saying what it stands in for. |
| Q6 | Write `docs/milestones/M2-*.md` | This file. Commit it as `docs/milestones/M2-web-shell.md`. |

**New dependencies (CLAUDE.md asks for approval):** `vite`, `@vitejs/plugin-react`, `react@18`, `react-dom@18`, `@xyflow/react`, `zustand`, `comlink`, `tailwindcss`, Radix primitives as needed (`@radix-ui/react-tabs`, `-tooltip`, `-dialog`, `-toggle`), `@playwright/test`, `@testing-library/react`, `@vitest/coverage-v8`. All of them are already in the tech stack; approve them once for this milestone.

---

### T0 · Web app scaffold
- `apps/web` (`@kp/web`): Vite + React 18 + strict TS, importing `@kp/engine` and `@kp/schema` from the workspace.
- Tailwind theme and CSS variables from the `docs/ui/ui-spec.md` color tokens (`--color-primary`, `--color-muted`, `--color-surface`, `--color-status`, `--color-danger`, `--color-border`). No hard-coded colors.
- ESLint browser config for `apps/web`; the engine's restricted-globals rule stays scoped to `packages/engine` (the worker host may use timers and `performance.now`).
- `pnpm dev`, `pnpm --filter @kp/web build`; CI also builds the web app.
- Playwright installed with one placeholder test that loads the page; CI runs it (Chromium only).
- **Coverage (NFR-4.4, open issue from M1-summary §4):** add `@vitest/coverage-v8`, run `pnpm test -- --coverage` in CI and upload the report as a build artifact. No minimum yet; set one once M2 is done and the numbers are known.
- **Pin the CI runner (open issue from M1-summary §4):** `ubuntu-latest` moved to Ubuntu 26 on 2026-10-19. Pin `runs-on: ubuntu-24.04` until the Playwright job is stable, then move to the new image in its own PR. Install browsers with `pnpm exec playwright install --with-deps chromium` so the system packages match the runner.
- **Done when:** `pnpm dev` serves an empty page with the theme applied; CI runs on the pinned runner: lint → typecheck → test (with coverage report) → build → e2e.

### T1 · Message timing: realistic engine, decorative dots (decided)
**Decision:** the engine keeps real-world timing and the dots are a visual aid, not a timing claim. The partition log, offsets and counters show the true order and timing.
- **Engine:** `networkDelayMs` stays at 1 ms. Inside one data centre a network round trip is well under 1 ms, and 1 ms is the smallest step of the integer sim clock. End-to-end, a record reaches a waiting consumer a few ms after `send()`, as in real Kafka. T2 makes the consumer side realistic too (long-poll fetch).
- **Dots** move at a fixed 120 px/s × sim speed (animations.md A1/A2). Rules so they never show the wrong order:
  1. A record's A2 dot (topic → consumer) starts only after its A1 dot (producer → topic) has arrived. Queue A2 by trace id.
  2. Log cells (A4), offsets, lag and counters update straight from snapshots and never wait for dots.
  3. If dots fall behind the engine (high rate, long edges), skip dots rather than slow the display down. The 500-dot cap from T9 still applies.
  4. A legend or tooltip on the overlay says: "Dots are slowed down so you can see them. Real delivery takes a few milliseconds." (NFR-3.3)
- **Done when:** the decision is recorded in `docs/adr/0008-message-timing.md` and animations.md A1/A2 state that dots are decorative and ordered by trace id.
- **Learning checkpoint:** In real Kafka, what happens between `send()` returning and the record being readable by a consumer? Which of those steps does the engine model, and roughly how long does each one take?

### T2 · Engine additions for the UI (engine PR)
- `UpdateConfig { id, patch }` command, validated with `@kp/schema` (`parseComponent` on the merged config):
  - **Topic partitions:** increase only. A decrease is rejected with the reason Kafka gives (`InvalidPartitionsException`: partitions can only be increased). New partitions start empty on the topic's broker. Producers' sticky state resets for that topic. Existing records stay where they are.
  - **Producer:** rate / key strategy / acks / batch.size / message size. A rate change restarts the schedule from now (new incarnation, stats kept). In-flight requests keep their old `acks`.
  - **Consumer:** `maxPollRecords`, `fetchMaxWaitMs`, `fetchMinBytes`, `processingTimeMs`, auto-commit settings apply from the next poll. Changing `groupId` or `autoOffsetReset` is rejected in M2 (it means leaving or joining a group, which is M3).
  - Ids are immutable; RF > 1 stays rejected.
- `ConfigUpdated` event (`{ id, before, after }`).
- **Realistic consumer fetch (replaces the `pollIntervalMs` game setting):** model the fetch as Kafka's long poll (FetchRequest; ConsumerConfig `fetch.min.bytes`, `fetch.max.wait.ms`).
  - poll() sends a fetch request to the partition leader (+ network delay).
  - If at least `fetch.min.bytes` (default 1) are available, the broker answers at once. Otherwise it holds the request for up to `fetch.max.wait.ms` (default 500 ms) and answers as soon as enough data is appended.
  - The response arrives after another network delay; `RecordConsumed` is emitted when poll() hands the records to the application.
  - Remove `pollIntervalMs` from `@kp/schema` and add `fetchMinBytes` and `fetchMaxWaitMs` with the real defaults. This is a schema change; there are no save files yet, so nothing needs migrating.
  - Update the "consumer fetching" entry in `docs/simplifications.md`. Prefetching (the real client sends the next fetch while the application is still processing) stays a documented simplification unless it turns out to be cheap.
- Trace ids made unique across producer lives (e.g. `p1@3#7` with the incarnation), because the overlay keys dots by trace id (see M1-summary §4).
- **Tests:** unit tests for each rule above; extend the fast-check arbitraries with `UpdateConfig` so the invariants and the determinism test cover it; a test that adding partitions remaps a key and leaves old records in place.
- **Learning checkpoint:** Why can't you reduce a topic's partition count? After adding partitions, which records for key `B` are where, and what does that do to per-key ordering for a consumer?

### T3 · Snapshot diff
- Replace the JSON deep copy in `getSnapshot()` with a targeted copy.
- `diffSnapshot(prev, next)` and `applyDiff(prev, diff)` in `packages/engine/src/snapshot`: changed producers, consumers and partition summaries by id; for logs, only records with offsets above what `prev` held (logs are append-only, so that is enough); removed ids listed explicitly.
- **Tests:** a property test that `applyDiff(prev, diffSnapshot(prev, next))` deep-equals `next` over random runs; a benchmark sanity check (e.g. 1,000 partitions, snapshot + diff well under one 33 ms frame).

### T4 · Worker host and Comlink API
- `apps/web/src/sim/`: a plain `SimHost` class (testable in Node without a Worker) plus a thin Comlink wrapper in `sim.worker.ts`.
- API: `init({ seed, topology })`, `dispatch(cmd) → DispatchResult`, `play()`, `pause()`, `step()` (next task), `stepBy(ms)`, `setSpeed(0.25–10)`, `reset()`, `subscribe(proxy(onFrame))`.
- Run loop: on each tick, advance sim time by wall-clock delta × speed with `runUntil`, buffer events, and send at most one frame per ~33 ms with the diff and the events since the last frame. Speed, pause and step are host concerns, never engine commands (matches M1 deviation).
- `reset()` builds a fresh engine from the same seed and initial topology and clears the command log.
- Bounded event buffer on the host (ring buffer, NFR-2.9).
- **Tests:** `SimHost` with a fake clock: speed scaling, pause stops sim time, step processes exactly one task, frames carry every event exactly once and in order.
- **Learning checkpoint:** Why can the worker host read `performance.now()` when the engine can't? Why are speed and pause not in the command log?

### T5 · Stores and the command path
- Zustand stores (architecture §5):
  - `topologyStore`: node positions and component configs. The UI owns this; positions never go to the engine.
  - `simStore`: latest snapshot (diffs applied), bounded event list, sim time, play state, speed.
  - `uiStore`: selection, active tabs, toasts, reduced-motion setting.
- One `actions` module turns every canvas edit into an engine command. It updates `topologyStore` only when `dispatch` returns `ok`; a rejection shows a warning toast (340 × 40) with the engine's reason.
- **Tests:** an action round trip against a real `SimHost` (accepted → store updated; rejected → store unchanged, toast shown).

### T6 · Sandbox layout shell (screen 05)
- Top bar 56 px with title and sim controls (40 × 40 buttons, speed 56 × 40, gap 8, vertically centered, 8 px from the right edge).
- Palette 240 px (left, full height), canvas flex, right panel 360 px (tabs: Properties · Partition log), bottom panel 200 px (tabs: Event list · Lag chart · Event timeline).
- Reference 1440 × 900, works at 1366 × 768 with no horizontal scroll.
- Sim controls wired to the worker: Play/Pause (active Play in primary), Step, Reset, speed (0.25× / 0.5× / 1× / 2× / 5× / 10×), sim clock `mm:ss.mmm`. Real `<button>`s with `aria-label`s.
- **Done when:** the layout matches wireframe 05 at both sizes (compare with screenshots).

### T7 · Canvas: nodes, palette, connections
- React Flow custom nodes at ui-spec sizes: Producer 150 × 72; Topic 230 × (36 + 48 per partition); Broker 226 × 84; Consumer shown as a group node (220–240 wide, header 36, one 196 × 40 row), so M3 only adds rows.
- Palette items 208 × 56 for Producer, Topic, Broker, Consumer; drag onto the canvas → `AddComponent` with schema defaults and a generated id.
- Connecting handles: check `canConnect` from `@kp/schema` before dispatching; an invalid link shows its reason (FR-1.2).
- Select, multi-select, delete (→ `RemoveComponent` / `Disconnect`), snap to grid, dotted grid background.
- Zoom controls (160 × 40: −, fit, +, map toggle) and the minimap (160 × 84, bottom-right), which appears only when content is off-screen, with a 200 ms fade (FR-1.7, B11).
- **Tests:** component tests for the connection-reason UI; Playwright: drag a producer and a topic, connect them, see the edge.

### T8 · Live state on nodes
- From the snapshot (FR-1.4): each topic partition row shows LEO, HW and the last N log cells (18 × 18, key letter in each cell, A4 slide-in); the consumer row shows status, position, committed offset and a lag badge; the producer shows sent/acked/failed.
- Lag badge (A5): derived from snapshot lag; the number counts to its new value over 300 ms and uses the danger color above a threshold. It always shows the number too, never color alone (NFR-7.1).
- Consumer stopped/running uses an icon + label. Kill / Restart consumer from a hover toolbar (engine commands from M1).
- Reduced motion: A4 and A5 become instant changes or a 150 ms fade.

### T9 · Message overlay (Canvas2D)
- One `<canvas>` above React Flow, sized to the viewport and following pan and zoom.
- A1: `RecordProduced` → a 16 px dot with the key letter (or `∅` for null keys) from the producer to the target partition row. A2: `RecordConsumed` → partition row to the consumer row, started only after the A1 dot with the same trace id has arrived (T1).
- Edge geometry from React Flow's node positions and handle bounds; dots stay on the edge path while nodes move.
- Dots are keyed by trace id (unique after T2). Pause freezes them; speed changes rescale them; Reset clears them.
- Cap at 500 dots (NFR-2.5): beyond that, skip new dots and show a small "too many to animate" chip. Flow mode is v0.2.
- Reduced motion: no movement; dots fade in at their destination for 150 ms.
- **Tests:** unit tests for path interpolation and the dot lifecycle; a dev-only FPS meter; manual check of 60 fps with 500 dots on the target hardware (NFR-2.1).

### T10 · Properties panel
- Opens on selection (FR-1.3). Fields render from `@kp/schema` (rows 328 × 56) with inline Zod validation; save → `UpdateConfig`; a rejected update shows the reason next to the field.
- Topic: stat tiles (104 × 48: partitions, LEO total, consumers), a partitions stepper with the warning "Adding partitions changes which partition each key goes to" (FR-4.7), and Delete.
- Game-tuned and game-setting labels as in Q5 (CLAUDE.md rule 6, NFR-3.3).
- Fields for things the engine doesn't model yet (RF > 1, min ISR, retention, idempotence…) are shown disabled with a "coming in M4" style tooltip, or hidden. Pick one and apply it everywhere.

### T11 · Bottom and right panels
- **Event list** (B7): formatted with `formatEvent`, newest at the bottom, Follow toggle, virtualised, bounded to the host's ring buffer; a filter by event type.
- **Partition log** tab (screen 05c, FR-6.1): partition picker, summary (leader, LEO, HW, committed), rows 328 × 28 with offset/key/value/timestamp, HW and committed marker rows 328 × 22 (B9). It is the clearest view of offsets for learning, so it is worth having here even though only the event list is required for "done".
- **Lag chart** and **Event timeline**: placeholder tabs ("arrives in M4") unless time allows (uPlot is approved).
- **Manual produce** form (FR-6.5) on the producer's properties → `ProduceManual`.
- **Learning checkpoint:** In the partition log, why does the committed marker trail the position, and when does it jump?

### T12 · Default scene, end-to-end test, worker determinism
- A default Sandbox scene: producer (2 msg/s) → topic `orders` (3 partitions) → consumer. **Use keys that actually spread over 3 partitions.** A/B/C all hash to partition 1 (confirmed again in this review), but D → 0, E → 1, F → 2. Note the same thing for M6 level design, because the wireframes use A/B/C dots.
- Playwright smoke test: open the app → Play → within N seconds the overlay has drawn dots (expose a dev-only counter), every partition's LEO > 0 and consumer lag stays bounded.
- **Worker determinism test:** run the same seed and command log in the browser worker and in Node, and compare the event-log hashes. This proves the bundled engine behaves the same in the browser.

### T13 · Docs
- `apps/web/README.md`: store layout, the command path, the worker frame protocol, how the overlay gets edge geometry.
- ADRs 0001–0003 (architecture §12: worker DES, pure-TS engine, React Flow + Canvas2D) and 0008 (message timing) from T1.
- `docs/simplifications.md` updated for `UpdateConfig` and the long-poll fetch.
- Architecture §4.4/§4.5 and animations.md updated for the decisions above; `docs/milestones/M2-summary.md`; CLAUDE.md "Current milestone" → M3.
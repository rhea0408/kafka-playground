# ADR 0001: Discrete-event simulation in a Web Worker

- **Status:** Accepted
- **Date:** 2026-10-21 (written down in M2-T13; the decision is from the architecture draft, §1, §4.1 and §12 item 1, and was built in M1 and M2)
- **Milestone:** M1 (engine core), M2-T4 (worker host)

## Context

The playground has to show how Kafka behaves over time: records produced every few milliseconds, fetches parked for `fetch.max.wait.ms`, auto-commits every 5 s, later session timeouts and leader elections. The same run must be repeatable (NFR-4.1): replay, the "replay the same failure" step of the Fix phase, level verification in CI and the fidelity harness all need the same seed and commands to give the same event log. The browser main thread also has to keep the canvas at 60 fps (NFR-2.1) while the simulation runs at up to 10× speed.

## Decision

1. **The engine is a discrete-event simulation (DES).** It has an integer sim clock in milliseconds and a priority queue of tasks ordered by `(time, seq)`; `seq` breaks ties, so the order is always the same. Every behaviour is a task that schedules later tasks (`packages/engine/src/core/`: clock, heap, loop, seeded sfc32 RNG). The engine has no timers and never reads wall time.
2. **The engine runs in a dedicated Web Worker**, owned by a host (`apps/web/src/sim/sim-host.ts`, `sim.worker.ts`):
   - The host maps wall time to sim time: each tick (about every 16 ms) it runs the engine to `t + wall delta × speed` with `runUntil`. A tick is capped at 250 ms of wall time.
   - **Speed (0.25×–10×), pause and step are host concerns, not engine commands.** They change how fast the engine is driven, not what happens in it, so they are not in the command log.
   - The host pushes frames to the main thread over Comlink about 30 times a second: a snapshot diff plus every event since the previous frame (frame protocol in `apps/web/README.md`).
3. The main thread sends commands and receives events and snapshots; it never changes engine state (CLAUDE.md engine rule 3).

## Consequences

- Runs are deterministic: same seed + same command log ⇒ byte-identical event log. CI checks this in Node (`determinism.test.ts`) and in the browser: the M2-T12 Playwright tests compare the bundled worker engine's hash with Node's.
- The sim clock can jump straight to the next task, so idle periods cost nothing and a 5-minute `metadata.max.age.ms` is just a number. Real Kafka defaults can stay real (ADR 0008).
- Engine work never blocks rendering. The price is an async boundary: every UI action is a `postMessage` round trip, and snapshots must be cheap to diff and clone (M2-T3: targeted snapshot copy and `diffSnapshot` / `applyDiff`).
- Tick size doesn't change results: "run until t" gives the same events however the time is cut into ticks, which the `SimHost` tests check.
- The host may use `performance.now` and timers; the engine may not. ESLint enforces the split (determinism rules apply to `packages/engine` and `packages/level-runtime` only).

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Fixed time-step loop** (advance every component every N ms) | Wastes work when nothing happens, and needs a step small enough for 1 ms network hops. Timeouts measured in minutes would take millions of steps. |
| **Real timers** (`setTimeout` per message) | Not deterministic, can't be sped up or stepped, and can't run headless in CI at full speed. |
| **Run the engine on the main thread** | Simpler, but a busy simulation at 10× would compete with React and the overlay for the 16 ms frame budget. |
| **Speed / pause as engine commands** | They would enter the command log, and a replay would depend on how fast the user's machine was. M1 already chose host controls (M1-summary §2). |

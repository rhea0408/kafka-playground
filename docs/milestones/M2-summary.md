# M2 summary: Web app shell

Tasks T0–T13 are done on stacked branches `m2-t0` … `m2-t13`, each a PR on the previous one with green CI. **Nothing is merged yet** (see Deviations). Every decision is in `M2-decisions.md` (start with its "Review these first" table); the learning checkpoints are in `M2-learning-checkpoints.md`. Tests: 482 unit/property tests (schema 28, engine 179, web 275) and 21 Playwright e2e tests.

## 1. Built

- **`@kp/engine` additions:** `UpdateConfig` command and `ConfigUpdated` event; long-poll fetch (classic consumer: `fetch.min.bytes`, `fetch.max.wait.ms`, fetch buffer, prefetch) replacing `pollIntervalMs`; `metadata.max.age.ms` client metadata refresh; trace ids unique across producer lives (`p1@3#7`); topic IDs (KIP-516); targeted snapshot copy + `diffSnapshot` / `applyDiff` (~1 ms per frame on a 1,000-partition benchmark); `hashEvents` / `createEventHasher` exported; committed offsets deleted with their topic (F1).
- **`@kp/web`** (`apps/web/README.md` has the details):
  - `sim/`: `SimHost` run loop in a Web Worker behind Comlink; lossless frames (full or diff + all events) at ≤ 30 Hz; 10,000-event ring buffer; `eventLogHash()` by replay.
  - `stores/` (topology / sim / ui, Zustand) and `actions/` (the command path: the store changes only after the engine accepts a command; refusals become toasts).
  - Sandbox layout (wireframe 05) with sim controls; React Flow canvas with palette, connection rules and reasons, minimap; live log cells, LEO / HW, counters and lag on nodes; Canvas2D message overlay (A1/A2, 500-dot cap); schema-driven properties panel with real Kafka names and defaults; event list (windowed, filter, Follow); partition log with HW / committed / LEO markers; manual produce; default scene.
- **Docs:** ADRs 0001–0003 and 0008; architecture §4.1/§4.4/§4.5 and animations.md A1/A2/A5 updated.
- **Commands:** `pnpm dev`, `pnpm test:coverage` (new), `pnpm --filter @kp/web build | preview | test | e2e`. CI: lint → typecheck → test with coverage (artifact) → engine demo → web build → Playwright.

## 2. Deviations

| Docs say | Built | Why |
|---|---|---|
| T4: `reset()` = same seed and initial topology | **Reset re-inits from the current canvas** (T5-6) | Keeps what the user built |
| T2: a rate change is a "new incarnation" | **A new send schedule, same incarnation** (T2-5) | A new incarnation would drop acks of in-flight requests |
| T2: on a partition increase "producers' sticky state resets" (implies clients see new partitions at once) | **Clients see them at their next metadata refresh** (`metadata.max.age.ms`, 5 min; T2-2) | Real Kafka behaviour; the panel warns about it |
| T6: bottom tabs Event list · Lag chart · Event timeline | **Event timeline · Event list · Lag chart** (T6-9) | ui-spec and wireframes order |
| T10: not-modelled fields disabled | Also **`min.insync.replicas` disabled** although the engine accepts it (T10-5) | With RF 1 it does nothing here but would fail sends in real Kafka |
| M1 process: merge after green CI | **Stacked PRs, nothing merged** (P1) | Merging without review was blocked. **Review is needed**: merge in order from `m2-t0` |
| — | **Engine bug found and fixed (F1):** a commit could land in a re-created topic, and a deleted topic's offsets stayed | Found by the "committed ≤ HW" property. **New simplification row "Commits after a topic is re-created" needs your review** |

## 3. Simplifications added (`docs/simplifications.md`)

Config changes at run time (`UpdateConfig`), consumer fetching (rewritten for the long poll), client metadata refresh, new partition placement, topic IDs, topic deletion (offsets now deleted), commits after a topic is re-created (F1), manual produce (no explicit partition / headers). The "(M1 only)" rows are relabelled "(until M3 / M4 / v0.4)".

## 4. Open issues / tech debt

- **60 fps on real hardware (NFR-2.1, T9-17):** only measured in headless Chromium (60 fps, 500 dots). Check on the target laptop: `/?fps`, then `window.__kp.overlay.spawnTestDots(500)`.
- **Coverage minimum (NFR-4.4):** numbers now known (statements / branches / functions / lines): engine **97.1 / 92.1 / 99.2 / 97.5**, schema **96.9 / 89.7 / 100 / 98.9**, web unit tests **87.4 / 79.4 / 92.6 / 89.5**. Suggest thresholds of 90 % lines and 85 % branches for engine and schema, 80 % lines for web.
- **CI runner:** still pinned to `ubuntu-24.04`; move to the new image in its own PR.
- **Slow local Playwright start (T0-10):** the web server sometimes takes ~2 min instead of ~2 s under WSL.
- **Follow drift at the 10,000 cap (T11-3):** while Follow is paused at the cap, dropping old events shifts the view.
- **Manual produce has no explicit partition** (or headers): `ProduceManual` would need a `partition` field (T11-13).
- **Debug hooks in production:** `window.__kp.sim` and `window.__kp.overlay` ship in every build (read-only); gate them before a public release.
- Smaller: no keyboard shortcuts (T6-12) or keyboard connecting (T7-14); `eslint-plugin-react-hooks` not added (T0-5); Lag chart and Event timeline are placeholders; the event list's entry fade is nearly constant at 10× (F3 note).

## 5. Fidelity status

Not started (harness is M5). New facts for M5 to verify: partitions seen only after `metadata.max.age.ms`; long-poll timing; commits after topic re-creation (F1); `linger.ms` = 5 ms is not modelled, so the harness should set `linger.ms=0` or allow for it (ADR 0008).

## 6. Questions / suggestions for M3

1. **Group protocol:** model the classic protocol first (the 4.x default, matching T2's consumer) and KIP-848 later? M3 replaces T2-3's "new partitions join the assignment without a rebalance".
2. **Game-tuned timeouts:** `session.timeout.ms` (45 s) and `heartbeat.interval.ms` (3 s) are shown disabled today. Which shortened values should M3 use? The `game-tuned` chip is ready (T10-2).
3. **Lag threshold:** keep a fixed 100, or make it per level / relative to rate?
4. **Group node:** the consumer node is already drawn as a group with one member row (T7-3). How should the player add a second member: drop a consumer onto the group node, or set the same `group.id` on a new consumer?
5. **Manual commits** (`commitSync` / `commitAsync`) and `group.id` / `auto.offset.reset` changes become possible in M3: should they be editable at run time or need a consumer restart?
6. Set the coverage thresholds above, and decide whether the debug hooks stay in production builds.

# Kafka Playground: Non-Functional Requirements (v0.1 draft)

Companion to the functional requirements and the Real Kafka Lab addendum.
Priority: **P0** = MVP, **P1** = next, **P2** = later

---

## 1. Target Hardware & Platforms

| ID | Requirement | Pri |
|---|---|---|
| NFR-1.1 | Game (no Real Lab) runs smoothly on a mid-range laptop: 4 cores, 8 GB RAM, integrated GPU | P0 |
| NFR-1.2 | Real Lab recommended spec: 16 GB RAM, 4+ cores, Docker Desktop (Windows/WSL2, macOS) or Docker Engine (Linux), ~10 GB free disk | P1 |
| NFR-1.3 | Browsers: latest Chrome, Edge, Firefox; Safari best-effort | P0 |
| NFR-1.4 | Desktop-first; minimum resolution 1366×768. Mobile/tablet not supported in MVP | P0 |
| NFR-1.5 | Dev machine reference: i7-13850HX, 32 GB RAM, Windows x64, 50 GB free (well above requirements) | — |

## 2. Performance

| ID | Requirement | Pri |
|---|---|---|
| NFR-2.1 | Rendering: 60 fps for typical levels; ≥ 30 fps worst case at the scale ceiling (NFR-2.3) | P0 |
| NFR-2.2 | Simulation engine runs in a Web Worker, so the UI never freezes during heavy simulation | P0 |
| NFR-2.3 | Scale ceiling (Sandbox): 50 brokers, 1,000 topics, 10,000 partitions, 200 clients, 5 regions | P1 |
| NFR-2.4 | Two simulation granularities: **per-message** (every message is an event; used in early levels, ≤ ~5,000 msg/s simulated) and **flow** (rates and aggregates; arbitrary simulated throughput such as 1M msg/s). Switching is automatic, with an indicator | P0 |
| NFR-2.5 | Per-message mode: at most ~500 animated message dots on screen; beyond that, switch to flow lines and heatmaps | P0 |
| NFR-2.6 | UI response to user actions (drag, connect, config change) < 100 ms; code panel update < 100 ms | P0 |
| NFR-2.7 | App cold start < 3 s; level load < 1 s | P0 |
| NFR-2.8 | Browser tab memory < 1 GB at the scale ceiling | P1 |
| NFR-2.9 | Long runs: a simulated hour at 10× speed without memory growth (bounded event history, ring buffers) | P1 |

## 3. Simulation Fidelity & Correctness

| ID | Requirement | Pri |
|---|---|---|
| NFR-3.1 | Behavior follows Apache Kafka 4.x semantics (KRaft only, no ZooKeeper) | P0 |
| NFR-3.2 | Each modelled behavior links to its source (Kafka docs section or KIP) in code comments and in the in-game "learn more" | P1 |
| NFR-3.3 | Simplifications are documented and stated in-game where relevant ("Simplified: real brokers also…") | P0 |
| NFR-3.4 | Resource model is directionally correct (more load → more saturation; replication multiplies traffic and storage), not a benchmark | P0 |
| NFR-3.5 | Fidelity test suite: core scenarios (partitioning by key, leader election, acks loss, rebalance, commit duplicates) run in both the sim and a real cluster, and the outcomes are compared (MVP fidelity harness; extended via Real Lab) | P0 |

## 4. Determinism & Testability

| ID | Requirement | Pri |
|---|---|---|
| NFR-4.1 | Same topology + config + seed + input actions → identical event log, on every browser | P0 |
| NFR-4.2 | Engine is headless and UI-independent (pure TypeScript), runnable in Node for tests | P0 |
| NFR-4.3 | Every level has a scripted solution that CI plays back automatically to verify the level is completable and objectives trigger correctly | P0 |
| NFR-4.4 | Engine unit-test coverage ≥ 80% | P1 |
| NFR-4.5 | Runs can be exported as a replay file for bug reports | P1 |

## 5. Offline, Storage & Data

| ID | Requirement | Pri |
|---|---|---|
| NFR-5.1 | Game is fully playable offline with no account. World map assets are bundled locally | P0 |
| NFR-5.2 | Progress and saves are stored locally (IndexedDB) | P0 |
| NFR-5.3 | Autosave every 30 s and on level events; restore after a crash or tab close | P0 |
| NFR-5.4 | Save/scenario files have a version number, with migrations between versions | P0 |
| NFR-5.5 | No telemetry by default | P0 |

## 6. Usability & Learning Experience

| ID | Requirement | Pri |
|---|---|---|
| NFR-6.1 | No prior Kafka knowledge assumed. Every Kafka term on screen links to the glossary | P0 |
| NFR-6.2 | Each level completes in ≤ 15 minutes (Learn + Break + Fix); Real Lab adds ≤ 10 minutes | P0 |
| NFR-6.3 | Concept cards ≤ 5 sentences + 1 diagram; dialogues ≤ 3 short messages before the player acts | P0 |
| NFR-6.4 | Undo for every canvas action; reset level at any time | P0 |
| NFR-6.5 | Keyboard shortcuts for common actions (play/pause, step, delete, undo) | P1 |
| NFR-6.6 | Light and dark themes | P1 |

## 7. Accessibility

| ID | Requirement | Pri |
|---|---|---|
| NFR-7.1 | Color is never the only signal: keys/partitions also use labels or shapes; broker state uses icons | P0 |
| NFR-7.2 | WCAG 2.1 AA text contrast | P1 |
| NFR-7.3 | "Reduced motion" setting (honors the OS preference) | P1 |
| NFR-7.4 | Keyboard navigation for panels, dialogues and core canvas actions | P2 |

## 8. Extensibility & Maintainability

| ID | Requirement | Pri |
|---|---|---|
| NFR-8.1 | Levels are data files (YAML) validated against a schema; adding a level needs no code changes | P0 |
| NFR-8.2 | Components are modular: a new component = UI definition + engine behavior module + code-panel templates | P1 |
| NFR-8.3 | Code panel templates per language live as separate files (Java / Go / CLI); adding a language needs no engine changes | P1 |
| NFR-8.4 | Engine emits a typed event stream; tips, alerts, objectives, animations and the future AI tutor all consume the same stream | P0 |
| NFR-8.5 | Monorepo with clear packages (see architecture §3) | P0 |

## 9. Real Lab (local Kafka) Requirements

| ID | Requirement | Pri |
|---|---|---|
| NFR-9.1 | `docker compose up` brings a 3-broker cluster to ready in < 60 s | P1 |
| NFR-9.2 | Real cluster state refreshes in the UI every ≤ 1 s | P1 |
| NFR-9.3 | Cluster RAM ≤ 4 GB for 3 brokers (heap limits set in compose) | P1 |
| NFR-9.4 | Bridge detects a missing or stopped cluster and shows clear setup steps; the game never breaks | P0 |
| NFR-9.5 | Bridge is a single Go binary with no other runtime dependencies (Java needed only for Java runners) | P1 |

## 10. Security & Privacy

| ID | Requirement | Pri |
|---|---|---|
| NFR-10.1 | Lab Bridge binds to `localhost` only | P1 |
| NFR-10.2 | Bridge exposes only an allowlist of actions (start/stop known containers, run known runners, inject latency); no arbitrary shell commands | P1 |
| NFR-10.3 | User-written code (FR-13.10) runs in an isolated container with resource limits | P2 |
| NFR-10.4 | Future AI tutor is opt-in; the API key is stored locally; no data is sent anywhere without consent | P2 |

## 11. Licensing

| ID | Requirement | Pri |
|---|---|---|
| NFR-11.1 | Only permissively licensed dependencies (MIT / Apache-2.0 / BSD); Kafka itself is Apache-2.0 | P0 |
| NFR-11.2 | World map from public-domain data (e.g. Natural Earth) | P1 |

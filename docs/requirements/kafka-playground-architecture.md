# Kafka Playground: Architecture (v0.2 draft, MVP focus)

Companion to the functional requirements v0.5, non-functional requirements v0.1, Real Kafka Lab and roadmap v0.3. UI layout, colors and animations: `docs/ui/ui-spec.md`, `docs/ui/animations.md`, `docs/ui/wireframes/`.

**Changes in v0.2:** UI tree updated from the wireframe review (right panel failure tabs, tabbed bottom panel, minimap), level YAML scoring updated to the three-star model, acks=all + minISR=1 loss case added, design tokens referenced.

---

## 1. High-level view

```
┌──────────────────────────── Browser ─────────────────────────────┐
│  Main thread (React)                    Web Worker               │
│  ┌──────────────────────────┐   cmds   ┌──────────────────────┐  │
│  │ Canvas (React Flow)      │ ───────▶ │ Simulation Engine    │  │
│  │ Message overlay (Canvas2D)│         │  (pure TS, seeded)   │  │
│  │ Panels: props, log, code │ ◀─────── │ Level Runtime        │  │
│  │ Dialogues / tips         │ events + │  (objectives, scoring)│ │
│  │ Zustand store            │ snapshots└──────────────────────┘  │
│  └──────────┬───────────────┘                                    │
│             │ IndexedDB (saves, progress, settings)              │
└─────────────┼────────────────────────────────────────────────────┘
              │ (v0.3) WebSocket
        ┌─────▼──────────┐        ┌──────────────────────────────┐
        │ Lab Bridge (Go)│ ─────▶ │ Kafka 4.x in Docker (3 KRaft) │
        └────────────────┘        └──────────────────────────────┘
                                  ▲
   MVP: Fidelity Harness ─────────┘ (Go runner vs TS engine runner, compared)
```

**Key principles**
- The **engine and level runtime are pure TypeScript with no DOM.** The same code runs in the browser worker, in Node for tests/CI, and in the fidelity harness.
- **One-way data flow:** the UI sends *commands*, and the worker returns *events* + *snapshots*. The UI never mutates simulation state.
- **Determinism:** seeded RNG, a single simulated clock, and commands stamped with simulated time. The same seed + same command log always gives the same run (NFR-4.1). This also powers "replay the same failure" in the Fix phase.

## 2. Tech stack

| Concern | Choice | Why |
|---|---|---|
| Build / dev server | Vite + TypeScript (strict) | Fast, standard |
| UI | React 18+ | Decision D8 |
| Node canvas | React Flow (`@xyflow/react`) | Mature drag/drop node-edge canvas; built-in minimap and controls |
| Message animation | Canvas2D overlay above React Flow | Hundreds of moving dots without DOM cost; PixiJS only if needed later |
| UI components | Radix UI primitives + Tailwind (shadcn/ui style) | Accessible, stable, themeable; tokens from ui-spec.md |
| App state | Zustand | Simple, works well with frequent snapshot updates |
| Worker messaging | Comlink | Typed RPC over postMessage |
| Schemas / validation | Zod | Topology, levels, saves, scenario files |
| Charts | uPlot | Fast time-series (lag, throughput) |
| Code panel | Shiki (read-only highlighting) | Lightweight; Monaco later for editable challenges |
| Persistence | IndexedDB via Dexie | Versioned schema + migrations |
| Tests | Vitest, fast-check (property tests), Playwright (UI smoke) | |
| Monorepo | pnpm workspaces (+ Turborepo if builds get slow) | |
| Real Kafka | `apache/kafka` 4.x image (KRaft), docker compose | Official image, no ZooKeeper |
| Go Kafka client | confluent-kafka-go | Same config names as Java (D6) |

## 3. Repository layout

```
kafka-playground/
├─ apps/
│  └─ web/                      React app (canvas, panels, dialogues, worker bootstrap)
├─ packages/
│  ├─ engine/                   Simulation engine (pure TS)
│  │  ├─ core/                  clock, event queue, seeded RNG, ids
│  │  ├─ model/                 Cluster, Broker, Topic, Partition, Replica, Producer, Consumer, Group
│  │  ├─ protocols/             produce, replication/ISR, controller/election, group coordinator, commits
│  │  ├─ commands/              command handlers (AddBroker, KillBroker, UpdateConfig, Produce…)
│  │  ├─ events/                typed domain events
│  │  └─ snapshot/              snapshot + diff builder for the UI
│  ├─ schema/                   Zod schemas shared by everything (topology, config, level, save, scenario)
│  ├─ level-runtime/            phases, steps, triggers, objectives, stars & quality checks (pure TS)
│  ├─ levels/                   level content (YAML) + assets + solution command logs; validated in CI
│  ├─ codegen/                  topology → Java / Go / CLI code with line→source mapping
│  └─ fidelity/                 scenario files + TS engine runner + result comparer
├─ lab/                         Go module
│  ├─ fidelity-runner/          (MVP) runs scenarios against real Kafka, writes result JSON
│  └─ bridge/                   (v0.3) Lab Bridge
├─ docker/
│  └─ compose.kraft-3.yml       3-broker KRaft cluster (+ toxiproxy later)
└─ docs/                        requirements, ui (spec, animations, wireframes), milestones, adr, simplifications
```

## 4. Simulation engine

### 4.1 Execution model: discrete-event simulation
- **Simulated clock** in milliseconds. The engine pops events from a **priority queue** ordered by `(time, sequence)`. The sequence number breaks ties so ordering is always deterministic.
- Every behavior is an event that schedules future events, e.g. `ProduceRequest@t` → `LeaderAppend@t+netDelay` → `FollowerFetch@t+fetchInterval` → `HighWatermarkAdvance` → `ProduceAck`.
- **Speed control** maps wall-clock to sim-clock (0.25×–10×). Step mode processes the next event (or the next N ms).
- The worker runs the loop and emits a **snapshot diff ~30×/s** plus every **domain event** as it happens.

### 4.2 Core model (MVP)

```ts
Cluster      { brokers: Broker[], controllerId, topics: Map<name, Topic> }
Broker       { id, rack, status: 'up'|'down' }
Topic        { name, partitions: Partition[], rf, minIsr, retention }
Partition    { id, leader: BrokerId|null, replicas: BrokerId[], isr: BrokerId[],
               leaderEpoch, log: Record[], hw, replicaLeo: Map<BrokerId, offset> }
Record       { offset, key, value, ts, producerId, traceId }
Producer     { id, config: { acks, retries, idempotence, partitioner, linger, rate, keyStrategy },
               inflight, stats }
ConsumerGroup{ id, generation, coordinator, members, assignment: Map<partition, memberId>,
               committed: Map<partition, offset>, state: 'Stable'|'PreparingRebalance'|… }
Consumer     { id, groupId, status, position: Map<partition, offset>, config: { autoCommit,
               commitInterval, maxPollRecords, processingTimeMs, sessionTimeout } }
```

### 4.3 Protocols modelled in MVP (simplified, documented)

| Protocol | Behavior | Simplification |
|---|---|---|
| Partitioning | Key → `toPositive(murmur2(key)) mod partitions` (matches Java default); null key → sticky | Batching not modelled in MVP |
| Produce & acks | acks=0: no wait; acks=1: leader append; acks=all: every **current ISR** member has it (HW ≥ offset). NotEnoughReplicas if ISR < minIsr | No request queueing until the v0.2 resource model |
| Replication | Followers fetch from the leader on an interval; the leader tracks replica LEO; HW = min LEO over the ISR | Fixed fetch interval |
| ISR | Follower removed if it hasn't caught up within `replica.lag.time.max.ms`; re-added once caught up | |
| Controller & election | On broker down, the controller picks the new leader from the ISR (clean election only); leader epoch++ | Election delay is a fixed configurable value |
| Data loss | Records that exist only on a failed leader are truncated when it rejoins as a follower. Happens with acks=0/1, **and with acks=all when the ISR had shrunk to the leader alone and min.insync.replicas = 1** | Truncation to HW, epoch-based |
| Group coordinator | Join/Sync/Heartbeat, session timeout, generations; range / round-robin / sticky / cooperative-sticky assignors | Classic protocol modelled first; the new KIP-848 consumer protocol is noted for later |
| Commits | Auto (interval) vs manual sync/async; on crash the new owner resumes from the last committed offset → duplicates or skips | |

**Game-time defaults:** real defaults (e.g. `session.timeout.ms` = 45 s) are too slow for gameplay. Levels can use shortened values, always shown as "game-tuned" with the real default beside them (NFR-3.3).

### 4.4 Commands (UI → engine)
`AddComponent`, `RemoveComponent`, `Connect`, `Disconnect`, `UpdateConfig`, `KillBroker`, `RestartBroker`, `KillConsumer`, `ProduceManual`, `SetSpeed`, `Step`, `Pause`, `Reset`, `RewindTo` (scrubber / "Try again").
Each accepted command is stamped with sim-time and appended to the **command log**, which is used for replay, save files, the failure replay in Fix, and CI playback.

### 4.5 Domain events (engine → UI and level runtime)
`RecordProduced`, `RecordAppended`, `RecordReplicated`, `HighWatermarkAdvanced`, `ProduceAcked`, `ProduceFailed`, `RecordConsumed`, `OffsetCommitted`, `BrokerDown`, `BrokerUp`, `LeaderElected`, `IsrShrunk`, `IsrExpanded`, `RecordsTruncated` (data loss), `RebalanceStarted`, `RebalanceCompleted`, `PartitionAssigned`, `ConsumerIdle`, `DuplicateProcessed`, `LagChanged`.
Typed as a discriminated union in `packages/engine/events`. This single stream feeds the UI, animations (`docs/ui/animations.md`), tips, objectives, alerts, and later the AI tutor (NFR-8.4).

### 4.6 Invariants (property tests with fast-check)
- For every partition: `hw ≤ leo(leader)` and `hw ≤ leo(r)` for every r in the ISR
- The leader is always in the ISR; the ISR is a subset of the replicas
- Within one group generation, each partition is assigned to at most one consumer
- With `acks=all` and `ISR ≥ minIsr ≥ 2` at ack time, an acknowledged record is never truncated after a single broker failure
- Per partition, offsets are contiguous and records with the same key keep produce order

### 4.7 Rewind (scrubber and "Try again")
Rewind = rebuild state by replaying the command log from the start (or from the nearest periodic checkpoint) up to the target sim time. Checkpoints are serialised engine state taken every N sim-seconds, so rewinds stay fast. Replay is valid because the engine is deterministic.

## 5. UI architecture (apps/web)

```
App
├─ Home (continue card, campaign map)                         [wireframe 01]
├─ ModeShell (Campaign | Sandbox)
│  ├─ TopBar: back, title, phase stepper, sim controls (fixed, 8 px from right)
│  ├─ ObjectivesStrip (Campaign only)
│  ├─ Palette (unlocked items only in Campaign; failure tools in Break it)
│  ├─ CanvasView
│  │   ├─ React Flow (nodes = components, edges = connections)
│  │   ├─ MessageOverlay (Canvas2D animated dots, labeled by key)
│  │   ├─ Banners (auto-pause / replay), warning toasts, hover toolbar
│  │   └─ Controls (zoom, fit, minimap toggle) + MiniMap (auto-shows when content off-screen)
│  ├─ RightPanel tabs: [What went wrong | Not quite]* · Properties · Code (Java/Go/CLI) · Partition log
│  │      * failure tab replaces nothing; it is added first while a failure/failed fix is active
│  ├─ BottomPanel tabs: Event timeline (scrubbable) · Event list · Lag chart   (same component everywhere)
│  └─ DialogueLayer: concept card, predict-then-run, debrief modal (3 stars / missed-star explanations)
└─ Stores (Zustand): topologyStore, simStore (latest snapshot), levelStore, uiStore
```
- **Topology** (what's on the canvas) lives in the UI and is sent to the engine as commands. **Runtime state** (offsets, leaders, lag) comes only from engine snapshots.
- Node components render from the snapshot: broker up/down, leader badges per partition, lag badges, ISR membership (dashed border = out of ISR).
- Connection rules live in `schema`, shared with the engine so UI and engine always agree.
- Colors come from the tokens in `docs/ui/ui-spec.md` (Tailwind theme / CSS variables).

## 6. Level runtime & content

Levels are YAML files validated by Zod. The runtime runs inside the worker next to the engine and subscribes to its events, which keeps it deterministic.

```yaml
id: w2-l7-acks
world: 2
title: "acks: how sure is 'sent'?"
unlocks: [producer.acks.all, topic.minIsr]
setup: { seed: 42, topology: ./w2-l7.topology.json, palette: [producer, consumer] }
phases:
  learn:
    conceptCard: cards/acks.md
    steps:
      - say: "Set the producer's acks to 1."
        highlight: producer-1.config.acks
        until: { config: { path: producer-1.acks, equals: "1" } }
      - predict:
          id: p1
          question: "If the leader dies right after acking, is the message safe?"
          options: [yes, no, depends]
          answer: depends
      - run: { untilEvent: ProduceAcked, count: 20 }
  break:
    prompt: "Kill the leader of orders-0 right after a write."
    doItForMe: { command: KillBroker, target: leaderOf(orders-0) }
    failureWhen: { event: RecordsTruncated }
    explain: dialogues/acks-loss.md
  fix:
    goal: "Survive the same failure with zero lost records."
    replay: sameBreak                    # rerun with the same seed + same break command
    passWhen: { noEvent: RecordsTruncated }
    notQuite:                            # shown when the replay still fails
      explain: dialogues/acks-not-quite.md
      hints:
        - { level: nudge, cost: free,    text: hints/acks-1.md }
        - { level: strong, cost: quality, text: hints/acks-2.md }
        - { level: solution, cost: maxOneStar }
stars:
  outcome: { passWhen: fix.passWhen }
  understanding: { predictionsFirstTry: [p1] }
  quality:
    maxFailedAttempts: 1
    maxPaidHints: 0
    budget: { maxBrokers: 3 }
    fragile:
      - when: { config: { path: topic.orders.minIsr, lt: 2 } }
        explain: "min.insync.replicas=1 lets acks=all succeed with a single copy."
```
- Triggers, objectives and star criteria are **declarative predicates** over events, config and snapshot fields, so no level-specific code is needed.
- Each level has `solution.commands.json`. CI plays it back headlessly and asserts 3 stars (NFR-4.3).

## 7. Code generation (code panel)

```
topology + configs → Component IR → templates (java/, go/, cli/) → lines[] with { text, sourcePath }
```
- Each generated line records which config field produced it (e.g. `producer-1.acks`), so the UI highlights exactly the lines a change affected and shows the per-line explanation tooltip.
- Templates are plain TS functions per language; adding a language needs no engine change.
- **Partitioner consistency:** the Java default partitioner uses murmur2, but librdkafka (used by confluent-kafka-go) defaults to a different hash. Generated Go code sets `"partitioner": "murmur2_random"` so the same key lands in the same partition in Java, Go and the simulator. This is also a learning tip in the panel.

## 8. Persistence

IndexedDB (Dexie) stores: `saves` (topology + configs + seed + command log), `progress` (best stars per criterion, unlocks, current level/phase, attempts), `settings` (reduced motion, tips). Every record carries a `schemaVersion`, with migration functions per version (NFR-5.4). Export/import uses the same JSON.

## 9. Fidelity Harness (MVP)

A correctness check for the engine, especially important because the code is AI-written.

```
packages/fidelity/scenarios/*.json   ← shared scenario definitions
        │                                     │
  TS runner (Node, engine)            Go runner (lab/fidelity-runner, real Kafka in Docker)
        │                                     │
  results/sim/<scenario>.json         results/real/<scenario>.json
        └──────────── comparer (per-field rules) ────────────┘
```

| Scenario | Compared |
|---|---|
| keys-to-partitions | exact partition per key (100 keys, 6 partitions) |
| ordering-per-key | same order per key, exact |
| group-assignment | number of partitions per consumer for 1–4 consumers (incl. an idle consumer) |
| leader-failover-acks-all | acked records lost = 0 (minISR = 2) |
| leader-failover-acks-1 | data loss *possible* (qualitative; real loss is timing-dependent) |
| isr-below-minisr | produce fails with NotEnoughReplicas |
| consumer-crash-autocommit | duplicates or skips occur (qualitative) |
| consumer-crash-manual-commit | no skips; duplicates bounded by the uncommitted batch |

- Comparison rules are **exact** for deterministic facts (partition mapping, counts, errors) and **qualitative** for timing-dependent ones (loss possible, duplicates > 0).
- Run locally with `docker compose -f docker/compose.kraft-3.yml up` + `pnpm fidelity`. It can also run in GitHub Actions, which supports Docker.
- The Go runner later grows into the Lab Bridge (v0.3).

## 10. Testing strategy

| Layer | Tool | What |
|---|---|---|
| Engine unit | Vitest | each protocol handler |
| Engine invariants | fast-check | §4.6 properties over random topologies + command sequences |
| Determinism | Vitest | same seed + commands → byte-identical event log |
| Levels | Vitest (headless) | solution playback earns 3 stars; schema validation |
| Codegen | Vitest snapshots | generated Java/Go/CLI per topology |
| UI | Playwright | smoke: drag, connect, play, complete level 1 |
| Fidelity | harness | sim vs real Kafka (§9) |

## 11. MVP build order (milestones)

| # | Milestone | Done when |
|---|---|---|
| M1 | Engine core: clock, queue, RNG, single broker, produce/consume, partitioning, offsets | unit + determinism tests pass (`docs/milestones/M1-engine-core.md`) |
| M2 | Web app shell: React Flow canvas, palette, properties, worker + Comlink, snapshot rendering, message overlay | a producer → topic → consumer flow animates |
| M3 | Consumer groups + rebalancing + commits | invariants pass; idle-consumer case visible |
| M4 | Multi-broker: replication, ISR, controller election, acks, failures, rewind | invariants pass; kill-broker works in UI |
| M5 | Fidelity harness + docker compose | all MVP scenarios agree |
| M6 | Level runtime (phases, Not quite, stars) + dialogues + World 1 | levels 1–5 playable; CI playback green |
| M7 | World 2 + code panel (Java/Go/CLI) | levels 6–10 playable |
| M8 | Sandbox, persistence, autosave, polish | MVP checklist complete |

## 12. Architecture decision records (to create in docs/adr)
1. Discrete-event simulation in a Web Worker
2. Pure-TS engine shared by browser, Node and harness
3. React + React Flow + Canvas2D overlay
4. Declarative YAML levels with predicate objectives and star criteria
5. murmur2 partitioner everywhere for cross-language consistency
6. Fidelity harness in MVP as an engine correctness check
7. Rewind by deterministic replay from checkpoints

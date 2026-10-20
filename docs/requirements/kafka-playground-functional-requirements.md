# Kafka Playground: Functional Requirements (v0.5 draft)

A browser-based visual tool for learning Kafka and simulating Kafka setups. It covers two use cases:
- **Message queueing**: competing consumers, work distribution, delivery guarantees, retries, dead-letter queue (DLQ)
- **Event streaming**: stream processing, stateful operations, windowing, joins, replay

It has two modes:
- **Learning mode (Campaign)**: a game with levels that starts from one node and builds up to a global, stressed, multi-region system. Each level teaches a concept first, then has the player break it and fix it. Missions are modelled on how Kafka is planned in industry.
- **Simulation mode (Sandbox)**: free drag-and-drop with no guidance, plus load/stress tools and a world-map deployment view.

Priority: **P0** = MVP, **P1** = next, **P2** = later

**Changes in v0.5:** added the evaluation & failure model: three stars meaning Outcome / Understanding / Quality, failed fix attempts, losable boss levels (§7.3, replaces the old FR-7.6). v0.4 added canvas navigation/minimap (FR-1.7).

---

## 0. Decisions Made

| # | Decision | Choice |
|---|---|---|
| D1 | How levels teach | **Explain-first, then fail-first.** A tutorial teaches the concept; then the player is prompted to add load or remove components, which triggers a failure. A dialogue explains why it failed and unlocks the fix |
| D2 | Stress / resource model depth | **Simple bars.** Network, disk throughput and storage per broker; directionally correct, not a benchmark |
| D3 | Global deployment view | **Real world map** with real-world regions and approximate inter-region latency |
| D4 | Tips & dialogues | **Scripted first, AI tutor later.** The simulation state must be exposed so an LLM tutor can plug in later |
| D12 | Evaluation | **Three meaningful stars** (Outcome, Understanding, Quality); fix attempts can fail with unlimited retries; boss levels can be lost; 1 star unlocks the next level |

Further decisions (D5–D11) are in the Real Kafka Lab and roadmap docs.

---

## 1. Canvas & Component Palette

| ID | Requirement | Pri |
|---|---|---|
| FR-1.1 | Drag-and-drop canvas with pan, zoom, snap-to-grid, multi-select, delete, undo/redo | P0 |
| FR-1.2 | Connections are checked against rules (e.g. Producer→Topic is allowed, Producer→Consumer is not); invalid links show a reason | P0 |
| FR-1.3 | Clicking a component opens a properties panel where its config can be edited | P0 |
| FR-1.4 | Components show live state (partition leader, lag badge, broker up/down, resource bars) | P0 |
| FR-1.5 | Two linked views of the same system: **Logical view** (data-flow canvas) and **Deployment view** (world map: region → availability zone → broker). See §5 | P1 |
| FR-1.6 | Bulk actions: "add N topics", "scale cluster to N brokers", "add N consumers to group" | P0 |
| FR-1.7 | Canvas navigation controls (zoom −, fit, zoom +, minimap toggle; 160 × 40). The **minimap** (160 × 84, bottom-right of the canvas) appears automatically only when part of the setup is off-screen, because it doesn't fit or the view is zoomed in. It hides when everything is visible (200 ms fade); the toggle overrides this manually | P0 |

### Component palette

| Component | Key configurable properties | Pri |
|---|---|---|
| Producer | rate (msg/s), message size, key strategy (null / fixed / random / skewed), partitioner, acks (0/1/all), retries, idempotence, batch.size, linger.ms, compression | P0 |
| Broker | id, rack/AZ, region, hardware size (small / medium / large), up/down | P0 |
| Cluster | brokers, controller (KRaft), region(s) | P0 |
| Topic | partitions, replication factor, min.insync.replicas, retention (time/size), cleanup.policy (delete/compact) | P0 |
| Consumer | group.id, commit mode, auto.offset.reset, max.poll.records, processing time per message, failure rate | P0 |
| Consumer Group | assignment strategy (range / round-robin / sticky / cooperative-sticky) | P0 |
| Load Generator | traffic profile driving producers (see §4) | P0 |
| Dead Letter Topic + Retry Topic | retry count, backoff | P1 |
| Stream Processor (Kafka Streams-style) | filter, map, flatMap, groupBy, count/aggregate, joins, windowing (tumbling / hopping / session) | P1 |
| State Store + changelog topic | backed by a compacted topic | P1 |
| KTable / GlobalKTable | | P1 |
| MirrorMaker 2 (cross-cluster replicator) | source cluster, target cluster, topics to mirror | P1 |
| Schema Registry | schema versions, compatibility mode | P2 |
| Kafka Connect (Source / Sink) + mock external systems | | P2 |

## 2. Simulation Engine

| ID | Requirement | Pri |
|---|---|---|
| FR-2.1 | Discrete-event simulation with Play, Pause, Step, Reset and a speed control (0.25x–10x) | P0 |
| FR-2.2 | Animated message flow, colored by key. At high load, individual dots switch to aggregated flow lines | P0 |
| FR-2.3 | Partitioning (key-hash / round-robin / sticky), with visible ordering-per-partition | P0 |
| FR-2.4 | Each partition is a log with offsets, log-start-offset, high-watermark, log-end-offset | P0 |
| FR-2.5 | Replication: leader/followers, in-sync replica set (ISR), follower lag, leader election on broker failure | P0 |
| FR-2.6 | acks semantics, with data loss possible under acks=0/1, and under acks=all when ISR has shrunk to 1 and min.insync.replicas=1; NotEnoughReplicas when ISR < min.insync.replicas | P0 |
| FR-2.7 | Consumer group assignment and rebalancing (eager vs cooperative); idle consumers when consumers > partitions | P0 |
| FR-2.8 | Offset commits showing duplicates (at-least-once) or skipped messages (at-most-once) after a crash | P0 |
| FR-2.9 | Consumer lag per partition and per group | P0 |
| FR-2.10 | Retention (time/size) and log compaction with tombstones | P1 |
| FR-2.11 | Idempotent producer and transactions / exactly-once | P1 |
| FR-2.12 | Retry and DLQ flow | P1 |
| FR-2.13 | Stream processing: stateful operators, event-time windows, late events and grace period, joins | P1 |
| FR-2.14 | Replay via consumer group offset reset | P1 |
| FR-2.15 | Deterministic, seeded runs (needed for level validation, failure replay and fair scoring) | P0 |
| FR-2.16 | Engine emits structured events (e.g. `BROKER_DOWN`, `ISR_SHRINK`, `REBALANCE_START`, `LAG_THRESHOLD`) that lesson scripts, tips and alerts subscribe to; the same stream later feeds the AI tutor | P0 |

## 2b. Resource & Capacity Model (simple bars)

| ID | Requirement | Pri |
|---|---|---|
| FR-2b.1 | Each broker has three capacity bars: **network** (MB/s in+out), **disk throughput** (MB/s write), **storage** (GB). Sizes are small / medium / large | P0 |
| FR-2b.2 | Load accounting per broker: produce traffic + replication traffic + consumer fetch traffic + stored bytes (rate × size × retention × replication factor) | P0 |
| FR-2b.3 | Saturation effects in stages: bar turns amber at 80%; above 100% produce latency rises, then producers are throttled or fail, followers drop out of the ISR, lag grows; a full disk takes the broker offline | P0 |
| FR-2b.4 | Partition-count pressure: a soft limit per broker with warnings and slower leader election after failure | P1 |
| FR-2b.5 | Hot-partition detection for skewed keys | P0 |
| FR-2b.6 | Bottleneck highlighting with a "why" tooltip | P0 |
| FR-2b.7 | Cost estimate (per broker size, cross-AZ and cross-region transfer) used in scoring and planning missions | P1 |
| FR-2b.8 | Quotas: per-client byte-rate limits | P2 |

## 3. Failure & Chaos Injection

| ID | Requirement | Pri |
|---|---|---|
| FR-3.1 | Kill or restart a broker, consumer or producer; remove a component while running | P0 |
| FR-3.2 | Slow consumer (higher processing time leads to lag buildup) | P0 |
| FR-3.3 | Network partition / slow follower that drops out of the ISR | P1 |
| FR-3.4 | Poison message injection | P1 |
| FR-3.5 | Availability-zone outage and region outage | P1 |
| FR-3.6 | Scripted chaos timeline (e.g. "t=30s kill broker 2, t=60s traffic ×5") | P1 |

## 4. Load & Stress Testing

| ID | Requirement | Pri |
|---|---|---|
| FR-4.1 | Load Generator profiles: constant, linear ramp, step, spike/burst, daily wave, custom curve drawn on a timeline | P0 |
| FR-4.2 | Load can be set per topic: rate, message size, key distribution (uniform / skewed / single hot key) | P0 |
| FR-4.3 | Scale stress by topic count: bulk-create N topics × M partitions | P0 |
| FR-4.4 | Scale stress by message volume: raise rate on one topic until saturation | P0 |
| FR-4.5 | Live saturation dashboard: broker resource bars, produce latency, throughput in/out, total lag, under-replicated partitions | P0 |
| FR-4.6 | "Find the breaking point" auto-ramp: ramps load until an SLO breaks and reports the max sustainable throughput and the bottleneck | P1 |
| FR-4.7 | Scaling while running: add brokers, partitions or consumers. Shows side effects: rebalance pauses, partition reassignment traffic, and changed key→partition mapping | P0 |
| FR-4.8 | Rendering scales to hundreds of topics and tens of brokers (aggregated flows, heatmaps) | P0 |
| FR-4.9 | Before/after comparison of two runs | P2 |

## 5. Global / Geo-Distributed Deployment (real world map)

| ID | Requirement | Pri |
|---|---|---|
| FR-5.1 | World map with selectable real-world regions (e.g. Mumbai, Singapore, Frankfurt, N. Virginia), each with 3 availability zones; brokers are dragged onto regions/AZs | P1 |
| FR-5.2 | Approximate inter-region round-trip latency table drives replication and acks=all latency | P1 |
| FR-5.3 | Rack/AZ awareness: replicas spread across AZs; the view shows what survives an AZ outage | P1 |
| FR-5.4 | Multi-region topologies with trade-offs shown: (a) single region, multi-AZ; (b) stretched cluster across regions; (c) cluster per region + MirrorMaker 2, active-passive and active-active; (d) hub-and-spoke aggregation | P1 |
| FR-5.5 | Cross-region data flow shown as animated arcs, with replication lag per link | P1 |
| FR-5.6 | Consumers read from the nearest replica (follower fetching) | P2 |
| FR-5.7 | Region failover drill: measure data loss (RPO) and recovery time (RTO) | P2 |

## 6. Observability & Inspection

| ID | Requirement | Pri |
|---|---|---|
| FR-6.1 | Partition log viewer (offset, key, value, timestamp, status, committed/HW markers) | P0 |
| FR-6.2 | Message inspector tracing the full path of a message | P0 |
| FR-6.3 | Bottom panel (840 × 200), one tabbed component on every screen: **Event timeline** (graphical, scrubbable), **Event list** (text log), **Lag chart** | P0 |
| FR-6.4 | Metrics charts over time: throughput, lag, latency, resource use | P0 |
| FR-6.5 | Manual "produce message" form | P0 |
| FR-6.6 | Alerts panel with standard production alerts (under-replicated / offline partitions, lag threshold, disk > 80%) | P1 |

## 7. Learning Mode: Campaign

### 7.1 The level loop (explain-first → fail-first)

```
PHASE A: LEARN (explain-first)
  1. Briefing: story + goal ("Your shop needs to record orders")
  2. Concept card: 3–5 sentences + a small diagram ("What is a partition?")
  3. Guided build: step-by-step instructions, with the target component highlighted
  4. Run & observe: a predict-then-run question ("Which consumer gets key=A?")
  5. Objective check (automatic): Tutorial complete

PHASE B: BREAK IT (fail-first)
  6. Challenge prompt: "Now push it. Triple the traffic" / "Remove broker 2" / "Add a 4th consumer"
  7. Failure happens: the sim auto-pauses at the critical moment and the failing part is highlighted
  8. "What went wrong" dialogue: cause → effect → which Kafka concept explains it
  9. Fix it: a new component/config is unlocked and the player applies the fix
 10. Replay the same failure (same seed); if it fails again → "Not quite" (§7.3), retry
 11. Debrief: stars per criterion, key takeaway, "in industry…" note, next level unlocked
```

| ID | Requirement | Pri |
|---|---|---|
| FR-7.1 | Campaign map of worlds and levels, played in order; a level unlocks when the previous one has ≥ 1 star | P0 |
| FR-7.2 | Every level implements the two-phase loop above | P0 |
| FR-7.3 | Phase B is triggered by the player's own action (adding load, removing/killing a component), not automatically; the prompt suggests the action, and a "do it for me" button is available | P0 |
| FR-7.4 | Failure moments auto-pause the sim with the cause highlighted on the canvas, plus timeline markers so the player can scrub back to watch how it happened | P0 |
| FR-7.5 | The fix unlocks a component/config, so the palette grows only as concepts are learned | P0 |
| FR-7.6 | Scoring follows §7.3 | P0 |
| FR-7.7 | Action-triggered contextual tips (scripted, keyed off engine events, FR-2.16) | P0 |
| FR-7.8 | Hint ladder: free nudge → stronger hint (costs Quality star) → show solution (max 1 star) | P0 |
| FR-7.9 | "Boss" incident levels at the end of each world: timed, combined failures, no guided build; can be lost (§7.3) | P1 |
| FR-7.10 | Lesson content is data-driven: levels are defined in YAML (briefing, concept card, steps, triggers, objectives, dialogues, **star criteria and quality checks**), so new levels need no code | P0 |
| FR-7.11 | AI tutor hook (later): a serialisable snapshot of sim state + recent events, so an LLM can answer "why is my lag growing?" | P2 |
| FR-7.12 | Progress save, achievements, glossary filled in as terms are met | P1 |

### 7.3 Evaluation, stars & failure

**Three stars, each with one meaning:**

| Star | Measures | How it's checked | Example (W2·L7 acks) |
|---|---|---|---|
| **Outcome** | The fix survives the same failure when replayed | Objectives evaluated on the replay run (same seed + same break) | 0 acknowledged records lost when the leader is killed again |
| **Understanding** | Predictions right on the first try | Predict-then-run answers in the Learn phase (and any in Fix) | Answered "It depends" before running |
| **Quality** | A clean, production-sensible fix | Per-level quality checks (below) | No paid hints; ≤ 3 brokers; not `min.insync.replicas = 1`; ≤ 1 failed attempt |

| ID | Requirement | Pri |
|---|---|---|
| FR-7.13 | **Quality checks** are declared per level: budget (brokers/cost), fragile-setting rules (configs that pass the test but are bad practice, each with an explanation), max failed attempts (default 1), paid hints used (default 0). The debrief lists each check with pass/miss | P0 |
| FR-7.14 | **Failed fix attempt ("Not quite"):** when the replay still fails, the sim pauses at the failure; a panel explains what still went wrong, shows the player's config with weak settings flagged, offers the free nudge hint, the paid hints, and "Try again" (rewinds to just before the failure, keeps config). Shows which stars are still possible. Retries are unlimited | P0 |
| FR-7.15 | **Debrief shows why** each star was earned or missed, and for a missed star a "How to get it" tip. "Replay for 3 stars" is available any time; the best score is kept | P0 |
| FR-7.16 | **Boss levels can be lost:** timer runs out, SLO breached, or data lost → "Incident lost" screen with a short post-mortem (timeline of what happened, what would have prevented it) and Retry. Stars on bosses: Outcome = incident resolved; Understanding = diagnosis questions; Quality = time, cost, data impact | P1 |
| FR-7.17 | Sandbox has no stars; Planning missions (§8) use the same three-star model | P1 |

### 7.2 Campaign outline (each level = Learn → Break → Fix)

**World 1: One Node**

| # | Learn | Break it (prompt) | Fix (unlocks) |
|---|---|---|---|
| 1 | Producer → topic → consumer | Stop the consumer for a while, then restart | Concept of offsets: nothing was lost |
| 2 | Offsets & replay | Set auto.offset.reset=latest on a new consumer | Earliest vs latest, offset reset |
| 3 | Partitions & keys | Remove keys from messages | Keys keep per-entity ordering |
| 4 | Consumer groups | Add a 4th consumer to a 3-partition topic | Partitions limit parallelism |
| 5 | *Boss:* single broker crashes under load | | Motivates World 2: multiple brokers |

**World 2: The Cluster**

| # | Learn | Break it | Fix |
|---|---|---|---|
| 6 | 3 brokers, replication factor 3 | Kill the leader of a partition | Leader election, ISR |
| 7 | acks=1 | Kill the leader right after a write | acks=all + min.insync.replicas=2 |
| 8 | Auto commit | Crash a consumer mid-batch | Manual commit, at-least-once, idempotent processing |
| 9 | Rebalancing | Restart consumers repeatedly | Cooperative-sticky assignment |
| 10 | *Boss:* lose a broker under load with zero data loss | | |

**World 3: Under Pressure**

| # | Learn | Break it | Fix |
|---|---|---|---|
| 11 | Broker resource bars | Triple traffic | Find the bottleneck, add brokers/partitions |
| 12 | Scaling consumers | Slow consumer under load | More partitions + consumers |
| 13 | Key distribution | 80% of traffic on one key | Better key design |
| 14 | Many topics | Bulk-create 1,000 topics | Partition budgeting |
| 15 | *Boss:* flash-sale spike ×10 on a budget | | |

**World 4: Patterns:** work queue + retries + DLQ, pub/sub fan-out, stream pipeline, windowed aggregation & late events, compaction & tables, exactly-once.

**World 5: Going Global:** multi-AZ (*Boss:* AZ outage), two regions with MirrorMaker 2 active-passive, active-active trade-offs, *Final boss:* region outage with RPO/RTO measured.

## 8. Industry Planning Missions

| ID | Requirement | Pri |
|---|---|---|
| FR-8.1 | Design brief with realistic requirements (throughput, message size, retention, durability, latency SLO, budget) | P1 |
| FR-8.2 | Capacity-planning worksheet with built-in calculators (partitions ≈ max(target ÷ producer throughput per partition, target ÷ consumer throughput); storage = rate × size × retention × replication factor) | P1 |
| FR-8.3 | Topic design step: naming, key choice, partitions, retention, schema | P1 |
| FR-8.4 | Validate the plan: deploy, run the brief's load test, check SLOs, compare estimate vs result | P1 |
| FR-8.5 | Production-readiness checklist (replication factor ≥ 3, min.insync.replicas = 2, multi-AZ, alerts, DLQ, monitoring) | P1 |
| FR-8.6 | Incident drills: alert → diagnose → fix | P2 |
| FR-8.7 | Mission lifecycle tracker: requirements → plan → design → deploy → load test → go-live → incident | P2 |

## 9. Simulation Mode (Sandbox)

| ID | Requirement | Pri |
|---|---|---|
| FR-9.1 | Full palette, tips off (can be turned on); all load, chaos and geo tools available | P0 |
| FR-9.2 | Starter templates: simple queue, fan-out, stream app, multi-AZ cluster, two-region DR | P1 |
| FR-9.3 | Non-blocking config warnings | P0 |
| FR-9.4 | Open any campaign level in the sandbox | P1 |

## 10. Persistence & Sharing

| ID | Requirement | Pri |
|---|---|---|
| FR-10.1 | Save and load locally; export/import JSON (topology + config + load profile + seed) | P0 |
| FR-10.2 | Generate real config / CLI snippets from the canvas | P1 |
| FR-10.3 | Share via link; accounts for cross-device progress | P2 |

## 11. Out of Scope (for now)

- Multi-user collaboration
- Exact hardware performance numbers
- (Real Kafka is in scope through Real Lab, v0.3, and the MVP fidelity harness; see the Real Kafka Lab doc)

## 12. UI Reference

Wireframes: `docs/ui/wireframes/` (open `index.html`); sizes and colors in `docs/ui/ui-spec.md`; animations in `docs/ui/animations.md`. Screens: 01 Home + Campaign map · 02 Learn · 03 Break it · 03b Fix failed ("Not quite") · 04 Debrief (3 stars) · 04b Debrief (2 stars) · 05 Sandbox · 05b Properties panel · 05c Partition log panel.

Note: priorities in §2b, §4 and §5 marked P0 belong to their release (v0.2 / v0.5), not the MVP. The roadmap defines what ships in each release.

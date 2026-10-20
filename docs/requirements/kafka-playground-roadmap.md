# Kafka Playground: Release Roadmap (v0.3 draft)

## Decisions

| # | Decision | Choice |
|---|---|---|
| D8 | UI framework | **React** (React Flow for the canvas); stable, mature UI ecosystem preferred |
| D9 | Simulation in MVP? | **Yes.** The engine is the core of the game; a basic Sandbox ships in the MVP. Heavy simulation features (scale, flow mode, geo) are deferred |
| D10 | Build approach | **AI-assisted development.** AI writes most code; Rhea owns design, review and Kafka correctness. Engine semantics are reviewed line by line, since learning Kafka is a project goal |
| D11 | Fidelity harness | **In MVP.** docker-compose 3-broker KRaft cluster + Go runner + scenario comparison against the TS engine, as a correctness check for AI-written engine code |

## Releases

### MVP (v0.1): "Core Kafka"
- Engine (TypeScript, Web Worker, seeded, per-message mode): partitions & keys, offsets, consumer groups + rebalancing, replication + ISR + leader election, acks + min.insync.replicas, auto/manual commits
- Canvas (React Flow): Producer, Broker, Topic, Consumer, Consumer Group; connection rules; properties panel
- Play / pause / step / speed / reset; kill/restart broker and consumer, slow consumer
- Partition log viewer, event timeline, lag badges, manual "produce message"
- **Campaign World 1 + World 2** (10 levels, Learn → Break → Fix), scripted tips and dialogues, stars, progress save
- **Basic Sandbox** (≤ 5 brokers); **code panel** (read-only, Java / Go / CLI)
- Local save/load, JSON export, autosave; CI playback of each level's solution
- **Fidelity harness** (sim vs real Kafka for 8 core scenarios). See architecture §9

### v0.2: "Under Pressure"
Resource bars, saturation, load generator, bulk topics, flow mode, heatmaps, metrics, alerts, World 3

### v0.3: "Real Lab"
Lab Bridge (grows from the fidelity runner), state mirroring, runners, Real Lab step for W1–W2, Sandbox "Live" toggle

### v0.4: "Patterns"
Retry/DLQ, fan-out, retention & compaction, idempotence & transactions, streams, World 4

### v0.5: "Going Global"
World map, AZs/regions, MirrorMaker 2, outages and failover, World 5

### Later
Planning missions, hint ladder, quizzes, own-code challenges, AI tutor, Schema Registry, Connect, share links

## Effort (rough, ~10 hrs/week)

| | Manual | AI-assisted |
|---|---|---|
| MVP (incl. fidelity harness) | 11–15 weeks | **~5–7 weeks** |
| Each later release | 4–8 weeks | ~2–4 weeks |

Where AI speeds things up most: React UI, canvas wiring, panels, code-panel templates, level YAML, tests, docker-compose, Go runner boilerplate.
Where time is still spent: verifying Kafka semantics, designing levels that teach, game-feel iteration, debugging timing-sensitive simulation logic.

MVP milestone order: see architecture §11 (M1–M8). Milestones are broken into tasks one at a time, in `docs/milestones/`.

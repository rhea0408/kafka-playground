# Kafka Playground: Real Kafka Lab (FR §13, addendum to functional requirements)

Goal: connect what the player learns in the simulator to real Kafka behavior and real client code, using a Kafka cluster running locally in Docker.

## Decisions

| # | Decision | Choice |
|---|---|---|
| D5 | Lab Bridge language | **Go** |
| D6 | Code panel languages | **Java, Go and CLI** tabs. Java is required for Kafka Streams labs (World 4). The Go examples use Confluent's Go client (config keys match Java) |
| D7 | Code panel direction | **One-way** (canvas → code) for the MVP; editable code challenges later (FR-13.10) |

## 13.1 Architecture

A browser cannot speak Kafka's wire protocol, so a small local backend (the Lab Bridge) sits in between:

```
Browser (game UI) ⇄ WebSocket/HTTP ⇄ Lab Bridge (Go, localhost) ⇄ Kafka cluster in Docker (3 brokers, KRaft)
                                         ├─ AdminClient: topics, partitions, offsets, consumer groups, lag
                                         ├─ runs producer/consumer runners (Go + Java)
                                         └─ Docker control: stop/start broker containers, inject latency (Toxiproxy)
```

## 13.2 Requirements

| ID | Requirement | Pri |
|---|---|---|
| FR-13.1 | One-command local cluster: docker-compose with 3 KRaft brokers, plus an optional 2nd cluster + MirrorMaker 2 for World 5 | P1 |
| FR-13.2 | Lab Bridge streams real cluster state to the UI: topics, partition leaders, ISR, offsets, consumer group members, lag | P1 |
| FR-13.3 | Mirror view: the canvas renders the *real* cluster state using the same components as the simulator | P1 |
| FR-13.4 | **Live code panel:** every canvas change updates the generated code immediately in the Java / Go / CLI tabs, with changed lines highlighted | P1 |
| FR-13.5 | Per-line explanations in the code panel (tooltip on each config/API call); shown by default in Learning mode | P1 |
| FR-13.6 | Canvas actions map to their real-world equivalents in the panel (kill broker → `docker stop broker-2`; add consumers → "run N instances of this code") | P1 |
| FR-13.7 | Run on real Kafka: runners execute the code shown in the panel against the cluster | P1 |
| FR-13.8 | Real failure actions: stop/start a broker, kill a consumer, add network latency (Toxiproxy / tc netem) | P1 |
| FR-13.9 | Sim vs Real comparison, side by side, with differences explained | P2 |
| FR-13.10 | Write-your-own-code challenges in an in-game editor, run in a sandboxed container | P2 |
| FR-13.11 | The game works fully without Docker; Real Lab steps are optional and skipped gracefully | P0 (for the design) |

## 13.3 Where it appears

**Game mode:** an optional 4th step in each level: Learn → Break → Fix → **Real Lab** (see the code, run it for real, repeat the same break for real, compare).
- W1–W2: excellent fit
- W3: scaled down
- W4: good fit (Java for Streams)
- W5: approximated with 2 local clusters + MirrorMaker 2 + injected latency

**Simulation mode:** a "Live" toggle. The canvas drives the real cluster within laptop limits. Scale and geo scenarios stay simulated.

**Principle:** the simulator handles scale and geography; Real Lab handles correctness and code.

## 13.4 Local footprint (Windows laptop, Docker Desktop + WSL2)

- 3-broker cluster: ~3–4 GB RAM; with a 2nd cluster + MirrorMaker 2: ~6–8 GB
- Disk: ~5–10 GB

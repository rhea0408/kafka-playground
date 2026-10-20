# M1 summary: Engine core

All tasks T0–T11 are done and merged into `develop` (PRs #1–#12), each with green CI.
155 tests: 129 in `@kp/engine`, 26 in `@kp/schema`.

## 1. Built

- **Repo:** pnpm workspace (`@kp/engine`, `@kp/schema`, placeholder `@kp/web`). Strict TS 6.0, ESLint (typescript-eslint strict, type-checked) + Prettier, Vitest + fast-check. CI: install → lint → typecheck → test → demo.
- **`@kp/schema`:** Zod schemas for broker / topic / producer / consumer, with real Kafka 4.x defaults (game-only settings marked). `parseTopology`, `parseComponent`, `canConnect(from, to)` and readable one-line errors.
- **`@kp/engine`** (module map in `packages/engine/README.md`):
  - `core/`: seeded sfc32 RNG with `fork`, integer sim clock, `(time, seq)` heap, generic event loop.
  - `model/`: partition log (LSO / LEO / HW), placement, state, data-only tasks.
  - `protocols/`: murmur2 + sticky partitioner, produce (acks 0/1/all), consume (poll loop, `auto.offset.reset`, auto-commit, close vs crash).
  - API: `createEngine`, `dispatch`, `step`, `runUntil`, `on`, `getSnapshot`, `commandLog`, `replayCommandLog`, `formatEvent`.
  - Commands: `AddComponent`, `RemoveComponent`, `Connect`, `Disconnect`, `ProduceManual`, `KillConsumer`, `RestartConsumer`.
  - Events: `ComponentAdded/Removed`, `Connected/Disconnected`, `RecordProduced`, `RecordAppended`, `ProduceAcked`, `ProduceFailed`, `RecordConsumed`, `OffsetCommitted`, `ConsumerStopped/Started`.
- **Commands:** `pnpm test`, `pnpm lint`, `pnpm typecheck`, `pnpm format`, `pnpm --filter @kp/engine demo [-- --seed N --until MS]`.

## 2. Deviations from the docs

| Doc says | Built | Why |
|---|---|---|
| T7: `maxPollRecords` "from each assigned partition" | Cap on the whole `poll()` over all partitions | That is what `max.poll.records` means (`ConsumerConfig` doc) |
| §4 / T8: lag = LEO − committed | Lag = **HW** − committed | `kafka-consumer-groups.sh` uses the end offset, which is the HW for read_uncommitted (`KafkaConsumer#endOffsets`). Equal in M1, different from M4 |
| T5: sticky "stay for N records" | Stay until `batch.size` bytes (record size × count), then switch | Matches `BuiltInPartitioner#updatePartitionInfo`; N = batch.size ÷ messageSizeBytes |
| T7: "auto-commit on an interval" | Checked at the start of `poll()` once the interval has passed | Classic consumer behaviour; gives the at-least-once semantics the javadoc describes |
| §4.4 lists Pause / SetSpeed / Step as commands | Not engine commands, and not logged | They change how fast the worker drives the engine, never the outcome |
| §4.4: commands stamped with sim time | Log entries are `{ t, afterStep, command }` | `t` alone can't place a command dispatched between two tasks of the same ms; a test shows a naive replay diverges |
| §4.5 event list | Added `ComponentAdded/Removed`, `Connected/Disconnected`, `ConsumerStopped/Started` | The UI needs topology and lifecycle events |
| Layout `packages/engine/core/…` | `packages/engine/src/core/…` | Conventional `src/`; same module names |
| `tsx` for the demo (proposed) | Node 22 runs the `.ts` directly; relative imports use `.ts` extensions | No new dependency |
| TypeScript latest (7.x) | TypeScript 6.0 | typescript-eslint supports TS < 6.1 |
| — | Engine rejects RF > 1, a second consumer in a group, and removing a broker that leads partitions, each with an explanation | M1 has no replication, groups or reassignment yet |

## 3. Simplifications added (`docs/simplifications.md`)

Partition placement, single replica, broker removal, sticky partitioner, key serialisation, network delay, producer batching, produce retries, producer workload, acks=all in M1, consumer fetching, records per poll, auto-commit timing, one consumer per group, manual commits, consumer crash detection, retention, topic deletion, integer time.

## 4. Open issues / tech debt

- **To confirm with real Kafka (M5):**
  - Auto-commit timing. The classic consumer checks inside `poll()`; the KIP-848 consumer uses a background timer.
  - Java's `?` replacement for unpaired surrogates in UTF-8 keys.
- **Partitioning surprise:** with 3 partitions, keys A, B and C all hash to partition 1 (cross-checked with an independent murmur2 port), so the demo uses only one partition. Lessons that teach key spread should use keys that actually spread.
- **Trace ids:** a producer re-added with the same id restarts at `p1#1`, so trace ids are unique only per producer life.
- **Snapshot performance:** `getSnapshot()` deep-copies through JSON on every call. That's fine for M1 sizes; M2 should add the planned snapshot diff before going to 30 Hz with many partitions.
- **Invariants:** only the M1 subset of §4.6 is covered. ISR, leader, group-assignment and acks=all-durability invariants are still to add (M3/M4).
- **Coverage (NFR-4.4, P1):** not measured. It would need `@vitest/coverage-v8`.
- **No flaky tests** seen. fast-check uses a new seed each run; a 3,000-case soak passed locally.
- **Environment:**
  - `gh pr edit` fails with a GitHub "Projects classic" error, so PR bases are changed through the REST API.
  - HTTPS pushes of workflow files need `gh auth refresh -s workflow`; the repo push URL is SSH.
  - The `ubuntu-latest` runner moves to Ubuntu 26 from 2026-10-19.

## 5. Fidelity status

Not started; the harness arrives in M5. Facts pinned in tests for M5 to verify: murmur2 key → partition (Kafka's `UtilsTest` vectors already pass), `acks=0` reports offset −1, `latest` = HW, auto-commit timing.

## 6. Questions / suggestions for M2

1. **`UpdateConfig`:** the properties panel needs it. Its semantics need deciding, e.g. a rate change restarts the producer schedule, and adding partitions remaps keys (FR-4.7).
2. **`LagChanged`:** animation A5 needs this event. Should it be emitted per partition when lag changes, or derived in the UI from snapshots?
3. **Animations:** should `RecordConsumed` drive A2 (the fetch dot) directly? It is emitted at poll time.
4. **Worker API:** Comlink exposing `dispatch` / `runUntil` / `getSnapshot`, with snapshot diffs ~30×/s (§4.1), and the event list reusing `formatEvent`.
5. **Game-tuned labels:** decide how they appear in the properties panel for `pollIntervalMs`, `processingTimeMs`, `messageSizeBytes` and `ratePerSec` (CLAUDE.md rule 6, NFR-3.3).
6. **Writing `docs/milestones/M2-*.md`.**

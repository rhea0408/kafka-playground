# Milestone 1: Engine core

**Goal:** a headless, deterministic simulation of one broker, topics with partitions, a producer and a consumer, with offsets and key-based partitioning. There is no UI in this milestone.
**Done when:** all tasks below pass, `pnpm test` is green, and `pnpm --filter @kp/engine demo` prints a readable event log.

Work through the tasks in order, using one Claude Code session or PR per task. Each task has a **learning checkpoint**: questions Rhea should be able to answer after reviewing the code. If she can't answer one yet, explain that part before moving on.

---

### T0 · Repo scaffold
- pnpm workspace; root `tsconfig.base.json` (strict, `noUncheckedIndexedAccess`), ESLint + Prettier, Vitest.
- Packages: `packages/engine` (`@kp/engine`), `packages/schema` (`@kp/schema`). Empty `apps/web` placeholder.
- ESLint rule in `packages/engine`: ban `Math.random`, `Date.now`, `performance.now`, `setTimeout`, `setInterval`, and any DOM globals.
- GitHub Actions: install → lint → typecheck → test.
- **Done when:** `pnpm lint && pnpm typecheck && pnpm test` pass on a placeholder test, both locally and in CI.

### T1 · Deterministic core
- `core/rng.ts`: seeded PRNG (e.g. mulberry32 or sfc32) with `next()`, `int(min,max)`, `pick(arr)`, `fork(label)`.
- `core/clock.ts`: simulated time in ms (integer).
- `core/queue.ts`: binary-heap priority queue ordered by `(time, seq)`, where `seq` is a monotonically increasing counter.
- **Tests:** tie-breaking order; the same seed gives the same sequence; 100k push/pop performance sanity check.
- **Learning checkpoint:** Why does the queue need `seq` as well as `time`? Why can't the engine call `Date.now()`?

### T2 · Engine loop, commands, events
- `createEngine({ seed, topology })` returning `{ dispatch(cmd), step(), runUntil(ms), getSnapshot(), on(listener), commandLog }`.
- Commands and events are **discriminated unions** (`type` field). Accepted commands are stamped with sim time and appended to `commandLog`.
- Events emitted in order with `{ t, seq, type, ...payload }`.
- **Tests:** dispatch → events; `runUntil` stops exactly at the boundary; replaying `commandLog` on a fresh engine reproduces the same event log.

### T3 · Schemas
- `@kp/schema`: Zod schemas for `Topology` (brokers, topics, producers, consumers, connections) and per-component config with defaults.
- Connection rules (producer→topic, topic→consumer allowed; others rejected with a reason) as a pure function shared with the UI later.
- **Tests:** valid and invalid topologies; error messages are human-readable.

### T4 · Broker, topic, partition log (single broker)
- Model classes or records for `Broker`, `Topic`, `Partition` with `log`, `logStartOffset`, `logEndOffset` (LEO), `highWatermark` (HW). With one broker, HW = LEO after each append.
- **Tests:** offsets start at 0 and are contiguous; append returns the assigned offset.
- **Learning checkpoint:** What is an offset? What is the difference between LEO and HW, and why are they equal with a single broker?

### T5 · Partitioner
- Keyed records: port Kafka's Java default exactly: `toPositive(murmur2(keyBytes)) % numPartitions`, using UTF-8 key bytes. Port `murmur2` line by line from Apache Kafka's `org.apache.kafka.common.utils.Utils` and cite the source in a comment.
- Null key: simplified sticky partitioner (stay on one partition for N records, then move on), documented in `docs/simplifications.md`.
- **Tests:** the same key always gives the same partition; distribution is reasonably even over 10k random keys; changing the partition count changes the mapping (this is taught in a later level). Exact parity with real Kafka is verified in M5 by the fidelity harness.
- **Learning checkpoint:** Why does the same key always go to the same partition? What breaks if you add partitions later?

### T6 · Producer
- Deterministic rate scheduling (e.g. 2 msg/s gives one record every 500 ms of sim time); key strategy (null / fixed / random-from-set / skewed) using the RNG.
- acks on a single broker: `0` = no ack wait; `1` / `all` = ack after the leader appends (they differ only once there are replicas, in M4).
- Events: `RecordProduced`, `RecordAppended`, `ProduceAcked`.
- **Tests:** rate accuracy over 10 s; key distribution; event order.

### T7 · Consumer (single consumer, no group yet)
- Poll loop every `pollIntervalMs`; reads up to `maxPollRecords` from each assigned partition starting at its position; per-message `processingTimeMs` delays the next poll.
- `auto.offset.reset`: `earliest` / `latest` when there is no committed offset.
- Simple committed-offset storage per partition (auto-commit on an interval). Full group semantics come in M3.
- Events: `RecordConsumed`, `OffsetCommitted`.
- **Tests:** reads every record exactly once in order per partition; `latest` skips existing records; a slow consumer accumulates lag.
- **Learning checkpoint:** What is the difference between a consumer's position and its committed offset? What happens to messages produced while a consumer is stopped?

### T8 · Snapshot and lag
- `getSnapshot()` returns a plain serialisable object: brokers, topics/partitions (LEO, HW, last N records), producers (stats), consumers (position, committed, lag per partition).
- Lag = LEO − committed offset (per partition, and summed per consumer).
- **Tests:** snapshot is JSON-serialisable; lag matches a hand-computed scenario.

### T9 · Determinism and invariant tests
- Determinism: run the same seed + command log twice and compare a hash of the event log. Run in CI.
- fast-check properties over random topologies and command sequences: contiguous offsets, per-key order preserved within a partition, committed offset ≤ LEO, no record consumed before it was appended.

### T10 · Demo script
- `pnpm --filter @kp/engine demo`: producer (keys A/B/C, 2 msg/s) → topic `orders` (3 partitions) → consumer, for 5 sim seconds. Print a readable event log, e.g. `00:01.500  RecordAppended  orders-1  offset=2  key=B`, then a final snapshot table.
- This is mainly for Rhea to *see* Kafka behaviour before the UI exists.

### T11 · Docs
- `packages/engine/README.md`: module map and how an event flows through the engine.
- `docs/simplifications.md`: every simplification so far, with the real Kafka behaviour beside it.

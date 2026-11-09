# M4 summary: Multi-broker replication, ISR, elections and failures

M4 started with the M3 review fixes (T0 PRs #42–#45). The replication milestone (`M4-replication.md`, T0–T13) was then built unattended from 2026-11-08, and each task was **merged into `develop` after green CI** (PRs #46–#58). As decided in R1, T3, T5/T6 and T8 also merged on green CI, each after an independent subagent review whose fixes were added before merging. **Review those three PRs first: #49, #51 and #53.** Every decision is in `M4-decisions.md`: start with its "Review these first" table and the Kafka facts K1–K14, which were checked against the Kafka 4.4.0 source. The learning checkpoints are in `M4-learning-checkpoints.md`. Rewind, the event timeline, the scrubber and the lag chart were split out into **M4b** (Q1). Tests: 752 unit/property tests (39 + 392 + 321 across schema, engine and web) and 31 Playwright tests (5 new in `cluster.spec.ts`).

## 1. Built

- **Target:** Kafka 4.4.0 KRaft (`apache/kafka:4.4.0`), with ELR (KIP-966) on by default and strict min ISR.
- **`@kp/schema`:**
  - Broker replication and controller configs: `replicaLagTimeMaxMs` 10 s (game-tuned), fetch wait, `broker.session.timeout.ms` / heartbeat, leader imbalance check 30 s (game-tuned), `offsetsTopicReplicationFactor`. They are read-only (static configs).
  - Topic `minInsyncReplicas` and `uncleanLeaderElectionEnable`, both dynamic.
  - Producer `retries`, `deliveryTimeoutMs`, `requestTimeoutMs`, retry backoff and its maximum, and `enableIdempotence`.
  - Scene `networkDelayMs`.
- **`@kp/engine`:**
  - `model/partition.ts`: one log per replica, with LEO, HW, leader-epoch cache and producer state, plus ISR / ELR / epochs and KIP-320 truncation.
  - `model/placement.ts`: a port of `StripedReplicaPlacer`.
  - `protocols/replication.ts`: long-poll fetchers, the HW over the maximal ISR, ISR shrink and expand through AlterPartition, DelayedProduce, and divergence and truncation.
  - `protocols/controller.ts`: registration, heartbeats, fencing at 9 s, elections (ISR → ELR → last known leader → unclean), controlled shutdown and preferred leader election.
  - `protocols/producer-client.ts`: per-partition queues with ≤ 5 requests in flight, KIP-580 backoff (none on a leader change, KIP-951), delivery and request timeouts, and idempotence (PID, epoch, sequence; KIP-360 epoch bump).
  - `protocols/offsets-store.ts`: `__consumer_offsets` as a real replicated topic. Commits are answered at the HW, group metadata is stored as records, and a new coordinator loads the group without a rebalance. Clients use FindCoordinator and handle `NOT_COORDINATOR`.
  - Commands `KillBroker`, `StopBroker` and `RestartBroker`.
  - 12 new events (architecture §4.5).
  - Broker, partition, producer and controller snapshot fields.
  - Invariants: replication, log matching, no acked loss without an unclean election, and idempotence. The brokers-liveness property and a cluster determinism scene are also in place, with a perf baseline of 60 sim-s of a 3-broker scene in ≈ 69 ms.
- **`@kp/web`:**
  - The broker node shows its state chip, the partitions it leads and follows, and the groups it coordinates. A6 is built. The hover toolbar has Kill (crash), Stop (shutdown) and Restart.
  - Partition rows show the leader badge (A7), replica chips (A9) and the "lost" chip (A8).
  - The KRaft controller chip is shown when there are ≥ 2 brokers.
  - The partition log has a replica table and the list of truncated records.
  - "Re-create topic with RF n"; minISR and unclean election are editable.
  - The producer shows a "retrying" chip.
  - Four scenes: `failover`, `acks1-loss`, `not-enough-replicas` and `coordinator-failover`, with `e2e/cluster.spec.ts`.
- **Docs:** ADR 0005 (replication model), architecture §4.2–§4.6 and §11, `animations.md` A3/A6–A9, ui-spec (broker, replica chips, controller chip), simplifications, and the READMEs.
- **Commands:** unchanged. Nightly now also runs `replication.prop.test.ts` (1000 cases) and `brokers-liveness.prop.test.ts` (300 cases).

## 2. Deviations

| Docs say | Built | Why |
|---|---|---|
| T5 and T6 as separate PRs (each reviewed by Rhea) | One PR, #51 (T5-1); every review-gated PR merged on green CI after a subagent review (R1) | Elections without truncation break the log-matching invariant, and Rhea asked to keep merging |
| T5: "last ISR member stays in the ISR" | **ELR** (K1): the ISR can become empty, and the last member moves to the ELR | Kafka 4.4 default |
| T3: a below-minISR produce fails with `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | Usually `REQUEST_TIMED_OUT` (K6) | The HW freezes under strict min ISR (K1b) |
| T1 warning "minISR > RF: acks=all always fails" | Kafka applies min(minISR, RF) (T3-4) | `effectiveMinIsr` in the 4.4 source |
| A restarted RF-1 leader is a clean election | Elected as the "last known leader", reported `unclean` (T5-4) | KIP-966 behaviour; the UI shouldn't call it data loss |
| Cluster frame ≈ 778 × 150 with a controller chip on it | No frame in the Sandbox; the controller chip sits top-right (T11-1) | Brokers stay free nodes; the frame comes with the level layouts (M6) |
| A3 replication dots, A9 on the broker border | A3 **not built** (T11-6); A9 is on each replica chip (T11-3) | No broker-to-broker paths, and ISR membership is per partition |
| RF editable | "Re-create topic with RF n" (records and offsets go; T11-5) | Kafka changes RF only through reassignment (v0.2) |
| — | Every first join takes 2 ms longer (FindCoordinator round trip; T8-2); one-broker hashes changed (T12-5) | Real protocol; no golden hashes are pinned |

## 3. Simplifications added (`docs/simplifications.md`)

- **New rows:**
  - controller always up (one abstract quorum, exact session deadlines);
  - no disk loss;
  - replica logs kept in memory;
  - no per-record replication events;
  - idempotence state limits (5 batches, epoch bumped per partition);
  - no reassignment;
  - game-tuned replication timers (lag 10 s, imbalance check 30 s);
  - network delay per scene;
  - `__consumer_offsets` loading is instant, with no compaction, and SyncGroup doesn't wait for its record;
  - no batching / `linger.ms` in the producer.
- **Removed or rewritten "until M4" rows:** replication, acks=all, broker removal, fetch offset range, committed offsets storage, coordinator lookup.

## 4. Open issues / tech debt

- **A3 replication dots** are not drawn. `ReplicaFetched` is emitted for them.
- **No broker removal while partitions are placed on it.** RemoveComponent of a broker that holds replicas is refused (reassignment is v0.2).
- **`commitSync` failing with a retriable error is reported and not retried** inside the call, as in M3. Kafka retries until `default.api.timeout.ms`.
- **Property-test runtime:** `diff.prop.test.ts` and `determinism.test.ts` needed larger budgets under coverage (#54 follow-up, budgets now 120 s / 90 s). Keep an eye on CI time (Actions budget).
- **The acks=1 loss scene depends on a 20 ms network delay** (T5-6, T12-2). With the default 1 ms, follower lag is too small for a kill to lose acked records.
- **Still open from M3:** web branch coverage has no threshold; the partition-chip fade is wall-clock; WSL Playwright start is slow; real-hardware 60 fps is not measured.

## 5. Fidelity status

The harness is M5. Scenarios M4 makes possible:
- `leader-failover-acks-all`: no failed or lost records; new leader 6–9 s after the kill.
- `leader-failover-acks-1`: acked records truncated when the old leader returns.
- `isr-below-minisr`: `NOT_ENOUGH_REPLICAS`, then `REQUEST_TIMED_OUT` for parked writes.
- Coordinator failover without a rebalance.
- Controlled shutdown without failed produces.
- The preferred leader returning (30 s game-tuned vs 300 s real).

Facts worth checking against a real 4.4 broker:
- ELR and last-known-leader elections (K1, T5-4);
- the frozen HW under min ISR (K1b);
- the K6 error codes;
- FindCoordinator creating the topic (K10).

The compose file needs `offsets.topic.replication.factor` ≤ brokers, and the game-tuned timers from T1, so that timings match.

## 6. Questions / suggestions for the next milestone

1. **M4b or M5 first?** M4b (rewind, timeline, scrubber, lag chart) is needed by M6 levels; M5 (fidelity harness) would validate the M4 Kafka facts early. Suggestion: M5 first, while the K-facts are fresh.
2. **Should A3 be built** with the level layouts' cluster frame (M6), or dropped in favour of the replica chips?
3. **Rewind (M4b)** replays the command log; with three brokers a 10-minute busy scene is ~10 s of engine time at the T9 baseline, so checkpoints will likely be needed (M4b T1 measures first).
4. **Producer batching / `linger.ms`** is still one record per request. Is it needed before v0.2 load testing?

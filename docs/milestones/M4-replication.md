# Milestone 4: Multi-broker — replication, ISR, elections, broker failures

**Goal:** topics get real replication. Every partition has replicas on several brokers. Followers fetch from the leader, the leader tracks the in-sync replicas (ISR), and the high watermark (HW) advances only once the ISR has a record. `acks=all` and `min.insync.replicas` mean what they mean in Kafka.

A broker can crash, shut down cleanly or restart:
- The KRaft controller notices a crash, fences the broker and elects new leaders from the ISR.
- A restarted broker truncates its divergent records, catches up and rejoins the ISR.
- Producers retry, and consumers and consumer groups follow the leaders. That includes the group coordinator moving to another broker.
- On the canvas you can see the cluster: brokers up or down, the leader and ISR of each partition, replication, and records lost to truncation.

**Done when** (architecture §11, M4):
- The invariants pass, including the new replication invariants (T9) and the extended liveness property.
- Kill-broker works in the UI. On a topic with RF 3 and `min.insync.replicas` 2 with `acks=all`, killing the leader elects a new leader about `broker.session.timeout.ms` later, and no acknowledged record is lost.
- On the same topic with `acks=1`, killing the leader just after a write loses acknowledged records. They show up as truncated when the old leader comes back.
- With the ISR below `min.insync.replicas`, `acks=all` fails with `NOT_ENOUGH_REPLICAS`, visibly.
- A consumer group keeps its committed offsets and its members when the broker hosting its coordinator dies.
- `pnpm lint && pnpm typecheck && pnpm test:coverage` and Playwright are green in CI, and the nightly job runs the new properties.
- The browser worker gives the same event-log hash as Node for a scene with broker kills.

**In scope:** FR-2.5, FR-2.6, FR-3.1 (brokers), FR-1.4 (broker up/down, leader), FR-6.1 (replica columns in the partition log). Also the M3 carry-overs: coordinator failover, `COORDINATOR_NOT_AVAILABLE` handling, the `preparing`-retry session test, nightly invariants, and the fetch-offset-range TODO.

**Out of scope:**
- Rewind, the event timeline and scrubber, and the lag chart: proposed as **M4b** (Q1).
- Controller failover. The controller quorum is always up in M4 (Q2).
- Partition reassignment and broker decommissioning: v0.2 (FR-4.7). Removing a broker that holds any replica stays rejected.
- Rack awareness and multi-region (v0.5), slow-follower or network-partition injection (FR-3.3, P1), disk failure, and the resource bars (v0.2).
- Transactions and `read_committed` (v0.4).
- KIP-848 (after M5, as agreed).

Work through the tasks in order, one PR per task, as in M3. PRs for T3, T5, T6 and T8 wait for Rhea's review before merging. The other PRs merge on green CI.

---

## Decisions to make before starting

| # | Question | Recommendation |
|---|---|---|
| Q1 | **Split the milestone?** As written in §11, M4 holds two independent blocks: replication + failures, and rewind + timeline. Each is about the size of M3. | **Split.** M4 = replication and failures (this file). **M4b** = rewind (replay from the command log), the event timeline and scrubber (B5/B6), and the lag chart. M4b needs only the command log and determinism, which already exist, so it can run before, after or alongside M5. M6 needs both. Update §11 and the roadmap. |
| Q2 | **How to model the controller** | One abstract **KRaft controller quorum**, always up, shown as a small chip on the cluster frame, not as a broker. Brokers send heartbeats to it, and it fences a broker that is silent for `broker.session.timeout.ms`. It handles AlterPartition (ISR changes) and leader elections. Each request costs one network delay. Controller failover and combined mode (controller on a broker) get a simplifications row and come later. |
| Q3 | **Kill, stop and restart a broker** (like M3 Q8 for consumers) | `KillBroker` = **crash**: requests in flight are lost, and the controller notices only after `broker.session.timeout.ms`. `StopBroker` = **controlled shutdown**: the controller moves leadership off the broker first, then the broker goes down, and nothing is lost. `RestartBroker` = the process comes back with **its log on disk**: it registers, truncates divergent records, catches up and rejoins the ISR. Losing a disk is not modelled in M4. |
| Q4 | **How followers replicate** | Followers **long-poll** the leader (`replica.fetch.wait.max.ms` 500, `replica.fetch.min.bytes` 1), reusing the fetch long-poll from `fetch.ts`, rather than the fixed interval in architecture §4.3. Replication then takes about two network delays, as in Kafka. Fetch responses carry the leader's HW to the follower. |
| Q5 | **ISR rules and game-tuned timers** | Shrink the ISR when a follower hasn't caught up to the leader's LEO within `replica.lag.time.max.ms`. Expand it when the follower reaches the HW. Both go through AlterPartition to the controller, which bumps the partition epoch. Game-tuned: `replica.lag.time.max.ms` **10 s** (real 30 s). Kept real: `broker.session.timeout.ms` **9 s**, broker heartbeat 2 s. Exact rules to confirm: facts K2–K4. |
| Q6 | **The "acks=all + minISR 1" data-loss case** | FR-2.6 and architecture §4.3 say this loses data when the ISR has shrunk to the leader. **With clean elections it doesn't.** An out-of-ISR replica can't become leader, and in KRaft the last ISR member stays in the ISR, so the partition goes **offline** until that broker returns, with its data. The loss needs `unclean.leader.election.enable=true` (default false). **Model that topic config** so a level can show the loss, and fix FR-2.6, §4.3 and §4.6. Eligible Leader Replicas (KIP-966) may be on by default in the version we target. That changes how the ISR shrinks below `min.insync.replicas`, so decide after checking K1. |
| Q7 | **Producer retries and idempotence** | Kafka 4.x defaults are `retries` = MAX, `delivery.timeout.ms` 120 s, `request.timeout.ms` 30 s and `enable.idempotence=true`. Without retries a leader kill shows failures that wouldn't happen, and with retries but no idempotence it shows duplicates that wouldn't happen. **Model both:** metadata refresh on `NOT_LEADER_OR_FOLLOWER`, retries with backoff, and idempotence as per-partition sequence numbers. The broker dedups a retried record from producer state rebuilt from its log, so dedup survives a failover. Game-tuned: `delivery.timeout.ms` **30 s**, `request.timeout.ms` **10 s**. `enable.idempotence` shows as fixed to true ("off: v0.4"). |
| Q8 | **`__consumer_offsets` and coordinator failover** | Make it a **real internal topic**: 50 partitions (`offsets.topic.num.partitions`), created lazily, and only partitions some group maps to hold records. Commits and group metadata (generation, members, assignment) are appended there and replicated like any partition. A commit is answered once it reaches the HW. When the coordinator's broker dies, the new leader loads the group from its replica, and members get `NOT_COORDINATOR`, look the coordinator up again and carry on. **No rebalance if the group was Stable** (K10). Hide internal topics on the canvas; the partition log gets a "show internal topics" toggle. The alternative, an in-memory map copied on failover, is cheaper but can't show a commit lost because it was never replicated. |
| Q9 | **`offsets.topic.replication.factor` with few brokers** | The real default is 3. Real Kafka can't create the topic with fewer brokers, so groups get no coordinator, which would break every 1-broker World 1 scene. Game default: **min(3, live brokers when the topic is created)**, labelled `game-tuned` with the real default and that reason. Show the value as read-only on the broker. |
| Q10 | **Replica placement** | Port the KRaft `StripedReplicaPlacer` rack-unaware path with the seeded RNG (K13), replacing the M2 round-robin stripe. The preferred leader is `replicas[0]`. Model preferred leader election: `auto.leader.rebalance.enable=true`, check interval game-tuned **30 s** (real 300 s). Otherwise a restarted broker never gets its leadership back, which surprises players even though it's real behaviour for 5 minutes. |
| Q11 | **Event volume** | No per-record `RecordReplicated` or `HighWatermarkAdvanced`: like `LagChanged` in M2, they would double or triple the stream. Instead, `ReplicaFetched {broker, topic, partition, fromOffset, toOffset}` per follower fetch that returns data drives the A3 dots, and the UI reads the HW from snapshots. Low-volume events: `BrokerStarted`, `BrokerStopped {reason: crash \| shutdown}`, `BrokerFenced`, `BrokerUnfenced`, `LeaderElected {leader, leaderEpoch, reason: initial \| failover \| controlled-shutdown \| preferred \| unclean}`, `PartitionOffline`, `IsrShrunk`, `IsrExpanded`, `RecordsTruncated {broker, from, to, acked}`, and `CoordinatorMoved`. **Please approve the names.** They replace the "Planned" list in §4.5. |

**New dependencies:** none.

---

### T0 · Prep and M3 carry-overs
- **Pin the target Kafka version:** the `apache/kafka` image tag M5 will use (`docker/compose.kraft-3.yml`). Model that version's defaults, and record it in ADR 0005 (below).
- **Check facts K1–K14** against that version's source, the way K1–K18 were checked in M3. Record the answers in `docs/milestones/M4-decisions.md` before T3. If an answer changes a decision above, stop and ask.
- **Nightly job:** make `NUM_RUNS` in `invariants.prop.test.ts` settable from the environment, and add the file to `nightly.yml` at 1000 cases.
- **Docs:** fix FR-2.6 and architecture §4.3/§4.6 per Q6. Update §11 and the roadmap per Q1.
- **ADR:** write **0005-replication-model**: controller abstraction, follower long-poll, ISR via AlterPartition, clean elections by default.
- **Done when:** CI is green, the decisions file has K1–K14, and the docs no longer claim acks=all loss without unclean election.

### T1 · Schema and commands
- **Topic:**
  - `replicationFactor` > 1 is allowed. Reject RF > brokers at creation with Kafka's `INVALID_REPLICATION_FACTOR` text.
  - `minInsyncReplicas` > RF is allowed, as in Kafka, but shows a warning chip ("acks=all will always fail").
  - New `uncleanLeaderElectionEnable` (default false).
- **Broker:**
  - `replicaLagTimeMaxMs` (10 s, `game-tuned`, real 30 s);
  - `replicaFetchWaitMaxMs` (500);
  - `offsetsTopicReplicationFactor` (read-only, Q9);
  - `autoLeaderRebalanceEnable` and `leaderImbalanceCheckIntervalMs` (30 s, `game-tuned`, real 300 s).
- **Cluster (controller chip, read-only):** `brokerSessionTimeoutMs` 9000, `brokerHeartbeatIntervalMs` 2000.
- **Producer:**
  - `retries` (MAX, shown as "∞");
  - `deliveryTimeoutMs` (30 s, `game-tuned`, real 120 s) and `requestTimeoutMs` (10 s, `game-tuned`, real 30 s), with the client's rule `delivery.timeout.ms ≥ linger.ms + request.timeout.ms`;
  - `retryBackoffMs` 100;
  - `enableIdempotence` fixed true (Q7).
- **Commands:** `KillBroker`, `StopBroker`, `RestartBroker`, each with a logged entry and a snapshot status. Removing a broker that holds any replica stays rejected, with a reason that says so.
- **Tests:** a schema unit test for every rule, and the new fields in the fast-check arbitraries.

### T2 · Replica model and placement
- **Partition:**
  - `replicas[]`, `isr[]`, `leader | null`, `leaderEpoch` and `partitionEpoch`;
  - **one log per replica**, needed for truncation;
  - the leader's view of each follower's LEO and last caught-up time;
  - a leader-epoch cache per replica (epoch → start offset), used for truncation in T6.
- **Placement:** port `StripedReplicaPlacer` (Q10). New partitions from `UpdateConfig` use the same placer.
- **Bookkeeping:**
  - each broker gets a replica manager view: partitions it leads and follows;
  - snapshots gain per-replica LEO and HW, ISR and leader epoch;
  - extend `diffSnapshot` / `applyDiff` and the round-trip property test.
- **Not yet:** replication itself (T3). With RF 1 everything behaves exactly as in M3, and existing tests and hashes stay as they are.
- **Tests:** placement unit tests (spread, preferred leaders balanced across brokers), and determinism.
- **Learning checkpoint:** why does Kafka spread the *leaders* of a topic across brokers, not just the replicas?

### T3 · Follower fetch, HW and acks=all (Rhea reviews)
- **Follower fetch:** one fetcher per (follower broker, leader broker), long-polling as in Q4. The leader updates the follower's LEO from the fetch offset and computes **HW = min LEO over the ISR** (K5 for the rules after a leader change). Followers set their HW from the response. `ReplicaFetched` events.
- **Produce:**
  - `acks=all` with ISR < `min.insync.replicas` at append fails with `NOT_ENOUGH_REPLICAS`;
  - otherwise the leader appends and parks the request (DelayedProduce) until HW > offset, or `REQUEST_TIMED_OUT` after the request's timeout;
  - an ISR that shrinks below minISR while the request waits gives `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (K6);
  - `acks=1` answers after the leader's append, as now.
- **Consumer fetch:** reads up to the leader's HW. Fix the M2 F-c TODO: `OFFSET_OUT_OF_RANGE` only above the LEO; between HW and LEO the answer is empty.
- **Tests:**
  - HW moves only after every ISR member fetched;
  - a follower with a long fetch delay holds the HW back;
  - each `NOT_ENOUGH_REPLICAS` path;
  - acks=1 vs acks=all ack timing.
- **Learning checkpoint:** a consumer never reads above the HW. What could go wrong if it read up to the leader's LEO?

### T4 · ISR management
- **Leader:** checks each follower's last caught-up time every `replica.lag.time.max.ms / 2` (K3), and sends AlterPartition to the controller to shrink or expand the ISR (Q5).
- **Controller:** validates the partition epoch, applies the change, bumps the epoch and answers. The leader uses the new ISR only once it's confirmed, which is how the leader and controller stay consistent (K4).
- **Events:** `IsrShrunk`, `IsrExpanded`.
- **Snapshot:** under-replicated and under-minISR flags per partition.
- **Tests:**
  - a stopped follower leaves the ISR after `replica.lag.time.max.ms` (± the check interval);
  - a caught-up follower returns;
  - the HW moves forward as soon as a slow follower leaves the ISR.
- **Learning checkpoint:** why does leaving the ISR *unblock* `acks=all` producers instead of failing them, as long as the ISR stays at or above minISR?

### T5 · Controller, broker liveness and elections (Rhea reviews)
- **Liveness:** brokers send a heartbeat every 2 s. The controller fences a broker after `broker.session.timeout.ms` without one, emits `BrokerFenced` and, for each partition:
  - removes the broker from the ISR, except the last member (K2);
  - if it was the leader, elects the first live ISR replica in replica order (K12), bumps `leaderEpoch` and emits `LeaderElected {failover}`;
  - with no live ISR member, the partition goes offline (`PartitionOffline`, leader null); with unclean election on, picks any live replica (`unclean`).
- **Commands** (Q3):
  - `KillBroker`: the process stops at once; in-flight requests and parked produces vanish, and clients see timeouts.
  - `StopBroker`: controlled shutdown (K11). Leaders move off with `LeaderElected {controlled-shutdown}`, then the broker stops.
  - `RestartBroker`: the broker registers, gets unfenced (`BrokerUnfenced`) and starts its followers (T6).
- **Preferred leader election** (Q10): every check interval, a partition whose preferred replica is in the ISR but not leading moves back (`preferred`).
- **Clients** learn about changes through metadata responses and errors (T7), never directly.
- **Tests:**
  - a kill → fenced at 9 s ± one heartbeat → new leader from the ISR;
  - controlled shutdown → no `PartitionOffline`, no failed acks=all produce;
  - last replica standing → offline, then back with its data on restart;
  - unclean on → the out-of-ISR replica leads, and data is lost (T6).
- **Learning checkpoint:** after a crash, what decides how long a partition has no leader? Why is controlled shutdown so much faster?

### T6 · Restart, truncation and data loss (Rhea reviews)
- **Rejoining follower:** finds where its log diverges from the new leader by leader epoch (fetch `DivergingEpoch`, or OffsetsForLeaderEpoch, K9), truncates there, then catches up and rejoins the ISR.
- **`RecordsTruncated {broker, topic, partition, from, to, acked}`**, where `acked` counts records whose producer had received a success. That is the data-loss signal for levels and the A8 animation.
- The DuplicateProcessed / RecordsSkipped detector from M3 needs no change, since consumers only ever read below the HW. Add an engine-only **`AckedRecordLost`** teaching-aid check, or reuse `RecordsTruncated.acked`; decide in the PR.
- **Tests:**
  - acks=1 + leader kill right after a write → acked records truncated on the old leader's restart;
  - acks=all + minISR 2 → nothing acked is ever truncated;
  - unclean election → loss counted.
- **Learning checkpoint:** why is truncation by leader epoch safer than the old "truncate to my HW" rule? (KIP-101 has the example.)

### T7 · Clients during failover
- **Producer** (Q7):
  - Metadata refresh on `NOT_LEADER_OR_FOLLOWER`, `NOT_ENOUGH_REPLICAS`, timeouts or a dead connection.
  - Retries with `retry.backoff.ms`. Model the exponential growth up to `retry.backoff.max.ms` (KIP-580, also carried from M3) if it's small.
  - `ProduceFailed` only once `delivery.timeout.ms` expires, or on a non-retriable error.
  - Idempotence: sequence numbers per partition. The leader dedups a retry and answers with the original offset. Producer state is rebuilt from the log on a new leader; after truncation, the producer's handling follows K8.
- **Consumer:**
  - `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` → metadata refresh and retry;
  - an offline partition → no fetch until a leader exists;
  - a new leader whose HW is behind the position → empty fetches until it catches up (K5).
- **Tests:**
  - a leader kill under load with acks=all → no `ProduceFailed`, no duplicates in the log;
  - a retried record lands once;
  - a delivery timeout with the partition offline for 40 s → `ProduceFailed`.
- **Learning checkpoint:** without idempotence, how can a retry write a record twice, and how do sequence numbers stop it?

### T8 · `__consumer_offsets` and coordinator failover (Rhea reviews)
- **The internal topic** (Q8, Q9):
  - created at the first FindCoordinator, with RF per Q9;
  - placement by the same placer;
  - hidden on the canvas, shown in the partition log behind a toggle.
- **Coordinator:**
  - appends commits and group-metadata records (on SyncGroup completion and member changes), and answers a commit only once it's replicated to the HW;
  - a coordinator that loses leadership answers `NOT_COORDINATOR`;
  - a new leader loads groups from its replica (`COORDINATOR_LOAD_IN_PROGRESS` while loading, K10) and restarts the members' session timers.
- **Client:**
  - FindCoordinator round trip (removes the M3 "instant lookup" simplification);
  - `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE` → mark the coordinator unknown, look it up again, retry (M3 carry-over);
  - commits in flight during the move fail as retriable.
- **Test the M3 carry-over:** a cooperative member in the `preparing` retry path in the middle of a long batch can hit `session.timeout.ms` (real Kafka does the same). Add a session-timeout term to the liveness precondition if the property finds it.
- **Tests:**
  - coordinator broker killed → `CoordinatorMoved`, no rebalance for a Stable group, committed offsets intact;
  - a commit acked before the kill survives;
  - a commit not yet replicated fails at the client and is not visible after the move.
- **Learning checkpoint:** why does Kafka store offsets in a topic instead of a database? What does a consumer see while its coordinator moves?

### T9 · Invariants, liveness and determinism
- **New invariants** (fast-check, in `group-checks.ts` style with tests that each rule rejects a bad log):
  1. Per partition: the leader is in the ISR, the ISR ⊆ replicas, and HW ≤ LEO of every ISR member.
  2. **Log matching:** any two replicas hold the same record at every offset below the smaller of their HWs.
  3. **Committed data is never lost:** no record that was ever below a leader's HW is truncated, unless an unclean election happened in that partition.
  4. acks=all with ISR ≥ minISR ≥ 2 at ack time: an acked record is never truncated after one broker failure (§4.6).
  5. Leader epochs and partition epochs only increase.
  6. A consumer never receives a record above the HW of the replica it read from.
  7. An idempotent producer's records are never duplicated in a log.
- **Liveness**, as its own property or by extending `liveness.prop.test.ts`. Each bound is in sim time, with the precondition stated.
  - After a kill, every partition with a live ISR member has a leader within `broker.session.timeout.ms` + one heartbeat + a few network delays.
  - After a restart, a follower that caught up is back in the ISR within `replica.lag.time.max.ms`.
  - An acks=all producer whose partitions have a leader and ISR ≥ minISR eventually gets every record acked.
  - Groups end Stable as before, now with coordinator moves.
- **Arbitraries:** 1–5 brokers, RF 1–3, minISR 1–3, kills, stops and restarts of brokers mixed with the existing client steps.
- **Determinism:** add broker steps to the determinism scenario and the worker hash e2e.
- **Performance:** record the engine time for 60 sim-seconds of the 3-broker default scene, headless, in the summary, as a baseline for NFR-2.4.

### T10 · Snapshot, events and formatting
- `formatEvent` text for every new event. New filter groups "Broker" and "Replication".
- Broker snapshot: status (`running`, `fenced`, `stopped`, `crashed`), partitions led and followed, and the coordinator for which groups.
- Controller chip snapshot: fenced brokers, offline partition count, under-replicated count.

### T11 · Cluster on the canvas
- **Cluster frame and broker nodes** (ui-spec: frame ≈ 778 × 150, broker 226 × 92 / 84 in the sandbox):
  - status with icon and word;
  - leader count, and an "offline" danger chip;
  - the controller chip on the frame.
- **Animations:** A6 failed broker (hatched fill and pulse), A7 leader badge flip, A9 out-of-ISR (dashed border plus the label "out of ISR", never color alone), A8 truncated cells, A3 replication dots from `ReplicaFetched` (decorative, capped like A1/A2).
- **Topic node:** per partition, the leader badge `L b2`, replica chips with ISR state, and an under-replicated chip.
- **Broker hover toolbar** (200 × 36): **Kill (crash)**, **Stop (shutdown)**, **Restart**, with tooltips explaining the difference, as for consumers.
- **Properties:**
  - topic: RF, minISR and unclean election, with the warning chips from T1;
  - broker: the new fields with `game-tuned` chips;
  - producer: retries, timeouts, idempotence.
- **Partition log** (05c): a summary with leader, replicas, ISR, leader epoch, and LEO/HW per replica, plus a column for truncated records.
- **Tests:** component tests for each state. Playwright: kill a broker from its toolbar, and see fenced → new leader → restart → back in the ISR.

### T12 · Scenes and end-to-end
- **New scene presets** in the Scene picker, all 3 brokers and RF 3, with record keys D/E/F as before:
  1. "Leader failover": acks=all, minISR 2; kill the leader of P0.
  2. "acks=1 data loss": kill the leader right after a write, then restart it. The truncated records show.
  3. "Not enough replicas": minISR 2; kill two followers.
  4. "Coordinator failover": kill the broker coordinating `orders-app`.
- **Playwright:**
  - a new leader within about 9 s, no acked loss (scene 1);
  - truncated cells visible and `RecordsTruncated.acked > 0` (scene 2);
  - `NOT_ENOUGH_REPLICAS` shown on the producer (scene 3);
  - the group stays Stable, committed offsets unchanged (scene 4);
  - worker determinism with broker kills.
- **Keep World 1 working:** the 1-broker scenes still behave as in M3. Their event-log hashes may change because of `__consumer_offsets` and FindCoordinator; say so in the PR.

### T13 · Docs
- **`docs/simplifications.md`:**
  - remove the "until M4" rows (replication, acks=all, broker removal wording, fetch offset range, committed offsets storage, coordinator lookup);
  - add rows for: controller always up, no disk loss, one log per replica in memory, no per-record replication events, idempotence state limits (K8), no reassignment, and the game-tuned timers.
- **Other docs:**
  - architecture §4.2–§4.6, plus §4.5 events (Q11);
  - `animations.md` (A3, A6–A9 now built);
  - ui-spec (broker states, replica chips);
  - ADR 0005.
- **`docs/milestones/M4-learning-checkpoints.md`**, `M4-summary.md`, and CLAUDE.md "Current milestone" set to the next one (M4b or M5, per Q1).

---

## Facts to check during T0 (against the pinned version's source)
- **K1** — ELR (KIP-966): is it on by default in the target version? If so, how does the ISR shrink below `min.insync.replicas`, and which replicas can be elected?
- **K2** — KRaft fencing: `broker.session.timeout.ms` and heartbeat defaults. Does the controller remove a fenced broker from every ISR, leaving the last member in place ("last replica standing") with leader -1?
- **K3** — ISR shrink: the exact `replica.lag.time.max.ms` condition (`lastCaughtUpTimeMs`) and how often the leader checks it.
- **K4** — ISR expand: when a follower counts as caught up (≥ HW, ≥ the leader epoch's start offset?), and how the leader handles a pending AlterPartition (it uses the larger ISR until confirmed?).
- **K5** — HW after a leader change: does the new leader advance its HW only after a record of its own epoch is replicated? What do consumers get for offsets between that HW and the old one?
- **K6** — `NOT_ENOUGH_REPLICAS` vs `NOT_ENOUGH_REPLICAS_AFTER_APPEND`: where each is checked, and which timeout the parked produce uses.
- **K7** — Producer defaults in 4.x (`delivery.timeout.ms`, `request.timeout.ms`, retry backoff and its maximum), and which errors trigger a metadata refresh.
- **K8** — Idempotence on failover: rebuilding producer state from the log, and what happens when a truncation leaves a sequence gap (`OUT_OF_ORDER_SEQUENCE_NUMBER`, KIP-360 epoch bump).
- **K9** — How a follower finds its truncation point in KRaft-era Kafka: `DivergingEpoch` in the fetch response (KIP-320) vs OffsetsForLeaderEpoch.
- **K10** — `__consumer_offsets`: creation when RF > live brokers, how a new coordinator loads groups, and whether Stable members carry on without a rebalance.
- **K11** — Controlled shutdown in KRaft: the request sequence, and when the broker actually stops.
- **K12** — Election order: the first live ISR member in *replica* order (preferred first)?
- **K13** — `StripedReplicaPlacer` without racks: the leader start position and how followers are spread.
- **K14** — Unclean election in KRaft: when the controller runs it once enabled (at once, or on the next election trigger).

## Fidelity notes for M5
- Scenarios from architecture §9 that M4 makes possible: `leader-failover-acks-all`, `leader-failover-acks-1`, `isr-below-minisr`.
- To add: coordinator failover without a rebalance, controlled shutdown without failed produces, and the preferred leader returning.
- The harness compose file needs `offsets.topic.replication.factor` ≤ brokers (M3 note) and the game-tuned timers from T1, so sim and real timings are comparable.

---

## M4b sketch (if Q1 = split)
- **T1 Rewind:** a host operation, `rewindTo(t)`, that replays the command log from the start to `t` (architecture §4.7). Measure first, and add periodic checkpoints (serialised engine state) only if a 10-minute busy scene takes more than about 1 s to replay. Propose that bound as a new NFR.
- **T2 Event timeline (B5):** markers by event group on the bottom panel, with the playhead following sim time.
- **T3 Scrubber (B6):** dragging rewinds, the canvas shows the state at that time, and "Play" continues from there. This is the base for FR-7.4 and "Try again" (FR-7.14).
- **T4 Lag chart:** uPlot, lag per group over sim time, using `uncommitted` (F-5) so unknown lag isn't drawn as 0.
- **Done when:** rewinding to any time gives a snapshot identical to the original run at that time (property test), and Playwright scrubs back and forth on a broker-kill scene.

# M3 summary: Consumer groups, rebalancing and commits

Tasks T0–T13 were built unattended and each was **merged into `develop` after green CI** (PRs #28–#41), as Rhea asked on 2026-11-01. Every decision is in `M3-decisions.md` (start with its "Review these first" table and the Kafka facts K1–K18, which were checked against the Kafka trunk source). The learning checkpoints are in `M3-learning-checkpoints.md`. Tests: 615 unit/property tests (schema 33, engine 284, web 298) and 27 Playwright tests.

## 1. Built

- **`@kp/schema`**:
  - Consumer `sessionTimeoutMs` (10 s game-tuned), `heartbeatIntervalMs`, `maxPollIntervalMs` (30 s game-tuned), `partitionAssignmentStrategy` and `commitMode` (game setting), with Kafka's validation.
  - Broker `groupInitialRebalanceDelayMs` (read-only).
- **`@kp/engine`**:
  - `protocols/assignors/`: range, round-robin, sticky and cooperative-sticky, ported from the client, plus `rebalanceProtocol`.
  - `group-coordinator.ts`: the classic broker side. State machine, generations, leader, protocol vote, initial-delay windows, rebalance / pending-sync / session timeouts, LeaveGroup and OffsetCommit validation.
  - `coordinator.ts`: `__consumer_offsets` lookup.
  - `consumer-coordinator.ts`: the client side. Joining from `poll()`, onJoinPrepare (commit, eager revoke or lose), the leader assigning, heartbeats, the poll timeout (zombies), and leader-only metadata rebalances.
  - Commit modes and the `StopConsumer` command (graceful), `KillConsumer` (crash) and `RestartConsumer` (a new member).
  - The `DuplicateProcessed` / `RecordsSkipped` teaching aid; run-time `group.id` changes.
  - 16 new events; group and member snapshots.
  - Topic-ID fetches (F-b).
  - Six new fast-check invariants with stronger generators.
- **`@kp/web`**:
  - **Group node** (`group:<groupId>`) with a row per member: status, partition chips, idle chip, lag. The header shows state, generation and group lag. One edge per topic and group.
  - **A12** rebalance animation.
  - Drop a consumer on a group to add a member; a member toolbar with Kill / Stop / Restart.
  - Group properties panel (tiles, assignor for every member, partition table, mismatch chip).
  - Partition log: group picker and segmented partition picker.
  - **Scene** select (Default, Idle consumer, Crash and duplicates, Slow consumer, Empty; or `?scene=`).
- **Docs**: ADR 0004, architecture §4.2–§4.6, `animations.md` A12, ui-spec group node, simplifications, READMEs.
- **Commands**: as before, plus `pnpm test:coverage` with thresholds per package (engine and schema 90 % lines / 85 % branches, web 80 % lines).

## 2. Deviations

| Docs say | Built | Why |
|---|---|---|
| T3: "a commit during a rebalance gets `REBALANCE_IN_PROGRESS`" | Accepted while **PreparingRebalance**, refused only in CompletingRebalance (K1) | Kafka's `ClassicGroup#validateOffsetCommit`; the eager commit-before-rejoin depends on it |
| T1: remove the "one member per group" rule | Removed in **T4** (T1-4) | Two members without a coordinator would have read every partition twice |
| T6: "every subscribed partition has exactly one owner when Stable" | "No gaps from partition 0" for eager assignments (T6-2) | Stale leader metadata and cooperative rounds leave partitions unowned on purpose |
| T8: group node 220 wide, 196 px rows | **240 wide, 216 px rows** (ui-spec allows 220–240; T11-4) | The group id, state chip and stats fit |
| T11 stretch: lag chart | Not done; still a placeholder (T11-5) | Allowed by the milestone |
| ADR list: number 4 = level YAML | ADR **0004** is the classic group protocol, as the M3 task list asked | See question 4 below |

**Behaviour changes to know**
- The first rebalance of consumers that start together takes **two** initial-delay windows (≈ 6 s; K3).
- A restarted (crashed) consumer waits for the old member's session timeout (≈ 10 s; T4-14).
- `OffsetCommitted` arrives 1 ms after the `poll()` that sent it.
- A member killed before its first commit, with `latest`, makes the new owner skip records (T12-2).

## 3. Simplifications added (`docs/simplifications.md`)

- **Group protocol:** consumer groups (classic only), static membership and rack-aware assignment, heartbeat thread timing, the sticky assignor with different subscriptions.
- **Coordinator:** group coordinator lookup, JoinGroup member ids (no MEMBER_ID_REQUIRED), protocol-vote ties and leader re-election, the new members' join timeout, committed offsets storage, session timeout limits.
- **Commits and lifecycle:** poll / rebalance game-tuned timeouts, application commits (`commitMode`), the duplicate and skip detector, graceful stop.
- **Fetching:** fetch offset range (M4 TODO).
- **Rewritten:** consumer crash, config changes at run time, client metadata refresh, commits after a topic is re-created.

## 4. Open issues / tech debt

- **Web coverage** is 88 % lines and 78 % branches; only lines have a threshold (80 %).
- **Not modelled:**
  - The MEMBER_ID_REQUIRED round trip and FindCoordinator requests (instant lookups).
  - `__consumer_offsets` replication.
  - KIP-580 retry backoff growth.
- **The group panel's assignor select** sends one `UpdateConfig` per member, with no batching, so a refusal in the middle leaves members mixed. The panel says so when lists differ.
- **Partition chips** use a wall-clock 200 ms fade, not scaled by sim speed. At 10× a fast eager rebalance can look instant.
- **Still open from M2:** the slow local Playwright web-server start under WSL, and real-hardware 60 fps not measured.

## 5. Fidelity status

The harness is M5. Facts for M5 to check against a real 4.x broker:
- The two-window initial delay (K3).
- Commits accepted in PreparingRebalance (K1).
- `UNKNOWN_TOPIC_ID` handling after a re-create (T0-3).
- Eager vs cooperative revocations with a fifth member.
- Crash → duplicates.
- Idle member with range.

Note: the harness needs `offsets.topic.replication.factor` at most the broker count.

## 6. Questions / suggestions for M4

1. **Coordinator failover:** with several brokers, the `__consumer_offsets` leaders are placed once (T3-4). M4 should move a group to a new coordinator when its broker dies (members get `NOT_COORDINATOR` and look it up again), and allow removing the broker.
2. **KIP-848** (`group.protocol=consumer`): which milestone? Most of the client poll loop and commit code can be shared.
3. **Lag chart** (uPlot, per group over sim time): M4 as planned?
4. **ADR numbering:** 0004 took the number the architecture list planned for the level-YAML ADR. OK to give that one the next free number?
5. **Levels (M6):** the skip case found in T12-2 (`latest` + crash before the first commit) would make a good "Break it" step.

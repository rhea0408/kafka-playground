# Milestone 3: Consumer groups, rebalancing and commits

**Goal:** several consumers share a group and split its partitions. The group coordinator runs the classic protocol (JoinGroup / SyncGroup / Heartbeat, generations), using the range, round-robin, sticky and cooperative-sticky assignors. Crashes are detected by session timeout. Commits can be automatic or manual, so a crash visibly causes duplicates (at-least-once) or skipped records (at-most-once). The canvas shows a group node with one row per member, which partitions each member owns, idle members, and the rebalance as it happens.
**Done when** (architecture §11):
- The invariants pass, including the new group invariants.
- The idle-consumer case is visible: four consumers on a three-partition topic gives one member row marked "Idle".
- A killed consumer is removed after `session.timeout.ms`, and its partitions move with duplicates shown.
- `pnpm lint && pnpm typecheck && pnpm test` and the Playwright tests are green in CI.
- The browser worker still gives the same event-log hash as Node.

**In scope:** FR-2.7, FR-2.8, FR-2.9 and FR-3.2 (slow consumer, through `max.poll.interval.ms`). Also the Consumer Group component (FR-1.1, assignment strategy), plus the commit-mode and group configs from the consumer properties table. Still one broker.
**Out of scope:**
- The KIP-848 consumer protocol (`group.protocol=consumer`), static membership (`group.instance.id`) and rack-aware assignment: later, documented as simplifications.
- Coordinator failover and multi-broker (M4).
- Offset reset tool (FR-2.14, P1): stretch only.
- Bulk "add N consumers" (FR-1.6): M8.
- Lag chart and event timeline: M4, unless time allows.
- `read_committed` / transactions: v0.4.

Work through the tasks in order, one PR per task, as in M2.

---

## Review of M2 (done 2026-10-21)

The work matches the M2 task list. On a fresh clone of `develop`:
- lint, typecheck and the engine demo all pass;
- all 482 unit and property tests pass (schema 28, engine 179, web 275);
- the web build passes;
- 21 of 21 Playwright tests pass.

T0–T12 are met. T10 is met except that no field uses the `game-tuned` chip yet; it gets its first real use in M3. T13 is met except for stale text (see F-a). The worker determinism test does compare against a hash computed in Node.

Fix these in **T0** below before starting groups:

| # | Finding | Severity |
|---|---|---|
| F-a | `M2-summary.md` (line 3 and the P1 deviation row) and `M2-decisions.md` (lines 11 and 28, P1) still say "nothing is merged / waiting for review". All PRs #13–#27 are merged into `develop`. | Docs |
| F-b | **Fetch has no topic ID.** `consume.ts:362-370` copies `result.topicId` from the fetch response into the consumer's position, but the request (`fetch.ts:83-96`) carries no topic ID. A position from the old life of a deleted and re-created topic can therefore move onto the new topic's ID, which skips its first records. Later commits then pass the F1 guard. Real Fetch v13+ (KIP-516) sends the topic ID, and the broker answers `UNKNOWN_TOPIC_ID` on a mismatch. Fix: add `topicId` to the fetch partition and return the error. | Engine risk |
| F-c | `OFFSET_OUT_OF_RANGE` checks `fetchOffset > highWatermark` (`fetch.ts:91`). Kafka checks against the log end offset; an offset between HW and LEO gets an empty response. The two are equal while there is one replica, but this goes wrong in M4. Add a `TODO(M4)` comment and a simplifications row now. | Engine, M4 |
| F-d | `canvas.spec.ts` "keyboard add, select, move and delete" failed once in a full local run and passed three times on its own. It is likely timing-sensitive under load; make it wait on state rather than time. | Flaky test |
| F-e | Set coverage thresholds as suggested in the M2 summary: engine and schema at 90 % lines and 85 % branches, web at 80 % lines. Decide whether `window.__kp` ships in production builds; the recommendation is to gate it behind `import.meta.env.DEV \|\| ?debug`. | Tech debt |
| F-g | UI pass with screenshots compared to wireframes 05, 05b and 05c, at 1440 × 900 and 1366 × 768. Layout, node sizes, live log cells, markers and properties match. Small differences: the partition picker in the partition log is a dropdown, not the wireframe's segmented control (328 × 32); the speed button wraps from 10× to 0.25×; the broker node doesn't list the partitions it leads; there's no favicon, which causes a 404 in the console. Fresh event rows stay blank during their entry fade, so the list looks half-empty while playing. | UI nits |
| F-f | Smaller issues: the id race on two quick palette drops (`actions.ts:126`, the id is created before the `await`); `sim.worker.ts:47` sends frames without awaiting or handling errors; `dots.ts` `reset()` doesn't clear `lastCapSkipAt`; a same-count partition patch is a silent no-op, which isn't documented. | Nits |

The M2 deviations look right and can be accepted as they are: the metadata-refresh delay for new partitions (T2-2), a rate change not starting a new incarnation (T2-5), and Reset rebuilding from the current canvas (T5-6).

---

## Decisions to make before starting (answers to M2-summary §6)

| # | Question | Recommendation |
|---|---|---|
| Q1 | Group protocol | **Classic protocol only** (still the default `group.protocol` in Kafka 4.x), matching the classic fetch from M2. KIP-848 gets a simplifications row and a later milestone. This replaces T2-3 from M2: new partitions now trigger a rebalance. |
| Q2 | Game-tuned timeouts | `session.timeout.ms` **10 s** (real 45 s), labelled `game-tuned`. 10 s stays above the broker's `group.min.session.timeout.ms` of 6 s, so a real broker would accept it in M5. `heartbeat.interval.ms` **3 s, the real default** (≤ ⅓ of the session timeout still holds). `max.poll.interval.ms` **30 s** (real 300 s), `game-tuned`. `group.initial.rebalance.delay.ms` is a broker setting: keep the real **3 s** and show it on the broker as read-only. |
| Q3 | Lag threshold | Keep a fixed 100 in M3. Levels get their own threshold with `LagThresholdCrossed` in M6. |
| Q4 | How a player adds a second member | Both ways, as in Kafka. **Drop a consumer onto a group node** creates a consumer with that `group.id`, connected to the group's topics. **Editing `group.id`** on an existing consumer moves it to that group (leave, then join). The group node is a UI view of consumers that share a `group.id`, not an engine component, because the engine has no "group" object to create: the coordinator creates a group on the first JoinGroup. |
| Q5 | Run-time changes to `group.id`, `auto.offset.reset` and commit mode | All of these are allowed. Changing `group.id` is modelled as close() plus a new consumer: the consumer sends LeaveGroup, auto-commits if auto-commit is on, then joins the new group as a new member. Changes to `auto.offset.reset` and the commit mode apply from the next `poll()`. Changing the assignor applies at the next rebalance. Remove these fields from the "cannot change" list in simplifications. |
| Q6 | Subscription | Connecting a topic to a group node sends `Connect` for every member. The engine keeps subscriptions per consumer, as Kafka does. The UI shows a warning chip when members of one group subscribe to different topics; this is allowed in Kafka, but range assignment then looks uneven. |
| Q7 | Manual commits | Add a game setting `commitMode` that stands in for the application code: `auto` (uses `enable.auto.commit`), `sync-after-processing`, `async-after-processing`, `sync-before-processing` (at-most-once). Show it with a `game setting` chip and a tooltip naming `commitSync()` / `commitAsync()`. M7's code panel generates the matching loop. |
| Q8 | Kill vs stop | `KillConsumer` becomes a **crash**: heartbeats stop, nothing is committed and no LeaveGroup is sent, so the coordinator notices only after `session.timeout.ms`. A new command, `StopConsumer`, does a **graceful `close()`**: a synchronous auto-commit if auto-commit is on, then LeaveGroup, which triggers an immediate rebalance. `RestartConsumer` starts a new client, which joins with a new member id. |
| Q9 | Duplicates and skips | When a record is handed to the application (`RecordConsumed`), the engine compares its offset with the highest offset the group has already handed out for that partition. A lower or equal offset emits `DuplicateProcessed`. A position that jumps past that highest offset + 1 emits `RecordsSkipped {from, to}`. This is O(1) per partition, bounded and deterministic. `RecordsSkipped` is a new event name, so please approve it. |

**New dependencies:** none. uPlot is already approved, if the lag chart fits.

---

### T0 · M2 follow-ups
- Fix F-a to F-f above. F-b needs tests: re-create a topic while a consumer is processing; the position must reset through `auto.offset.reset` instead of keeping its old offset.
- Record the Q1–Q9 answers in `docs/milestones/M3-decisions.md` and write ADR `0004-classic-group-protocol.md`.
- **Done when:** CI is green with the coverage thresholds, and the M2 docs no longer say "unmerged".

### T1 · Schema: group and commit configs
- Consumer fields, with names and defaults from the ConsumerConfig docs:
  - `sessionTimeoutMs`, `heartbeatIntervalMs`, `maxPollIntervalMs`, `commitMode` (Q7);
  - `partitionAssignmentStrategy`: an ordered list. The real default is `[range, cooperative-sticky]`; offer the presets range / roundrobin / sticky / cooperative-sticky.
- Validation:
  - `heartbeatIntervalMs < sessionTimeoutMs`, as the client enforces;
  - the session timeout must be within the broker's `group.min/max.session.timeout.ms` (6 s – 30 min); otherwise reject it with Kafka's `INVALID_SESSION_TIMEOUT` reason.
- Broker field `groupInitialRebalanceDelayMs` (3000), read-only in M3.
- Remove the M2 rule "a group already has a member" (`apply.ts:120`).
- Field metadata: `game-tuned` for the session timeout and the poll interval (Q2), with the Kafka default line; `game setting` for `commitMode`; `group.instance.id` shown disabled ("not modelled").
- **Tests:** schema unit tests for every rule. Add the new fields to the fast-check arbitraries.

### T2 · Assignors (pure functions)
- `packages/engine/src/protocols/assignors/`: `range`, `roundrobin`, `sticky` and `cooperativeSticky`. Each takes `(members with subscriptions and owned partitions, partitions per topic)` and returns an assignment. Port the logic from the Java client (`RangeAssignor`, `RoundRobinAssignor`, `AbstractStickyAssignor`, `CooperativeStickyAssignor`) and cite them.
- Member ordering follows Kafka's: members are sorted by member id (range sorts per topic, round-robin sorts across all topics).
- Cooperative-sticky: if a partition changes owner, the first assignment leaves it unowned (revoked). It is given to its new owner in a follow-up rebalance.
- **Tests:** port a handful of cases from Kafka's own assignor tests. Property tests:
  - every subscribed partition is assigned exactly once when there is at least one member;
  - when all members subscribe to the same topics, the counts differ by at most 1 for round-robin and sticky;
  - stickiness: sticky keeps every partition whose owner is still a member, as long as the assignment stays balanced;
  - with more members than partitions, the extra members get an empty assignment.
- **Learning checkpoint:** with 2 topics of 3 partitions each and 4 consumers, what do range and round-robin each give? Why does range leave a consumer idle per topic?

### T3 · Group coordinator (broker side)
- Coordinator lookup: `partitionFor(groupId) = abs(javaStringHashCode(groupId)) % offsets.topic.num.partitions (50)`; the coordinator is the leader of that `__consumer_offsets` partition. With one broker this is trivial, but implementing it now prepares M4.
- Group state machine: `Empty → PreparingRebalance → CompletingRebalance → Stable`, plus `Dead`, as in `ClassicGroupState`. Generations, member ids, the leader (the first member to join) and the protocol choice (the first assignor that every member supports).
- JoinGroup:
  - when an Empty group gets its first member, wait `group.initial.rebalance.delay.ms`, extended as more members join, but never beyond the rebalance timeout;
  - otherwise wait for every known member to rejoin, until the rebalance timeout (`max.poll.interval.ms`), then remove the members that didn't rejoin.
- SyncGroup: the leader sends the assignment it computed on the client; the coordinator hands each member its part.
- Heartbeat: answers `REBALANCE_IN_PROGRESS` while the group is rebalancing. If no heartbeat arrives within `session.timeout.ms`, the member is removed and the group rebalances.
- LeaveGroup removes the member at once and starts a rebalance.
- OffsetCommit checks the generation and the member: a stale generation gets `ILLEGAL_GENERATION`, and a commit during a rebalance gets `REBALANCE_IN_PROGRESS`. Either way the commit fails.
- Events: `RebalanceStarted {groupId, generation, reason}` (reasons: join, leave, session timeout, poll timeout, metadata change); `RebalanceCompleted {groupId, generation, protocol, assignment}`; `MemberJoined`; `MemberRemoved {reason}`; `CommitFailed`.
- **Tests:** a unit test for each transition; timeouts tested on the sim clock.
- **Learning checkpoint:** why does the *client* leader compute the assignment, and not the coordinator? (KIP-848 moves it to the broker.)

### T4 · Consumer coordinator (client side)
- Joining happens inside `poll()`, as in the classic client. A member stuck processing therefore delays the rebalance for the whole group. This "stop the world" effect is the main lesson of level 9.
- Heartbeat thread: a timer independent of processing, every `heartbeat.interval.ms`. It stops on Kill (a crash).
- Poll timeout: if `poll()` isn't called within `max.poll.interval.ms`, the heartbeat thread sends LeaveGroup. The consumer only finds out at its next `poll()`, so a slow consumer turns into a zombie that keeps processing records it no longer owns. That is how FR-3.2 shows up.
- Eager assignors (range, roundrobin, sticky): before rejoining, revoke every partition, run a synchronous auto-commit when `enable.auto.commit` is on (`onJoinPrepare`), and drop the fetch buffer.
- Cooperative-sticky: keep the partitions that stay owned and give up only the revoked ones. A second rebalance follows on its own.
- Fetch only from assigned partitions. A new assignment starts from the committed offset, otherwise from `auto.offset.reset`.
- A metadata refresh that shows new partitions on a subscribed topic triggers a rebalance. This replaces M2 T2-3.
- Events: `PartitionsRevoked {consumerId, partitions}`, `PartitionsAssigned {consumerId, partitions}` (named after `ConsumerRebalanceListener`), and `ConsumerIdle {consumerId, groupId}` when a Stable assignment gives a member nothing.
- **Tests:** a crash is detected after the session timeout (±1 heartbeat); a slow consumer is removed after `max.poll.interval.ms`; eager revokes everything while cooperative keeps unchanged partitions; adding a fourth member to three partitions gives one `ConsumerIdle`.
- **Learning checkpoint:** what is the difference between a session timeout and a poll timeout, and which thread detects each?

### T5 · Commits, crashes and delivery semantics
- Commit modes (Q7):
  - `sync-after-processing` commits the position after the batch has been processed and blocks the next poll until the commit response arrives;
  - `async-after-processing` doesn't block, and a failure is only logged (no retry, as in the real API);
  - `sync-before-processing` commits right after `poll()` returns, which gives at-most-once.
- Auto-commit keeps the M2 behaviour (commit in `poll()` once the interval has passed) and also commits on revoke and on close.
- `KillConsumer` becomes a crash, `StopConsumer` a graceful close, and `RestartConsumer` starts a new member (Q8). Each needs an engine command, a logged entry and a snapshot status.
- `DuplicateProcessed` and `RecordsSkipped` (Q9).
- `group.id` and `auto.offset.reset` changes through `UpdateConfig` (Q5).
- **Tests:**
  - auto-commit and a crash mid-batch gives duplicates of the records processed since the last commit;
  - `sync-before-processing` and a crash gives skipped records;
  - after a rebalance, a commit from the old generation fails;
  - a zombie slow consumer's commit is rejected while the new owner's commit is accepted.
- **Learning checkpoint:** why is "commit after processing" at-least-once rather than exactly-once? What makes processing idempotent?

### T6 · Invariants and determinism
- New fast-check invariants:
  1. Within one generation, each partition is assigned to at most one member (architecture §4.6).
  2. When a group is Stable and has members, every subscribed partition has exactly one owner.
  3. An accepted `OffsetCommitted` always comes from a member of the current generation that owns that partition.
  4. Committed ≤ HW (exists).
  5. Generations only increase.
  6. A member never fetches from a partition it doesn't own in its current generation.
- **Don't** claim "two consumers never process the same partition at the same time". Real Kafka doesn't guarantee it (zombies). Invariant 3 is the real guarantee.
- Extend the arbitraries:
  - add or remove members, Kill, Stop and Restart;
  - change the assignor, `group.id` or commit mode;
  - increase partitions;
  - vary the processing time around `max.poll.interval.ms`.
- Add group steps to the determinism scenario.

### T7 · Snapshot, diff and events
- Snapshot: `groups` with state, generation, protocol, leader, members and assignment, plus committed offsets and lag per partition. Each consumer also gets its member id, generation, owned partitions, and status: `joining`, `stable`, `idle`, `stopped` or `crashed`.
- Extend `diffSnapshot` / `applyDiff` and the round-trip property test to cover them.
- Add `formatEvent` text for every new event, plus event-list filter groups ("Group", "Commit").

### T8 · Group node and rebalance on the canvas
- Consumer group node (ui-spec: 220–240 wide, header 36, one 196 × 40 row per member), built on the M2 group node:
  - the header shows the `group.id`, a state chip (`Stable`, `Rebalancing…`) and the generation;
  - each row shows the member, its owned partition chips (`P0 P2`), status (icon + label), position and lag;
  - an idle row shows "Idle: no partition", as a flat muted chip with an icon, never color alone.
- New animation **A12 rebalance**, added to animations.md:
  - on `RebalanceStarted`, the header chip changes to "Rebalancing…";
  - partition chips being revoked fade out over 200 ms; eager revokes all of them, cooperative only the ones that move;
  - on `PartitionsAssigned`, the chips fade in on their new rows over 200 ms.
  - Under reduced motion the change is instant.
- Topic → group edges. A2 dots end at the row of the member that consumed the record.

### T9 · Building groups on the canvas
- Drop a consumer from the palette onto a group node to add a member with that group's `group.id` and subscriptions (Q4, Q6). Dropping it on empty canvas creates a new group.
- Removing the last member removes the group node. The committed offsets stay with the coordinator, as in Kafka, until the topic is deleted.
- Hover toolbar on a member row: **Kill (crash)**, **Stop (leave)**, **Restart**, each with a tooltip explaining the difference.
- **Tests:** component tests for the drop target; Playwright: add four members to a three-partition topic and see one idle row.

### T10 · Properties
- Consumer: the new fields from T1, with the `game-tuned` chips and Kafka default lines. This is the first real use of the T10 game-tuned chip.
- Group (shown when the group header is selected):
  - stat tiles for members, generation and total lag;
  - the assignor, applied to every member;
  - a per-partition table with owner, committed offset, LEO and lag (FR-2.9);
  - the subscription-mismatch chip (Q6).

### T11 · Partition log and lag per group
- Partition log: when several groups read a topic, add a group picker. The committed marker belongs to the selected group.
- Lag badge per member row (that member's partitions), plus the group total in the header.
- *Stretch:* lag chart with uPlot (lag per group over sim time). Otherwise it stays a placeholder until M4.

### T12 · Scenes and end-to-end
- Add scene presets alongside the default, using keys D/E/F as in M2:
  1. "Idle consumer": 3 partitions, 4 members.
  2. "Crash and duplicates": auto-commit 5 s; kill a member mid-batch.
  3. "Slow consumer": processing time × `max.poll.records` > `max.poll.interval.ms`.
- Playwright:
  - the idle row is visible;
  - after a kill, a rebalance completes within about the session timeout and at least one `DuplicateProcessed` appears;
  - eager vs cooperative: with cooperative-sticky, the members that keep their partitions show no revoke.
- Re-run the worker determinism test with a group scenario.

### T13 · Docs
- `docs/simplifications.md`:
  - remove "Consumer groups (until M3)" and "Manual commits (until M3)";
  - rewrite "Consumer crash";
  - add rows for: classic protocol only (no KIP-848); no static membership; no rack-aware assignment; `__consumer_offsets` not modelled as a topic (commits apply instantly at the coordinator); auto-commit on close; the duplicate/skip detector being a teaching aid with no Kafka equivalent.
- Architecture §4.2–§4.6 (group model, events, invariants), animations.md A12, and ui-spec for the group node rows.
- `docs/milestones/M3-learning-checkpoints.md`: short answers to the T2–T5 learning checkpoints, in the same style as `M2-learning-checkpoints.md`.
- ADR 0004, `docs/milestones/M3-summary.md`, and CLAUDE.md "Current milestone" set to M4.

---

## Facts to check during M3 (fidelity notes for M5)
- Whether the classic consumer in 4.x auto-commits in `onJoinPrepare` for cooperative-sticky as well as eager. Check `ConsumerCoordinator` on trunk.
- How long the initial rebalance delay can grow when members keep joining (`InitialDelayedJoin`).
- The exact error a commit gets during `CompletingRebalance` compared with `PreparingRebalance`.
- M5 scenarios to add: idle consumer, crash leading to duplicates, and cooperative vs eager revocations.

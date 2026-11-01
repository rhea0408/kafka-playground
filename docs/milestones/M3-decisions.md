# M3 decisions log

Rhea asked for M3 to be built unattended, with each task merged into `develop` once CI is green. Where a decision was needed, the rule was the same as in M2: **behave like real Kafka 4.x as closely as practical**, and write the decision down here. Each entry gives the decision, the alternatives and the reason. The milestone summary is `M3-summary.md`; the task list is `M3-consumer-groups.md`.

## Review these first

The decisions with the most consequences, most important first. Engine semantics come before UI.

| # | Decision | Why it matters |
|---|---|---|
| [P1](#process) | **Each task is merged into `develop` after green CI**, one PR per task. | Unlike M2, nothing waits for review. Read the PRs in order (#28 onwards) to review afterwards. |
| [K1](#kafka-facts-checked-against-the-source) | **Commits are accepted while the group is PreparingRebalance**; only CompletingRebalance answers `REBALANCE_IN_PROGRESS`. | The milestone text said "a commit during a rebalance gets `REBALANCE_IN_PROGRESS`". The source says otherwise, and the eager "commit before rejoining" only works because of it. |
| [K2](#kafka-facts-checked-against-the-source) | **The default `partition.assignment.strategy` `[range, cooperative-sticky]` rebalances eagerly**, because the client uses the protocol every listed assignor supports. Cooperative needs `cooperative-sticky` alone. | Visible in every lesson that compares eager with cooperative. |
| [Q8](#answers-to-the-m3-questions-q1q9) | **`StopConsumer` finishes the batch it is processing, then closes**; `KillConsumer` crashes at once. | A graceful stop shows no duplicates and no skips; a crash shows them. |
| [Q9](#answers-to-the-m3-questions-q1q9) | **The duplicate/skip detector rolls back the unprocessed tail of a crashed batch.** | Without it, an at-most-once crash would never show `RecordsSkipped`. |
| [T0-1](#t0--m2-follow-ups) | **`window.__kp` only in dev builds or with `?debug` / `?fps`**. Playwright opens the app with `?debug`. | Production builds no longer expose the hooks. |
| [T0-3](#t0--m2-follow-ups) | **`UNKNOWN_TOPIC_ID` drops the position**: the partition restarts from the committed offset or `auto.offset.reset`. | Our reading of the client; the fidelity harness (M5) should confirm it. |

## Process

| # | Decision | Alternatives | Why |
|---|---|---|---|
| P1 | One branch per task, `m3-t<k>` from `develop`; a PR into `develop`; merged with a merge commit once GitHub Actions CI is green. Never merged on a red or missing run. | Stacked PRs waiting for review (M2) | Rhea asked on 2026-11-01: "keep merging each story after CI passes". |
| P2 | No new dependencies. | — | None were needed. |

## Answers to the M3 questions (Q1–Q9)

The recommendations in `M3-consumer-groups.md` are adopted. Where building them showed a detail, it is added here.

| # | Decision | Notes |
|---|---|---|
| Q1 | **Classic protocol only** (`group.protocol=classic`, the 4.x default). KIP-848 gets a simplifications row and a later milestone. New partitions now trigger a rebalance (replaces M2 T2-3). | Only the **leader** notices new partitions (K11), as in the classic client. |
| Q2 | `session.timeout.ms` **10 s** (real 45 s, `game-tuned`); `heartbeat.interval.ms` **3 s** (real default); `max.poll.interval.ms` **30 s** (real 300 s, `game-tuned`); broker `group.initial.rebalance.delay.ms` **3 s** (real default, read-only). | 10 s is above the broker's `group.min.session.timeout.ms` (6 s). |
| Q3 | Lag threshold stays a fixed 100 records. | Levels get their own in M6. |
| Q4 | Both ways to add a member: drop a consumer on a group node, or edit `group.id`. The group node is a UI view of consumers that share a `group.id`; the engine creates a group on the first JoinGroup, as the coordinator does. | |
| Q5 | `group.id`, `auto.offset.reset`, the commit mode and the assignor can change at run time. Changing `group.id` = close() + a new member of the other group. `auto.offset.reset` and the commit mode apply from the next `poll()`. The assignor list, `session.timeout.ms` and `max.poll.interval.ms` (the rebalance timeout) travel in JoinGroup, so the coordinator sees them at the next rebalance; the client uses the new heartbeat and poll intervals at once. | A real client is recreated to change any of these. |
| Q6 | Subscriptions are per consumer (its connected topics). A subscription change makes that member rejoin (`joinedSubscription` differs, K11). A consumer with no subscription is not a member: it leaves when its last topic is disconnected (`unsubscribe()`). | |
| Q7 | Game setting `commitMode`: `auto` (uses `enable.auto.commit`), `sync-after-processing`, `async-after-processing`, `sync-before-processing`. It is independent of `enable.auto.commit`, as in a real app that calls `commitSync()` with auto-commit still on. | |
| Q8 | `KillConsumer` = crash: no heartbeats, no commit, no LeaveGroup; detected after `session.timeout.ms`. `StopConsumer` = graceful: the application **finishes the batch it is processing** (and commits as its commit mode says), then `close()`: a synchronous auto-commit if auto-commit is on, then LeaveGroup. A consumer blocked in `poll()` closes at once (`wakeup()`). `RestartConsumer` starts a new client that joins with a new member id. `RemoveComponent` closes at once without waiting for the batch (the node is gone), counting the batch as processed. | Finishing the batch is what a shutdown hook calling `wakeup()` gives in a real app. |
| Q9 | `DuplicateProcessed` and `RecordsSkipped {from, to}` are approved. The group remembers the highest offset it has handed to the application per partition. **On a crash, the records of the current batch that were not processed yet are taken back** (if no one else has moved past them), so a crash after `sync-before-processing` shows `RecordsSkipped` for exactly the lost records. | A teaching aid with no Kafka equivalent (simplifications). |

## Kafka facts checked against the source

Checked on apache/kafka trunk (2026-11-01): `GroupMetadataManager`, `ClassicGroup`, `ConsumerCoordinator`, `AbstractCoordinator`, `ClassicKafkaConsumer`, the assignors, `KafkaApis#getCoordinator`. These answer the "Facts to check during M3" list and shape the engine.

| # | Fact | How the engine uses it |
|---|---|---|
| K1 | OffsetCommit validation (`ClassicGroup#validateOffsetCommit`): unknown member → `UNKNOWN_MEMBER_ID` (checked first); stale generation → `ILLEGAL_GENERATION`; **PreparingRebalance with the current generation → accepted**; CompletingRebalance → `REBALANCE_IN_PROGRESS`. Partition ownership is **not** checked. | Same rules. Invariant 3 ("an accepted commit comes from a current-generation member that owns the partition") holds because the client only commits partitions it owns, not because the broker checks. |
| K2 | The client's rebalance protocol is the highest one **every** configured assignor supports. Range supports only EAGER, so the default `[range, cooperative-sticky]` is eager; every member then votes for `range`. | `rebalanceProtocol(strategies)` in the engine. |
| K3 | Initial delay: when an Empty group starts its first rebalance, the join phase waits `group.initial.rebalance.delay.ms`. When the timer fires, if a member joined during the window and time remains, it waits another window; the total is capped by the rebalance timeout (max `max.poll.interval.ms` of the members). The initial join phase never completes early, even if everyone joined. | Modelled exactly. |
| K4 | Heartbeat: PreparingRebalance → `REBALANCE_IN_PROGRESS`; CompletingRebalance and Stable → OK; unknown member → `UNKNOWN_MEMBER_ID`; wrong generation → `ILLEGAL_GENERATION`. Members waiting in JoinGroup or SyncGroup do not expire. | Modelled. A member that got its JoinGroup response but has not sent SyncGroup yet is not protected; with 1 ms network delays this window is 0 in practice. |
| K5 | When the last member leaves, the group goes PreparingRebalance → Empty, and the **generation increments**. | Modelled; `RebalanceCompleted` with no members marks it. |
| K6 | Protocol selection: candidates are the assignors every member supports; each member votes for its first candidate; most votes wins. **Ties are broken by HashMap order**, i.e. by chance. | Ties are broken by the earliest-joined member's preference order (deterministic; simplifications row). |
| K7 | The first member to join is the leader. If the leader leaves or is removed, Kafka picks another member in HashMap order. | The longest-standing remaining member becomes leader (deterministic; simplifications row). |
| K8 | After the join phase, members that do not send SyncGroup within the rebalance timeout are removed and the group rebalances (pending sync). | Modelled (`sync-timeout`). |
| K9 | A known member's JoinGroup in Stable: from the leader, it always forces a rebalance; from a follower, only if its metadata (subscription, owned partitions, assignors) changed; otherwise it gets the current generation back. | Modelled. |
| K10 | `onJoinPrepare` auto-commits (eager **and** cooperative) and waits for the commit before sending JoinGroup. With a reset generation (kicked out), owned partitions are **lost** (`onPartitionsLost`), not revoked. Eager revokes everything; cooperative revokes only partitions of topics no longer subscribed. | Modelled, including `PartitionsLost`. The commit carries the current generation, and the coordinator (PreparingRebalance) accepts it. |
| K11 | Only the **leader** watches metadata for new partitions; every member rejoins when its own subscription changes. | Modelled. |
| K12 | `poll()` does not block on the join (`waitForJoinGroup=false`): a cooperative consumer keeps returning records from partitions it still owns during a rebalance. | Modelled; an eager consumer owns nothing during the rebalance, so it returns nothing ("stop the world"). |
| K13 | Poll timeout: the heartbeat thread sends LeaveGroup ("consumer poll timeout has expired."), resets the generation and member id. A later `commitSync()` fails **locally** with `CommitFailedException` ("…not part of an active group…"). A commit the broker rejects with `UNKNOWN_MEMBER_ID` / `ILLEGAL_GENERATION` also becomes `CommitFailedException`. | Modelled; `CommitFailed` carries the error. |
| K14 | Cooperative-sticky removes partitions that change owner from the first assignment; the member that lost them rejoins ("need to revoke partitions and re-join"). | Modelled. |
| K15 | Member order for range / round-robin: static members first, then by member id (string order). Partitions by number; round-robin walks topics in sorted order. | Same (no static members). |
| K16 | Sticky (`constrainedAssign`): quotas `floor(P/N)` / `ceil(P/N)`; owned partitions kept up to the quota; a partition claimed by two members goes to the higher generation (same generation: to neither); the rest are dealt round-robin to members below the minimum, then to members at the minimum until the over-minimum count is reached. | Ported (T2). |
| K17 | Coordinator = leader of `__consumer_offsets` partition `abs(groupId.hashCode()) % 50`. The topic is created on the first FindCoordinator; with the default `offsets.topic.replication.factor` 3 a single-broker cluster cannot create it and FindCoordinator keeps answering `COORDINATOR_NOT_AVAILABLE`. | The coordinator lookup is modelled; the internal topic's replication factor is capped at the number of brokers (simplifications row), as single-broker setups configure it. |
| K18 | JoinGroup v4+: the first JoinGroup with an empty member id is answered `MEMBER_ID_REQUIRED` with a new id; the client rejoins at once. | Not modelled: one extra round trip (2 ms). Simplifications row. |

## T0 · M2 follow-ups

| # | Decision | Alternatives | Why |
|---|---|---|---|
| T0-1 | Debug hooks (`window.__kp.sim`, `window.__kp.overlay`) are installed only in `vite dev` or when the URL has `?debug` or `?fps` (`debugHooksEnabled`). The Playwright specs that read them open `/?debug…`. | Keep them in every build | The M3 review recommended it. The e2e suite still checks the production bundle. |
| T0-2 | Coverage thresholds as per-package `test:coverage` scripts (`--coverage.thresholds.*`): engine and schema 90 % lines / 85 % branches, web 80 % lines. Root `pnpm test:coverage` runs them. | `vitest.config.ts` files | No new config files to lint and type-check; `pnpm test` stays fast. |
| T0-3 | **Fetch carries the topic ID (F-b).** `FetchPartition.topicId` is the position's topic life; the broker answers `UNKNOWN_TOPIC_ID` when the topic was re-created. The consumer then drops the position (and asks for metadata): the next `poll()` starts the partition from the committed offset (deleted with the old topic) or `auto.offset.reset`. Records already in the fetch buffer from the old topic can still be returned once. | Keep the position and let `OFFSET_OUT_OF_RANGE` catch it | That only catches it when the new topic is shorter; otherwise the consumer silently skips the new topic's first records. The milestone asks for the reset. |
| T0-4 | F-c: `TODO(M4)` in `fetch.ts` and a simplifications row "Fetch offset range". | — | |
| T0-5 | F-d: the keyboard test now waits on DOM state (`expect.poll`) for every transform read and checks the node is selected before the arrow key. | Retries | Removes the timing assumption. |
| T0-6 | F-f: ids are reserved while an AddComponent is in flight; `DotModel.reset()` clears the cap-chip timer; the worker unsubscribes a frame listener whose call fails (and logs it); the "same-count partition patch is a silent no-op" rule is documented in `update-config.ts`. | — | |
| T0-7 | F-g: favicon added (no more 404); event-list rows start their entry fade at 40 % opacity so a busy list is readable. **Deferred:** the partition picker as a segmented control (T11 reworks the partition log), the broker node listing the partitions it leads (M4, when there is more than one broker), and the speed button cycling 10× → 0.25× (it is a cycle button by design; a menu is a UI-spec question). | Do all now | Keeps T0 small; each deferred item has a natural home. |
| T0-8 | `queue.test.ts` "100k push/pop within 2 s" timed out once under coverage: the 100k `expect()` calls inside the loop were the slow part. It now counts out-of-order pops and asserts once. | Raise the timeout | The test still checks what it says. |

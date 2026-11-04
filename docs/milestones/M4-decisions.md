# M4 decisions log

M4 starts with T0: the fixes from the M3 review (findings F-1 to F-9). As in M3, the rule is to **behave like real Kafka 4.x as closely as practical** and to write each decision down here, with the alternatives and the reason.

## Review these first

| # | Decision | Why it matters |
|---|---|---|
| [D1](#t0--m3-review-fixes) | **The onJoinPrepare auto-commit is sent once per rebalance.** Once it is answered, or after `rebalance.timeout.ms`, JoinGroup goes out without another commit. | Fixes F-1: under load, cooperative members never rejoined and were removed. |
| [D2](#t0--m3-review-fixes) | **The JoinGroup answer sets the generation; a successful SyncGroup makes the member stable at once.** `poll()` still applies the assignment. | Fixes F-2: busy cooperative members heartbeated with the old generation and lost their partitions. |
| [D3](#t0--m3-review-fixes) | **The commit invariant no longer requires the committer to own the partition in the current generation.** A member may commit a partition it still holds from the previous generation, and only one member commits a partition per generation. | This is an invariant change. The old one checked a rule Kafka doesn't have. |
| [L1](#liveness-bounds-found-by-the-f-3-property) | **A cooperative member with auto-commit needs two batches within the rebalance timeout**, plus one heartbeat interval. | This is real Kafka behaviour, not a bug. The liveness property (F-3, PR 2) uses this bound. |

## Process

| # | Decision | Why |
|---|---|---|
| P1 | T0 is split into four PRs: (1) F-1 + F-2 with regression tests, (2) F-3 + F-4 property tests, (3) F-5 to F-7, starting with a small engine commit for the snapshot fields, (4) F-8 + F-9. | Rhea's plan of 2026-11-04. |
| P2 | **PR 1 waits for Rhea's review before it merges.** PRs 2 to 4 merge once CI is green, as in M3. | Both M3 bugs got through green CI because no test exercised liveness. |

## T0 – M3 review fixes

| # | Decision | Alternatives | Why |
|---|---|---|---|
| D1 | `ClientGroupState.prepareCommitDone` is set when the onJoinPrepare commit is answered (success or error) or when its wait times out. The next `joinGroup()` skips the commit and sends JoinGroup. The flag is cleared when JoinGroup is sent, on `resetGeneration`, and on close (`resetClientGroup`). The wait is limited by a sim-clock task, `ConsumerJoinPrepareTimeout`, after `max.poll.interval.ms` (the rebalance timeout). When it fires, the member joins and ignores the late answer. | Commit again at every `poll()` (the M3 behaviour, a livelock). Drop the "already committed" skip first (F-8): that would make eager members livelock too. | `ConsumerCoordinator#onJoinPrepare` sends the commit only while `autoCommitOffsetRequestFuture` is null, and keeps the future until it is done. `joinPrepareTimer` (rebalance timeout) ends the wait: "Asynchronous auto-commit of offsets failed: joinPrepare timeout. Will continue to join group". The timer came in with KAFKA-13310, a ticket number still to confirm against the source. |
| D2 | `onJoinResponse` sets `memberId` and `generation` from the answer. `onSyncResponse` sets `state = 'stable'` and `synced = true`, and stores the assignment, which `poll()` applies in `onJoinComplete` as before. A failed JoinGroup or SyncGroup without a reset falls back to `stable` if the member has `synced` since its last reset, and to `unjoined` otherwise. The old rule (`generation < 0`) would now mark a first-time joiner stable with nothing owned, so the snapshot showed it as "Idle". | Keep the generation update in `onJoinComplete` (the M3 bug). | `AbstractCoordinator.JoinGroupResponseHandler` stores the new `Generation`, and `SyncGroupResponseHandler` sets `state = MemberState.STABLE`, both on the network thread. So heartbeats in CompletingRebalance carry the new generation, and commits after SyncGroup succeed before the next `poll()`. |
| D3 | The invariant "an accepted commit comes from a member … that owns the partition" now also accepts a member that owned the partition in the **previous** completed generation and has not yet emitted `PartitionsRevoked` (or `PartitionsLost`) for it, so it is tied to the event log. New check: within one generation, only one member commits a given partition. | Drop the ownership check (too weak). Keep it (it rejects real Kafka behaviour once D2 is in). | `ClassicGroup#validateOffsetCommit` checks only the member id, the generation and the group state, never ownership. After D2, a cooperative member is STABLE in generation N+1 but revokes the partitions it gives up only at its next `poll()`. Until then it may commit them with N+1, and Kafka accepts that. No one else can own those partitions in N+1, because they move only in the follow-up rebalance (KIP-429). The new check therefore catches what the old one was protecting against. The F-2 regression test pins one such commit (c1, P2, generation 2). |

## Open questions

| # | Question |
|---|---|
| O1 | On any JoinGroup or SyncGroup failure, Kafka's `initiateJoinGroup` failure listener sets `state = UNJOINED`. `needsJoinPrepare` stays false, so the retry sends JoinGroup without running onJoinPrepare again. We keep the M3 fallback: a synced member stays `stable` and runs onJoinPrepare again, which means one more auto-commit. D2 changed only the first-join case, as Rhea asked. Modelling UNJOINED exactly would change commit and heartbeat behaviour during retries, so it is left for a later task, possibly with the coordinator failover work (`NOT_COORDINATOR`). |

## Liveness bounds found by the F-3 property

The liveness property (review F-3, lands in PR 2) was run locally against PR 1, 2000 cases. It found no new livelock. It did find two cases where the bound in the review ("batch time under `max.poll.interval.ms`") is too loose for real Kafka. Both traces match the client's behaviour, so they become the property's precondition, not engine changes.

| # | Finding |
|---|---|
| L1 | **Cooperative + auto-commit needs two batches.** At the first `poll()` after it learns of the rebalance, the member sends the onJoinPrepare commit, and that same `poll()` returns the next batch (K12). JoinGroup goes out only at the poll after that batch. Counterexample: 369 records × 59 ms = 21.8 s per batch, rebalance timeout 30.5 s. c1 polled at 69.4 s (sent the commit, got a batch) and would have joined at about 91.2 s. The join phase ended at 79.6 s, so it was removed (`rebalance-timeout`). Eager members return nothing while the commit is in flight, so one batch is enough for them. |
| L2 | **Add one heartbeat interval.** A member learns of a rebalance only from a Heartbeat answer (`REBALANCE_IN_PROGRESS`), so it may start a new batch just before it hears. Counterexample: range, 3.9 s batches, `heartbeat.interval.ms` 2.5 s, rebalance timeout 5.3 s. c3 was removed at 26.37 s and would have joined at 26.42 s. |

The precondition is: `batches × batch time + heartbeat.interval.ms + 1 s < max.poll.interval.ms`, where `batches` is 2 for a cooperative member with auto-commit and 1 otherwise. L1 is a good Break-it candidate for a later level: "cooperative isn't free when batches are long."

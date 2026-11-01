# ADR 0004: Consumer groups use the classic group protocol

- **Status:** Accepted
- **Date:** 2026-11-01
- **Milestone:** M3 (see `docs/milestones/M3-consumer-groups.md` Q1 and `docs/milestones/M3-decisions.md`)

## Context

Kafka 4.x has two consumer group protocols:

- **Classic** (`group.protocol=classic`, still the default in 4.x). The group coordinator on a broker runs a state machine (`Empty → PreparingRebalance → CompletingRebalance → Stable`, plus `Dead`). Members join with JoinGroup; the coordinator picks a **leader member**, and the leader runs the assignor *on the client* and sends the assignment back in SyncGroup. Members heartbeat from a background thread. Every membership change starts a new **generation**. With an eager assignor all members give up all partitions while the group rebalances ("stop the world"); with `cooperative-sticky` (KIP-429) only the partitions that move are given up, over two rebalances.
- **Consumer** (`group.protocol=consumer`, KIP-848, generally available since 4.0). The broker computes assignments itself and changes them member by member through heartbeats, with no group-wide barrier.

The playground needs one of them for M3. The engine already models the classic `KafkaConsumer` (M2: the poll loop, long-poll fetch, auto-commit inside `poll()`).

## Decision

Model the **classic protocol** first:

- Broker side (`protocols/group-coordinator.ts`): coordinator lookup (`abs(groupId.hashCode()) % 50` → leader of that `__consumer_offsets` partition), the group state machine, generations, leader choice, protocol vote, the initial rebalance delay, rebalance and pending-sync timeouts, session timeouts, LeaveGroup, and OffsetCommit validation, ported from `GroupMetadataManager` / `ClassicGroup`.
- Client side (`protocols/consumer-coordinator.ts`): joining from inside `poll()`, `onJoinPrepare` (commit, revoke or lose partitions), the leader running the assignor, `onJoinComplete`, the heartbeat thread, and the poll timeout, ported from `ConsumerCoordinator` / `AbstractCoordinator`.
- Assignors (`protocols/assignors/`): range, round-robin, sticky and cooperative-sticky as pure functions.

KIP-848 is a simplifications row and a later milestone.

## Consequences

- The lessons that matter most for people running Kafka today work as they do in production: stop-the-world rebalances, a slow consumer delaying the whole group, zombies after `max.poll.interval.ms`, `ILLEGAL_GENERATION` and `CommitFailedException`, and the eager vs cooperative difference.
- The default `partition.assignment.strategy` `[range, cooperative-sticky]` gives eager range rebalances, as in a real 4.x client (decision K2).
- The fidelity harness (M5) can compare against a real 4.x broker with default client settings.
- KIP-848 needs a second coordinator implementation later. Most of the client poll loop and the commit handling can be shared.
- The assignment is computed on the client (the leader), so a stale leader metadata cache leaves new partitions unassigned until the leader refreshes; the invariants account for that.

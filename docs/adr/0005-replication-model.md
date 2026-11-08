# ADR 0005: Replication, ISR and elections

- **Status:** Accepted
- **Date:** 2026-11-08
- **Milestone:** M4 (see `docs/milestones/M4-replication.md` Q2–Q6 and `docs/milestones/M4-decisions.md` K1–K14)
- **Target version:** Apache Kafka **4.4.0** (KRaft only), image `apache/kafka:4.4.0` for the M5 fidelity harness

## Context

Until M3 every partition had one replica and the HW moved with every append. M4 adds replication, broker failures and elections. Real Kafka splits this between the brokers (replica fetchers, the leader's ISR and HW logic in `Partition`) and the KRaft controller quorum (broker liveness, ISR changes through AlterPartition, leader elections in `ReplicationControlManager` / `PartitionChangeBuilder`). We need a model that shows the lessons (acks, min ISR, failover time, truncation) faithfully, and stays deterministic and small.

## Decision

- **Controller:** one abstract KRaft controller quorum, always up. It is not a broker; the canvas shows it as a chip on the cluster frame. Brokers heartbeat every `broker.heartbeat.interval.ms` (2 s); the controller fences a broker silent for `broker.session.timeout.ms` (9 s). Every request to it (heartbeat, AlterPartition, controlled shutdown) costs one network delay each way. Controller failover and combined mode are not modelled.
- **Followers long-poll the leader:** one fetcher per (follower, leader) pair, `replica.fetch.wait.max.ms` 500 and `replica.fetch.min.bytes` 1, starting from the follower's LEO with its last fetched epoch. Responses carry the leader's HW and, when logs diverge, a diverging epoch (KIP-320) the follower truncates to.
- **Each replica has its own log** (sharing frozen record objects) and a leader-epoch cache, so truncation and divergence are real.
- **ISR via AlterPartition:** the leader shrinks or expands the ISR through the controller, which validates the partition epoch and bumps it; the leader uses the larger "maximal ISR" for the HW until the change is confirmed (KIP-497).
- **HW rule:** HW = min LEO over the maximal ISR (and caught-up replicas eligible to join); it never decreases and does not move while the ISR is below `min.insync.replicas` (strict min ISR, KIP-966).
- **Clean elections by default, with Eligible Leader Replicas (KIP-966) on**, as in a fresh 4.4 cluster: new leaders come from the ISR, then the ELR, then the last known leader; out-of-sync replicas lead only with `unclean.leader.election.enable=true`.
- **Brokers:** crash (`KillBroker`), controlled shutdown (`StopBroker`) and restart with the log on disk (`RestartBroker`). Disks are never lost.

## Consequences

- acks=all with minISR ≥ 2 never loses acknowledged records under a single failure; acks=1 does, visibly, when the old leader returns and truncates.
- With minISR 1 and clean elections, losing the last in-sync replica takes the partition **offline** rather than losing data: the documented FR-2.6 case needs unclean election. The docs were corrected (Q6).
- Failover after a crash takes about `broker.session.timeout.ms`; a controlled shutdown moves leaders in a few network delays. Both are visible lessons.
- Each replica log costs memory proportional to records × RF; fine for game-scale scenes, a note for the v0.2 load work.
- The fidelity harness (M5) can compare failover timings against a real 4.4 cluster with the same game-tuned timers.

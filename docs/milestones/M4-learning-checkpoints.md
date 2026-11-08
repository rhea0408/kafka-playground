# M4 learning checkpoints

Short answers to the "Learning checkpoint" questions in `docs/milestones/M4-replication.md`, written for someone new to Kafka. Each one links the Kafka behaviour to what the engine does.

## T2 · Replica placement

**Q: Why does Kafka spread the *leaders* of a topic across brokers, not just the replicas?**

Every replica stores the data, but only the **leader** does the work clients notice: all produce requests for a partition go to its leader, all consumer fetches (by default) are served by it, and the followers fetch from it too. A follower only writes what it copies. If one broker led every partition of a busy topic, that broker would take all the network traffic, request handling and disk writes for the topic, while the others sat nearly idle holding copies. Spreading the leaders spreads that load. It also limits the damage of one crash: if a broker that leads a third of the partitions dies, only that third pauses until new leaders are elected (about `broker.session.timeout.ms`, 9 s); if it led everything, the whole topic would stop. That is why `StripedReplicaPlacer` starts each partition one broker further along: partition 0's preferred leader is broker *k*, partition 1's is *k + 1*, and so on (`model/placement.ts`). The first replica in the list is the **preferred leader**. After a failover, leadership drifts to whoever was in sync, and `auto.leader.rebalance.enable` periodically moves it back to the preferred replica (T5), restoring the spread.

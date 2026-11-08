# M4 learning checkpoints

Short answers to the "Learning checkpoint" questions in `docs/milestones/M4-replication.md`, written for someone new to Kafka. Each one links the Kafka behaviour to what the engine does.

## T2 · Replica placement

**Q: Why does Kafka spread the *leaders* of a topic across brokers, not just the replicas?**

Every replica stores the data, but only the **leader** does the work clients notice: all produce requests for a partition go to its leader, all consumer fetches (by default) are served by it, and the followers fetch from it too. A follower only writes what it copies. If one broker led every partition of a busy topic, that broker would take all the network traffic, request handling and disk writes for the topic, while the others sat nearly idle holding copies. Spreading the leaders spreads that load. It also limits the damage of one crash: if a broker that leads a third of the partitions dies, only that third pauses until new leaders are elected (about `broker.session.timeout.ms`, 9 s); if it led everything, the whole topic would stop. That is why `StripedReplicaPlacer` starts each partition one broker further along: partition 0's preferred leader is broker *k*, partition 1's is *k + 1*, and so on (`model/placement.ts`). The first replica in the list is the **preferred leader**. After a failover, leadership drifts to whoever was in sync, and `auto.leader.rebalance.enable` periodically moves it back to the preferred replica (T5), restoring the spread.

## T3 · The high watermark

**Q: A consumer never reads above the HW. What could go wrong if it read up to the leader's LEO?**

Records between the HW and the LEO are on the leader but not yet on every in-sync replica. If the leader crashed right then, the new leader (an in-sync follower) might not have them, so those records would vanish: the old leader truncates them when it comes back (T6). A consumer that had already read them would have acted on records that, as far as the cluster is concerned, never existed. It might have charged a card, sent an email, or updated a database. Worse, the new leader then gives the same offsets to *different* records from new writes, so the consumer's position would point into a different history, and it would silently skip or mis-number records. Reading only below the HW guarantees that everything a consumer sees survives any failure that the ISR can absorb. That is the same promise acks=all gives producers, and both lean on the same mark: an acks=all producer is answered exactly when the HW passes its record (`replication.ts`, DelayedProduce). The cost is a little latency, about two network delays while followers fetch. With the ISR below `min.insync.replicas`, Kafka 4.x freezes the HW altogether ("strict min ISR"), so consumers wait rather than read data that is on too few replicas.

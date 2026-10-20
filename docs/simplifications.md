# Simplifications

Where the simulation differs from real Kafka. Add an entry for every simplification (real behaviour vs ours, and why).

| Area | Real Kafka | Kafka Playground | Why |
|---|---|---|---|
| Partition placement | In KRaft the controller's `StripedReplicaPlacer` stripes replicas across brokers and racks from a randomised start | Round-robin over brokers sorted by id, from a seeded random start; racks ignored | Same spreading idea, predictable for lessons; racks arrive with multi-AZ (v0.5) |
| Replication (M1 only) | Each partition has `replication.factor` replicas; HW = min LEO over the ISR | One replica per partition; topics with RF > 1 are rejected; HW = LEO after every append | Replication, ISR and elections arrive in M4 |
| Broker removal (M1 only) | Partitions can be reassigned to other brokers before a broker is decommissioned | Removing a broker that leads partitions is rejected | Reassignment arrives with multi-broker work (M4) |
| Sticky partitioner (null key) | Uniform sticky partitioner (KIP-480, KIP-794): switch after `batch.size` bytes, or up to 2 × `batch.size` while batches are not full; with adaptive partitioning the next partition is weighted by broker queue sizes | Switch as soon as `batch.size` bytes were produced to the partition; next partition uniform over all partitions (may repeat), from the seeded RNG | No batching or broker queues are modelled in M1; the "stay, then move" pattern is what matters for lessons |
| Key serialisation | `StringSerializer` encodes with a configurable charset, UTF-8 by default | Always UTF-8; an unpaired surrogate becomes `?` (0x3f), as Java's encoder does | Non-default key encodings are not used in lessons |

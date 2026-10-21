# M2 learning checkpoints

Short answers to the "Learning checkpoint" questions in `docs/milestones/M2-web-shell.md`. They are written for someone new to Kafka; each one links the Kafka behaviour to what the engine does.

## T1 · Message timing

**Q: In real Kafka, what happens between `send()` returning and the record being readable by a consumer? Which of those steps does the engine model, and roughly how long does each one take?**

`send()` only puts the record into a per-partition batch in the producer's memory and returns at once. The batch waits until it is full or `linger.ms` has passed (5 ms by default since Kafka 4.0, KIP-1030), then goes over the network to the partition leader (well under 1 ms in one data centre). The leader appends it to its log in the page cache (microseconds), and the followers copy it; once every in-sync replica has it, the high watermark moves past it (about one or two network round trips). Only then can consumers read it, whatever `acks` the producer used: `acks` decides when the *producer* hears back, not when consumers can see the record. A consumer's fetch request is usually already waiting at the leader (long poll), so it is answered as soon as the high watermark moves, and `poll()` hands the record to the application; end to end that is a few milliseconds. The engine models the partitioner, a fixed 1 ms network hop each way, the leader append and the produce response, and from M2-T2 the long-poll fetch. It does not model linger or batching (each record is sent at once), disk time, or replication (one replica, so the high watermark moves on every append until M4). The dots on the canvas are deliberately much slower than all this; the partition log and counters show the real timing (ADR 0008).

# ADR 0008: Message timing: realistic engine, decorative dots

- **Status:** Accepted
- **Date:** 2026-10-20
- **Milestone:** M2-T1 (see `docs/milestones/M2-web-shell.md`, decisions Q3 and T1)

## Context

In a real cluster inside one data centre, a record goes from `send()` to a waiting consumer in a few milliseconds. A dot that crosses a 300 px edge at 120 px/s takes 2.5 s. The canvas can't be both true to time and readable, so we have to choose which one the engine follows and which one the dots follow.

### The real path from `send()` to a consumer (Kafka 4.x, one data centre)

| # | Step | What happens | Typical time |
|---|---|---|---|
| 1 | `send()` | The producer serialises key and value, picks a partition (murmur2 of the key, or the sticky partitioner for null keys; KIP-480, KIP-794) and appends the record to a per-partition batch in the **record accumulator**. `send()` returns a `Future` at once; it does not wait for the broker. The very first send to a topic can block for metadata (up to `max.block.ms`, 60 s). | microseconds |
| 2 | Linger / batching | The sender thread sends a batch when it reaches `batch.size` (16 KiB) or when `linger.ms` has passed. **Kafka 4.0 changed the `linger.ms` default from 0 to 5 ms** (KIP-1030; 4.0 upgrade notes: larger batches usually give similar or lower latency despite the wait). librdkafka, which the Go client in `lab/` uses, also defaults to 5 ms. | 0–5 ms (up to `linger.ms`) |
| 3 | Network to leader | A ProduceRequest goes to the partition leader. Up to `max.in.flight.requests.per.connection` (5) requests can be outstanding. | ~0.1–0.5 ms one way |
| 4 | Leader append | The leader appends the batch to the active log segment. This is a write to the OS page cache; Kafka does not fsync per message by default and relies on replication for durability (Design, "Persistence"). The log end offset (LEO) moves. | well under 1 ms |
| 5 | Replication and HW advance | Followers have fetch requests parked at the leader (`replica.fetch.min.bytes` 1, `replica.fetch.wait.max.ms` 500 ms), so the append wakes them at once. Each follower appends and its next fetch tells the leader its new LEO. The high watermark (HW) moves to the lowest LEO in the ISR (Design, "Replication"). | ~1–2 network round trips, about 0.5–2 ms |
| 6 | Produce response | With `acks=all` (the default since Kafka 3.0, KIP-679) the leader holds the response until the HW passes the batch, and then it answers. With `acks=1` it answers after step 4; with `acks=0` there is no response. | + one network trip |
| 7 | Consumer fetch | The consumer long-polls: its FetchRequest waits at the leader until `fetch.min.bytes` (1) are available or `fetch.max.wait.ms` (500 ms) passes. A consumer only reads records **below the HW**, so the parked fetch completes when the HW moves in step 5. **`acks` only changes when the producer hears back; it does not change when consumers can read.** | + one network trip after the HW moves |
| 8 | `poll()` | The fetched records sit in the consumer's buffer until the application calls `poll()`, which returns at most `max.poll.records` (500). If the application is busy processing, this is where records wait. | 0 to the application's processing time |

**End to end:** about 1–3 ms with `linger.ms=0`; up to about 5 ms more with the 4.x default of 5 ms. Disk, GC pauses, cross-AZ links or a busy broker add to that, but "a few milliseconds" is the honest summary.

Sources: Kafka documentation (Producer configs `linger.ms`, `batch.size`, `acks`, `max.in.flight.requests.per.connection`, `max.block.ms`; Consumer configs `fetch.min.bytes`, `fetch.max.wait.ms`, `max.poll.records`; Broker configs `replica.fetch.min.bytes`, `replica.fetch.wait.max.ms`; Design §4.2 "Persistence", §4.7 "Replication"; "Upgrading to 4.0"); KIP-1030 (new defaults in 4.0, including `linger.ms`); KIP-679 (`acks=all` and idempotence on by default); KIP-480 and KIP-794 (sticky partitioner).

### What the engine models (after M1, and after M2-T2)

| Step | Engine |
|---|---|
| 1 `send()` + partitioner | Modelled: murmur2 for keys, sticky for null keys. `RecordProduced` is emitted at send time. |
| 2 Linger / batching | **Not modelled.** Each record is its own request, sent at once (see "Producer batching" in `docs/simplifications.md`). |
| 3 Network | Fixed `networkDelayMs` = 1 ms each way, no queueing ("Network" in `simplifications.md`). 1 ms is the smallest step of the integer sim clock. |
| 4 Leader append | Modelled: `LeaderAppend` task, `RecordAppended` event, no disk time. |
| 5 Replication | Not modelled in M1/M2 (RF = 1, HW = LEO after each append). Arrives in M4. |
| 6 Produce response | Modelled: `ProduceResponse` after another `networkDelayMs`; `acks=0` acks at send time. |
| 7 Consumer fetch | M1: `poll()` reads the log directly; an empty poll waits `pollIntervalMs` (game setting). **M2-T2 replaces this with the long poll** (`fetch.min.bytes`, `fetch.max.wait.ms`, two network trips). |
| 8 `poll()` | Modelled: `max.poll.records`, `processingTimeMs` per record; `RecordConsumed` when records reach the application. |

So with T2 in place, a record sent at sim time *t* is appended at *t* + 1 ms, acked at *t* + 2 ms, and reaches a consumer whose fetch was waiting at about *t* + 2 ms. That is the real order of magnitude, minus the linger wait.

## Decision

1. **The engine keeps real-world timing.** `networkDelayMs` stays at 1 ms. We don't slow the engine down to match the animation. The partition log, offsets, HW, lag, counters and the event list are the source of truth for order and timing.
2. **Message dots are decorative.** They show *that* a record moved and *which way*, not *when*. They move at 120 px/s × sim speed (animations.md A1/A2), so they are always far slower than the engine.
3. Rules so dots never show a wrong order:
   1. A record's A2 dot (topic → consumer) starts only after its A1 dot (producer → topic) with the same trace id has arrived. The overlay queues A2 by trace id.
   2. Log cells (A4), offsets, lag (A5) and counters update straight from snapshots and events and never wait for dots.
   3. When dots fall behind the engine (high rate, long edges), new dots are **skipped**, not delayed. At most 500 dots are on screen (NFR-2.5, M2-T9).
   4. The overlay has a legend/tooltip: **"Dots are slowed down so you can see them. Real delivery takes a few milliseconds."** (NFR-3.3)

## Consequences

- The player sees a record in the partition log and in the consumer's counters well before its dot arrives. The legend explains this, and it is the truthful view.
- Latency numbers, lag charts (v0.2) and the fidelity harness (M5) can compare the engine with real Kafka directly, because nothing in the engine is stretched for the animation.
- At higher rates many records get no dot. Dots stop being a per-record trace and become a sample of the flow, which is what flow mode (A11, v0.2) formalises.
- The overlay needs a small per-trace-id queue (A1 arrived → A2 may start) and needs trace ids that are unique across producer restarts (M2-T2).
- Because `linger.ms` isn't modelled, the engine is up to 5 ms faster than a default Kafka 4.x producer. That is invisible at dot speed but will show in the fidelity harness; the harness should set `linger.ms=0` on the real producer or allow for it.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Slow the engine to dot speed** (e.g. `networkDelayMs` of 1–2 s) | Teaches wrong numbers: Kafka would look like it takes seconds per hop. It also breaks every timeout-based lesson (`session.timeout.ms`, `fetch.max.wait.ms`, `delivery.timeout.ms`), which would all need rescaling, and the fidelity harness could no longer compare timings. |
| **Make dot speed match engine time** | A 1 ms hop can't be seen; at 1× nothing would visibly move. |
| **Hold engine state (log cells, lag) until the dot arrives** | Makes the UI lie about when a record is readable, and couples rendering to engine state. Breaks one-way data flow (CLAUDE.md engine rule 3) in spirit. |
| **Delay dots instead of skipping them when behind** | The backlog grows without bound at high rates, and dots end up showing records from long ago next to the current log. |
| **No ordering rule between A1 and A2** | With 1 ms engine timing, the A2 dot would often leave the topic before the A1 dot reached it, which shows a record being read before it was written. |

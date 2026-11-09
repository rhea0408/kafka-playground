# Animation spec

Every orange "ANIM" tag in the wireframes is listed here with its trigger and timing. The wireframe HTML files also contain working CSS `@keyframes` for most of them, so you can open a file to see the motion. Durations are starting values; tune them during polish.

**Rules for all animations**
- Animations are driven by **engine events / snapshots**, never by timers in UI components. The UI only renders what the engine says happened.
- Sim speed (0.25×–10×) scales message-flow animations; UI chrome animations (panels, toasts) do not scale.
- `prefers-reduced-motion` (and the in-app Reduced motion setting): replace movement with instant state changes or a 150 ms fade; keep pulses off (A6 pulse, A7 flip and A8 shake are off; the hatch, badges and dashed borders stay).
- Message dots are drawn on a **Canvas2D overlay**, not as DOM nodes (performance: up to ~500 dots).

## Canvas / simulation

| # | Element | Trigger (engine event) | Animation | Timing | Screens |
|---|---|---|---|---|---|
| A1 | Message dot on producer → topic edge (decorative, see below) | `RecordProduced` (it already carries the partition the partitioner chose) | 16 px dot with key letter (`∅` for a null key) travels from the producer's handle to the target **partition row** of the topic, so its last stretch leaves the edge line and curves into the row | path length ÷ (120 px/s × sim speed); px are flow px, see below | 02, 05 |
| A2 | Message dot on topic → consumer group edge (decorative, see below) | `RecordConsumed` (when `poll()` returns the record) | Same as A1, from the partition row to the **row of the member that consumed it** in its group node (M3-T8); starts only after the A1 dot with the same trace id has arrived | same | 02, 05 |
| A3 | Replication dot between brokers (**not built in M4**: there is no cluster frame or broker-to-broker edge to draw it on, and no per-record event; M4 decisions T11-6. Replication shows as replica chips catching up and in the partition log's replica table) | `ReplicaFetched` (one per follower fetch, replaces the planned `RecordReplicated`) | 10 px grey dot along dashed edge | ~1.6 s at 1× | 02, 05 |
| A4 | New offset cell in partition row | `RecordAppended` | Cell slides in from left + fades in; never waits for dots | 250 ms ease-out | 02 |
| A5 | Lag badge | Lag derived from each snapshot (sum of HW − committed per partition); there is no `LagChanged` event (M2 Q2, architecture §4.5) | Number counts to new value; badge widens; turns danger color above threshold; never waits for dots | 300 ms | 02, 05 |
| A6 | Failed broker (M4-T11) | `BrokerStopped` (followed through the broker snapshot's `state`) | Hatched fill while crashed or stopped; danger pulse ring only while crashed (a clean stop doesn't pulse). The state chip (icon + word: ● Running, ⊘ Fenced, ◐ Shutting down, ■ Stopped, ✕ Crashed) carries the meaning without color | pulse 1.4 s loop until restart | 03, 03b |
| A7 | Leader badge (M4-T11) | `LeaderElected` (the partition snapshot's `leader` changes) | The `L b2` badge on the topic's partition row flips (rotateX) to the new broker id; with no leader it shows "⚠ offline" in danger colors | 400 ms (UI time) | 03 |
| A8 | Lost records (M4-T11) | `RecordsTruncated` with `acked > 0` (the partition snapshot's `lostAcked` grows) | The "⚠ N lost" chip on the partition row (dashed danger border) shakes twice; the partition log lists the deleted records struck through, "acknowledged lost" when they had been acked. The log cells themselves don't shake: the leader's log never had them | 600 ms (2 × 300 ms) | 03, 03b |
| A9 | Follower leaving ISR (M4-T11) | `IsrShrunk` / `IsrExpanded` | On the **replica chip** of that broker in the partition row (not the broker node, since a broker is in some ISRs and not others): border fades solid → dashed, danger color, "·out" suffix; back to solid on expand | 300 ms | 03b |
| A10 | Auto-pause | failure event defined by level | Sim pauses; banner slides down from top of canvas; uninvolved nodes dim to 70% | 250 ms | 03 |
| A11 | Flow mode (high load) | rate above threshold (v0.2) | Dots cross-fade into aggregated flow lines | 400 ms | — |
| A12 | Rebalance (M3-T8) | `RebalanceStarted` → `PartitionsRevoked` / `PartitionsLost` → `PartitionsAssigned` → `RebalanceCompleted` (followed through snapshots, which change with these events) | The group header chip changes to "↻ Rebalancing…" until the group is Stable again ("✓ Stable", or "∅ Empty"). Partition chips on a member row fade out when revoked or lost: an eager rebalance fades every chip of every member, a cooperative one only the chips that move. Chips fade in on the row of the member they are assigned to. An idle member shows the flat "◇ Idle: no partition" chip. Never waits for dots | chips 200 ms (UI time, not scaled by sim speed); header chip instant | 05 |

### Message dots are decorative (A1, A2)

Decided in M2-T1, see `docs/adr/0008-message-timing.md`. The engine runs at real Kafka timing (a record reaches a waiting consumer a few ms after `send()`); the dots are a visual aid, not a timing claim. The partition log, offsets, lag and counters show the true order and timing.

- **Speed:** dots move at 120 px/s × sim speed, so they are always much slower than the engine.
- **Units:** dot size (16 px) and speed (120 px/s) are in React Flow **flow coordinates**: dots are drawn with the canvas's pan / zoom transform, so they scale with zoom and take the same time to cross an edge at any zoom. Paths are React Flow's bezier edges, with endpoints re-read every frame, so dots stay on their path while nodes are dragged (M2-T9).
- **Order by trace id:** a record's A2 dot (topic → consumer) starts only after the A1 dot (producer → topic) with the same trace id has arrived. The overlay queues A2 dots by trace id. If the record has no A1 dot (it was skipped), the A2 dot is skipped too.
- **Never block state:** log cells (A4), offsets, HW, lag (A5), counters and the event list update straight from snapshots and events and never wait for a dot.
- **Skip, don't delay:** if dots fall behind the engine (high rate, long edges), new dots are skipped rather than queued for later.
- **Cap:** at most 500 dots on screen (NFR-2.5). Beyond that, new dots are skipped and a small "too many to animate" chip is shown.
- **Legend:** a legend or tooltip on the overlay says: "Dots are slowed down so you can see them. Real delivery takes a few milliseconds." (NFR-3.3)

## Panels and chrome

| # | Element | Trigger | Animation | Timing | Screens |
|---|---|---|---|---|---|
| B1 | Objectives counter | objective progress | Counter ticks; progress bar fills | 200 ms per step | 02 |
| B2 | Failed objective | replay fails | Text shakes once | 400 ms | 03b |
| B3 | "Not quite" / "What went wrong" panel | failure / failed fix | Slides in from right + fade | 250 ms ease-out | 03, 03b |
| B4 | Star tags in "Stars still possible" | star lost | Tag greys out | 300 ms | 03b |
| B5 | Event timeline playhead | sim clock | Playhead moves with sim time; new events drop in as markers | continuous | 02 |
| B6 | Timeline scrubber | user drag | Dragging rewinds; canvas replays state at that time | live | 03 |
| B7 | Event list | new event | Newest row slides in at bottom (auto-scroll when Follow is on) | 200 ms | 05 |
| B8 | Lag chart | snapshot tick | Chart scrolls left continuously | 30 fps | 05 |
| B9 | Partition log rows | `RecordAppended` | New row slides up at bottom; HW / committed markers move down | 200 ms | 05c |
| B10 | Code panel changed line | config change | Line background glows primary-tint, fades | 1.5 s | 05 |
| B11 | Minimap | content off-screen ↔ fully visible | Fade in / out | 200 ms | 05 |
| B12 | Stat tiles | snapshot tick | Numbers tick up | 200 ms | 05b |

## Campaign and debrief

| # | Element | Trigger | Animation | Timing | Screens |
|---|---|---|---|---|---|
| C1 | Current level node | on map load | Primary pulse ring | 1.6 s loop | 01 |
| C2 | Continue card progress bar | on load | Fills to current progress | 600 ms | 01 |
| C3 | Debrief stars | debrief opens | Earned stars pop in one by one (scale 0 → 1.25 → 1) | 0.3 s apart | 04, 04b |
| C4 | Missed star | after earned stars | Fades in as grey | 300 ms | 04b |
| C5 | Unlocked tags | after stars | Slide up + fade in | 200 ms, staggered | 04 |

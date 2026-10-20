# Animation spec

Every orange "ANIM" tag in the wireframes is listed here with its trigger and timing. The wireframe HTML files also contain working CSS `@keyframes` for most of them, so you can open a file to see the motion. Durations are starting values; tune them during polish.

**Rules for all animations**
- Animations are driven by **engine events / snapshots**, never by timers in UI components. The UI only renders what the engine says happened.
- Sim speed (0.25×–10×) scales message-flow animations; UI chrome animations (panels, toasts) do not scale.
- `prefers-reduced-motion` (and the in-app Reduced motion setting): replace movement with instant state changes or a 150 ms fade; keep pulses off.
- Message dots are drawn on a **Canvas2D overlay**, not as DOM nodes (performance: up to ~500 dots).

## Canvas / simulation

| # | Element | Trigger (engine event) | Animation | Timing | Screens |
|---|---|---|---|---|---|
| A1 | Message dot on producer → topic edge | `RecordProduced` | 16 px dot with key letter travels along the edge | edge length ÷ (120 px/s × sim speed) | 02, 05 |
| A2 | Message dot on topic → consumer edge | `RecordConsumed` (fetch) | Same as A1, from partition row to consumer row | same | 02, 05 |
| A3 | Replication dot between brokers | `RecordReplicated` | 10 px grey dot along dashed edge | ~1.6 s at 1× | 02, 05 |
| A4 | New offset cell in partition row | `RecordAppended` | Cell slides in from left + fades in | 250 ms ease-out | 02 |
| A5 | Lag badge | `LagChanged` | Number counts to new value; badge widens; turns danger color above threshold | 300 ms | 02, 05 |
| A6 | Failed broker | `BrokerDown` | Hatched fill + danger pulse ring | pulse 1.4 s loop until restart | 03, 03b |
| A7 | Leader badge | `LeaderElected` | Flip (rotateX) from old to new broker id | 400 ms | 03 |
| A8 | Lost records | `RecordsTruncated` | Cells shake (2×), then strike through with dashed danger border | 600 ms | 03, 03b |
| A9 | Follower leaving ISR | `IsrShrunk` | Broker border fades solid → dashed | 300 ms | 03b |
| A10 | Auto-pause | failure event defined by level | Sim pauses; banner slides down from top of canvas; uninvolved nodes dim to 70% | 250 ms | 03 |
| A11 | Flow mode (high load) | rate above threshold (v0.2) | Dots cross-fade into aggregated flow lines | 400 ms | — |

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

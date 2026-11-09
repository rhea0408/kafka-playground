# UI Spec: layout, sizes, colors

Source of truth for layout is `docs/ui/wireframes/*.html` (open `index.html` in a browser). Purple tags in the wireframes give sizes and orange "ANIM" tags mark animations (see `animations.md`). This file restates the important numbers as text.

## Reference screen
- Design at **1440 × 900**; minimum supported **1366 × 768**. Desktop only for MVP.
- Side panels keep fixed widths; the canvas flexes to fill the remaining space.

## Level screen layout (Campaign) — screens 02, 03, 03b

| Region | Size (px) | Notes |
|---|---|---|
| Top bar | full width × 56 | Back link · level title · phase stepper (Learn / Break it / Fix / Real Lab opt.) · sim controls |
| Sim controls | buttons 40 × 40 (speed 56 × 40), gap 8 | Absolutely positioned: vertically centered, **8 px from right edge**. Same spot on every screen |
| Objectives strip | full width × 44 | Current objectives, progress counter, attempt count |
| Palette (left) | 240 wide | Items 208 × 56 (failure tools 208 × 44). Campaign shows unlocked items only; locked ones dashed |
| Canvas | remaining (≈ 840 × 600 at reference) | Dotted grid background |
| Right panel | 360 wide | Tabs: Properties · Code · Partition log. During a failure the first tab becomes "What went wrong" (Break it) or "Not quite" (failed fix) |
| Bottom panel | canvas width × 200 | **One tabbed component everywhere**: Event timeline · Event list · Lag chart |

Sandbox (screen 05): same, minus the objectives strip; palette and right panel are full height (844).

## Canvas node sizes
| Node | Size |
|---|---|
| Producer | 150 × 72 |
| Topic | 230 wide × (36 header + 48 per partition) |
| Consumer group | 240 wide (220–240); header 36 with group id, state chip, generation and group lag ("group lag ≥ N · K uncommitted" while K partitions have no committed offset, "-" when none has one; M4-T0 F-5); one 216 × 40 row per member (8 px gap): line 1 consumer id, status (icon + word, including "⚠ left group" for a member that is out of the group but still processing, F-7) and lag badge ("lag ≥ N" likewise); line 2 partition chips (`P0 P2`, or "◇ Idle: no partition") and position / committed. While the coordinator still holds a member from an earlier life of the consumer (crashed, or killed and restarted), the row shows that member's partitions as dashed grey chips with "held · timeout in 6.2 s" (session countdown), or "held · waiting for rebalance" while its parked JoinGroup has no deadline yet (F-6), next to the new process's own chips; a member out of the group shows "still processing · pos N". Height 36 + 48 × members + 8 |
| Cluster frame | ≈ 778 × 150 (**not built in M4**: brokers stay separate nodes, M4 decisions T11-1) |
| Broker (M4-T11) | 226 wide; header with broker id; state chip (icon + word: ● Running, ⊘ Fenced, ◐ Shutting down, ■ Stopped, ✕ Crashed; danger colors when not running, tooltip explains the state); "Leads N · follows M"; "Coordinator: group…" while it leads a group's `__consumer_offsets` partition. Down: hatched fill (A6) |
| Topic partition row, replicas (M4-T11) | right column: leader badge `L b2` (or "⚠ offline"), one replica chip per replica (9 px mono, solid border in sync, dashed danger border + "·out" when out of the ISR, tooltip with LEO / HW / ELR), "⚠ N lost" chip after acked records were truncated. Shown only for RF > 1 |
| Controller chip (M4-T11) | top-right canvas panel, ≈ 170 wide, only with ≥ 2 brokers, pointer events pass through to the canvas: "KRaft controller", the read-only timers ("session 9 s · heartbeat 2 s"), "Fenced: …", "⚠ N offline", "N under-replicated"; danger border while any broker is fenced or a partition offline |
| Message dot | 16 px circle with key letter (A/B/C); replication dot 10 px |
| Log cell | 18 × 18 |
| Zoom controls | 160 × 40 (−, fit, +, map toggle) |
| Minimap | 160 × 84, bottom-right; **auto-shows only when content is off-screen**, 200 ms fade |
| Hover toolbar (broker) | 200 × 36: "✕ Kill (crash)" and "■ Stop (shutdown)" while up, "▶ Restart" while down; tooltips explain crash vs controlled shutdown (M4 Q3) |
| Warning toast | 340 × 40 |

## Overlays
| Element | Size |
|---|---|
| Concept card | 380 × 220 modal at step 2; collapses to a 210 × 32 chip |
| Predict card | canvas width − 64 × 76; answer buttons 96 × 44 |
| Auto-pause / replay banner | canvas width − 32 × 44, dark |
| Debrief modal | 720 × auto (fits content), centered, 50% scrim behind |
| Debrief buttons | 160–220 × 48 |

## Right panel contents
- **Properties**: field rows 328 × 56. Topic shows stat tiles (104 × 48), partitions stepper with warning, RF, min.insync.replicas, retention, cleanup.policy, consumers, Delete. See 05b.
- **Code**: language tabs Java · Go · CLI (328 × 32); code block scrolls horizontally; changed line highlighted; per-line explanation tooltip. See 05.
- **Partition log**: partition picker (328 × 32), summary (leader/replicas/ISR/LEO/HW/committed), replica table (broker, role, in sync / ELR, LEO, HW; M4), truncated-records list (struck through, "acknowledged lost"; M4), key filter + Follow toggle, rows 328 × 28, marker rows 328 × 22 (group committed offset, high-watermark). See 05c.

## Color tokens
| Token | Value | Used for |
|---|---|---|
| `--color-primary` | `#0d6efd` | Primary buttons (filled), secondary buttons (outline + text), active Play, stars, progress bars, completed/current level nodes, active tab/phase pill, guided highlight, pins |
| `--color-muted` | `#e9ecef` | Tags/chips, stat tiles, disabled buttons, locked worlds, missed stars, hint/takeaway boxes |
| `--color-surface` | `#ffffff` | Page, canvas, panels, objectives strip |
| `--color-status` | `#212529` | Status banners (PAUSED / REPLAY), hover toolbar |
| `--color-danger` | `#c2410c` (text `#9a3412`, bg `#fff4ec`) | Lost records, failed objectives, failing broker pulse, warnings |
| `--color-border` | `#9a9a9a` / `#cccccc` | Panel borders / dividers |
| Key colors | greys + letter labels | Message keys; never rely on color alone |

Info tags (e.g. "Unlocked: acks = all") are flat `#e9ecef`, monospace, no border, **not clickable**. Anything that looks like a pill with a border must be a button.

## Accessibility rules
- Color is never the only signal (letters on key dots, icons/labels on broker state, dashed border = out of ISR).
- Text contrast WCAG AA. Respect `prefers-reduced-motion` (see animations.md).
- Real `<button>`/`<a>`/`<label>` elements; icon-only buttons need `aria-label`.

# ADR 0003: React + React Flow canvas with a Canvas2D message overlay

- **Status:** Accepted
- **Date:** 2026-10-21 (written down in M2-T13; the decision is from the architecture draft, §2 "Tech stack" and §12 item 3, and roadmap decision D8; it was built in M2-T7 and M2-T9)
- **Milestone:** M2-T7 (canvas), M2-T9 (overlay)

## Context

The player builds a Kafka topology by dragging producers, topics, brokers and consumers onto a canvas and wiring them, and then watches records move. That needs two different things:

- **Nodes and edges** with rich, accessible content (partition rows, offsets, lag badges, toolbars, keyboard focus), drag and drop, selection, zoom, pan and a minimap (FR-1.x).
- **Moving message dots**: up to about 500 at once at 60 fps (NFR-2.1, NFR-2.5), keyed by record and following the edges as nodes move.

DOM elements are good at the first and bad at the second: 500 absolutely positioned elements changing every frame means 500 style updates and layouts per frame.

## Decision

1. **React 18** for the app (roadmap D8).
2. **React Flow (`@xyflow/react`)** for nodes, edges, handles, selection, zoom, pan and the minimap. Nodes are React components that subscribe to their own slice of the snapshot (M2-T8). Partition rows carry invisible handles (`p<i>:in` / `p<i>:out`) so React Flow measures where each row is.
3. **One Canvas2D `<canvas>` overlay** inside React Flow, above nodes and edges, with `pointer-events: none`. It draws every dot in one `requestAnimationFrame` loop, outside React. It reads edge endpoints from React Flow's `nodeLookup` (`internals.handleBounds`) every frame, with a layout-constant fallback for unmeasured nodes, and draws in flow coordinates with React Flow's viewport transform (`apps/web/README.md`, "How the overlay gets edge geometry").
4. Dots are decorative and slower than the engine (ADR 0008). PixiJS or WebGL only if Canvas2D turns out too slow.

## Consequences

- Nodes stay ordinary, accessible React (real buttons, labels, focus) with token colors; the canvas features come from a maintained library.
- Dot drawing cost doesn't depend on React: frames that only move dots cause no React renders. Measured in headless Chromium under WSL2: 60 fps with 500 dots (M2-decisions T9-17). **The check on the target hardware is still open.**
- The overlay depends on React Flow internals (`nodeLookup`, `handleBounds`, the bezier formula of `getBezierPath`). A React Flow upgrade can change them; `anchors.ts` and `path.ts` are unit-tested against the expected geometry, and the fallback covers unmeasured nodes.
- Dots are not in the DOM, so they are invisible to assistive technology. That is acceptable because they are decorative: the event list, partition log and node counters carry the same information as text.
- React Flow's attribution badge is hidden (MIT allows it); `apps/web/README.md` credits React Flow instead.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Dots as React / DOM nodes** | Hundreds of elements re-rendered 60 times a second would cost far more than one canvas draw. |
| **Whole scene in Canvas2D, WebGL or PixiJS** | Selection, text, focus, toolbars and accessibility would all have to be rebuilt by hand. |
| **SVG animation along the edge paths** | Still one DOM element per dot; also hard to keep dots on paths that change while nodes are dragged. |
| **Another node-graph library** (JointJS, Cytoscape.js, hand-written SVG) | React Flow is React-native, MIT-licensed and has the built-in minimap and controls the wireframes ask for (architecture §2). |

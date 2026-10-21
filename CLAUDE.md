# CLAUDE.md: Kafka Playground

## What this project is
A browser game and simulator for learning Apache Kafka. A drag-and-drop canvas of producers, brokers, topics and consumers runs on a **simulated Kafka engine** in the browser.
- **Campaign (learning mode):** levels with a Learn → Break it → Fix loop, scripted tips and dialogues, and 3-star scoring (Outcome / Understanding / Quality).
- **Sandbox (simulator mode):** free building, no tips, failure injection.
- Later: load/stress testing (v0.2), Real Lab against real Kafka in Docker (v0.3), patterns and streaming (v0.4), world-map multi-region (v0.5).

The owner, Rhea, is **learning Kafka through this project**. Correct Kafka behaviour matters more than speed, and explaining that behaviour is part of the job (see "Working with Rhea").

## Read these first
| Doc | What's in it |
|---|---|
| `docs/requirements/kafka-playground-functional-requirements.md` | Features (FR-x.y IDs), level loop, campaign outline, scoring model |
| `docs/requirements/kafka-playground-non-functional-requirements.md` | Performance, determinism, offline, accessibility targets (NFR-x.y) |
| `docs/requirements/kafka-playground-architecture.md` | Engine design, data flow, repo layout, level YAML, fidelity harness, milestones |
| `docs/requirements/kafka-playground-roadmap.md` | Release scope (MVP, v0.2–v0.5) and decisions D8–D11 |
| `docs/requirements/kafka-playground-real-kafka-lab.md` | Real Lab and Lab Bridge (v0.3), code panel decisions D5–D7 |
| `docs/ui/ui-spec.md` | Layout sizes, color tokens, accessibility rules |
| `docs/ui/animations.md` | Every animation with its trigger event and timing |
| `docs/ui/wireframes/index.html` | Wireframes (open in a browser; CSS keyframes show the animations) |
| `docs/milestones/M<n>-*.md` | Task list for the current milestone |
| `docs/simplifications.md` | Where the simulation differs from real Kafka (keep it updated) |

When requirements and code disagree, ask before changing either one. Cite FR/NFR IDs in PR descriptions.

## Tech stack
TypeScript (strict) · pnpm workspaces · Vite · React 18 · React Flow (`@xyflow/react`) · Canvas2D overlay for message dots · Zustand · Radix UI + Tailwind · Comlink (worker RPC) · Zod · Dexie (IndexedDB) · uPlot · Shiki · Vitest + fast-check + Playwright. Go is used only in `lab/` (fidelity runner, later the Lab Bridge), with confluent-kafka-go. Real Kafka: `apache/kafka` 4.x image, KRaft, docker compose.

Don't add a dependency without asking first. Prefer what's already listed.

## Repo layout
```
apps/web/                 React app: canvas, panels, dialogues, worker bootstrap
packages/engine/          Simulation engine: pure TS, no DOM (core/ model/ protocols/ commands/ events/ snapshot/)
packages/schema/          Zod schemas + connection rules shared by UI and engine
packages/level-runtime/   Phases, steps, triggers, objectives, scoring: pure TS
packages/levels/          Level YAML + concept cards + solution command logs
packages/codegen/         Topology → Java / Go / CLI lines with source mapping
packages/fidelity/        Scenario files, TS engine runner, result comparer
lab/                      Go: fidelity-runner (MVP), bridge (v0.3)
docker/                   compose.kraft-3.yml
docs/                     requirements, ui, milestones, adr, simplifications
```

## Commands
```
pnpm install
pnpm dev                          # web app (Vite dev server, apps/web)
pnpm test                         # all unit + property tests
pnpm test:coverage                # same, with a v8 coverage report in each <package>/coverage
pnpm lint && pnpm typecheck      # lint = eslint + prettier --check
pnpm format                       # prettier --write
pnpm --filter @kp/engine demo     # headless event-log demo (-- --seed 7 --until 12000)
pnpm --filter @kp/web build       # production build of the web app (apps/web/dist)
pnpm --filter @kp/web e2e         # Playwright (Chromium); builds + serves via vite preview itself
pnpm --filter @kp/web exec playwright install chromium   # once, to get the browser locally
pnpm levels:verify                # replay every level's solution headlessly (from M6)
docker compose -f docker/compose.kraft-3.yml up -d && pnpm fidelity   # sim vs real Kafka (from M5)
```
Update this section when commands change.

## Non-negotiable engine rules
1. **Pure and headless.** `packages/engine` and `packages/level-runtime` must not import React, the DOM, or browser APIs. They run in a Web Worker, in Node tests, and in the fidelity harness.
2. **Deterministic.** No `Math.random`, `Date.now`, `performance.now`, timers or unordered iteration that affects results. Use the seeded RNG and the sim clock. Order the event queue by `(time, seq)`. Same seed + same command log ⇒ byte-identical event log. A CI test enforces this.
3. **One-way data flow.** The UI sends **commands**; the engine returns **events** and **snapshots**. The UI never mutates engine state. Commands and events are discriminated unions in `packages/engine`.
4. **Kafka-faithful.** Model Kafka 4.x (KRaft only) semantics. In a comment, cite the Kafka doc section or KIP for each modelled behaviour. If you simplify, add an entry to `docs/simplifications.md` (real behaviour vs ours). Never invent Kafka behaviour. If unsure, say so and ask.
5. **Partitioner:** keyed records use `toPositive(murmur2(keyBytes)) % numPartitions`, ported from the Java client. Generated Go code must set `"partitioner": "murmur2_random"`.
6. **Game-tuned defaults** (e.g. shorter `session.timeout.ms`) are allowed only when labelled in the UI with the real default beside them.
7. **Every engine change needs tests**: unit tests, the invariants in architecture §4.6 (fast-check), and the determinism test. Don't weaken an invariant to make a test pass.

## UI rules
- Match the wireframes' layout and the sizes in `docs/ui/ui-spec.md`. Use the color tokens from there (primary `#0d6efd`, muted `#e9ecef`, surface white, status `#212529`, danger orange) as CSS variables / Tailwind theme values, not hard-coded colors.
- Animations follow `docs/ui/animations.md`: driven by engine events, scaled by sim speed where stated, and off or faded under `prefers-reduced-motion`.
- Message dots go on the Canvas2D overlay, not as React nodes. Keep 60 fps with ~500 dots.
- Accessibility: never color-only signals, real buttons and labels, WCAG AA contrast.
- Only things that act are styled as buttons. Info tags are flat muted chips.

## Levels
- Levels are YAML validated by Zod (`packages/schema`). No level-specific code. Add a predicate type to the runtime if a level needs a new kind of check.
- Every level ships a `solution.commands.json`. `pnpm levels:verify` must pass.
- Each level defines its star criteria and quality checks (FR-7.13). Dialogue copy: concept cards ≤ 5 sentences, at most 3 short dialogue messages before the player acts.

## Code style
- TypeScript strict; no `any` (use `unknown` + Zod). Named exports. Small pure functions in the engine.
- Use `type` unions for commands and events; switch on them exhaustively (`assertNever`).
- Test files live next to source (`*.test.ts`). Property tests are named `*.prop.test.ts`.
- Conventional commits (`feat(engine): …`, `fix(web): …`). One milestone task per PR where possible.

## Working with Rhea
- She's an experienced full-stack developer (TypeScript, Go, Angular) but **new to Kafka**.
- When you implement engine or protocol code, finish with a short **"Kafka behind this change"** note: what real Kafka does, how the code models it, and any simplification. Keep it to 3–6 sentences, plain language.
- Before larger changes, propose a short plan and wait for a go-ahead.
- Ask rather than guess about requirements, Kafka semantics you're not sure of, new dependencies, and anything that changes the save-file or level schema.
- At the end of a task, list what to review closely (engine semantics first) and the commands to verify it.

## End-of-milestone summary (required)
When the last task of a milestone is done, write `docs/milestones/M<n>-summary.md`. Planning happens outside this repo, so this file is how the next milestone gets planned. Keep it to one page:
1. **Built:** what exists now (packages, main modules, public APIs, commands).
2. **Deviations:** anything that differs from the requirements/architecture docs, and why.
3. **Simplifications added:** new entries in `docs/simplifications.md`.
4. **Open issues / tech debt:** known bugs, TODOs, flaky tests.
5. **Fidelity status:** which harness scenarios pass (from M5 on).
6. **Questions / suggestions** for the next milestone.
Also update "Current milestone" below.

## Current milestone
M2: Web app shell. See `docs/milestones/M2-web-shell.md`; M1 handover notes are in `docs/milestones/M1-summary.md`. Update this line when a milestone is done.

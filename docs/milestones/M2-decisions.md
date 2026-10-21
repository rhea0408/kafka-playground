# M2 decisions log

Rhea asked for M2 to be built unattended. Where a decision was needed, the rule was: **behave like real Kafka 4.x as closely as practical**, and write the decision down here. Each entry gives the decision, the alternatives and the reason. Review these first.

## Process

| # | Decision | Alternatives | Why |
|---|---|---|---|
| P1 | Each task is its own `m2-t<k>` branch, **stacked** on the previous task's branch (`m2-t0` from `develop`, `m2-t1` from `m2-t0`, …). Each has a PR based on the previous branch, and CI must be green. **Nothing is merged**: merging without review was blocked, and the M1 no-review rule was only for M1. | Merge each PR after green CI (as in M1) | Review stays with Rhea. To merge: review the PRs in order, merging `m2-t0` into `develop` first. When a merged branch is deleted, GitHub moves the next PR's base to `develop`. If it doesn't, change the base with `gh api -X PATCH …/pulls/N -f base=develop`, then close and reopen the PR. |
| P2 | The milestone doc already approves the M2 dependencies. Any other dependency is listed under its task below with the reason. | Stop and ask | Rhea is away. These are small companion packages (test DOM, React types). |


## T0 · Web app scaffold

| # | Decision | Alternatives | Why |
|---|---|---|---|
| T0-1 | Vite 8 + `@vitejs/plugin-react` 6, React 18.3, `@types/react@18` / `@types/react-dom@18` (extra dev deps: the React types). | Vite 7; React 19 | Vite 8 is the current stable and is accepted by Vitest 5. React 18 is what the tech stack names. The type packages are needed for strict TS. |
| T0-2 | Tailwind **v4** with the `@tailwindcss/vite` plugin (extra dev dep). Tokens live in a `@theme static` block in `apps/web/src/index.css`; no `tailwind.config.js`. | Tailwind v3 + PostCSS + config file | v4 is the current release and its Vite plugin needs no PostCSS setup. `@theme` gives CSS variables and utilities from one place. `static` emits every variable even when unused, so the Canvas2D overlay (T9) can read them with `getComputedStyle`. |
| T0-3 | Token names: `--color-primary`, `--color-muted`, `--color-surface`, `--color-status`, `--color-danger`, plus `--color-danger-text` (#9a3412) and `--color-danger-bg` (#fff4ec) for the two extra danger values, `--color-border` (#9a9a9a, panel borders) and `--color-divider` (#cccccc, dividers). | One token with two values | ui-spec lists two values for danger and border; each value needs its own name. |
| T0-4 | Tailwind's default palette is cleared (`--color-*: initial`) and an ESLint rule in `apps/web/src` rejects hex / `rgb()` / `hsl()` / `bg-[#…]` color literals. | Rely on review | Makes "no hard-coded colors" (CLAUDE.md UI rules) enforced, not just a convention. |
| T0-5 | `eslint-plugin-react-hooks` **not** added. | Add it | It isn't on the approved list and isn't required to build. Worth approving once hooks-heavy code lands (T5+). |
| T0-6 | ESLint: `apps/web/src/**` gets browser globals and no determinism rules (the worker host may use timers and `performance.now`). `*.worker.ts` gets worker globals and bans DOM-only globals (`window`, `document`, …) via `no-restricted-globals`, because TS's `no-undef` is off and `src/` shares one DOM tsconfig. Vite/Playwright configs and `e2e/` get Node globals and may default-export. | Separate tsconfig with the WebWorker lib for workers | A separate worker tsconfig is better left to T4, when the first real worker exists. The lint ban catches the obvious mistakes now. |
| T0-7 | Two tsconfigs in `apps/web`: `tsconfig.app.json` (src, DOM + JSX, `vite/client` types) and `tsconfig.node.json` (configs + e2e, Node types, plus the DOM lib because `page.evaluate` callbacks run in the page). `tsconfig.json` is a solution file that references both, so typescript-eslint's project service finds them. | One tsconfig including Node types | Keeps Node types out of app code. |
| T0-8 | Coverage: `@vitest/coverage-v8` at the repo root and a root script **`pnpm test:coverage`** (`pnpm -r --if-present test --coverage`). Each package writes `<package>/coverage/` (text + html + json + clover, Vitest defaults). | `pnpm test -- --coverage` as the milestone says | pnpm forwards `-- --coverage` to Vitest as a file filter after `--`, so no coverage is produced. A dedicated script is clear. No threshold yet (milestone says set one after M2). |
| T0-9 | `apps/web` `test` script is `vitest run --passWithNoTests` with `include: src/**/*.test.{ts,tsx}` in `vite.config.ts`. | Add a dummy unit test | No component code worth testing yet; the include keeps Vitest away from Playwright specs in `e2e/`. Drop `--passWithNoTests` once T4 adds tests. |
| T0-10 | Playwright runs against the production build: `webServer` runs `pnpm build && pnpm preview --host 127.0.0.1 --port 4173`. Chromium only; CI retries once, writes an HTML report, which is uploaded only on failure together with `test-results/`. | Vite dev server | The worker bundle (T4, T12 determinism test) should be tested as shipped. The explicit `127.0.0.1` removes any IPv4/IPv6 `localhost` ambiguity between Vite and Playwright. Locally (WSL) a run usually takes ~2 s, but two runs took ~2 min with no clear cause; watch for it in CI. CI builds the app twice (build step + e2e); it takes under a second. |
| T0-11 | CI pinned to `ubuntu-24.04`; order: install → lint → typecheck → test with coverage (artifact `coverage`, uploaded unless cancelled) → engine demo → web build → `playwright install --with-deps chromium` → e2e. `actions/upload-artifact@v7` to match the other `@v7` actions. | Separate e2e job | One job keeps the Actions budget low; split later if it gets slow. |
| T0-12 | The placeholder page is a 56 px top bar (title, a muted "Sandbox" chip and the sim clock from the engine's `formatSimTime(0)`). | Fully empty page | Importing from `@kp/engine` proves the workspace package bundles in Vite. |

## T1 · Message timing

| # | Decision | Alternatives | Why |
|---|---|---|---|
| T1-1 | Engine keeps real timing; dots are decorative (as decided in the milestone doc). Written up as `docs/adr/0008-message-timing.md` (Status: Accepted). | Slow the engine to dot speed; hold log/lag updates until dots arrive | Real numbers stay real, timeout lessons and the fidelity harness need no rescaling. Full reasoning in the ADR. |
| T1-2 | `networkDelayMs` checked: default is still 1 ms (`packages/engine/src/engine.ts`). No engine code changed. | — | Task says check, not change. |
| T1-3 | `linger.ms` is not modelled by the engine. No new simplification entry: the existing "Producer batching" row already names `batch.size` and `linger.ms` and says each record is its own request. The ADR spells out the consequence (engine up to 5 ms faster than a default 4.x producer). | Add a separate "linger" row | Avoids a duplicate row; the existing entry already covers it. |
| T1-4 | ADR states `linger.ms` default is 5 ms in Kafka 4.x clients (KIP-1030, 4.0 upgrade notes) and that librdkafka (Go client) also defaults to 5 ms. | — | Confident from the 4.0 release notes; worth a glance at the fidelity harness config (M5), which should set `linger.ms=0` or allow for it. |
| T1-5 | If a record's A1 dot was skipped (cap or falling behind), its A2 dot is skipped too. | Start A2 immediately when there is no A1 dot | Keeps "A2 only after A1 arrived" true without exceptions; a lone A2 dot would show a read with no visible write. |
| T1-6 | animations.md: A1/A2 rows marked decorative, A2 row states the trace-id rule, A4/A5 rows say "never waits for dots", plus a "Message dots are decorative" section with speed, ordering, skip, 500 cap, "too many to animate" chip and the legend text. A5's trigger (`LagChanged`) left as is; Q2 (derive lag from snapshots) is for T13 to apply. | Rewrite A5 now | Keep T1 to its scope. |
| T1-7 | New file `docs/milestones/M2-learning-checkpoints.md` holds the checkpoint answers for all M2 tasks, one heading per task. | One file per task | One place for Rhea to read. |

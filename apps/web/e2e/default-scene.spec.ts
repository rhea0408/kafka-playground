/**
 * M2-T12: the default Sandbox scene end to end, and the worker determinism check.
 *
 * The determinism tests run the production bundle's engine in the browser worker, read its event
 * log hash through `window.__kp.sim` (sim/debug-hook.ts), then run the same seed and commands on
 * the engine *source* here in Node (Playwright's runner is Node) and compare. Equal hashes mean
 * the bundled worker engine and the Node engine produce the same events, byte for byte (NFR-4.1).
 */

import {
  createEngine,
  hashEvents,
  replayCommandLog,
  type CommandLogEntry,
  type EngineEvent,
} from '@kp/engine';
import { expect, test, type Page } from '@playwright/test';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import type { HostStatus } from '../src/sim/protocol.ts';
import {
  DEFAULT_PARTITIONS,
  DEFAULT_TOPIC,
  defaultSceneState,
} from '../src/stores/default-scene.ts';
import { toTopologyInput } from '../src/stores/topology-store.ts';

/** Screenshots to look at by eye; the folder is gitignored. */
const SHOTS = fileURLToPath(new URL('../e2e-screenshots/', import.meta.url));

/** The worker ring buffer's size (protocol.ts EVENT_HISTORY_SIZE). */
const EVENT_HISTORY_SIZE = 10_000;

/**
 * Lag bound. The badge shows committed lag (HW − committed offset, as kafka-consumer-groups.sh
 * reports it; M1 deviation), not HW − position. The consumer long-polls and reads each record
 * within a few ms, but it only auto-commits every auto.commit.interval.ms = 5 s, so committed lag
 * climbs to about 2 msg/s × 5 s = 10 between commits and drops back after each one (10 is the
 * exact maximum for seed 7, see default-scene.test.ts). 15 leaves room for a commit landing one
 * poll late; a consumer that really fell behind would grow past it within seconds at 10×.
 */
const LAG_BOUND = 15;

interface SimHook {
  readonly seed: number;
  readonly status: () => Promise<HostStatus>;
  readonly commandLog: () => Promise<readonly CommandLogEntry[]>;
  readonly eventLogHash: () => Promise<string>;
  readonly eventHistory: () => Promise<readonly EngineEvent[]>;
}
type HookWindow = { __kp?: { sim?: SimHook; overlay?: { dotsDrawn: number } } };

const node = (page: Page, id: string) => page.locator(`.react-flow__node[data-id="${id}"]`);
const props = (page: Page) => page.getByTestId('properties-panel');

/** Opens the Sandbox and waits until the engine's first snapshot reached the nodes. */
async function open(page: Page): Promise<void> {
  await page.goto('/');
  await expect(node(page, DEFAULT_TOPIC).locator('[data-slot="leader"]')).toHaveCount(
    DEFAULT_PARTITIONS,
  );
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
}

async function speed10(page: Page): Promise<void> {
  for (const next of ['2×', '5×', '10×']) {
    await page.getByRole('button', { name: /^Speed / }).click();
    await expect(page.getByRole('button', { name: new RegExp(`^Speed ${next}`) })).toBeVisible();
  }
}

async function pause(page: Page): Promise<HostStatus> {
  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
  const status = await readSim(page, 'status');
  expect(status.playing).toBe(false);
  return status;
}

/** Calls one of the hook's read-only methods in the page. */
function readSim<K extends Exclude<keyof SimHook, 'seed'>>(
  page: Page,
  method: K,
): Promise<Awaited<ReturnType<SimHook[K]>>> {
  return page.evaluate(async (m) => {
    const sim = (window as unknown as HookWindow).__kp?.sim;
    if (sim === undefined) throw new Error('window.__kp.sim is missing');
    return (await sim[m]()) as never;
  }, method);
}

const seedOf = (page: Page): Promise<number> =>
  page.evaluate(() => (window as unknown as HookWindow).__kp?.sim?.seed ?? -1);

const leoTexts = (page: Page): Promise<string[]> =>
  node(page, DEFAULT_TOPIC).locator('[data-slot="partition-log"]').allTextContents();

test.use({ viewport: { width: 1440, height: 900 } });

test('the default scene plays: dots drawn, every partition filled, lag bounded', async ({
  page,
}) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await open(page);
  await expect(node(page, 'p1')).toBeVisible();
  await expect(node(page, 'c1')).toBeVisible();
  await expect(node(page, 'b1')).toBeVisible();
  await expect(page.locator('.react-flow__edge')).toHaveCount(2);
  // The minimap stays hidden: the scene fits the canvas.
  await expect(page.getByTestId('minimap')).toHaveAttribute('data-visible', 'false');

  await speed10(page);
  await page.getByRole('button', { name: 'Play' }).click();

  await expect
    .poll(
      () => page.evaluate(() => (window as unknown as HookWindow).__kp?.overlay?.dotsDrawn ?? 0),
      { timeout: 15_000 },
    )
    .toBeGreaterThan(0);
  // Keys D, E, F go to partitions 0, 1, 2: every partition gets records.
  await expect
    .poll(async () => (await leoTexts(page)).map((text) => /LEO (\S+)/.exec(text)?.[1] ?? '0'), {
      timeout: 15_000,
    })
    .not.toContain('0');

  // Sample the lag badge for ~3 s wall (~30 s sim at 10×, six auto-commit cycles).
  const lag = node(page, 'c1').locator('[data-slot="lag"]');
  const samples: number[] = [];
  for (let i = 0; i < 15; i++) {
    const value = await lag.getAttribute('data-lag');
    if (value !== null && value !== '') samples.push(Number(value));
    await page.waitForTimeout(200);
  }
  expect(samples.length).toBeGreaterThan(0);
  expect(Math.max(...samples)).toBeLessThanOrEqual(LAG_BOUND);

  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}default-scene.png` });
  await pause(page);

  // It still fits at the minimum supported size (canvas 766 × 512).
  await page.setViewportSize({ width: 1366, height: 768 });
  await expect(page.getByTestId('minimap')).toHaveAttribute('data-visible', 'false');
  await page.screenshot({ path: `${SHOTS}default-scene-1366x768.png` });
  expect(errors).toEqual([]);
});

test('worker determinism: the default scene alone gives the same hash as Node', async ({
  page,
}) => {
  // The Node side runs the engine source, not a bundle.
  expect(fileURLToPath(import.meta.resolve('@kp/engine'))).toMatch(
    /packages[\\/]engine[\\/]src[\\/]index\.ts$/,
  );
  await open(page);
  await speed10(page);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(async () => (await readSim(page, 'status')).t, { timeout: 15_000 })
    .toBeGreaterThan(20_000);
  const { t } = await pause(page);
  // Play, Pause and speed are host controls, not commands: the log stays empty.
  expect(await readSim(page, 'commandLog')).toEqual([]);
  const workerHash = await readSim(page, 'eventLogHash');
  const history = await readSim(page, 'eventHistory');

  // Independently specified here: the scene's topology and seed, run to the same sim time.
  const engine = createEngine({
    seed: await seedOf(page),
    topology: toTopologyInput(defaultSceneState()),
  });
  const events: EngineEvent[] = [];
  engine.on((event) => events.push(event));
  engine.runUntil(t);

  expect(events.length).toBeGreaterThan(100);
  expect(events.length).toBeLessThan(EVENT_HISTORY_SIZE);
  expect(workerHash).toBe(hashEvents(events));
  // The live stream (not only the worker's replay) matches too.
  expect(history.length).toBe(events.length);
  expect(hashEvents(history)).toBe(hashEvents(events));
});

test('worker determinism: UI commands replayed in Node give the same hash', async ({
  page,
}, testInfo) => {
  await open(page);
  await speed10(page);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(async () => (await readSim(page, 'status')).t, { timeout: 15_000 })
    .toBeGreaterThan(5_000);

  // UpdateConfig while playing: one partition more on `orders`.
  await node(page, DEFAULT_TOPIC).click();
  await props(page).getByRole('button', { name: 'One partition more' }).click();
  await props(page).getByRole('button', { name: 'Add 1 partition' }).click();
  await expect(node(page, DEFAULT_TOPIC).locator('[data-partition-row]')).toHaveCount(4);

  // ProduceManual from the producer's form.
  await node(page, 'p1').click();
  await props(page)
    .getByLabel(/^Record key/)
    .fill('D');
  await props(page).getByLabel('Record value').fill('manual-D');
  await props(page).getByRole('button', { name: 'Send record' }).click();
  await expect(props(page).getByTestId('mp-deliveries')).toContainText(/offset \d+\./);

  const reached = (await readSim(page, 'status')).t;
  await expect
    .poll(async () => (await readSim(page, 'status')).t, { timeout: 15_000 })
    .toBeGreaterThan(reached + 5_000);
  const { t } = await pause(page);

  const log = await readSim(page, 'commandLog');
  expect(log.map((entry) => entry.command.type)).toEqual(['UpdateConfig', 'ProduceManual']);
  const workerHash = await readSim(page, 'eventLogHash');
  const history = await readSim(page, 'eventHistory');

  // Through a file, as a bug report would carry it.
  const file = testInfo.outputPath('command-log.json');
  writeFileSync(file, JSON.stringify(log, null, 2));
  const fromFile = JSON.parse(readFileSync(file, 'utf8')) as CommandLogEntry[];

  const events: EngineEvent[] = [];
  const replay = replayCommandLog(
    { seed: await seedOf(page), topology: toTopologyInput(defaultSceneState()) },
    fromFile,
    (event) => events.push(event),
  );
  replay.runUntil(t);

  expect(events.length).toBeGreaterThan(100);
  expect(events.length).toBeLessThan(EVENT_HISTORY_SIZE);
  expect(events.some((e) => e.type === 'ConfigUpdated')).toBe(true);
  expect(workerHash).toBe(hashEvents(events));
  expect(history.length).toBe(events.length);
  expect(hashEvents(history)).toBe(hashEvents(events));
});

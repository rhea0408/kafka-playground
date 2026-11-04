/**
 * M3 scene presets end to end (M3-T12): the idle member, a crash that ends in a rebalance with
 * duplicates, eager vs cooperative revocations, and the worker giving the same event-log hash as
 * Node for a group scenario with commands.
 */

import { hashEvents, replayCommandLog, type CommandLogEntry, type EngineEvent } from '@kp/engine';
import { expect, test, type Page } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import type { HostStatus } from '../src/sim/protocol.ts';
import { DEFAULT_GROUP, sceneById } from '../src/stores/default-scene.ts';
import { toTopologyInput } from '../src/stores/topology-store.ts';

const SHOTS = fileURLToPath(new URL('../e2e-screenshots/', import.meta.url));
const GROUP = `group:${DEFAULT_GROUP}`;

interface SimHook {
  readonly seed: number;
  readonly status: () => Promise<HostStatus>;
  readonly commandLog: () => Promise<readonly CommandLogEntry[]>;
  readonly eventLogHash: () => Promise<string>;
  readonly eventHistory: () => Promise<readonly EngineEvent[]>;
}
type HookWindow = { __kp?: { sim?: SimHook } };

const node = (page: Page, id: string) => page.locator(`.react-flow__node[data-id="${id}"]`);

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

const simTime = async (page: Page): Promise<number> => (await readSim(page, 'status')).t;

async function open(page: Page, scene: string): Promise<void> {
  await page.goto(`/?debug&scene=${scene}`);
  await expect(node(page, GROUP)).toBeVisible();
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
}

async function play10(page: Page): Promise<void> {
  for (const next of ['2×', '5×', '10×']) {
    await page.getByRole('button', { name: /^Speed / }).click();
    await expect(page.getByRole('button', { name: new RegExp(`^Speed ${next}`) })).toBeVisible();
  }
  await page.getByRole('button', { name: 'Play' }).click();
}

async function runPast(page: Page, t: number): Promise<void> {
  await expect.poll(() => simTime(page), { timeout: 30_000 }).toBeGreaterThan(t);
}

async function pause(page: Page): Promise<number> {
  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
  return simTime(page);
}

test.use({ viewport: { width: 1440, height: 900 } });

test('scene "Idle consumer": one of four members is idle', async ({ page }) => {
  await open(page, 'idle');
  await expect(page.getByLabel('Scene')).toHaveValue('idle');
  await play10(page);
  await expect(node(page, GROUP).locator('[data-slot="idle"]')).toHaveCount(1, { timeout: 15_000 });
});

test('scene "Crash and duplicates": a killed member is removed after the session timeout; duplicates follow', async ({
  page,
}) => {
  await open(page, 'crash');
  await play10(page);
  await runPast(page, 13_000);
  // Kill c1 from the member toolbar (hover its row).
  await node(page, GROUP).locator('[data-consumer-row="c1"]').hover();
  await page.getByRole('button', { name: 'Kill consumer c1' }).click();
  const killedAt = await simTime(page);
  await expect(
    node(page, GROUP).locator('[data-consumer-row="c1"] [data-slot="status"]'),
  ).toHaveText(/crashed/);
  await runPast(page, killedAt + 14_000);
  await pause(page);

  const history = await readSim(page, 'eventHistory');
  const removed = history.find((e) => e.type === 'MemberRemoved' && e.consumerId === 'c1');
  expect(removed).toMatchObject({ reason: 'session-timeout' });
  // Measured from the engine's crash event: the clock read after the click runs a little late.
  const crashedAt =
    history.find(
      (e) => e.type === 'ConsumerStopped' && e.consumerId === 'c1' && e.reason === 'crash',
    )?.t ?? Infinity;
  // About session.timeout.ms (10 s) after the last heartbeat, which is ≤ 3 s before the kill.
  expect((removed?.t ?? 0) - crashedAt).toBeGreaterThan(10_000 - 3_100);
  expect((removed?.t ?? 0) - crashedAt).toBeLessThanOrEqual(10_100);
  const completed = history.find(
    (e) => e.type === 'RebalanceCompleted' && e.t >= (removed?.t ?? Infinity),
  );
  expect(completed).toBeDefined();
  expect(history.filter((e) => e.type === 'DuplicateProcessed').length).toBeGreaterThan(0);
  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}scene-crash.png` });
});

/** Idle scene, assignor set from the group panel, then a fifth member joins. */
async function fifthMember(page: Page, assignor: string | null): Promise<readonly EngineEvent[]> {
  await open(page, 'idle');
  if (assignor !== null) {
    await node(page, GROUP).locator('[data-slot="group-header"]').click();
    await page
      .getByRole('combobox', { name: 'ASSIGNMENT STRATEGY (EVERY MEMBER)' })
      .selectOption(assignor);
  }
  await play10(page);
  await runPast(page, 10_000);
  const box = await node(page, GROUP).boundingBox();
  const canvas = await page.getByRole('main', { name: 'Canvas' }).boundingBox();
  if (box === null || canvas === null) throw new Error('not visible');
  await page
    .getByRole('complementary', { name: 'Palette' })
    .getByRole('button', { name: 'Consumer' })
    .dragTo(page.getByRole('main', { name: 'Canvas' }), {
      targetPosition: { x: box.x - canvas.x + box.width / 2, y: box.y - canvas.y + 20 },
    });
  await expect(node(page, GROUP).locator('[data-consumer-row="c5"]')).toBeVisible();
  const joined = await simTime(page);
  await runPast(page, joined + 8_000);
  await pause(page);
  const history = await readSim(page, 'eventHistory');
  // From c5's JoinGroup on: at 10× the rebalance can finish before `joined` was read.
  const c5 = history.find((e) => e.type === 'MemberJoined' && e.consumerId === 'c5');
  expect(c5).toBeDefined();
  return history.filter((e) => e.seq >= (c5?.seq ?? Infinity));
}

test('eager (default): every member gives up its partitions when a fifth one joins', async ({
  page,
}) => {
  const after = await fifthMember(page, null);
  expect(after.filter((e) => e.type === 'RebalanceCompleted').length).toBeGreaterThan(0);
  const revoked = new Set(
    after.flatMap((e) => (e.type === 'PartitionsRevoked' ? [e.consumerId] : [])),
  );
  expect(revoked.size).toBe(3);
});

test('cooperative-sticky: members that keep their partitions show no revoke', async ({ page }) => {
  const after = await fifthMember(page, 'cooperative-sticky');
  expect(after.filter((e) => e.type === 'RebalanceCompleted').length).toBeGreaterThan(0);
  expect(after.filter((e) => e.type === 'PartitionsRevoked')).toEqual([]);
  await expect(node(page, GROUP).locator('[data-chip]')).toHaveCount(3);
});

test('worker determinism with a group scenario: UI commands replayed in Node give the same hash', async ({
  page,
}) => {
  await open(page, 'crash');
  await play10(page);
  await runPast(page, 9_000);
  await node(page, GROUP).locator('[data-consumer-row="c2"]').hover();
  await page.getByRole('button', { name: 'Stop consumer c2' }).click();
  await runPast(page, 15_000);
  await node(page, GROUP).locator('[data-consumer-row="c1"]').hover();
  await page.getByRole('button', { name: 'Kill consumer c1' }).click();
  await runPast(page, 20_000);
  const t = await pause(page);

  const log = await readSim(page, 'commandLog');
  expect(log.map((entry) => entry.command.type)).toEqual(['StopConsumer', 'KillConsumer']);
  const workerHash = await readSim(page, 'eventLogHash');
  const scene = sceneById('crash');
  if (scene === undefined) throw new Error('no crash scene');
  const events: EngineEvent[] = [];
  const seed = await page.evaluate(() => (window as unknown as HookWindow).__kp?.sim?.seed ?? -1);
  replayCommandLog({ seed, topology: toTopologyInput(scene.state()) }, log, (e) =>
    events.push(e),
  ).runUntil(t);
  expect(events.some((e) => e.type === 'RebalanceCompleted')).toBe(true);
  expect(workerHash).toBe(hashEvents(events));
});

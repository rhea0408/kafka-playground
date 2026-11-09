/**
 * M4 cluster scenes end to end (M4-T12): leader failover, acks=1 data loss, not enough replicas,
 * coordinator failover, and the worker giving the same event-log hash as Node with broker kills.
 * Brokers are killed and restarted from their hover toolbar, as a player would.
 */

import { hashEvents, replayCommandLog, type CommandLogEntry, type EngineEvent } from '@kp/engine';
import { expect, test, type Page } from '@playwright/test';
import type { HostStatus } from '../src/sim/protocol.ts';
import { sceneById } from '../src/stores/default-scene.ts';
import { toTopologyInput } from '../src/stores/topology-store.ts';

interface SimHook {
  readonly seed: number;
  readonly status: () => Promise<HostStatus>;
  readonly commandLog: () => Promise<readonly CommandLogEntry[]>;
  readonly eventLogHash: () => Promise<string>;
  readonly eventHistory: () => Promise<readonly EngineEvent[]>;
}
type HookWindow = { __kp?: { sim?: SimHook } };

test.use({ viewport: { width: 1440, height: 900 } });

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
const events = (page: Page) => readSim(page, 'eventHistory');

async function open(page: Page, scene: string): Promise<void> {
  await page.goto(`/?debug&scene=${scene}`);
  await expect(node(page, 'b3')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
}

async function play10(page: Page): Promise<void> {
  for (const next of ['2×', '5×', '10×']) {
    await page.getByRole('button', { name: /^Speed / }).click();
    await expect(page.getByRole('button', { name: new RegExp(`^Speed ${next}`) })).toBeVisible();
  }
  await page.getByRole('button', { name: 'Play' }).click();
}

async function runPast(page: Page, t: number, timeout = 30_000): Promise<void> {
  await expect.poll(() => simTime(page), { timeout }).toBeGreaterThan(t);
}

/** The broker that leads orders-0, from the topic node's leader badge ("L b2"). */
async function leaderOfP0(page: Page): Promise<string> {
  const badge = node(page, 'orders').locator('[data-partition-row="0"] [data-slot="leader"]');
  const text = (await badge.textContent()) ?? '';
  const match = /^L (b\d)$/.exec(text.trim());
  if (match?.[1] === undefined) throw new Error(`no leader in "${text}"`);
  return match[1];
}

/** Hover the broker and press one of its toolbar buttons. */
async function brokerAction(page: Page, broker: string, action: 'Kill' | 'Stop' | 'Restart') {
  await node(page, broker).hover();
  const toolbar = page.getByRole('toolbar', { name: `Actions for broker ${broker}` });
  await toolbar.getByRole('button', { name: `${action} broker ${broker}` }).click();
}

const ofType = <T extends EngineEvent['type']>(list: readonly EngineEvent[], type: T) =>
  list.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

test('leader failover: kill from the toolbar → fenced → new leader → restart → back in the ISR', async ({
  page,
}) => {
  await open(page, 'failover');
  await play10(page);
  await runPast(page, 5000);
  const leader = await leaderOfP0(page);
  await brokerAction(page, leader, 'Kill');
  const killedAt = await simTime(page);
  await expect(node(page, leader).locator('[data-slot="broker-state"]')).toHaveText('✕Crashed');
  // About broker.session.timeout.ms later the controller fences it and P0 gets a new leader.
  await expect(page.getByTestId('controller-chip')).toContainText(`Fenced: ${leader}`, {
    timeout: 20_000,
  });
  await expect.poll(() => leaderOfP0(page), { timeout: 20_000 }).not.toBe(leader);
  const elected = ofType(await events(page), 'LeaderElected').find(
    (e) => e.topic === 'orders' && e.partition === 0,
  );
  expect(elected?.t).toBeGreaterThan(killedAt + 6000);
  expect(elected?.t).toBeLessThanOrEqual(killedAt + 9100);
  // Restart: it rejoins as a follower; its replica chip is solid again.
  await brokerAction(page, leader, 'Restart');
  const chip = node(page, 'orders').locator(
    `[data-partition-row="0"] [data-slot="replicas"] [aria-label^="Replica ${leader} "]`,
  );
  await expect(chip).toHaveAttribute('data-in-sync', 'true', { timeout: 20_000 });
  const all = await events(page);
  expect(ofType(all, 'ProduceFailed')).toEqual([]);
  expect(ofType(all, 'RecordsTruncated').reduce((n, e) => n + e.acked, 0)).toBe(0);
});

test('acks=1 data loss: the old leader truncates acknowledged records when it returns', async ({
  page,
}) => {
  await open(page, 'acks1-loss');
  await play10(page);
  await runPast(page, 5000);
  const leader = await leaderOfP0(page);
  await brokerAction(page, leader, 'Kill');
  await expect.poll(() => leaderOfP0(page), { timeout: 20_000 }).not.toBe(leader);
  await brokerAction(page, leader, 'Restart');
  await expect
    .poll(
      async () => ofType(await events(page), 'RecordsTruncated').reduce((n, e) => n + e.acked, 0),
      { timeout: 30_000 },
    )
    .toBeGreaterThan(0);
  await expect(node(page, 'orders').locator('[data-slot="lost"]')).toBeVisible();
  // The partition log lists the deleted records, struck through.
  await page.getByRole('button', { name: 'Pause' }).click();
  await page.getByRole('tab', { name: 'Partition log' }).click();
  const log = page.getByTestId('partition-log');
  await log.getByRole('radio', { name: 'P0' }).click();
  await expect(log.getByTestId('pl-truncated')).toContainText('acknowledged lost');
  await expect(log.getByTestId('pl-replicas')).toBeVisible();
});

test('not enough replicas: two followers down → NOT_ENOUGH_REPLICAS, shown on the producer', async ({
  page,
}) => {
  await open(page, 'not-enough-replicas');
  await play10(page);
  await runPast(page, 5000);
  const leader = await leaderOfP0(page);
  const followers = ['b1', 'b2', 'b3'].filter((b) => b !== leader);
  for (const b of followers) await brokerAction(page, b, 'Kill');
  await expect(node(page, 'p1').locator('[data-slot="retrying"]')).toContainText(
    'NOT_ENOUGH_REPLICAS',
    { timeout: 30_000 },
  );
});

test('under min ISR: two followers down → "HW frozen" chip on P0; one restart → back to under-replicated (G-3)', async ({
  page,
}) => {
  await open(page, 'not-enough-replicas');
  await play10(page);
  await runPast(page, 5000);
  const leader = await leaderOfP0(page);
  const followers = ['b1', 'b2', 'b3'].filter((b) => b !== leader);
  const chip = node(page, 'orders').locator('[data-partition-row="0"] [data-slot="isr-state"]');
  await expect(chip).toHaveCount(0);
  for (const b of followers) await brokerAction(page, b, 'Kill');
  // After broker.session.timeout.ms the controller fences them and shrinks the ISR to the leader.
  await expect(chip).toHaveText('⚠ under min ISR · HW frozen', { timeout: 30_000 });
  await expect(chip).toHaveAttribute('title', /min\.insync\.replicas \(2\)/);
  await expect(chip).toHaveAttribute('data-state', 'under-min-isr');
  // One follower back in the ISR: 2 of 3 in sync, which is enough for the HW again.
  const [back] = followers;
  await brokerAction(page, back ?? '', 'Restart');
  await expect(chip).toHaveAttribute('data-state', 'under-replicated', { timeout: 30_000 });
  await expect(chip).toHaveText('under-replicated');
});

test('coordinator failover: the group stays Stable with its committed offsets', async ({
  page,
}) => {
  await open(page, 'coordinator-failover');
  await play10(page);
  await runPast(page, 10_000);
  const coordinator = page
    .locator('.react-flow__node[data-kind="broker"], .react-flow__node')
    .filter({ hasText: 'Coordinator: orders-app' })
    .first();
  const id = await coordinator.getAttribute('data-id');
  expect(id).toMatch(/^b\d$/);
  const before = ofType(await events(page), 'RebalanceCompleted').at(-1)?.generation;
  await brokerAction(page, id ?? '', 'Kill');
  await expect
    .poll(async () => ofType(await events(page), 'CoordinatorMoved').length, { timeout: 30_000 })
    .toBeGreaterThan(0);
  const moved = ofType(await events(page), 'CoordinatorMoved')[0];
  expect(moved).toMatchObject({ groupId: 'orders-app', state: 'Stable', generation: before });
  await runPast(page, (moved?.t ?? 0) + 5000);
  expect(
    ofType(await events(page), 'RebalanceStarted').filter((e) => e.t > (moved?.t ?? 0)),
  ).toEqual([]);
  await expect(node(page, 'group:orders-app')).toContainText('Stable');
});

test('worker determinism with broker kills: the same hash as Node', async ({ page }) => {
  await open(page, 'failover');
  await play10(page);
  await runPast(page, 4000);
  const leader = await leaderOfP0(page);
  await brokerAction(page, leader, 'Kill');
  await runPast(page, 15_000);
  await brokerAction(page, leader, 'Restart');
  await runPast(page, 20_000);
  await page.getByRole('button', { name: 'Pause' }).click();
  const { t } = await readSim(page, 'status');
  const log = await readSim(page, 'commandLog');
  expect(log.map((e) => e.command.type)).toEqual(['KillBroker', 'RestartBroker']);
  const workerHash = await readSim(page, 'eventLogHash');
  const seed = await page.evaluate(() => (window as unknown as HookWindow).__kp?.sim?.seed ?? -1);
  const state = sceneById('failover')?.state();
  if (state === undefined) throw new Error('scene');
  const replayed: EngineEvent[] = [];
  replayCommandLog({ seed, topology: toTopologyInput(state) }, log, (e) =>
    replayed.push(e),
  ).runUntil(t);
  expect(ofType(replayed, 'LeaderElected').length).toBeGreaterThan(0);
  expect(workerHash).toBe(hashEvents(replayed));
});

test('internal topics: a 1-broker restart shows no __consumer_offsets rows until the toggle is on (G-1)', async ({
  page,
}) => {
  await page.goto('/?debug&scene=default');
  await expect(node(page, 'b1')).toBeVisible();
  await play10(page);
  await runPast(page, 3000);
  await brokerAction(page, 'b1', 'Kill');
  // About broker.session.timeout.ms later the controller fences b1: every partition goes offline.
  await expect
    .poll(async () => ofType(await events(page), 'PartitionOffline').length, { timeout: 20_000 })
    .toBeGreaterThan(3);
  // Read before the click: at 10× the election can land before a read after it.
  const restartedAt = await simTime(page);
  await brokerAction(page, 'b1', 'Restart');
  await expect
    .poll(
      async () =>
        ofType(await events(page), 'LeaderElected').filter(
          (e) => e.topic === 'orders' && e.t > restartedAt,
        ).length,
      { timeout: 20_000 },
    )
    .toBe(3);
  await page.getByRole('button', { name: 'Pause' }).click();

  // Only the leader changes, so every row fits in the windowed list.
  const list = page.getByTestId('event-list');
  await list.getByRole('button', { name: /^Filter: / }).click();
  const panel = page.getByRole('group', { name: 'Event types to show' });
  await panel.getByRole('button', { name: 'Hide all' }).click();
  await panel.getByRole('checkbox', { name: 'LeaderElected' }).check();
  await panel.getByRole('checkbox', { name: 'PartitionOffline' }).check();
  const rows = page.getByRole('region', { name: 'Event list' });
  await expect(rows).toContainText('orders-0');
  await expect(rows).not.toContainText('__consumer_offsets');
  // G-2: b1 held the only copy, so the election reads as a last-known-leader one, not data loss.
  await expect(rows).toContainText('unclean (last known leader: it held the only copy');
  await expect(rows).not.toContainText('records may be lost');
  await expect(node(page, 'orders').locator('[data-slot="lost"]')).toHaveCount(0);
  await expect(list.getByRole('button', { name: /^Filter: / })).toContainText(
    'internal topics hidden',
  );

  await panel.getByRole('checkbox', { name: /Show internal topics/ }).check();
  await expect(rows).toContainText('__consumer_offsets');
});

import { expect, test, type Page } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** Screenshots to look at by eye; the folder is gitignored. */
const SHOTS = fileURLToPath(new URL('../e2e-screenshots/', import.meta.url));

async function dropFromPalette(page: Page, label: string, x: number, y: number): Promise<void> {
  const canvas = page.getByRole('main', { name: 'Canvas' });
  await page
    .getByRole('complementary', { name: 'Palette' })
    .getByRole('button', { name: label })
    .dragTo(canvas, { targetPosition: { x, y } });
}

const node = (page: Page, id: string) => page.locator(`.react-flow__node[data-id="${id}"]`);
const panel = (page: Page) => page.getByTestId('properties-panel');

test.use({ viewport: { width: 1440, height: 900 } });

test('select a topic and add a partition: the node gets a new row', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/?scene=empty');
  await expect(panel(page)).toContainText('Select a component on the canvas');

  await dropFromPalette(page, 'Topic', 420, 220);
  await node(page, 't1').click();
  await expect(panel(page).getByRole('heading', { name: 't1' })).toBeVisible();
  await expect(node(page, 't1').locator('[data-partition-row]')).toHaveCount(1);

  const minus = panel(page).getByRole('button', { name: 'One partition fewer' });
  await expect(minus).toBeDisabled();
  await panel(page).getByRole('button', { name: 'One partition more' }).click();
  await panel(page).getByRole('button', { name: 'One partition more' }).click();
  await expect(panel(page).getByRole('spinbutton', { name: 'Partitions' })).toHaveValue('3');
  await panel(page).getByRole('button', { name: 'Add 2 partitions' }).click();

  await expect(node(page, 't1').locator('[data-partition-row]')).toHaveCount(3);
  const box = await node(page, 't1').boundingBox();
  expect(box?.height).toBe(36 + 3 * 48);
  await expect(panel(page).getByRole('group', { name: 'Topic stats' })).toContainText(
    'Partitions3',
  );
  await expect(minus).toBeDisabled();

  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}properties-topic.png` });
  expect(errors).toEqual([]);
});

test('edit a consumer field; read-only and not-modelled fields explain themselves', async ({
  page,
}) => {
  await page.goto('/?scene=empty');
  await dropFromPalette(page, 'Consumer', 480, 260);
  await node(page, 'c1').click();
  await expect(panel(page).getByRole('heading', { name: 'c1' })).toBeVisible();

  const wait = panel(page).getByLabel('Fetch max wait', { exact: true });
  await wait.fill('-5');
  await expect(panel(page)).toContainText('fetch.max.wait.ms cannot be negative.');
  await wait.fill('100');
  await wait.press('Enter');
  await expect(wait).toHaveValue('100');
  await expect(wait).toHaveAttribute('aria-invalid', 'false');

  const group = panel(page).getByLabel('Group', { exact: true });
  await expect(group).toHaveAttribute('readonly', '');
  await expect(panel(page)).toContainText('Consumer groups arrive in M3');
  await expect(panel(page).getByLabel('Session timeout', { exact: true })).toBeDisabled();

  // The (i) tooltip on keyboard focus.
  await panel(page).getByRole('img', { name: 'About Fetch max wait' }).focus();
  await expect(page.getByRole('tooltip')).toContainText('Kafka default: 500 ms');

  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}properties-consumer.png` });

  // The change reached the engine: the event list shows its ConfigUpdated event, and
  // re-selecting the consumer shows the stored value.
  await expect(
    page.getByTestId('event-list').locator('li', { hasText: 'ConfigUpdated' }),
  ).toContainText(/consumer c1\s+fetchMaxWaitMs: 500 → 100/);
  await page.getByRole('main', { name: 'Canvas' }).click({ position: { x: 40, y: 400 } });
  await expect(panel(page)).toContainText('Select a component on the canvas');
  await node(page, 'c1').click();
  await expect(panel(page).getByLabel('Fetch max wait', { exact: true })).toHaveValue('100');
});

test('a producer shows game-setting chips', async ({ page }) => {
  await page.goto('/?scene=empty');
  await dropFromPalette(page, 'Producer', 160, 260);
  await node(page, 'p1').click();
  const rate = panel(page).locator('[data-field="ratePerSec"]');
  await expect(rate.locator('[data-chip="game-setting"]')).toHaveText('game setting');
  await rate.locator('[data-chip="game-setting"]').focus();
  await expect(page.getByRole('tooltip')).toContainText('Stands in for how often');
  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}properties-producer.png` });
});

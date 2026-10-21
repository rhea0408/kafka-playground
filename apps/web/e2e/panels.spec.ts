import { expect, test, type Locator, type Page } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** Screenshots to look at by eye; the folder is gitignored. */
const SHOTS = fileURLToPath(new URL('../e2e-screenshots/', import.meta.url));

async function centre(locator: Locator): Promise<{ x: number; y: number }> {
  const b = await locator.boundingBox();
  if (b === null) throw new Error('Element is not visible');
  return { x: b.x + b.width / 2, y: b.y + b.height / 2 };
}

async function dropFromPalette(page: Page, label: string, x: number, y: number): Promise<void> {
  const canvas = page.getByRole('main', { name: 'Canvas' });
  await page
    .getByRole('complementary', { name: 'Palette' })
    .getByRole('button', { name: label })
    .dragTo(canvas, { targetPosition: { x, y } });
}

async function connect(page: Page, from: Locator, to: Locator): Promise<void> {
  const a = await centre(from);
  const b = await centre(to);
  await page.mouse.move(a.x, a.y);
  await page.mouse.down();
  await page.mouse.move((a.x + b.x) / 2, (a.y + b.y) / 2, { steps: 5 });
  await page.mouse.move(b.x, b.y, { steps: 5 });
  await page.mouse.up();
}

const node = (page: Page, id: string) => page.locator(`.react-flow__node[data-id="${id}"]`);
const handle = (page: Page, id: string, handleId: string) =>
  node(page, id).locator(`.react-flow__handle[data-handleid="${handleId}"]`);
const props = (page: Page) => page.getByTestId('properties-panel');

/** Producer → topic (with `partitions`) → consumer, playing. */
async function build(page: Page, partitions: number): Promise<void> {
  await page.goto('/?scene=empty');
  await dropFromPalette(page, 'Producer', 120, 300);
  await dropFromPalette(page, 'Topic', 400, 300);
  await dropFromPalette(page, 'Consumer', 700, 300);
  await expect(node(page, 'c1')).toBeVisible();
  await connect(page, handle(page, 'p1', 'out'), handle(page, 't1', 'in'));
  await connect(page, handle(page, 't1', 'out'), handle(page, 'c1', 'row:c1'));
  await expect(page.locator('.react-flow__edge')).toHaveCount(2);
  if (partitions > 1) {
    await node(page, 't1').click();
    const more = props(page).getByRole('button', { name: 'One partition more' });
    for (let i = 1; i < partitions; i++) await more.click();
    await props(page)
      .getByRole('button', { name: `Add ${String(partitions - 1)} partitions` })
      .click();
    await expect(node(page, 't1').locator('[data-partition-row]')).toHaveCount(partitions);
  }
  await page.getByRole('button', { name: 'Play' }).click();
}

async function produceD(page: Page): Promise<void> {
  await node(page, 'p1').click();
  await props(page)
    .getByLabel(/^Record key/)
    .fill('D');
  await props(page).getByLabel('Record value').fill('manual-D');
  await props(page).getByRole('button', { name: 'Send record' }).click();
  await expect(props(page).getByTestId('mp-deliveries')).toContainText(/t1-0, offset \d+\./);
}

const keyDRow = (page: Page) =>
  page
    .getByTestId('partition-log')
    .locator('tr[data-offset]')
    .filter({ has: page.locator('td', { hasText: /^manual-D$/ }) });

test.use({ viewport: { width: 1440, height: 900 } });

test('manual record with key D lands in partition 0 of a 3-partition topic', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await build(page, 3);
  await produceD(page);

  await page.getByRole('tab', { name: 'Partition log' }).click();
  const log = page.getByTestId('partition-log');
  await log.getByLabel('Topic', { exact: true }).selectOption('t1');
  await log.getByLabel('Partition', { exact: true }).selectOption('0');
  await expect(keyDRow(page)).toHaveCount(1);
  await expect(keyDRow(page).locator('td').nth(1)).toHaveText('D');
  await expect(log.locator('[data-marker="hw"]')).toBeVisible();
  await expect(log.locator('[data-marker="leo"]')).toBeVisible();
  // Wait for an auto-commit (5 s) so the committed marker shows too.
  await expect(log.locator('[data-marker="committed"]')).toBeVisible({ timeout: 10_000 });

  // Event list: the record's events are there, newest at the bottom, following.
  const events = page.getByTestId('event-list');
  await expect(events.getByRole('button', { name: 'Follow' })).toHaveAttribute(
    'aria-pressed',
    'true',
  );
  await expect(events.locator('li').last()).toBeVisible();
  await page.getByRole('button', { name: 'Pause' }).click();
  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}panels-event-list-partition-log.png` });

  // Filter: hide topology events; the list still shows record events.
  await events.getByRole('button', { name: /^Filter:/ }).click();
  await page.getByRole('checkbox', { name: 'RecordAppended' }).uncheck();
  await expect(events.getByRole('button', { name: 'Filter: 12 of 13 types' })).toBeVisible();
  await page.screenshot({ path: `${SHOTS}panels-event-filter.png` });
  await page.keyboard.press('Escape');
  await expect(events.locator('li', { hasText: 'RecordAppended' })).toHaveCount(0);
  expect(errors).toEqual([]);
});

test('with one partition, key D is simply in partition 0', async ({ page }) => {
  await build(page, 1);
  await produceD(page);
  await page.getByRole('tab', { name: 'Partition log' }).click();
  await expect(
    page.getByTestId('partition-log').getByLabel('Partition', { exact: true }),
  ).toHaveValue('0');
  await expect(keyDRow(page)).toHaveCount(1);
});

test('event list: scrolling up pauses Follow, the toggle resumes it', async ({ page }) => {
  await build(page, 1);
  const events = page.getByTestId('event-list');
  const follow = events.getByRole('button', { name: 'Follow' });
  const list = events.getByRole('region', { name: 'Event list' });
  await expect
    .poll(async () => list.evaluate((el) => el.scrollHeight > el.clientHeight + 100), {
      timeout: 15_000,
    })
    .toBe(true);
  await list.hover();
  await page.mouse.wheel(0, -300);
  await expect(follow).toHaveAttribute('aria-pressed', 'false');
  await follow.click();
  await expect(follow).toHaveAttribute('aria-pressed', 'true');
  await expect
    .poll(() => list.evaluate((el) => el.scrollTop + el.clientHeight >= el.scrollHeight - 2))
    .toBe(true);
});

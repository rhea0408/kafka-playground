import { expect, test, type Page } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { DEFAULT_GROUP } from '../src/stores/default-scene.ts';

/** Consumer groups on the canvas (M3-T9): building a group by dropping consumers onto it. */

const SHOTS = fileURLToPath(new URL('../e2e-screenshots/', import.meta.url));
const GROUP = `group:${DEFAULT_GROUP}`;
const node = (page: Page, id: string) => page.locator(`.react-flow__node[data-id="${id}"]`);

/** Drags the palette's Consumer item onto the centre of the group node. */
async function dropConsumerOnGroup(page: Page): Promise<void> {
  const box = await node(page, GROUP).boundingBox();
  const canvas = await page.getByRole('main', { name: 'Canvas' }).boundingBox();
  if (box === null || canvas === null) throw new Error('not visible');
  await page
    .getByRole('complementary', { name: 'Palette' })
    .getByRole('button', { name: 'Consumer' })
    .dragTo(page.getByRole('main', { name: 'Canvas' }), {
      targetPosition: { x: box.x - canvas.x + box.width / 2, y: box.y - canvas.y + 20 },
    });
}

async function speed10(page: Page): Promise<void> {
  for (const next of ['2×', '5×', '10×']) {
    await page.getByRole('button', { name: /^Speed / }).click();
    await expect(page.getByRole('button', { name: new RegExp(`^Speed ${next}`) })).toBeVisible();
  }
}

test.use({ viewport: { width: 1440, height: 900 } });

test('four members on a three-partition topic: one row is idle', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/');
  await expect(node(page, GROUP)).toBeVisible();
  for (const id of ['c2', 'c3', 'c4']) {
    await dropConsumerOnGroup(page);
    await expect(node(page, GROUP).locator(`[data-consumer-row="${id}"]`)).toBeVisible();
  }
  // One group node, four rows, and the new members read the group's topic: still one edge.
  await expect(page.locator('.react-flow__node[data-id^="group:"]')).toHaveCount(1);
  await expect(page.locator('.react-flow__edge')).toHaveCount(2);

  await speed10(page);
  await page.getByRole('button', { name: 'Play' }).click();
  const idle = node(page, GROUP).locator('[data-slot="idle"]');
  await expect(idle).toHaveCount(1, { timeout: 15_000 });
  await expect(idle).toHaveText('◇Idle: no partition');
  await expect(node(page, GROUP).locator('[data-slot="group-state"]')).toHaveText('✓Stable');
  // The other three members own one partition each.
  await expect(node(page, GROUP).locator('[data-chip]')).toHaveCount(3);
  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}groups-idle-member.png` });
  expect(errors).toEqual([]);
});

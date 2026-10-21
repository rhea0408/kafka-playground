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

/** Drags a palette item onto the canvas at (x, y) relative to the canvas's top-left corner. */
async function dropFromPalette(page: Page, label: string, x: number, y: number): Promise<void> {
  const canvas = page.getByRole('main', { name: 'Canvas' });
  await page
    .getByRole('complementary', { name: 'Palette' })
    .getByRole('button', { name: label })
    .dragTo(canvas, { targetPosition: { x, y } });
}

/** Drags with the mouse from one handle to another, pausing over the target. */
async function dragHandle(page: Page, from: Locator, to: Locator): Promise<void> {
  const a = await centre(from);
  const b = await centre(to);
  await page.mouse.move(a.x, a.y);
  await page.mouse.down();
  await page.mouse.move((a.x + b.x) / 2, (a.y + b.y) / 2, { steps: 5 });
  await page.mouse.move(b.x, b.y, { steps: 5 });
}

const node = (page: Page, id: string) => page.locator(`.react-flow__node[data-id="${id}"]`);
const handle = (page: Page, id: string, handleId: string) =>
  node(page, id).locator(`.react-flow__handle[data-handleid="${handleId}"]`);

test.use({ viewport: { width: 1440, height: 900 } });

test('build producer → topic from the palette and connect them', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/?scene=empty');

  // The Sandbox starts with broker b1.
  await expect(node(page, 'b1')).toBeVisible();
  await expect(node(page, 'b1')).toHaveAttribute('aria-label', 'Broker b1');

  await dropFromPalette(page, 'Producer', 120, 260);
  await dropFromPalette(page, 'Topic', 400, 260);
  await expect(node(page, 'p1')).toBeVisible();
  await expect(node(page, 't1')).toBeVisible();

  // Sizes from the ui-spec.
  const p1 = await node(page, 'p1').boundingBox();
  const t1 = await node(page, 't1').boundingBox();
  expect([p1?.width, p1?.height]).toEqual([150, 72]);
  expect([t1?.width, t1?.height]).toEqual([230, 36 + 48]);

  // Dropped centred on the pointer (snapped to the 16 px grid, so within 8 px).
  const canvasBox = await page.getByRole('main', { name: 'Canvas' }).boundingBox();
  if (canvasBox === null || p1 === null) throw new Error('no box');
  expect(Math.abs(p1.x + p1.width / 2 - (canvasBox.x + 120))).toBeLessThanOrEqual(8);

  await dragHandle(page, handle(page, 'p1', 'out'), handle(page, 't1', 'in'));
  await expect(page.getByTestId('connection-reason')).toHaveCount(0);
  await page.mouse.up();

  const edge = page.locator('.react-flow__edge[data-id="p1→t1"]');
  await expect(edge).toBeVisible();

  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}canvas-producer-topic.png` });

  // Select the edge and delete it with Backspace → Disconnect.
  await edge.locator('.react-flow__edge-interaction').click({ force: true });
  await expect(edge).toHaveClass(/selected/);
  await page.keyboard.press('Backspace');
  await expect(edge).toHaveCount(0);
  await expect(node(page, 'p1')).toBeVisible();
  expect(errors).toEqual([]);
});

test('producer → consumer is refused with its reason', async ({ page }) => {
  await page.goto('/?scene=empty');
  await dropFromPalette(page, 'Producer', 120, 260);
  await dropFromPalette(page, 'Consumer', 480, 260);
  await expect(node(page, 'c1')).toBeVisible();

  await dragHandle(page, handle(page, 'p1', 'out'), handle(page, 'c1', 'row:c1'));
  // While dragging over the consumer: the reason next to the pointer.
  const hint = page.getByTestId('connection-reason');
  await expect(hint).toContainText("A producer can't send straight to a consumer.");
  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}canvas-invalid-connection.png` });
  await page.mouse.up();

  // After the drop: no edge, and the same reason as a warning toast.
  await expect(hint).toHaveCount(0);
  await expect(page.locator('.react-flow__edge')).toHaveCount(0);
  await expect(page.getByRole('status')).toContainText(
    "A producer can't send straight to a consumer.",
  );
});

test('keyboard add, select, move and delete', async ({ page }) => {
  await page.goto('/?scene=empty');
  const producerItem = page
    .getByRole('complementary', { name: 'Palette' })
    .getByRole('button', { name: 'Producer' });
  await producerItem.focus();
  await page.keyboard.press('Enter');
  await expect(node(page, 'p1')).toBeVisible();

  // Drag the node: it lands on the 16 px grid and stays there.
  const before = await centre(node(page, 'p1'));
  await page.mouse.move(before.x, before.y);
  await page.mouse.down();
  await page.mouse.move(before.x + 37, before.y + 21, { steps: 5 });
  await page.mouse.up();
  const transform = await node(page, 'p1').evaluate((el) => (el as HTMLElement).style.transform);
  const [x, y] = /translate\((-?[\d.]+)px, (-?[\d.]+)px\)/.exec(transform)?.slice(1) ?? [];
  expect(Number(x) % 16).toBe(0);
  expect(Number(y) % 16).toBe(0);

  // Arrow keys move the selected node by one grid step; the move is kept.
  await page.keyboard.press('ArrowRight');
  await expect
    .poll(() => node(page, 'p1').evaluate((el) => (el as HTMLElement).style.transform))
    .toBe(`translate(${String(Number(x) + 16)}px, ${String(Number(y))}px)`);
  // Another store change re-derives the nodes from topologyStore: p1 keeps its new position.
  await page
    .getByRole('complementary', { name: 'Palette' })
    .getByRole('button', { name: 'Topic' })
    .click();
  await expect(node(page, 't1')).toBeVisible();
  expect(await node(page, 'p1').evaluate((el) => (el as HTMLElement).style.transform)).toBe(
    `translate(${String(Number(x) + 16)}px, ${String(Number(y))}px)`,
  );

  // Selected (clicked) node is removed with Delete.
  await node(page, 'p1').click();
  await page.keyboard.press('Delete');
  await expect(node(page, 'p1')).toHaveCount(0);
});

test('minimap shows only when content is off-screen; the map toggle overrides', async ({
  page,
}) => {
  await page.goto('/?scene=empty');
  const minimap = page.getByTestId('minimap');
  const nav = page.getByRole('group', { name: 'Canvas navigation' });
  expect(await nav.boundingBox()).toMatchObject({ width: 160, height: 40 });
  await expect(minimap).toHaveAttribute('data-visible', 'false');

  // Zoom in until b1 no longer fits.
  for (let i = 0; i < 4; i++) await nav.getByRole('button', { name: 'Zoom in' }).click();
  await expect(minimap).toHaveAttribute('data-visible', 'true');
  const box = await page.locator('.react-flow__minimap').boundingBox();
  expect([box?.width, box?.height]).toEqual([160, 84]);
  mkdirSync(SHOTS, { recursive: true });
  await page.waitForTimeout(300); // let the 200 ms fade finish before the screenshot
  await page.screenshot({ path: `${SHOTS}canvas-minimap.png` });

  await nav.getByRole('button', { name: 'Fit view' }).click();
  await expect(minimap).toHaveAttribute('data-visible', 'false');

  const toggle = nav.getByRole('button', { name: 'Minimap' });
  await toggle.click();
  await expect(toggle).toHaveAttribute('aria-pressed', 'true');
  await expect(minimap).toHaveAttribute('data-visible', 'true');
});

/** Number in a slot's text after `label`, e.g. "LEO 12 · HW 12" → 12 for "LEO". */
async function numberAfter(locator: Locator, label: string): Promise<number> {
  const text = (await locator.textContent()) ?? '';
  const match = new RegExp(`${label} ([\\d,]+)`).exec(text);
  return match?.[1] === undefined ? Number.NaN : Number(match[1].replaceAll(',', ''));
}

test('live state: LEO grows, the consumer position advances, Kill / Restart from the toolbar', async ({
  page,
}) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/?scene=empty');
  await dropFromPalette(page, 'Producer', 120, 300);
  await dropFromPalette(page, 'Topic', 400, 300);
  await dropFromPalette(page, 'Consumer', 700, 300);
  await expect(node(page, 'c1')).toBeVisible();

  await dragHandle(page, handle(page, 'p1', 'out'), handle(page, 't1', 'in'));
  await page.mouse.up();
  await dragHandle(page, handle(page, 't1', 'out'), handle(page, 'c1', 'row:c1'));
  await page.mouse.up();
  await expect(page.locator('.react-flow__edge')).toHaveCount(2);

  await page.getByRole('button', { name: 'Play' }).click();

  const log = node(page, 't1').locator('[data-partition-row="0"] [data-slot="partition-log"]');
  const offsets = node(page, 'c1').locator('[data-slot="offsets"]');
  await expect.poll(() => numberAfter(log, 'LEO'), { timeout: 10_000 }).toBeGreaterThan(0);
  await expect(node(page, 't1').locator('.kp-cell').first()).toBeVisible();
  await expect(node(page, 't1').getByLabel('Leader: broker b1')).toBeVisible();
  await expect(node(page, 'p1').locator('[data-stat="sent"]')).not.toHaveText('0');

  await expect.poll(() => numberAfter(offsets, 'pos'), { timeout: 10_000 }).toBeGreaterThan(0);
  const first = await numberAfter(offsets, 'pos');
  await expect.poll(() => numberAfter(offsets, 'pos'), { timeout: 10_000 }).toBeGreaterThan(first);
  await expect(node(page, 'c1').getByText('running')).toBeVisible();

  mkdirSync(SHOTS, { recursive: true });
  await node(page, 'c1').hover();
  const kill = page.getByRole('button', { name: 'Kill consumer c1' });
  await expect(kill).toBeVisible();
  await page.screenshot({ path: `${SHOTS}canvas-live-state.png` });

  await kill.click();
  await expect(node(page, 'c1').getByText('stopped')).toBeVisible();
  await page.getByRole('button', { name: 'Restart consumer c1' }).click();
  await expect(node(page, 'c1').getByText('running')).toBeVisible();
  expect(errors).toEqual([]);
});

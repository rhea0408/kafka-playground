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

/** `window.__kp.overlay` (apps/web/src/canvas/overlay/debug.ts), as the test reads it. */
interface OverlayCounters {
  dotsDrawn: number;
  dotsOnScreen: number;
  dotsActive: number;
  dotsQueued: number;
  dotsStarted: number;
  skippedCap: number;
  skippedNoA1: number;
  fps: number;
  capChip: boolean;
}
type OverlayWindow = {
  __kp?: { overlay?: OverlayCounters & { spawnTestDots?: (count: number) => number } };
};

const counters = (page: Page): Promise<OverlayCounters> =>
  page.evaluate(() => {
    const o = (window as unknown as OverlayWindow).__kp?.overlay;
    if (o === undefined) throw new Error('no overlay counters');
    const { spawnTestDots: _hook, ...rest } = o;
    return rest;
  });

async function buildFlow(page: Page, withConsumer: boolean, topicX = 480): Promise<void> {
  await dropFromPalette(page, 'Producer', 100, 300);
  await dropFromPalette(page, 'Topic', topicX, 300);
  await expect(node(page, 't1')).toBeVisible();
  await connect(page, handle(page, 'p1', 'out'), handle(page, 't1', 'in'));
  if (withConsumer) {
    await dropFromPalette(page, 'Consumer', 820, 300);
    await expect(node(page, 'c1')).toBeVisible();
    await connect(page, handle(page, 't1', 'out'), handle(page, 'c1', 'row:c1'));
  }
  await expect(page.locator('.react-flow__edge')).toHaveCount(withConsumer ? 2 : 1);
}

test.use({ viewport: { width: 1440, height: 900 } });

test('message dots: Play draws A1 and A2 dots on the overlay', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/');
  await expect(page.getByTestId('message-overlay')).toBeAttached();
  await expect(page.getByTestId('overlay-legend')).toHaveText(
    /Dots are slowed down so you can see them\. Real delivery takes a few milliseconds\./,
  );
  await buildFlow(page, true);
  expect((await counters(page)).dotsDrawn).toBe(0);

  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(async () => (await counters(page)).dotsDrawn, { timeout: 10_000 })
    .toBeGreaterThan(0);
  // A2 dots start after their A1 dots arrive: more dots than records produced so far means some
  // partition → consumer dots ran too.
  await expect
    .poll(async () => (await counters(page)).dotsStarted, { timeout: 15_000 })
    .toBeGreaterThan(3);
  await expect
    .poll(async () => (await counters(page)).dotsOnScreen, { timeout: 5_000 })
    .toBeGreaterThan(0);
  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}overlay-dots.png` });

  // Pause freezes the dots: two pictures of the overlay's own pixels a moment apart are identical.
  // Wait for the paused frame first (the button turns back into Play): the click returns before
  // the worker stops, so a record can still be appended just after it. Read the canvas bitmap,
  // not an element screenshot: a screenshot also shows the nodes under the transparent canvas,
  // and a new log cell's A4 slide-in (250 ms, wall-clock, animations.md) may still be running.
  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
  const overlayPixels = (): Promise<string> =>
    page
      .getByTestId('message-overlay')
      .evaluate((canvas) => (canvas as HTMLCanvasElement).toDataURL());
  const before = await overlayPixels();
  await page.waitForTimeout(400);
  expect((await overlayPixels()) === before, 'overlay pixels changed while paused').toBe(true);
  expect((await counters(page)).dotsActive).toBeGreaterThan(0);

  // Reset (a full frame) clears them.
  await page.getByRole('button', { name: 'Reset' }).click();
  await expect.poll(async () => (await counters(page)).dotsActive).toBe(0);
  expect((await counters(page)).skippedCap).toBe(0);
  expect(errors).toEqual([]);
});

test('500 dots: cap, "too many to animate" chip and the FPS meter (?fps)', async ({ page }) => {
  await page.goto('/?fps');
  // A long edge (~500 px, ~4 s per dot at 1×) so the dots are still moving when FPS is read.
  await buildFlow(page, false, 700);
  await expect(page.getByTestId('overlay-fps')).toBeVisible();

  // Not playing: the dots stay where they were spawned, so every frame draws all of them.
  const started = await page.evaluate(
    () => (window as unknown as OverlayWindow).__kp?.overlay?.spawnTestDots?.(520) ?? -1,
  );
  expect(started).toBe(500);
  await expect(page.getByTestId('overlay-cap-chip')).toBeVisible();

  await page.waitForTimeout(1500);
  const frozen = await counters(page);
  expect(frozen.dotsActive).toBe(500);
  expect(frozen.dotsOnScreen).toBe(500);
  mkdirSync(SHOTS, { recursive: true });
  await page.screenshot({ path: `${SHOTS}overlay-500-dots.png` });

  // Moving: Play at 1× (the producer's own dots are skipped while the cap is full).
  await page.getByRole('button', { name: 'Play' }).click();
  await page.waitForTimeout(1000);
  const moving = await counters(page);
  // The chip hides 1 s after the last skip once there is room again.
  test.info().annotations.push({
    type: 'fps',
    description: `frozen 500 dots: ${String(frozen.fps)} fps; moving ${String(moving.dotsOnScreen)} dots: ${String(moving.fps)} fps (headless Chromium, not target hardware)`,
  });
  console.log(test.info().annotations.at(-1)?.description);
  expect(frozen.fps).toBeGreaterThan(0);
});

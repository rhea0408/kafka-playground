import { expect, test, type Locator, type Page } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** Screenshots for comparing with wireframe 05 by eye; the folder is gitignored. */
const SHOTS = fileURLToPath(new URL('../e2e-screenshots/', import.meta.url));

interface Box {
  x: number;
  y: number;
  width: number;
  height: number;
}

async function box(locator: Locator): Promise<Box> {
  const b = await locator.boundingBox();
  if (b === null) throw new Error('Element is not visible');
  return b;
}

function expectBox(actual: Box, expected: Box): void {
  // Sub-pixel tolerance only (1.5 px borders); the sizes must match the ui-spec exactly.
  for (const key of ['x', 'y', 'width', 'height'] as const) {
    expect(actual[key], key).toBeCloseTo(expected[key], 0);
  }
}

function regions(page: Page) {
  return {
    top: page.getByRole('banner'),
    palette: page.getByRole('complementary', { name: 'Palette' }),
    canvas: page.getByRole('main', { name: 'Canvas' }),
    bottom: page.locator('[role="tablist"][aria-label="Bottom panel"]').locator('..'),
    right: page.locator('[role="tablist"][aria-label="Right panel"]').locator('..'),
  };
}

// ui-spec "Level screen layout" and wireframe 05. Reference 1440 × 900, minimum 1366 × 768.
for (const { width, height } of [
  { width: 1440, height: 900 },
  { width: 1366, height: 768 },
]) {
  test.describe(`sandbox layout at ${String(width)} × ${String(height)}`, () => {
    test.use({ viewport: { width, height } });

    test('panel sizes match the ui-spec and nothing scrolls horizontally', async ({ page }) => {
      await page.goto('/');
      const r = regions(page);
      const canvasW = width - 240 - 360;
      const canvasH = height - 56 - 200;

      expectBox(await box(r.top), { x: 0, y: 0, width, height: 56 });
      expectBox(await box(r.palette), { x: 0, y: 56, width: 240, height: height - 56 });
      expectBox(await box(r.canvas), { x: 240, y: 56, width: canvasW, height: canvasH });
      expectBox(await box(r.bottom), { x: 240, y: 56 + canvasH, width: canvasW, height: 200 });
      expectBox(await box(r.right), { x: 240 + canvasW, y: 56, width: 360, height: height - 56 });

      // Tab bars: 40 (right), 36 (bottom), as in wireframe 05.
      expect((await box(page.getByRole('tablist', { name: 'Right panel' }))).height).toBe(40);
      expect((await box(page.getByRole('tablist', { name: 'Bottom panel' }))).height).toBe(36);

      // Palette item shells 208 × 56.
      for (const item of await page.getByTestId('palette-item').all()) {
        const b = await box(item);
        expect([b.width, b.height]).toEqual([208, 56]);
      }

      // Sim controls: 40 × 40 (speed 56 × 40), gap 8, centred, 8 px from the right edge.
      const buttons = await page
        .getByRole('group', { name: 'Simulation controls' })
        .getByRole('button')
        .all();
      const boxes = await Promise.all(buttons.map(box));
      expect(boxes.map((b) => [b.width, b.height])).toEqual([
        [40, 40],
        [40, 40],
        [56, 40],
        [40, 40],
      ]);
      for (const b of boxes) expect(b.y + b.height / 2).toBe(28);
      for (let i = 1; i < boxes.length; i++) {
        const prev = boxes[i - 1] as Box;
        expect((boxes[i] as Box).x - (prev.x + prev.width)).toBe(8);
      }
      const last = boxes[boxes.length - 1] as Box;
      expect(width - (last.x + last.width)).toBe(8);

      const scroll = await page.evaluate(() => {
        const el = document.scrollingElement;
        return el === null ? null : { sw: el.scrollWidth, cw: el.clientWidth };
      });
      expect(scroll).not.toBeNull();
      expect(scroll?.sw).toBeLessThanOrEqual(scroll?.cw ?? 0);

      mkdirSync(SHOTS, { recursive: true });
      await page.screenshot({ path: `${SHOTS}sandbox-${String(width)}x${String(height)}.png` });
    });
  });
}

test('Play advances the sim clock, Pause stops it, Reset returns to zero', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/');
  const clock = page.getByRole('timer', { name: 'Sim time' });
  await expect(clock).toHaveText('00:00.000');

  await page.getByRole('button', { name: 'Play' }).click();
  // The label says what the button will do next.
  await expect(page.getByRole('button', { name: 'Pause' })).toBeVisible();
  await expect(clock).not.toHaveText('00:00.000');

  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
  const paused = await clock.textContent();
  await page.waitForTimeout(300);
  await expect(clock).toHaveText(paused ?? '');

  await page.getByRole('button', { name: /^Speed 1×/ }).click();
  await expect(page.getByRole('button', { name: /^Speed 2×/ })).toBeVisible();

  await page.getByRole('button', { name: /^Reset/ }).click();
  await expect(clock).toHaveText('00:00.000');
  expect(errors).toEqual([]);
});

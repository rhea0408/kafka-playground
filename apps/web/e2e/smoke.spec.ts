import { expect, test } from '@playwright/test';

test('loads the app shell with the ui-spec theme', async ({ page }) => {
  // Uncaught errors in the page (e.g. the sim worker bootstrap) fail the test.
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/');
  await expect(page).toHaveTitle('Kafka Playground');
  await expect(page.getByRole('heading', { level: 1 })).toContainText('Kafka Playground');

  // The color tokens from docs/ui/ui-spec.md are exposed as CSS variables.
  const primary = await page.evaluate(() =>
    getComputedStyle(document.documentElement).getPropertyValue('--color-primary').trim(),
  );
  expect(primary).toBe('#0d6efd');

  expect(errors).toEqual([]);
});

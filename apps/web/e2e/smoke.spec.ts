import { expect, test } from '@playwright/test';

test('loads the app shell with the ui-spec theme', async ({ page }) => {
  await page.goto('/');
  await expect(page).toHaveTitle('Kafka Playground');
  await expect(page.getByRole('banner')).toContainText('Kafka Playground');

  // The color tokens from docs/ui/ui-spec.md are exposed as CSS variables.
  const primary = await page.evaluate(() =>
    getComputedStyle(document.documentElement).getPropertyValue('--color-primary').trim(),
  );
  expect(primary).toBe('#0d6efd');
});

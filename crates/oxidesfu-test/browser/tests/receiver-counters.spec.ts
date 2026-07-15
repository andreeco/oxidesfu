import { expect, test } from '@playwright/test';

type ReceiverSample = {
  packetsReceived: number;
  framesDecoded: number;
  trackId: string;
  pcId: string;
};

test('final adaptive low request keeps the active Firefox receiver advancing', async ({ page }) => {
  test.skip(
    !process.env.OXIDESFU_BROWSER_HARNESS_URL,
    'Set OXIDESFU_BROWSER_HARNESS_URL to the local browser conformance harness.',
  );

  await page.goto('/');
  await expect(page.getByTestId('browser-harness-ready')).toBeVisible();

  const first = await page.evaluate(async () => {
    return (window as typeof window & {
      oxidesfuReceiverSample: () => Promise<ReceiverSample>;
    }).oxidesfuReceiverSample();
  });
  await page.waitForTimeout(5_000);
  const second = await page.evaluate(async () => {
    return (window as typeof window & {
      oxidesfuReceiverSample: () => Promise<ReceiverSample>;
    }).oxidesfuReceiverSample();
  });

  expect(second.pcId).toBe(first.pcId);
  expect(second.trackId).toBe(first.trackId);
  expect(second.packetsReceived).toBeGreaterThan(first.packetsReceived);
  expect(second.framesDecoded).toBeGreaterThan(first.framesDecoded);
});

import { createHmac, randomUUID } from 'node:crypto';
import { expect, test } from '@playwright/test';

type ReceiverSample = {
  packetsReceived: number;
  framesDecoded: number;
  trackId: string;
  pcId: string;
};

function token(identity: string, room: string): string {
  const key = process.env.OXIDESFU_API_KEY;
  const secret = process.env.OXIDESFU_API_SECRET;
  if (!key || !secret) throw new Error('Missing OxideSFU API credentials');
  const now = Math.floor(Date.now() / 1000);
  const encode = (value: object) => Buffer.from(JSON.stringify(value)).toString('base64url');
  const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
    iss: key,
    sub: identity,
    iat: now,
    exp: now + 300,
    video: { roomJoin: true, room, canPublish: true, canSubscribe: true },
  })}`;
  return `${unsigned}.${createHmac('sha256', secret).update(unsigned).digest('base64url')}`;
}

const hasServerCredentials = Boolean(
  process.env.OXIDESFU_URL && process.env.OXIDESFU_API_KEY && process.env.OXIDESFU_API_SECRET,
);
test.skip(!hasServerCredentials, 'Set OXIDESFU_URL, OXIDESFU_API_KEY, and OXIDESFU_API_SECRET.');

async function waitForHarnessReady(page: import('@playwright/test').Page, label: string, timeoutMs = 30_000) {
  const status = page.getByTestId('browser-harness-ready');
  const deadline = Date.now() + timeoutMs;
  let lastStatus = '';

  while (Date.now() < deadline) {
    lastStatus = (await status.textContent())?.trim() ?? '';
    if (lastStatus === 'ready') return;
    if (lastStatus.startsWith('error:') || lastStatus.startsWith('disconnected:')) {
      throw new Error(`${label} harness failed before ready: ${lastStatus}`);
    }
    await page.waitForTimeout(250);
  }

  throw new Error(
    `${label} harness did not become ready within ${timeoutMs}ms (last status: ${lastStatus || 'empty'})`,
  );
}

test('final adaptive low request keeps the active Firefox receiver advancing', async ({ browser }) => {
  const serverUrl = process.env.OXIDESFU_URL;

  const room = `browser-adaptive-${randomUUID()}`;
  // Firefox does not support Playwright's context-level camera permission API.
  // The project-level media.navigator.* preferences provide fake media access.
  const publisherContext = await browser.newContext();
  const subscriberContext = await browser.newContext();
  const publisher = await publisherContext.newPage();
  const subscriber = await subscriberContext.newPage();
  const publisherUrl = `/?role=publisher&url=${encodeURIComponent(serverUrl!)}&token=${encodeURIComponent(token('browser-publisher', room))}`;
  const subscriberUrl = `/?role=subscriber&url=${encodeURIComponent(serverUrl!)}&token=${encodeURIComponent(token('browser-subscriber', room))}`;

  await publisher.goto(publisherUrl);
  await waitForHarnessReady(publisher, 'publisher');
  await subscriber.goto(subscriberUrl);
  await waitForHarnessReady(subscriber, 'subscriber');
  await expect(subscriber.getByTestId('remote-video')).toHaveJSProperty('srcObject', expect.anything());

  await subscriber.evaluate(() => {
    window.oxidesfuSetQuality('high');
    window.oxidesfuSetQuality('low');
    window.oxidesfuSetQuality('high');
    window.oxidesfuSetQuality('low');
  });
  await subscriber.waitForTimeout(250);

  const first = await subscriber.evaluate(() => window.oxidesfuReceiverSample()) as ReceiverSample;
  await subscriber.waitForTimeout(5_000);
  const second = await subscriber.evaluate(() => window.oxidesfuReceiverSample()) as ReceiverSample;

  expect(second.pcId).toBe(first.pcId);
  expect(second.trackId).toBe(first.trackId);
  expect(second.packetsReceived).toBeGreaterThan(first.packetsReceived);
  expect(second.framesDecoded).toBeGreaterThan(first.framesDecoded);

  await publisher.evaluate(() => window.oxidesfuClose());
  await subscriber.evaluate(() => window.oxidesfuClose());
  await publisherContext.close();
  await subscriberContext.close();
});

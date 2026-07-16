import { createHmac, randomUUID } from 'node:crypto';
import { expect, test } from '@playwright/test';

type ReceiverSample = {
  packetsReceived: number;
  framesDecoded: number;
  codec: string;
  trackId: string;
  pcId: string;
};

type DataChannelSample = {
  pcId: string;
  origin: 'local' | 'remote';
  label: string;
  readyState: string;
  bufferedAmount: number;
  ordered: boolean;
};

type PeerConnectionSample = {
  pcId: string;
  connectionState: string;
  iceConnectionState: string;
};

type SessionDescriptionSample = {
  pcId: string;
  direction: 'local' | 'remote';
  type: string | null;
  sections: Array<{
    media: string;
    mid?: string;
    direction?: string;
    setup?: string;
    hasIceCredentials: boolean;
    candidateCount: number;
    hasEndOfCandidates: boolean;
    hasSctpPort: boolean;
  }>;
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

async function waitForReliableDataChannelOpen(
  page: import('@playwright/test').Page,
  label: string,
  timeoutMs = 10_000,
  origin: 'local' | 'remote' = 'local',
) {
  const deadline = Date.now() + timeoutMs;
  let latest: DataChannelSample[] = [];

  while (Date.now() < deadline) {
    latest = await page.evaluate(() => window.oxidesfuDataChannelSample()) as DataChannelSample[];
    if (latest.some((channel) => channel.origin === origin && channel.label === '_reliable' && channel.readyState === 'open')) {
      return;
    }
    await page.waitForTimeout(250);
  }

  const descriptions = await page.evaluate(() => window.oxidesfuSessionDescriptionSample()) as SessionDescriptionSample[];
  throw new Error(`${label} did not open ${origin} _reliable within ${timeoutMs}ms: channels=${JSON.stringify(latest)}, descriptions=${JSON.stringify(descriptions)}`);
}

async function waitForPeerConnectionCount(
  page: import('@playwright/test').Page,
  label: string,
  expectedCount: number,
  timeoutMs = 10_000,
) {
  const deadline = Date.now() + timeoutMs;
  let latest: PeerConnectionSample[] = [];

  while (Date.now() < deadline) {
    latest = await page.evaluate(() => window.oxidesfuPeerConnectionSample()) as PeerConnectionSample[];
    if (latest.length >= expectedCount) {
      return;
    }
    await page.waitForTimeout(250);
  }

  throw new Error(`${label} did not create ${expectedCount} peer connections within ${timeoutMs}ms: ${JSON.stringify(latest)}`);
}

async function waitForReceiverSample(
  page: import('@playwright/test').Page,
  label: string,
  timeoutMs = 10_000,
): Promise<ReceiverSample> {
  const deadline = Date.now() + timeoutMs;
  let lastError = '';

  while (Date.now() < deadline) {
    try {
      return await page.evaluate(() => window.oxidesfuReceiverSample()) as ReceiverSample;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      await page.waitForTimeout(250);
    }
  }

  throw new Error(`${label} did not produce inbound video RTP within ${timeoutMs}ms: ${lastError}`);
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
  await expect.poll(
    () => subscriber.evaluate(() => document.querySelector('video[data-testid="remote-video"]')?.srcObject !== null),
  ).toBe(true);

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

test('Firefox VP9 SVC receiver keeps decoding after adaptive quality churn', async ({ browser }) => {
  const serverUrl = process.env.OXIDESFU_URL;
  const room = `browser-vp9-svc-${randomUUID()}`;
  const publisherContext = await browser.newContext();
  const subscriberContext = await browser.newContext();
  const publisher = await publisherContext.newPage();
  const subscriber = await subscriberContext.newPage();
  const publisherUrl = `/?role=publisher&codec=vp9&scalabilityMode=L3T3_KEY&url=${encodeURIComponent(serverUrl!)}&token=${encodeURIComponent(token('browser-vp9-publisher', room))}`;
  const subscriberUrl = `/?role=subscriber&url=${encodeURIComponent(serverUrl!)}&token=${encodeURIComponent(token('browser-vp9-subscriber', room))}`;

  try {
    await publisher.goto(publisherUrl);
    await waitForHarnessReady(publisher, 'VP9 publisher');
    await subscriber.goto(subscriberUrl);
    await waitForHarnessReady(subscriber, 'VP9 subscriber');
    await expect.poll(
      () => subscriber.evaluate(() => document.querySelector('video[data-testid="remote-video"]')?.srcObject !== null),
    ).toBe(true);

    await expect.poll(
      () => publisher.evaluate(() => window.oxidesfuPublisherSample().then((sample) => sample.codec)),
    ).toBe('video/vp9');
    expect(
      await publisher.evaluate(() => window.oxidesfuPublisherSample().then((sample) => sample.requestedScalabilityMode)),
    ).toBe('L3T3_KEY');

    await subscriber.evaluate(() => {
      window.oxidesfuSetQuality('high');
      window.oxidesfuSetQuality('low');
      window.oxidesfuSetQuality('high');
      window.oxidesfuSetQuality('low');
    });
    await subscriber.waitForTimeout(250);

    const first = await waitForReceiverSample(subscriber, 'VP9 subscriber');
    await subscriber.waitForTimeout(5_000);
    const second = await waitForReceiverSample(subscriber, 'VP9 subscriber');

    expect(second.pcId).toBe(first.pcId);
    expect(second.trackId).toBe(first.trackId);
    expect(second.codec).toBe('video/vp9');
    expect(second.packetsReceived).toBeGreaterThan(first.packetsReceived);
    expect(second.framesDecoded).toBeGreaterThan(first.framesDecoded);
  } finally {
    await publisherContext.close();
    await subscriberContext.close();
  }
});

test('Meet-style chat delivery keeps the active Firefox video receiver advancing', async ({ browser }) => {
  const serverUrl = process.env.OXIDESFU_URL;
  const room = `browser-chat-video-${randomUUID()}`;
  const publisherContext = await browser.newContext();
  const subscriberContext = await browser.newContext();
  const publisher = await publisherContext.newPage();
  const subscriber = await subscriberContext.newPage();
  const publisherUrl = `/?role=publisher&url=${encodeURIComponent(serverUrl!)}&token=${encodeURIComponent(token('browser-chat-publisher', room))}`;
  const subscriberUrl = `/?role=publisher&url=${encodeURIComponent(serverUrl!)}&token=${encodeURIComponent(token('browser-chat-subscriber', room))}`;
  const message = `chat-${randomUUID()}`;

  await publisher.goto(publisherUrl);
  await waitForHarnessReady(publisher, 'publisher');
  await subscriber.goto(subscriberUrl);
  await waitForHarnessReady(subscriber, 'subscriber');
  await expect.poll(
    () => subscriber.evaluate(() => document.querySelector('video[data-testid="remote-video"]')?.srcObject !== null),
  ).toBe(true);
  await waitForReliableDataChannelOpen(publisher, 'publisher');
  await waitForReliableDataChannelOpen(subscriber, 'subscriber');

  const before = await subscriber.evaluate(() => window.oxidesfuReceiverSample()) as ReceiverSample;
  await publisher.evaluate((message) => window.oxidesfuSendChatMessage(message), message);
  await expect.poll(
    () => subscriber.evaluate(() => window.oxidesfuReceivedChatMessages()),
  ).toContain(message);

  let previous = before;
  for (const delayMs of [1_000, 2_000, 2_000]) {
    await subscriber.waitForTimeout(delayMs);
    const after = await subscriber.evaluate(() => window.oxidesfuReceiverSample()) as ReceiverSample;

    expect(after.pcId).toBe(before.pcId);
    expect(after.trackId).toBe(before.trackId);
    expect(after.packetsReceived).toBeGreaterThan(previous.packetsReceived);
    expect(after.framesDecoded).toBeGreaterThan(previous.framesDecoded);
    previous = after;
  }

  await publisher.evaluate(() => window.oxidesfuClose());
  await subscriber.evaluate(() => window.oxidesfuClose());
  await publisherContext.close();
  await subscriberContext.close();
});

test('dual-PC chat delivery keeps the active Firefox video receiver advancing', async ({ browser }) => {
  const serverUrl = process.env.OXIDESFU_URL;
  const room = `browser-dual-pc-chat-video-${randomUUID()}`;
  const publisherContext = await browser.newContext();
  const subscriberContext = await browser.newContext();
  const publisher = await publisherContext.newPage();
  const subscriber = await subscriberContext.newPage();
  const publisherUrl = `/?role=publisher&singlePeerConnection=false&url=${encodeURIComponent(serverUrl!)}&token=${encodeURIComponent(token('browser-dual-pc-publisher', room))}`;
  const subscriberUrl = `/?role=publisher&singlePeerConnection=false&url=${encodeURIComponent(serverUrl!)}&token=${encodeURIComponent(token('browser-dual-pc-subscriber', room))}`;
  const message = `dual-pc-chat-${randomUUID()}`;

  try {
    await publisher.goto(publisherUrl);
    await waitForHarnessReady(publisher, 'dual-PC publisher');
    await subscriber.goto(subscriberUrl);
    await waitForHarnessReady(subscriber, 'dual-PC subscriber');
    await waitForPeerConnectionCount(publisher, 'dual-PC publisher', 2);
    await waitForPeerConnectionCount(subscriber, 'dual-PC subscriber', 2);
    await expect.poll(
      () => subscriber.evaluate(() => document.querySelector('video[data-testid="remote-video"]')?.srcObject !== null),
    ).toBe(true);
    await waitForReliableDataChannelOpen(subscriber, 'dual-PC subscriber', 10_000, 'remote');

    const before = await waitForReceiverSample(subscriber, 'dual-PC subscriber');
    const sendChat = publisher.evaluate((message) => window.oxidesfuSendChatMessage(message), message);
    await waitForReliableDataChannelOpen(publisher, 'dual-PC publisher');
    await sendChat;
    await expect.poll(
      () => subscriber.evaluate(() => window.oxidesfuReceivedChatMessages()),
    ).toContain(message);

    let previous = before;
    for (const delayMs of [1_000, 2_000, 2_000]) {
      await subscriber.waitForTimeout(delayMs);
      const after = await subscriber.evaluate(() => window.oxidesfuReceiverSample()) as ReceiverSample;

      expect(after.pcId).toBe(before.pcId);
      expect(after.trackId).toBe(before.trackId);
      expect(after.packetsReceived).toBeGreaterThan(previous.packetsReceived);
      expect(after.framesDecoded).toBeGreaterThan(previous.framesDecoded);
      previous = after;
    }
  } finally {
    await publisherContext.close();
    await subscriberContext.close();
  }
});

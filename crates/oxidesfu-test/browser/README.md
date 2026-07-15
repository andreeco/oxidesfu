# Firefox browser conformance harness

This opt-in Playwright project tests the browser-visible media contract that native Rust SDK probes cannot cover: the active Firefox receiver must keep receiving and decoding frames after adaptive-stream settings churn.

## Install

```bash
cd crates/oxidesfu-test/browser
npm install
npm run install:browsers
```

`@playwright/test` is pinned to `1.61.1`. Playwright is intentionally isolated from the Rust workspace and does not run from `cargo test`.

## Required harness contract

The bundled Vite page connects fresh publisher/subscriber clients to a local OxideSFU instance. It exposes:

```ts
window.oxidesfuReceiverSample(): Promise<{
  pcId: string;
  trackId: string;
  packetsReceived: number;
  framesDecoded: number;
}>
```

It must return stats for the `RTCRtpReceiver` currently attached to the rendered remote video element, not an old peer connection. The page must expose an element with:

```html
<div data-testid="browser-harness-ready"></div>
```

The harness should install its `RTCPeerConnection` observer before loading LiveKit, keep a mapping from the rendered video element to its receiver track, and execute the adaptive sequence:

```text
HIGH -> LOW -> HIGH -> LOW
```

For Meet-specific coverage it should additionally drive the visibility sequence:

```text
visible=false -> visible=true -> final LOW dimensions
```

## Run

Start OxideSFU separately, then provide its URL and fresh local API credentials only as process environment variables:

```bash
OXIDESFU_URL=http://127.0.0.1:7880 \
OXIDESFU_API_KEY=devkey \
OXIDESFU_API_SECRET=secret \
npm run test:firefox
```

The test mints short-lived tokens in memory; it never writes them to artifacts.

On failure, Playwright retains trace, screenshot, and video artifacts. Never place JWTs, API secrets, or full Meet URLs in test artifacts.

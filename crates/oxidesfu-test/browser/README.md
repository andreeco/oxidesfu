# Firefox browser conformance harness

This opt-in Playwright project tests browser-visible media contracts that native Rust SDK probes cannot cover: the active Firefox receiver must keep receiving and decoding frames after adaptive-stream settings churn and after a reliable chat data packet is sent.

## Install

```bash
cd crates/oxidesfu-test/browser
npm install
npm run install:browsers
```

`install:browsers` uses `playwright install firefox` (without `--with-deps`) to avoid distro-specific apt package resolution issues.

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

It must return stats for the `RTCRtpReceiver` currently attached to the rendered remote video element, not an old peer connection. The page also exposes:

```ts
window.oxidesfuDataChannelSample(): Array<{
  pcId: string;
  origin: 'local' | 'remote';
  label: string;
  readyState: RTCDataChannelState;
  bufferedAmount: number;
  ordered: boolean;
}>
```

The single-PC chat regression requires `_reliable` to be open on both Firefox clients before it sends. In legacy dual-PC mode, the server-created subscriber `_reliable` is remote and opens first; `sendText()` then creates and negotiates the publisher's local channels. Failures include the observed channel labels, origins, ready states, buffered amounts, and redacted SDP section shapes (media/MID/direction/candidate counts only), distinguishing an unopened SCTP channel from a post-send media stall without exposing credentials or candidate addresses.

The page must expose an element with:

```html
<div data-testid="browser-harness-ready"></div>
```

The harness should install its `RTCPeerConnection` observer before loading LiveKit and keep a mapping from the rendered video element to its receiver track. The Playwright contracts exercise both:

```text
HIGH -> LOW -> HIGH -> LOW
single-PC: both `_reliable` channels open -> reliable data packet delivered
legacy dual-PC: remote subscriber `_reliable` opens -> chat creates and opens local publisher `_reliable` -> reliable data packet delivered
same video receiver continues decoding at repeated post-send samples
```

For Meet-specific coverage it should additionally drive the visibility sequence:

```text
visible=false -> visible=true -> final LOW dimensions
```

## Run

Provide OxideSFU URL and local API credentials via environment variables:

```bash
OXIDESFU_URL=ws://127.0.0.1:7880 \
OXIDESFU_API_KEY=devkey \
OXIDESFU_API_SECRET=secret \
npm run test:firefox
```

`test:firefox` now auto-starts `oxidesfu-server` when nothing is listening on `OXIDESFU_URL` and stops it after the test run.

If you want to run against an already-running server only, disable auto-start:

```bash
OXIDESFU_AUTOSTART=0 npm run test:firefox
```

For debugging the Playwright invocation without wrapper logic:

```bash
npm run test:firefox:raw
```

The test mints short-lived tokens in memory; it never writes them to artifacts.

By default, this harness keeps screenshots on failure, but disables trace/video to avoid storing JWT-bearing websocket URLs in artifacts.

You can opt in for deeper debugging:

```bash
PLAYWRIGHT_TRACE=1 PLAYWRIGHT_VIDEO=1 npm run test:firefox
```

If you enable trace/video, treat generated artifacts as sensitive and avoid sharing them unredacted.

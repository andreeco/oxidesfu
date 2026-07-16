# Legacy dual-PC Firefox chat/data-channel freeze

## Status

**Open compatibility investigation.** Two Firefox clients using the legacy LiveKit dual-peer-connection topology can exchange media, but chat/data delivery through the publisher transport does not complete.

This note records observed behavior and reference mapping. It deliberately does not contain JWTs, API secrets, or other credentials.

## Scope

- Target: OxideSFU, current investigation working tree based on `9b67db76b299bd7cb0ff97050cae11fb10415f28`.
- Upstream LiveKit reference: `ae09b7d0ad94d764f0c97d183efd36476163e819`.
- WebRTC reference checkout: `a1f15cd14b3ea6555c49702bd1d7c8e3fd793fff`.
- Browser SDK used by the local Firefox harness: `livekit-client` `2.20.1`.
- Production symptom: two `meet.livekit.io` clients can exchange media, but a chat send freezes/unresponsively affects remote video and the message is not delivered while both participants remain in the room.

## Reproduction contract

The Firefox harness has a dual-PC regression in:

- `crates/oxidesfu-test/browser/tests/receiver-counters.spec.ts`

It explicitly configures:

```text
singlePeerConnection=false
```

That selects the legacy v0/dual-PC LiveKit topology indicated by `subscriber_initial_offer_created` in server logs. The test uses two publishing participants so active remote video is present on both sides.

Expected behavior:

1. Subscriber transport data channels open.
2. Calling `sendText()` causes the client to create and negotiate publisher `_reliable`, `_lossy`, and `_data_track` channels if they do not yet exist.
3. Publisher `_reliable` opens.
4. The message reaches the remote participant.
5. The pre-existing remote video receiver continues advancing.

## What is reproduced

The original harness assertion waited for a *local* publisher `_reliable` before invoking `sendText()`. That is not valid for the legacy topology:

- `livekit-client` creates publisher channels lazily in `RTCEngine.ensureDataTransportConnected()`.
- In subscriber-primary legacy mode, the initial publisher media offer intentionally has no SCTP section.
- `sendText()` is the action that calls `ensurePublisherConnected()`, creates publisher channels, and initiates the SCTP renegotiation.

The regression was therefore adjusted to:

1. verify the server-created subscriber `_reliable` as a **remote** browser channel;
2. start `sendText()`;
3. wait for the publisher’s **local** `_reliable` to open;
4. await send completion and assert delivery/video continuity.

With that corrected sequence, the browser creates local publisher `_lossy`, `_reliable`, and `_data_track` channels, but all remain `connecting` for the timeout. The server receives the late publisher offer and answers it, but no publisher-side `OnDataChannel` event follows.

A full Firefox run against a fresh local server had three passing browser tests and this dual-PC test failing. The focused run after correcting the trigger reached the real failure rather than the premature assertion.

## OxideSFU signal trace

For the affected publisher after `sendText()`:

1. Initial publisher media offer:
   - `offer_has_sctp=false`
   - server answer: `answer_has_sctp=false`
2. Chat-triggered publisher offer:
   - `offer_has_sctp=true`
   - server answer: `answer_has_sctp=true`
3. Browser observation:
   - subscriber PC has remote `_reliable`, `_lossy`, `_data_track` in `open` state;
   - publisher PC has local `_reliable`, `_lossy`, `_data_track` in `connecting` state;
   - server does **not** log a publisher-target `peer_connection_data_channel_received` event.

The redacted browser diagnostic recorded a late publisher local offer with a candidate-bearing video section and a candidate-free SCTP application section. Oxide's remote answer had the same video and SCTP section structure; the candidate-propagation experiment caused the remote application section to contain the copied candidates and `end-of-candidates`, but did not change the `connecting` outcome.

The late publisher answer preserves an SCTP section such as:

```text
m=application 9 UDP/DTLS/SCTP webrtc-datachannel
a=setup:active
a=mid:1
a=sctp-port:5000
a=ice-ufrag:...
a=ice-pwd:...
a=sendrecv
```

The application section originally had shared ICE credentials but did not include its own candidate lines; candidates appeared on the earlier bundled media section. A candidate-propagation experiment copied bundled candidate and end-of-candidates lines into that application section. A fresh focused Firefox run confirmed that the normalized remote answer did contain candidates in both the media and application sections, but publisher `_lossy`, `_reliable`, and `_data_track` still remained `connecting`. The experiment was therefore removed; candidate placement is **not** the root cause.

## Relevant OxideSFU paths

- `crates/oxidesfu-signaling/src/router/session.rs`
  - `create_subscriber_offer`: creates the server-offered subscriber transport channels.
  - `answer_publisher_offer`: accepts initial media offers and the later client SCTP offer.
  - `forward_peer_connection_events`: receives remote data channels and relays data.
- `crates/oxidesfu-rtc/src/webrtc_adapter.rs`
  - `EventPeerConnectionHandler::on_data_channel` forwards a WebRTC event to the signaling event stream.
- `crates/oxidesfu-rtc/src/peer_connection.rs`
  - contains an in-process regression for a media-only offer followed by a data-channel offer.
- `crates/oxidesfu-rtc/src/data_channel.rs`
  - wraps channel open/read/write semantics; no evidence currently identifies it as the late-negotiation failure.
- `crates/oxidesfu-rtc/src/data_channel_store.rs`
  - currently keys channels by `(room, identity, kind)`, without a publisher/subscriber transport dimension. This is a separate dual-PC routing concern that must be addressed once the publisher SCTP association opens.

## Upstream reference map

### LiveKit JS client

- `client-sdk-js/src/room/RTCEngine.ts`
  - `join()` preserves lazy behavior for V0 dual-PC connections.
  - `createDataChannels()` creates `_reliable`, `_lossy`, and `_data_track` on the publisher transport.
  - `ensureDataTransportConnected()` creates those publisher channels and calls `negotiate()` when data is first sent.
  - `ensurePublisherConnected()` is used by `sendDataPacket()` and therefore by text-stream chat.

### LiveKit server

- `livekit/pkg/rtc/transportmanager.go`
  - `NewTransportManager()` calls `createDataChannelsForSubscriber()` when `SubscriberAsPrimary` is set.
  - `SendDataMessage()` explicitly writes downstream data through the primary transport.
- `livekit/pkg/rtc/transport.go`
  - `PCTransport.createPeerConnection()` registers `OnDataChannel` before remote descriptions are processed.
  - `PCTransport.HandleRemoteDescription()` applies a publisher offer with `SetRemoteDescription`; `GetAnswer()` creates and sets the local answer.
  - `PCTransport.onDataChannel()` installs writers on `OnOpen`, detaches channels, and reads each in a dedicated goroutine.
  - transport-owned reliable/lossy/data-track writers are independent for publisher and subscriber PCs.
- `livekit/pkg/rtc/transportmanager.go`
  - `HandleOffer()` delegates a client publisher offer to `publisher.HandleRemoteDescription()`.
  - This source inspection identifies no obvious upstream difference in data-channel callback registration ordering; an actual upstream Firefox run remains required to compare the full late exchange.

## Confirmed non-root cause

The focused RTC regression:

```text
client_created_data_channel_opens_after_media_only_offer_is_renegotiated_with_sctp
```

in `crates/oxidesfu-rtc/src/peer_connection.rs` passes. It proves that the Oxide RTC wrapper and pinned WebRTC implementation can perform a media-only offer followed by an SCTP/data-channel renegotiation in-process. The production failure therefore depends on signaling, browser Firefox behavior, candidate timing/SDP shape, or the interaction of those factors.

## Next steps

1. Compare the redacted late browser publisher offer/remote-answer shape with an upstream LiveKit server run. The Oxide trace establishes: a media-only initial publisher offer; then a publisher offer with video candidates and a candidate-free SCTP application section; and an answer with the same SCTP section. Candidate propagation did not open SCTP.
2. Add a signaling-level end-to-end regression that uses the actual `answer_publisher_offer` path with:
   - an initial media-only publisher offer;
   - a later SCTP publisher offer;
   - ICE trickle on both rounds;
   - an asserted publisher `OnDataChannel` event and channel open.
3. After the publisher channel opens, separate data-channel ownership by transport in `DataChannelStore` and select the participant’s primary transport for downstream data, matching upstream `TransportManager` behavior.

## Validation recorded

- `cargo test -p oxidesfu-rtc client_created_data_channel_opens_after_media_only_offer_is_renegotiated_with_sctp -- --nocapture` — passed.
- `cargo fmt --all` — passed after the retained changes.
- `npm --prefix crates/oxidesfu-test/browser run build` — passed.
- `cargo test -p oxidesfu-signaling --lib` — passed (541 passed, 2 ignored).
- Full Firefox run before the candidate experiment — 3 passed, 1 failed (dual-PC regression).
- Fresh focused Firefox run after the candidate experiment: still failed with the publisher's three local data channels in `connecting`; no publisher-target data-channel event. The redacted SDP diagnostic confirmed that the experiment added candidates and end-of-candidates to the application answer section, so it was removed.
- The diagnostic-focused retry also exposed an intermittent pre-chat media-forwarding failure: a rendered remote `srcObject` can exist before Firefox exposes an inbound RTP report. The regression now waits for an actual receiver sample before starting chat; this is separate from the SCTP failure.

# Legacy dual-PC Firefox chat/data-channel freeze

## Status

**Resolved for the local Firefox regression.** OxideSFU omitted LiveKit's `JoinResponse.fast_publish` capability. A legacy dual-PC Firefox client therefore delayed publisher SCTP negotiation until chat, producing a media-only publisher offer followed by a late SCTP renegotiation whose local channels remained `connecting`.

Setting `fast_publish=true` for publish-authorized joins makes the browser establish the publisher SCTP transport at join, matching upstream. The focused dual-PC Firefox chat/data/video continuity regression now passes against both OxideSFU and upstream LiveKit.

This note contains no JWTs, API secrets, ICE credentials, or candidate addresses.

## Scope

- Target: OxideSFU investigation started from `9b67db76b299bd7cb0ff97050cae11fb10415f28`; root-cause fix is in the current working tree.
- Upstream LiveKit reference: `ae09b7d0ad94d764f0c97d183efd36476163e819` (`1.13.3`).
- WebRTC reference checkout: `a1f15cd14b3ea6555c49702bd1d7c8e3fd793fff`.
- Browser SDK: `livekit-client` `2.20.1`.

## Reproduction contract

`crates/oxidesfu-test/browser/tests/receiver-counters.spec.ts` explicitly uses:

```text
singlePeerConnection=false
```

The test connects two publishing Firefox participants, requires active remote video, sends chat, and asserts delivery plus continued receiver packet/frame progress. The browser harness records local/remote data-channel state and, on demand, only redacted SDP section shape (media, MID, direction, SCTP presence, candidate count).

## Root-cause evidence

### Failing Oxide behavior before the fix

For a publisher in subscriber-primary legacy mode:

1. Initial publisher offer: media only, `offer_has_sctp=false`.
2. `sendText()` creates local `_lossy`, `_reliable`, and `_data_track`, then sends an SCTP offer.
3. Oxide answers with SCTP, but all three local browser channels remain `connecting` and the server never receives a publisher `OnDataChannel` event.

An experiment that copied bundled candidates and `end-of-candidates` into the late application answer was ineffective and removed. Redacted diagnostics confirmed the copied candidates reached the application section, while SCTP still did not open.

### Passing upstream comparison

A locally built upstream LiveKit server at `ae09b7d…` passed the same focused Firefox test. Its redacted publisher-PC trace showed an application/SCTP-only publisher offer and answer **before** the later video offer. The publisher data channels consequently opened before chat.

Relevant source behavior:

- `livekit/pkg/rtc/room.go:createJoinResponseLocked` sets `FastPublish` when `participant.CanPublish()` and no ICE fallback preference is active.
- `client-sdk-js/src/room/RTCEngine.ts:join` calls `negotiate()` when `!subscriberPrimary || joinResponse.fastPublish`.
- `livekit/pkg/rtc/transportmanager.go` creates subscriber transport channels for subscriber-primary topology; `transport.go` registers `OnDataChannel` before remote descriptions are applied.

Oxide did not populate `fast_publish`, so the JS client took its delayed publisher-transport path. Oxide does not currently model upstream ICE fallback preferences, hence every publish-authorized join is eligible for `fast_publish`.

## Implementation

- `crates/oxidesfu-signaling/src/router.rs`
  - local join responses now set `fast_publish` from the authorized publish grant.
- `crates/oxidesfu-server/src/relay_worker.rs`
  - non-local room-owner join responses preserve the same field from the relay intent.
- `crates/oxidesfu-signaling/src/router/tests.rs`
  - v0 WebSocket join contract asserts `fast_publish` for a publish-authorized participant.
- `crates/oxidesfu-test/browser/tests/receiver-counters.spec.ts`
  - the dual-PC browser contract and optional redacted upstream-comparison diagnostic remain in place.

## Follow-up dual-PC transport parity

The initial `fast_publish` fix made publisher SCTP establishment deterministic, exposing a second upstream parity requirement: LiveKit keeps publisher/subscriber writers separate and sends downstream data through the primary transport.

This slice now:

- keys `DataChannelStore` by `(room, identity, transport target, kind)`;
- preserves both publisher and subscriber channels with the same label;
- uses subscriber-first lookup for downstream packets, with publisher fallback for single-PC/publisher-primary sessions;
- creates `_reliable`, `_lossy`, and `_data_track` before the initial subscriber offer, matching upstream channel readiness; and
- keeps the open-triggered data-track subscription reconciliation without the former fixed 250 ms creation delay.

For relayed joins, the owner response now advertises default ICE servers and uses effective subscriber-primary topology (`requested && can_subscribe`), preventing a denied-subscribe participant from receiving an incompatible subscriber-primary response. Configured per-participant ICE/client configuration propagation, enabled publish-codec policy, region metadata, and SIF trailer support require their own backing Oxide configuration/state model; they were not fabricated in this transport compatibility fix.

## Validation

- Upstream focused Firefox dual-PC test (`ws://127.0.0.1:7882`): passed.
- Upstream focused Firefox test with redacted description output: passed; confirmed initial publisher SCTP negotiation.
- Oxide focused Firefox dual-PC test (`ws://127.0.0.1:7880`): passed in 7.1 seconds after the fix.
- Manual live verification: confirmed working by the developer.
- `cargo test -p oxidesfu-signaling rtc_v0_websocket_sends_join_response_without_join_request -- --nocapture`: passed after the regression was added.
- `cargo test -p oxidesfu-signaling --lib`: passed (541 passed, 2 ignored).
- `cargo test -p oxidesfu-server --lib`: passed (110 passed).
- A broad four-test Firefox run was intentionally stopped by the developer after two unrelated tests had reported failures; it is not a completed suite result for this fix.
- Follow-up transport ownership slice: `cargo test -p oxidesfu-rtc --lib` passed (41 passed); `cargo test -p oxidesfu-signaling --lib` passed (541 passed, 2 ignored); `cargo test -p oxidesfu-server --lib` passed (112 passed); and the focused Firefox dual-PC test passed after subscriber-first routing and initial `_data_track` creation.
- Previous investigation coverage remains relevant:
  - `cargo test -p oxidesfu-rtc client_created_data_channel_opens_after_media_only_offer_is_renegotiated_with_sctp -- --nocapture`: passed.
  - `cargo test -p oxidesfu-signaling --lib`: previously passed (541 passed, 2 ignored) before this focused fix.

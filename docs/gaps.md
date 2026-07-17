# OxideSFU compatibility gap register

This document records known or plausible differences from the upstream LiveKit server that are not yet represented by a completed OxideSFU compatibility contract. It is a prioritization register, not an endpoint checklist: each item needs a focused test before implementation is considered complete.

## Scope and references

- OxideSFU baseline: compatibility work through `722fb686`.
- Upstream server reference: LiveKit `ae09b7d0ad94d764f0c97d183efd36476163e819` (`1.13.3`).
- WebRTC wrapper reference: `andreeco/webrtc` `bede31c8`, including the shared ICE/TCP mux and dispatch hardening.
- Browser regression client: `livekit-client` `2.20.1` on Firefox.

## Status levels

- **Proven** — source and/or a failing compatibility test demonstrates a behavioral difference.
- **High risk** — source differs and can affect ordinary client behavior, but an end-to-end failure has not been isolated.
- **Unsupported model** — upstream behavior requires server state/configuration Oxide does not represent yet. Do not emit invented protocol values.
- **Covered** — a previously identified gap now has a regression and implementation; retain its record for historical context.

## Covered in the current dual-PC slice

### Legacy publisher SCTP did not establish eagerly

**Status:** Covered.

Oxide omitted `JoinResponse.fast_publish`, causing legacy subscriber-primary Firefox clients to defer publisher SCTP negotiation until a chat/data send. The later SCTP renegotiation left local browser channels in `connecting`.

- Upstream: `livekit/pkg/rtc/room.go:createJoinResponseLocked` sets `FastPublish` for publish-authorized participants without ICE fallback preferences.
- Client: `client-sdk-js/src/room/RTCEngine.ts:join` negotiates when `!subscriberPrimary || joinResponse.fastPublish`.
- Oxide: local and relayed join responses now set `fast_publish` for authorized publishers.
- Regression: `crates/oxidesfu-test/browser/tests/receiver-counters.spec.ts` legacy dual-PC Firefox chat/video test.

### Legacy dual-PC downstream channel ownership

**Status:** Covered.

Upstream keeps publisher and subscriber writers independent and routes downstream data through the primary transport. Oxide formerly keyed data channels only by `(room, identity, kind)`, allowing a publisher channel to replace the server-created subscriber channel.

- Upstream: `livekit/pkg/rtc/transportmanager.go:getTransport(true)` selects the subscriber PC for subscriber-primary sessions.
- Oxide: `DataChannelStore` now includes a transport target and downstream lookup is subscriber-first with publisher fallback.
- Related readiness alignment: `_data_track` is now created before the initial subscriber offer, not after a fixed timer.

## Covered in the current transport slice

### Shared fixed-port ICE/TCP listener

**Status:** Covered for implementation and deployment; TCP-only public-browser selection remains environment-dependent.

OxideSFU now owns one shared passive ICE/TCP listener and routes accepted RFC 4571 streams by the destination ICE ufrag. Peer connections register and unregister their routes without binding the fixed port independently.

- WebRTC fork: `andreeco/webrtc` commits `f95d4f56` and `bede31c8`; `TcpMux` owns the listener, dispatches the initial STUN frame, bounds initial dispatch with a timeout and connection limit, and preserves existing per-peer TCP behavior.
- OxideSFU: `crates/oxidesfu-server/src/main.rs` creates the server-lifetime mux; `crates/oxidesfu-rtc/src/peer_connection.rs` carries it to every compatible peer connection; `crates/oxidesfu-signaling/src/router.rs` filters it by client ICE/TCP support.
- Tests: WebRTC two-peer fixed-port TCP/data-channel regression; Oxide RTC shared-mux regression; server configuration and router-filtering regressions.
- Deployment: the VPS advertises the public passive TCP candidate on port `7881`; the hardened image reports healthy.
- Browser evidence: deployed Firefox publisher/subscriber video and adaptive-quality contracts pass. The opt-in TCP-only browser contract is present but cannot force host TCP through the standard browser/LiveKit API; it must run from a network that independently blocks UDP.
- Hardening: initial RFC 4571/STUN dispatch has a bounded wait and bounded concurrent dispatch capacity. A future production stress profile may tune these defaults if operational measurements require it.

### LiveKit connection-test generic peer-connection error

**Status:** Not an OxideSFU-specific gap.

The connection-test page can report `could not establish pc connection` while separately reporting successful TURN connectivity and audio/video publishing. The same output was reproduced against a real LiveKit server, so this message is not evidence of an OxideSFU wire-compatibility failure. The stronger contracts are selected candidate/media tests and the explicit TURN check.

## Open gaps

### Relay JoinResponse preserves configured ICE servers

**Status:** Covered.

The relay owner resolves ICE through `SignalState`, the same source used by local
joins. This preserves configured static servers and participant-SID-specific ICE
providers instead of hard-coding default STUN.

- Upstream: `livekit/pkg/rtc/room.go:createJoinResponseLocked` returns the room participant’s resolved `IceServers`.
- Oxide: `SignalState::ice_servers_for_participant`; `RoomStoreRelayJoinIntentExecutor`.
- Regression: `relay_executor_uses_owner_ice_and_client_configuration_resolution`
  proves provider credentials use the assigned SID; the two-process Redis
  `redis_relay_process_returns_room_owner_ice_servers` contract proves that the
  origin delivers the selected owner’s ICE entry to an SDK client.

### Relay JoinResponse preserves client configuration

**Status:** Covered.

Relay intents carry encoded `JoinRequest.client_info`, and the selected owner
uses `SignalState::client_configuration_for_participant`, the same resolver used
by local joins. Therefore candidate-protocol preferences remain owner-local and
browser codec policy is reconstructed from the original client metadata.

- Upstream: `livekit/pkg/rtc/room.go:createJoinResponseLocked` returns `participant.GetClientConfiguration()`.
- Client: `client-sdk-js/src/room/RTCEngine.ts:makeRTCConfiguration` maps `forceRelay` to relay-only ICE.
- Regression: `relay_executor_uses_owner_ice_and_client_configuration_resolution`
  proves Safari’s disabled-AV1 policy survives relay serialization. Existing
  candidate-protocol signal-request coverage continues to exercise
  `force_relay` through the shared `SignalState` resolver.

### Enabled publish codecs are not advertised

**Status:** Proven wire-shape difference; runtime impact depends on configured policy.

Upstream includes `enabled_publish_codecs` in every join response. Oxide leaves it empty, even though local track processing already has codec acceptance behavior.

- Upstream: `livekit/pkg/rtc/room.go:createJoinResponseLocked` uses `participant.GetEnabledPublishCodecs()`.
- Client: `client-sdk-js/src/room/Room.ts` passes the field to `LocalParticipant.setEnabledPublishCodecs`; the SDK uses it to select a supported fallback codec.
- Oxide related code: `crates/oxidesfu-signaling/src/router/session.rs:disabled_publish_codecs_for_participant`.

**Required contract:** configure a publish codec policy that excludes a requested browser codec; assert the JoinResponse advertises allowed codecs and the client publishes an allowed fallback.

**Implementation direction:** define a single room/participant codec-policy source first. Do not advertise the current implicit defaults until their intended policy semantics are documented.

### Region metadata is incomplete

**Status:** Proven wire-shape difference; low runtime impact.

Upstream fills both `server_region` and `server_info.region`. Oxide leaves them empty, including when server configuration has a region.

- Upstream: `livekit/pkg/rtc/room.go:createJoinResponseLocked`.
- Oxide: `crates/oxidesfu-signaling/src/router.rs:join_server_info`; server configuration is in `crates/oxidesfu-core/src/config.rs`.

**Required contract:** configure a server region and assert local/reconnect/relayed responses contain it in both protocol-compatible fields.

**Implementation direction:** inject immutable server identity/region into `SignalState` rather than letting router helpers read environment or duplicate server configuration.

### SIF trailer is unavailable

**Status:** Unsupported model.

Upstream includes a per-room `sif_trailer`, used by E2EE/server-injected frame behavior. Oxide currently has no room trailer generation, lifecycle, or E2EE injection model.

- Upstream: `livekit/pkg/rtc/room.go:createJoinResponseLocked` returns `r.trailer`.
- Client: `client-sdk-js/src/e2ee/E2eeManager.ts` ignores an empty trailer, so normal non-E2EE rooms are unaffected.

**Required contract before implementation:** E2EE conformance that proves trailer generation, distribution, rotation/lifecycle, and consumption by a supported client. Do not send arbitrary bytes merely to make the field nonempty.

### Subscriber-primary transport behavior under permission changes

**Status:** High risk.

Local and relay paths now agree that a participant without subscribe permission must not be told it has an active subscriber-primary transport. The deferred transport must still be created correctly when permission is later granted.

- Oxide local reconciliation: `crates/oxidesfu-signaling/src/router.rs:ensure_subscriber_transport_after_permission_grant`.
- Relay owner topology decision: `crates/oxidesfu-server/src/relay_worker.rs`.

**Required contract:** a relayed v0 participant joins without subscribe permission, receives no subscriber offer, is granted permission, receives exactly one subscriber offer, and subsequently receives media/data through the subscriber transport.

### End-to-end transport ownership under both channel-open orders

**Status:** High risk despite store-level coverage.

The store has target-aware unit coverage and the Firefox chat regression passes, but there is not yet a deterministic signaling-level test forcing both transport creation orders.

**Required contract:** create subscriber and publisher `_reliable`, `_lossy`, and `_data_track` channels in both orders; relay server-to-client reliable/lossy/data-track traffic; assert each arrives once on subscriber transport for subscriber-primary topology and never on publisher transport.

### Broader browser regression stability

**Status:** Open test-infrastructure quality item.

The focused dual-PC Firefox regression is stable in the validated runs. A broader four-test Firefox invocation was intentionally stopped after unrelated failures, so it is not a completed suite result.

**Required contract:** run the complete Firefox suite against a fresh local server, classify each pre-chat media failure, and make receiver readiness assertions deterministic before treating the suite as a release gate.

## Process for closing an item

1. Record the exact upstream files and commit inspected.
2. Add a focused failing contract test before behavior changes.
3. Implement the smallest Rust-native behavior that satisfies that contract.
4. Run focused tests, relevant crate suites, formatter, and browser/conformance coverage where applicable.
5. Move the item to **Covered** with the relevant commit and validation evidence.

# Simulcast layer-selection implementation status

**Status:** partial implementation; do not treat this as completed LiveKit-compatible allocator behavior.

**Last updated:** 2026-07-16

## Purpose

This note records the work completed after identifying OxideSFU's packet-order-dependent simulcast selection behavior, and explicitly separates it from the compatibility work that remains.

The target externally observable behavior is LiveKit-compatible per-subscriber layer selection:

- a requested high quality target does not permanently lock to low merely because low RTP arrived first;
- spatial source changes begin only at decodable boundaries;
- target acquisition requests keyframes for a bounded period;
- unavailable targets fall back without an unbounded PLI storm;
- every subscriber has independent selection and RTP rewrite state.

## Reference map

Reference revisions inspected:

| Repository | Revision | Files | Derived behavior |
|---|---|---|---|
| LiveKit | `ae09b7d0ad94d764f0c97d183efd36476163e819` | `pkg/rtc/subscribedtrack.go`, `pkg/sfu/downtrack.go`, `pkg/sfu/forwarder.go`, `pkg/sfu/videolayerselector/{base.go,simulcast.go}` | Subscriber settings set max spatial/temporal bounds; forwarding retains max, target, current, and seen layers; spatial changes are decodable-boundary gated. |
| WebRTC Rust fork (currently pinned by Oxide) | `24b69d02220ffdaf67af4550482d5986089a95aa` | `rtc-rtp/src/codec/{vp9,h264,av1}`, `rtc-media/src/io/ivf_writer` | VP9 frame-start/non-predicted, H264 IDR, and AV1 new-coded-sequence boundaries usable for source-switch detection. |
| WebRTC Rust compatibility fork (published/pinned) | outer `3b0b2f0d8f0443deeab47fb83dada7eb4d7778ea`, nested RTC `56a36e408913475baeeb5672bd3e30036dea820f` | `rtc-rtp/src/extension/dependency_descriptor_extension/{mod.rs,dependency_descriptor_extension_test.rs}` | Exposes active-target packet metadata and distinguishes an active decode target with DTI `Switch`; a frame boundary by itself is not a safe source-switch point. |
| OxideSFU | working tree following `3d6331078e8a2a2c0587fe5bb16da939efb89bd2` | `crates/oxidesfu-signaling/src/{media/video_ingress.rs,router/session.rs}` | Original first-eligible-SSRC latch was in the reader-owned forwarding target. |

## Completed work

### Target-local selector

`crates/oxidesfu-signaling/src/media/video_ingress.rs` now contains a reader-local `SubscriberVideoLayerSelector`.

It tracks:

- a `LayerPolicy` containing spatial `max` and `desired` fields;
- the current selected source SSRC and spatial layer;
- observed source SSRCs by spatial layer;
- acquisition/fallback state;
- timer-driven PLI retry cadence and a bounded per-acquisition retry budget.

The selector is synchronous, owns no locks, performs no async I/O, and does not read wall-clock time per RTP packet. It returns a decision to the reader; the reader performs RTCP I/O and RTP rewrite/write side effects.

### Packet forwarding integration

`crates/oxidesfu-signaling/src/router/session.rs` now:

- applies target policy without resetting a currently decodable source on settings revision changes;
- runs selector retry scheduling on a dedicated 250 ms timer;
- preserves the independent three-second forwarding diagnostics heartbeat;
- routes selector-generated PLI requests separately from downstream subscriber PLI/FIR feedback;
- applies a target-local temporal controller after spatial admission: known VP8/VP9/H265 temporal IDs are capped by an explicit maximum/desired/current temporal policy, while metadata-poor codecs retain deterministic timestamp gating;
- passes selected source packets to the existing `SubscriberRtpForwarder`, preserving its existing outgoing SSRC, sequence-number, timestamp, retransmission, and source-history behavior.

### Decodable source-switch boundaries

The reader currently recognizes the following RTP-level source-switch starts:

| Codec | Accepted boundary |
|---|---|
| VP8 | Partition-zero keyframe start. |
| VP9 | Verified dependency-descriptor frame start with an active DTI `Switch` target when descriptor metadata is available; otherwise beginning of a non-predicted frame. |
| H264 | IDR NAL, including STAP-A and FU-A/FU-B starts. |
| AV1 | Verified dependency-descriptor frame start with an active DTI `Switch` target when descriptor metadata is available; otherwise new coded-video-sequence RTP packet. |

Unknown codecs are not silently treated as arbitrary decodable delta frames.

### Tests completed

Production selector tests cover:

- high target does not latch low when low arrives first;
- switching to a new source waits for a decodable boundary;
- bounded fallback to an observed permitted source;
- high → low → high transitions remain boundary-gated;
- selector target state is isolated between subscribers;
- a persistently unusable target has a bounded PLI request budget.

The native SDK contract in `crates/oxidesfu-test/src/probes/media.rs` now additionally verifies that:

- a low-quality decoded frame is smaller than a high-quality decoded frame;
- a low → high update recovers larger decoded dimensions;
- media remains decodable throughout the transition.

## Known missing work

### 1. Allocation-policy plumbing is complete; bandwidth/layout producer remains

`TrackAllocationStore` is now an independent, private, target-scoped allocation input. It publishes semantic revisioned changes to the reader that owns each `ForwardTarget`; the reader merges its desired quality with `UpdateTrackSettings` maximum quality and clamps desired to that ceiling. Allocation changes therefore use the same keyframe-gated selector transition as subscription changes without mutating client permission/settings.

The store defaults to no allocation, making `desired = max` and preserving current behavior. Its store and pure policy tests prove target isolation, semantic change notification, downgrade, and maximum clamping.

Remaining work:

- implement the bandwidth/layout allocator producer that writes `TrackAllocationStore` from actual receiver transport estimates and layout policy;
- add end-to-end allocation-driven downgrade/upgrade coverage once that producer exists.

### 2. Temporal target state is implemented; allocator temporal intent remains

`SubscriberVideoTemporalController` is reader-local state in each `ForwardTarget`. For a requested FPS and receiver-observed temporal cadence it derives an explicit `TemporalLayerPolicy` with `max` and `desired`, admits only temporal IDs at or below that maximum, and records the highest currently forwarded temporal layer. A policy reduction clamps the current state without resetting spatial source selection or RTP rewrite history.

When source temporal metadata or cadence estimates are unavailable, the controller explicitly selects the existing timestamp gate. It does not guess that an unknown packet is a desired temporal enhancement layer. The timestamp gate is still used for an observed layer whose advertised cadence materially exceeds the requested FPS.

Covered deterministic tests:

- request-to-available-layer maximum clamping;
- high-temporal to low-temporal policy reduction;
- independent temporal decisions for two targets receiving identical packets;
- metadata-poor timestamp-gate fallback.

The native Rust SDK FPS-isolation contract also passes with the controller in the production reader.

Remaining work:

- extend allocator output so it can set an independent desired temporal target, rather than deriving `desired = max` only from `UpdateTrackSettings.fps`;
- add end-to-end allocation-driven temporal downgrade/upgrade coverage once that producer exists.

### 3. Dependency-descriptor decode targets are used for VP9/AV1 switching; end-to-end stream coverage remains

Oxide is pinned to outer WebRTC `3b0b2f0d8f0443deeab47fb83dada7eb4d7778ea`, nested RTC `56a36e408913475baeeb5672bd3e30036dea820f`.

`RemoteTrack` parses and retains a current-packet descriptor switch result per incoming SSRC. The forwarding reader consumes that result for VP9 and AV1: when descriptor metadata is available, a source transition requires both `first_packet_in_frame` and an active DTI `Switch` target. A parsed non-switchable descriptor deliberately overrides VP9/AV1 payload keyframe heuristics; it cannot trigger an unsafe fallback switch. When descriptor metadata is absent, the established codec-specific VP9/AV1 boundary detector remains the compatibility fallback. VP8 and H264 continue to use their codec-specific paths.

Covered regressions:

- nested RTC parser: an active descriptor `Switch` target is distinct from a frame boundary (nine parser tests);
- `oxidesfu-rtc`: both descriptor frame start and `Switch` are required for the exposed result;
- signaling: a descriptor `false` result blocks a VP9 payload-keyframe heuristic, a descriptor `true` result permits the transition, and absent metadata falls back to the payload detector.

Remaining work:

- add RTP packet-sequence integration coverage that exercises descriptor state through a real scalable VP9/AV1 remote track and verifies source-switch RTP continuity;
- add native SDK scalable-stream coverage when a deterministic dependency-descriptor publisher fixture is available.

### 4. Source liveness expiry is complete; decodability availability remains limited

The selector now ages observed SSRCs only from its 250 ms timer. A source with no RTP for two seconds expires; if it was current, the selector clears it and performs a bounded acquisition of a live permitted fallback. Once that fallback is locked, it does not resume timer-driven target acquisition or issue another selector PLI. A later desired-layer decodable boundary may still promote the target, which is concrete renewed availability rather than a packet-order switch. The deterministic `stale_current_source_reacquires_a_live_fallback_without_oscillation` regression covers both stability and this safe promotion.

Remaining limitation:

- availability currently means recently observed RTP, not independently verified decoder usability;
- dependency-descriptor decode targets are still needed for scalable VP9/AV1 availability semantics.

### 5. Production observability is partially complete

The existing three-second target heartbeat now reports reader-local maximum/desired/current spatial layers, maximum/desired/current temporal layers, selected RID/SSRC, layer transitions, categorized spatial and temporal drops, selector PLI requests, rewrite drops, successful RTP packet count, successful rewritten payload bytes, and write errors. Counters update without locks, allocation, formatting, or clock reads in the RTP path.

Still required:

- selector PLI suppression reasons and receiver-feedback PLI/FIR counters as separate fields;
- full RTP wire-byte accounting and a reporting-window bytes/sec export rather than cumulative payload bytes in debug heartbeat;
- acquisition/fallback state and waiting duration in a machine-readable profiler snapshot.

### 6. Black-box target isolation is complete

`rust_sdk_room_simulcast_video_quality_isolated_per_subscriber_contract` publishes one real simulcast source to two native SDK subscribers, drains stale frames after settings propagation, and proves that every sampled low decoded dimension is lower than every sampled high dimension. It then upgrades only the low subscriber and proves that it recovers high dimensions without reducing the independent high subscriber.

### 7. Differential evidence remains incomplete

The paired profiler currently compares aggregate workload output. It does not retain the per-track post-warm-up selection information needed to establish equal media work.

Required report fields:

- subscriber identity and track SID;
- requested/max/desired/current layer;
- selected RID/SSRC;
- decoded dimensions from a client-observed probe;
- successful bytes/sec;
- selector PLI count and selection transition count;
- secondary driver-channel wait/backpressure evidence.

Oxide server internals cannot provide the equivalent Go selection state. A fair Go/Oxide differential report requires client-observed per-track data or separately scoped Go instrumentation.

## Validation completed for the current slice

```sh
cargo fmt --all -- --check
cargo check -p oxidesfu-signaling
cargo test -p oxidesfu-signaling --lib
cargo test -p oxidesfu-test \
  rust_sdk_room_simulcast_video_quality_switch_preserves_video_delivery_contract \
  -- --nocapture
```

The focused signaling suite passed with `508 passed, 3 ignored` after the temporal-controller slice. The focused native SDK quality-transition, concurrent spatial-isolation, and concurrent FPS-isolation contracts passed serially. Full workspace testing and clippy remain required after the remaining work above is implemented; known unrelated workspace flakes must be reported separately.

## Completion criteria

This work should be called complete only when:

1. allocation can set desired and maximum spatial/temporal targets independently;
2. temporal allocator transitions have end-to-end delivery coverage in addition to the current FPS-derived controller tests;
3. source switching is decodable for all supported scalable/simulcast codec paths, including dependency descriptors where applicable;
4. source availability, fallback, and retry behavior are bounded and observable;
5. concurrent real subscribers prove independent low/high decoded dimensions and isolated updates;
6. paired Go/Oxide runs capture comparable post-warm-up per-track delivery evidence; and
7. focused tests, workspace tests, and clippy have documented outcomes.

# Simulcast layer-selection implementation status

**Status:** implementation complete for the target-local simulcast and one-SSRC scalable forwarding slice; remaining items are independent evidence and workspace-hygiene follow-up, not known forwarding-path gaps.

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
| WebRTC Rust compatibility fork (published/pinned) | outer `2133ab09ae3681872b7a98773bd56d682056ed87`, nested RTC `69571fe` | `rtc-rtp/src/codec/{vp9,h264,av1}`, `rtc-rtp/src/extension/dependency_descriptor_extension/{mod.rs,dependency_descriptor_extension_test.rs}` | Provides codec switch-boundary parsing, frame/chain dependency metadata, and raw dependency-descriptor active-target-mask rewriting while preserving all unrelated descriptor bits. |
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

### Runtime codec reconciliation and VP9 forwarding

A browser publisher can initially register a generic video track before the first primary RTP SSRC reveals its negotiated codec. The reader now reconciles that actual primary RTP codec before forwarding it: it updates the published track's MIME/codec metadata, tears down stale subscriber senders, recreates them with the resolved codec, and renegotiates before forwarding the next packet. The triggering packet is deliberately dropped rather than sending VP9 RTP through a sender negotiated as VP8.

`oxidesfu-rtc::PeerConnection` now treats `video/vp9` as a first-class forwarding codec: it uses the pinned RTC's VP9 profile 0 (`PT 98`, `profile-id=0`) and constrains both dual-PC and single-PC forwarding transceivers to VP9 when the resolved track requires it. The browser harness explicitly requests `VP9` with `L3T3_KEY` for the scalable-video contract.

The first fresh-server Firefox run exposed one further reader integration issue: a one-SSRC VP9 SVC source has no simulcast SSRC/RID quality catalog, so every packet was counted as `drop_unknown_layer`. The reader now classifies known VP9/AV1 tracks without an advertised SSRC/RID catalog as an explicit **single scalable source**. This mode acquires one source only on a decodable boundary, keeps its source liveness independent of packet-level spatial IDs, and requires a new decodable boundary after a stale source SSRC is replaced. It deliberately does **not** reinterpret VP9/AV1 packet spatial IDs as simulcast source quality: those are scalable decode targets and require descriptor-aware decode-target forwarding rather than the source selector. Ambiguous non-scalable input remains observable and is not silently selected.

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
- a persistently unusable target has a bounded PLI request budget;
- a one-SSRC scalable source is acquired at a decodable boundary without conflating packet spatial metadata with simulcast policy;
- a stale one-SSRC scalable source cannot be replaced until the new source reaches a fresh decodable boundary.

The native SDK contract in `crates/oxidesfu-test/src/probes/media.rs` now additionally verifies that:

- a low-quality decoded frame is smaller than a high-quality decoded frame;
- a low → high update recovers larger decoded dimensions;
- media remains decodable throughout the transition.

## Known missing work

### 1. Allocation policy, bandwidth/layout producer, and end-to-end transitions are complete

`TrackAllocationStore` is now an independent, private, target-scoped allocation input. It publishes semantic revisioned changes to the reader that owns each `ForwardTarget`; the reader merges its desired quality with `UpdateTrackSettings` maximum quality and clamps desired to that ceiling. Allocation changes therefore use the same keyframe-gated selector transition as subscription changes without mutating client permission/settings.

The store defaults to no allocation, making `desired = max` and preserving current behavior. Its store and pure policy tests prove target isolation, semantic change notification, downgrade, and maximum clamping.

A one-second timer-driven producer now reads each subscriber receiver's congestion-feedback `available_outgoing_bitrate`, divides it across eligible video subscriptions according to layout weight, and selects the highest advertised `VideoLayer.bitrate` that fits. A subscriber `UpdateTrackSettings` viewport area takes precedence for weight; otherwise the largest advertised layer dimensions are used. It writes semantic spatial and temporal targets through `TrackAllocationStore`; no RTP packet performs BWE lookup or allocation work.

The native Rust SDK allocation contract now uses a server test-support capacity override to drive the production one-second allocator through `2 Mbps → 100 kbps → 2 Mbps`. It proves a real subscriber's decoded dimensions and frame cadence downgrade then recover without calling SDK quality/FPS controls. The override is room/subscriber scoped, restores normal candidate-pair statistics when removed, and is not a client protocol or production configuration input.

### 2. Temporal target state and allocator temporal intent are complete

`SubscriberVideoTemporalController` is reader-local state in each `ForwardTarget`. For a requested FPS and receiver-observed temporal cadence it derives an explicit `TemporalLayerPolicy` with `max` and `desired`, admits only temporal IDs at or below that maximum, and records the highest currently forwarded temporal layer. A policy reduction clamps the current state without resetting spatial source selection or RTP rewrite history.

When source temporal metadata or cadence estimates are unavailable, the controller explicitly selects the existing timestamp gate. It does not guess that an unknown packet is a desired temporal enhancement layer. The timestamp gate is still used for an observed layer whose advertised cadence materially exceeds the requested FPS.

Covered deterministic tests:

- request-to-available-layer maximum clamping;
- high-temporal to low-temporal policy reduction;
- independent temporal decisions for two targets receiving identical packets;
- metadata-poor timestamp-gate fallback.

The native Rust SDK FPS-isolation contract also passes with the controller in the production reader.

`TrackAllocationStore` now also carries a bounded target-local desired temporal layer (`T0`–`T2`). The reader merges it with the FPS-derived maximum, clamps it to that maximum, and drops only enhancement layers above the allocator target. The default remains `desired = max` when no allocation is present.

The one-second receiver-bandwidth allocator supplies this temporal intent alongside spatial policy, with the same viewport-weighted per-subscription budget described above. The allocation transition contract also proves temporal downgrade and recovery by observing decoded cadence under the same production allocator changes.

### 3. Dependency-descriptor decode targets and wire output are complete; native SDK fixture boundary remains

Oxide is pinned to outer WebRTC `2133ab09ae3681872b7a98773bd56d682056ed87`, nested RTC `69571fe`.

`RemoteTrack` parses and retains a current-packet descriptor switch result per incoming SSRC. The forwarding reader consumes that result for VP9 and AV1: when descriptor metadata is available, a source transition requires both `first_packet_in_frame` and an active DTI `Switch` target. A parsed non-switchable descriptor deliberately overrides VP9/AV1 payload keyframe heuristics; it cannot trigger an unsafe fallback switch. When descriptor metadata is absent, the established codec-specific VP9/AV1 boundary detector remains the compatibility fallback. VP8 and H264 continue to use their codec-specific paths.

Covered regressions:

- nested RTC parser: an active descriptor `Switch` target is distinct from a frame boundary (nine parser tests);
- `oxidesfu-rtc`: both descriptor frame start and `Switch` are required for the exposed result;
- signaling: a descriptor `false` result blocks a VP9 payload-keyframe heuristic, a descriptor `true` result permits the transition, and absent metadata falls back to the payload detector.

The RTC integration regression now feeds stateful real RTP header-extension sequences into `RemoteTrackState`: a non-`Switch` frame start blocks selection, an active `Switch` frame start permits it, and a following descriptor-free packet cannot inherit stale eligibility. `RemoteTrack` additionally retains an owned current descriptor snapshot per SSRC, including active targets, target-layer mapping, DTIs, frame/chain differences, and chain protection; it clears the snapshot for a descriptor-absent packet. Each forwarding target now owns a bounded descriptor frame selector that applies its own spatial/temporal policy before legacy temporal admission, preserves direct/chain dependencies, and makes one decision for all fragments in a frame. Forwarding-facing VP9 and AV1 regressions continue to prove continuous outgoing sequence/timestamp translation across permitted source switches.

Live Firefox validation now passes against a freshly built local OxideSFU server: all three receiver-counter contracts pass, including `Firefox VP9 SVC receiver keeps decoding after adaptive quality churn`.

Native Rust SDK fixture boundary:

- Oxide's native probes are pinned to Rust SDK `9afe85bb8593c9e955de4ee4949706fc04699ed9`. It exposes `SvcEncodedVideoFrame` and `NativeVideoSource::capture_svc_encoded_frame`, and the local SDK-contract test validates a three-spatial-layer active `Switch` descriptor at this boundary.
- `crates/oxidesfu-test/fixtures/vp9-svc/` now contains reproducibly generated, independently decodable `320×180` and `1280×720` VP9 keyframes plus its `ffmpeg` generator. The VP9 `L3T3_KEY` low → high native contract now runs unignored with `NativeVideoSource::new_encoded` and `VideoEncoderBackend::PreEncoded`, so it exercises the SDK passthrough encoder rather than the raw-frame path.
- Delivery now passes after rust-sdks commit `2dc87b5cde110365aef8c090d19a50d0fdbec5ae` (preserve codec-specific packetization metadata in descriptor-aware passthrough frames). The full workspace run includes `rust_sdk_room_vp9_svc_quality_low_to_high_preserves_delivery_contract` passing.
- Oxide now pins this SDK fix from the published remote fork at `https://github.com/andreeco/rust-sdks.git` rev `2dc87b5cde110365aef8c090d19a50d0fdbec5ae`; the temporary local git URL pin has been removed.

### 4. Source liveness expiry is complete; decodability availability remains limited

The selector now ages observed SSRCs only from its 250 ms timer. A source with no RTP for two seconds expires; if it was current, the selector clears it and performs a bounded acquisition of a live permitted fallback. Once that fallback is locked, it does not resume timer-driven target acquisition or issue another selector PLI. A later desired-layer decodable boundary may still promote the target, which is concrete renewed availability rather than a packet-order switch. The deterministic `stale_current_source_reacquires_a_live_fallback_without_oscillation` regression covers both stability and this safe promotion.

Remaining limitation:

- availability currently means recently observed RTP, not independently verified decoder usability;
- descriptor switch targets protect source transitions, but are not yet used to establish complete decoder-usability availability semantics.

### 5. Production observability and profiler snapshot are complete

The existing three-second target heartbeat now reports reader-local maximum/desired/current spatial layers, maximum/desired/current temporal layers, selector acquisition state, waiting layer, acquisition age, remaining selector PLI budget, selected RID/SSRC, layer transitions, categorized spatial and temporal drops, selector PLI requests, rewrite drops, successful RTP packet count, successful rewritten payload bytes, and write errors. Counters update without locks, allocation, formatting, or clock reads in the RTP path.

The bounded reader-heartbeat snapshot now separately exports selector PLI sent/suppression reasons, downstream PLI/FIR received/scheduled/suppressed counters, RTP wire bytes and packets per reporting window, acquisition/fallback state and waiting information, layer policy/current selection, and forwarding outcomes. `GET /debug/forwarding-snapshots` returns bounded JSONL for local profiler collection; the paired profiler retains it as `oxide-forwarding-snapshot.jsonl` only for Oxide runs.

The first real paired sweep reached build completion but encountered an empty Go `perf.data` during post-processing before workload artifacts could be retained. The profiler now records this as a non-fatal missing CPU profile so a rerun can preserve media evidence.

### 6. Black-box target isolation is complete

`rust_sdk_room_simulcast_video_quality_isolated_per_subscriber_contract` publishes one real simulcast source to two native SDK subscribers, drains stale frames after settings propagation, and proves that every sampled low decoded dimension is lower than every sampled high dimension. It then upgrades only the low subscriber and proves that it recovers high dimensions without reducing the independent high subscriber.

### 7. Paired client-observed evidence is implemented; a real sweep and server-internal correlation remain

`tools/profiling/profile-paired-scale-sweep.sh` now runs a Rust SDK observer for every Go and Oxide point and writes a versioned `client-media-evidence.json` after an independent warm-up/window. The parity-safe report includes subscriber/publisher/track identifiers, received bytes/sec, decoded dimensions/frames, drops/discards, PLI/NACK counts, dimension transitions, and an explicit `backpressure.available = false` value rather than inventing a server metric for Go.

Remaining work:

A real one-run, 30-second paired sweep completed at `target/profiles/paired-mixed_room_high_simulcast_large-20260716T074243Z-2555c723`. The client-observed artifacts confirm the media remains unequal: Go video points were about `1.00–1.01 MB/s` per observer with `1280×720` video, while Oxide was about `0.21–0.46 MB/s` and included `320×150`/`320×180` video. Audio-only remained matched (`10.25` vs `10.31 KB/s`). This confirms the remaining divergence is still video selection/allocation/delivery, not the common audio path.

Remaining work:

Snapshot correlation identified a production publisher-demand defect: aggregate subscribed-quality updates considered only explicit subscription entries, excluding active default subscribers. A high default subscriber could therefore fail to request high simulcast demand from its publisher. The aggregate now includes active default subscribers, still excludes explicit unsubscribes, and excludes retained disconnected participants without a signal connection. The regression covers both active default demand and participant-leave recomputation.

A repeat one-run, 30-second paired sweep at `target/profiles/paired-mixed_room_high_simulcast_large-20260716T075350Z-228c19ba` closes the observed media gap: Oxide video observers receive about `0.97–0.98 MB/s` at `1280×720`, versus Go at about `1.00–1.01 MB/s`; audio remains matched. Some transient waiting snapshots remain during observer join, but the post-warm-up client-visible media now converges to high quality.

A further complete one-run, 30-second five-point sweep at `target/profiles/paired-mixed_room_high_simulcast_large-20260716T130224Z-ff0beac2` confirms the result: every video point has four observer tracks at `1280×720`; Go reports `1,000,805–1,003,199 B/s` and Oxide `974,209–1,001,683 B/s` aggregate observer video delivery. Audio-only remains matched (`10,267 B/s` Go, `10,301 B/s` Oxide). This is one retained paired round, not a capacity conclusion.

Remaining evidence work:

- run multiple paired rounds on an otherwise idle host and compare the retained artifacts before making CPU conclusions;
- use separately scoped Go instrumentation only if client-observed evidence is insufficient.

## Final wire-correctness slice (2026-07-16)

Commit `2daecd11` completes target-local dependency-descriptor rewriting. For descriptor-backed one-SSRC VP9/AV1 forwarding, Oxide now removes the publisher descriptor extension, rewrites/injects the selected active decode-target mask using the published RTC helper, installs it at the subscriber-negotiated dependency-descriptor extension ID, strips it when the subscriber did not negotiate the extension, and replaces the retransmission-cache representation with the final target-local packet. Source packets remain unchanged and different subscribers receive independent descriptor state.

Validation for this slice:

- `cargo test -p oxidesfu-rtc --lib`: 38 passed;
- `cargo test -p oxidesfu-signaling --lib`: 533 passed, 3 ignored;
- descriptor rewrite and retransmission focused regressions passed;
- fresh-server Firefox receiver suite: 3/3 passed, including VP9 `L3T3_KEY` SVC;
- `cargo check -p oxidesfu-rtc -p oxidesfu-signaling` and `git diff --check` passed.

The remaining implementation-side forwarding work is complete for this compatibility slice. The native SDK fixture corpus, descriptor injection, and native VP9 low→high end-to-end contract are now all passing. Repeated performance sweeps and workspace hygiene are evidence/maintenance work, not known forwarding correctness gaps.

## Validation completed for the current slice

```sh
cargo fmt --all -- --check
cargo check -p oxidesfu-signaling
cargo test -p oxidesfu-rtc --lib
cargo test -p oxidesfu-signaling --lib
cargo test -p oxidesfu-test \
  rust_sdk_room_simulcast_video_quality_switch_preserves_video_delivery_contract \
  -- --nocapture
```

The focused RTC suite passed with `38 passed`, including the VP9-only forwarding SDP regression; the focused signaling suite now passes with `534 passed, 3 ignored`. The dependency-descriptor-gated VP9/AV1 RTP-continuity regression passes. The browser harness production build passes, and a fresh-server Firefox run passed all three receiver-counter contracts, including the VP9 SVC quality-churn contract. Focused native SDK quality-transition, concurrent spatial-isolation, concurrent FPS-isolation, allocation-transition, and AV1 DD cadence contracts passed. The former signal-only quality-aggregate probe is superseded: upstream LiveKit sends dynacast demand after `SubscribedTrack.OnCodecNegotiated`, so Oxide now suppresses premature all-off demand until a compatible receiver is active and emits enabled demand at that activation boundary.

`cargo test --workspace` now passes with `119 passed, 9 ignored`. Strict Clippy now passes workspace-wide with `cargo clippy --workspace --all-targets -- -D warnings`, including the `oxidesfu-test` support suite after surgical lint cleanup.

## Completion criteria

This work should be called complete only when:

1. one-SSRC scalable forwarding remains covered by both Firefox VP9 SVC browser and native SDK descriptor-aware VP9 low→high contracts;
2. repeat paired Go/Oxide runs on an otherwise idle host before making CPU/capacity conclusions; and
3. retain docs and profiler artifacts for handoff continuity.

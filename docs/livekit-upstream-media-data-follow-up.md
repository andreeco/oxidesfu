# LiveKit upstream media and data conformance follow-up

_This document serves as a kind of memory for an LLM to fix issues._

## Status

After the owned-loopback TURN ICE-candidate fix in OxideSFU commit `97030583`,
the focused upstream LiveKit TURN contract passes. The latest recheck on
2026-07-15 distinguishes one reproducible single-worker external compatibility
gap from timing-sensitive failures:

1. media lifecycle during disconnect, same-identity rejoin, and republish
   (`TestMultinodePublishingUponJoining`).

`TestConnectionStats` is now resolved. In single-PC/v1, a client can reuse a
MID that previously carried a remote forwarding sender for its own publishing
section. Oxide now detaches that sender, clears its stale forwarding state, and
requeues the still-published remote track for a fresh receive section before
answering the publishing offer.

`TestMultiNodeUpdateAttributes` failed in the preceding 4-worker full suite,
but passed in a clean single-worker rerun for `v0`, `v0-single-peer-connection`,
and `v1`; classify it as shard-load/timing-sensitive until repeated-run evidence
proves otherwise. `TestDataPublishSlowSubscriber` passes as an isolated Rust
port and external LiveKit shard after the reliable writer was fixed to start
bitrate sampling at the first post-open write attempt. Its all-topology Rust
port remains explicitly run because parallel WebRTC-heavy crate tests can delay
its synthetic reader below the intended threshold.

The native Rust port
`upstream_livekit::singlenode::test_single_node_update_subscription_permissions`
failed once and passed once in consecutive serial isolated runs. Its timeout
waiting for data-track bytes is therefore also timing-sensitive; it must not be
reported as a deterministic regression without a stable reproduction.

## Current Oxide-side test changes

The current working-tree test change is limited to
`crates/oxidesfu-test/src/upstream_livekit/singlenode.rs`:

- replace separate one-track audio and video receive windows with one
  two-track receive window per participant;
- assert that each received set is exactly Opus plus VP8 RTP, including a
  non-empty RTP payload and RTP version 2, rather than accepting audio first
  and video later.

This prevents the local native test from passing when a later video
renegotiation has removed the earlier audio attachment. It passed on
2026-07-15 with:

```sh
cargo test -p oxidesfu-test test_connection_stats -- --nocapture
```

The test passed for `v0`, `v0-single-peer-connection`, and `v1`. It does not
replace the upstream Go `RTCClient` conformance test: the native harness's
publication and offer timing differs from Go's requirement-driven receive-only
renegotiation sequence. Keep the external `TestConnectionStats` shard as the
wire-compatibility regression.

## Reference revisions

| Repository | Revision | Inspected behavior |
| --- | --- | --- |
| OxideSFU | `e00e6b73` | Current documentation/rerun baseline; latest isolated external and native-test evidence. |
| OxideSFU TURN fix | `97030583b587a4038c6d728b7297a866b6c9e185` | Corrected owned loopback TURN candidate configuration and runner isolation. |
| LiveKit server | `ae09b7d0ad94d764f0c97d183efd36476163e819` | Upstream `./test` contracts and media/data lifecycle behavior. |
| Pion WebRTC | `6fbce156e0de9764f1ce46ac581c0469ec1d7a04` | Relay candidate gathering and SCTP buffered-amount primitives. |
| turn-rs | `79d1bc2a0b92329df51f827036d284ad577ca1ff` | Owned TURN runtime used by OxideSFU. |

## Shared reproduction setup

Run from the `oxidesfu/` repository root. The absolute log directory prevents
the LiveKit checkout's working-directory change from relocating artifacts.

```sh
export OXIDESFU_DISCOVERY_LOG_DIR=/home/andre/rustprojects/oxidesfu/target/conformance-investigation
export OXIDESFU_DISCOVERY_LIVEKIT_SHARD_WORKERS=1
```

Run one test at a time:

```sh
OXIDESFU_DISCOVERY_GO_TEST_RUN_PATTERN='^TestMultinodePublishingUponJoining$' \
  bash tools/conformance/livekit-full-suite-all.sh

OXIDESFU_DISCOVERY_GO_TEST_RUN_PATTERN='^TestConnectionStats$' \
  bash tools/conformance/livekit-full-suite-all.sh

OXIDESFU_DISCOVERY_GO_TEST_RUN_PATTERN='^TestDataPublishSlowSubscriber$' \
  bash tools/conformance/livekit-full-suite-all.sh

OXIDESFU_DISCOVERY_GO_TEST_RUN_PATTERN='^TestMultiNodeUpdateAttributes$' \
  bash tools/conformance/livekit-full-suite-all.sh

cargo test -p oxidesfu-test \
  upstream_livekit::singlenode::test_single_node_update_subscription_permissions \
  -- --nocapture --test-threads=1
```

The runner prints the exact per-shard `go-test.log` location. Preserve that
log before beginning a new investigation; it contains the upstream assertion
that failed.

## Resolved prerequisite: owned local TURN

The earlier `TestTurnRelay/allow` failure was caused by a non-routable server
host candidate, not by `turn-rs`:

- `crates/oxidesfu-server/src/config.rs` widened a loopback signaling bind to
  `0.0.0.0` for RTC UDP.
- The pinned WebRTC gatherer serialized that wildcard address in the server
  candidate.
- Pion successfully allocated a relay candidate but could not form a candidate
  pair with server candidate `0.0.0.0:<port>`.

Commit `97030583` keeps the concrete loopback RTC bind when OxideSFU owns the
loopback TURN listener. The focused upstream contract now passes:

```text
TestTurnRelay/allow: PASS
TestTurnRelay/not-allowed: PASS
TestTurnRelay/denied-overrides-allowed: PASS
```

The conformance runner was also corrected to configure and advertise an owned
TURN endpoint only for TURN shards. Non-TURN shards previously received an
unreachable advertised TURN URL.

## Gap 1: media disconnect, rejoin, and republish

### Failing contract

`livekit/test/multinode_test.go:TestMultinodePublishingUponJoining` delegates
to `livekit/test/scenarios.go:scenarioPublishingUponJoining`.

The scenario connects three clients, has two publish audio/video, disconnects
one publisher, verifies its tracks disappear, reconnects that publisher under
the same identity, republishes, and expects existing subscribers to attach to
the replacement tracks.

### Isolated result

Latest rerun timestamp: `2026-07-15 16:06`; shard log:
`target/conformance-investigation/fix-rejoin-coalesced/livekit-shards-20260715-160632/TestMultinodePublishingUponJoining/go-test.log`.

```text
FAIL TestMultinodePublishingUponJoining (94.03s)
  FAIL v0:
    c3 should be subscribed to 2 tracks from c2, actual: 1
  FAIL v0-single-peer-connection:
    c3 should be subscribed to 0 tracks from c2, actual: 2
  FAIL v1:
    c3 should be subscribed to 0 tracks from c2, actual: 2
```

The v0 log still shows replacement c2 audio reaching c3 while replacement
video is initially `inactive`. More importantly, the single-PC and v1 results
now show that c2's original two tracks are not removed from c3 after disconnect.
The remaining defect is therefore distributed disconnect/unpublish lifecycle
propagation, not a generic SDP-offer debounce. The local reconnect-grace path
in `router.rs` removes tracks locally; trace why the non-owner topology does
not receive the corresponding participant update and forwarding cleanup before
same-identity rejoin.

### Upstream behavior to preserve

LiveKit closes the former `MediaTrack`, removes its track SID from the
participant and room track manager, broadcasts updated participant state, and
removes associated subscriber downtracks. A replacement publication gets a
fresh track SID and is auto-subscribed as a new source.

Relevant upstream files:

- `livekit/pkg/rtc/participant.go` — media-track setup and removal;
- `livekit/pkg/rtc/uptrackmanager.go` — published-track ownership;
- `livekit/pkg/rtc/mediatrackreceiver.go` — receiver close callbacks;
- `livekit/pkg/rtc/mediatracksubscriptions.go` — downtrack close/removal;
- `livekit/pkg/rtc/room.go` — `onTrackPublished` and `onTrackUnpublished`.

### Repair plan

1. Existing router coverage (`livekit_multinode_publishing_upon_joining_contract_for_c3_track_counts`)
   proves a manually staged room/forward-track lifecycle, but it passes and
   does not drive live peer-connection teardown. Add a real WebRTC integration
   regression for initial attachment, old-track removal after disconnect, and
   fresh attachment after same-identity republish.
2. Trace state keyed by both participant SID and publication/track SID. A
   stale reader, forwarding lease, subscription, or sender must not survive a
   replacement participant session.
3. Verify removal broadcasts update all subscribers and trigger their sender /
   downtrack cleanup before accepting the fresh publication.
4. Run the upstream scenario in isolation and then with all `v0`,
   `v0-single-peer-connection`, and `v1` modes.

Do not treat the existing narrow inactive-SDP unpublish handling as sufficient:
it only covers one unbound dual-PC reconciliation case.

## Resolved: single-PC simultaneous media subscriptions

### Former failing contract

`livekit/test/singlenode_test.go:TestConnectionStats` first requires both
participants to publish audio and video and receive both tracks from each
other. The analytics-stat checks happen only after this observable media
precondition succeeds.

### Isolated result

The former failure was reproduced in v0-single-PC and v1, then fixed with
`single_pc_local_publisher_mid_collision_reclaims_remote_forward_and_requeues_audio`
in `crates/oxidesfu-signaling/src/router/tests.rs`. That regression failed
before the repair and verifies that a repurposed local publishing MID removes
the old remote forwarding row, detaches its sender, and requeues the remote
audio track.

External validation after the repair:

```text
PASS TestConnectionStats
```

The isolated upstream shard is under
`target/conformance-investigation/fix-single-pc-connection-stats/` and passes
all v0, v0-single-PC, and v1 modes.

### Upstream behavior to preserve

Classic v0 creates separate publisher and subscriber peer connections. Both
v0-with-`join_request` and `/rtc/v1` use one combined peer connection, so
server-to-client track attachment, sender negotiation, and initial RTCP
handling must use that combined publisher-primary connection.

Relevant upstream files:

- `livekit/pkg/service/rtcservice.go` — v0/v1 join parsing;
- `livekit/pkg/rtc/transportmanager.go` — PC topology and `AddTrackLocal`;
- `livekit/pkg/rtc/participant.go` — single-PC setup;
- `livekit/test/integration_helpers.go` — test mode matrix.

### Repair delivered

1. Keep the native `test_connection_stats` as a fast RTP regression.
2. Keep the router-level MID-reuse regression as the deterministic signaling
   guard.
3. Keep the external `TestConnectionStats` shard as the authoritative Go/Pion
   wire-compatibility check.

## Resolved: slow reliable-data subscriber contiguity

`livekit/test/singlenode_test.go:TestDataPublishSlowSubscriber` creates a
publisher, fast subscriber, slow-but-not-dropping subscriber, and
slow-dropping subscriber. It requires a drop and publisher backpressure for the
below-threshold reader while preserving ordered delivery for the fast and
above-threshold readers.

Oxide created `SendRateSamples` when it created the server `_reliable` channel,
before SDP negotiation and data-channel open. The first post-open write was
therefore measured against pre-open idle time; a later timeout could
misclassify an above-threshold receiver as slow and drop one of its reliable
packets.

`crates/oxidesfu-rtc/src/data_channel.rs` now starts sampling lazily at the
first post-open write attempt. It also makes every reliable packet take a fresh
write/deadline/rate decision rather than retaining a prior slow-reader drop as
pre-write drop state. This matches the upstream writer lifecycle and policy:

- `livekit/pkg/rtc/transport.go` creates reliable writers on data-channel open;
- `livekit/pkg/sfu/datachannel/datachannel_writer.go` retries each timed-out
  write while bitrate is unknown or at least the configured threshold;
- `livekit/test/singlenode_test.go` defines the fast, above-threshold, and
  below-threshold delivery contract.

The Rust port
`upstream_livekit::singlenode::test_data_publish_slow_subscriber` passes for
v0 dual-PC, v0 single-PC, and v1 when run in isolation. It remains explicitly
run because concurrent WebRTC-heavy crate tests can delay its synthetic reader
below its intended threshold. The focused three-subscriber relay regression is
normal signalling test coverage, and the external LiveKit shard is the
cross-process compatibility gate.

## Non-goals and safety notes

- Do not revert the owned-loopback TURN fix to make these tests pass; TURN is
  now independently validated by the three `TestTurnRelay` policy cases.
- Do not modify `turn-rs` for these media/data failures. The owned TURN test
  and upstream forced-relay contract pass.
- Keep the focused native integration change and this note uncommitted until
  they are grouped with the corresponding implementation fix, unless a
  maintainer explicitly requests a separate documentation/test commit.

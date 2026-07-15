# LiveKit upstream media and data conformance follow-up

## Status

After the owned-loopback TURN ICE-candidate fix in OxideSFU commit `97030583`,
the focused upstream LiveKit TURN contract passes. The latest recheck on
2026-07-15 at OxideSFU `HEAD` `e00e6b73` distinguishes two reproducible
single-worker external compatibility gaps from timing-sensitive failures:

1. media lifecycle during disconnect, same-identity rejoin, and republish
   (`TestMultinodePublishingUponJoining`);
2. single-peer-connection media forwarding when both participants publish
   (`TestConnectionStats`).

`TestMultiNodeUpdateAttributes` failed in the preceding 4-worker full suite,
but passed in a clean single-worker rerun for `v0`, `v0-single-peer-connection`,
and `v1`; classify it as shard-load/timing-sensitive until repeated-run evidence
proves otherwise. `TestDataPublishSlowSubscriber` also passed its latest
single-worker rerun, so it is a watch item rather than a currently reproducible
reliable-data compatibility gap.

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

Rerun timestamp: `2026-07-15 15:51`; shard log:
`target/conformance-investigation/isolated-multinode-publishing/livekit-shards-20260715-155138/TestMultinodePublishingUponJoining/go-test.log`.

```text
FAIL TestMultinodePublishingUponJoining (93.73s)
  FAIL v0:
    c3 should be subscribed to 2 tracks from c2, actual: 1
  FAIL v0-single-peer-connection:
    did not receive tracks from c1
  FAIL v1:
    did not receive tracks from c1
```

The v0 log shows that replacement c2's audio reaches c3 while its replacement
video section is offered as `inactive`; c3 therefore observes only one of the
two replacement tracks. The single-PC and v1 variants fail earlier in their
combined-PC lifecycle, so they require separate topology-aware tracing.

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

## Gap 2: single-PC simultaneous media subscriptions

### Failing contract

`livekit/test/singlenode_test.go:TestConnectionStats` first requires both
participants to publish audio and video and receive both tracks from each
other. The analytics-stat checks happen only after this observable media
precondition succeeds.

### Isolated result

Rerun timestamp: `2026-07-15 15:50`; shard log:
`target/conformance-investigation/isolated-connection-stats/livekit-shards-20260715-155013/TestConnectionStats/go-test.log`.

```text
FAIL TestConnectionStats (63.30s)
  PASS v0
  FAIL v0-single-peer-connection (31.61s):
    c2 did not subscribe to both tracks from c1
  FAIL v1 (31.45s):
    c2 did not subscribe to both tracks from c1
```

This is not an analytics-statistics defect. It is a single-PC/v1 forwarding or
negotiation failure when two participants publish concurrently.

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

### Repair plan

1. The native `test_connection_stats` now asserts two concurrent tracks in
   both directions for every topology. Keep it as a fast local regression.
2. Add or adapt a protocol-level integration harness that reproduces the Go
   client's sequential `MediaSectionsRequirement` / receive-only offer flow;
   the native test alone currently passes.
3. Trace the connection kind used for `ensure_subscriber_forwarding`, sender
   attachment, offer generation, answer acceptance, and remote-track-to-track
   correlation.
4. Ensure each single-PC subscriber negotiation includes both media kinds and
   that its result is not overwritten by concurrent publication negotiation.
5. Prove the fix with isolated `TestConnectionStats`, then a broader single-PC
   media suite.

## Watch item: classic v0 slow reliable-data subscriber

### Contract under observation

`livekit/test/singlenode_test.go:TestDataPublishSlowSubscriber` creates a
publisher, fast subscriber, slow-but-not-dropping subscriber, and
slow-dropping subscriber. It expects the server to drop for the slow reader,
report the corresponding writer error/backpressure, and retain ordered delivery
for the fast and eligible slow subscribers.

### Isolated result

Latest rerun timestamp: `2026-07-15 15:53`; shard log:
`target/conformance-investigation/isolated-slow-subscriber/livekit-shards-20260715-155318/TestDataPublishSlowSubscriber/go-test.log`.

```text
PASS TestDataPublishSlowSubscriber
```

The preceding isolated v0 connection failure did not reproduce in this rerun.
Do not claim that the reliable-writer policy is fixed: retain this as a
repeat-run watch item, and investigate only if it becomes reproducible again.

### Upstream behavior to preserve

LiveKit enables data-channel block-write when a slow threshold is configured
and wraps every reliable/unlabeled data channel in a reliable data-channel
writer. The writer measures slow-reader behavior and surfaces
`ErrDataDroppedBySlowReader`; its policy must apply regardless of whether the
reliable channel resides on a separate subscriber PC or a combined PC.

Relevant upstream files:

- `livekit/pkg/rtc/transport.go` — peer connection and channel writer setup;
- `livekit/pkg/rtc/transportmanager.go` — result handling;
- `livekit/pkg/sfu/datachannel/datachannel_writer.go` — reliable writer policy;
- `livekit/test/singlenode_test.go` — contract assertion.

Oxide currently configures buffered thresholds in
`crates/oxidesfu-signaling/src/router/session.rs` during subscriber channel
creation and on incoming data-channel registration. That code remains the
likely next policy area only after the v0 data-only connection setup works;
the current focused failure does not yet reach the writer path.

### Repair plan

1. If the v0 data-only dual-PC connection failure reappears, capture the
   initial server subscriber offer and establish whether either slow subscriber
   timed out before the writer policy was exercised.
2. Repair or replace the existing ignored real-WebRTC port
   (`upstream_livekit::singlenode::test_data_publish_slow_subscriber`) so a
   reproducible connection phase can become a focused Oxide regression.
3. Once all data-only peers connect, use that regression with a configured slow
   threshold and an explicit reliable channel on the subscriber PC.
4. Then verify the server selects the channel used by the v0 forwarding path,
   not a publisher or stale replacement channel, and that the threshold
   produces the expected slow-reader/drop signal while the fast subscriber
   remains ordered.
5. Run isolated `TestDataPublishSlowSubscriber`, then the complete data-track
   and data-packet suite.

## Non-goals and safety notes

- Do not revert the owned-loopback TURN fix to make these tests pass; TURN is
  now independently validated by the three `TestTurnRelay` policy cases.
- Do not modify `turn-rs` for these media/data failures. The owned TURN test
  and upstream forced-relay contract pass.
- Keep the focused native integration change and this note uncommitted until
  they are grouped with the corresponding implementation fix, unless a
  maintainer explicitly requests a separate documentation/test commit.

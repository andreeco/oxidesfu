# Temporary handoff: actor-owned OxideSFU media plane

> Updated 2026-07-17 after the first production ownership extraction. This is an operational continuation note, not a claim that the complete actor/shard/egress design is implemented.

## Current repository state

- Repository: `/home/andre/rustprojects/oxidesfu`
- Branch: `master`
- Current implementation commit: `0a34aa6d refactor: own RTP state in forwarding targets`
- Durable architecture/evidence note: `docs/forwarding-performance-plan.md`

The first media-plane ownership slice is committed. The larger actor-owned end state is **not complete**.

## Completed work

### Correctness and benchmark baseline

- Fixed Go SDK one-layer video descriptors being incorrectly treated as simulcast.
- Benchmarks require RTP for every expected subscriber/track pair and reject implausible Go/Rust delivery differences.
- Validated high mixed H.264 simulcast delivery: both implementations delivered all 160 expected tracks with nonzero RTP and zero reported loss.
- High mixed CPU remains the real performance problem, not a missing-media artifact.

### Production ownership extraction (`0a34aa6d`)

`ForwardTarget` in `crates/oxidesfu-signaling/src/router/session.rs` now owns:

```rust
rtp_state: SubscriberRtpState
```

The serialized publisher-track reader owns both `RemoteTrackEvent::RtpPacket` and `RemoteTrackEvent::RtcpPacket`, so the following target-local state now has one writer without a hot-path `Arc<Mutex<_>>` or compound-key map lookup:

- RTP sequence/timestamp rewriting and duplicate history;
- retransmission cache;
- PLI/FIR throttle and FIR sequence counters;
- sender-report timestamp/SSRC mapping;
- receiver-report/TWCC feedback and quality recommendation state.

`media/rtcp.rs` now accepts `&mut SubscriberRtpState`. The RTCP branch refreshes the cached target vector before dispatch, iterates that vector directly, and no longer creates forwarding state from delayed RTCP for a removed target.

The retransmission cache is replaced after final target-local descriptor/extension mutation, so NACK replays the packet representation actually sent to the target.

`SignalState.rtp_forwarding` and all production `RtpForwardingStore` cleanup/context plumbing have been removed. A `#[cfg(test)]` compatibility fixture remains only for legacy state-algorithm tests and must not be reintroduced into production code.

### Target incarnation attempt

`ForwardTrackStore` now assigns a monotonic incarnation on insertion and the reader retains target RTP state only when both key and incarnation match. This was intended to distinguish a same-lifetime transport refresh from remove/re-add/replacement.

## Critical unfinished correctness work

Do **not** start target shards, egress actors, compact retransmission metadata, or `webrtc-rs` changes yet.

The current `ForwardTrackStore` lifecycle data is split across independent mutexes:

- `tracks`;
- `target_incarnations`;
- `active`;
- `active_by_track`.

That makes the current incarnation implementation racy. A concurrent insert/remove/list can produce a new incarnation paired with an old transport, or an old cleanup can remove a just-inserted replacement. The incarnation map also is not removed on every target removal path.

### Required next slice: atomic forwarding-target lifecycle

Replace those separate lifecycle maps with one private store state guarded by one mutex:

```rust
struct ForwardTrackStoreState {
    tracks: HashMap<ForwardTrackKey, LocalRtpTrack>,
    target_incarnations: HashMap<ForwardTrackKey, u64>,
    active: HashSet<ForwardTrackKey>,
    active_by_track: HashMap<ForwardTrackReaderKey, HashSet<ForwardTrackKey>>,
}
```

Requirements:

1. Insert, activate, deactivate, remove, and list an active target atomically with respect to this lifecycle state.
2. `list_for_track_with_incarnation` must return a coherent `(key, transport, incarnation)` tuple from one state snapshot.
3. Every removal path must remove the target incarnation in the same lifecycle operation:
   - single key;
   - subscriber MID;
   - publisher track;
   - publisher departure;
   - participant removal.
4. Bump the store revision only after a fully committed lifecycle mutation.
5. Keep reader leases (`started`) separate unless their semantics require an explicit lifecycle handoff.
6. Add deterministic tests for same-key replacement, remove/re-add, bulk removal, and incarnation-map reclamation. A test-only synchronization hook is acceptable for forced interleavings.

### Required refresh ordering correction

`refresh_forward_targets_for_track` currently flushes pending video batches before it validates the new lifecycle snapshot. Refactor it to reconcile first:

- same key + same incarnation: retain state and flush the retained target batch;
- missing target or changed incarnation: discard old pending batch and state without writing to its old transport;
- new snapshot target: create fresh state.

The reader’s keyframe-retry and allocation timer branches must also refresh when the forwarding revision changed before they inspect cached targets; otherwise a removed target can receive timer-driven selector work until another event or the debug heartbeat arrives.

## Later slices — still not implemented

1. **Packet instrumentation**: sampled counters for clone/cache bytes, NACK hits/misses, target write time, driver queue fallback/wait, and batch sizes.
2. **In-order RTP fast path**: retain duplicate/rollover/source-switch behavior while avoiding HashSet/VecDeque work for ordinary contiguous packets.
3. **Compact retransmission**: only after a bounded source-packet retention/lookup design proves final target packets can be reconstructed exactly.
4. **Stable bounded target shards**: only if post-instrumentation measurement shows serial source-reader fanout is materially blocking. No per-packet spawning.
5. **Subscriber egress/pacer**: conditional on measured driver/core contention after the preceding slices. Existing bounded WebRTC driver queues may already be sufficient.
6. **Paired performance conclusion**: repeated delivery-validated Go/Rust runs using process CPU plus `cycles:u`, instructions, context switches, and packet-plane counters.

## Validation already completed for `0a34aa6d`

```bash
cargo fmt --all
cargo test -p oxidesfu-signaling --lib
# 548 passed, 2 ignored
cargo clippy -p oxidesfu-signaling --all-targets -- -D warnings
git diff --check
```

## Reference map

- Oxide: `crates/oxidesfu-signaling/src/router/session.rs`, `media/rtp_forwarding.rs`, `media/rtcp.rs`, `stores/forwarding.rs`, `state.rs`.
- LiveKit source baseline: `ae09b7d0ad94d764f0c97d183efd36476163e819`.
  - `pkg/sfu/downtrack.go`
  - `pkg/sfu/sequencer.go`
  - `pkg/sfu/utils/downtrackspreader.go`
  - `pkg/rtc/mediatrack.go`
- WebRTC inspection checkout used in prior analysis: `bede31c803e25f4f06830725236efd89425bec8f`.

## Safety constraints

- TDD before each behavior change.
- Preserve LiveKit wire behavior; do not mechanically port Go goroutines.
- Keep state owner ordering: RTP and RTCP for one target must remain ordered.
- No unbounded queues, per-packet task spawning, or new public API without a focused design and compatibility tests.
- Run format, focused tests, full signaling tests, and clippy before committing each slice.

# Rust SDK media unpublish investigation

## Status

The Rust SDK media unpublish/republish lifecycle contract currently fails against OxideSFU:

```text
cargo test -p oxidesfu-test \
  rust_sdk_room_publisher_unpublish_then_republish_audio_emits_clean_remote_lifecycle \
  -- --nocapture
```

On 2026-07-15 the test consistently timed out while Bob waited for the first
track's `RoomEvent::TrackUnpublished`.

This is **not a regression from the 2026-07-15 WebRTC/Rust SDK dependency
rebase**. The identical focused test failed at pre-rebase OxideSFU commit
`6b414bce6527a30c9f5bf6133c861f95b5703639`, using `webrtc-rs`
`3fa6182fa8f8a9f4eafa047e83880115c905714f` and Rust SDK
`d5e98afd78fcf4129393ab75cc9436c564a6f105`.

## Reference map

### Rust SDK

Inspected the OxideSFU-pinned fork at `88aab601eac42fbba83e196a9b30206cf63945b8`
(`rebase/rust-sdks-compat-2026-07-15-upstream-main`) and these available
branches/revisions:

| Ref | Revision |
| --- | --- |
| `main`, `oxidesfu/compat`, `andreeco/main`, backup | `d5e98afd78fcf4129393ab75cc9436c564a6f105` |
| `rebase/rust-sdks-compat-2026-07-15-upstream-main` | `88aab601eac42fbba83e196a9b30206cf63945b8` |
| `origin/main` | `4724cbb1622da1924c57f60813199b14bd336628` |
| `feature/oxidesfu-raw-rtp-rtcp` | `8ead5c37f5bc7e0c83673edbab62047fd3110325` |

Relevant files are `livekit/src/room/participant/local_participant.rs`,
`livekit/src/rtc_engine/rtc_session.rs`,
`livekit/src/room/participant/remote_participant.rs`, and
`livekit/src/room/mod.rs`.

All inspected branches use the same protocol behavior:

1. `LocalParticipant::unpublish_track` removes the RTP sender and requests a
   publisher renegotiation.
2. The signaling session does **not** handle the standalone
   `SignalResponse::TrackUnpublished` message.
3. A remote `TrackUnpublished` event is emitted only when a
   `SignalResponse::Update` contains the publisher with the old track SID
   absent. `RemoteParticipant::update_info` then removes the publication.

Thus adding a standalone `TrackUnpublished` response in OxideSFU would not fix
this client contract. OxideSFU must send an updated `ParticipantInfo` without
the track.

### Upstream LiveKit server

Inspected LiveKit commit `ae09b7d0ad94d764f0c97d183efd36476163e819`:

- `pkg/rtc/participant.go`: a published media track registers an `AddOnClose`
  callback that invokes `ParticipantListener.OnTrackUnpublished` when the
  media track closes.
- `pkg/rtc/room.go`: `Room.onTrackUnpublished` removes the track from the track
  manager and broadcasts the publisher's participant state to peers (excluding
  the source).
- `pkg/rtc/participant_signal.go`: `sendTrackUnpublished` is used for a
  server-initiated unpublish to the *publisher* (for example when permission is
  revoked); it is not the mechanism by which Rust SDK subscribers learn that a
  remote track disappeared.

## OxideSFU finding

OxideSFU's
`reconcile_publisher_media_tracks_after_answer` in
`crates/oxidesfu-signaling/src/router/session.rs` removes a publication and
broadcasts its participant update only when the publication MID occurs in the
new offer with `a=inactive`. The resulting `ParticipantInfo` has a newer
version, so it would correctly drive the Rust SDK remote lifecycle event.

The test fails before that broadcast reaches Bob. The probable mismatch is
that the Rust SDK's `remove_track` renegotiation preserves the media section as
`recvonly` (or otherwise not `inactive`), while OxideSFU deliberately rejects
that form as an unpublish signal to avoid deleting tracks for Firefox's
single-PC receive/reserve sections.

This direction detail is an inference from the failing end-to-end test and
source paths; capture the publisher's post-unpublish SDP before changing the
rule.

## Next step

Add a regression test using the actual Rust SDK post-`remove_track` SDP (or a
minimal captured equivalent). It should prove the exact section direction and
then adjust the reconciliation rule only for the unambiguous dual-PC publisher
case. Preserve the existing single-PC behavior for browser receive/reserve
sections. The test must assert that the removal causes a participant update
without the old SID and that Bob receives exactly one `TrackUnpublished`.

# Native upstream LiveKit test ports

This directory contains Rust-native, logic-preserving ports of upstream
`livekit/test/*` contracts. The upstream Go test is the behavioral reference;
native tests execute only through OxideSFU/Rust code—never by shelling out to
`go test` or wrapping `tools/conformance/livekit-full-suite-all.sh`.

## Contract matrix

The matrix contains every upstream contract that passed in the latest external
Go conformance baseline. The external result and native result are independent:
an external pass is evidence of server compatibility, not proof that the native
port is complete.

A native row may be marked **Pass** only when all of the following are true:

1. its Rust test preserves the upstream contract and relevant helper/scenario
   behavior;
2. it has focused passing validation; and
3. it is not known to be blocked by an OxideSFU or harness behavior gap.

`Blocked` means a native test exists but does not satisfy that standard. Do not
promote it based on an external pass. `Not ported` means no native equivalent
has been verified yet.

Native test names below are relative to `upstream_livekit`; run them with
`cargo test -p oxidesfu-test --features harness-e2e <native test> -- --exact --nocapture`.

| Upstream LiveKit contract | External Go baseline | Native Rust port | Native status |
| --- | --- | --- | --- |
| `TestAgentMultiNode` | Pass | `agents::test_agent_multi_node` | Pass |
| `TestAgentNamespaces` | Pass | `agents::test_agent_namespaces` | Pass |
| `TestAgents` | Pass | `agents::test_agents` | Pass |
| `TestAutoCreate` | Pass | `singlenode::test_auto_create` | Pass |
| `TestClientConnectDuplicate` | Pass | `singlenode::test_client_connect_duplicate` | Pass |
| `TestClientCouldConnect` | Pass | `singlenode::test_client_could_connect` | Pass |
| `TestCloseDisconnectedParticipantOnSignalClose` | Pass | `multinode::test_close_disconnected_participant_on_signal_close` | Pass |
| `TestConnectWithoutCreation` | Pass | `multinode::test_connect_without_creation` | Pass |
| `TestConnectionStats` | Pass | `singlenode::test_connection_stats` | Pass |
| `TestDataPublishSlowSubscriber` | Pass | `singlenode::test_data_publish_slow_subscriber` | Blocked — timing-sensitive above-threshold contiguity assertion failed in the full serial run and focused rerun; external contract passes |
| `TestDeviceCodecOverride` | Pass | `singlenode::test_device_codec_override` | Pass |
| `TestFireTrackBySdp` | Pass | `singlenode::test_fire_track_by_sdp` | Pass |
| `TestMultiNodeCloseNonRTCRoom` | Pass | `multinode::test_multi_node_close_non_rtc_room` | Pass |
| `TestMultiNodeDataBlob` | Pass | `multinode::test_multi_node_data_blob` | Pass |
| `TestMultiNodeJoinAfterClose` | Pass | `multinode::test_multi_node_join_after_close` | Pass |
| `TestMultiNodeMutePublishedTrack` | Pass | `multinode::test_multi_node_mute_published_track` | Pass |
| `TestMultiNodeRefreshToken` | Pass | `multinode::test_multi_node_refresh_token` | Pass |
| `TestMultiNodeRemoveParticipant` | Pass | `multinode::test_multi_node_remove_participant` | Pass |
| `TestMultiNodeRevokePublishPermission` | Pass | `multinode::test_multi_node_revoke_publish_permission` | Pass |
| `TestMultiNodeRoomList` | Pass | `multinode::test_multi_node_room_list` | Pass |
| `TestMultiNodeRouting` | Pass | `multinode::test_multi_node_routing` | Pass |
| `TestMultiNodeUpdateAttributes` | Pass | `multinode::test_multi_node_update_attributes` | Pass |
| `TestMultiNodeUpdateParticipantMetadata` | Pass | `multinode::test_multi_node_update_participant_metadata` | Pass |
| `TestMultiNodeUpdateRoomMetadata` | Pass | `multinode::test_multi_node_update_room_metadata` | Pass |
| `TestMultinodeDataPublishing` | Fail — data-track publishing upon joining | `multinode::test_multinode_data_publishing` | Blocked — external `scenarioDataTracksPublishingUponJoining/testRTCServicePath=v0` fails |
| `TestMultinodePublishingUponJoining` | Fail — tracks remain after participant departure | `multinode::test_multinode_publishing_upon_joining` | Blocked — external contract fails for v0, single-PC, and v1 |
| `TestMultinodeReceiveBeforePublish` | Pass | `multinode::test_multinode_receive_before_publish` | Pass |
| `TestMultinodeReconnectAfterNodeShutdown` | Pass | `multinode::test_multinode_reconnect_after_node_shutdown` | Pass |
| `TestPingPong` | Pass | `singlenode::test_ping_pong` | Pass |
| `TestSingleNodeAttributes` | Pass | `singlenode::test_single_node_attributes` | Pass |
| `TestSingleNodeCORS` | Pass | `singlenode::test_single_node_cors` | Pass |
| `TestSingleNodeCloseNonRTCRoom` | Pass | `singlenode::test_single_node_close_non_rtc_room` | Pass |
| `TestSingleNodeDataBlob` | Pass | `singlenode::test_single_node_data_blob` | Pass |
| `TestSingleNodeDataBlobDisabled` | Pass | `singlenode::test_single_node_data_blob_disabled` | Pass |
| `TestSingleNodeDoubleSlash` | Pass | `singlenode::test_single_node_double_slash` | Pass |
| `TestSingleNodeJoinAfterClose` | Pass | `singlenode::test_single_node_join_after_close` | Pass |
| `TestSingleNodeRoomList` | Pass | `singlenode::test_single_node_room_list` | Pass |
| `TestSingleNodeUpdateParticipant` | Pass | `singlenode::test_single_node_update_participant` | Pass |
| `TestSingleNodeUpdateSubscriptionPermissions` | Pass | `singlenode::test_single_node_update_subscription_permissions` | Pass |
| `TestSinglePublisher` | Pass | `singlenode::test_single_publisher` | Pass |
| `TestSinglePublisherDataTrack` | Pass | `singlenode::test_single_publisher_data_track` | Pass |
| `TestSubscribeToCodecUnsupported` | Pass | `singlenode::test_subscribe_to_codec_unsupported` | Pass |
| `TestTurnAuthFailure` | Pass | `singlenode::test_turn_auth_failure` | Pass |
| `TestTurnRelay` | Pass | `singlenode::test_turn_relay` | Pass |
| `TestWebhooks` | Pass | `webhooks::test_webhooks` | Pass |
| `Test_WhenAutoSubscriptionDisabled_ClientShouldNotReceiveAnyPublishedTracks` | Pass | `singlenode::test_when_auto_subscription_disabled_client_should_not_receive_any_published_tracks` | Pass |
| `Test_RenegotiationWithDifferentCodecs` | Pass | `singlenode::test_renegotiation_with_different_codecs` | Pass |

The matrix inventories both passing and blocked contracts. The latest full
external baseline does **not** pass `TestMultinodeDataPublishing` or
`TestMultinodePublishingUponJoining`. `TestConnectionStats` failed in that
full run but passed in a focused single-worker rerun, so it is currently
classified as flaky. Blocked rows remain useful native regressions, but must
not be represented as compatibility-complete.

## Porting rules

When adding or changing a row:

1. Read the mapped upstream Go test and every helper/scenario that defines its
   observable behavior.
2. Add or update the Rust port before changing production behavior.
3. Preserve every required upstream assertion; additional assertions are
   allowed, weaker or substitute assertions are not.
4. Run the focused native test and the module compile check.
5. Mark the row **Pass** only after the criteria above are met. Otherwise use
   `Blocked` or `Not ported` with a concise reason.

## Running

The module is behind `harness-e2e` because it uses SDK, media, and
process-based end-to-end paths.

List all native ports:

```bash
cargo test -p oxidesfu-test --features harness-e2e upstream_livekit -- --list
```

Compile all native ports:

```bash
cargo test -p oxidesfu-test --features harness-e2e upstream_livekit --no-run
```

Run a focused native port:

```bash
cargo test -p oxidesfu-test --features harness-e2e \
  harness::support::upstream_livekit::singlenode::test_client_could_connect \
  -- --exact --nocapture
```

Run the module serially when debugging cross-test resource issues:

```bash
cargo test -p oxidesfu-test --features harness-e2e upstream_livekit -- --nocapture --test-threads=1
```

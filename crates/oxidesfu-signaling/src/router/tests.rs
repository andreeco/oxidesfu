#![allow(deprecated, clippy::let_unit_value, clippy::manual_ignore_case_cmp)]

use std::{
    collections::{HashMap, HashSet},
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{body::Body, http::Request};
use base64::{Engine, engine::general_purpose};
use flate2::{Compression, write::GzEncoder};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use oxidesfu_auth::{ApiKeyStore, Claims, VideoGrants};
use oxidesfu_room::{RegisteredNode, RoomNodeRegistry};
use prost::Message as _;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use tower::ServiceExt;

use super::*;

const API_KEY: &str = "devkey";
const API_SECRET: &str = "secret";

fn state() -> SignalState {
    let mut keys = ApiKeyStore::new();
    keys.insert(API_KEY, API_SECRET);
    SignalState::new(RoomStore::default(), TokenVerifier::new(keys))
        .with_reconnect_participant_retention_grace(Duration::from_millis(75))
}

fn state_with_ice_servers(ice_servers: Vec<proto::IceServer>) -> SignalState {
    state().with_ice_servers(ice_servers)
}

fn state_with_participant_ice_server_provider() -> SignalState {
    state().with_ice_server_provider(|participant_sid| {
        vec![proto::IceServer {
            urls: vec!["turn:turn.example.net:3478?transport=udp".to_string()],
            username: format!("turn-{participant_sid}"),
            credential: format!("credential-{participant_sid}"),
        }]
    })
}

async fn wait_for_reliable_data_channel_registration(
    state: &SignalState,
    room_name: &str,
    identity: &str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state.data_channels.get(room_name, identity).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reliable data channel should be registered after opening");
}

fn state_with_webhook_collector() -> (SignalState, Arc<Mutex<Vec<proto::WebhookEvent>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let collector = events.clone();
    let state = state().with_webhook_event_handler(Some(Arc::new(move |event| {
        collector
            .lock()
            .expect("webhook collector lock should not be poisoned")
            .push(event);
    })));
    (state, events)
}

#[test]
fn pending_media_section_request_is_retried_after_offer_omits_matching_receive_section() {
    let pending = crate::stores::PendingMediaSectionRequestStore::default();
    let room = "pending-media-section-retry-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    assert!(pending.insert_once(
        room,
        publisher,
        "TR_audio",
        subscriber,
        crate::stores::PendingMediaSectionKind::Audio,
    ));
    assert_eq!(
        pending.take_unrequested_counts(room, subscriber).audios,
        1,
        "the initial audio section requirement should be emitted"
    );
    assert_eq!(
        pending.take_unrequested_counts(room, subscriber).audios,
        0,
        "the request remains in flight until the client answers with a receive section"
    );

    pending.release_requested_for_unresolved(room, subscriber);

    assert_eq!(
        pending.take_unrequested_counts(room, subscriber).audios,
        1,
        "an offer that omitted the requested receive section must allow the requirement to retry"
    );
}

#[test]
fn only_answer_responses_activate_deferred_forward_tracks() {
    let answer = proto::SignalResponse {
        message: Some(proto::signal_response::Message::Answer(
            proto::SessionDescription {
                r#type: "answer".to_string(),
                ..Default::default()
            },
        )),
    };
    let offer = proto::SignalResponse {
        message: Some(proto::signal_response::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                ..Default::default()
            },
        )),
    };
    let requirement = proto::SignalResponse {
        message: Some(proto::signal_response::Message::MediaSectionsRequirement(
            proto::MediaSectionsRequirement {
                num_audios: 1,
                num_videos: 0,
            },
        )),
    };

    assert!(response_activates_forward_tracks(&answer));
    assert!(!response_activates_forward_tracks(&offer));
    assert!(!response_activates_forward_tracks(&requirement));
}

fn state_with_room_nodes_and_placement(
    room_nodes: Arc<dyn RoomNodeDirectory>,
    local_room_node_id: Option<String>,
    reject_non_local_room_placement: bool,
) -> SignalState {
    let mut keys = ApiKeyStore::new();
    keys.insert(API_KEY, API_SECRET);
    SignalState::with_data_channels_room_nodes_and_placement(
        RoomStore::default(),
        TokenVerifier::new(keys),
        DataChannelStore::default(),
        Some(room_nodes),
        local_room_node_id,
        reject_non_local_room_placement,
    )
}

fn state_with_room_nodes_placement_and_relay_dispatcher(
    room_nodes: Arc<dyn RoomNodeDirectory>,
    local_room_node_id: Option<String>,
    reject_non_local_room_placement: bool,
    relay_dispatcher: Arc<dyn NonLocalRelayDispatcher>,
) -> SignalState {
    let mut keys = ApiKeyStore::new();
    keys.insert(API_KEY, API_SECRET);
    SignalState::with_data_channels_room_nodes_placement_and_relay_dispatcher(
        RoomStore::default(),
        TokenVerifier::new(keys),
        DataChannelStore::default(),
        Some(room_nodes),
        local_room_node_id,
        reject_non_local_room_placement,
        relay_dispatcher,
    )
}

#[derive(Default)]
struct RecordingRelayDispatcher {
    intents: std::sync::Mutex<Vec<NonLocalRelayJoinIntent>>,
    terminations: std::sync::Mutex<Vec<NonLocalRelaySessionTerminationIntent>>,
}

struct AcceptingRelayDispatcher;

impl NonLocalRelayDispatcher for AcceptingRelayDispatcher {
    fn dispatch_non_local_join(
        &self,
        _intent: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String> {
        Ok(Some(NonLocalRelayJoinResponse::Accepted {
            participant_sid: "PA_remote".to_string(),
            server_version: "relay-proxy".to_string(),
            ping_interval: 9,
            ping_timeout: 19,
        }))
    }

    fn dispatch_non_local_termination(
        &self,
        _intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[test]
fn compare_client_version_matches_upstream_contract() {
    let client_info = proto::ClientInfo {
        version: "1".to_string(),
        ..Default::default()
    };

    assert_eq!(compare_client_version(&client_info, "0.1.0"), 1);
    assert_eq!(compare_client_version(&client_info, "1.0.0"), 0);
    assert_eq!(compare_client_version(&client_info, "1.0.5"), -1);
}

#[test]
fn client_supports_ice_tcp_matches_upstream_sdk_policy() {
    let go = proto::ClientInfo {
        sdk: proto::client_info::Sdk::Go as i32,
        ..Default::default()
    };
    assert!(!client_supports_ice_tcp(Some(&go)));

    let swift_old = proto::ClientInfo {
        sdk: proto::client_info::Sdk::Swift as i32,
        version: "1.0.4".to_string(),
        ..Default::default()
    };
    assert!(!client_supports_ice_tcp(Some(&swift_old)));

    let swift_supported = proto::ClientInfo {
        sdk: proto::client_info::Sdk::Swift as i32,
        version: "1.0.5".to_string(),
        ..Default::default()
    };
    assert!(client_supports_ice_tcp(Some(&swift_supported)));
}

#[test]
fn disabled_codecs_for_client_info_matches_upstream_static_rules() {
    let safari = proto::ClientInfo {
        browser: "safari".to_string(),
        browser_version: "18.2".to_string(),
        ..Default::default()
    };
    let safari_conf =
        disabled_codecs_for_client_info(&safari).expect("safari should get disabled codec config");
    assert_eq!(safari_conf.codecs.len(), 1);
    assert_eq!(safari_conf.codecs[0].mime, "video/AV1");
    assert!(safari_conf.publish.is_empty());

    let safari_new = proto::ClientInfo {
        browser: "safari".to_string(),
        browser_version: "18.4".to_string(),
        ..Default::default()
    };
    let safari_new_conf = disabled_codecs_for_client_info(&safari_new)
        .expect("new safari should get disabled codec config");
    assert_eq!(safari_new_conf.codecs.len(), 1);
    assert_eq!(safari_new_conf.codecs[0].mime, "video/AV1");
    assert_eq!(safari_new_conf.publish.len(), 1);
    assert_eq!(safari_new_conf.publish[0].mime, "video/VP9");

    let firefox_linux = proto::ClientInfo {
        browser: "firefox".to_string(),
        os: "linux".to_string(),
        ..Default::default()
    };
    let firefox_linux_conf = disabled_codecs_for_client_info(&firefox_linux)
        .expect("firefox linux should get disabled codec config");
    assert!(firefox_linux_conf.codecs.is_empty());
    assert_eq!(firefox_linux_conf.publish.len(), 1);
    assert_eq!(firefox_linux_conf.publish[0].mime, "video/H264");
}

#[test]
fn switch_candidate_reconnect_prefers_tcp_when_available() {
    let state = state().with_rtc_transport_config(oxidesfu_rtc::RtcTransportConfig {
        udp_addrs: vec!["0.0.0.0:50000".to_string()],
        tcp_addrs: vec!["0.0.0.0:7881".to_string()],
        nat_1to1_ips: Vec::new(),
    });

    let request = proto::JoinRequest {
        reconnect: true,
        reconnect_reason: proto::ReconnectReason::RrSwitchCandidate as i32,
        client_info: Some(proto::ClientInfo {
            sdk: proto::client_info::Sdk::Js as i32,
            ..Default::default()
        }),
        ..Default::default()
    };

    let effective = effective_rtc_transport_for_join_request(&state, &request);
    assert!(effective.udp_addrs.is_empty());
    assert_eq!(effective.tcp_addrs, vec!["0.0.0.0:7881"]);
}

#[test]
fn switch_candidate_reconnect_for_go_sdk_does_not_use_tcp() {
    let state = state().with_rtc_transport_config(oxidesfu_rtc::RtcTransportConfig {
        udp_addrs: vec!["0.0.0.0:50000".to_string()],
        tcp_addrs: vec!["0.0.0.0:7881".to_string()],
        nat_1to1_ips: Vec::new(),
    });

    let request = proto::JoinRequest {
        reconnect: true,
        reconnect_reason: proto::ReconnectReason::RrSwitchCandidate as i32,
        client_info: Some(proto::ClientInfo {
            sdk: proto::client_info::Sdk::Go as i32,
            ..Default::default()
        }),
        ..Default::default()
    };

    let effective = effective_rtc_transport_for_join_request(&state, &request);
    assert_eq!(effective.udp_addrs, vec!["0.0.0.0:50000"]);
    assert!(effective.tcp_addrs.is_empty());
}

#[test]
fn non_switch_reconnect_keeps_udp_transport_configuration() {
    let state = state().with_rtc_transport_config(oxidesfu_rtc::RtcTransportConfig {
        udp_addrs: vec!["0.0.0.0:50000".to_string()],
        tcp_addrs: vec!["0.0.0.0:7881".to_string()],
        nat_1to1_ips: Vec::new(),
    });

    let request = proto::JoinRequest {
        reconnect: true,
        reconnect_reason: proto::ReconnectReason::RrPublisherFailed as i32,
        client_info: Some(proto::ClientInfo {
            sdk: proto::client_info::Sdk::Js as i32,
            ..Default::default()
        }),
        ..Default::default()
    };

    let effective = effective_rtc_transport_for_join_request(&state, &request);
    assert_eq!(effective.udp_addrs, vec!["0.0.0.0:50000"]);
    assert_eq!(effective.tcp_addrs, vec!["0.0.0.0:7881"]);
}

#[test]
fn join_webhook_events_include_room_started_for_first_participant() {
    let (state, events) = state_with_webhook_collector();
    let room = proto::Room {
        sid: "RM_test".to_string(),
        name: "room-a".to_string(),
        ..Default::default()
    };
    let participant = proto::ParticipantInfo {
        sid: "PA_test".to_string(),
        identity: "alice".to_string(),
        name: "Alice".to_string(),
        ..Default::default()
    };

    emit_join_webhook_events(&state, &room, &participant, true);

    let stored = events
        .lock()
        .expect("webhook collector lock should not be poisoned")
        .clone();
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].event, "room_started");
    assert_eq!(stored[1].event, "participant_joined");
    assert_eq!(
        stored[1].participant.as_ref().map(|p| p.identity.as_str()),
        Some("alice")
    );
}

#[tokio::test]
async fn cleanup_participant_runtime_state_emits_track_unpublished_then_participant_left() {
    let (state, events) = state_with_webhook_collector();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room-a".to_string(),
            ..Default::default()
        })
        .expect("room should create");
    state
        .rooms
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");
    state
        .rooms
        .add_participant_track(
            "room-a",
            "alice",
            proto::TrackInfo {
                sid: "TR_test".to_string(),
                ..Default::default()
            },
        )
        .expect("track should be added");

    cleanup_participant_runtime_state(&state, "room-a", "alice", true).await;

    let stored = events
        .lock()
        .expect("webhook collector lock should not be poisoned")
        .clone();
    let event_names = stored
        .iter()
        .map(|event| event.event.as_str())
        .collect::<Vec<_>>();
    assert_eq!(event_names, vec!["track_unpublished", "participant_left"]);
}

#[tokio::test]
async fn cleanup_participant_runtime_state_is_idempotent_after_disconnect() {
    let (state, events) = state_with_webhook_collector();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room-a".to_string(),
            ..Default::default()
        })
        .expect("room should create");
    state
        .rooms
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");
    state
        .rooms
        .add_participant_track(
            "room-a",
            "alice",
            proto::TrackInfo {
                sid: "TR_test".to_string(),
                ..Default::default()
            },
        )
        .expect("track should be added");

    cleanup_participant_runtime_state(&state, "room-a", "alice", true).await;
    cleanup_participant_runtime_state(&state, "room-a", "alice", true).await;

    assert!(
        state.rooms.get_participant("room-a", "alice").is_err(),
        "participant should remain disconnected after repeated cleanup"
    );

    let stored = events
        .lock()
        .expect("webhook collector lock should not be poisoned")
        .clone();
    let event_names = stored
        .iter()
        .map(|event| event.event.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        event_names,
        vec!["track_unpublished", "participant_left"],
        "repeated cleanup should not duplicate disconnect webhooks"
    );
}

#[tokio::test]
async fn cleanup_for_old_participant_sid_does_not_remove_same_identity_rejoin() {
    let state = state();
    let room = "sid-fenced-cleanup-room";
    let identity = "c2";
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: room.to_string(),
            ..Default::default()
        })
        .expect("room should create");
    let (_, old, _) = state
        .rooms
        .join_participant(room, identity, "Client 2", String::new(), HashMap::new())
        .expect("old participant should join");
    state
        .rooms
        .remove_participant(room, identity)
        .expect("old participant should depart before rejoin");
    let (_, rejoined, _) = state
        .rooms
        .join_participant(room, identity, "Client 2", String::new(), HashMap::new())
        .expect("new participant should join");
    assert_ne!(
        old.sid, rejoined.sid,
        "rejoin must create a new session SID"
    );
    state
        .rooms
        .add_participant_track(
            room,
            identity,
            proto::TrackInfo {
                sid: "TR_rejoined".to_string(),
                r#type: proto::TrackType::Audio as i32,
                ..Default::default()
            },
        )
        .expect("rejoined track should be added");

    let cleaned = cleanup_participant_runtime_state_for_participant_sid(
        &state, room, identity, &old.sid, true,
    )
    .await;

    assert!(!cleaned, "old-session cleanup must be ignored after rejoin");
    let current = state
        .rooms
        .get_participant(room, identity)
        .expect("rejoined participant must remain");
    assert_eq!(current.sid, rejoined.sid);
    assert!(
        current
            .tracks
            .iter()
            .any(|track| track.sid == "TR_rejoined"),
        "old-session cleanup must not unpublish new-session tracks"
    );
}

#[tokio::test]
async fn cleanup_if_active_session_ungraceful_close_detaches_subscriber_sid_orphans_without_forward_store_rows()
 {
    let state = state();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room-a".to_string(),
            ..Default::default()
        })
        .expect("room should create");
    state
        .rooms
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("publisher should join");
    state
        .rooms
        .join_participant("room-a", "bob", "Bob", String::new(), HashMap::new())
        .expect("subscriber should join");

    state
        .rooms
        .add_participant_track(
            "room-a",
            "alice",
            proto::TrackInfo {
                sid: "TR_audio".to_string(),
                r#type: proto::TrackType::Audio as i32,
                ..Default::default()
            },
        )
        .expect("track should be added");

    let publisher_sid = state
        .rooms
        .get_participant("room-a", "alice")
        .expect("publisher should exist")
        .sid;

    let subscriber_pc = state.peer_connections.insert(
        "room-a",
        "bob",
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("subscriber peer connection should create"),
    );

    let _stale_sender = subscriber_pc
        .add_forwarding_track(
            &publisher_sid,
            "TR_audio",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("stale sender should be added");

    let before = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary before cleanup should succeed");
    let before_sender_count = before
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();
    assert!(
        before_sender_count >= 1,
        "subscriber should have at least one sender before ungraceful close"
    );

    let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel::<proto::SignalResponse>();
    state
        .signal_connections
        .insert("room-a", "alice", outbound_tx.clone());

    cleanup_if_active_session(&state, "room-a", "alice", &outbound_tx, false).await;

    let after = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary after cleanup should succeed");
    let after_sender_count = after
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();

    assert!(
        after_sender_count < before_sender_count,
        "ungraceful close should detach subscriber sender even when forward-track store rows are missing"
    );

    let _ = subscriber_pc.close().await;
}

#[tokio::test]
async fn cleanup_if_active_session_ungraceful_close_unpublishes_tracks_before_reconnect_grace() {
    let (state, events) = state_with_webhook_collector();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room-a".to_string(),
            ..Default::default()
        })
        .expect("room should create");
    state
        .rooms
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("publisher should join");
    state
        .rooms
        .join_participant("room-a", "bob", "Bob", String::new(), HashMap::new())
        .expect("subscriber should join");

    let alice_pc = state.peer_connections.insert(
        "room-a",
        "alice",
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("peer connection should create"),
    );

    for (sid, kind) in [
        ("TR_audio", proto::TrackType::Audio as i32),
        ("TR_video", proto::TrackType::Video as i32),
    ] {
        state
            .rooms
            .add_participant_track(
                "room-a",
                "alice",
                proto::TrackInfo {
                    sid: sid.to_string(),
                    r#type: kind,
                    ..Default::default()
                },
            )
            .expect("track should be added");
    }

    let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel::<proto::SignalResponse>();
    state
        .signal_connections
        .insert("room-a", "alice", outbound_tx.clone());

    cleanup_if_active_session(&state, "room-a", "alice", &outbound_tx, false).await;

    let participant_after_ungraceful_close = state
        .rooms
        .get_participant("room-a", "alice")
        .expect("participant should be retained during reconnect grace");
    assert!(
        participant_after_ungraceful_close.tracks.is_empty(),
        "published tracks should be unpublished immediately on ungraceful close"
    );
    assert!(
        state
            .peer_connections
            .get("room-a", "alice", SignalConnectionTarget::Subscriber)
            .is_some(),
        "peer connection should be retained until reconnect grace elapses"
    );

    let events_after_ungraceful_close = events
        .lock()
        .expect("webhook collector lock should not be poisoned")
        .clone();
    let unpublished_count = events_after_ungraceful_close
        .iter()
        .filter(|event| event.event == "track_unpublished")
        .count();
    assert_eq!(
        unpublished_count, 2,
        "each published track should emit track_unpublished on ungraceful close"
    );
    assert!(
        !events_after_ungraceful_close
            .iter()
            .any(|event| event.event == "participant_left"),
        "participant_left should wait for reconnect grace cleanup"
    );

    tokio::time::sleep(Duration::from_millis(130)).await;

    assert!(
        state.rooms.get_participant("room-a", "alice").is_err(),
        "participant should be removed after reconnect grace elapses"
    );
    assert!(
        state
            .peer_connections
            .get("room-a", "alice", SignalConnectionTarget::Subscriber)
            .is_none(),
        "peer connection should be removed after reconnect grace elapses"
    );

    let events_after_grace = events
        .lock()
        .expect("webhook collector lock should not be poisoned")
        .clone();
    assert!(
        events_after_grace
            .iter()
            .any(|event| event.event == "participant_left"),
        "participant_left should be emitted after reconnect grace cleanup"
    );

    let _ = alice_pc.close().await;
}

#[tokio::test]
async fn cleanup_participant_runtime_state_detaches_departed_publisher_forward_tracks() {
    let state = state();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room-a".to_string(),
            ..Default::default()
        })
        .expect("room should create");

    let (_room, publisher, _participants) = state
        .rooms
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("publisher should join");
    state
        .rooms
        .join_participant("room-a", "bob", "Bob", String::new(), HashMap::new())
        .expect("subscriber should join");

    let track = proto::TrackInfo {
        sid: "TR_audio".to_string(),
        r#type: proto::TrackType::Audio as i32,
        ..Default::default()
    };
    state
        .rooms
        .add_participant_track("room-a", "alice", track.clone())
        .expect("track should be added");

    let subscriber_pc = state.peer_connections.insert(
        "room-a",
        "bob",
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("subscriber peer connection should create"),
    );

    let forward_track = subscriber_pc
        .add_forwarding_track(
            &publisher.sid,
            &track.sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("forwarding track should be added");

    state
        .forward_tracks
        .insert("room-a", "alice", &track.sid, "bob", forward_track);

    let before = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary before cleanup should succeed");
    let before_sender_count = before
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();
    assert!(
        before_sender_count >= 1,
        "subscriber should have at least one sender before publisher cleanup"
    );

    cleanup_participant_runtime_state(&state, "room-a", "alice", true).await;

    let after = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary after cleanup should succeed");
    let after_sender_count = after
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();

    assert!(
        after_sender_count < before_sender_count,
        "publisher cleanup should detach forwarding senders from subscriber transport"
    );
}

#[tokio::test]
async fn runtime_audio_codec_change_detaches_existing_forwarding_sender_and_clears_state() {
    let state = state();
    let room = "runtime-codec-change-room";
    let publisher = "alice";
    let subscriber = "bob";
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: room.to_string(),
            ..Default::default()
        })
        .expect("room should create");
    let (_room, publisher_info, _participants) = state
        .rooms
        .join_participant(room, publisher, "Alice", String::new(), HashMap::new())
        .expect("publisher should join");
    state
        .rooms
        .join_participant(room, subscriber, "Bob", String::new(), HashMap::new())
        .expect("subscriber should join");

    let track = proto::TrackInfo {
        sid: "TR_audio".to_string(),
        r#type: proto::TrackType::Audio as i32,
        mime_type: "audio/opus".to_string(),
        codecs: vec![proto::SimulcastCodecInfo {
            mime_type: "audio/opus".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    state
        .rooms
        .add_participant_track(room, publisher, track.clone())
        .expect("audio track should be added");

    let subscriber_pc = state.peer_connections.insert(
        room,
        subscriber,
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("subscriber peer connection should create"),
    );
    let forward_track = subscriber_pc
        .add_forwarding_track_with_mime(
            &publisher_info.sid,
            &track.sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
            Some("audio/opus"),
        )
        .await
        .expect("Opus forwarding sender should be added");
    state
        .forward_tracks
        .insert(room, publisher, &track.sid, subscriber, forward_track);
    assert!(
        state
            .media_forwarding
            .insert_once(room, publisher, &track.sid, subscriber)
    );

    let before_sender_count = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary before codec change should succeed")
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();

    session::rebuild_forwarding_tracks_after_runtime_codec_change(
        &state, room, publisher, &track.sid,
    )
    .await;

    let after_sender_count = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary after codec change should succeed")
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();
    assert!(
        after_sender_count < before_sender_count,
        "a runtime codec change must detach the stale forwarding sender"
    );
    assert!(
        state
            .forward_tracks
            .list_for_track(room, publisher, &track.sid)
            .is_empty(),
        "the stale forwarding sender must be removed from the forwarding store"
    );
    assert!(
        !state
            .media_forwarding
            .contains(room, publisher, &track.sid, subscriber),
        "the forwarding marker must be cleared so the PCMA sender can be recreated"
    );
}

#[tokio::test]
async fn cleanup_participant_runtime_state_detaches_orphaned_forward_tracks_not_in_room_snapshot() {
    let state = state();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room-a".to_string(),
            ..Default::default()
        })
        .expect("room should create");

    let (_room, publisher, _participants) = state
        .rooms
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("publisher should join");
    state
        .rooms
        .join_participant("room-a", "bob", "Bob", String::new(), HashMap::new())
        .expect("subscriber should join");

    let stale_track = proto::TrackInfo {
        sid: "TR_orphan_audio".to_string(),
        r#type: proto::TrackType::Audio as i32,
        ..Default::default()
    };

    let subscriber_pc = state.peer_connections.insert(
        "room-a",
        "bob",
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("subscriber peer connection should create"),
    );

    let forward_track = subscriber_pc
        .add_forwarding_track(
            &publisher.sid,
            &stale_track.sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("forwarding track should be added");

    state
        .forward_tracks
        .insert("room-a", "alice", &stale_track.sid, "bob", forward_track);

    let before = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary before cleanup should succeed");
    let before_sender_count = before
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();
    assert!(
        before_sender_count >= 1,
        "subscriber should have at least one sender before cleanup"
    );

    cleanup_participant_runtime_state(&state, "room-a", "alice", true).await;

    let after = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary after cleanup should succeed");
    let after_sender_count = after
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();

    assert!(
        after_sender_count < before_sender_count,
        "cleanup should detach orphaned forwarding senders even when the track is absent from room snapshot"
    );
}

#[tokio::test]
async fn cleanup_participant_runtime_state_detaches_sid_orphans_without_forward_store_rows() {
    let state = state();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room-a".to_string(),
            ..Default::default()
        })
        .expect("room should create");

    let (_room, publisher, _participants) = state
        .rooms
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("publisher should join");
    state
        .rooms
        .join_participant("room-a", "bob", "Bob", String::new(), HashMap::new())
        .expect("subscriber should join");

    let subscriber_pc = state.peer_connections.insert(
        "room-a",
        "bob",
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("subscriber peer connection should create"),
    );

    let _stale_sender = subscriber_pc
        .add_forwarding_track(
            &publisher.sid,
            "TR_not_in_forward_store",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("stale sender should be added");

    let before = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary before cleanup should succeed");
    let before_sender_count = before
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();
    assert!(
        before_sender_count >= 1,
        "subscriber should have at least one sender before cleanup"
    );

    cleanup_participant_runtime_state(&state, "room-a", "alice", true).await;

    let after = subscriber_pc
        .debug_transceiver_summary()
        .await
        .expect("transceiver summary after cleanup should succeed");
    let after_sender_count = after
        .iter()
        .filter(|entry| entry.contains("has_sender=true"))
        .count();

    assert!(
        after_sender_count < before_sender_count,
        "cleanup should detach publisher-sid orphaned senders even when forward-track store rows are missing"
    );
}

#[tokio::test]
async fn add_track_response_emits_track_published_webhook() {
    let (state, events) = state_with_webhook_collector();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room-a".to_string(),
            ..Default::default()
        })
        .expect("room should create");
    state
        .rooms
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");
    state
        .publish_permissions
        .set_can_publish_media("room-a", "alice", true);

    let _ = add_track_response(
        &state,
        "room-a",
        "alice",
        proto::AddTrackRequest {
            cid: "cid-1".to_string(),
            name: "cam".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            ..Default::default()
        },
    )
    .await;

    let stored = events
        .lock()
        .expect("webhook collector lock should not be poisoned")
        .clone();
    assert!(stored.iter().any(|event| event.event == "track_published"));
}

struct RelaySignalBackhaulDispatcher;

impl NonLocalRelayDispatcher for RelaySignalBackhaulDispatcher {
    fn dispatch_non_local_join(
        &self,
        _intent: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String> {
        Ok(Some(NonLocalRelayJoinResponse::Accepted {
            participant_sid: "PA_remote_backhaul".to_string(),
            server_version: "relay-proxy".to_string(),
            ping_interval: 9,
            ping_timeout: 19,
        }))
    }

    fn dispatch_non_local_termination(
        &self,
        _intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String> {
        Ok(())
    }

    fn dispatch_non_local_signal_request(
        &self,
        intent: NonLocalRelaySignalRequestIntent,
    ) -> Result<Option<NonLocalRelaySignalRequestResponse>, String> {
        let request = proto::SignalRequest::decode(intent.signal_request.as_slice())
            .map_err(|err| err.to_string())?;
        let Some(proto::signal_request::Message::TrackSetting(request)) = request.message else {
            return Ok(Some(NonLocalRelaySignalRequestResponse::NoResponse));
        };

        let direct = proto::SignalResponse {
            message: Some(proto::signal_response::Message::Pong(1234)),
        };
        let outbound = proto::SignalResponse {
            message: Some(proto::signal_response::Message::SubscribedQualityUpdate(
                subscribed_quality_updates_from_track_settings(&request)
                    .into_iter()
                    .next()
                    .expect("track-setting request should include a track sid"),
            )),
        };

        Ok(Some(NonLocalRelaySignalRequestResponse::Response {
            signal_response: direct.encode_to_vec(),
            outbound_signal_responses: vec![outbound.encode_to_vec()],
        }))
    }
}

#[derive(Default)]
struct PersistentOutboundRelayDispatcher {
    pending: std::sync::atomic::AtomicBool,
}

impl NonLocalRelayDispatcher for PersistentOutboundRelayDispatcher {
    fn dispatch_non_local_join(
        &self,
        _intent: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String> {
        Ok(Some(NonLocalRelayJoinResponse::Accepted {
            participant_sid: "PA_remote_persistent".to_string(),
            server_version: "relay-proxy".to_string(),
            ping_interval: 9,
            ping_timeout: 19,
        }))
    }

    fn dispatch_non_local_termination(
        &self,
        _intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String> {
        Ok(())
    }

    fn dispatch_non_local_signal_request(
        &self,
        intent: NonLocalRelaySignalRequestIntent,
    ) -> Result<Option<NonLocalRelaySignalRequestResponse>, String> {
        let request = proto::SignalRequest::decode(intent.signal_request.as_slice())
            .map_err(|err| err.to_string())?;
        if matches!(
            request.message,
            Some(proto::signal_request::Message::TrackSetting(_))
        ) {
            self.pending
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(Some(NonLocalRelaySignalRequestResponse::NoResponse))
    }

    fn drain_non_local_outbound_signal_responses(
        &self,
        _query: NonLocalRelayOutboundSignalQuery,
    ) -> Result<Vec<Vec<u8>>, String> {
        if self
            .pending
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            let outbound = proto::SignalResponse {
                message: Some(proto::signal_response::Message::SubscribedQualityUpdate(
                    proto::SubscribedQualityUpdate {
                        track_sid: "TR_remote_persistent".to_string(),
                        ..Default::default()
                    },
                )),
            };
            return Ok(vec![outbound.encode_to_vec()]);
        }
        Ok(Vec::new())
    }
}

struct RejectingRelayDispatcher;

impl NonLocalRelayDispatcher for RejectingRelayDispatcher {
    fn dispatch_non_local_join(
        &self,
        _intent: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String> {
        Ok(Some(NonLocalRelayJoinResponse::Rejected {
            code: "permission_denied".to_string(),
            msg: "remote denied".to_string(),
        }))
    }

    fn dispatch_non_local_termination(
        &self,
        _intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Default)]
struct AcceptingAndRecordingTerminationDispatcher {
    terminations: std::sync::Mutex<Vec<NonLocalRelaySessionTerminationIntent>>,
}

impl AcceptingAndRecordingTerminationDispatcher {
    fn take_terminations(&self) -> Vec<NonLocalRelaySessionTerminationIntent> {
        self.terminations
            .lock()
            .expect("relay terminations lock should not be poisoned")
            .clone()
    }
}

impl NonLocalRelayDispatcher for AcceptingAndRecordingTerminationDispatcher {
    fn dispatch_non_local_join(
        &self,
        _intent: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String> {
        Ok(Some(NonLocalRelayJoinResponse::Accepted {
            participant_sid: "PA_remote".to_string(),
            server_version: "relay-proxy".to_string(),
            ping_interval: 9,
            ping_timeout: 19,
        }))
    }

    fn dispatch_non_local_termination(
        &self,
        intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String> {
        self.terminations
            .lock()
            .expect("relay terminations lock should not be poisoned")
            .push(intent);
        Ok(())
    }
}

struct AcceptingMailboxDriver;

#[derive(Default)]
struct RecordingTerminationMailboxDriver {
    seen: Arc<std::sync::Mutex<Vec<NonLocalRelaySessionTerminationIntent>>>,
}

impl RelayIntentExecutionDriver<InMemoryHashStore> for AcceptingMailboxDriver {
    fn drive_for_node(
        &self,
        mailbox: &RedisRelayMailbox<InMemoryHashStore>,
        selected_room_node_id: &str,
    ) -> Result<(), String> {
        if let Some((receipt, _intent)) = mailbox
            .claim_next_intent_for_node(selected_room_node_id)
            .map_err(|err| err.to_string())?
        {
            mailbox
                .store_response(
                    &receipt,
                    &NonLocalRelayJoinResponse::Accepted {
                        participant_sid: "PA_remote_mailbox".to_string(),
                        server_version: "relay-mailbox".to_string(),
                        ping_interval: 6,
                        ping_timeout: 12,
                    },
                )
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}

impl RelayIntentExecutionDriver<InMemoryHashStore> for RecordingTerminationMailboxDriver {
    fn drive_for_node(
        &self,
        _mailbox: &RedisRelayMailbox<InMemoryHashStore>,
        _selected_room_node_id: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    fn drive_termination_for_node(
        &self,
        mailbox: &RedisRelayMailbox<InMemoryHashStore>,
        selected_room_node_id: &str,
    ) -> Result<(), String> {
        while let Some(intent) = mailbox
            .claim_next_termination_intent_for_node(selected_room_node_id)
            .map_err(|err| err.to_string())?
        {
            self.seen
                .lock()
                .expect("termination recording lock should not be poisoned")
                .push(intent);
        }
        Ok(())
    }
}

struct AcceptingFlakyMailboxDriver;

impl RelayIntentExecutionDriver<FlakyHsetHashStore> for AcceptingFlakyMailboxDriver {
    fn drive_for_node(
        &self,
        mailbox: &RedisRelayMailbox<FlakyHsetHashStore>,
        selected_room_node_id: &str,
    ) -> Result<(), String> {
        if let Some((receipt, _intent)) = mailbox
            .claim_next_intent_for_node(selected_room_node_id)
            .map_err(|err| err.to_string())?
        {
            mailbox
                .store_response(
                    &receipt,
                    &NonLocalRelayJoinResponse::Accepted {
                        participant_sid: "PA_remote_mailbox".to_string(),
                        server_version: "relay-mailbox".to_string(),
                        ping_interval: 6,
                        ping_timeout: 12,
                    },
                )
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}

impl RecordingRelayDispatcher {
    fn take(&self) -> Vec<NonLocalRelayJoinIntent> {
        self.intents
            .lock()
            .expect("relay intents lock should not be poisoned")
            .clone()
    }
}

impl NonLocalRelayDispatcher for RecordingRelayDispatcher {
    fn dispatch_non_local_join(
        &self,
        target: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String> {
        self.intents
            .lock()
            .expect("relay intents lock should not be poisoned")
            .push(target);
        Ok(None)
    }

    fn dispatch_non_local_termination(
        &self,
        intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String> {
        self.terminations
            .lock()
            .expect("relay terminations lock should not be poisoned")
            .push(intent);
        Ok(())
    }
}

fn token(room: &str) -> String {
    token_for(room, "alice", "Alice")
}

fn token_for(room: &str, identity: &str, name: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let claims = Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        sub: identity.to_string(),
        name: name.to_string(),
        video: VideoGrants {
            room_join: true,
            room: room.to_string(),
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            ..Default::default()
        },
        ..Default::default()
    };
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(API_SECRET.as_bytes()),
    )
    .expect("token should encode")
}

fn token_for_with_grants(room: &str, identity: &str, name: &str, mut video: VideoGrants) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    if video.room.is_empty() {
        video.room = room.to_string();
    }
    video.room_join = true;
    let claims = Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        sub: identity.to_string(),
        name: name.to_string(),
        video,
        ..Default::default()
    };
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(API_SECRET.as_bytes()),
    )
    .expect("token should encode")
}

fn token_for_with_publish_data(room: &str, identity: &str, name: &str) -> String {
    token_for_with_permissions(room, identity, name, true, true)
}

fn token_for_with_room_config(
    room: &str,
    identity: &str,
    name: &str,
    room_config: serde_json::Value,
) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let claims = Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        sub: identity.to_string(),
        name: name.to_string(),
        video: VideoGrants {
            room_join: true,
            room: room.to_string(),
            ..Default::default()
        },
        room_config: Some(room_config),
        ..Default::default()
    };
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(API_SECRET.as_bytes()),
    )
    .expect("token should encode")
}

fn token_for_without_subscribe(room: &str, identity: &str, name: &str) -> String {
    token_for_with_permissions(room, identity, name, true, false)
}

fn token_for_with_permissions(
    room: &str,
    identity: &str,
    name: &str,
    can_publish_data: bool,
    can_subscribe: bool,
) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let claims = Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        sub: identity.to_string(),
        name: name.to_string(),
        video: VideoGrants {
            room_join: true,
            room: room.to_string(),
            can_publish: true,
            can_publish_data,
            can_subscribe,
            ..Default::default()
        },
        ..Default::default()
    };
    token_for_claims_with_secret(claims, API_SECRET)
}

fn token_for_claims(claims: Claims) -> String {
    token_for_claims_with_secret(claims, API_SECRET)
}

fn token_for_claims_with_secret(claims: Claims, secret: &str) -> String {
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("token should encode")
}

fn wrapped_join_request_param(join: proto::JoinRequest) -> String {
    let wrapped = proto::WrappedJoinRequest {
        compression: proto::wrapped_join_request::Compression::None as i32,
        join_request: join.encode_to_vec(),
    };
    general_purpose::URL_SAFE.encode(wrapped.encode_to_vec())
}

fn join_request_param() -> String {
    wrapped_join_request_param(proto::JoinRequest {
        metadata: "join metadata".to_string(),
        ..Default::default()
    })
}

fn gzip_join_request_param() -> String {
    let join = proto::JoinRequest {
        metadata: "join metadata".to_string(),
        ..Default::default()
    };
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&join.encode_to_vec())
        .expect("gzip write should succeed");
    let wrapped = proto::WrappedJoinRequest {
        compression: proto::wrapped_join_request::Compression::Gzip as i32,
        join_request: encoder.finish().expect("gzip finish should succeed"),
    };
    general_purpose::URL_SAFE.encode(wrapped.encode_to_vec())
}

async fn body_text(response: Response) -> String {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body should collect")
        .to_bytes();
    String::from_utf8(bytes.to_vec()).expect("response should be utf8")
}

fn join_participant_for_data_track_test(state: &SignalState, room: &str, identity: &str) {
    let _ = state
        .rooms
        .join_participant(room, identity, identity, String::new(), HashMap::new());
    state
        .publish_permissions
        .set_can_publish_media(room, identity, true);
    state
        .publish_permissions
        .set_can_publish_data(room, identity, true);
}

#[test]
fn subscribe_permission_defaults_to_allowed_when_not_set() {
    let permissions = SubscribePermissionStore::default();
    assert!(permissions.can_subscribe("unknown-room", "unknown-identity"));
}

#[test]
fn subscribe_permission_remove_participant_clears_override() {
    let permissions = SubscribePermissionStore::default();
    let room = "permission-room";
    let identity = "participant";

    permissions.set_can_subscribe(room, identity, false);
    assert!(!permissions.can_subscribe(room, identity));

    permissions.remove_participant(room, identity);
    assert!(permissions.can_subscribe(room, identity));
}

#[test]
fn unpublish_data_track_removes_subscriber_mappings_and_allows_clean_republish() {
    let state = state();
    let room = "data-track-unpublish-cleanup-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let first_publish = publish_data_track_response(
        &state,
        room,
        publisher,
        proto::PublishDataTrackRequest {
            pub_handle: 44,
            name: "telemetry".to_string(),
            ..Default::default()
        },
    );
    let Some(proto::signal_response::Message::PublishDataTrackResponse(first_published)) =
        first_publish.message
    else {
        panic!("expected publish data track response");
    };
    let first_info = first_published.info.expect("first info should be present");

    let first_subscribe = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: first_info.sid.clone(),
                subscribe: true,
                options: None,
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(first_handles)) =
        first_subscribe.message
    else {
        panic!("expected subscriber handles");
    };
    assert_eq!(first_handles.sub_handles.len(), 1);
    assert_eq!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 44)
            .len(),
        1
    );

    let unpublish = unpublish_data_track_response(
        &state,
        room,
        publisher,
        proto::UnpublishDataTrackRequest { pub_handle: 44 },
    );
    let Some(proto::signal_response::Message::UnpublishDataTrackResponse(unpublished)) =
        unpublish.message
    else {
        panic!("expected unpublish data track response");
    };
    assert_eq!(
        unpublished.info.expect("unpublished info should exist").sid,
        first_info.sid
    );
    assert!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 44)
            .is_empty(),
        "unpublish should remove stale subscriber mappings"
    );

    let second_publish = publish_data_track_response(
        &state,
        room,
        publisher,
        proto::PublishDataTrackRequest {
            pub_handle: 44,
            name: "telemetry".to_string(),
            ..Default::default()
        },
    );
    let Some(proto::signal_response::Message::PublishDataTrackResponse(second_published)) =
        second_publish.message
    else {
        panic!("expected second publish data track response");
    };
    let second_info = second_published
        .info
        .expect("second info should be present");
    assert_eq!(second_info.name, "telemetry");
    assert_eq!(second_info.pub_handle, 44);

    let second_subscribe = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: second_info.sid,
                subscribe: true,
                options: None,
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(second_handles)) =
        second_subscribe.message
    else {
        panic!("expected second subscriber handles");
    };
    assert_eq!(second_handles.sub_handles.len(), 1);
    assert_eq!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 44)
            .len(),
        1
    );
}

#[test]
fn update_data_subscription_unsubscribe_removes_subscriber_handle_mapping() {
    let state = state();
    let room = "data-track-subscriptions-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let published = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 7,
                name: "telemetry".to_string(),
                ..Default::default()
            },
        )
        .expect("publish should succeed");

    let subscribe_response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid.clone(),
                subscribe: true,
                options: None,
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(subscribed_handles)) =
        subscribe_response.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert_eq!(subscribed_handles.sub_handles.len(), 1);
    assert_eq!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, published.pub_handle)
            .len(),
        1
    );

    let unsubscribe_response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid,
                subscribe: false,
                options: None,
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(unsubscribed_handles)) =
        unsubscribe_response.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert!(unsubscribed_handles.sub_handles.is_empty());
    assert!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 7)
            .is_empty()
    );
}

#[test]
fn update_data_subscription_without_can_subscribe_ignores_subscribe_but_applies_unsubscribe() {
    let state = state();
    let room = "data-track-subscription-no-subscribe-mixed-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, true);

    let published = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 32,
                name: "telemetry".to_string(),
                ..Default::default()
            },
        )
        .expect("publish should succeed");

    let _ = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid.clone(),
                subscribe: true,
                options: None,
            }],
        },
    );

    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, false);

    let response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![
                proto::update_data_subscription::Update {
                    track_sid: "DTR_missing".to_string(),
                    subscribe: true,
                    options: None,
                },
                proto::update_data_subscription::Update {
                    track_sid: published.sid,
                    subscribe: false,
                    options: None,
                },
            ],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
        response.message
    else {
        panic!("expected data-track subscriber handles response");
    };

    assert!(handles.sub_handles.is_empty());
    assert!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 32)
            .is_empty()
    );
}

#[test]
fn update_data_subscription_without_can_subscribe_still_allows_unsubscribe() {
    let state = state();
    let room = "data-track-subscription-no-subscribe-unsubscribe-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, true);

    let published = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 31,
                name: "telemetry".to_string(),
                ..Default::default()
            },
        )
        .expect("publish should succeed");

    let subscribed_response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid.clone(),
                subscribe: true,
                options: None,
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(subscribed_handles)) =
        subscribed_response.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert_eq!(subscribed_handles.sub_handles.len(), 1);

    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, false);

    let unsubscribed_response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid,
                subscribe: false,
                options: None,
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(unsubscribed_handles)) =
        unsubscribed_response.message
    else {
        panic!("expected data-track subscriber handles response");
    };

    assert!(unsubscribed_handles.sub_handles.is_empty());
    assert!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 31)
            .is_empty()
    );
}

#[tokio::test]
async fn add_track_response_returns_track_published_and_updates_participant_tracks() {
    let state = state();
    let room = "media-add-track-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-1".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;

    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    assert_eq!(track_published.cid, "audio-cid-1");
    let track_info = track_published.track.expect("track info should be present");
    assert_eq!(track_info.r#type, proto::TrackType::Audio as i32);
    assert_eq!(track_info.name, "mic");
    assert_eq!(track_info.mime_type, "audio/opus");
    assert!(!track_info.sid.is_empty());

    let participant = state
        .rooms
        .get_participant(room, publisher)
        .expect("participant should exist");
    assert_eq!(participant.tracks.len(), 1);
    assert_eq!(participant.tracks[0].name, "mic");
    assert_eq!(participant.tracks[0].r#type, proto::TrackType::Audio as i32);
}

#[tokio::test]
async fn add_track_response_rejects_when_participant_lacks_publish_permission() {
    let state = state();
    let room = "media-add-track-denied-room";
    let publisher = "publisher";
    let _ = state
        .rooms
        .join_participant(room, publisher, publisher, String::new(), HashMap::new());
    state
        .publish_permissions
        .set_can_publish_media(room, publisher, false);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-1".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;

    let Some(proto::signal_response::Message::RequestResponse(response)) = response.message else {
        panic!("expected RequestResponse");
    };
    assert_eq!(
        response.reason,
        proto::request_response::Reason::NotAllowed as i32
    );
    assert!(matches!(
        response.request,
        Some(proto::request_response::Request::AddTrack(_))
    ));
    let participant = state
        .rooms
        .get_participant(room, publisher)
        .expect("participant should exist");
    assert!(participant.tracks.is_empty());
}

#[tokio::test]
async fn add_track_response_rejects_when_source_not_allowed_by_can_publish_sources() {
    let state = state();
    let room = "media-add-track-source-denied-room";
    let publisher = "publisher";
    let _ = state
        .rooms
        .join_participant(room, publisher, publisher, String::new(), HashMap::new());
    state
        .publish_permissions
        .set_can_publish_media(room, publisher, true);
    state
        .publish_permissions
        .set_can_publish_sources(room, publisher, &["camera".to_string()]);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-source-denied".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;

    let Some(proto::signal_response::Message::RequestResponse(response)) = response.message else {
        panic!("expected RequestResponse");
    };
    assert_eq!(
        response.reason,
        proto::request_response::Reason::NotAllowed as i32
    );
}

#[tokio::test]
async fn add_track_response_allows_source_when_in_can_publish_sources() {
    let state = state();
    let room = "media-add-track-source-allowed-room";
    let publisher = "publisher";
    let _ = state
        .rooms
        .join_participant(room, publisher, publisher, String::new(), HashMap::new());
    state
        .publish_permissions
        .set_can_publish_media(room, publisher, true);
    state
        .publish_permissions
        .set_can_publish_sources(room, publisher, &["camera".to_string()]);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "video-cid-source-allowed".to_string(),
            name: "cam".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            ..Default::default()
        },
    )
    .await;

    assert!(matches!(
        response.message,
        Some(proto::signal_response::Message::TrackPublished(_))
    ));
}

#[test]
fn mid_to_track_id_from_offer_sdp_extracts_send_media_track_ids() {
    let offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=msid:stream-a audio-cid-1\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=msid:stream-b should-be-ignored\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
            a=mid:2\r\n\
            a=sendonly\r\n\
            a=msid:stream-v TR_video|TR_video\r\n";

    let mapping = mid_to_track_id_from_offer_sdp(offer_sdp);
    assert_eq!(mapping.get("0"), Some(&"audio-cid-1".to_string()));
    assert_eq!(mapping.get("2"), Some(&"TR_video".to_string()));
    assert!(!mapping.contains_key("1"));
}

#[test]
fn receive_section_counts_from_offer_ignores_client_publish_sections_with_msid() {
    let offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=msid:{browser-stream} {browser-audio-track}\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 120\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:2\r\n\
            a=sendrecv\r\n";

    let counts = receive_section_counts_from_offer(offer_sdp);

    assert_eq!(counts.audio, 1);
    assert_eq!(counts.video, 1);
}

#[test]
fn receive_section_counts_from_offer_excludes_sendonly_publish_sections() {
    let offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:3\r\n\
            a=sendonly\r\n\
            a=msid:{browser-stream} {browser-audio-track}\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 120\r\n\
            a=mid:4\r\n\
            a=inactive\r\n";

    let counts = receive_section_counts_from_offer(offer_sdp);

    assert_eq!(counts.audio, 0);
    assert_eq!(counts.video, 0);
}

#[test]
fn receive_section_counts_from_offer_ignores_rejected_recvonly_sections() {
    let offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            m=audio 0 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:2\r\n\
            a=bundle-only\r\n\
            a=recvonly\r\n";

    let counts = receive_section_counts_from_offer(offer_sdp);

    assert_eq!(
        counts.audio, 1,
        "rejected m=audio 0 section should not be counted as a receive section"
    );
    assert_eq!(counts.video, 0);
}

#[test]
fn active_publisher_mids_from_offer_includes_sendonly_without_msid() {
    let offer_sdp = "v=0\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:1\r\n\
            a=sendonly\r\n";

    let active_mids = active_publisher_mids_from_offer(offer_sdp);

    assert!(
        active_mids.contains("1"),
        "single-PC publish offers can have sendonly media sections without msid; they must not be reused as downtrack sections"
    );
}

#[test]
fn single_pc_mid_classification_ignores_rejected_recvonly_media_sections() {
    let state = state();
    let room = "single-pc-rejected-recvonly-classification-room";
    let identity = "listener";

    join_participant_for_data_track_test(&state, room, identity);

    let offer_sdp = "v=0\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            m=audio 0 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:2\r\n\
            a=bundle-only\r\n\
            a=recvonly\r\n";

    let (_publisher_mids, receive_sections) =
        classify_single_pc_offer_sections(&state, room, identity, offer_sdp, &HashMap::new());

    assert!(receive_sections.iter().any(|section| section.mid == "1"));
    assert!(
        receive_sections.iter().all(|section| section.mid != "2"),
        "rejected m=audio 0 section should not be considered a usable receive section"
    );
}

#[test]
fn force_sendonly_sections_without_msid_recvonly_rewrites_orphan_sendonly_sections_inactive() {
    let answer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:1\r\n\
            a=sendonly\r\n\
            a=msid:PA_0000000000000001|TR_a TR_a\r\n\
            a=ssrc:1111 msid:PA_0000000000000001|TR_a TR_a\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:3\r\n\
            a=sendonly\r\n";

    let rewritten =
        session::force_sendonly_sections_without_msid_recvonly(answer_sdp, &HashSet::new());

    assert!(
        rewritten.contains("a=mid:1\r\na=sendonly\r\na=msid:PA_0000000000000001|TR_a TR_a\r\n"),
        "attached downtrack sections must remain sendonly"
    );
    assert!(
        rewritten.contains("a=mid:3\r\na=inactive\r\n"),
        "sendonly sections without msid cause Firefox to fabricate stream IDs and should be rewritten to inactive"
    );
    assert!(!rewritten.contains("a=mid:3\r\na=sendonly\r\n"));
}

#[test]
fn force_sendonly_sections_without_msid_recvonly_keeps_attached_forwarding_mid_sendonly() {
    let answer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:forwarding-audio\r\n\
            a=sendonly\r\n\
            a=rtpmap:109 opus/48000/2\r\n";
    let attached_mids = HashSet::from(["forwarding-audio"]);

    let rewritten =
        session::force_sendonly_sections_without_msid_recvonly(answer_sdp, &attached_mids);

    assert!(
        rewritten.contains("a=mid:forwarding-audio\r\na=sendonly\r\n"),
        "an attached forwarding section without SDP msid must remain active: {rewritten}"
    );
    assert!(
        !rewritten.contains("a=mid:forwarding-audio\r\na=inactive\r\n"),
        "attached forwarding section must not be rewritten inactive: {rewritten}"
    );
}

#[test]
fn single_pc_mid_classification_uses_sdp_publish_mids_when_mid_map_is_partial() {
    let state = state();
    let room = "single-pc-partial-mid-map-classification-room";
    let identity = "publisher-singlepc";
    let remote_identity = "remote-publisher";
    let remote_track_sid = "TR_remote_audio";

    join_participant_for_data_track_test(&state, room, identity);
    join_participant_for_data_track_test(&state, room, remote_identity);

    state
        .rooms
        .add_participant_track(
            room,
            remote_identity,
            proto::TrackInfo {
                sid: remote_track_sid.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("remote track should be added");

    let offer_sdp = "v=0\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:1\r\n\
            a=msid:PA_remote|TR_remote_audio TR_remote_audio\r\n\
            a=sendrecv\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:2\r\n\
            a=sendonly\r\n";

    let offer_mid_to_track_id = HashMap::from([("1".to_string(), remote_track_sid.to_string())]);
    let (publisher_mids, receive_sections) = classify_single_pc_offer_sections(
        &state,
        room,
        identity,
        offer_sdp,
        &offer_mid_to_track_id,
    );

    assert!(
        !publisher_mids.contains("1"),
        "mid referencing an existing remote track must remain a receive/downtrack section"
    );
    assert!(
        receive_sections
            .iter()
            .any(|section| section.mid == "1" && section.kind == ReceiveSectionKind::Audio)
    );
    assert!(
        publisher_mids.contains("2"),
        "partial mid_to_track_id maps must not hide SDP sendonly publish sections"
    );
    assert!(receive_sections.iter().all(|section| section.mid != "2"));
}

#[test]
fn single_pc_mid_classification_treats_sendonly_without_msid_as_publisher() {
    let state = state();
    let room = "single-pc-sendonly-no-msid-classification-room";
    let identity = "publisher-singlepc";
    let remote_identity = "remote-publisher";

    join_participant_for_data_track_test(&state, room, identity);
    join_participant_for_data_track_test(&state, room, remote_identity);

    state
        .rooms
        .add_participant_track(
            room,
            remote_identity,
            proto::TrackInfo {
                sid: "TR_remote_audio".to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("remote track should be added");

    let offer_sdp = "v=0\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            a=mid:1\r\n\
            a=sendonly\r\n";

    let (publisher_mids, receive_sections) =
        classify_single_pc_offer_sections(&state, room, identity, offer_sdp, &HashMap::new());

    assert!(publisher_mids.contains("1"));
    assert!(
        receive_sections.iter().all(|section| section.mid != "1"),
        "a sendonly publish section must not be selected for remote downtrack forwarding"
    );
}

#[test]
fn single_pc_mid_classification_uses_state_not_tr_prefix() {
    let state = state();
    let room = "single-pc-mid-classification-room";
    let identity = "publisher-singlepc";
    let remote_identity = "remote-publisher";

    join_participant_for_data_track_test(&state, room, identity);
    join_participant_for_data_track_test(&state, room, remote_identity);

    state
        .rooms
        .add_participant_track(
            room,
            remote_identity,
            proto::TrackInfo {
                sid: "TR_remote_real".to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("remote track should be added");

    state
        .media_track_cids
        .insert(room, identity, "TR_local_cid", "TR_local_sid");

    let offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=msid:stream-local TR_local_cid\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            a=mid:1\r\n\
            a=sendrecv\r\n\
            a=msid:stream-remote TR_remote_real\r\n";

    let offer_mid_to_track_id = HashMap::from([
        ("0".to_string(), "TR_local_cid".to_string()),
        ("1".to_string(), "TR_remote_real".to_string()),
    ]);

    let (publisher_mids, receive_sections) = classify_single_pc_offer_sections(
        &state,
        room,
        identity,
        offer_sdp,
        &offer_mid_to_track_id,
    );

    assert!(publisher_mids.contains("0"));
    assert!(
        !publisher_mids.contains("1"),
        "remote track sid mid must not be treated as local publisher mid"
    );
    assert!(
        receive_sections
            .iter()
            .any(|section| { section.mid == "1" && section.kind == ReceiveSectionKind::Audio }),
        "remote track sid mid should remain a receive/downtrack section"
    );
}

fn sdp_media_section_lines_for_mid<'a>(sdp: &'a str, target_mid: &str) -> Vec<&'a str> {
    let mut sections = Vec::<Vec<&str>>::new();
    let mut current = Vec::new();
    for line in sdp.lines() {
        if line.starts_with("m=") && !current.is_empty() {
            sections.push(current);
            current = Vec::new();
        }
        current.push(line);
    }
    if !current.is_empty() {
        sections.push(current);
    }

    sections
        .into_iter()
        .find(|section| {
            section
                .iter()
                .any(|line| *line == format!("a=mid:{target_mid}"))
        })
        .unwrap_or_default()
}

fn sdp_media_kind_for_mid(sdp: &str, target_mid: &str) -> Option<String> {
    sdp_media_section_lines_for_mid(sdp, target_mid)
        .first()
        .and_then(|line| line.strip_prefix("m="))
        .and_then(|media_line| media_line.split_whitespace().next())
        .map(str::to_string)
}

fn sdp_direction_for_mid(sdp: &str, target_mid: &str) -> Option<String> {
    let mut in_target_section = false;
    for line in sdp.lines() {
        if line.starts_with("m=") {
            in_target_section = false;
            continue;
        }
        if let Some(mid) = line.strip_prefix("a=mid:") {
            in_target_section = mid.trim() == target_mid;
            continue;
        }
        if in_target_section
            && matches!(
                line,
                "a=sendrecv" | "a=sendonly" | "a=recvonly" | "a=inactive"
            )
        {
            return Some(line.trim_start_matches("a=").to_string());
        }
    }
    None
}

#[tokio::test]
async fn single_pc_rebuilds_publisher_pc_when_offer_reuses_mid_for_different_kind() {
    let state = state();
    let room = "single-pc-kind-change-room";
    let identity = "alice";
    join_participant_for_data_track_test(&state, room, identity);
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    let initial_video_offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            a=ssrc:1234 cname:webcam\r\n\
            a=ssrc:1234 msid:webcam video\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n";

    answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: initial_video_offer_sdp.to_string(),
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("initial video offer should be answered");

    let audio_then_video_offer_sdp = "v=0\r\n\
            o=- 1 3 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1 2\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=rtpmap:111 opus/48000/2\r\n\
            a=ssrc:2222 cname:webcam\r\n\
            a=ssrc:2222 msid:webcam audio\r\n\
            a=ice-ufrag:test2\r\n\
            a=ice-pwd:testpwd2\r\n\
            a=rtcp-mux\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=sendrecv\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            a=ssrc:3333 cname:webcam\r\n\
            a=ssrc:3333 msid:webcam video\r\n\
            a=ice-ufrag:test2\r\n\
            a=ice-pwd:testpwd2\r\n\
            a=rtcp-mux\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:2\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test2\r\n\
            a=ice-pwd:testpwd2\r\n\
            a=sctp-port:5000\r\n";

    let response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: audio_then_video_offer_sdp.to_string(),
            id: 2,
            ..Default::default()
        },
        &state,
        room,
        identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("kind-changing offer should be answered");

    let Some(proto::signal_response::Message::Answer(answer)) = response.message else {
        panic!("expected answer response");
    };
    assert_eq!(
        sdp_media_kind_for_mid(&answer.sdp, "0").as_deref(),
        Some("audio"),
        "answer must not reuse a stale video transceiver for an audio offer mid"
    );
    assert_eq!(
        sdp_media_kind_for_mid(&answer.sdp, "1").as_deref(),
        Some("video")
    );
}

#[tokio::test]
async fn single_pc_partial_mid_map_offer_answer_does_not_send_on_publish_sendonly_mid() {
    let state = state();
    let room = "single-pc-partial-mid-map-answer-room";
    let identity = "alice";
    let remote_identity = "bot:room";
    let remote_track_sid = "TR_remote_audio";

    join_participant_for_data_track_test(&state, room, identity);
    join_participant_for_data_track_test(&state, room, remote_identity);
    state
        .rooms
        .add_participant_track(
            room,
            remote_identity,
            proto::TrackInfo {
                sid: remote_track_sid.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("remote track should be added");

    let offer_sdp = "v=0\r\n\
            o=- 2453878777137639001 448440133 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1 2\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:jKCNUYHYeaQRPHFs\r\n\
            a=ice-pwd:ofxzYtJWQwzuxlzgubcQWuVenKHUSsIH\r\n\
            a=sctp-port:5000\r\n\
            a=max-message-size:65536\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 9 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:9 G722/8000/1\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=fmtp:109 maxplaybackrate=48000;stereo=1;useinbandfec=1;maxaveragebitrate=48000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=msid:PA_remote|TR_remote_audio TR_remote_audio\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:jKCNUYHYeaQRPHFs\r\n\
            a=ice-pwd:ofxzYtJWQwzuxlzgubcQWuVenKHUSsIH\r\n\
            a=ssrc:179824014 cname:PA_remote|TR_remote_audio\r\n\
            a=ssrc:179824014 msid:PA_remote|TR_remote_audio TR_remote_audio\r\n\
            a=rtcp-mux\r\n\
            a=rtcp-rsize\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 8 0 9 109\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:9 G722/8000/1\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=fmtp:109 maxplaybackrate=48000;stereo=1;useinbandfec=1\r\n\
            a=setup:actpass\r\n\
            a=mid:2\r\n\
            a=sendonly\r\n\
            a=ice-ufrag:jKCNUYHYeaQRPHFs\r\n\
            a=ice-pwd:ofxzYtJWQwzuxlzgubcQWuVenKHUSsIH\r\n\
            a=rtcp-mux\r\n\
            a=rtcp-rsize\r\n";
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let initial_offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("initial data-channel offer should create");
    let _initial_answer = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: initial_offer_sdp,
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("initial offer should be answered");

    let response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: offer_sdp.to_string(),
            id: 3,
            mid_to_track_id: HashMap::from([("1".to_string(), remote_track_sid.to_string())]),
        },
        &state,
        room,
        identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("offer should be answered");

    let Some(proto::signal_response::Message::Answer(answer)) = response.message else {
        panic!("expected answer response");
    };
    let mid2_direction =
        sdp_direction_for_mid(&answer.sdp, "2").expect("answer should include direction for mid 2");
    assert_ne!(
        mid2_direction, "sendonly",
        "answer must not send on the client's sendonly publish mid"
    );

    let mid1_lines = sdp_media_section_lines_for_mid(&answer.sdp, "1");
    let direction_index = mid1_lines
        .iter()
        .position(|line| {
            matches!(
                *line,
                "a=sendrecv" | "a=sendonly" | "a=recvonly" | "a=inactive"
            )
        })
        .expect("mid 1 answer section should include direction");
    if let Some(first_media_identity_index) = mid1_lines.iter().position(|line| {
        line.starts_with("a=msid:")
            || line.starts_with("a=ssrc:")
            || line.starts_with("a=ssrc-group:")
    }) {
        assert!(
            direction_index < first_media_identity_index,
            "Firefox should see answer direction before msid/ssrc attributes"
        );
    }

    offerer.close().await.expect("offerer should close");
}

#[tokio::test]
async fn single_pc_audio_mid_stays_active_when_adding_video_receive_section() {
    let state = state();
    let room = "single-pc-audio-stays-active-when-video-added-room";
    let publisher_identity = "u:1";
    let subscriber_identity = "listener";
    let audio_track_sid = "TR_remote_audio";
    let video_track_sid = "TR_remote_video";

    join_participant_for_data_track_test(&state, room, publisher_identity);
    join_participant_for_data_track_test(&state, room, subscriber_identity);

    state
        .rooms
        .add_participant_track(
            room,
            publisher_identity,
            proto::TrackInfo {
                sid: audio_track_sid.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                name: "lang:Original".to_string(),
                ..Default::default()
            },
        )
        .expect("audio track should be added");

    state.pending_media_section_requests.insert_once(
        room,
        publisher_identity,
        audio_track_sid,
        subscriber_identity,
        crate::stores::PendingMediaSectionKind::Audio,
    );

    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    let initial_offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            a=max-message-size:65536\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let initial_response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: initial_offer_sdp.to_string(),
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        subscriber_identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("initial offer should be answered");

    let Some(proto::signal_response::Message::Answer(initial_answer)) = initial_response.message
    else {
        panic!("expected initial answer response");
    };
    assert_eq!(
        sdp_direction_for_mid(&initial_answer.sdp, "1").as_deref(),
        Some("sendonly"),
        "initial audio receive section should be answered as sendonly"
    );

    state
        .rooms
        .add_participant_track(
            room,
            publisher_identity,
            proto::TrackInfo {
                sid: video_track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                name: "video:camera".to_string(),
                ..Default::default()
            },
        )
        .expect("video track should be added");

    state.pending_media_section_requests.insert_once(
        room,
        publisher_identity,
        video_track_sid,
        subscriber_identity,
        crate::stores::PendingMediaSectionKind::Video,
    );

    let second_offer_sdp = "v=0\r\n\
            o=- 1 3 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1 2\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            a=max-message-size:65536\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96 97\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            a=rtpmap:97 rtx/90000\r\n\
            a=fmtp:97 apt=96\r\n\
            a=setup:actpass\r\n\
            a=mid:2\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let second_response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: second_offer_sdp.to_string(),
            id: 2,
            ..Default::default()
        },
        &state,
        room,
        subscriber_identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("second offer should be answered");

    let Some(proto::signal_response::Message::Answer(second_answer)) = second_response.message
    else {
        panic!("expected second answer response");
    };

    assert_eq!(
        sdp_direction_for_mid(&second_answer.sdp, "1").as_deref(),
        Some("sendonly"),
        "audio mid should remain sendonly (active) after adding video receive section"
    );
    assert_eq!(
        sdp_direction_for_mid(&second_answer.sdp, "2").as_deref(),
        Some("sendonly"),
        "video mid should be sendonly when forwarding is attached"
    );

    let mid1_lines = sdp_media_section_lines_for_mid(&second_answer.sdp, "1");
    assert!(
        mid1_lines.iter().any(|line| line.starts_with("a=msid:")),
        "audio mid must carry msid after renegotiation so browsers keep the remote stream active"
    );
}

#[tokio::test]
async fn single_pc_unattached_audio_and_video_receive_sections_are_inactive() {
    let state = state();
    let room = "single-pc-unattached-receive-sections-room";
    let identity = "browser-publisher";
    join_participant_for_data_track_test(&state, room, identity);

    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1 2\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            a=max-message-size:65536\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            a=setup:actpass\r\n\
            a=mid:2\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: offer_sdp.to_string(),
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("initial browser offer should be answered");

    let Some(proto::signal_response::Message::Answer(answer)) = response.message else {
        panic!("expected answer response");
    };
    assert_eq!(
        sdp_direction_for_mid(&answer.sdp, "1").as_deref(),
        Some("inactive"),
        "an unattached audio receive section must not answer recvonly"
    );
    assert_eq!(
        sdp_direction_for_mid(&answer.sdp, "2").as_deref(),
        Some("inactive"),
        "an unattached video receive section must not answer recvonly"
    );
}

#[tokio::test]
async fn single_pc_vp8_only_receive_section_rejects_h264_with_inactive_answer_mid() {
    let state = state();
    let room = "single-pc-vp8-only-h264-rejection-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let h264_track_sid = "TR_h264";
    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: h264_track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/h264".to_string(),
                ..Default::default()
            },
        )
        .expect("H264 track should be added");
    state.pending_media_section_requests.insert_once(
        room,
        publisher,
        h264_track_sid,
        subscriber,
        crate::stores::PendingMediaSectionKind::Video,
    );

    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, subscriber, outbound_tx.clone());
    let offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=group:BUNDLE 0 1\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: offer_sdp.to_string(),
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        subscriber,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("single-PC offer should be answered");

    let Some(proto::signal_response::Message::Answer(answer)) = response.message else {
        panic!("expected single-PC answer");
    };
    assert_eq!(
        sdp_direction_for_mid(&answer.sdp, "1").as_deref(),
        Some("inactive"),
        "an H264 track rejected by a VP8-only receive section must not remain sendonly"
    );
    let codec_error = tokio::time::timeout(Duration::from_millis(250), outbound_rx.recv())
        .await
        .expect("unsupported H264 binding should emit a response")
        .expect("outbound signaling channel should remain open");
    let Some(proto::signal_response::Message::SubscriptionResponse(codec_error)) =
        codec_error.message
    else {
        panic!("expected unsupported-codec subscription response");
    };
    assert_eq!(codec_error.track_sid, h264_track_sid);
    assert_eq!(
        codec_error.err,
        proto::SubscriptionError::SeCodecUnsupported as i32
    );
    assert_eq!(
        state.media_subscriptions.explicit_subscription(
            room,
            publisher,
            h264_track_sid,
            subscriber,
        ),
        Some(false)
    );
    assert!(
        state
            .forward_tracks
            .list_for_track(room, publisher, h264_track_sid)
            .is_empty(),
        "the rejected H264 forwarding sender must be removed"
    );
}

#[tokio::test]
async fn single_pc_attaches_audio_forward_track_for_each_publisher_direction() {
    let state = state();
    let room = "single-pc-attach-audio-bidirectional";
    let c1 = "c1";
    let c2 = "c2";
    let c1_audio_sid = "TR_c1_audio";
    let c2_audio_sid = "TR_c2_audio";
    let c1_audio_cid = "c1-audio-cid";
    let c2_audio_cid = "c2-audio-cid";

    join_participant_for_data_track_test(&state, room, c1);
    join_participant_for_data_track_test(&state, room, c2);

    state
        .rooms
        .add_participant_track(
            room,
            c1,
            proto::TrackInfo {
                sid: c1_audio_sid.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("c1 audio track should be added");
    state
        .rooms
        .add_participant_track(
            room,
            c2,
            proto::TrackInfo {
                sid: c2_audio_sid.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("c2 audio track should be added");
    state
        .media_track_cids
        .insert(room, c1, c1_audio_cid, c1_audio_sid);
    state
        .media_track_cids
        .insert(room, c2, c2_audio_cid, c2_audio_sid);

    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";
    let local_publish_section = |cid: &str| {
        format!(
            "m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
             c=IN IP4 0.0.0.0\r\n\
             a=rtpmap:109 opus/48000/2\r\n\
             a=rtpmap:0 PCMU/8000\r\n\
             a=rtpmap:8 PCMA/8000\r\n\
             a=setup:actpass\r\n\
             a=mid:2\r\n\
             a=sendonly\r\n\
             a=msid:local-stream {cid}\r\n\
             a=ssrc:123456 cname:local-stream\r\n\
             a=ssrc:123456 msid:local-stream {cid}\r\n\
             a=ice-ufrag:test\r\n\
             a=ice-pwd:testpwd\r\n\
             a=rtcp-mux\r\n"
        )
    };
    let c1_offer_sdp = format!("{offer_sdp}{}", local_publish_section(c1_audio_cid));
    let c2_offer_sdp = format!("{offer_sdp}{}", local_publish_section(c2_audio_cid));

    state.pending_media_section_requests.insert_once(
        room,
        c2,
        c2_audio_sid,
        c1,
        crate::stores::PendingMediaSectionKind::Audio,
    );
    assert!(
        state
            .pending_media_section_requests
            .contains(room, c2, c2_audio_sid, c1),
        "c1 should start with a pending request for c2 audio"
    );

    let c1_response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: c1_offer_sdp,
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        c1,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("c1 offer should be answered");
    let Some(proto::signal_response::Message::Answer(c1_answer)) = c1_response.message else {
        panic!("expected c1 answer response");
    };
    assert_eq!(
        c1_answer.mid_to_track_id.get("1").map(String::as_str),
        Some(c2_audio_sid),
        "c1 audio receive section should map to c2 audio"
    );
    assert_eq!(
        state
            .forward_tracks
            .list_for_track(room, c2, c2_audio_sid)
            .len(),
        1,
        "c2 audio should have one forward track for c1"
    );
    assert!(
        !state
            .pending_media_section_requests
            .contains(room, c2, c2_audio_sid, c1),
        "c1 pending request for c2 audio should be consumed"
    );

    let c1_still_publishes_audio = state
        .rooms
        .list_participants(room)
        .expect("participants should list")
        .into_iter()
        .find(|participant| participant.identity == c1)
        .map(|participant| {
            participant
                .tracks
                .into_iter()
                .any(|track| track.sid == c1_audio_sid)
        })
        .unwrap_or(false);
    assert!(
        c1_still_publishes_audio,
        "c1 audio track should still exist before c2 attachment"
    );

    state.pending_media_section_requests.insert_once(
        room,
        c1,
        c1_audio_sid,
        c2,
        crate::stores::PendingMediaSectionKind::Audio,
    );
    assert!(
        state
            .pending_media_section_requests
            .contains(room, c1, c1_audio_sid, c2),
        "c2 should start with a pending request for c1 audio"
    );

    let (_c2_publish_mids, c2_receive_sections) =
        classify_single_pc_offer_sections(&state, room, c2, &c2_offer_sdp, &HashMap::new());
    assert_eq!(
        c2_receive_sections
            .iter()
            .map(|section| section.mid.as_str())
            .collect::<Vec<_>>(),
        vec!["1"],
        "c2 offer should classify one audio receive section before attach"
    );

    let c2_response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: c2_offer_sdp,
            id: 2,
            ..Default::default()
        },
        &state,
        room,
        c2,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("c2 offer should be answered");
    let Some(proto::signal_response::Message::Answer(c2_answer)) = c2_response.message else {
        panic!("expected c2 answer response");
    };
    let c2_mid_map = c2_answer.mid_to_track_id.get("1").map(String::as_str);
    let c1_forward_track_count = state
        .forward_tracks
        .list_for_track(room, c1, c1_audio_sid)
        .len();
    let c2_pending_after_answer =
        state
            .pending_media_section_requests
            .contains(room, c1, c1_audio_sid, c2);
    assert_eq!(
        c2_mid_map,
        Some(c1_audio_sid),
        "c2 audio receive section should map to c1 audio (forward_tracks={c1_forward_track_count}, pending_after={c2_pending_after_answer})"
    );
    assert_eq!(
        c1_forward_track_count, 1,
        "c1 audio should have one forward track for c2"
    );
    assert!(
        !c2_pending_after_answer,
        "c2 pending request for c1 audio should be consumed"
    );
}

#[tokio::test]
async fn single_pc_mid_reclaim_requeues_still_published_track_instead_of_unsubscribing() {
    let state = state();
    let room = "single-pc-mid-reclaim-requeue-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_a = "TR_audio_a";
    let track_b = "TR_audio_b";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_a.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("track a should be added");
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_b.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("track b should be added");

    let publisher_sid = state
        .rooms
        .get_participant(room, publisher)
        .expect("publisher should be present")
        .sid;

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let answerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("answerer peer connection should create");
    let seed_offer = "v=0\r\n\
        o=- 1 2 IN IP4 0.0.0.0\r\n\
        s=-\r\n\
        t=0 0\r\n\
        a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
        a=group:BUNDLE 0 1\r\n\
        m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
        c=IN IP4 0.0.0.0\r\n\
        a=setup:actpass\r\n\
        a=mid:0\r\n\
        a=recvonly\r\n\
        a=ice-ufrag:test\r\n\
        a=ice-pwd:testpwd\r\n\
        a=rtpmap:111 opus/48000/2\r\n\
        a=rtcp-mux\r\n\
        m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
        c=IN IP4 0.0.0.0\r\n\
        a=setup:actpass\r\n\
        a=mid:1\r\n\
        a=recvonly\r\n\
        a=ice-ufrag:test\r\n\
        a=ice-pwd:testpwd\r\n\
        a=rtpmap:111 opus/48000/2\r\n\
        a=rtcp-mux\r\n";
    answerer
        .set_remote_offer(seed_offer.to_string())
        .await
        .expect("seed remote offer should set");

    let forward_a = answerer
        .add_forwarding_track_to_mid(
            "0",
            &publisher_sid,
            track_a,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("track a forwarding should bind to mid 0");
    let forward_b = answerer
        .add_forwarding_track_to_mid(
            "1",
            &publisher_sid,
            track_b,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("track b forwarding should bind to mid 1");

    state
        .forward_tracks
        .insert_inactive(room, publisher, track_a, subscriber, forward_a);
    state
        .forward_tracks
        .insert_inactive(room, publisher, track_b, subscriber, forward_b);
    state
        .media_forwarding
        .insert_once(room, publisher, track_a, subscriber);
    state
        .media_forwarding
        .insert_once(room, publisher, track_b, subscriber);
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_a, subscriber, true);
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_b, subscriber, true);
    let _ = state
        .rooms
        .set_media_track_subscribed(room, publisher, track_a, subscriber, true);
    let _ = state
        .rooms
        .set_media_track_subscribed(room, publisher, track_b, subscriber, true);

    state.pending_media_section_requests.insert_once(
        room,
        publisher,
        track_a,
        subscriber,
        crate::stores::PendingMediaSectionKind::Audio,
    );

    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let _ = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: offer_sdp.to_string(),
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        subscriber,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("subscriber offer should be answered");

    assert!(
        state
            .pending_media_section_requests
            .contains(room, publisher, track_b, subscriber),
        "reclaimed still-published track should be re-queued for a later receive section"
    );
    assert!(
        state
            .media_subscriptions
            .is_subscribed(room, publisher, track_b, subscriber),
        "reclaimed still-published track should remain logically subscribed"
    );
    assert!(
        state
            .rooms
            .is_media_track_subscribed(room, publisher, track_b, subscriber),
        "room subscription state should not be force-cleared for a still-published reclaimed track"
    );

    offerer.close().await.expect("offerer should close");
    answerer.close().await.expect("answerer should close");
}

#[tokio::test]
async fn single_pc_local_publisher_mid_collision_reclaims_remote_forward_and_requeues_audio() {
    let state = state();
    let room = "single-pc-local-publisher-mid-collision-room";
    let remote_publisher = "remote-publisher";
    let subscriber = "subscriber";
    let remote_track_sid = "TR_remote_audio";
    let local_track_cid = "local-audio-cid";

    join_participant_for_data_track_test(&state, room, remote_publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .rooms
        .add_participant_track(
            room,
            remote_publisher,
            proto::TrackInfo {
                sid: remote_track_sid.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("remote audio track should be added");
    state
        .media_track_cids
        .insert(room, subscriber, local_track_cid, "TR_local_audio");
    state.pending_media_section_requests.insert_once(
        room,
        remote_publisher,
        remote_track_sid,
        subscriber,
        crate::stores::PendingMediaSectionKind::Audio,
    );

    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let receive_offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=group:BUNDLE 1\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let initial_response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: receive_offer_sdp.to_string(),
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        subscriber,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("initial receive offer should be answered");
    let Some(proto::signal_response::Message::Answer(initial_answer)) = initial_response.message
    else {
        panic!("expected initial answer response");
    };
    assert_eq!(
        initial_answer.mid_to_track_id.get("1").map(String::as_str),
        Some(remote_track_sid),
        "the receive section should initially carry the remote forward"
    );
    assert_eq!(
        state
            .forward_tracks
            .list_for_track(room, remote_publisher, remote_track_sid)
            .len(),
        1,
        "the remote audio forward should occupy MID 1 before the collision"
    );

    let publisher_offer_sdp = "v=0\r\n\
            o=- 1 3 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=group:BUNDLE 1\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=sendonly\r\n\
            a=msid:local-stream local-audio-cid\r\n\
            a=ssrc:123456 cname:local-stream\r\n\
            a=ssrc:123456 msid:local-stream local-audio-cid\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: publisher_offer_sdp.to_string(),
            id: 2,
            mid_to_track_id: HashMap::from([("1".to_string(), local_track_cid.to_string())]),
        },
        &state,
        room,
        subscriber,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("publisher MID collision offer should be answered");
    let Some(proto::signal_response::Message::Answer(answer)) = response.message else {
        panic!("expected collision answer response");
    };

    assert!(
        state
            .forward_tracks
            .list_for_track(room, remote_publisher, remote_track_sid)
            .is_empty(),
        "a local publisher section must reclaim the old remote forwarding row on its MID"
    );
    assert!(
        state.pending_media_section_requests.contains(
            room,
            remote_publisher,
            remote_track_sid,
            subscriber,
        ),
        "the still-published remote audio track must be re-queued for a later receive section"
    );
    let publisher_mid_lines = sdp_media_section_lines_for_mid(&answer.sdp, "1");
    assert_eq!(
        sdp_direction_for_mid(&answer.sdp, "1").as_deref(),
        Some("recvonly"),
        "the repurposed local publisher section must answer as recvonly"
    );
    assert!(
        publisher_mid_lines
            .iter()
            .all(|line| !line.contains(remote_track_sid)),
        "a recvonly publisher section must not retain the reclaimed remote forwarding MSID"
    );
}

#[tokio::test]
async fn single_pc_cross_publisher_audio_mid_stays_mapped_when_other_publisher_adds_video() {
    let state = state();
    let room = "single-pc-cross-publisher-audio-video-room";
    let bot_identity = "bot:room";
    let video_publisher_identity = "u:1";
    let subscriber_identity = "listener";
    let bot_audio_track_sid = "TR_bot_english_audio";
    let user_video_track_sid = "TR_user_camera";

    join_participant_for_data_track_test(&state, room, bot_identity);
    join_participant_for_data_track_test(&state, room, video_publisher_identity);
    join_participant_for_data_track_test(&state, room, subscriber_identity);

    state
        .rooms
        .add_participant_track(
            room,
            bot_identity,
            proto::TrackInfo {
                sid: bot_audio_track_sid.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                name: "lang:English".to_string(),
                ..Default::default()
            },
        )
        .expect("bot audio track should be added");
    state.pending_media_section_requests.insert_once(
        room,
        bot_identity,
        bot_audio_track_sid,
        subscriber_identity,
        crate::stores::PendingMediaSectionKind::Audio,
    );

    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    let initial_offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let initial_response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: initial_offer_sdp.to_string(),
            id: 1,
            ..Default::default()
        },
        &state,
        room,
        subscriber_identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("initial bot audio offer should be answered");

    let Some(proto::signal_response::Message::Answer(initial_answer)) = initial_response.message
    else {
        panic!("expected initial answer response");
    };
    assert_eq!(
        initial_answer.mid_to_track_id.get("1").map(String::as_str),
        Some(bot_audio_track_sid),
        "initial audio mid should map to bot audio track"
    );

    state
        .rooms
        .add_participant_track(
            room,
            video_publisher_identity,
            proto::TrackInfo {
                sid: user_video_track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                name: "video:camera".to_string(),
                ..Default::default()
            },
        )
        .expect("user video track should be added");
    state.pending_media_section_requests.insert_once(
        room,
        video_publisher_identity,
        user_video_track_sid,
        subscriber_identity,
        crate::stores::PendingMediaSectionKind::Video,
    );

    let second_offer_sdp = "v=0\r\n\
            o=- 1 3 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1 2\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 109 0 8\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:109 opus/48000/2\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=rtpmap:8 PCMA/8000\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96 97\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            a=rtpmap:97 rtx/90000\r\n\
            a=fmtp:97 apt=96\r\n\
            a=setup:actpass\r\n\
            a=mid:2\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let second_response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: second_offer_sdp.to_string(),
            id: 2,
            ..Default::default()
        },
        &state,
        room,
        subscriber_identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("video-add offer should be answered");

    let Some(proto::signal_response::Message::Answer(second_answer)) = second_response.message
    else {
        panic!("expected second answer response");
    };

    assert_eq!(
        second_answer.mid_to_track_id.get("1").map(String::as_str),
        Some(bot_audio_track_sid),
        "existing bot audio mid must remain mapped to the bot audio track when another publisher adds video"
    );
    assert_eq!(
        second_answer.mid_to_track_id.get("2").map(String::as_str),
        Some(user_video_track_sid),
        "new video mid should map to the publishing user's video track"
    );
    assert_eq!(
        sdp_direction_for_mid(&second_answer.sdp, "1").as_deref(),
        Some("sendonly"),
        "existing bot audio mid should remain active"
    );
    assert_eq!(
        sdp_direction_for_mid(&second_answer.sdp, "2").as_deref(),
        Some("sendonly"),
        "new user video mid should be active"
    );
    let mid1_lines = sdp_media_section_lines_for_mid(&second_answer.sdp, "1");
    assert!(
        mid1_lines
            .iter()
            .any(|line| line.contains(bot_audio_track_sid)),
        "audio mid should retain the bot audio msid/track identity in the answer"
    );

    let initial_mid1_lines = sdp_media_section_lines_for_mid(&initial_answer.sdp, "1");
    let initial_mid1_identity_lines = initial_mid1_lines
        .iter()
        .filter(|line| {
            line.starts_with("a=msid:") || (line.starts_with("a=ssrc:") && line.contains(" msid:"))
        })
        .copied()
        .collect::<Vec<_>>();
    let second_mid1_identity_lines = mid1_lines
        .iter()
        .filter(|line| {
            line.starts_with("a=msid:") || (line.starts_with("a=ssrc:") && line.contains(" msid:"))
        })
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(
        second_mid1_identity_lines, initial_mid1_identity_lines,
        "existing audio m-line MSID identity should be stable when another publisher adds video"
    );
}

#[test]
fn single_pc_recvonly_force_policy_is_false_without_forwardable_remote_tracks() {
    let state = state();
    let room = "single-pc-recvonly-policy-empty-room";
    let subscriber_identity = "subscriber";

    join_participant_for_data_track_test(&state, room, subscriber_identity);

    assert!(
        !should_force_recvonly_for_single_pc_receive_sections(&state, room, subscriber_identity),
        "without remote forwardable tracks, receive sections should not be forced recvonly"
    );
}

#[test]
fn single_pc_recvonly_force_policy_is_true_with_forwardable_remote_track() {
    let state = state();
    let room = "single-pc-recvonly-policy-forwardable-room";
    let publisher_identity = "publisher";
    let subscriber_identity = "subscriber";
    let track_sid = "TR_remote_audio";

    join_participant_for_data_track_test(&state, room, publisher_identity);
    join_participant_for_data_track_test(&state, room, subscriber_identity);

    state
        .rooms
        .add_participant_track(
            room,
            publisher_identity,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Audio as i32,
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
        )
        .expect("remote track should be added");

    state.media_subscriptions.set_subscribed(
        room,
        publisher_identity,
        track_sid,
        subscriber_identity,
        true,
    );
    state
        .rooms
        .set_media_track_subscribed(
            room,
            publisher_identity,
            track_sid,
            subscriber_identity,
            true,
        )
        .expect("room-level media subscription should apply");

    assert!(
        should_force_recvonly_for_single_pc_receive_sections(&state, room, subscriber_identity),
        "forwardable remote tracks should force recvonly on receive sections"
    );
}

#[test]
fn single_pc_classifies_native_recvonly_audio_section_with_two_publishers() {
    let state = state();
    let room = "single-pc-classify-two-publisher-audio";
    let subscriber = "subscriber";
    join_participant_for_data_track_test(&state, room, subscriber);

    for (identity, sid) in [("publisher-a", "TR_audio_a"), ("publisher-b", "TR_audio_b")] {
        join_participant_for_data_track_test(&state, room, identity);
        state
            .rooms
            .add_participant_track(
                room,
                identity,
                proto::TrackInfo {
                    sid: sid.to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    mime_type: "audio/opus".to_string(),
                    ..Default::default()
                },
            )
            .expect("remote audio track should be added");
    }

    let offer_sdp = "v=0\r\n\
        o=- 0 0 IN IP4 127.0.0.1\r\n\
        s=-\r\n\
        t=0 0\r\n\
        m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
        a=mid:audio-recv-0\r\n\
        a=recvonly\r\n";
    let (_, receive_sections) =
        classify_single_pc_offer_sections(&state, room, subscriber, offer_sdp, &HashMap::new());

    assert_eq!(receive_sections.len(), 1);
    assert_eq!(receive_sections[0].mid, "audio-recv-0");
    assert_eq!(receive_sections[0].kind, ReceiveSectionKind::Audio);
}

#[allow(deprecated)]
#[tokio::test]
async fn add_track_response_assigns_unique_track_sids_for_back_to_back_requests() {
    let state = state();
    let room = "media-add-track-unique-sid-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);

    let response_a = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-a".to_string(),
            name: "mic-a".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;
    let response_b = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-b".to_string(),
            name: "mic-b".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;

    let sid_a = match response_a.message {
        Some(proto::signal_response::Message::TrackPublished(track_published)) => {
            track_published
                .track
                .expect("first track info should be present")
                .sid
        }
        other => panic!("unexpected first response: {other:?}"),
    };
    let sid_b = match response_b.message {
        Some(proto::signal_response::Message::TrackPublished(track_published)) => {
            track_published
                .track
                .expect("second track info should be present")
                .sid
        }
        other => panic!("unexpected second response: {other:?}"),
    };

    assert_ne!(sid_a, sid_b, "back-to-back track SIDs should be unique");

    let participant = state
        .rooms
        .get_participant(room, publisher)
        .expect("participant should exist");
    assert_eq!(
        participant.tracks.len(),
        2,
        "both tracks should be retained"
    );
}

#[tokio::test]
async fn add_track_response_preserves_simulcast_layers_and_codecs() {
    let state = state();
    let room = "media-add-track-simulcast-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "video-cid-simulcast".to_string(),
            name: "cam".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            layers: vec![proto::VideoLayer {
                quality: proto::VideoQuality::High as i32,
                width: 1280,
                height: 720,
                bitrate: 1_900_000,
                ssrc: 0,
                ..Default::default()
            }],
            simulcast_codecs: vec![proto::SimulcastCodec {
                codec: "video/vp8".to_string(),
                cid: "simulcast-cid-high".to_string(),
                layers: vec![proto::VideoLayer {
                    quality: proto::VideoQuality::High as i32,
                    width: 1280,
                    height: 720,
                    bitrate: 1_900_000,
                    ssrc: 0,
                    ..Default::default()
                }],
                video_layer_mode: proto::video_layer::Mode::OneSpatialLayerPerStream as i32,
            }],
            ..Default::default()
        },
    )
    .await;

    let published = match response.message {
        Some(proto::signal_response::Message::TrackPublished(published)) => published,
        other => panic!("unexpected response: {other:?}"),
    };
    let track = published
        .track
        .expect("published response should include track info");
    assert!(track.simulcast);
    assert_eq!(track.mime_type, "video/vp8");
    assert_eq!(track.layers.len(), 1);
    assert_eq!(track.codecs.len(), 1);
    assert_eq!(track.codecs[0].mime_type, "video/vp8");
    assert_eq!(track.codecs[0].cid, "simulcast-cid-high");
}

#[allow(deprecated)]
#[tokio::test]
async fn add_track_response_preserves_multi_codec_simulcast_shape() {
    let state = state();
    let room = "media-add-track-multi-codec-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "video-cid-multi-codec".to_string(),
            name: "cam-multi".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            simulcast_codecs: vec![
                proto::SimulcastCodec {
                    codec: "vp8".to_string(),
                    cid: "simulcast-vp8".to_string(),
                    layers: vec![
                        proto::VideoLayer {
                            quality: proto::VideoQuality::Low as i32,
                            width: 320,
                            height: 180,
                            bitrate: 150_000,
                            ssrc: 0,
                            ..Default::default()
                        },
                        proto::VideoLayer {
                            quality: proto::VideoQuality::High as i32,
                            width: 1280,
                            height: 720,
                            bitrate: 1_800_000,
                            ssrc: 0,
                            ..Default::default()
                        },
                    ],
                    video_layer_mode: proto::video_layer::Mode::OneSpatialLayerPerStream as i32,
                },
                proto::SimulcastCodec {
                    codec: "h264".to_string(),
                    cid: "simulcast-h264".to_string(),
                    layers: vec![proto::VideoLayer {
                        quality: proto::VideoQuality::Medium as i32,
                        width: 640,
                        height: 360,
                        bitrate: 600_000,
                        ssrc: 0,
                        ..Default::default()
                    }],
                    video_layer_mode: proto::video_layer::Mode::OneSpatialLayerPerStream as i32,
                },
            ],
            ..Default::default()
        },
    )
    .await;

    let published = match response.message {
        Some(proto::signal_response::Message::TrackPublished(published)) => published,
        other => panic!("unexpected response: {other:?}"),
    };
    let track = published
        .track
        .expect("published response should include track info");

    assert!(track.simulcast);
    assert_eq!(track.codecs.len(), 2);
    assert_eq!(track.codecs[0].mime_type, "video/vp8");
    assert_eq!(track.codecs[0].cid, "simulcast-vp8");
    assert_eq!(track.codecs[0].layers.len(), 2);
    assert_eq!(track.codecs[1].mime_type, "video/h264");
    assert_eq!(track.codecs[1].cid, "simulcast-h264");
    assert_eq!(track.codecs[1].layers.len(), 1);
}

#[tokio::test]
#[allow(deprecated)]
async fn add_track_response_falls_back_when_client_publish_codec_is_disabled() {
    let state = state();
    let room = "media-add-track-disabled-publish-codec-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);
    state.remember_participant_client_info(
        room,
        publisher,
        Some(proto::ClientInfo {
            browser: "firefox".to_string(),
            os: "linux".to_string(),
            ..Default::default()
        }),
    );

    let h264_response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "video-disabled-h264".to_string(),
            name: "cam-disabled-h264".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            simulcast_codecs: vec![proto::SimulcastCodec {
                codec: "h264".to_string(),
                cid: "video-disabled-h264".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await;

    let h264_published = match h264_response.message {
        Some(proto::signal_response::Message::TrackPublished(published)) => published,
        other => panic!("unexpected response: {other:?}"),
    };
    let h264_track = h264_published
        .track
        .expect("published response should include track info");
    assert!(!h264_track.codecs.is_empty());
    assert_eq!(h264_track.codecs[0].mime_type, "video/vp8");
    assert!(
        h264_track
            .codecs
            .iter()
            .all(|codec| codec.mime_type.to_ascii_lowercase() != "video/h264")
    );

    let vp8_response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "video-supported-vp8".to_string(),
            name: "cam-vp8".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            simulcast_codecs: vec![proto::SimulcastCodec {
                codec: "vp8".to_string(),
                cid: "video-supported-vp8".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await;

    let vp8_published = match vp8_response.message {
        Some(proto::signal_response::Message::TrackPublished(published)) => published,
        other => panic!("unexpected response: {other:?}"),
    };
    let vp8_track = vp8_published
        .track
        .expect("published response should include track info");
    assert!(!vp8_track.codecs.is_empty());
    assert_eq!(vp8_track.codecs[0].mime_type, "video/vp8");
}

#[tokio::test]
async fn answer_publisher_offer_filters_h264_from_answer_for_firefox_linux_client() {
    let state = state();
    let room = "single-pc-answer-filter-h264-room";
    let identity = "subscriber";

    join_participant_for_data_track_test(&state, room, identity);
    state.remember_participant_client_info(
        room,
        identity,
        Some(proto::ClientInfo {
            browser: "firefox".to_string(),
            os: "linux".to_string(),
            ..Default::default()
        }),
    );

    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    let offer_sdp = "v=0\r\n\
            o=- 1 2 IN IP4 0.0.0.0\r\n\
            s=-\r\n\
            t=0 0\r\n\
            a=fingerprint:sha-256 D9:D5:EF:C3:37:B8:DC:12:14:87:47:0B:C9:73:2C:6F:D8:1A:1E:3C:C4:CE:2B:D4:EE:32:AC:B6:9B:26:D4:BF\r\n\
            a=msid-semantic: WMS *\r\n\
            a=group:BUNDLE 0 1\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=setup:actpass\r\n\
            a=mid:0\r\n\
            a=sendrecv\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=sctp-port:5000\r\n\
            a=max-message-size:65536\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96 97 102\r\n\
            c=IN IP4 0.0.0.0\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            a=rtpmap:97 rtx/90000\r\n\
            a=fmtp:97 apt=96\r\n\
            a=rtpmap:102 H264/90000\r\n\
            a=fmtp:102 profile-level-id=42e01f;packetization-mode=1\r\n\
            a=setup:actpass\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=ice-ufrag:test\r\n\
            a=ice-pwd:testpwd\r\n\
            a=rtcp-mux\r\n";

    let response = answer_publisher_offer(
        proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: offer_sdp.to_string(),
            id: 33,
            ..Default::default()
        },
        &state,
        room,
        identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    .expect("offer should be answered");

    let Some(proto::signal_response::Message::Answer(answer)) = response.message else {
        panic!("expected answer response");
    };

    assert!(
        answer.sdp.to_ascii_lowercase().contains("a=rtpmap:")
            && answer.sdp.to_ascii_lowercase().contains("vp8/90000"),
        "answer should retain VP8 codec"
    );
    assert!(
        !answer.sdp.to_ascii_lowercase().contains("h264/90000"),
        "answer should remove H264 codec for firefox/linux publish path"
    );
}

#[test]
#[allow(deprecated)]
fn subscribed_quality_update_skips_track_removed_before_emission() {
    let state = state();
    let room = "stale-subscribed-quality-room";
    let publisher = "publisher";
    let stale_track = proto::TrackInfo {
        sid: "TR_stale_video".to_string(),
        r#type: proto::TrackType::Video as i32,
        mime_type: "video/vp8".to_string(),
        ..Default::default()
    };

    join_participant_for_data_track_test(&state, room, publisher);
    state
        .rooms
        .add_participant_track(room, publisher, stale_track.clone())
        .expect("publisher track should be added");
    state
        .rooms
        .remove_participant_track(room, publisher, &stale_track.sid)
        .expect("publisher track should be removed");

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);

    super::session::emit_aggregate_subscribed_quality_update_for_track(
        &state,
        room,
        publisher,
        &stale_track,
        false,
    );

    assert!(
        publisher_outbound_rx.try_recv().is_err(),
        "a removed track must not send quality control to a later same-identity connection"
    );
}

#[test]
#[allow(deprecated)]
fn subscribed_quality_update_waits_for_active_media_receiver_then_emits_demand() {
    let state = state();
    let room = "quality-demand-activation-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track = proto::TrackInfo {
        sid: "TR_quality_activation".to_string(),
        r#type: proto::TrackType::Video as i32,
        mime_type: "video/vp8".to_string(),
        ..Default::default()
    };

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .rooms
        .add_participant_track(room, publisher, track.clone())
        .expect("publisher track should be added");
    state
        .media_subscriptions
        .set_subscribed(room, publisher, &track.sid, subscriber, true);
    let _ = state
        .rooms
        .set_media_track_subscribed(room, publisher, &track.sid, subscriber, false);

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);

    super::session::emit_aggregate_subscribed_quality_update_for_track(
        &state, room, publisher, &track, false,
    );
    assert!(
        publisher_outbound_rx.try_recv().is_err(),
        "a requested but unbound subscription must not send an all-off publisher demand"
    );

    let _ = state
        .rooms
        .set_media_track_subscribed(room, publisher, &track.sid, subscriber, true);
    super::session::emit_aggregate_subscribed_quality_update_for_track(
        &state, room, publisher, &track, false,
    );

    let update = publisher_outbound_rx
        .try_recv()
        .expect("an active receiver should emit publisher quality demand");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) = update.message
    else {
        panic!("expected SubscribedQualityUpdate");
    };
    assert!(
        update
            .subscribed_qualities
            .iter()
            .all(|quality| quality.enabled),
        "an active default-quality receiver should request every spatial layer"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn track_setting_request_updates_dynacast_control_plane_state() {
    let state = state();
    let room = "track-setting-room";
    let participant = "subscriber";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, participant);
    join_participant_for_data_track_test(&state, room, publisher);
    for (track_sid, track_type, mime_type) in [
        ("TR_dynacast_a", proto::TrackType::Video, "video/vp8"),
        ("TR_dynacast_b", proto::TrackType::Video, "video/vp8"),
        ("TR_dynacast_audio", proto::TrackType::Audio, "audio/opus"),
    ] {
        state
            .rooms
            .add_participant_track(
                room,
                publisher,
                proto::TrackInfo {
                    sid: track_sid.to_string(),
                    r#type: track_type as i32,
                    mime_type: mime_type.to_string(),
                    ..Default::default()
                },
            )
            .expect("publisher track should be added");
    }
    for track_sid in ["TR_dynacast_a", "TR_dynacast_b"] {
        state
            .media_subscriptions
            .set_subscribed(room, publisher, track_sid, participant, true);
        let _ =
            state
                .rooms
                .set_media_track_subscribed(room, publisher, track_sid, participant, true);
    }
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);

    let request = proto::UpdateTrackSettings {
        track_sids: vec![
            "TR_dynacast_a".to_string(),
            "TR_dynacast_b".to_string(),
            "TR_dynacast_audio".to_string(),
        ],
        disabled: true,
        quality: proto::VideoQuality::Low as i32,
        width: 320,
        height: 180,
        fps: 15,
        priority: 10,
    };

    let response = signal_response_for_request(
        proto::SignalRequest {
            message: Some(proto::signal_request::Message::TrackSetting(
                request.clone(),
            )),
        },
        &state,
        room,
        participant,
        &outbound_tx,
    )
    .await
    .expect("track-setting request should decode");

    assert!(response.is_none());
    tokio::time::sleep(Duration::from_millis(120)).await;

    let persisted = state
        .track_settings
        .get_for_track(room, participant, "TR_dynacast_a")
        .expect("track-setting state should be persisted");
    assert!(persisted.disabled);
    assert_eq!(persisted.quality, proto::VideoQuality::Low as i32);
    assert_eq!(persisted.width, 320);
    assert_eq!(persisted.height, 180);
    assert_eq!(persisted.fps, 15);
    assert_eq!(persisted.priority, 10);

    assert!(outbound_rx.try_recv().is_err());

    let first = publisher_outbound_rx
        .recv()
        .await
        .expect("first subscribed-quality update should be emitted to publisher");
    let second = publisher_outbound_rx
        .recv()
        .await
        .expect("second subscribed-quality update should be emitted to publisher");
    assert!(
        publisher_outbound_rx.try_recv().is_err(),
        "audio tracks must not receive video subscribed-quality updates"
    );

    let updates = [first, second]
        .into_iter()
        .map(|response| match response.message {
            Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) => update,
            other => panic!("expected SubscribedQualityUpdate, got: {other:?}"),
        })
        .collect::<Vec<_>>();

    assert_eq!(updates.len(), 2);
    for update in &updates {
        assert!(
            update.track_sid == "TR_dynacast_a" || update.track_sid == "TR_dynacast_b",
            "unexpected track sid in update: {}",
            update.track_sid
        );
        assert_eq!(update.subscribed_qualities.len(), 3);
        assert!(
            update
                .subscribed_qualities
                .iter()
                .all(|quality| !quality.enabled),
            "disabled track setting should emit disabled subscribed qualities"
        );
        assert_eq!(update.subscribed_codecs.len(), 1);
        assert_eq!(update.subscribed_codecs[0].codec, "video/vp8");
        assert_eq!(update.subscribed_codecs[0].qualities.len(), 3);
        assert!(
            update.subscribed_codecs[0]
                .qualities
                .iter()
                .all(|quality| !quality.enabled)
        );
    }
}

#[tokio::test]
#[allow(deprecated)]
async fn track_setting_disable_then_enable_restores_forwarding_and_publisher_layer_demand() {
    let state = state();
    let room = "track-setting-enable-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_adaptive_video";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    let track = proto::TrackInfo {
        sid: track_sid.to_string(),
        r#type: proto::TrackType::Video as i32,
        mime_type: "video/av1".to_string(),
        simulcast: true,
        layers: vec![
            proto::VideoLayer {
                quality: proto::VideoQuality::Low as i32,
                width: 384,
                height: 216,
                ..Default::default()
            },
            proto::VideoLayer {
                quality: proto::VideoQuality::Medium as i32,
                width: 960,
                height: 540,
                ..Default::default()
            },
            proto::VideoLayer {
                quality: proto::VideoQuality::High as i32,
                width: 1280,
                height: 720,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    state
        .rooms
        .add_participant_track(room, publisher, track.clone())
        .expect("publisher video track should be added");
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, subscriber, true);
    state
        .rooms
        .set_media_track_subscribed(room, publisher, track_sid, subscriber, true)
        .expect("room subscription should be set");

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    for (disabled, width, height) in [(true, 948, 121), (false, 948, 940)] {
        signal_response_for_request(
            proto::SignalRequest {
                message: Some(proto::signal_request::Message::TrackSetting(
                    proto::UpdateTrackSettings {
                        track_sids: vec![track_sid.to_string()],
                        disabled,
                        quality: proto::VideoQuality::High as i32,
                        width,
                        height,
                        ..Default::default()
                    },
                )),
            },
            &state,
            room,
            subscriber,
            &outbound_tx,
        )
        .await
        .expect("track setting should process");

        let response = publisher_outbound_rx
            .recv()
            .await
            .expect("publisher should receive a subscribed-quality update");
        let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) =
            response.message
        else {
            panic!("expected SubscribedQualityUpdate");
        };
        assert_eq!(update.track_sid, track_sid);
        if disabled {
            assert!(
                update
                    .subscribed_qualities
                    .iter()
                    .all(|quality| !quality.enabled),
                "disabling must remove publisher layer demand"
            );
            assert!(
                !session::should_forward_media_for_subscriber_with_track_settings(
                    &state.media_subscriptions,
                    &state.auto_subscribe_preferences,
                    &state.track_settings,
                    &state.rooms,
                    room,
                    publisher,
                    track_sid,
                    subscriber,
                ),
                "disabled settings must stop this subscriber's forwarding"
            );
        } else {
            assert!(
                update
                    .subscribed_qualities
                    .iter()
                    .all(|quality| quality.enabled),
                "the final enabled setting must restore publisher layer demand"
            );
            assert!(
                session::should_forward_media_for_subscriber_with_track_settings(
                    &state.media_subscriptions,
                    &state.auto_subscribe_preferences,
                    &state.track_settings,
                    &state.rooms,
                    room,
                    publisher,
                    track_sid,
                    subscriber,
                ),
                "the final enabled setting must restore forwarding"
            );
        }
    }

    assert_eq!(
        session::requested_video_quality_for_track(
            &state.track_settings,
            room,
            subscriber,
            track_sid,
            Some(&track),
        ),
        Some(proto::VideoQuality::High),
        "the final dimensions, not the transient disabled dimensions, determine the active layer"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn aggregate_requested_quality_uses_default_subscription_when_no_explicit_entry() {
    let state = state();
    let room = "aggregate-default-subscription-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_default_subscription_aggregate";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                simulcast: true,
                layers: vec![
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Low as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Medium as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::High as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .expect("publisher track should be added");

    // No explicit MediaSubscriptionStore entry is set here. The active signal
    // session makes this participant a default subscriber rather than a retained leaver.
    let _ = state
        .rooms
        .set_media_track_subscribed(room, publisher, track_sid, subscriber, true);
    let (subscriber_signal_tx, _subscriber_signal_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, subscriber, subscriber_signal_tx);

    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let high_request = proto::UpdateTrackSettings {
        track_sids: vec![track_sid.to_string()],
        quality: proto::VideoQuality::High as i32,
        ..Default::default()
    };
    signal_response_for_request(
        proto::SignalRequest {
            message: Some(proto::signal_request::Message::TrackSetting(high_request)),
        },
        &state,
        room,
        subscriber,
        &outbound_tx,
    )
    .await
    .expect("high track setting should process");

    assert_eq!(
        session::aggregate_requested_quality_for_track(&state, room, publisher, track_sid),
        Some(proto::VideoQuality::High),
        "default-subscribed tracks should contribute to aggregate publisher quality demand"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn track_setting_request_uses_aggregate_max_quality_across_subscribers() {
    let state = state();
    let room = "track-setting-aggregate-room";
    let publisher = "publisher";
    let high_subscriber = "high-subscriber";
    let low_subscriber = "low-subscriber";
    let track_sid = "TR_aggregate_quality";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, high_subscriber);
    join_participant_for_data_track_test(&state, room, low_subscriber);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                simulcast: true,
                layers: vec![
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Low as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Medium as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::High as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .expect("publisher track should be added");

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, high_subscriber, true);
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, low_subscriber, true);
    let _ =
        state
            .rooms
            .set_media_track_subscribed(room, publisher, track_sid, high_subscriber, true);
    let _ =
        state
            .rooms
            .set_media_track_subscribed(room, publisher, track_sid, low_subscriber, true);

    let high_request = proto::UpdateTrackSettings {
        track_sids: vec![track_sid.to_string()],
        quality: proto::VideoQuality::High as i32,
        ..Default::default()
    };
    signal_response_for_request(
        proto::SignalRequest {
            message: Some(proto::signal_request::Message::TrackSetting(high_request)),
        },
        &state,
        room,
        high_subscriber,
        &outbound_tx,
    )
    .await
    .expect("high track setting should process");

    let high_response = publisher_outbound_rx
        .recv()
        .await
        .expect("high aggregate update should be emitted");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(high_update)) =
        high_response.message
    else {
        panic!("expected SubscribedQualityUpdate message");
    };
    assert!(
        high_update
            .subscribed_qualities
            .iter()
            .all(|quality| quality.enabled)
    );

    let low_request = proto::UpdateTrackSettings {
        track_sids: vec![track_sid.to_string()],
        quality: proto::VideoQuality::Low as i32,
        ..Default::default()
    };
    signal_response_for_request(
        proto::SignalRequest {
            message: Some(proto::signal_request::Message::TrackSetting(low_request)),
        },
        &state,
        room,
        low_subscriber,
        &outbound_tx,
    )
    .await
    .expect("low track setting should process");

    let aggregate_response = publisher_outbound_rx
        .recv()
        .await
        .expect("aggregate update should be emitted");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(aggregate_update)) =
        aggregate_response.message
    else {
        panic!("expected SubscribedQualityUpdate message");
    };

    assert_eq!(aggregate_update.track_sid, track_sid);
    assert!(
        aggregate_update
            .subscribed_qualities
            .iter()
            .all(|quality| quality.enabled),
        "low subscriber must not disable high while another subscriber still requests high"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn unsubscribe_recomputes_aggregate_quality_after_track_setting_removal() {
    let state = state();
    let room = "aggregate-unsubscribe-room";
    let publisher = "publisher";
    let high_subscriber = "high-subscriber";
    let low_subscriber = "low-subscriber";
    let track_sid = "TR_unsub_aggregate";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, high_subscriber);
    join_participant_for_data_track_test(&state, room, low_subscriber);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                simulcast: true,
                layers: vec![
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Low as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::High as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .expect("track should be added");

    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, high_subscriber, true);
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, low_subscriber, true);
    let _ =
        state
            .rooms
            .set_media_track_subscribed(room, publisher, track_sid, high_subscriber, true);
    let _ =
        state
            .rooms
            .set_media_track_subscribed(room, publisher, track_sid, low_subscriber, true);

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    for (identity, quality) in [
        (high_subscriber, proto::VideoQuality::High),
        (low_subscriber, proto::VideoQuality::Low),
    ] {
        signal_response_for_request(
            proto::SignalRequest {
                message: Some(proto::signal_request::Message::TrackSetting(
                    proto::UpdateTrackSettings {
                        track_sids: vec![track_sid.to_string()],
                        quality: quality as i32,
                        ..Default::default()
                    },
                )),
            },
            &state,
            room,
            identity,
            &outbound_tx,
        )
        .await
        .expect("track setting should process");
        let _ = publisher_outbound_rx.recv().await;
    }

    handle_media_subscription_request(
        &state,
        room,
        high_subscriber,
        proto::UpdateSubscription {
            track_sids: vec![track_sid.to_string()],
            subscribe: false,
            ..Default::default()
        },
        false,
    )
    .await;

    let response = publisher_outbound_rx
        .recv()
        .await
        .expect("aggregate update should be emitted on unsubscribe");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) = response.message
    else {
        panic!("expected SubscribedQualityUpdate");
    };

    assert_eq!(update.track_sid, track_sid);
    assert!(update.subscribed_qualities[0].enabled);
    assert!(!update.subscribed_qualities[1].enabled);
    assert!(!update.subscribed_qualities[2].enabled);
    assert!(
        state
            .track_settings
            .get_for_track(room, high_subscriber, track_sid)
            .is_none(),
        "unsubscribing should clear cached track setting for that track"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn participant_leave_recomputes_aggregate_quality() {
    let state = state();
    let room = "aggregate-leave-room";
    let publisher = "publisher";
    let high_subscriber = "high-subscriber";
    let low_subscriber = "low-subscriber";
    let track_sid = "TR_leave_aggregate";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, high_subscriber);
    join_participant_for_data_track_test(&state, room, low_subscriber);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                simulcast: true,
                layers: vec![
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Low as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::High as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .expect("track should be added");

    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, high_subscriber, true);
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, low_subscriber, true);
    let _ =
        state
            .rooms
            .set_media_track_subscribed(room, publisher, track_sid, high_subscriber, true);
    let _ =
        state
            .rooms
            .set_media_track_subscribed(room, publisher, track_sid, low_subscriber, true);

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    for (identity, quality) in [
        (high_subscriber, proto::VideoQuality::High),
        (low_subscriber, proto::VideoQuality::Low),
    ] {
        signal_response_for_request(
            proto::SignalRequest {
                message: Some(proto::signal_request::Message::TrackSetting(
                    proto::UpdateTrackSettings {
                        track_sids: vec![track_sid.to_string()],
                        quality: quality as i32,
                        ..Default::default()
                    },
                )),
            },
            &state,
            room,
            identity,
            &outbound_tx,
        )
        .await
        .expect("track setting should process");
        let _ = publisher_outbound_rx.recv().await;
    }

    cleanup_participant_runtime_state(&state, room, high_subscriber, true).await;

    let response = publisher_outbound_rx
        .recv()
        .await
        .expect("aggregate update should be emitted on leave");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) = response.message
    else {
        panic!("expected SubscribedQualityUpdate");
    };

    assert_eq!(update.track_sid, track_sid);
    assert!(update.subscribed_qualities[0].enabled);
    assert!(!update.subscribed_qualities[1].enabled);
    assert!(!update.subscribed_qualities[2].enabled);
}

#[allow(deprecated)]
#[test]
fn aggregate_requested_quality_for_track_defaults_to_high_for_subscribed_without_setting() {
    let state = state();
    let room = "aggregate-default-high-room";
    let publisher = "publisher";
    let subscriber_no_setting = "subscriber-a";
    let subscriber_low = "subscriber-b";
    let track_sid = "TR_default_high";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber_no_setting);
    join_participant_for_data_track_test(&state, room, subscriber_low);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                simulcast: true,
                layers: vec![
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Low as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::High as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .expect("track should be added");

    state.media_subscriptions.set_subscribed(
        room,
        publisher,
        track_sid,
        subscriber_no_setting,
        true,
    );
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, subscriber_low, true);
    let _ = state.rooms.set_media_track_subscribed(
        room,
        publisher,
        track_sid,
        subscriber_no_setting,
        true,
    );
    let _ =
        state
            .rooms
            .set_media_track_subscribed(room, publisher, track_sid, subscriber_low, true);

    state.track_settings.upsert_from_request(
        room,
        subscriber_low,
        &proto::UpdateTrackSettings {
            track_sids: vec![track_sid.to_string()],
            quality: proto::VideoQuality::Low as i32,
            ..Default::default()
        },
    );

    let aggregate =
        super::session::aggregate_requested_quality_for_track(&state, room, publisher, track_sid);
    assert_eq!(aggregate, Some(proto::VideoQuality::High));
}

#[allow(deprecated)]
#[test]
fn requested_video_quality_for_track_prefers_dimensions_over_default_high_quality() {
    let state = state();
    let room = "dimension-quality-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_dimension_quality";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let track = proto::TrackInfo {
        sid: track_sid.to_string(),
        r#type: proto::TrackType::Video as i32,
        mime_type: "video/vp8".to_string(),
        simulcast: true,
        layers: vec![
            proto::VideoLayer {
                quality: proto::VideoQuality::Low as i32,
                width: 320,
                height: 180,
                ..Default::default()
            },
            proto::VideoLayer {
                quality: proto::VideoQuality::Medium as i32,
                width: 640,
                height: 360,
                ..Default::default()
            },
            proto::VideoLayer {
                quality: proto::VideoQuality::High as i32,
                width: 1280,
                height: 720,
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    state
        .rooms
        .add_participant_track(room, publisher, track.clone())
        .expect("track should be added");

    state.track_settings.upsert_from_request(
        room,
        subscriber,
        &proto::UpdateTrackSettings {
            track_sids: vec![track_sid.to_string()],
            // server-sdk-go defaults this field to HIGH; dimensions should still drive layer selection.
            quality: proto::VideoQuality::High as i32,
            width: 320,
            height: 180,
            ..Default::default()
        },
    );

    let requested = super::session::requested_video_quality_for_track(
        &state.track_settings,
        room,
        subscriber,
        track_sid,
        Some(&track),
    );
    assert_eq!(requested, Some(proto::VideoQuality::Low));
}

#[allow(deprecated)]
#[test]
fn aggregate_requested_quality_for_track_uses_dimension_derived_quality() {
    let state = state();
    let room = "aggregate-dimension-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_aggregate_dimension";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                simulcast: true,
                layers: vec![
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Low as i32,
                        width: 320,
                        height: 180,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Medium as i32,
                        width: 640,
                        height: 360,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::High as i32,
                        width: 1280,
                        height: 720,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .expect("track should be added");

    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, subscriber, true);
    let _ = state
        .rooms
        .set_media_track_subscribed(room, publisher, track_sid, subscriber, true);

    state.track_settings.upsert_from_request(
        room,
        subscriber,
        &proto::UpdateTrackSettings {
            track_sids: vec![track_sid.to_string()],
            quality: proto::VideoQuality::High as i32,
            width: 320,
            height: 180,
            ..Default::default()
        },
    );

    let aggregate =
        super::session::aggregate_requested_quality_for_track(&state, room, publisher, track_sid);
    assert_eq!(aggregate, Some(proto::VideoQuality::Low));
}

#[allow(deprecated)]
#[test]
fn layer_quality_maps_prefer_track_metadata_before_rid_heuristics() {
    let track = proto::TrackInfo {
        sid: "TR_layers".to_string(),
        r#type: proto::TrackType::Video as i32,
        codecs: vec![proto::SimulcastCodecInfo {
            mime_type: "video/vp8".to_string(),
            layers: vec![
                proto::VideoLayer {
                    quality: proto::VideoQuality::Medium as i32,
                    rid: "x-custom".to_string(),
                    ssrc: 777,
                    ..Default::default()
                },
                proto::VideoLayer {
                    quality: proto::VideoQuality::High as i32,
                    rid: "f".to_string(),
                    ssrc: 888,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };

    let (ssrc_map, rid_map) = super::session::layer_quality_maps_for_track(&track);

    assert_eq!(
        super::session::packet_video_quality_for_track(777, Some("q"), &ssrc_map, &rid_map),
        Some(proto::VideoQuality::Medium),
        "SSRC metadata should override heuristic RID quality"
    );

    assert_eq!(
        super::session::packet_video_quality_for_track(1, Some("x-custom"), &ssrc_map, &rid_map),
        Some(proto::VideoQuality::Medium),
        "metadata RID should map even when non-standard"
    );
}

#[test]
fn fps_forwarding_state_limits_frame_rate_by_timestamp() {
    let mut state = super::session::FpsForwardingState::default();

    assert!(state.should_forward_packet(0, 15));
    assert!(
        state.should_forward_packet(0, 15),
        "same timestamp frame packets should consistently follow first decision"
    );

    // 15 fps target => min delta about 6000 at 90kHz clock, this should be dropped.
    assert!(!state.should_forward_packet(3_000, 15));

    // Crossing threshold should pass.
    assert!(state.should_forward_packet(6_100, 15));
}

#[test]
fn retain_fps_forwarding_state_for_current_targets_prunes_stale_entries() {
    let active_key = (
        "room".to_string(),
        "publisher".to_string(),
        "track".to_string(),
        "subscriber-a".to_string(),
    );
    let stale_key = (
        "room".to_string(),
        "publisher".to_string(),
        "track".to_string(),
        "subscriber-b".to_string(),
    );

    let mut fps_states = std::collections::HashMap::from([
        (
            active_key.clone(),
            super::session::FpsForwardingState::default(),
        ),
        (stale_key, super::session::FpsForwardingState::default()),
    ]);
    let current_forward_keys = std::collections::HashSet::from([active_key.clone()]);

    super::session::retain_fps_forwarding_state_for_current_targets(
        &mut fps_states,
        &current_forward_keys,
    );

    assert_eq!(fps_states.len(), 1);
    assert!(fps_states.contains_key(&active_key));
}

#[test]
fn forwarding_target_refresh_is_revision_scoped_and_prunes_stale_state() {
    let active_key = (
        "room".to_string(),
        "publisher".to_string(),
        "track".to_string(),
        "subscriber-a".to_string(),
    );
    let stale_key = (
        "room".to_string(),
        "publisher".to_string(),
        "track".to_string(),
        "subscriber-b".to_string(),
    );
    let current_forward_keys = std::collections::HashSet::from([active_key.clone()]);
    let mut states = std::collections::HashMap::from([(active_key.clone(), 1_u8), (stale_key, 2)]);

    assert!(super::session::forwarding_target_revision_changed(None, 7));
    assert!(super::session::forwarding_target_revision_changed(
        Some(6),
        7
    ));
    assert!(!super::session::forwarding_target_revision_changed(
        Some(7),
        7
    ));

    super::session::retain_forwarding_state_for_current_targets(&mut states, &current_forward_keys);

    assert_eq!(states, std::collections::HashMap::from([(active_key, 1)]));
}

#[test]
fn forwarding_debug_heartbeat_is_consumed_by_the_next_video_packet() {
    let mut heartbeat_due = true;

    assert!(super::session::take_forwarding_debug_heartbeat(
        &mut heartbeat_due
    ));
    assert!(
        !super::session::take_forwarding_debug_heartbeat(&mut heartbeat_due),
        "one timer tick must produce at most one debug-count scan"
    );
}

#[test]
fn vp8_temporal_layer_id_from_payload_extracts_tid_when_present() {
    // Minimal VP8 payload descriptor with extension and T/K byte present.
    // 0x90 => X=1, S=1. 0x20 => T/K present only. 0x80 => TID=2 (10b).
    let payload = [0x90, 0x20, 0x80, 0x00];
    assert_eq!(
        super::session::vp8_temporal_layer_id_from_payload(&payload),
        Some(2)
    );
}

#[test]
fn vp8_temporal_layer_id_from_payload_rejects_out_of_range_tid() {
    // TID bits 11 => 3, which is outside common [0,1,2] layering.
    let payload = [0x90, 0x20, 0xC0, 0x00];
    assert_eq!(
        super::session::vp8_temporal_layer_id_from_payload(&payload),
        None
    );
}

#[test]
fn should_forward_video_packet_for_requested_fps_uses_vp8_temporal_layer_when_available() {
    let mut fps_state = super::session::FpsForwardingState::default();

    let receiver_temporal_layer_fps = Some([Some(8.0), Some(16.0), Some(30.0)]);

    // TID=2 should be dropped for very low FPS requests.
    let tid2_payload = [0x90, 0x20, 0x80, 0x00];
    assert!(
        !super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/vp8"),
            receiver_temporal_layer_fps,
            15_000,
            &tid2_payload,
            None,
            &mut fps_state,
        )
    );

    // TID=0 should pass under the same request.
    let tid0_payload = [0x90, 0x20, 0x00, 0x00];
    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/vp8"),
            receiver_temporal_layer_fps,
            24_000,
            &tid0_payload,
            None,
            &mut fps_state,
        )
    );
    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/vp8"),
            receiver_temporal_layer_fps,
            24_001,
            &tid0_payload,
            None,
            &mut fps_state,
        ),
        "a selected base temporal layer near the requested FPS is not timestamp-capped"
    );
}

#[test]
fn vp9_temporal_layer_id_from_payload_extracts_tid_when_layer_info_present() {
    // First byte: I=0, L=1, rest 0. Next byte layer info with TID=2 (0b010xxxxx).
    let payload = [0x20, 0x40, 0x00];
    assert_eq!(
        super::session::vp9_temporal_layer_id_from_payload(&payload),
        Some(2)
    );
}

#[test]
fn vp9_temporal_layer_id_from_payload_rejects_out_of_range_tid() {
    // TID 111 => 7, outside common [0,1,2] layering.
    let payload = [0x20, 0xE0, 0x00];
    assert_eq!(
        super::session::vp9_temporal_layer_id_from_payload(&payload),
        None
    );
}

#[test]
fn single_scalable_source_requires_known_scalable_codec_without_simulcast_mapping() {
    assert!(super::session::is_single_scalable_source(
        Some("video/vp9"),
        false
    ));
    assert!(super::session::is_single_scalable_source(
        Some("video/AV1"),
        false
    ));
    assert!(!super::session::is_single_scalable_source(
        Some("video/vp8"),
        false
    ));
    assert!(!super::session::is_single_scalable_source(
        Some("video/vp9"),
        true
    ));
}

#[test]
fn vp9_spatial_layer_id_from_payload_extracts_sid_when_present() {
    // L bit set, layer info byte with SID=3 (bits 4..1) and TID=2.
    let payload = [0x20, 0x46, 0x00];
    assert_eq!(
        super::session::vp9_spatial_layer_id_from_payload(&payload),
        Some(3)
    );
}

#[test]
fn vp9_spatial_layer_id_from_payload_rejects_out_of_range_sid() {
    // SID=4 is outside common supported range [0..=3].
    let payload = [0x20, 0x48, 0x00];
    assert_eq!(
        super::session::vp9_spatial_layer_id_from_payload(&payload),
        None
    );
}

#[test]
fn should_forward_video_packet_for_requested_fps_uses_vp9_temporal_layer_when_available() {
    let mut fps_state = super::session::FpsForwardingState::default();

    let receiver_temporal_layer_fps = Some([Some(8.0), Some(16.0), Some(30.0)]);

    // VP9 layer info byte with TID=2 should be dropped.
    let tid2_payload = [0x20, 0x40, 0x00];
    assert!(
        !super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/vp9"),
            receiver_temporal_layer_fps,
            15_000,
            &tid2_payload,
            None,
            &mut fps_state,
        )
    );

    // VP9 layer info byte with TID=0 should pass.
    let tid0_payload = [0x20, 0x00, 0x00];
    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/vp9"),
            receiver_temporal_layer_fps,
            24_000,
            &tid0_payload,
            None,
            &mut fps_state,
        )
    );
}

#[test]
fn h265_temporal_layer_id_from_payload_extracts_tid_when_present() {
    // H265 NAL header low 3 bits are tid_plus_one.
    // 0x03 => tid_plus_one=3 => temporal=2.
    let payload_tid2 = [0x02, 0x03, 0x00, 0x00];
    assert_eq!(
        super::session::h265_temporal_layer_id_from_payload(&payload_tid2),
        Some(2)
    );

    // 0x01 => tid_plus_one=1 => temporal=0.
    let payload_tid0 = [0x02, 0x01, 0x00, 0x00];
    assert_eq!(
        super::session::h265_temporal_layer_id_from_payload(&payload_tid0),
        Some(0)
    );
}

#[test]
fn h265_temporal_layer_id_from_payload_rejects_invalid_or_out_of_range_tid() {
    assert_eq!(
        super::session::h265_temporal_layer_id_from_payload(&[0x02]),
        None
    );

    let invalid_tid_zero = [0x02, 0x00, 0x00, 0x00];
    assert_eq!(
        super::session::h265_temporal_layer_id_from_payload(&invalid_tid_zero),
        None
    );

    let payload_tid3 = [0x02, 0x04, 0x00, 0x00];
    assert_eq!(
        super::session::h265_temporal_layer_id_from_payload(&payload_tid3),
        None
    );
}

#[test]
fn should_forward_video_packet_for_requested_fps_uses_h265_temporal_layer_when_available() {
    let mut fps_state = super::session::FpsForwardingState::default();

    let receiver_temporal_layer_fps = Some([Some(8.0), Some(16.0), Some(30.0)]);

    // temporal=2 should be dropped for low FPS requests.
    let tid2_payload = [0x02, 0x03, 0x00, 0x00];
    assert!(
        !super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/h265"),
            receiver_temporal_layer_fps,
            15_000,
            &tid2_payload,
            None,
            &mut fps_state,
        )
    );

    // temporal=0 should pass.
    let tid0_payload = [0x02, 0x01, 0x00, 0x00];
    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/h265"),
            receiver_temporal_layer_fps,
            24_000,
            &tid0_payload,
            None,
            &mut fps_state,
        )
    );
}

#[test]
fn should_forward_video_packet_for_requested_fps_uses_temporal_layer_hint_when_codec_payload_has_no_temporal_bits()
 {
    let mut fps_state = super::session::FpsForwardingState::default();

    let receiver_temporal_layer_fps = Some([Some(8.0), Some(16.0), Some(30.0)]);
    let payload_without_temporal = [0x00, 0x00, 0x00, 0x00];

    assert!(
        !super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/av1"),
            receiver_temporal_layer_fps,
            9_000,
            &payload_without_temporal,
            Some(2),
            &mut fps_state,
        ),
        "hinted temporal layer should be respected even when codec payload parser has no temporal metadata"
    );

    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/av1"),
            receiver_temporal_layer_fps,
            18_000,
            &payload_without_temporal,
            Some(0),
            &mut fps_state,
        ),
        "low hinted temporal layer should pass"
    );
}

#[test]
fn should_forward_video_packet_for_requested_fps_applies_timestamp_cap_when_selected_temporal_layer_exceeds_requested_cadence()
 {
    let mut fps_state = super::session::FpsForwardingState::default();

    // Receiver reports only one temporal layer cadence effectively at 30fps.
    let receiver_temporal_layer_fps = Some([Some(30.0), None, None]);

    // VP8 payload with TID=0.
    let tid0_payload = [0x90, 0x20, 0x00, 0x00];

    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/vp8"),
            receiver_temporal_layer_fps,
            0,
            &tid0_payload,
            None,
            &mut fps_state,
        ),
        "first frame should pass"
    );

    assert!(
        !super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/vp8"),
            receiver_temporal_layer_fps,
            3_000,
            &tid0_payload,
            None,
            &mut fps_state,
        ),
        "timestamp gate should drop frames that arrive too soon for requested fps"
    );

    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            8,
            Some("video/vp8"),
            receiver_temporal_layer_fps,
            12_000,
            &tid0_payload,
            None,
            &mut fps_state,
        ),
        "timestamp gate should allow frames once cadence threshold is met"
    );
}

#[test]
fn should_forward_video_packet_for_requested_fps_falls_back_to_timestamp_gate_without_temporal_info()
 {
    let mut fps_state = super::session::FpsForwardingState::default();

    let payload_without_temporal = [0x10, 0x00, 0x00, 0x00];
    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            15,
            Some("video/h264"),
            None,
            0,
            &payload_without_temporal,
            None,
            &mut fps_state,
        )
    );
    assert!(
        !super::session::should_forward_video_packet_for_requested_fps(
            15,
            Some("video/h264"),
            None,
            3_000,
            &payload_without_temporal,
            None,
            &mut fps_state,
        )
    );
    assert!(
        super::session::should_forward_video_packet_for_requested_fps(
            15,
            Some("video/h264"),
            None,
            6_100,
            &payload_without_temporal,
            None,
            &mut fps_state,
        )
    );
}

#[test]
fn max_temporal_layer_for_requested_fps_from_receiver_respects_tolerance() {
    let receiver_temporal_layer_fps = [Some(8.0), Some(16.0), Some(30.0)];

    // request=8 => effective=7.2, should pick first satisfying layer => 0.
    assert_eq!(
        super::session::max_temporal_layer_for_requested_fps_from_receiver(
            8,
            &receiver_temporal_layer_fps,
        ),
        Some(0)
    );

    // request=14 => effective=12.6, layer0(8) not enough, layer1(16) should match.
    assert_eq!(
        super::session::max_temporal_layer_for_requested_fps_from_receiver(
            14,
            &receiver_temporal_layer_fps,
        ),
        Some(1)
    );
}

#[allow(deprecated)]
#[test]
fn infer_video_quality_from_rid_maps_common_simulcast_rids() {
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("q")),
        Some(proto::VideoQuality::Low)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("h")),
        Some(proto::VideoQuality::Medium)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("f")),
        Some(proto::VideoQuality::High)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("low")),
        Some(proto::VideoQuality::Low)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("medium")),
        Some(proto::VideoQuality::Medium)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("high")),
        Some(proto::VideoQuality::High)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("2")),
        Some(proto::VideoQuality::Low)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("1")),
        Some(proto::VideoQuality::Medium)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("0")),
        Some(proto::VideoQuality::High)
    );
    assert_eq!(
        super::session::infer_video_quality_from_rid(Some("unknown")),
        None
    );
    assert_eq!(super::session::infer_video_quality_from_rid(None), None);
}

#[allow(deprecated)]
#[test]
fn should_forward_video_packet_for_requested_quality_enforces_max_when_known() {
    assert!(
        super::session::should_forward_video_packet_for_requested_quality(
            Some(proto::VideoQuality::Low),
            Some(proto::VideoQuality::Low),
        )
    );
    assert!(
        !super::session::should_forward_video_packet_for_requested_quality(
            Some(proto::VideoQuality::Low),
            Some(proto::VideoQuality::Medium),
        )
    );
    assert!(
        !super::session::should_forward_video_packet_for_requested_quality(
            Some(proto::VideoQuality::Medium),
            Some(proto::VideoQuality::High),
        )
    );
    assert!(
        super::session::should_forward_video_packet_for_requested_quality(
            Some(proto::VideoQuality::High),
            Some(proto::VideoQuality::Medium),
        )
    );

    // Unknown packet quality should not black-hole traffic.
    assert!(
        super::session::should_forward_video_packet_for_requested_quality(
            Some(proto::VideoQuality::Low),
            None,
        )
    );

    // No explicit max quality means forward.
    assert!(
        super::session::should_forward_video_packet_for_requested_quality(
            None,
            Some(proto::VideoQuality::High),
        )
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn recommended_subscribed_quality_update_emits_low_when_degraded_without_manual_track_settings()
 {
    let state = state();
    let room = "recommended-quality-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_auto_quality";

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);

    join_participant_for_data_track_test(&state, room, publisher);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                simulcast: true,
                layers: vec![proto::VideoLayer {
                    quality: proto::VideoQuality::High as i32,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .expect("publisher track should be added");

    session::maybe_emit_recommended_subscribed_quality_update(
        &state.signal_connections,
        &state.track_settings,
        &state.rooms,
        room,
        publisher,
        track_sid,
        subscriber,
        RecommendedVideoQuality::Low,
    );

    let response = publisher_outbound_rx
        .recv()
        .await
        .expect("recommended quality update should be emitted");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) = response.message
    else {
        panic!("expected SubscribedQualityUpdate message");
    };

    assert_eq!(update.track_sid, track_sid);
    assert_eq!(update.subscribed_qualities.len(), 3);
    assert!(update.subscribed_qualities[0].enabled);
    assert!(!update.subscribed_qualities[1].enabled);
    assert!(!update.subscribed_qualities[2].enabled);
}

#[tokio::test]
#[allow(deprecated)]
async fn recommended_subscribed_quality_update_emits_medium_when_recommended() {
    let state = state();
    let room = "recommended-quality-medium-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_auto_quality_medium";

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);

    join_participant_for_data_track_test(&state, room, publisher);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                simulcast: true,
                layers: vec![
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Low as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Medium as i32,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::High as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .expect("publisher track should be added");

    session::maybe_emit_recommended_subscribed_quality_update(
        &state.signal_connections,
        &state.track_settings,
        &state.rooms,
        room,
        publisher,
        track_sid,
        subscriber,
        RecommendedVideoQuality::Medium,
    );

    let response = publisher_outbound_rx
        .recv()
        .await
        .expect("recommended medium quality update should be emitted");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) = response.message
    else {
        panic!("expected SubscribedQualityUpdate message");
    };

    assert_eq!(update.track_sid, track_sid);
    assert_eq!(update.subscribed_qualities.len(), 3);
    assert!(update.subscribed_qualities[0].enabled);
    assert!(update.subscribed_qualities[1].enabled);
    assert!(!update.subscribed_qualities[2].enabled);
}

#[tokio::test]
#[allow(deprecated)]
async fn recommended_subscribed_quality_update_skips_for_non_simulcast_track() {
    let state = state();
    let room = "recommended-quality-non-simulcast-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_non_sim_quality";

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);

    join_participant_for_data_track_test(&state, room, publisher);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                ..Default::default()
            },
        )
        .expect("publisher track should be added");

    session::maybe_emit_recommended_subscribed_quality_update(
        &state.signal_connections,
        &state.track_settings,
        &state.rooms,
        room,
        publisher,
        track_sid,
        subscriber,
        RecommendedVideoQuality::Low,
    );

    assert!(publisher_outbound_rx.try_recv().is_err());
}

#[tokio::test]
#[allow(deprecated)]
async fn recommended_subscribed_quality_update_skips_when_track_not_present_in_publisher_state() {
    let state = state();
    let room = "recommended-quality-missing-track-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_missing_quality";

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);

    session::maybe_emit_recommended_subscribed_quality_update(
        &state.signal_connections,
        &state.track_settings,
        &state.rooms,
        room,
        publisher,
        track_sid,
        subscriber,
        RecommendedVideoQuality::Low,
    );

    assert!(publisher_outbound_rx.try_recv().is_err());
}

#[tokio::test]
#[allow(deprecated)]
async fn recommended_subscribed_quality_update_skips_when_manual_track_settings_exist() {
    let state = state();
    let room = "recommended-quality-manual-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_manual_quality";

    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);

    state.track_settings.upsert_from_request(
        room,
        subscriber,
        &proto::UpdateTrackSettings {
            track_sids: vec![track_sid.to_string()],
            disabled: false,
            quality: proto::VideoQuality::High as i32,
            ..Default::default()
        },
    );

    session::maybe_emit_recommended_subscribed_quality_update(
        &state.signal_connections,
        &state.track_settings,
        &state.rooms,
        room,
        publisher,
        track_sid,
        subscriber,
        RecommendedVideoQuality::Low,
    );

    assert!(publisher_outbound_rx.try_recv().is_err());
}

#[tokio::test]
#[allow(deprecated)]
async fn relayed_signal_request_bytes_captures_outbound_only_responses() {
    let state = state();
    let room = "relayed-outbound-capture-room";
    let participant = "subscriber";
    let publisher = "publisher";
    join_participant_for_data_track_test(&state, room, participant);
    join_participant_for_data_track_test(&state, room, publisher);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: "TR_relayed_dynacast".to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                ..Default::default()
            },
        )
        .expect("publisher track should be added");
    let (publisher_outbound_tx, mut publisher_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, publisher, publisher_outbound_tx);
    state.media_subscriptions.set_subscribed(
        room,
        publisher,
        "TR_relayed_dynacast",
        participant,
        true,
    );
    let _ = state.rooms.set_media_track_subscribed(
        room,
        publisher,
        "TR_relayed_dynacast",
        participant,
        true,
    );

    let request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::TrackSetting(
            proto::UpdateTrackSettings {
                track_sids: vec!["TR_relayed_dynacast".to_string()],
                disabled: true,
                quality: proto::VideoQuality::Low as i32,
                ..Default::default()
            },
        )),
    };

    let response = state
        .handle_relayed_signal_request_bytes(room, participant, &request.encode_to_vec())
        .await;

    let NonLocalRelaySignalRequestResponse::NoResponse = response else {
        panic!(
            "expected no direct relay response when update is routed via publisher signal connection, got: {response:?}"
        );
    };

    let published_update = publisher_outbound_rx
        .recv()
        .await
        .expect("publisher outbound signal connection should receive subscribed quality update");

    let decoded = match published_update.message {
        Some(message) => proto::SignalResponse {
            message: Some(message),
        },
        None => panic!("publisher outbound signal response should contain a message"),
    };
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) = decoded.message
    else {
        panic!("expected subscribed quality update routed to publisher signal connection");
    };
    assert_eq!(update.track_sid, "TR_relayed_dynacast");
    assert!(
        update
            .subscribed_qualities
            .iter()
            .all(|quality| !quality.enabled)
    );
}

#[tokio::test]
async fn reconcile_publisher_media_tracks_keeps_single_pc_unbound_track_when_offer_is_inactive() {
    let state = state();
    let room = "reconcile-no-active-mids-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-reconcile".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let published_track = track_published.track.expect("track info should be present");

    let participant_before = state
        .rooms
        .get_participant(room, publisher)
        .expect("participant should exist");
    assert_eq!(participant_before.tracks.len(), 1);
    assert_eq!(participant_before.tracks[0].sid, published_track.sid);
    assert!(participant_before.tracks[0].mid.is_empty());

    let unpublish_like_offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            a=mid:0\r\n\
            a=inactive\r\n";
    reconcile_publisher_media_tracks_after_answer(
        &state,
        room,
        publisher,
        unpublish_like_offer_sdp,
        &HashMap::new(),
        &HashMap::new(),
        true,
    )
    .await;

    let participant_after = state
        .rooms
        .get_participant(room, publisher)
        .expect("participant should exist");
    assert_eq!(
        participant_after.tracks.len(),
        1,
        "a single-PC inactive reserve section must not remove an unbound publication"
    );
    assert_eq!(participant_after.tracks[0].sid, published_track.sid);
}

#[tokio::test]
async fn reconcile_publisher_media_tracks_falls_back_to_single_unbound_browser_track() {
    let state = state();
    let room = "reconcile-browser-sdp-cid-room";
    let publisher = "publisher";
    let signal_cid = "signal-video-cid";
    let browser_sdp_track_id = "{browser-generated-track-id}";

    join_participant_for_data_track_test(&state, room, publisher);
    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: signal_cid.to_string(),
            name: "camera".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let track = track_published
        .track
        .expect("video track should be present");

    let offer_sdp = "v=0\r\n\
        m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
        a=mid:3\r\n\
        a=sendonly\r\n\
        a=msid:browser-stream {browser_sdp_track_id}\r\n";
    let mut sdp_track_ids = HashMap::new();
    sdp_track_ids.insert("3".to_string(), browser_sdp_track_id.to_string());
    let signal_cids = HashMap::new();

    reconcile_publisher_media_tracks_after_answer(
        &state,
        room,
        publisher,
        offer_sdp,
        &sdp_track_ids,
        &signal_cids,
        false,
    )
    .await;

    assert_eq!(
        state
            .media_track_cids
            .find_track_sid(room, publisher, browser_sdp_track_id),
        Some(track.sid.clone())
    );
    let participant = state
        .rooms
        .get_participant(room, publisher)
        .expect("publisher should exist");
    let reconciled = participant
        .tracks
        .iter()
        .find(|candidate| candidate.sid == track.sid)
        .expect("published track should remain");
    assert_eq!(reconciled.mid, "3");
    assert!(
        reconciled
            .codecs
            .iter()
            .all(|codec| codec.sdp_cid == browser_sdp_track_id)
    );
}

#[tokio::test]
async fn resolve_forward_track_info_matches_browser_track_by_negotiated_mid() {
    let state = state();
    let room = "resolve-browser-mid-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);
    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "signal-video-cid".to_string(),
            name: "camera".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let published_track = track_published
        .track
        .expect("video track should be present");
    state
        .rooms
        .set_participant_track_mid(room, publisher, &published_track.sid, "3")
        .expect("track MID should be set");

    let resolved = crate::session::resolve_forward_track_info(
        &state.rooms,
        &state.media_track_cids,
        room,
        publisher,
        "{browser-generated-track-id}",
        Some("3"),
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
    )
    .await
    .expect("remote track should resolve through its negotiated MID");

    assert_eq!(resolved.sid, published_track.sid);
}

#[tokio::test]
async fn reconcile_publisher_media_tracks_after_answer_does_not_remove_a_track_added_after_the_offer()
 {
    let (state, events) = state_with_webhook_collector();
    let room = "reconcile-late-track-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);

    let audio = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-old-offer".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(audio_published)) = audio.message
    else {
        panic!("expected audio TrackPublished response");
    };
    let audio = audio_published
        .track
        .expect("audio track should be present");
    state
        .rooms
        .set_participant_track_mid(room, publisher, &audio.sid, "0")
        .expect("audio track mid should be set");

    let video = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "video-cid-added-after-offer".to_string(),
            name: "camera".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::Camera as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(video_published)) = video.message
    else {
        panic!("expected video TrackPublished response");
    };
    let video = video_published
        .track
        .expect("video track should be present");
    state
        .rooms
        .set_participant_track_mid(room, publisher, &video.sid, "1")
        .expect("video track mid should be set");

    let old_offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            a=mid:0\r\n\
            a=inactive\r\n";
    let mut old_offer_mid_to_track_id = HashMap::new();
    old_offer_mid_to_track_id.insert("0".to_string(), audio.sid.clone());
    reconcile_publisher_media_tracks_after_answer(
        &state,
        room,
        publisher,
        old_offer_sdp,
        &old_offer_mid_to_track_id,
        &HashMap::new(),
        false,
    )
    .await;

    let participant = state
        .rooms
        .get_participant(room, publisher)
        .expect("publisher should remain in the room");
    assert!(
        !participant
            .tracks
            .iter()
            .any(|track| track.sid == audio.sid),
        "the inactive track from the reconciled offer should be removed"
    );
    assert!(
        participant
            .tracks
            .iter()
            .any(|track| track.sid == video.sid),
        "a track introduced after the reconciled offer must remain published"
    );

    let unpublished_sids = events
        .lock()
        .expect("webhook collector lock should not be poisoned")
        .iter()
        .filter(|event| event.event == "track_unpublished")
        .filter_map(|event| event.track.as_ref().map(|track| track.sid.clone()))
        .collect::<Vec<_>>();
    assert_eq!(unpublished_sids, vec![audio.sid]);
}

#[tokio::test]
async fn reconcile_publisher_media_tracks_after_answer_removes_dual_pc_track_when_sender_is_removed()
 {
    let state = state();
    let room = "reconcile-no-active-mids-removes-negotiated-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-reconcile-negotiated".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let published_track = track_published.track.expect("track info should be present");
    assert_eq!(
        state
            .rooms
            .get_participant(room, publisher)
            .expect("participant should exist")
            .tracks
            .first()
            .expect("publication should be stored")
            .sid,
        published_track.sid,
    );

    let unpublish_offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            a=mid:0\r\n\
            a=inactive\r\n";
    reconcile_publisher_media_tracks_after_answer(
        &state,
        room,
        publisher,
        unpublish_offer_sdp,
        &HashMap::new(),
        &HashMap::new(),
        false,
    )
    .await;

    let participant_after = state
        .rooms
        .get_participant(room, publisher)
        .expect("participant should still exist");
    assert!(
        participant_after.tracks.is_empty(),
        "a dual-PC publisher's inactive section must unpublish its uniquely matching unbound track"
    );
}

#[tokio::test]
async fn reconcile_publisher_media_tracks_keeps_single_pc_unbound_track_for_inactive_reserve() {
    let state = state();
    let room = "reconcile-single-pc-recvonly-reserve-room";
    let publisher = "publisher";

    join_participant_for_data_track_test(&state, room, publisher);
    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-single-pc-reserve".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let published_track = track_published.track.expect("track info should be present");
    let reserve_offer_sdp = "v=0\r\n\
            m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
            a=mid:0\r\n\
            a=inactive\r\n";
    reconcile_publisher_media_tracks_after_answer(
        &state,
        room,
        publisher,
        reserve_offer_sdp,
        &HashMap::new(),
        &HashMap::new(),
        true,
    )
    .await;

    let participant_after = state
        .rooms
        .get_participant(room, publisher)
        .expect("participant should still exist");
    assert_eq!(participant_after.tracks.len(), 1);
    assert_eq!(participant_after.tracks[0].sid, published_track.sid);
}

#[test]
fn requested_media_track_sids_merges_top_level_and_participant_tracks() {
    let request = proto::UpdateSubscription {
        track_sids: vec!["TR_b".to_string(), "TR_a".to_string()],
        subscribe: false,
        participant_tracks: vec![
            proto::ParticipantTracks {
                participant_sid: "PA_1".to_string(),
                track_sids: vec!["TR_c".to_string(), "TR_a".to_string()],
            },
            proto::ParticipantTracks {
                participant_sid: "PA_2".to_string(),
                track_sids: vec!["TR_d".to_string()],
            },
        ],
    };

    assert_eq!(
        requested_media_track_sids(&request),
        vec![
            "TR_a".to_string(),
            "TR_b".to_string(),
            "TR_c".to_string(),
            "TR_d".to_string(),
        ]
    );
}

#[tokio::test]
async fn handle_media_subscription_request_unsubscribe_from_participant_tracks_marks_unsubscribed()
{
    let state = state();
    let room = "media-unsubscribe-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-2".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;

    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let track_info = track_published.track.expect("track info should be present");

    handle_media_subscription_request(
        &state,
        room,
        subscriber,
        proto::UpdateSubscription {
            track_sids: Vec::new(),
            subscribe: false,
            participant_tracks: vec![proto::ParticipantTracks {
                participant_sid: "ignored".to_string(),
                track_sids: vec![track_info.sid.clone()],
            }],
        },
        false,
    )
    .await;

    assert!(
        !state
            .media_subscriptions
            .is_subscribed(room, publisher, &track_info.sid, subscriber,)
    );
}

#[test]
fn should_forward_media_for_subscriber_requires_both_signaling_and_room_store_subscriptions() {
    let state = state();
    let room = "media-forward-gate-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: "TR_gate".to_string(),
                ..Default::default()
            },
        )
        .expect("publisher track should be added");

    assert!(
        should_forward_media_for_subscriber(
            &state.media_subscriptions,
            &state.rooms,
            room,
            publisher,
            "TR_gate",
            subscriber,
        ),
        "default subscription state should forward when both stores allow"
    );

    state
        .rooms
        .set_media_track_subscribed(room, publisher, "TR_gate", subscriber, false)
        .expect("room store unsubscribe should apply");
    assert!(
        !should_forward_media_for_subscriber(
            &state.media_subscriptions,
            &state.rooms,
            room,
            publisher,
            "TR_gate",
            subscriber,
        ),
        "room-store unsubscribe should block forwarding even if signaling store is subscribed"
    );

    state
        .rooms
        .set_media_track_subscribed(room, publisher, "TR_gate", subscriber, true)
        .expect("room store resubscribe should apply");
    state
        .media_subscriptions
        .set_subscribed(room, publisher, "TR_gate", subscriber, false);
    assert!(
        !should_forward_media_for_subscriber(
            &state.media_subscriptions,
            &state.rooms,
            room,
            publisher,
            "TR_gate",
            subscriber,
        ),
        "signaling-store unsubscribe should block forwarding even if room store is subscribed"
    );

    state
        .media_subscriptions
        .set_subscribed(room, publisher, "TR_gate", subscriber, true);
    state.track_settings.upsert_from_request(
        room,
        subscriber,
        &proto::UpdateTrackSettings {
            track_sids: vec!["TR_gate".to_string()],
            disabled: true,
            ..Default::default()
        },
    );
    assert!(
        !session::should_forward_media_for_subscriber_with_track_settings(
            &state.media_subscriptions,
            &state.auto_subscribe_preferences,
            &state.track_settings,
            &state.rooms,
            room,
            publisher,
            "TR_gate",
            subscriber,
        ),
        "disabled track setting should gate forwarding even when subscription stores allow"
    );

    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: "TR_auto".to_string(),
                ..Default::default()
            },
        )
        .expect("auto-subscribe test track should be added");
    state
        .rooms
        .set_media_track_subscribed(room, publisher, "TR_auto", subscriber, true)
        .expect("room store should allow forwarding for auto-subscribe test track");
    state
        .auto_subscribe_preferences
        .set_auto_subscribe(room, subscriber, false);

    assert!(
        !session::should_forward_media_for_subscriber_with_track_settings(
            &state.media_subscriptions,
            &state.auto_subscribe_preferences,
            &state.track_settings,
            &state.rooms,
            room,
            publisher,
            "TR_auto",
            subscriber,
        ),
        "auto-subscribe=false with no explicit subscription should block forwarding"
    );

    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, false);
    state
        .auto_subscribe_preferences
        .set_auto_subscribe(room, subscriber, true);
    assert!(
        !session::should_forward_media_for_subscriber_with_track_settings(
            &state.media_subscriptions,
            &state.auto_subscribe_preferences,
            &state.track_settings,
            &state.rooms,
            room,
            publisher,
            "TR_gate",
            subscriber,
        ),
        "can_subscribe=false should block forwarding even when subscription stores allow"
    );
}

#[tokio::test]
async fn media_subscription_subscribe_switches_same_named_audio_track_to_new_sid() {
    let state = state();
    let room = "subscription-switch-same-name-audio-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let old_track = proto::TrackInfo {
        sid: "TR_old_original".to_string(),
        r#type: proto::TrackType::Audio as i32,
        name: "lang:Original".to_string(),
        mime_type: "audio/opus".to_string(),
        ..Default::default()
    };
    let new_track = proto::TrackInfo {
        sid: "TR_new_original".to_string(),
        r#type: proto::TrackType::Audio as i32,
        name: "lang:Original".to_string(),
        mime_type: "audio/opus".to_string(),
        ..Default::default()
    };

    state
        .rooms
        .add_participant_track(room, publisher, old_track.clone())
        .expect("old track should be added");
    state
        .rooms
        .add_participant_track(room, publisher, new_track.clone())
        .expect("new track should be added");

    state
        .media_subscriptions
        .set_subscribed(room, publisher, &old_track.sid, subscriber, true);

    handle_media_subscription_request(
        &state,
        room,
        subscriber,
        proto::UpdateSubscription {
            track_sids: vec![new_track.sid.clone()],
            subscribe: true,
            ..Default::default()
        },
        false,
    )
    .await;

    assert!(
        !state
            .media_subscriptions
            .is_subscribed(room, publisher, &old_track.sid, subscriber),
        "old same-name audio track should be unsubscribed when switching to new source sid"
    );
    assert!(
        state
            .media_subscriptions
            .is_subscribed(room, publisher, &new_track.sid, subscriber),
        "new same-name audio track should be subscribed"
    );
}

#[tokio::test]
async fn client_update_subscription_requires_can_subscribe_permission() {
    let state = state();
    let room = "media-subscribe-permission-gate-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, false);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-perm-gate".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let track_sid = track_published.track.expect("track should exist").sid;
    state
        .media_subscriptions
        .set_subscribed(room, publisher, &track_sid, subscriber, false);

    handle_media_subscription_request(
        &state,
        room,
        subscriber,
        proto::UpdateSubscription {
            track_sids: vec![track_sid.clone()],
            subscribe: true,
            ..Default::default()
        },
        false,
    )
    .await;

    assert!(
        !state
            .media_subscriptions
            .is_subscribed(room, publisher, &track_sid, subscriber),
        "client subscription request must be blocked when can_subscribe is false"
    );
}

#[tokio::test]
async fn handle_media_subscription_request_emits_codec_unsupported_response_for_unsupported_video()
{
    let state = state();
    let room = "media-subscribe-unsupported-codec-request-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let (subscriber_outbound_tx, mut subscriber_outbound_rx) =
        tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, subscriber, subscriber_outbound_tx);

    state.remember_participant_subscribe_video_mime_types(
        room,
        subscriber,
        &std::collections::HashSet::from(["video/vp8".to_string()]),
    );

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "video-cid-unsupported-request".to_string(),
            name: "screen".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::ScreenShare as i32,
            simulcast_codecs: vec![proto::SimulcastCodec {
                codec: "h264".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await;

    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let track_sid = track_published.track.expect("track should exist").sid;

    handle_media_subscription_request(
        &state,
        room,
        subscriber,
        proto::UpdateSubscription {
            track_sids: vec![track_sid.clone()],
            subscribe: true,
            ..Default::default()
        },
        false,
    )
    .await;

    let subscription_response = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        subscriber_outbound_rx.recv(),
    )
    .await
    .expect("subscription response should arrive before timeout")
    .expect("expected outbound subscription response");

    let Some(proto::signal_response::Message::SubscriptionResponse(subscription_response)) =
        subscription_response.message
    else {
        panic!("expected SubscriptionResponse message");
    };

    assert_eq!(subscription_response.track_sid, track_sid);
    assert_eq!(
        subscription_response.err,
        proto::SubscriptionError::SeCodecUnsupported as i32
    );
    assert_eq!(
        state
            .media_subscriptions
            .explicit_subscription(room, publisher, &track_sid, subscriber),
        Some(false),
        "unsupported codec request should record explicit unsubscribe"
    );
    assert!(
        !state
            .rooms
            .is_media_track_subscribed(room, publisher, &track_sid, subscriber),
        "unsupported codec request should be reflected in room subscription state"
    );
}

#[test]
fn automatic_video_forwarding_defers_codec_rejection_until_negotiation() {
    let state = state();
    let room = "automatic-video-codec-negotiation-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_h264";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state.remember_participant_subscribe_video_mime_types(
        room,
        subscriber,
        &std::collections::HashSet::from(["video/vp8".to_string()]),
    );

    let h264_track = proto::TrackInfo {
        sid: track_sid.to_string(),
        r#type: proto::TrackType::Video as i32,
        mime_type: "video/h264".to_string(),
        ..Default::default()
    };

    assert!(
        !super::session::reject_unsupported_video_subscription_if_needed(
            &state,
            room,
            publisher,
            subscriber,
            &h264_track,
            false,
        ),
        "automatic forwarding must offer a newly published codec before treating a previous VP8-only answer as rejection"
    );
    assert_eq!(
        state
            .media_subscriptions
            .explicit_subscription(room, publisher, track_sid, subscriber),
        None,
        "deferred automatic negotiation must not persist an explicit unsubscribe"
    );
}

#[test]
fn raw_reliable_payload_data_packet_wraps_payload_and_sender_sid() {
    let state = state();
    let room = "raw-reliable-wrap-room";
    let sender = "sender";
    join_participant_for_data_track_test(&state, room, sender);
    let participant = state
        .rooms
        .get_participant(room, sender)
        .expect("participant should be present");

    let packet = session::raw_reliable_payload_data_packet(
        b"raw payload".to_vec(),
        &state.rooms,
        room,
        sender,
    );

    assert_eq!(packet.kind, proto::data_packet::Kind::Reliable as i32);
    assert_eq!(packet.participant_identity, sender);
    let Some(proto::data_packet::Value::User(user)) = packet.value else {
        panic!("expected user packet");
    };
    assert_eq!(user.payload, b"raw payload");
    assert_eq!(user.participant_sid, participant.sid);
    assert_eq!(user.participant_identity, sender);
}

#[tokio::test]
async fn subscriber_answer_rejecting_video_mid_emits_codec_unsupported_response() {
    let state = state();
    let room = "media-subscribe-unsupported-codec-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let (subscriber_outbound_tx, mut subscriber_outbound_rx) =
        tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, subscriber, subscriber_outbound_tx);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "video-cid-unsupported".to_string(),
            name: "screen".to_string(),
            r#type: proto::TrackType::Video as i32,
            source: proto::TrackSource::ScreenShare as i32,
            simulcast_codecs: vec![proto::SimulcastCodec {
                codec: "h264".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await;

    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let track_sid = track_published.track.expect("track should exist").sid;

    state
        .media_subscriptions
        .set_subscribed(room, publisher, &track_sid, subscriber, true);
    state
        .rooms
        .set_media_track_subscribed(room, publisher, &track_sid, subscriber, true)
        .expect("track subscription should update");
    state.remember_subscriber_offer_mid_track_ids(
        room,
        subscriber,
        7,
        std::collections::HashMap::from([("1".to_string(), track_sid.clone())]),
    );

    session::reject_unaccepted_video_tracks_from_subscriber_answer(
        &state,
        room,
        subscriber,
        7,
        "v=0\r\n\
 m=video 0 UDP/TLS/RTP/SAVPF 108\r\n\
 a=mid:1\r\n\
 a=inactive\r\n",
    )
    .await;

    let subscription_response = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        subscriber_outbound_rx.recv(),
    )
    .await
    .expect("subscription response should arrive before timeout")
    .expect("expected outbound subscription response");

    let Some(proto::signal_response::Message::SubscriptionResponse(subscription_response)) =
        subscription_response.message
    else {
        panic!("expected SubscriptionResponse message");
    };

    assert_eq!(subscription_response.track_sid, track_sid);
    assert_eq!(
        subscription_response.err,
        proto::SubscriptionError::SeCodecUnsupported as i32
    );
    assert!(
        !state
            .media_subscriptions
            .is_subscribed(room, publisher, &track_sid, subscriber),
        "unsupported codec must not remain subscribed"
    );
}

#[tokio::test]
async fn unsupported_bound_video_track_emits_one_error_and_is_removed() {
    let state = state();
    let room = "bound-video-codec-unsupported-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_h264";
    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/h264".to_string(),
                ..Default::default()
            },
        )
        .expect("H264 track should add");
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, subscriber, true);
    state
        .rooms
        .set_media_track_subscribed(room, publisher, track_sid, subscriber, true)
        .expect("room subscription should set");
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, subscriber, outbound_tx);

    state.remember_participant_subscribe_video_mime_types(
        room,
        subscriber,
        &std::collections::HashSet::from(["video/vp8".to_string()]),
    );

    let forwarder = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("forwarder should create");
    let receiver = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("receiver should create");
    let forward_track = forwarder
        .add_forwarding_track_with_mime(
            "PA_publisher",
            track_sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
            Some("video/h264"),
        )
        .await
        .expect("H264 forwarding track should add");
    let offer = forwarder.create_offer().await.expect("offer should create");
    receiver
        .set_remote_offer(offer)
        .await
        .expect("receiver should accept offer");
    let answer = receiver
        .create_answer()
        .await
        .expect("answer should create");
    let h264_pt = answer
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix("a=rtpmap:")?;
            let (pt, codec) = rest.split_once(' ')?;
            codec
                .to_ascii_lowercase()
                .starts_with("h264/")
                .then_some(pt)
        })
        .expect("answer should advertise H264")
        .to_string();
    let vp8_only_answer = answer
        .lines()
        .map(|line| {
            if line.starts_with("m=video ") {
                line.replace(&h264_pt, "96")
            } else if line.starts_with(&format!("a=rtpmap:{h264_pt} ")) {
                "a=rtpmap:96 VP8/90000".to_string()
            } else if line.starts_with(&format!("a=fmtp:{h264_pt}")) {
                String::new()
            } else if line.starts_with(&format!("a=rtcp-fb:{h264_pt}")) {
                line.replacen(&h264_pt, "96", 1)
            } else {
                line.to_string()
            }
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\r\n");
    forwarder
        .set_remote_answer(format!("{vp8_only_answer}\r\n"))
        .await
        .expect("forwarder should accept answer");
    assert_eq!(
        forward_track.bind_result().await,
        oxidesfu_rtc::ForwardTrackBindResult::UnsupportedCodec
    );
    state
        .forward_tracks
        .insert_inactive(room, publisher, track_sid, subscriber, forward_track);

    let track_sids = std::collections::HashSet::from([track_sid.to_string()]);
    session::activate_tracks_with_compatible_bind_results(&state, room, subscriber, &track_sids)
        .await;
    let response = tokio::time::timeout(std::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("unsupported binding should emit a response before timeout")
        .expect("unsupported binding should emit a response");
    let Some(proto::signal_response::Message::SubscriptionResponse(response)) = response.message
    else {
        panic!("expected subscription response");
    };
    assert_eq!(response.track_sid, track_sid);
    assert_eq!(
        response.err,
        proto::SubscriptionError::SeCodecUnsupported as i32
    );
    assert!(
        state
            .forward_tracks
            .list_for_track(room, publisher, track_sid)
            .is_empty(),
        "unsupported track must not remain forwardable"
    );
    assert_eq!(
        state
            .media_subscriptions
            .explicit_subscription(room, publisher, track_sid, subscriber),
        Some(false)
    );

    session::activate_tracks_with_compatible_bind_results(&state, room, subscriber, &track_sids)
        .await;
    assert!(
        outbound_rx.try_recv().is_err(),
        "error must be emitted once"
    );

    forwarder.close().await.expect("forwarder should close");
    receiver.close().await.expect("receiver should close");
}

#[tokio::test]
async fn dual_pc_subscriber_answer_correlates_vp8_only_mid_to_h264_forward_track() {
    let state = state();
    let room = "dual-pc-answer-bind-correlation-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let track_sid = "TR_h264";
    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    let publisher_info = state
        .rooms
        .get_participant(room, publisher)
        .expect("publisher should exist");
    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/h264".to_string(),
                ..Default::default()
            },
        )
        .expect("H264 track should add");
    state
        .media_subscriptions
        .set_subscribed(room, publisher, track_sid, subscriber, true);
    state
        .rooms
        .set_media_track_subscribed(room, publisher, track_sid, subscriber, true)
        .expect("room subscription should set");

    let forwarder = state.peer_connections.insert(
        room,
        subscriber,
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("forwarder should create"),
    );
    let receiver = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("receiver should create");
    let forward_track = forwarder
        .add_forwarding_track_with_mime(
            &publisher_info.sid,
            track_sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
            Some("video/h264"),
        )
        .await
        .expect("H264 forwarding track should add");
    state
        .forward_tracks
        .insert_inactive(room, publisher, track_sid, subscriber, forward_track);

    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, subscriber, outbound_tx.clone());
    session::signal_media_forwarding_negotiation_with_offer_id(
        &state,
        &state.subscriber_offer_ids,
        room,
        subscriber,
        &forwarder,
        MediaForwardingConnectionKind::DualPcSubscriber,
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        &outbound_tx,
    )
    .await
    .expect("forwarding offer should emit");
    let offer_response = outbound_rx.recv().await.expect("offer should arrive");
    let Some(proto::signal_response::Message::Offer(offer)) = offer_response.message else {
        panic!("expected forwarding offer");
    };
    let offered_mids = state.subscriber_offer_mid_track_ids(room, subscriber, offer.id);
    assert!(
        offered_mids.values().any(|sid| sid == track_sid),
        "the emitted offer ID must correlate the H264 track"
    );

    receiver
        .set_remote_offer(offer.sdp)
        .await
        .expect("receiver should accept offer");
    let answer = receiver
        .create_answer()
        .await
        .expect("answer should create");
    let h264_pt = answer
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix("a=rtpmap:")?;
            let (pt, codec) = rest.split_once(' ')?;
            codec
                .to_ascii_lowercase()
                .starts_with("h264/")
                .then_some(pt)
        })
        .expect("answer should advertise H264")
        .to_string();
    let vp8_only_answer = answer
        .lines()
        .map(|line| {
            if line.starts_with("m=video ") {
                line.replace(&h264_pt, "96")
            } else if line.starts_with(&format!("a=rtpmap:{h264_pt} ")) {
                "a=rtpmap:96 VP8/90000".to_string()
            } else if line.starts_with(&format!("a=fmtp:{h264_pt}")) {
                String::new()
            } else if line.starts_with(&format!("a=rtcp-fb:{h264_pt}")) {
                line.replacen(&h264_pt, "96", 1)
            } else {
                line.to_string()
            }
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\r\n");

    signal_response_for_request(
        proto::SignalRequest {
            message: Some(proto::signal_request::Message::Answer(
                proto::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp: format!("{vp8_only_answer}\r\n"),
                    id: offer.id,
                    ..Default::default()
                },
            )),
        },
        &state,
        room,
        subscriber,
        &outbound_tx,
    )
    .await
    .expect("answer handler should succeed");

    let response = outbound_rx
        .recv()
        .await
        .expect("unsupported codec response should arrive");
    let Some(proto::signal_response::Message::SubscriptionResponse(response)) = response.message
    else {
        panic!("expected subscription response");
    };
    assert_eq!(response.track_sid, track_sid);
    assert_eq!(
        response.err,
        proto::SubscriptionError::SeCodecUnsupported as i32
    );
    assert_eq!(
        state
            .media_subscriptions
            .explicit_subscription(room, publisher, track_sid, subscriber),
        Some(false)
    );
    assert!(
        state
            .forward_tracks
            .list_for_track(room, publisher, track_sid)
            .is_empty()
    );

    forwarder.close().await.expect("forwarder should close");
    receiver.close().await.expect("receiver should close");
}

#[tokio::test]
async fn dual_pc_subscriber_answer_rejects_new_h264_without_disrupting_existing_audio_and_vp8() {
    let state = state();
    let room = "dual-pc-multitrack-answer-bind-correlation-room";
    let publisher = "publisher";
    let subscriber = "subscriber";
    let audio_track_sid = "TR_audio";
    let vp8_track_sid = "TR_vp8";
    let h264_track_sid = "TR_h264";
    let followup_vp8_track_sid = "TR_vp8_followup";
    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    let publisher_info = state
        .rooms
        .get_participant(room, publisher)
        .expect("publisher should exist");

    for (track_sid, track_type, mime_type) in [
        (
            audio_track_sid,
            proto::TrackType::Audio as i32,
            "audio/opus",
        ),
        (vp8_track_sid, proto::TrackType::Video as i32, "video/vp8"),
        (h264_track_sid, proto::TrackType::Video as i32, "video/h264"),
    ] {
        state
            .rooms
            .add_participant_track(
                room,
                publisher,
                proto::TrackInfo {
                    sid: track_sid.to_string(),
                    r#type: track_type,
                    mime_type: mime_type.to_string(),
                    ..Default::default()
                },
            )
            .expect("publisher track should add");
        state
            .media_subscriptions
            .set_subscribed(room, publisher, track_sid, subscriber, true);
        state
            .rooms
            .set_media_track_subscribed(room, publisher, track_sid, subscriber, true)
            .expect("room subscription should set");
    }

    let forwarder = state.peer_connections.insert(
        room,
        subscriber,
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("forwarder should create"),
    );
    let receiver = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("receiver should create");
    let audio_forward_track = forwarder
        .add_forwarding_track_with_mime(
            &publisher_info.sid,
            audio_track_sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
            Some("audio/opus"),
        )
        .await
        .expect("audio forwarding track should add");
    let vp8_forward_track = forwarder
        .add_forwarding_track_with_mime(
            &publisher_info.sid,
            vp8_track_sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
            Some("video/vp8"),
        )
        .await
        .expect("VP8 forwarding track should add");
    state.forward_tracks.insert_inactive(
        room,
        publisher,
        audio_track_sid,
        subscriber,
        audio_forward_track,
    );
    state.forward_tracks.insert_inactive(
        room,
        publisher,
        vp8_track_sid,
        subscriber,
        vp8_forward_track,
    );

    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert(room, subscriber, outbound_tx.clone());
    session::signal_media_forwarding_negotiation_with_offer_id(
        &state,
        &state.subscriber_offer_ids,
        room,
        subscriber,
        &forwarder,
        MediaForwardingConnectionKind::DualPcSubscriber,
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        &outbound_tx,
    )
    .await
    .expect("initial audio and VP8 forwarding offer should emit");
    let initial_offer_response = outbound_rx
        .recv()
        .await
        .expect("initial offer should arrive");
    let Some(proto::signal_response::Message::Offer(initial_offer)) =
        initial_offer_response.message
    else {
        panic!("expected initial forwarding offer");
    };
    receiver
        .set_remote_offer(initial_offer.sdp)
        .await
        .expect("receiver should accept initial offer");
    let initial_answer = receiver
        .create_answer()
        .await
        .expect("receiver should create initial answer");
    signal_response_for_request(
        proto::SignalRequest {
            message: Some(proto::signal_request::Message::Answer(
                proto::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp: initial_answer,
                    id: initial_offer.id,
                    ..Default::default()
                },
            )),
        },
        &state,
        room,
        subscriber,
        &outbound_tx,
    )
    .await
    .expect("initial answer handler should succeed");
    assert!(
        !state
            .forward_tracks
            .list_for_track(room, publisher, audio_track_sid)
            .is_empty(),
        "initial audio forwarding should remain active"
    );
    assert!(
        !state
            .forward_tracks
            .list_for_track(room, publisher, vp8_track_sid)
            .is_empty(),
        "initial VP8 forwarding should remain active"
    );

    let h264_forward_track = forwarder
        .add_forwarding_track_with_mime(
            &publisher_info.sid,
            h264_track_sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
            Some("video/h264"),
        )
        .await
        .expect("H264 forwarding track should add");
    state.forward_tracks.insert_inactive(
        room,
        publisher,
        h264_track_sid,
        subscriber,
        h264_forward_track,
    );
    session::signal_media_forwarding_negotiation_with_offer_id(
        &state,
        &state.subscriber_offer_ids,
        room,
        subscriber,
        &forwarder,
        MediaForwardingConnectionKind::DualPcSubscriber,
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        &outbound_tx,
    )
    .await
    .expect("H264 forwarding offer should emit");
    let h264_offer_response = outbound_rx.recv().await.expect("H264 offer should arrive");
    let Some(proto::signal_response::Message::Offer(h264_offer)) = h264_offer_response.message
    else {
        panic!("expected H264 forwarding offer");
    };
    let h264_mid = state
        .subscriber_offer_mid_track_ids(room, subscriber, h264_offer.id)
        .into_iter()
        .find_map(|(mid, track_sid)| (track_sid == h264_track_sid).then_some(mid))
        .expect("H264 offer MID should correlate to the new H264 forwarding track");
    receiver
        .set_remote_offer(h264_offer.sdp)
        .await
        .expect("receiver should accept H264 offer");
    let h264_answer = receiver
        .create_answer()
        .await
        .expect("receiver should create H264 answer");
    let mut answer_lines = h264_answer
        .lines()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let h264_section_start = answer_lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.starts_with("m="))
        .map(|(index, _)| index)
        .find(|&start| {
            let end = answer_lines[start + 1..]
                .iter()
                .position(|line| line.starts_with("m="))
                .map_or(answer_lines.len(), |offset| start + 1 + offset);
            answer_lines[start..end]
                .iter()
                .any(|line| line == &format!("a=mid:{h264_mid}"))
        })
        .expect("answer should contain the H264 MID");
    let h264_section_end = answer_lines[h264_section_start + 1..]
        .iter()
        .position(|line| line.starts_with("m="))
        .map_or(answer_lines.len(), |offset| h264_section_start + 1 + offset);
    let mut direction_set = false;
    for line in &mut answer_lines[h264_section_start..h264_section_end] {
        if line == "a=sendrecv" || line == "a=sendonly" || line == "a=recvonly" {
            *line = "a=inactive".to_string();
            direction_set = true;
        }
    }
    if !direction_set {
        answer_lines.insert(h264_section_end, "a=inactive".to_string());
    }
    let vp8_only_h264_answer = answer_lines.join("\r\n");

    state
        .rooms
        .add_participant_track(
            room,
            publisher,
            proto::TrackInfo {
                sid: followup_vp8_track_sid.to_string(),
                r#type: proto::TrackType::Video as i32,
                mime_type: "video/vp8".to_string(),
                ..Default::default()
            },
        )
        .expect("follow-up VP8 track should add");
    state.media_subscriptions.set_subscribed(
        room,
        publisher,
        followup_vp8_track_sid,
        subscriber,
        true,
    );
    state
        .rooms
        .set_media_track_subscribed(room, publisher, followup_vp8_track_sid, subscriber, true)
        .expect("follow-up VP8 subscription should set");
    let followup_vp8_forward_track = forwarder
        .add_forwarding_track_with_mime(
            &publisher_info.sid,
            followup_vp8_track_sid,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
            Some("video/vp8"),
        )
        .await
        .expect("follow-up VP8 forwarding track should add");
    state.forward_tracks.insert_inactive(
        room,
        publisher,
        followup_vp8_track_sid,
        subscriber,
        followup_vp8_forward_track,
    );
    session::signal_media_forwarding_negotiation_with_offer_id(
        &state,
        &state.subscriber_offer_ids,
        room,
        subscriber,
        &forwarder,
        MediaForwardingConnectionKind::DualPcSubscriber,
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        &outbound_tx,
    )
    .await
    .expect("follow-up VP8 negotiation request should be accepted");
    assert!(
        outbound_rx.try_recv().is_err(),
        "a second server offer must be coalesced while the H264 offer is outstanding"
    );

    signal_response_for_request(
        proto::SignalRequest {
            message: Some(proto::signal_request::Message::Answer(
                proto::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp: format!("{vp8_only_h264_answer}\r\n"),
                    id: h264_offer.id,
                    ..Default::default()
                },
            )),
        },
        &state,
        room,
        subscriber,
        &outbound_tx,
    )
    .await
    .expect("H264 answer handler should succeed");

    let response = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let response = outbound_rx
                .recv()
                .await
                .expect("outbound signaling channel should remain open");
            match response.message {
                Some(proto::signal_response::Message::SubscriptionResponse(response)) => {
                    break response;
                }
                Some(proto::signal_response::Message::MediaSectionsRequirement(_)) => {}
                other => panic!("expected subscription response, got {other:?}"),
            }
        }
    })
    .await
    .expect("unsupported codec response should arrive before timeout");
    assert_eq!(response.track_sid, h264_track_sid);
    assert_eq!(
        response.err,
        proto::SubscriptionError::SeCodecUnsupported as i32
    );
    let corrective_offer_response =
        tokio::time::timeout(Duration::from_millis(250), outbound_rx.recv())
            .await
            .expect("removing the rejected H264 sender should emit a corrective offer")
            .expect("outbound signaling channel should remain open");
    let Some(proto::signal_response::Message::Offer(corrective_offer)) =
        corrective_offer_response.message
    else {
        panic!("expected corrective forwarding offer after H264 rejection");
    };
    assert!(
        !crate::media::mid_to_track_id_from_offer_sdp(&corrective_offer.sdp)
            .into_values()
            .any(|track_sid| track_sid == h264_track_sid),
        "the corrective offer must remove the rejected H264 sender"
    );
    assert!(
        crate::media::mid_to_track_id_from_offer_sdp(&corrective_offer.sdp)
            .into_values()
            .any(|track_sid| track_sid == followup_vp8_track_sid),
        "the coalesced offer must include the follow-up VP8 sender"
    );
    assert!(
        outbound_rx.try_recv().is_err(),
        "H264 cleanup should emit exactly one error and one corrective offer"
    );
    assert_eq!(
        state.media_subscriptions.explicit_subscription(
            room,
            publisher,
            h264_track_sid,
            subscriber
        ),
        Some(false),
        "the unsupported H264 track should be explicitly unsubscribed"
    );
    assert!(
        state
            .forward_tracks
            .list_for_track(room, publisher, h264_track_sid)
            .is_empty(),
        "only the unsupported H264 track should be removed"
    );
    assert!(
        !state
            .forward_tracks
            .list_for_track(room, publisher, vp8_track_sid)
            .is_empty(),
        "existing VP8 forwarding should remain active after H264 rejection"
    );

    forwarder.close().await.expect("forwarder should close");
    receiver.close().await.expect("receiver should close");
}

#[tokio::test]
async fn service_update_subscriptions_can_override_can_subscribe_false() {
    let state = state();
    let room = "media-subscribe-service-override-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, false);

    let response = add_track_response(
        &state,
        room,
        publisher,
        proto::AddTrackRequest {
            cid: "audio-cid-service-override".to_string(),
            name: "mic".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        },
    )
    .await;
    let Some(proto::signal_response::Message::TrackPublished(track_published)) = response.message
    else {
        panic!("expected TrackPublished response");
    };
    let track_sid = track_published.track.expect("track should exist").sid;

    state
        .apply_twirp_update_subscriptions(
            room,
            subscriber,
            std::slice::from_ref(&track_sid),
            &[],
            true,
        )
        .await;

    assert!(
        state
            .media_subscriptions
            .is_subscribed(room, publisher, &track_sid, subscriber),
        "service update-subscriptions should override can_subscribe=false policy"
    );
}

#[tokio::test]
async fn mute_track_request_marks_track_muted_and_notifies_other_participants() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let room_store = signal_state.rooms.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut alice_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    alice_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut alice_socket, _) = connect_async(alice_request)
        .await
        .expect("alice websocket should connect");
    let _alice_join = alice_socket
        .next()
        .await
        .expect("alice join should arrive")
        .expect("alice join should be ok");

    let mut bob_request = url.into_client_request().expect("request should build");
    bob_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token_for("test-room", "bob", "Bob")))
            .expect("auth header should parse"),
    );
    let (mut bob_socket, _) = connect_async(bob_request)
        .await
        .expect("bob websocket should connect");
    let _bob_join = bob_socket
        .next()
        .await
        .expect("bob join should arrive")
        .expect("bob join should be ok");
    let _alice_sees_bob = alice_socket
        .next()
        .await
        .expect("alice should receive bob join update")
        .expect("alice update should be ok");

    let add_track = proto::SignalRequest {
        message: Some(proto::signal_request::Message::AddTrack(
            proto::AddTrackRequest {
                cid: "audio-cid-mute".to_string(),
                name: "mic".to_string(),
                r#type: proto::TrackType::Audio as i32,
                source: proto::TrackSource::Microphone as i32,
                ..Default::default()
            },
        )),
    };
    alice_socket
        .send(Message::Binary(add_track.encode_to_vec().into()))
        .await
        .expect("add track should send");

    let track_published_message = alice_socket
        .next()
        .await
        .expect("track published response should arrive")
        .expect("track published message should be ok");
    let Message::Binary(track_published_bytes) = track_published_message else {
        panic!("expected binary track published response");
    };
    let track_published_response = proto::SignalResponse::decode(track_published_bytes.as_ref())
        .expect("track published response should decode");
    let Some(proto::signal_response::Message::TrackPublished(track_published)) =
        track_published_response.message
    else {
        panic!("expected track published response");
    };
    let track_sid = track_published.track.expect("track info should exist").sid;

    let _bob_sees_publication = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = bob_socket.next().await {
            let message = message.expect("bob message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response =
                proto::SignalResponse::decode(bytes.as_ref()).expect("bob response should decode");
            if let Some(proto::signal_response::Message::Update(update)) = response.message
                && update
                    .participants
                    .iter()
                    .any(|participant| participant.identity == "alice")
            {
                return;
            }
        }
        panic!("bob socket closed before publication update");
    })
    .await
    .expect("bob should see publication update");

    let mute = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Mute(
            proto::MuteTrackRequest {
                sid: track_sid.clone(),
                muted: true,
            },
        )),
    };
    alice_socket
        .send(Message::Binary(mute.encode_to_vec().into()))
        .await
        .expect("mute request should send");

    let muted_update = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = bob_socket.next().await {
            let message = message.expect("bob message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response =
                proto::SignalResponse::decode(bytes.as_ref()).expect("bob response should decode");
            if let Some(proto::signal_response::Message::Update(update)) = response.message {
                for participant in update.participants {
                    if participant.identity == "alice"
                        && participant
                            .tracks
                            .iter()
                            .any(|track| track.sid == track_sid && track.muted)
                    {
                        return;
                    }
                }
            }
        }
        panic!("bob socket closed before muted update");
    })
    .await;
    assert!(
        muted_update.is_ok(),
        "bob should observe muted track update"
    );

    let alice = room_store
        .get_participant("test-room", "alice")
        .expect("alice participant should exist after mute");
    assert!(
        alice
            .tracks
            .iter()
            .any(|track| track.sid == track_sid && track.muted),
        "room snapshot should mark track as muted"
    );

    server.abort();
}

#[test]
fn track_subscribed_signal_emission_requires_distinct_known_subscriber_identity() {
    let state = state();
    let room = "track-subscribed-signal-gate-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    assert!(session::should_emit_track_subscribed_for_subscriber(
        &state.rooms,
        room,
        publisher,
        subscriber,
    ));
    assert!(
        !session::should_emit_track_subscribed_for_subscriber(
            &state.rooms,
            room,
            publisher,
            publisher
        ),
        "publisher self-subscription should not emit TrackSubscribed"
    );
    assert!(
        !session::should_emit_track_subscribed_for_subscriber(
            &state.rooms,
            room,
            publisher,
            "missing",
        ),
        "unknown subscriber identity should not emit TrackSubscribed"
    );
}

#[test]
fn forward_track_reader_lease_is_owned_by_one_remote_track_instance() {
    let store = ForwardTrackStore::default();

    let first = store
        .acquire_track_reader("room", "publisher", "TR_a")
        .expect("first remote track should acquire its reader lease");
    assert!(
        store
            .acquire_track_reader("room", "publisher", "TR_a")
            .is_none(),
        "a concurrent remote track must not share the reader lease"
    );
    assert!(
        !store.release_track_reader("room", "publisher", "TR_a", first.wrapping_add(1)),
        "a stale remote track must not release the current reader lease"
    );
    assert!(store.owns_track_reader("room", "publisher", "TR_a", first));
    assert!(
        store.revoke_track_reader("room", "publisher", "TR_a"),
        "a runtime codec replacement must revoke the old reader lease"
    );
    assert!(
        !store.owns_track_reader("room", "publisher", "TR_a", first),
        "the revoked reader must observe that it no longer owns the track"
    );

    let replacement = store
        .acquire_track_reader("room", "publisher", "TR_a")
        .expect("a codec-replacement remote track should acquire immediately");
    assert_ne!(replacement, first);
    assert!(
        !store.release_track_reader("room", "publisher", "TR_a", first),
        "the old reader must not clear the replacement reader lease"
    );

    store.clear_track_readers_for_publisher("room", "publisher");
    assert!(
        store
            .acquire_track_reader("room", "publisher", "TR_a")
            .is_some(),
        "publisher teardown must release all reader leases"
    );
}

#[test]
fn publisher_session_fence_rejects_a_rejoined_identitys_old_session() {
    let state = state();
    let (_, first, _) = state
        .rooms
        .join_participant(
            "room",
            "publisher",
            "Publisher",
            String::new(),
            Default::default(),
        )
        .expect("publisher should join");

    assert!(session::publisher_session_is_current(
        &state.rooms,
        "room",
        "publisher",
        &first.sid,
    ));

    let (_, replacement, _) = state
        .rooms
        .join_participant(
            "room",
            "publisher",
            "Publisher",
            String::new(),
            Default::default(),
        )
        .expect("publisher should rejoin");

    assert_ne!(first.sid, replacement.sid);
    assert!(
        !session::publisher_session_is_current(&state.rooms, "room", "publisher", &first.sid),
        "a stale remote-track task must not resolve against the replacement publisher session"
    );
    assert!(session::publisher_session_is_current(
        &state.rooms,
        "room",
        "publisher",
        &replacement.sid,
    ));
}

#[tokio::test]
async fn inactive_forward_tracks_are_hidden_until_subscriber_answer_activates_them() {
    let store = ForwardTrackStore::default();
    let peer_connection = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("peer connection should create");
    let forward_track = peer_connection
        .add_forwarding_track(
            "PA_publisher",
            "TR_audio",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("forwarding track should add");

    store.insert_inactive("room", "publisher", "TR_audio", "subscriber", forward_track);

    assert!(
        store
            .list_for_track("room", "publisher", "TR_audio")
            .is_empty(),
        "inactive forward tracks must not receive RTP before subscriber answer"
    );

    store.activate_subscriber("room", "subscriber");

    assert_eq!(
        store.list_for_track("room", "publisher", "TR_audio").len(),
        1,
        "subscriber answer should activate negotiated forward tracks"
    );

    peer_connection
        .close()
        .await
        .expect("peer connection should close");
}

#[tokio::test]
async fn answer_activation_scopes_to_track_sids_declared_in_answer_sdp() {
    let state = state();
    let peer_connection = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("peer connection should create");

    let audio_forward_track = peer_connection
        .add_forwarding_track(
            "PA_publisher",
            "TR_audio",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("audio forward track should add");
    let video_forward_track = peer_connection
        .add_forwarding_track(
            "PA_publisher",
            "TR_video",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        )
        .await
        .expect("video forward track should add");

    state.forward_tracks.insert_inactive(
        "room",
        "publisher",
        "TR_audio",
        "subscriber",
        audio_forward_track,
    );
    state.forward_tracks.insert_inactive(
        "room",
        "publisher",
        "TR_video",
        "subscriber",
        video_forward_track,
    );

    let response = proto::SignalResponse {
        message: Some(proto::signal_response::Message::Answer(
            proto::SessionDescription {
                r#type: "answer".to_string(),
                sdp: "v=0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:0\r\n\
a=sendonly\r\n\
a=msid:PA_0000000000000001 TR_audio\r\n\
m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
a=mid:1\r\n\
a=recvonly\r\n"
                    .to_string(),
                ..Default::default()
            },
        )),
    };

    activate_forward_tracks_after_sent_response(&state, "room", "subscriber", &response);
    tokio::time::sleep(FORWARD_TRACK_ACTIVATION_AFTER_ANSWER_DELAY + Duration::from_millis(20))
        .await;

    assert_eq!(
        state
            .forward_tracks
            .list_for_track("room", "publisher", "TR_audio")
            .len(),
        1,
        "answer-declared track should activate"
    );
    assert!(
        state
            .forward_tracks
            .list_for_track("room", "publisher", "TR_video")
            .is_empty(),
        "track not declared in answer msid should remain inactive"
    );

    peer_connection
        .close()
        .await
        .expect("peer connection should close");
}

#[tokio::test]
async fn answer_activation_falls_back_to_forwarding_mids_when_answer_has_no_track_sid_mapping() {
    let state = state();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room".to_string(),
            ..Default::default()
        })
        .expect("room should create");
    state
        .rooms
        .join_participant(
            "room",
            "publisher",
            "Publisher",
            String::new(),
            HashMap::new(),
        )
        .expect("publisher should join");
    state
        .rooms
        .join_participant(
            "room",
            "subscriber",
            "Subscriber",
            String::new(),
            HashMap::new(),
        )
        .expect("subscriber should join");

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let peer_connection = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("peer connection should create");

    let offer_sdp = offerer
        .create_audio_offer()
        .await
        .expect("offer should create");
    peer_connection
        .set_remote_offer(offer_sdp)
        .await
        .expect("remote offer should set");

    let forward_track = peer_connection
        .add_forwarding_track_to_mid(
            "0",
            "PA_publisher",
            "TR_audio",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("forward track should add");

    state.forward_tracks.insert_inactive(
        "room",
        "publisher",
        "TR_audio",
        "subscriber",
        forward_track,
    );

    let response = proto::SignalResponse {
        message: Some(proto::signal_response::Message::Answer(
            proto::SessionDescription {
                r#type: "answer".to_string(),
                sdp: "v=0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:0\r\n\
a=sendonly\r\n"
                    .to_string(),
                ..Default::default()
            },
        )),
    };

    activate_forward_tracks_after_sent_response(&state, "room", "subscriber", &response);
    tokio::time::sleep(FORWARD_TRACK_ACTIVATION_AFTER_ANSWER_DELAY + Duration::from_millis(20))
        .await;

    assert_eq!(
        state
            .forward_tracks
            .list_for_track("room", "publisher", "TR_audio")
            .len(),
        1,
        "answer without track-sid mapping should activate track via accepted MID fallback"
    );

    peer_connection
        .close()
        .await
        .expect("peer connection should close");
    offerer
        .close()
        .await
        .expect("offerer peer connection should close");
}

#[tokio::test]
async fn single_pc_disconnect_rejoin_flow_reactivates_new_track_via_mid_fallback_without_stale_track()
 {
    let state = state();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room".to_string(),
            ..Default::default()
        })
        .expect("room should create");
    state
        .rooms
        .join_participant(
            "room",
            "publisher-c2",
            "Publisher C2",
            String::new(),
            HashMap::new(),
        )
        .expect("publisher should join");
    state
        .rooms
        .join_participant(
            "room",
            "subscriber-c3",
            "Subscriber C3",
            String::new(),
            HashMap::new(),
        )
        .expect("subscriber should join");

    state
        .rooms
        .add_participant_track(
            "room",
            "publisher-c2",
            proto::TrackInfo {
                sid: "TR_c2_old".to_string(),
                r#type: proto::TrackType::Audio as i32,
                ..Default::default()
            },
        )
        .expect("old track should be added");

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let subscriber_pc = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("subscriber peer connection should create");

    let offer_sdp = offerer
        .create_audio_offer()
        .await
        .expect("offer should create");
    subscriber_pc
        .set_remote_offer(offer_sdp)
        .await
        .expect("remote offer should set");
    let answer_sdp = subscriber_pc
        .create_answer()
        .await
        .expect("subscriber answer should create");
    offerer
        .set_remote_answer(answer_sdp)
        .await
        .expect("offerer remote answer should set");
    let subscriber_pc = state.peer_connections.insert(
        "room",
        "subscriber-c3",
        SignalConnectionTarget::Publisher,
        subscriber_pc,
    );
    let (subscriber_outbound_tx, mut subscriber_outbound_rx) =
        tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert("room", "subscriber-c3", subscriber_outbound_tx);

    let old_forward_track = subscriber_pc
        .add_forwarding_track_to_mid(
            "0",
            "PA_c2",
            "TR_c2_old",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("old forward track should add");

    state.forward_tracks.insert_inactive(
        "room",
        "publisher-c2",
        "TR_c2_old",
        "subscriber-c3",
        old_forward_track,
    );

    let response = proto::SignalResponse {
        message: Some(proto::signal_response::Message::Answer(
            proto::SessionDescription {
                r#type: "answer".to_string(),
                sdp: "v=0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:0\r\n\
a=sendonly\r\n"
                    .to_string(),
                ..Default::default()
            },
        )),
    };

    activate_forward_tracks_after_sent_response(&state, "room", "subscriber-c3", &response);
    tokio::time::sleep(FORWARD_TRACK_ACTIVATION_AFTER_ANSWER_DELAY + Duration::from_millis(20))
        .await;

    assert_eq!(
        state
            .forward_tracks
            .list_for_track("room", "publisher-c2", "TR_c2_old")
            .len(),
        1,
        "first publisher-c2 session track should be active after answer"
    );

    cleanup_participant_runtime_state(&state, "room", "publisher-c2", true).await;

    let cleanup_offer = subscriber_outbound_rx
        .try_recv()
        .expect("combined-PC cleanup should emit a server offer");
    assert!(matches!(
        cleanup_offer.message,
        Some(proto::signal_response::Message::Offer(_))
    ));

    assert!(
        state
            .forward_tracks
            .list_for_track("room", "publisher-c2", "TR_c2_old")
            .is_empty(),
        "cleanup should remove old publisher-c2 forward track"
    );

    state
        .rooms
        .join_participant(
            "room",
            "publisher-c2",
            "Publisher C2",
            String::new(),
            HashMap::new(),
        )
        .expect("publisher should rejoin");
    state
        .rooms
        .add_participant_track(
            "room",
            "publisher-c2",
            proto::TrackInfo {
                sid: "TR_c2_new".to_string(),
                r#type: proto::TrackType::Audio as i32,
                ..Default::default()
            },
        )
        .expect("new track should be added");

    let new_forward_track = subscriber_pc
        .add_forwarding_track_to_mid(
            "0",
            "PA_c2",
            "TR_c2_new",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("new forward track should add");

    state.forward_tracks.insert_inactive(
        "room",
        "publisher-c2",
        "TR_c2_new",
        "subscriber-c3",
        new_forward_track,
    );

    activate_forward_tracks_after_sent_response(&state, "room", "subscriber-c3", &response);
    tokio::time::sleep(FORWARD_TRACK_ACTIVATION_AFTER_ANSWER_DELAY + Duration::from_millis(20))
        .await;

    assert!(
        state
            .forward_tracks
            .list_for_track("room", "publisher-c2", "TR_c2_old")
            .is_empty(),
        "old publisher-c2 track must stay removed after reconnect"
    );
    assert_eq!(
        state
            .forward_tracks
            .list_for_track("room", "publisher-c2", "TR_c2_new")
            .len(),
        1,
        "new publisher-c2 session track should be active after reconnect"
    );

    subscriber_pc
        .close()
        .await
        .expect("subscriber peer connection should close");
    offerer
        .close()
        .await
        .expect("offerer peer connection should close");
}

#[tokio::test]
async fn livekit_multinode_publishing_upon_joining_contract_for_c3_track_counts() {
    let state = state();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room".to_string(),
            ..Default::default()
        })
        .expect("room should create");

    for (identity, name) in [("c1", "Client 1"), ("c2", "Client 2"), ("c3", "Client 3")] {
        state
            .rooms
            .join_participant("room", identity, name, String::new(), HashMap::new())
            .expect("participant should join");
    }

    for (identity, audio_sid, video_sid) in [
        ("c1", "TR_c1_audio", "TR_c1_video"),
        ("c2", "TR_c2_audio", "TR_c2_video"),
    ] {
        state
            .rooms
            .add_participant_track(
                "room",
                identity,
                proto::TrackInfo {
                    sid: audio_sid.to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    ..Default::default()
                },
            )
            .expect("audio track should be added");
        state
            .rooms
            .add_participant_track(
                "room",
                identity,
                proto::TrackInfo {
                    sid: video_sid.to_string(),
                    r#type: proto::TrackType::Video as i32,
                    ..Default::default()
                },
            )
            .expect("video track should be added");
    }

    let c3_pc = state.peer_connections.insert(
        "room",
        "c3",
        SignalConnectionTarget::Subscriber,
        oxidesfu_rtc::create_peer_connection()
            .await
            .expect("subscriber peer connection should create"),
    );
    let (c3_outbound_tx, mut c3_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .signal_connections
        .insert("room", "c3", c3_outbound_tx);

    for (publisher, track_sid, kind) in [
        (
            "c1",
            "TR_c1_audio",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        ),
        (
            "c1",
            "TR_c1_video",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        ),
        (
            "c2",
            "TR_c2_audio",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        ),
        (
            "c2",
            "TR_c2_video",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        ),
    ] {
        let forward_track = c3_pc
            .add_forwarding_track("PA_publisher", track_sid, kind)
            .await
            .expect("forward track should add");
        state
            .forward_tracks
            .insert("room", publisher, track_sid, "c3", forward_track);
    }

    let c3_tracks_from_c1_before = state
        .forward_tracks
        .list_for_track("room", "c1", "TR_c1_audio")
        .len()
        + state
            .forward_tracks
            .list_for_track("room", "c1", "TR_c1_video")
            .len();
    let c3_tracks_from_c2_before = state
        .forward_tracks
        .list_for_track("room", "c2", "TR_c2_audio")
        .len()
        + state
            .forward_tracks
            .list_for_track("room", "c2", "TR_c2_video")
            .len();
    assert_eq!(
        c3_tracks_from_c1_before, 2,
        "c3 should initially see 2 tracks from c1"
    );
    assert_eq!(
        c3_tracks_from_c2_before, 2,
        "c3 should initially see 2 tracks from c2"
    );

    cleanup_participant_runtime_state(&state, "room", "c2", true).await;

    let cleanup_offers = std::iter::from_fn(|| c3_outbound_rx.try_recv().ok())
        .filter_map(|response| match response.message {
            Some(proto::signal_response::Message::Offer(offer)) => Some(offer),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        cleanup_offers.len(),
        1,
        "c2 cleanup must issue one consolidated c3 renegotiation offer rather than racing intermediate offers"
    );
    assert!(
        !cleanup_offers[0].sdp.contains("TR_c2_audio")
            && !cleanup_offers[0].sdp.contains("TR_c2_video"),
        "the consolidated cleanup offer must not retain either c2 forwarding track"
    );

    let c3_tracks_from_c1_after_c2_leave = state
        .forward_tracks
        .list_for_track("room", "c1", "TR_c1_audio")
        .len()
        + state
            .forward_tracks
            .list_for_track("room", "c1", "TR_c1_video")
            .len();
    let c3_tracks_from_c2_after_leave = state
        .forward_tracks
        .list_for_track("room", "c2", "TR_c2_audio")
        .len()
        + state
            .forward_tracks
            .list_for_track("room", "c2", "TR_c2_video")
            .len();

    assert_eq!(
        c3_tracks_from_c2_after_leave, 0,
        "after c2 leaves, c3 should converge to 0 tracks from c2"
    );
    assert_eq!(
        c3_tracks_from_c1_after_c2_leave, 2,
        "c1 subscriptions should remain intact when c2 leaves"
    );

    state
        .rooms
        .join_participant("room", "c2", "Client 2", String::new(), HashMap::new())
        .expect("c2 should rejoin");
    state
        .rooms
        .add_participant_track(
            "room",
            "c2",
            proto::TrackInfo {
                sid: "TR_c2_audio_rejoin".to_string(),
                r#type: proto::TrackType::Audio as i32,
                ..Default::default()
            },
        )
        .expect("c2 rejoin audio track should be added");
    state
        .rooms
        .add_participant_track(
            "room",
            "c2",
            proto::TrackInfo {
                sid: "TR_c2_video_rejoin".to_string(),
                r#type: proto::TrackType::Video as i32,
                ..Default::default()
            },
        )
        .expect("c2 rejoin video track should be added");

    for (track_sid, kind) in [
        (
            "TR_c2_audio_rejoin",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        ),
        (
            "TR_c2_video_rejoin",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        ),
    ] {
        let forward_track = c3_pc
            .add_forwarding_track("PA_publisher", track_sid, kind)
            .await
            .expect("rejoin forward track should add");
        state
            .forward_tracks
            .insert("room", "c2", track_sid, "c3", forward_track);
    }

    let c3_tracks_from_c2_after_rejoin = state
        .forward_tracks
        .list_for_track("room", "c2", "TR_c2_audio_rejoin")
        .len()
        + state
            .forward_tracks
            .list_for_track("room", "c2", "TR_c2_video_rejoin")
            .len();

    assert_eq!(
        c3_tracks_from_c2_after_rejoin, 2,
        "after c2 rejoins and republishes, c3 should again see 2 tracks from c2"
    );

    c3_pc
        .close()
        .await
        .expect("subscriber peer connection should close");
}

#[tokio::test]
async fn participant_cleanup_prevents_delayed_answer_activation_from_resurrecting_forward_tracks() {
    let state = state();
    state
        .rooms
        .create_room(proto::CreateRoomRequest {
            name: "room".to_string(),
            ..Default::default()
        })
        .expect("room should create");
    state
        .rooms
        .join_participant(
            "room",
            "publisher",
            "Publisher",
            String::new(),
            HashMap::new(),
        )
        .expect("publisher should join");
    state
        .rooms
        .join_participant(
            "room",
            "subscriber",
            "Subscriber",
            String::new(),
            HashMap::new(),
        )
        .expect("subscriber should join");

    let peer_connection = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("peer connection should create");
    let forward_track = peer_connection
        .add_forwarding_track(
            "PA_publisher",
            "TR_audio",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("forward track should add");

    state.forward_tracks.insert_inactive(
        "room",
        "publisher",
        "TR_audio",
        "subscriber",
        forward_track,
    );

    cleanup_participant_runtime_state(&state, "room", "subscriber", true).await;

    let response = proto::SignalResponse {
        message: Some(proto::signal_response::Message::Answer(
            proto::SessionDescription {
                r#type: "answer".to_string(),
                sdp: "v=0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:0\r\n\
a=sendonly\r\n\
a=msid:PA_0000000000000001 TR_audio\r\n"
                    .to_string(),
                ..Default::default()
            },
        )),
    };

    activate_forward_tracks_after_sent_response(&state, "room", "subscriber", &response);
    tokio::time::sleep(FORWARD_TRACK_ACTIVATION_AFTER_ANSWER_DELAY + Duration::from_millis(20))
        .await;

    assert!(
        state
            .forward_tracks
            .list_for_track("room", "publisher", "TR_audio")
            .is_empty(),
        "cleanup should remove subscriber-scoped forward tracks before delayed activation"
    );

    peer_connection
        .close()
        .await
        .expect("peer connection should close");
}

#[tokio::test]
async fn forward_track_store_remove_subscriber_mid_reclaims_only_matching_mid_tracks() {
    let store = ForwardTrackStore::default();
    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let answerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("answerer peer connection should create");

    let offer_sdp = offerer
        .create_audio_offer()
        .await
        .expect("audio offer should create");
    answerer
        .set_remote_offer(offer_sdp)
        .await
        .expect("remote offer should set");

    let mid_bound_track = answerer
        .add_forwarding_track_to_mid(
            "0",
            "PA_publisher_a",
            "TR_audio_a",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("forwarding track should bind to mid 0");
    let non_mid_bound_track = answerer
        .add_forwarding_track(
            "PA_publisher_b",
            "TR_audio_b",
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
        )
        .await
        .expect("forwarding track without explicit mid should add");

    store.insert(
        "room",
        "publisher-a",
        "TR_audio_a",
        "subscriber",
        mid_bound_track,
    );
    store.insert(
        "room",
        "publisher-b",
        "TR_audio_b",
        "subscriber",
        non_mid_bound_track,
    );

    let removed = store.remove_subscriber_mid("room", "subscriber", "0");

    assert_eq!(
        removed.len(),
        1,
        "only one mid-bound track should be reclaimed"
    );
    assert_eq!(removed[0].0, "publisher-a");
    assert_eq!(removed[0].1, "TR_audio_a");
    assert!(
        store
            .list_for_track("room", "publisher-a", "TR_audio_a")
            .is_empty(),
        "reclaimed mid-bound track should no longer be active"
    );
    assert_eq!(
        store
            .list_for_track("room", "publisher-b", "TR_audio_b")
            .len(),
        1,
        "non-mid-bound track should remain"
    );

    offerer.close().await.expect("offerer should close");
    answerer.close().await.expect("answerer should close");
}

fn rtp_packet(sequence_number: u16, payload_byte: u8) -> rtc::rtp::Packet {
    rtc::rtp::Packet {
        header: rtc::rtp::header::Header {
            sequence_number,
            ..Default::default()
        },
        payload: vec![payload_byte].into(),
    }
}

#[test]
fn rtp_forwarding_slice1_sequence_continuity_gap_out_of_order_and_duplicate_drop() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_audio".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let first = store
        .rewrite_packet_for_subscriber(&key, rtp_packet(100, 1))
        .expect("first packet should forward");
    assert_eq!(first.header.sequence_number, 100);

    let gap = store
        .rewrite_packet_for_subscriber(&key, rtp_packet(103, 2))
        .expect("gap packet should forward");
    assert_eq!(
        gap.header.sequence_number, 101,
        "gap should be rewritten contiguously"
    );

    let out_of_order = store
        .rewrite_packet_for_subscriber(&key, rtp_packet(102, 3))
        .expect("out-of-order packet should still forward once");
    assert_eq!(
        out_of_order.header.sequence_number, 102,
        "out-of-order unique packet should keep continuity"
    );

    let duplicate = store.rewrite_packet_for_subscriber(&key, rtp_packet(103, 9));
    assert!(duplicate.is_none(), "duplicate incoming packet should drop");
}

#[test]
fn rtp_forwarding_slice2_retransmission_cache_returns_recent_packets() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_audio".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let first = store
        .rewrite_packet_for_subscriber(&key, rtp_packet(4000, 11))
        .expect("first packet should forward");
    let second = store
        .rewrite_packet_for_subscriber(&key, rtp_packet(4001, 12))
        .expect("second packet should forward");

    let retransmit_first = store
        .get_retransmission_packet(&key, first.header.sequence_number)
        .expect("recent first packet should be present in retransmission cache");
    assert_eq!(retransmit_first.payload, vec![11]);

    let retransmit_second = store
        .get_retransmission_packet(&key, second.header.sequence_number)
        .expect("recent second packet should be present in retransmission cache");
    assert_eq!(retransmit_second.payload, vec![12]);
}

#[test]
fn rtp_forwarding_rewrites_audio_to_the_destination_negotiated_payload_type() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_audio".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();
    let mut incoming = rtp_packet(5100, 9);
    incoming.header.payload_type = 109;

    let rewritten = store
        .rewrite_packet_for_subscriber_with_target_ssrc_and_payload_type(
            &key,
            incoming,
            None,
            Some(111),
        )
        .expect("packet should forward");
    assert_eq!(rewritten.header.payload_type, 111);

    let retransmission = store
        .get_retransmission_packet(&key, rewritten.header.sequence_number)
        .expect("rewritten packet should be cached for retransmission");
    assert_eq!(retransmission.header.payload_type, 111);
}

#[test]
fn rtp_forwarding_slice4_keyframe_request_gate_throttles_pli_and_fir_independently() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    assert!(store.should_forward_keyframe_request(&key, KeyFrameRequestKind::Pli, 1_000, 300));
    assert!(
        !store.should_forward_keyframe_request(&key, KeyFrameRequestKind::Pli, 1_200, 300),
        "second PLI inside gate should be dropped"
    );
    assert!(
        store.should_forward_keyframe_request(&key, KeyFrameRequestKind::Pli, 1_350, 300),
        "PLI should reopen once gate expires"
    );

    assert!(store.should_forward_keyframe_request(&key, KeyFrameRequestKind::Fir, 2_000, 300));
    assert!(
        !store.should_forward_keyframe_request(&key, KeyFrameRequestKind::Fir, 2_150, 300),
        "FIR gate should be independent and throttle separately"
    );
}

#[test]
fn rtp_forwarding_slice5_sender_report_mapping_preserves_timestamp_continuity_and_target_ssrc() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let first = store.map_sender_report(&key, 0x1122_3344, 90_000);
    assert_eq!(first.ssrc, 0x1122_3344);
    assert_eq!(first.rtp_timestamp, 90_000);

    let second = store.map_sender_report(&key, 0x1122_3344, 90_900);
    assert_eq!(second.ssrc, 0x1122_3344);
    assert_eq!(second.rtp_timestamp, 90_900);

    let switched_ssrc = store.map_sender_report(&key, 0x5566_7788, 12_345);
    assert_eq!(switched_ssrc.ssrc, 0x5566_7788);
    assert_eq!(
        switched_ssrc.rtp_timestamp, 12_345,
        "new target SSRC should rebase sender-report timestamp mapping"
    );
}

#[test]
fn rtp_forwarding_slice6_per_subscriber_state_isolation() {
    let key_a = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber-a".to_string(),
    );
    let key_b = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber-b".to_string(),
    );
    let store = RtpForwardingStore::default();

    let a_first = store
        .rewrite_packet_for_subscriber(&key_a, rtp_packet(10, 1))
        .expect("first packet for subscriber A should forward");
    let b_first = store
        .rewrite_packet_for_subscriber(&key_b, rtp_packet(10, 2))
        .expect("first packet for subscriber B should forward");
    assert_eq!(a_first.header.sequence_number, 10);
    assert_eq!(b_first.header.sequence_number, 10);

    let _a_second = store
        .rewrite_packet_for_subscriber(&key_a, rtp_packet(11, 3))
        .expect("second packet for subscriber A should forward");

    let b_duplicate_first = store.rewrite_packet_for_subscriber(&key_b, rtp_packet(10, 9));
    assert!(
        b_duplicate_first.is_none(),
        "subscriber B duplicate suppression should be isolated to B state"
    );

    let b_second = store
        .rewrite_packet_for_subscriber(&key_b, rtp_packet(12, 4))
        .expect("subscriber B unique packet should still forward independently");
    assert_eq!(
        b_second.header.sequence_number, 11,
        "subscriber B continuity should not be affected by subscriber A progression"
    );
}

#[test]
fn rtcp_action_derivation_classifies_nack_pli_fir_sr_rr_and_twcc() {
    let packets: Vec<Box<dyn rtc::rtcp::Packet>> = vec![
        Box::new(
            rtc::rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack {
                sender_ssrc: 1,
                media_ssrc: 2,
                nacks: vec![
                    rtc::rtcp::transport_feedbacks::transport_layer_nack::NackPair {
                        packet_id: 500,
                        lost_packets: 0b11,
                    },
                ],
            },
        ),
        Box::new(
            rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                sender_ssrc: 3,
                media_ssrc: 4,
            },
        ),
        Box::new(
            rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest {
                sender_ssrc: 5,
                media_ssrc: 6,
                fir: Vec::new(),
            },
        ),
        Box::new(rtc::rtcp::sender_report::SenderReport {
            ssrc: 0x1111_2222,
            rtp_time: 90_000,
            ..Default::default()
        }),
        Box::new(rtc::rtcp::receiver_report::ReceiverReport {
            ssrc: 0x2222_3333,
            ..Default::default()
        }),
        Box::new(
            rtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc {
                media_ssrc: 0x4444_5555,
                packet_status_count: 7,
                ..Default::default()
            },
        ),
    ];

    let actions = derive_rtcp_forward_actions(&packets);

    assert!(
        actions.contains(&RtcpForwardAction::RetransmitSequence(500)),
        "nack packet id should map to retransmission action"
    );
    assert!(
        actions.contains(&RtcpForwardAction::RetransmitSequence(501)),
        "nack bit 0 should map to retransmission action"
    );
    assert!(
        actions.contains(&RtcpForwardAction::RetransmitSequence(502)),
        "nack bit 1 should map to retransmission action"
    );
    assert!(actions.contains(&RtcpForwardAction::KeyFrameRequest {
        kind: KeyFrameRequestKind::Pli,
        media_ssrc: 4,
    }));
    assert!(actions.contains(&RtcpForwardAction::KeyFrameRequest {
        kind: KeyFrameRequestKind::Fir,
        media_ssrc: 6,
    }));
    assert!(actions.iter().any(|action| matches!(
        action,
        RtcpForwardAction::SenderReport { report }
            if report.ssrc == 0x1111_2222 && report.rtp_time == 90_000
    )));
    assert!(actions.iter().any(|action| matches!(
        action,
        RtcpForwardAction::ReceiverReportObserved {
            ssrc: 0x2222_3333,
            ..
        }
    )));
    assert!(
        actions.contains(&RtcpForwardAction::TransportWideCcObserved {
            media_ssrc: 0x4444_5555,
            packet_status_count: 7,
        })
    );
}

struct SpyRtpSink {
    packets: Arc<Mutex<Vec<rtc::rtp::Packet>>>,
}

impl RtpRetransmitSink for SpyRtpSink {
    fn send_rtp<'a>(
        &'a self,
        packet: rtc::rtp::Packet,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Ok(mut packets) = self.packets.lock() {
                packets.push(packet);
            }
        })
    }
}

struct SpyRtcpFeedbackSink {
    feedback_packet_kinds: Arc<Mutex<Vec<String>>>,
}

impl RtcpFeedbackSink for SpyRtcpFeedbackSink {
    fn send_feedback_rtcp<'a>(
        &'a self,
        packet: Box<dyn rtc::rtcp::Packet>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let kind = if packet
                    .as_any()
                    .downcast_ref::<
                        rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
                    >()
                    .is_some()
                {
                    "pli".to_string()
                } else if packet
                    .as_any()
                    .downcast_ref::<
                        rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest,
                    >()
                    .is_some()
                {
                    "fir".to_string()
                } else {
                    "other".to_string()
                };
            if let Ok(mut kinds) = self.feedback_packet_kinds.lock() {
                kinds.push(kind);
            }
        })
    }
}

struct SpySenderReportSink {
    reports: Arc<Mutex<Vec<rtc::rtcp::sender_report::SenderReport>>>,
}

struct FailingFeedbackSink {
    failures: Arc<Mutex<usize>>,
}

impl RtcpFeedbackSink for FailingFeedbackSink {
    fn send_feedback_rtcp<'a>(
        &'a self,
        _packet: Box<dyn rtc::rtcp::Packet>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Ok(mut failures) = self.failures.lock() {
                *failures += 1;
            }
            // Simulates internal transport failure handling: swallow and return.
        })
    }
}

impl SenderReportSink for SpySenderReportSink {
    fn send_sender_report_rtcp<'a>(
        &'a self,
        packet: Box<dyn rtc::rtcp::Packet>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Some(report) = packet
                .as_any()
                .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
                .cloned()
                && let Ok(mut reports) = self.reports.lock()
            {
                reports.push(report);
            }
        })
    }
}

#[tokio::test]
async fn execute_rtcp_outbound_effects_mixed_burst_dispatches_nack_keyframe_and_sender_report() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let rewritten = store
        .rewrite_packet_for_subscriber(&key, rtp_packet(6000, 0xcc))
        .expect("packet should be rewritten and cached");

    let packets: Vec<Box<dyn rtc::rtcp::Packet>> = vec![
        Box::new(
            rtc::rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack {
                sender_ssrc: 1,
                media_ssrc: 2,
                nacks: vec![
                    rtc::rtcp::transport_feedbacks::transport_layer_nack::NackPair {
                        packet_id: rewritten.header.sequence_number,
                        lost_packets: 0,
                    },
                ],
            },
        ),
        Box::new(
            rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                sender_ssrc: 3,
                media_ssrc: 4,
            },
        ),
        Box::new(
            rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest {
                sender_ssrc: 5,
                media_ssrc: 4,
                fir: Vec::new(),
            },
        ),
        Box::new(rtc::rtcp::sender_report::SenderReport {
            ssrc: 0xabcdef,
            rtp_time: 91_000,
            ..Default::default()
        }),
    ];
    let actions = derive_rtcp_forward_actions(&packets);
    let effects = build_rtcp_outbound_effects(&key, &actions, &store, 95_000);

    let rtp_packets = Arc::new(Mutex::new(Vec::new()));
    let feedback_packet_kinds = Arc::new(Mutex::new(Vec::new()));
    let sender_reports = Arc::new(Mutex::new(Vec::new()));
    let rtp_sink = SpyRtpSink {
        packets: rtp_packets.clone(),
    };
    let feedback_sink = SpyRtcpFeedbackSink {
        feedback_packet_kinds: feedback_packet_kinds.clone(),
    };
    let sender_report_sink = SpySenderReportSink {
        reports: sender_reports.clone(),
    };

    execute_rtcp_outbound_effects(effects, &rtp_sink, &feedback_sink, &sender_report_sink).await;

    let sent_rtp = rtp_packets
        .lock()
        .expect("rtp packets lock should not be poisoned");
    assert_eq!(sent_rtp.len(), 1);
    assert_eq!(sent_rtp[0].payload, vec![0xcc]);

    let feedback_kinds = feedback_packet_kinds
        .lock()
        .expect("feedback kinds lock should not be poisoned");
    assert_eq!(feedback_kinds.len(), 2);
    assert!(feedback_kinds.iter().any(|kind| kind == "pli"));
    assert!(feedback_kinds.iter().any(|kind| kind == "fir"));

    let sent_reports = sender_reports
        .lock()
        .expect("sender reports lock should not be poisoned");
    assert_eq!(sent_reports.len(), 1);
    assert_eq!(sent_reports[0].ssrc, 0xabcdef);
    assert_eq!(sent_reports[0].rtp_time, 91_000);
}

#[test]
fn rtcp_execution_plan_rr_and_twcc_observed_have_no_outbound_side_effects() {
    let key = (
        "room-rr-twcc".to_string(),
        "publisher".to_string(),
        "TR_media".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let actions = vec![
        RtcpForwardAction::ReceiverReportObserved {
            ssrc: 0x2222_3333,
            max_fraction_lost: 88,
            report_count: 2,
        },
        RtcpForwardAction::TransportWideCcObserved {
            media_ssrc: 0x4444_5555,
            packet_status_count: 7,
        },
    ];

    let plan = build_rtcp_execution_plan(&key, &actions, &store, 123_456);
    assert!(
        plan.retransmit_packets.is_empty(),
        "RR/TWCC observation should not produce retransmit packets"
    );
    assert!(
        plan.keyframe_requests.is_empty(),
        "RR/TWCC observation should not produce keyframe requests"
    );
    assert!(
        plan.rewritten_sender_reports.is_empty(),
        "RR/TWCC observation should not produce sender reports"
    );
    assert_eq!(plan.media_feedback.last_rr_ssrc, Some(0x2222_3333));
    assert_eq!(plan.media_feedback.rr_report_count, 2);
    assert_eq!(plan.media_feedback.rr_max_fraction_lost, 88);
    assert_eq!(plan.media_feedback.last_twcc_media_ssrc, Some(0x4444_5555));
    assert_eq!(plan.media_feedback.twcc_packet_status_count, 7);
    assert!(plan.media_feedback.is_degraded);

    let effects = build_rtcp_outbound_effects(&key, &actions, &store, 123_456);
    assert!(effects.retransmit_packets.is_empty());
    assert!(effects.feedback_packets.is_empty());
    assert!(effects.sender_report_packets.is_empty());
}

#[tokio::test]
async fn execute_rtcp_outbound_effects_continues_after_feedback_sink_internal_failure() {
    let effects = RtcpOutboundEffects {
        retransmit_packets: vec![rtp_packet(778, 0xdd)],
        feedback_packets: vec![Box::new(
            rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                sender_ssrc: 0,
                media_ssrc: 66,
            },
        )],
        sender_report_packets: vec![Box::new(rtc::rtcp::sender_report::SenderReport {
            ssrc: 0x4321,
            rtp_time: 99_000,
            ..Default::default()
        })],
        recommended_video_quality: None,
    };

    let rtp_packets = Arc::new(Mutex::new(Vec::new()));
    let feedback_failures = Arc::new(Mutex::new(0usize));
    let sender_reports = Arc::new(Mutex::new(Vec::new()));
    let rtp_sink = SpyRtpSink {
        packets: rtp_packets.clone(),
    };
    let feedback_sink = FailingFeedbackSink {
        failures: feedback_failures.clone(),
    };
    let sender_report_sink = SpySenderReportSink {
        reports: sender_reports.clone(),
    };

    execute_rtcp_outbound_effects(effects, &rtp_sink, &feedback_sink, &sender_report_sink).await;

    let sent_rtp = rtp_packets
        .lock()
        .expect("rtp packets lock should not be poisoned");
    assert_eq!(sent_rtp.len(), 1);
    assert_eq!(sent_rtp[0].payload, vec![0xdd]);

    let failures = feedback_failures
        .lock()
        .expect("feedback failures lock should not be poisoned");
    assert_eq!(*failures, 1);

    let sent_reports = sender_reports
        .lock()
        .expect("sender reports lock should not be poisoned");
    assert_eq!(sent_reports.len(), 1);
    assert_eq!(sent_reports[0].ssrc, 0x4321);
}

#[tokio::test]
async fn execute_rtcp_outbound_effects_dispatches_packets_to_expected_sinks() {
    let effects = RtcpOutboundEffects {
        retransmit_packets: vec![rtp_packet(777, 0xaa)],
        feedback_packets: vec![Box::new(
            rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                sender_ssrc: 0,
                media_ssrc: 55,
            },
        )],
        sender_report_packets: vec![Box::new(rtc::rtcp::sender_report::SenderReport {
            ssrc: 0x1234,
            rtp_time: 88_000,
            ..Default::default()
        })],
        recommended_video_quality: None,
    };

    let rtp_packets = Arc::new(Mutex::new(Vec::new()));
    let feedback_packet_kinds = Arc::new(Mutex::new(Vec::new()));
    let sender_reports = Arc::new(Mutex::new(Vec::new()));
    let rtp_sink = SpyRtpSink {
        packets: rtp_packets.clone(),
    };
    let feedback_sink = SpyRtcpFeedbackSink {
        feedback_packet_kinds: feedback_packet_kinds.clone(),
    };
    let sender_report_sink = SpySenderReportSink {
        reports: sender_reports.clone(),
    };

    execute_rtcp_outbound_effects(effects, &rtp_sink, &feedback_sink, &sender_report_sink).await;

    let sent_rtp = rtp_packets
        .lock()
        .expect("rtp packets lock should not be poisoned");
    assert_eq!(sent_rtp.len(), 1);
    assert_eq!(sent_rtp[0].payload, vec![0xaa]);

    let feedback_kinds = feedback_packet_kinds
        .lock()
        .expect("feedback kinds lock should not be poisoned");
    assert_eq!(feedback_kinds.as_slice(), ["pli"]);

    let sent_reports = sender_reports
        .lock()
        .expect("sender reports lock should not be poisoned");
    assert_eq!(sent_reports.len(), 1);
    assert_eq!(sent_reports[0].ssrc, 0x1234);
    assert_eq!(sent_reports[0].rtp_time, 88_000);
}

#[test]
fn rtcp_execution_plan_nack_burst_retransmits_only_cached_packets_with_bounded_cache() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let mut first_outgoing_sequence = None;
    let mut last_outgoing_sequence = None;
    for i in 0..(RTP_RETRANSMISSION_CACHE_SIZE + 4) {
        let rewritten = store
            .rewrite_packet_for_subscriber(&key, rtp_packet(4000 + i as u16, (i % 255) as u8))
            .expect("packet should be rewritten and cached");
        if i == 0 {
            first_outgoing_sequence = Some(rewritten.header.sequence_number);
        }
        if i == RTP_RETRANSMISSION_CACHE_SIZE + 3 {
            last_outgoing_sequence = Some(rewritten.header.sequence_number);
        }
    }

    let oldest = first_outgoing_sequence.expect("oldest sequence should exist");
    let newest = last_outgoing_sequence.expect("newest sequence should exist");
    let missing = newest.wrapping_add(1);

    let plan = build_rtcp_execution_plan(
        &key,
        &[
            RtcpForwardAction::RetransmitSequence(oldest),
            RtcpForwardAction::RetransmitSequence(newest),
            RtcpForwardAction::RetransmitSequence(missing),
        ],
        &store,
        12_000,
    );

    assert_eq!(
        plan.retransmit_packets.len(),
        1,
        "only newest sequence should still be in bounded retransmission cache"
    );
    assert_eq!(plan.retransmit_packets[0].header.sequence_number, newest);
}

#[test]
fn rtcp_execution_plan_retransmits_cached_packets_for_nack_actions() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let rewritten = store
        .rewrite_packet_for_subscriber(&key, rtp_packet(2000, 0x5a))
        .expect("packet should be rewritten and cached");

    let plan = build_rtcp_execution_plan(
        &key,
        &[RtcpForwardAction::RetransmitSequence(
            rewritten.header.sequence_number,
        )],
        &store,
        10_000,
    );

    assert_eq!(plan.retransmit_packets.len(), 1);
    assert_eq!(plan.retransmit_packets[0].payload, vec![0x5a]);
}

#[test]
fn rtcp_outbound_effects_include_retransmit_and_feedback_packets() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let rewritten = store
        .rewrite_packet_for_subscriber(&key, rtp_packet(3000, 0x7b))
        .expect("packet should be rewritten and cached");

    let actions = [
        RtcpForwardAction::RetransmitSequence(rewritten.header.sequence_number),
        RtcpForwardAction::KeyFrameRequest {
            kind: KeyFrameRequestKind::Pli,
            media_ssrc: 0x4455_6677,
        },
    ];

    let effects = build_rtcp_outbound_effects(&key, &actions, &store, 40_000);

    assert_eq!(effects.retransmit_packets.len(), 1);
    assert_eq!(effects.retransmit_packets[0].payload, vec![0x7b]);
    assert_eq!(effects.feedback_packets.len(), 1);

    let pli = effects.feedback_packets[0]
            .as_any()
            .downcast_ref::<
                rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
            >()
            .expect("feedback packet should be PLI");
    assert_eq!(pli.media_ssrc, 0x4455_6677);
    assert_eq!(pli.sender_ssrc, 0);
}

#[test]
fn rtcp_outbound_effects_isolate_state_across_subscribers() {
    let key_a = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber-a".to_string(),
    );
    let key_b = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber-b".to_string(),
    );
    let store = RtpForwardingStore::default();

    let seq_a = store
        .rewrite_packet_for_subscriber(&key_a, rtp_packet(5000, 0xa1))
        .expect("subscriber-a packet should be cached")
        .header
        .sequence_number;
    let seq_b = store
        .rewrite_packet_for_subscriber(&key_b, rtp_packet(5001, 0xb1))
        .expect("subscriber-b packet should be cached")
        .header
        .sequence_number;

    let actions_a = [
        RtcpForwardAction::RetransmitSequence(seq_a),
        RtcpForwardAction::KeyFrameRequest {
            kind: KeyFrameRequestKind::Fir,
            media_ssrc: 0x1001,
        },
        RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0xaa,
                rtp_time: 90_000,
                ..Default::default()
            },
        },
    ];
    let actions_b = [
        RtcpForwardAction::RetransmitSequence(seq_b),
        RtcpForwardAction::KeyFrameRequest {
            kind: KeyFrameRequestKind::Fir,
            media_ssrc: 0x1001,
        },
        RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0xbb,
                rtp_time: 120_000,
                ..Default::default()
            },
        },
    ];

    let effects_a_first = build_rtcp_outbound_effects(&key_a, &actions_a, &store, 60_000);
    let effects_b_first = build_rtcp_outbound_effects(&key_b, &actions_b, &store, 60_000);
    let effects_a_second = build_rtcp_outbound_effects(&key_a, &actions_a, &store, 60_400);

    assert_eq!(effects_a_first.retransmit_packets.len(), 1);
    assert_eq!(effects_a_first.retransmit_packets[0].payload, vec![0xa1]);
    assert_eq!(effects_b_first.retransmit_packets.len(), 1);
    assert_eq!(effects_b_first.retransmit_packets[0].payload, vec![0xb1]);

    let fir_a_first = effects_a_first.feedback_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest>()
        .expect("subscriber-a feedback should be FIR");
    let fir_b_first = effects_b_first.feedback_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest>()
        .expect("subscriber-b feedback should be FIR");
    let fir_a_second = effects_a_second.feedback_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest>()
        .expect("subscriber-a second feedback should be FIR");

    assert_eq!(fir_a_first.fir[0].sequence_number, 0);
    assert_eq!(fir_b_first.fir[0].sequence_number, 0);
    assert_eq!(fir_a_second.fir[0].sequence_number, 1);

    let sr_a = effects_a_first.sender_report_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        .expect("subscriber-a sender report should exist");
    let sr_b = effects_b_first.sender_report_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        .expect("subscriber-b sender report should exist");
    assert_eq!(sr_a.ssrc, 0xaa);
    assert_eq!(sr_a.rtp_time, 90_000);
    assert_eq!(sr_b.ssrc, 0xbb);
    assert_eq!(sr_b.rtp_time, 120_000);
}

#[test]
fn rtcp_outbound_effects_sender_report_rewrite_handles_timestamp_wrap_continuity() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let first = build_rtcp_outbound_effects(
        &key,
        &[RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0x5555,
                rtp_time: 0xffff_ff00,
                ..Default::default()
            },
        }],
        &store,
        70_000,
    );
    let second = build_rtcp_outbound_effects(
        &key,
        &[RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0x5555,
                rtp_time: 0x0000_0010,
                ..Default::default()
            },
        }],
        &store,
        70_400,
    );

    let first_sr = first.sender_report_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        .expect("first sender report should be present");
    let second_sr = second.sender_report_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        .expect("second sender report should be present");

    assert_eq!(first_sr.ssrc, 0x5555);
    assert_eq!(first_sr.rtp_time, 0xffff_ff00);
    assert_eq!(second_sr.ssrc, 0x5555);
    assert_eq!(
        second_sr.rtp_time, 0x0000_0010,
        "rtp timestamp wrap should remain coherent across rewrite output"
    );
}

#[test]
fn rtcp_outbound_effects_sender_report_resets_mapping_on_ssrc_switch() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let a1 = build_rtcp_outbound_effects(
        &key,
        &[RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0x1010,
                rtp_time: 90_000,
                ..Default::default()
            },
        }],
        &store,
        80_000,
    );
    let b = build_rtcp_outbound_effects(
        &key,
        &[RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0x2020,
                rtp_time: 10_000,
                ..Default::default()
            },
        }],
        &store,
        80_400,
    );
    let a2 = build_rtcp_outbound_effects(
        &key,
        &[RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0x1010,
                rtp_time: 91_000,
                ..Default::default()
            },
        }],
        &store,
        80_800,
    );

    let a1_sr = a1.sender_report_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        .expect("first A sender report should exist");
    let b_sr = b.sender_report_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        .expect("B sender report should exist");
    let a2_sr = a2.sender_report_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        .expect("second A sender report should exist");

    assert_eq!(a1_sr.ssrc, 0x1010);
    assert_eq!(a1_sr.rtp_time, 90_000);
    assert_eq!(b_sr.ssrc, 0x2020);
    assert_eq!(b_sr.rtp_time, 10_000);
    assert_eq!(a2_sr.ssrc, 0x1010);
    assert_eq!(a2_sr.rtp_time, 91_000);
}

#[test]
fn rtcp_outbound_effects_include_rewritten_sender_report_packet() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();
    let actions = [RtcpForwardAction::SenderReport {
        report: rtc::rtcp::sender_report::SenderReport {
            ssrc: 0x7777,
            rtp_time: 123_456,
            packet_count: 12,
            octet_count: 34,
            ..Default::default()
        },
    }];

    let effects = build_rtcp_outbound_effects(&key, &actions, &store, 41_000);
    assert_eq!(effects.sender_report_packets.len(), 1);
    let rewritten = effects.sender_report_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        .expect("sender report packet should be present");
    assert_eq!(rewritten.ssrc, 0x7777);
    assert_eq!(rewritten.rtp_time, 123_456);
    assert_eq!(rewritten.packet_count, 12);
    assert_eq!(rewritten.octet_count, 34);
}

#[test]
fn rtcp_outbound_effects_assign_incrementing_fir_feedback_sequence_numbers() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let actions = [RtcpForwardAction::KeyFrameRequest {
        kind: KeyFrameRequestKind::Fir,
        media_ssrc: 0x9999,
    }];

    let first = build_rtcp_outbound_effects(&key, &actions, &store, 50_000);
    assert_eq!(first.feedback_packets.len(), 1);
    let first_fir = first.feedback_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest>()
        .expect("feedback packet should be FIR");
    assert_eq!(first_fir.fir.len(), 1);
    assert_eq!(first_fir.fir[0].sequence_number, 0);

    let second = build_rtcp_outbound_effects(&key, &actions, &store, 50_400);
    assert_eq!(second.feedback_packets.len(), 1);
    let second_fir = second.feedback_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest>()
        .expect("feedback packet should be FIR");
    assert_eq!(second_fir.fir.len(), 1);
    assert_eq!(second_fir.fir[0].sequence_number, 1);

    let other_ssrc_actions = [RtcpForwardAction::KeyFrameRequest {
        kind: KeyFrameRequestKind::Fir,
        media_ssrc: 0xaaaa,
    }];
    let other_ssrc = build_rtcp_outbound_effects(&key, &other_ssrc_actions, &store, 50_800);
    assert_eq!(other_ssrc.feedback_packets.len(), 1);
    let other_fir = other_ssrc.feedback_packets[0]
        .as_any()
        .downcast_ref::<rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest>()
        .expect("feedback packet should be FIR");
    assert_eq!(other_fir.fir.len(), 1);
    assert_eq!(other_fir.fir[0].sequence_number, 0);
}

#[test]
fn rtcp_execution_plan_throttles_repeated_keyframe_requests_within_gate() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();
    let actions = [
        RtcpForwardAction::KeyFrameRequest {
            kind: KeyFrameRequestKind::Pli,
            media_ssrc: 1234,
        },
        RtcpForwardAction::KeyFrameRequest {
            kind: KeyFrameRequestKind::Fir,
            media_ssrc: 1234,
        },
    ];

    let first = build_rtcp_execution_plan(&key, &actions, &store, 20_000);
    assert_eq!(first.keyframe_requests.len(), 2);

    let second = build_rtcp_execution_plan(&key, &actions, &store, 20_100);
    assert!(
        second.keyframe_requests.is_empty(),
        "second requests inside gate window should be suppressed"
    );

    let third = build_rtcp_execution_plan(&key, &actions, &store, 20_400);
    assert_eq!(
        third.keyframe_requests.len(),
        2,
        "requests should reopen after gate window"
    );
}

#[test]
fn build_keyframe_feedback_packet_emits_pli_with_target_media_ssrc() {
    let request = KeyframeFeedbackRequest {
        kind: KeyFrameRequestKind::Pli,
        media_ssrc: 0x1122_3344,
        fir_sequence_number: None,
    };

    let packet = build_keyframe_feedback_packet(&request);
    let pli = packet
            .as_any()
            .downcast_ref::<
                rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
            >()
            .expect("packet should be PLI");

    assert_eq!(pli.media_ssrc, 0x1122_3344);
    assert_eq!(pli.sender_ssrc, 0);
}

#[test]
fn rewrite_sender_report_packet_preserves_fields_and_applies_mapped_identity() {
    let original = rtc::rtcp::sender_report::SenderReport {
        ssrc: 0x1111,
        ntp_time: 0x2222_3333_4444_5555,
        rtp_time: 90_000,
        packet_count: 123,
        octet_count: 456,
        reports: Vec::new(),
        profile_extensions: Default::default(),
    };
    let mapped = MappedSenderReport {
        ssrc: 0x9999,
        rtp_timestamp: 180_000,
    };

    let rewritten = rewrite_sender_report_packet(&original, mapped);

    assert_eq!(rewritten.ssrc, 0x9999);
    assert_eq!(rewritten.rtp_time, 180_000);
    assert_eq!(rewritten.ntp_time, original.ntp_time);
    assert_eq!(rewritten.packet_count, original.packet_count);
    assert_eq!(rewritten.octet_count, original.octet_count);
    assert_eq!(rewritten.reports, original.reports);
    assert_eq!(rewritten.profile_extensions, original.profile_extensions);
}

#[test]
fn build_keyframe_feedback_packet_emits_fir_with_planned_sequence_number() {
    let request = KeyframeFeedbackRequest {
        kind: KeyFrameRequestKind::Fir,
        media_ssrc: 0x5566_7788,
        fir_sequence_number: Some(17),
    };

    let packet = build_keyframe_feedback_packet(&request);
    let fir = packet
        .as_any()
        .downcast_ref::<rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest>()
        .expect("packet should be FIR");

    assert_eq!(fir.media_ssrc, 0x5566_7788);
    assert_eq!(fir.sender_ssrc, 0);
    assert_eq!(fir.fir.len(), 1);
    assert_eq!(fir.fir[0].ssrc, 0x5566_7788);
    assert_eq!(fir.fir[0].sequence_number, 17);
}

#[test]
fn rtcp_execution_plan_assigns_incrementing_fir_sequence_numbers_per_subscriber_and_ssrc() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();
    let actions = [RtcpForwardAction::KeyFrameRequest {
        kind: KeyFrameRequestKind::Fir,
        media_ssrc: 0x1234,
    }];

    let first = build_rtcp_execution_plan(&key, &actions, &store, 30_000);
    assert_eq!(first.keyframe_requests.len(), 1);
    assert_eq!(first.keyframe_requests[0].fir_sequence_number, Some(0));

    let second = build_rtcp_execution_plan(&key, &actions, &store, 30_400);
    assert_eq!(second.keyframe_requests.len(), 1);
    assert_eq!(second.keyframe_requests[0].fir_sequence_number, Some(1));

    let other_media_actions = [RtcpForwardAction::KeyFrameRequest {
        kind: KeyFrameRequestKind::Fir,
        media_ssrc: 0x5678,
    }];
    let other_media = build_rtcp_execution_plan(&key, &other_media_actions, &store, 30_800);
    assert_eq!(other_media.keyframe_requests.len(), 1);
    assert_eq!(
        other_media.keyframe_requests[0].fir_sequence_number,
        Some(0)
    );
}

#[test]
fn rtcp_execution_plan_rewrites_sender_reports_into_subscriber_state() {
    let key = (
        "room".to_string(),
        "publisher".to_string(),
        "TR_video".to_string(),
        "subscriber".to_string(),
    );
    let store = RtpForwardingStore::default();

    let first = build_rtcp_execution_plan(
        &key,
        &[RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0x9999,
                rtp_time: 90_000,
                packet_count: 10,
                octet_count: 20,
                ..Default::default()
            },
        }],
        &store,
        1,
    );
    assert_eq!(first.rewritten_sender_reports.len(), 1);
    assert_eq!(first.rewritten_sender_reports[0].ssrc, 0x9999);
    assert_eq!(first.rewritten_sender_reports[0].rtp_time, 90_000);
    assert_eq!(first.rewritten_sender_reports[0].packet_count, 10);

    let second = build_rtcp_execution_plan(
        &key,
        &[RtcpForwardAction::SenderReport {
            report: rtc::rtcp::sender_report::SenderReport {
                ssrc: 0x9999,
                rtp_time: 90_900,
                packet_count: 11,
                octet_count: 21,
                ..Default::default()
            },
        }],
        &store,
        2,
    );
    assert_eq!(second.rewritten_sender_reports.len(), 1);
    assert_eq!(second.rewritten_sender_reports[0].ssrc, 0x9999);
    assert_eq!(second.rewritten_sender_reports[0].rtp_time, 90_900);
    assert_eq!(second.rewritten_sender_reports[0].packet_count, 11);
}

#[test]
fn update_data_subscription_regrant_can_subscribe_allows_subscribe_again() {
    let state = state();
    let room = "data-track-subscription-regrant-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, true);

    let first = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 41,
                name: "telemetry-first".to_string(),
                ..Default::default()
            },
        )
        .expect("first publish should succeed");
    let second = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 42,
                name: "telemetry-second".to_string(),
                ..Default::default()
            },
        )
        .expect("second publish should succeed");

    let first_subscribe = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: first.sid.clone(),
                subscribe: true,
                options: None,
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(first_handles)) =
        first_subscribe.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert_eq!(first_handles.sub_handles.len(), 1);

    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, false);

    let denied_subscribe = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: second.sid.clone(),
                subscribe: true,
                options: Some(proto::DataTrackSubscriptionOptions {
                    target_fps: Some(30),
                }),
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(denied_handles)) =
        denied_subscribe.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert_eq!(
        denied_handles.sub_handles.len(),
        1,
        "denied subscribe should not add new handles, but existing subscription handle remains"
    );
    assert_eq!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 41)
            .len(),
        1
    );
    assert!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 42)
            .is_empty()
    );

    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, true);

    let regranted_subscribe = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: second.sid,
                subscribe: true,
                options: Some(proto::DataTrackSubscriptionOptions {
                    target_fps: Some(30),
                }),
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(regranted_handles)) =
        regranted_subscribe.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert_eq!(regranted_handles.sub_handles.len(), 2);
    assert_eq!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 41)
            .len(),
        1
    );
    assert_eq!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 42)
            .len(),
        1
    );
}

#[test]
fn update_data_subscription_without_can_subscribe_does_not_create_mapping() {
    let state = state();
    let room = "data-track-subscription-permissions-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);
    state
        .subscribe_permissions
        .set_can_subscribe(room, subscriber, false);

    let published = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 21,
                name: "telemetry".to_string(),
                ..Default::default()
            },
        )
        .expect("publish should succeed");

    let response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid,
                subscribe: true,
                options: Some(proto::DataTrackSubscriptionOptions {
                    target_fps: Some(15),
                }),
            }],
        },
    );

    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
        response.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert!(handles.sub_handles.is_empty());
    assert!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 21)
            .is_empty()
    );
}

#[test]
fn update_data_subscription_unknown_track_does_not_clear_existing_handles() {
    let state = state();
    let room = "data-track-subscription-unknown-track-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let published = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 11,
                name: "known-track".to_string(),
                ..Default::default()
            },
        )
        .expect("publish should succeed");

    let initial_response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid,
                subscribe: true,
                options: None,
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(initial_handles)) =
        initial_response.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert_eq!(initial_handles.sub_handles.len(), 1);

    let response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: "DTR_missing".to_string(),
                subscribe: true,
                options: Some(proto::DataTrackSubscriptionOptions {
                    target_fps: Some(20),
                }),
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
        response.message
    else {
        panic!("expected data-track subscriber handles response");
    };

    assert_eq!(handles.sub_handles.len(), 1);
}

#[test]
fn update_data_subscription_mixed_updates_apply_sequentially() {
    let state = state();
    let room = "data-track-subscription-mixed-updates-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let first_track = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 41,
                name: "first".to_string(),
                ..Default::default()
            },
        )
        .expect("first publish should succeed");
    let second_track = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 42,
                name: "second".to_string(),
                ..Default::default()
            },
        )
        .expect("second publish should succeed");

    let _ = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: first_track.sid.clone(),
                subscribe: true,
                options: None,
            }],
        },
    );

    let response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![
                proto::update_data_subscription::Update {
                    track_sid: first_track.sid,
                    subscribe: false,
                    options: None,
                },
                proto::update_data_subscription::Update {
                    track_sid: second_track.sid,
                    subscribe: true,
                    options: Some(proto::DataTrackSubscriptionOptions {
                        target_fps: Some(10),
                    }),
                },
            ],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
        response.message
    else {
        panic!("expected data-track subscriber handles response");
    };

    assert_eq!(handles.sub_handles.len(), 1);
    assert!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 41)
            .is_empty()
    );
    assert_eq!(
        state
            .data_track_subscriptions
            .subscribers_for_packet(room, publisher, 42)
            .len(),
        1
    );
}

#[test]
fn update_data_subscription_subscribe_updates_existing_options() {
    let state = state();
    let room = "data-track-subscription-options-room";
    let publisher = "publisher";
    let subscriber = "subscriber";

    join_participant_for_data_track_test(&state, room, publisher);
    join_participant_for_data_track_test(&state, room, subscriber);

    let published = state
        .data_tracks
        .publish(
            room,
            publisher,
            &proto::PublishDataTrackRequest {
                pub_handle: 9,
                name: "telemetry".to_string(),
                ..Default::default()
            },
        )
        .expect("publish should succeed");

    let initial_response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid.clone(),
                subscribe: true,
                options: Some(proto::DataTrackSubscriptionOptions {
                    target_fps: Some(12),
                }),
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(initial_handles)) =
        initial_response.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert_eq!(initial_handles.sub_handles.len(), 1);
    let (sub_handle, _) = initial_handles
        .sub_handles
        .iter()
        .next()
        .expect("sub handle should be present");
    assert_eq!(
        state
            .data_track_subscriptions
            .options_for_track(room, subscriber, &published.sid)
            .and_then(|options| options.target_fps),
        Some(12)
    );

    let update_response = update_data_subscription_response(
        &state,
        room,
        subscriber,
        proto::UpdateDataSubscription {
            updates: vec![proto::update_data_subscription::Update {
                track_sid: published.sid.clone(),
                subscribe: true,
                options: Some(proto::DataTrackSubscriptionOptions {
                    target_fps: Some(30),
                }),
            }],
        },
    );
    let Some(proto::signal_response::Message::DataTrackSubscriberHandles(updated_handles)) =
        update_response.message
    else {
        panic!("expected data-track subscriber handles response");
    };
    assert_eq!(updated_handles.sub_handles.len(), 1);
    assert!(updated_handles.sub_handles.contains_key(sub_handle));
    assert_eq!(
        state
            .data_track_subscriptions
            .options_for_track(room, subscriber, &published.sid)
            .and_then(|options| options.target_fps),
        Some(30)
    );
}

#[tokio::test]
async fn validate_v1_accepts_valid_token_and_join_request() {
    let response = router(state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/rtc/v1/validate?join_request={}",
                    join_request_param()
                ))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", token("test-room")),
                )
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "success");
}

#[tokio::test]
async fn validate_v1_accepts_gzip_join_request() {
    let response = router(state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/rtc/v1/validate?join_request={}",
                    gzip_join_request_param()
                ))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", token("test-room")),
                )
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn validate_v1_sets_access_control_allow_origin_header_on_success() {
    let response = router(state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/rtc/v1/validate?join_request={}",
                    join_request_param()
                ))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", token("test-room")),
                )
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
}

#[tokio::test]
async fn validate_v1_rejects_malformed_join_request_with_bad_request_and_cors_header() {
    let response = router(state())
        .oneshot(
            Request::builder()
                .uri("/rtc/v1/validate?join_request=not-base64")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", token("test-room")),
                )
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
    let body = body_text(response).await;
    assert!(
        !body.is_empty(),
        "malformed join_request response should include error details"
    );
}

#[tokio::test]
async fn validate_v1_sets_access_control_allow_origin_header_on_auth_errors() {
    let response = router(state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/rtc/v1/validate?join_request={}",
                    join_request_param()
                ))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
}

#[tokio::test]
async fn rtc_v1_join_with_auto_subscribe_false_marks_existing_tracks_unsubscribed() {
    let signal_state = state();
    signal_state
        .rooms
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("publisher should join room");
    signal_state
        .rooms
        .add_participant_track(
            "test-room",
            "alice",
            proto::TrackInfo {
                sid: "TR_auto_subscribe".to_string(),
                r#type: proto::TrackType::Video as i32,
                ..Default::default()
            },
        )
        .expect("publisher track should be added");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let join_request = wrapped_join_request_param(proto::JoinRequest {
        connection_settings: Some(proto::ConnectionSettings {
            auto_subscribe: false,
            ..Default::default()
        }),
        ..Default::default()
    });
    let url = format!("ws://{addr}/rtc/v1?join_request={join_request}");
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token_for("test-room", "bob", "Bob")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _ = socket.next().await;

    assert!(
        !signal_state.media_subscriptions.is_subscribed(
            "test-room",
            "alice",
            "TR_auto_subscribe",
            "bob",
        ),
        "auto_subscribe=false should mark existing tracks as unsubscribed"
    );

    let _ = socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn rtc_v1_join_defaults_auto_subscribe_true_when_parameter_absent() {
    let signal_state = state();
    signal_state
        .rooms
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("publisher should join room");
    signal_state
        .rooms
        .add_participant_track(
            "test-room",
            "alice",
            proto::TrackInfo {
                sid: "TR_auto_subscribe_default".to_string(),
                r#type: proto::TrackType::Video as i32,
                ..Default::default()
            },
        )
        .expect("publisher track should be added");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token_for("test-room", "bob", "Bob")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _ = socket.next().await;

    assert!(
        signal_state.media_subscriptions.is_subscribed(
            "test-room",
            "alice",
            "TR_auto_subscribe_default",
            "bob",
        ),
        "auto_subscribe should default to true when join parameter is absent"
    );

    let _ = socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_accepts_access_token_query_parameter() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let request = format!(
        "ws://{addr}/rtc/v1?join_request={}&access_token={}",
        join_request_param(),
        token("test-room")
    )
    .into_client_request()
    .expect("request should build");

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect with query token");
    let message = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");
    let Message::Binary(bytes) = message else {
        panic!("expected binary join response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };
    assert_eq!(join.room.expect("room should exist").name, "test-room");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_rejects_missing_join_request_with_bad_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let request = format!("ws://{addr}/rtc/v1?access_token={}", token("test-room"))
        .into_client_request()
        .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("missing join_request should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::BAD_REQUEST);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_invalid_binary_signal_request_frame_does_not_close_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    socket
        .send(Message::Binary(vec![0, 1, 2, 3, 4].into()))
        .await
        .expect("invalid binary payload should send");

    let ping_timestamp = 4242;
    let ping_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: ping_timestamp,
            ..Default::default()
        })),
    };
    socket
        .send(Message::Binary(ping_request.encode_to_vec().into()))
        .await
        .expect("ping should send after invalid frame");

    let pong_message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("pong should arrive")
        .expect("pong frame should exist")
        .expect("pong frame should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    let Some(proto::signal_response::Message::PongResp(pong)) = pong.message else {
        panic!("expected pong_resp response");
    };
    assert_eq!(pong.last_ping_timestamp, ping_timestamp);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_empty_signal_request_is_ignored_and_socket_remains_usable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    socket
        .send(Message::Binary(
            proto::SignalRequest { message: None }
                .encode_to_vec()
                .into(),
        ))
        .await
        .expect("empty signal request should send");

    let ping_timestamp = 9898;
    socket
        .send(Message::Binary(
            proto::SignalRequest {
                message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                    timestamp: ping_timestamp,
                    ..Default::default()
                })),
            }
            .encode_to_vec()
            .into(),
        ))
        .await
        .expect("follow-up ping should send");

    let pong_message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("pong should arrive")
        .expect("pong frame should exist")
        .expect("pong frame should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    let Some(proto::signal_response::Message::PongResp(pong)) = pong.message else {
        panic!("expected pong_resp response");
    };
    assert_eq!(pong.last_ping_timestamp, ping_timestamp);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_rejects_missing_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let request = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param())
        .into_client_request()
        .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("missing token should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_rejects_empty_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let request = format!(
        "ws://{addr}/rtc/v1?join_request={}&access_token=",
        join_request_param()
    )
    .into_client_request()
    .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("empty token should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_rejects_malformed_jwt() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let request = format!(
        "ws://{addr}/rtc/v1?join_request={}&access_token=not-a-jwt",
        join_request_param()
    )
    .into_client_request()
    .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("malformed jwt should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_rejects_unsigned_or_badly_signed_jwt() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let bad_signature = token_for_claims_with_secret(
        Claims {
            iss: API_KEY.to_string(),
            exp: now + Duration::from_secs(60).as_secs() as usize,
            sub: "alice".to_string(),
            video: VideoGrants {
                room_join: true,
                room: "test-room".to_string(),
                ..Default::default()
            },
            ..Default::default()
        },
        "wrong-secret",
    );

    let request = format!(
        "ws://{addr}/rtc/v1?join_request={}&access_token={bad_signature}",
        join_request_param()
    )
    .into_client_request()
    .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("badly signed jwt should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test]
async fn rtc_v0_websocket_rejects_missing_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let request = format!("ws://{addr}/rtc")
        .into_client_request()
        .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("missing token should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_join_rejects_expired_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let expired = token_for_claims(Claims {
        iss: API_KEY.to_string(),
        exp: now.saturating_sub(120),
        sub: "alice".to_string(),
        video: VideoGrants {
            room_join: true,
            room: "test-room".to_string(),
            ..Default::default()
        },
        ..Default::default()
    });

    let request = format!(
        "ws://{addr}/rtc/v1?join_request={}&access_token={expired}",
        join_request_param()
    )
    .into_client_request()
    .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("expired token should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_join_rejects_future_nbf_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let future_nbf = token_for_claims(Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(300).as_secs() as usize,
        nbf: now + Duration::from_secs(120).as_secs() as usize,
        sub: "alice".to_string(),
        video: VideoGrants {
            room_join: true,
            room: "test-room".to_string(),
            ..Default::default()
        },
        ..Default::default()
    });

    let request = format!(
        "ws://{addr}/rtc/v1?join_request={}&access_token={future_nbf}",
        join_request_param()
    )
    .into_client_request()
    .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("future nbf token should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_join_rejects_room_join_token_without_identity() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let no_identity = token_for_claims(Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        video: VideoGrants {
            room_join: true,
            room: "test-room".to_string(),
            ..Default::default()
        },
        ..Default::default()
    });

    let request = format!(
        "ws://{addr}/rtc/v1?join_request={}&access_token={no_identity}",
        join_request_param()
    )
    .into_client_request()
    .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("room join token without identity should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::FORBIDDEN);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_join_rejects_token_without_room_join_grant() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let no_room_join = token_for_claims(Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        sub: "alice".to_string(),
        video: VideoGrants {
            room_join: false,
            room: "test-room".to_string(),
            ..Default::default()
        },
        ..Default::default()
    });

    let request = format!(
        "ws://{addr}/rtc/v1?join_request={}&access_token={no_room_join}",
        join_request_param()
    )
    .into_client_request()
    .expect("request should build");

    let err = connect_async(request)
        .await
        .expect_err("token without room join grant should reject websocket handshake");
    let status = match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP handshake rejection, got {other:?}"),
    };
    assert_eq!(status, StatusCode::FORBIDDEN);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_sends_join_response() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    assert_eq!(join.room.expect("room should exist").name, "test-room");
    let participant = join.participant.expect("participant should exist");
    assert_eq!(participant.identity, "alice");
    assert_eq!(participant.name, "Alice");
    assert_eq!(participant.metadata, "join metadata");
    assert_eq!(
        participant.kind,
        proto::participant_info::Kind::Standard as i32,
        "default token kind should map to standard"
    );
    let permission = participant
        .permission
        .as_ref()
        .expect("join participant permission should be present");
    assert!(permission.can_publish);
    assert!(permission.can_subscribe);
    assert!(permission.can_publish_data);
    assert!(!permission.can_update_metadata);
    assert!(!permission.hidden);
    assert_eq!(join.ping_interval, PING_INTERVAL_SECONDS);
    assert_eq!(join.ping_timeout, PING_TIMEOUT_SECONDS);
    let server_info = join
        .server_info
        .as_ref()
        .expect("join server_info should be present");
    assert_eq!(
        server_info.edition,
        proto::server_info::Edition::Standard as i32
    );
    assert!(!server_info.version.is_empty());
    assert_eq!(join.ice_servers.len(), 1);
    assert_eq!(
        join.ice_servers[0].urls,
        vec!["stun:stun.l.google.com:19302"]
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_join_response_uses_participant_kind_from_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &Claims {
            iss: API_KEY.to_string(),
            exp: now + Duration::from_secs(60).as_secs() as usize,
            sub: "agent-1".to_string(),
            name: "Agent One".to_string(),
            kind: "agent".to_string(),
            video: VideoGrants {
                room_join: true,
                room: "test-room".to_string(),
                ..Default::default()
            },
            ..Default::default()
        },
        &EncodingKey::from_secret(API_SECRET.as_bytes()),
    )
    .expect("token should encode");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };
    let participant = join.participant.expect("participant should exist");
    assert_eq!(
        participant.kind,
        proto::participant_info::Kind::Agent as i32
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_join_response_includes_token_name_metadata_and_attributes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let token = token_for_claims(Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        sub: "alice".to_string(),
        name: "Alice Token Name".to_string(),
        metadata: "token-metadata".to_string(),
        attributes: HashMap::from([
            ("tier".to_string(), "gold".to_string()),
            ("lang".to_string(), "en".to_string()),
        ]),
        video: VideoGrants {
            room_join: true,
            room: "test-room".to_string(),
            ..Default::default()
        },
        ..Default::default()
    });

    let join_request = wrapped_join_request_param(proto::JoinRequest {
        metadata: String::new(),
        ..Default::default()
    });
    let url = format!("ws://{addr}/rtc/v1?join_request={join_request}");
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };
    let participant = join.participant.expect("participant should exist");
    assert_eq!(participant.name, "Alice Token Name");
    assert_eq!(participant.metadata, "token-metadata");
    assert_eq!(
        participant.attributes.get("tier"),
        Some(&"gold".to_string())
    );
    assert_eq!(participant.attributes.get("lang"), Some(&"en".to_string()));

    server.abort();
}

#[tokio::test]
async fn rtc_v1_first_join_applies_room_config_from_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let room_config = serde_json::json!({
        "emptyTimeout": 21,
        "departureTimeout": 17,
        "maxParticipants": 33,
        "metadata": "room-meta-a"
    });
    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_room_config("test-room", "alice", "Alice", room_config)
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };
    let room = join.room.expect("room should exist");
    assert_eq!(room.empty_timeout, 21);
    assert_eq!(room.departure_timeout, 17);
    assert_eq!(room.max_participants, 33);
    assert_eq!(room.metadata, "room-meta-a");

    let _ = socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn rtc_v1_later_join_ignores_room_config_when_room_already_exists() {
    let signal_state = state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let first_room_config = serde_json::json!({
        "emptyTimeout": 25,
        "departureTimeout": 9,
        "maxParticipants": 100,
        "metadata": "room-meta-first"
    });
    let second_room_config = serde_json::json!({
        "emptyTimeout": 99,
        "departureTimeout": 77,
        "maxParticipants": 200,
        "metadata": "room-meta-second"
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_room_config("test-room", "alice", "Alice", first_room_config)
        ))
        .expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("websocket should connect");
    let _ = first_socket.next().await;

    let mut second_request = url.into_client_request().expect("request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_room_config("test-room", "bob", "Bob", second_room_config)
        ))
        .expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("websocket should connect");
    let second_message = second_socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(second_bytes) = second_message else {
        panic!("expected binary protobuf signal response");
    };
    let second_response = proto::SignalResponse::decode(second_bytes.as_ref())
        .expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(second_join)) = second_response.message else {
        panic!("expected join response");
    };
    let room = second_join.room.expect("room should exist");
    assert_eq!(room.empty_timeout, 25);
    assert_eq!(room.departure_timeout, 9);
    assert_eq!(room.max_participants, 100);
    assert_eq!(room.metadata, "room-meta-first");

    let _ = first_socket.close(None).await;
    let _ = second_socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn rtc_v1_join_request_add_track_requests_emit_track_published() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let join_request = wrapped_join_request_param(proto::JoinRequest {
        add_track_requests: vec![proto::AddTrackRequest {
            cid: "audio-cid-prepublish".to_string(),
            name: "mic-prepublish".to_string(),
            r#type: proto::TrackType::Audio as i32,
            source: proto::TrackSource::Microphone as i32,
            ..Default::default()
        }],
        ..Default::default()
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={join_request}");
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let join_message = socket
        .next()
        .await
        .expect("join websocket message should arrive")
        .expect("join websocket message should be ok");
    let Message::Binary(join_bytes) = join_message else {
        panic!("expected binary protobuf join response");
    };
    let join_response =
        proto::SignalResponse::decode(join_bytes.as_ref()).expect("join response should decode");
    let Some(proto::signal_response::Message::Join(_)) = join_response.message else {
        panic!("expected join response");
    };

    let track_published_message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("track published response should arrive")
        .expect("track published websocket message should exist")
        .expect("track published websocket message should be ok");
    let Message::Binary(track_bytes) = track_published_message else {
        panic!("expected binary protobuf track published response");
    };
    let track_response =
        proto::SignalResponse::decode(track_bytes.as_ref()).expect("track response should decode");
    let Some(proto::signal_response::Message::TrackPublished(track_published)) =
        track_response.message
    else {
        panic!("expected track published response");
    };

    assert_eq!(track_published.cid, "audio-cid-prepublish");
    let published_track = track_published.track.expect("track info should be present");
    assert_eq!(published_track.name, "mic-prepublish");
    assert_eq!(published_track.r#type, proto::TrackType::Audio as i32);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_uses_configured_ice_servers_in_join_response() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let configured_ice_servers = vec![
        proto::IceServer {
            urls: vec!["stun:stun.example.net:3478".to_string()],
            ..Default::default()
        },
        proto::IceServer {
            urls: vec!["turn:turn.example.net:3478?transport=udp".to_string()],
            username: "turn-user".to_string(),
            credential: "turn-pass".to_string(),
        },
    ];
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_ice_servers(configured_ice_servers)),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    assert_eq!(join.ice_servers.len(), 2);
    assert_eq!(join.ice_servers[0].urls, vec!["stun:stun.example.net:3478"]);
    assert_eq!(
        join.ice_servers[1].urls,
        vec!["turn:turn.example.net:3478?transport=udp"]
    );
    assert_eq!(join.ice_servers[1].username, "turn-user");
    assert_eq!(join.ice_servers[1].credential, "turn-pass");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_uses_new_participant_sid_for_dynamic_ice_servers() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_participant_ice_server_provider()),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");
    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    let participant_sid = join.participant.expect("participant should exist").sid;
    assert_ne!(
        participant_sid, "alice",
        "provider must not receive identity"
    );
    assert_eq!(join.ice_servers.len(), 1);
    assert_eq!(
        join.ice_servers[0].username,
        format!("turn-{participant_sid}")
    );
    assert_eq!(
        join.ice_servers[0].credential,
        format!("credential-{participant_sid}")
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_omits_ice_servers_when_state_is_configured_empty() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state_with_ice_servers(Vec::new())))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    assert!(
        join.ice_servers.is_empty(),
        "empty configured ICE list should propagate as empty join response"
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_invalid_offer_does_not_close_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let _join = socket
        .next()
        .await
        .expect("join response should arrive")
        .expect("join response should be ok");

    let invalid_offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: "not-valid-sdp".to_string(),
                id: 99,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(invalid_offer.encode_to_vec().into()))
        .await
        .expect("invalid offer send should not fail");

    let ping_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: 77,
            rtt: 0,
        })),
    };
    socket
        .send(Message::Binary(ping_request.encode_to_vec().into()))
        .await
        .expect("ping request send should succeed");

    let pong_message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("pong response should arrive before timeout")
        .expect("websocket should stay open after invalid offer")
        .expect("pong response should be ok");

    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary protobuf signal response");
    };
    let pong_response =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    assert!(matches!(
        pong_response.message,
        Some(proto::signal_response::Message::PongResp(_))
    ));

    server.abort();
}

#[tokio::test]
async fn rtc_v1_trickle_before_offer_does_not_close_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let _join = socket
        .next()
        .await
        .expect("join response should arrive")
        .expect("join response should be ok");

    let trickle_before_offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Trickle(
            proto::TrickleRequest {
                candidate_init: r#"{"candidate":"candidate:0 1 UDP 2122252543 127.0.0.1 12345 typ host","sdpMid":"0","sdpMLineIndex":0}"#.to_string(),
                target: proto::SignalTarget::Publisher as i32,
                r#final: false,
            },
        )),
    };
    socket
        .send(Message::Binary(trickle_before_offer.encode_to_vec().into()))
        .await
        .expect("trickle before offer should send");

    let ping_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: 31415,
            rtt: 0,
        })),
    };
    socket
        .send(Message::Binary(ping_request.encode_to_vec().into()))
        .await
        .expect("ping request send should succeed");

    let pong_message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("pong response should arrive before timeout")
        .expect("websocket should stay open after trickle-before-offer")
        .expect("pong response should be ok");

    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary protobuf signal response");
    };
    let pong_response =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    assert!(matches!(
        pong_response.message,
        Some(proto::signal_response::Message::PongResp(_))
    ));

    server.abort();
}

#[tokio::test]
async fn rtc_v1_invalid_trickle_does_not_close_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let _join = socket
        .next()
        .await
        .expect("join response should arrive")
        .expect("join response should be ok");

    let invalid_trickle = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Trickle(
            proto::TrickleRequest {
                candidate_init: "{".to_string(),
                target: proto::SignalTarget::Publisher as i32,
                r#final: false,
            },
        )),
    };
    socket
        .send(Message::Binary(invalid_trickle.encode_to_vec().into()))
        .await
        .expect("invalid trickle send should not fail");

    let ping_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: 42,
            rtt: 0,
        })),
    };
    socket
        .send(Message::Binary(ping_request.encode_to_vec().into()))
        .await
        .expect("ping request send should succeed");

    let pong_message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("pong response should arrive before timeout")
        .expect("websocket should stay open after invalid trickle")
        .expect("pong response should be ok");

    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary protobuf signal response");
    };
    let pong_response =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    assert!(matches!(
        pong_response.message,
        Some(proto::signal_response::Message::PongResp(_))
    ));

    server.abort();
}

#[tokio::test]
async fn rtc_v1_simulate_node_failure_emits_leave_reconnect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let join_message = socket
        .next()
        .await
        .expect("join websocket message should arrive")
        .expect("join websocket message should be ok");
    let Message::Binary(join_bytes) = join_message else {
        panic!("expected binary protobuf join response");
    };
    let join_response =
        proto::SignalResponse::decode(join_bytes.as_ref()).expect("join response should decode");
    let Some(proto::signal_response::Message::Join(_)) = join_response.message else {
        panic!("expected join response");
    };

    let simulate = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Simulate(
            proto::SimulateScenario {
                scenario: Some(proto::simulate_scenario::Scenario::NodeFailure(true)),
            },
        )),
    };
    socket
        .send(Message::Binary(simulate.encode_to_vec().into()))
        .await
        .expect("simulate request should send");

    let leave_message = socket
        .next()
        .await
        .expect("leave websocket message should arrive")
        .expect("leave websocket message should be ok");
    let Message::Binary(leave_bytes) = leave_message else {
        panic!("expected binary protobuf leave response");
    };
    let leave_response =
        proto::SignalResponse::decode(leave_bytes.as_ref()).expect("leave response should decode");
    let Some(proto::signal_response::Message::Leave(leave_request)) = leave_response.message else {
        panic!("expected leave response");
    };
    assert_eq!(
        leave_request.action,
        proto::leave_request::Action::Reconnect as i32
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_switch_candidate_tls_reconnect_enables_force_relay() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let token_value = token(room);
    let join_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut first_request = join_url
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(first_request)
        .await
        .expect("websocket should connect");
    let join_message = socket
        .next()
        .await
        .expect("join websocket message should arrive")
        .expect("join websocket message should be ok");
    let Message::Binary(join_bytes) = join_message else {
        panic!("expected binary protobuf join response");
    };
    let join_response =
        proto::SignalResponse::decode(join_bytes.as_ref()).expect("join response should decode");
    let Some(proto::signal_response::Message::Join(join)) = join_response.message else {
        panic!("expected join response");
    };
    let participant_sid = join.participant.expect("participant should exist").sid;

    // Establish a publisher peer connection first so reconnect resumes an active session.
    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(_answer)) = answer.message else {
        panic!("expected answer response");
    };

    let simulate = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Simulate(
            proto::SimulateScenario {
                scenario: Some(proto::simulate_scenario::Scenario::SwitchCandidateProtocol(
                    proto::CandidateProtocol::Tls as i32,
                )),
            },
        )),
    };
    socket
        .send(Message::Binary(simulate.encode_to_vec().into()))
        .await
        .expect("simulate request should send");

    let leave_request = loop {
        let message = socket
            .next()
            .await
            .expect("leave websocket message should arrive")
            .expect("leave websocket message should be ok");
        let Message::Binary(bytes) = message else {
            continue;
        };
        let response =
            proto::SignalResponse::decode(bytes.as_ref()).expect("leave response should decode");
        if let Some(proto::signal_response::Message::Leave(leave_request)) = response.message {
            break leave_request;
        }
    };
    assert_eq!(
        leave_request.action,
        proto::leave_request::Action::Reconnect as i32
    );
    assert_eq!(
        leave_request.reason,
        proto::DisconnectReason::ClientInitiated as i32
    );

    // Simulate an abrupt disconnect so the session remains resumable.
    drop(socket);

    let reconnect_join_request = wrapped_join_request_param(proto::JoinRequest {
        reconnect: true,
        participant_sid,
        ..Default::default()
    });
    let reconnect_url = format!("ws://{addr}/rtc/v1?join_request={reconnect_join_request}");
    let mut reconnect_request = reconnect_url
        .into_client_request()
        .expect("reconnect request should build");
    reconnect_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut reconnect_socket, _) = connect_async(reconnect_request)
        .await
        .expect("reconnect websocket should connect");
    let reconnect_message = reconnect_socket
        .next()
        .await
        .expect("reconnect websocket message should arrive")
        .expect("reconnect websocket message should be ok");
    let Message::Binary(reconnect_bytes) = reconnect_message else {
        panic!("expected binary protobuf reconnect response");
    };
    let reconnect_response = proto::SignalResponse::decode(reconnect_bytes.as_ref())
        .expect("reconnect response should decode");
    let Some(proto::signal_response::Message::Reconnect(reconnect)) = reconnect_response.message
    else {
        panic!("expected reconnect response");
    };
    let client_configuration = reconnect
        .client_configuration
        .expect("switch-candidate TLS reconnect should include client configuration");
    assert_eq!(
        client_configuration.force_relay,
        proto::ClientConfigSetting::Enabled as i32
    );

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
async fn rtc_v1_full_rejoin_after_switch_candidate_tls_enables_force_relay_on_join() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let token_value = token(room);
    let join_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut first_request = join_url
        .clone()
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(first_request)
        .await
        .expect("websocket should connect");
    let _join_message = socket
        .next()
        .await
        .expect("join websocket message should arrive")
        .expect("join websocket message should be ok");

    let simulate = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Simulate(
            proto::SimulateScenario {
                scenario: Some(proto::simulate_scenario::Scenario::SwitchCandidateProtocol(
                    proto::CandidateProtocol::Tls as i32,
                )),
            },
        )),
    };
    socket
        .send(Message::Binary(simulate.encode_to_vec().into()))
        .await
        .expect("simulate request should send");

    let leave_message = socket
        .next()
        .await
        .expect("leave websocket message should arrive")
        .expect("leave websocket message should be ok");
    let Message::Binary(leave_bytes) = leave_message else {
        panic!("expected binary protobuf leave response");
    };
    let leave_response =
        proto::SignalResponse::decode(leave_bytes.as_ref()).expect("leave response should decode");
    let Some(proto::signal_response::Message::Leave(_)) = leave_response.message else {
        panic!("expected leave response");
    };

    socket
        .close(None)
        .await
        .expect("socket close should succeed");

    // Full rejoin (not reconnect=true) is used by some SDK recovery paths.
    let mut second_request = join_url
        .into_client_request()
        .expect("second request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect");
    let second_message = second_socket
        .next()
        .await
        .expect("second websocket message should arrive")
        .expect("second websocket message should be ok");
    let Message::Binary(second_bytes) = second_message else {
        panic!("expected binary protobuf join response");
    };
    let second_response = proto::SignalResponse::decode(second_bytes.as_ref())
        .expect("second response should decode");
    let Some(proto::signal_response::Message::Join(join)) = second_response.message else {
        panic!("expected join response");
    };
    let client_configuration = join
        .client_configuration
        .expect("full rejoin after TLS switch should include client configuration");
    assert_eq!(
        client_configuration.force_relay,
        proto::ClientConfigSetting::Enabled as i32
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_reconnect_after_ungraceful_close_uses_existing_participant_sid_for_dynamic_ice_servers()
 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_participant_ice_server_provider()),
        )
        .await
        .expect("test server should run");
    });

    let room = "test-room";
    let token_value = token(room);

    let join_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut first_request = join_url
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let first_message = first_socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");
    let Message::Binary(first_bytes) = first_message else {
        panic!("expected binary protobuf signal response");
    };
    let first_response =
        proto::SignalResponse::decode(first_bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(first_join)) = first_response.message else {
        panic!("expected join response");
    };
    let participant_sid = first_join
        .participant
        .expect("participant should exist")
        .sid;
    assert!(!participant_sid.is_empty());

    // Establish a publisher peer connection first so reconnect resumes an active session.
    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    first_socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let answer_message = first_socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(_answer)) = answer.message else {
        panic!("expected answer response");
    };

    // Simulate an ungraceful disconnect by dropping the socket without a Leave request.
    drop(first_socket);

    let reconnect_join_request = wrapped_join_request_param(proto::JoinRequest {
        reconnect: true,
        participant_sid: participant_sid.clone(),
        ..Default::default()
    });
    let reconnect_url = format!("ws://{addr}/rtc/v1?join_request={reconnect_join_request}");
    let mut reconnect_request = reconnect_url
        .into_client_request()
        .expect("reconnect request should build");
    reconnect_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut reconnect_socket, _) = connect_async(reconnect_request)
        .await
        .expect("reconnect websocket should connect");
    let reconnect_message = reconnect_socket
        .next()
        .await
        .expect("reconnect websocket message should arrive")
        .expect("reconnect websocket message should be ok");
    let Message::Binary(reconnect_bytes) = reconnect_message else {
        panic!("expected binary protobuf signal response");
    };
    let reconnect_response = proto::SignalResponse::decode(reconnect_bytes.as_ref())
        .expect("reconnect response should decode");
    let Some(proto::signal_response::Message::Reconnect(reconnect)) = reconnect_response.message
    else {
        panic!("expected reconnect response");
    };
    assert_eq!(reconnect.ice_servers.len(), 1);
    assert_eq!(
        reconnect.ice_servers[0].username,
        format!("turn-{participant_sid}"),
        "reconnect provider should receive the existing participant SID"
    );
    assert_eq!(
        reconnect.ice_servers[0].credential,
        format!("credential-{participant_sid}")
    );

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
async fn rtc_v1_reconnect_missing_participant_sid_returns_state_mismatch_leave_disconnect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let token_value = token(room);

    let join_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut first_request = join_url
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    drop(first_socket);

    let reconnect_join_request = wrapped_join_request_param(proto::JoinRequest {
        reconnect: true,
        ..Default::default()
    });
    let reconnect_url = format!("ws://{addr}/rtc/v1?join_request={reconnect_join_request}");
    let mut reconnect_request = reconnect_url
        .into_client_request()
        .expect("reconnect request should build");
    reconnect_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut reconnect_socket, _) = connect_async(reconnect_request)
        .await
        .expect("reconnect websocket should connect");
    let leave_message = reconnect_socket
        .next()
        .await
        .expect("leave websocket message should arrive")
        .expect("leave websocket message should be ok");
    let Message::Binary(leave_bytes) = leave_message else {
        panic!("expected binary protobuf leave response");
    };
    let leave_response =
        proto::SignalResponse::decode(leave_bytes.as_ref()).expect("leave response should decode");
    let Some(proto::signal_response::Message::Leave(leave)) = leave_response.message else {
        panic!("expected leave response");
    };
    assert_eq!(leave.reason, proto::DisconnectReason::StateMismatch as i32);
    assert_eq!(
        leave.action,
        proto::leave_request::Action::Disconnect as i32
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_reconnect_stale_participant_sid_returns_state_mismatch_leave_disconnect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let token_value = token(room);

    let join_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut first_request = join_url
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    drop(first_socket);

    let reconnect_join_request = wrapped_join_request_param(proto::JoinRequest {
        reconnect: true,
        participant_sid: "PA_stale_sid".to_string(),
        ..Default::default()
    });
    let reconnect_url = format!("ws://{addr}/rtc/v1?join_request={reconnect_join_request}");
    let mut reconnect_request = reconnect_url
        .into_client_request()
        .expect("reconnect request should build");
    reconnect_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut reconnect_socket, _) = connect_async(reconnect_request)
        .await
        .expect("reconnect websocket should connect");
    let leave_message = reconnect_socket
        .next()
        .await
        .expect("leave websocket message should arrive")
        .expect("leave websocket message should be ok");
    let Message::Binary(leave_bytes) = leave_message else {
        panic!("expected binary protobuf leave response");
    };
    let leave_response =
        proto::SignalResponse::decode(leave_bytes.as_ref()).expect("leave response should decode");
    let Some(proto::signal_response::Message::Leave(leave)) = leave_response.message else {
        panic!("expected leave response");
    };
    assert_eq!(leave.reason, proto::DisconnectReason::StateMismatch as i32);
    assert_eq!(
        leave.action,
        proto::leave_request::Action::Disconnect as i32
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_reconnect_reason_matrix_returns_reconnect_and_pongresp() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let token_value = token(room);

    let join_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut first_request = join_url
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let first_message = first_socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");
    let Message::Binary(first_bytes) = first_message else {
        panic!("expected binary protobuf signal response");
    };
    let first_response =
        proto::SignalResponse::decode(first_bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(first_join)) = first_response.message else {
        panic!("expected join response");
    };
    let participant_sid = first_join
        .participant
        .expect("participant should exist")
        .sid;

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    first_socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let _answer_message = first_socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");

    drop(first_socket);

    let reasons = [
        proto::ReconnectReason::RrUnknown as i32,
        proto::ReconnectReason::RrSignalDisconnected as i32,
        proto::ReconnectReason::RrPublisherFailed as i32,
        proto::ReconnectReason::RrSubscriberFailed as i32,
        proto::ReconnectReason::RrSwitchCandidate as i32,
        999,
    ];

    for (idx, reason) in reasons.into_iter().enumerate() {
        let reconnect_join_request = wrapped_join_request_param(proto::JoinRequest {
            reconnect: true,
            participant_sid: participant_sid.clone(),
            reconnect_reason: reason,
            ..Default::default()
        });
        let reconnect_url = format!("ws://{addr}/rtc/v1?join_request={reconnect_join_request}");
        let mut reconnect_request = reconnect_url
            .into_client_request()
            .expect("reconnect request should build");
        reconnect_request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token_value}"))
                .expect("auth header should parse"),
        );

        let (mut reconnect_socket, _) = connect_async(reconnect_request)
            .await
            .expect("reconnect websocket should connect");
        let reconnect_message = reconnect_socket
            .next()
            .await
            .expect("reconnect websocket message should arrive")
            .expect("reconnect websocket message should be ok");
        let Message::Binary(reconnect_bytes) = reconnect_message else {
            panic!("expected binary protobuf reconnect response");
        };
        let reconnect_response = proto::SignalResponse::decode(reconnect_bytes.as_ref())
            .expect("reconnect response should decode");
        assert!(matches!(
            reconnect_response.message,
            Some(proto::signal_response::Message::Reconnect(_))
        ));

        let ping_timestamp = 7000 + idx as i64;
        let ping = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp,
                rtt: 0,
            })),
        };
        reconnect_socket
            .send(Message::Binary(ping.encode_to_vec().into()))
            .await
            .expect("ping should send on reconnect socket");

        let pong_message = reconnect_socket
            .next()
            .await
            .expect("pong should arrive")
            .expect("pong should be ok");
        let Message::Binary(pong_bytes) = pong_message else {
            panic!("expected binary pong response");
        };
        let pong_response =
            proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong should decode");
        let Some(proto::signal_response::Message::PongResp(pong)) = pong_response.message else {
            panic!("expected pong_resp response");
        };
        assert_eq!(pong.last_ping_timestamp, ping_timestamp);

        drop(reconnect_socket);
    }

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
async fn reconnect_then_old_socket_late_leave_does_not_remove_new_session() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let rooms = signal_state.rooms.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let identity = "alice";
    let token_value = token(room);

    let join_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut first_request = join_url
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let first_message = first_socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");
    let Message::Binary(first_bytes) = first_message else {
        panic!("expected binary protobuf signal response");
    };
    let first_response =
        proto::SignalResponse::decode(first_bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(first_join)) = first_response.message else {
        panic!("expected join response");
    };
    let participant_sid = first_join
        .participant
        .expect("participant should exist")
        .sid;

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    first_socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");
    let _answer_message = first_socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");

    let reconnect_join_request = wrapped_join_request_param(proto::JoinRequest {
        reconnect: true,
        participant_sid,
        ..Default::default()
    });
    let reconnect_url = format!("ws://{addr}/rtc/v1?join_request={reconnect_join_request}");
    let mut reconnect_request = reconnect_url
        .into_client_request()
        .expect("reconnect request should build");
    reconnect_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut reconnect_socket, _) = connect_async(reconnect_request)
        .await
        .expect("reconnect websocket should connect");
    let reconnect_message = reconnect_socket
        .next()
        .await
        .expect("reconnect websocket message should arrive")
        .expect("reconnect websocket message should be ok");
    let Message::Binary(reconnect_bytes) = reconnect_message else {
        panic!("expected binary protobuf reconnect response");
    };
    let reconnect_response = proto::SignalResponse::decode(reconnect_bytes.as_ref())
        .expect("reconnect response should decode");
    assert!(matches!(
        reconnect_response.message,
        Some(proto::signal_response::Message::Reconnect(_))
    ));

    let late_leave = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Leave(
            proto::LeaveRequest::default(),
        )),
    };
    first_socket
        .send(Message::Binary(late_leave.encode_to_vec().into()))
        .await
        .expect("stale socket leave should send");

    let ping_timestamp = 9090;
    let ping = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: ping_timestamp,
            rtt: 0,
        })),
    };
    reconnect_socket
        .send(Message::Binary(ping.encode_to_vec().into()))
        .await
        .expect("ping should send on active reconnect socket");
    let pong_message = reconnect_socket
        .next()
        .await
        .expect("pong should arrive on active reconnect socket")
        .expect("pong should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong_response =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    let Some(proto::signal_response::Message::PongResp(pong)) = pong_response.message else {
        panic!("expected pong_resp response");
    };
    assert_eq!(pong.last_ping_timestamp, ping_timestamp);

    assert!(
        rooms.get_participant(room, identity).is_ok(),
        "late leave from stale socket must not remove active reconnected participant"
    );

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
async fn rtc_v1_reconnect_succeeds_after_initial_token_expiration_if_session_is_resumable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state().with_reconnect_participant_retention_grace(Duration::from_secs(3));
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let short_lived_token = token_for_claims(Claims {
        iss: API_KEY.to_string(),
        exp: now + 1,
        sub: "alice".to_string(),
        video: VideoGrants {
            room_join: true,
            room: room.to_string(),
            ..Default::default()
        },
        ..Default::default()
    });

    let join_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut first_request = join_url
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {short_lived_token}"))
            .expect("auth header should parse"),
    );

    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("initial websocket should connect before token expiration");
    let first_message = first_socket
        .next()
        .await
        .expect("initial websocket message should arrive")
        .expect("initial websocket message should be ok");
    let Message::Binary(first_bytes) = first_message else {
        panic!("expected binary protobuf signal response");
    };
    let first_response =
        proto::SignalResponse::decode(first_bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(first_join)) = first_response.message else {
        panic!("expected join response");
    };
    let participant_sid = first_join
        .participant
        .expect("participant should exist")
        .sid;

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    first_socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");
    let _answer_message = first_socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");

    drop(first_socket);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let reconnect_join_request = wrapped_join_request_param(proto::JoinRequest {
        reconnect: true,
        participant_sid,
        ..Default::default()
    });
    let reconnect_url = format!("ws://{addr}/rtc/v1?join_request={reconnect_join_request}");
    let mut reconnect_request = reconnect_url
        .into_client_request()
        .expect("reconnect request should build");
    reconnect_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {short_lived_token}"))
            .expect("auth header should parse"),
    );

    let (mut reconnect_socket, _) = connect_async(reconnect_request)
        .await
        .expect("reconnect websocket should still connect after token expiry");
    let reconnect_message = reconnect_socket
        .next()
        .await
        .expect("reconnect websocket message should arrive")
        .expect("reconnect websocket message should be ok");
    let Message::Binary(reconnect_bytes) = reconnect_message else {
        panic!("expected binary protobuf reconnect response");
    };
    let reconnect_response = proto::SignalResponse::decode(reconnect_bytes.as_ref())
        .expect("reconnect response should decode");
    assert!(matches!(
        reconnect_response.message,
        Some(proto::signal_response::Message::Reconnect(_))
    ));

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
async fn rtc_v0_reconnect_query_params_return_reconnect_response() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let token_value = token(room);

    let join_url = format!("ws://{addr}/rtc");
    let mut first_request = join_url
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let first_message = first_socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");
    let Message::Binary(first_bytes) = first_message else {
        panic!("expected binary protobuf signal response");
    };
    let first_response =
        proto::SignalResponse::decode(first_bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(first_join)) = first_response.message else {
        panic!("expected join response");
    };
    let participant_sid = first_join
        .participant
        .expect("participant should exist")
        .sid;
    assert!(!participant_sid.is_empty());

    // Simulate abrupt disconnect before reconnecting.
    drop(first_socket);

    let reconnect_url = format!("ws://{addr}/rtc?reconnect=1&sid={participant_sid}");
    let mut reconnect_request = reconnect_url
        .into_client_request()
        .expect("reconnect request should build");
    reconnect_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut reconnect_socket, _) = connect_async(reconnect_request)
        .await
        .expect("reconnect websocket should connect");
    let reconnect_message = reconnect_socket
        .next()
        .await
        .expect("reconnect websocket message should arrive")
        .expect("reconnect websocket message should be ok");
    let Message::Binary(reconnect_bytes) = reconnect_message else {
        panic!("expected binary protobuf signal response");
    };
    let reconnect_response = proto::SignalResponse::decode(reconnect_bytes.as_ref())
        .expect("reconnect response should decode");
    assert!(matches!(
        reconnect_response.message,
        Some(proto::signal_response::Message::Reconnect(_))
    ));

    let maybe_follow_up =
        tokio::time::timeout(Duration::from_millis(150), reconnect_socket.next()).await;
    assert!(
        maybe_follow_up.is_err(),
        "v0 reconnect should not trigger immediate fresh subscriber offer when peer connections are retained"
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v0_ungraceful_disconnect_cleans_up_peer_connections_after_reconnect_grace() {
    let signal_state = state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let room = "test-room";
    let identity = "alice";
    let token_value = token(room);

    let join_url = format!("ws://{addr}/rtc");
    let mut request = join_url
        .into_client_request()
        .expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token_value}")).expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let _join_message = socket
        .next()
        .await
        .expect("join websocket message should arrive")
        .expect("join websocket message should be ok");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if signal_state
                .peer_connections
                .get(room, identity, SignalConnectionTarget::Subscriber)
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("subscriber peer connection should be created");

    drop(socket);

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        signal_state
            .peer_connections
            .get(room, identity, SignalConnectionTarget::Subscriber)
            .is_some(),
        "subscriber peer connection should be retained during reconnect grace"
    );

    tokio::time::sleep(Duration::from_millis(130)).await;
    assert!(
        signal_state
            .peer_connections
            .get(room, identity, SignalConnectionTarget::Subscriber)
            .is_none(),
        "subscriber peer connection should be cleaned up after reconnect grace elapses"
    );

    server.abort();
}

#[test]
fn room_node_placement_outcome_reports_non_local_as_relay_candidate() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let state =
        state_with_room_nodes_and_placement(room_nodes, Some("node-local".to_string()), false);

    let outcome = room_node_placement_outcome(&state, "relay-candidate-room");
    assert_eq!(
        outcome,
        RoomNodePlacementOutcome::NonLocalNeedsRelay {
            selected_room_node_id: "node-remote".to_string(),
        }
    );
}

#[test]
fn room_node_placement_outcome_reports_local_when_selected_node_is_local() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-local".to_string(),
            region: "local-region".to_string(),
        })
        .expect("local node should register");

    let state =
        state_with_room_nodes_and_placement(room_nodes, Some("node-local".to_string()), false);

    let outcome = room_node_placement_outcome(&state, "local-room");
    assert_eq!(outcome, RoomNodePlacementOutcome::LocalHandling);
}

#[test]
fn room_node_placement_outcome_reports_local_without_directory() {
    let state = state();
    let outcome = room_node_placement_outcome(&state, "no-directory-room");
    assert_eq!(outcome, RoomNodePlacementOutcome::LocalHandling);
}

#[test]
fn room_node_placement_outcome_keeps_existing_room_on_draining_local_owner() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-local".to_string(),
            region: "local-region".to_string(),
        })
        .expect("local node should register");
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");
    room_nodes
        .set_node_for_room("active-room", "node-local")
        .expect("active room should map to local node");
    room_nodes
        .set_node_draining("node-local", true)
        .expect("local node should transition to draining");

    let state =
        state_with_room_nodes_and_placement(room_nodes, Some("node-local".to_string()), false);

    let outcome = room_node_placement_outcome(&state, "active-room");
    assert_eq!(outcome, RoomNodePlacementOutcome::LocalHandling);
}

#[test]
fn room_node_placement_outcome_new_room_avoids_draining_local_owner() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-local".to_string(),
            region: "local-region".to_string(),
        })
        .expect("local node should register");
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");
    room_nodes
        .set_node_draining("node-local", true)
        .expect("local node should transition to draining");

    let state =
        state_with_room_nodes_and_placement(room_nodes, Some("node-local".to_string()), false);

    let outcome = room_node_placement_outcome(&state, "new-room");
    assert_eq!(
        outcome,
        RoomNodePlacementOutcome::NonLocalNeedsRelay {
            selected_room_node_id: "node-remote".to_string(),
        }
    );
}

#[test]
fn non_local_relay_intent_from_outcome_extracts_selected_node_identity_and_room() {
    let outcome = RoomNodePlacementOutcome::NonLocalNeedsRelay {
        selected_room_node_id: "node-remote".to_string(),
    };

    let auth = AuthContext {
        api_key: API_KEY.to_string(),
        claims: Claims {
            sub: "alice".to_string(),
            name: "Alice".to_string(),
            ..Default::default()
        },
    };
    let request = proto::JoinRequest {
        participant_sid: "PA_reconnect".to_string(),
        ..Default::default()
    };

    let target = non_local_relay_intent_from_outcome("relay-room", &auth, &request, &outcome, true)
        .expect("non-local outcome should derive relay target metadata");
    assert_eq!(target.room, "relay-room");
    assert_eq!(target.identity, "alice");
    assert_eq!(target.name, "Alice");
    assert_eq!(
        target.requested_participant_sid.as_deref(),
        Some("PA_reconnect")
    );
    assert_eq!(target.selected_room_node_id, "node-remote");
}

#[test]
fn non_local_relay_intent_from_outcome_is_none_for_local_handling() {
    let auth = AuthContext {
        api_key: API_KEY.to_string(),
        claims: Claims::default(),
    };
    let request = proto::JoinRequest::default();
    let target = non_local_relay_intent_from_outcome(
        "local-room",
        &auth,
        &request,
        &RoomNodePlacementOutcome::LocalHandling,
        false,
    );
    assert!(target.is_none());
}

#[derive(Debug, Default, Clone)]
struct InMemoryHashStore {
    values: Arc<std::sync::Mutex<HashMap<(String, String), String>>>,
}

#[derive(Debug, Default, Clone)]
struct FailingHashStore;

#[derive(Debug, Default, Clone)]
struct FlakyHsetHashStore {
    inner: InMemoryHashStore,
    remaining_hset_failures: Arc<std::sync::atomic::AtomicU8>,
}

impl RedisHashStore for InMemoryHashStore {
    fn hset(
        &self,
        key: &str,
        field: &str,
        value: &str,
    ) -> Result<(), oxidesfu_room::RoomNodeRegistryError> {
        self.values
            .lock()
            .expect("hash store lock should not be poisoned")
            .insert((key.to_string(), field.to_string()), value.to_string());
        Ok(())
    }

    fn hget(
        &self,
        key: &str,
        field: &str,
    ) -> Result<Option<String>, oxidesfu_room::RoomNodeRegistryError> {
        Ok(self
            .values
            .lock()
            .expect("hash store lock should not be poisoned")
            .get(&(key.to_string(), field.to_string()))
            .cloned())
    }

    fn hdel(&self, key: &str, field: &str) -> Result<(), oxidesfu_room::RoomNodeRegistryError> {
        self.values
            .lock()
            .expect("hash store lock should not be poisoned")
            .remove(&(key.to_string(), field.to_string()));
        Ok(())
    }

    fn hvals(&self, key: &str) -> Result<Vec<String>, oxidesfu_room::RoomNodeRegistryError> {
        let values = self
            .values
            .lock()
            .expect("hash store lock should not be poisoned")
            .iter()
            .filter_map(|((k, _), value)| (k == key).then_some(value.clone()))
            .collect();
        Ok(values)
    }
}

impl RedisHashStore for FailingHashStore {
    fn hset(
        &self,
        _key: &str,
        _field: &str,
        _value: &str,
    ) -> Result<(), oxidesfu_room::RoomNodeRegistryError> {
        Err(oxidesfu_room::RoomNodeRegistryError::Backend {
            message: "simulated redis outage on HSET".to_string(),
        })
    }

    fn hget(
        &self,
        _key: &str,
        _field: &str,
    ) -> Result<Option<String>, oxidesfu_room::RoomNodeRegistryError> {
        Err(oxidesfu_room::RoomNodeRegistryError::Backend {
            message: "simulated redis outage on HGET".to_string(),
        })
    }

    fn hdel(&self, _key: &str, _field: &str) -> Result<(), oxidesfu_room::RoomNodeRegistryError> {
        Err(oxidesfu_room::RoomNodeRegistryError::Backend {
            message: "simulated redis outage on HDEL".to_string(),
        })
    }

    fn hvals(&self, _key: &str) -> Result<Vec<String>, oxidesfu_room::RoomNodeRegistryError> {
        Err(oxidesfu_room::RoomNodeRegistryError::Backend {
            message: "simulated redis outage on HVALS".to_string(),
        })
    }
}

impl RedisHashStore for FlakyHsetHashStore {
    fn hset(
        &self,
        key: &str,
        field: &str,
        value: &str,
    ) -> Result<(), oxidesfu_room::RoomNodeRegistryError> {
        let remaining = self
            .remaining_hset_failures
            .load(std::sync::atomic::Ordering::Relaxed);
        if remaining > 0 {
            self.remaining_hset_failures
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            return Err(oxidesfu_room::RoomNodeRegistryError::Backend {
                message: "simulated transient HSET failure".to_string(),
            });
        }
        self.inner.hset(key, field, value)
    }

    fn hget(
        &self,
        key: &str,
        field: &str,
    ) -> Result<Option<String>, oxidesfu_room::RoomNodeRegistryError> {
        self.inner.hget(key, field)
    }

    fn hdel(&self, key: &str, field: &str) -> Result<(), oxidesfu_room::RoomNodeRegistryError> {
        self.inner.hdel(key, field)
    }

    fn hvals(&self, key: &str) -> Result<Vec<String>, oxidesfu_room::RoomNodeRegistryError> {
        self.inner.hvals(key)
    }
}

#[test]
fn relay_join_reject_propagates_actionable_reason_to_origin_node() {
    let response = NonLocalRelayJoinResponse::Rejected {
        code: "permission_denied".to_string(),
        msg: "room join not allowed".to_string(),
    };

    let details = non_local_relay_rejection_details(&response)
        .expect("rejected relay response should provide actionable details");
    assert_eq!(details.0, "permission_denied");
    assert_eq!(details.1, "room join not allowed");
}

#[test]
fn relay_join_accept_returns_join_response_shape_compatible_with_sdk() {
    let response = NonLocalRelayJoinResponse::Accepted {
        participant_sid: "PA_remote".to_string(),
        server_version: "0.1.0".to_string(),
        ping_interval: 5,
        ping_timeout: 15,
    };

    let join = non_local_relay_join_response_shape(&response)
        .expect("accepted relay response should map to join response shape");
    assert_eq!(
        join.participant.expect("participant should be present").sid,
        "PA_remote"
    );
    assert_eq!(join.server_version, "0.1.0");
    assert_eq!(join.ping_interval, 5);
    assert_eq!(join.ping_timeout, 15);
}

#[test]
fn redis_relay_dispatch_roundtrip_delivers_join_intent_and_response() {
    let mailbox = RedisRelayMailbox::with_store(InMemoryHashStore::default());

    let intent = NonLocalRelayJoinIntent {
        room: "relay-room".to_string(),
        identity: "alice".to_string(),
        name: "Alice".to_string(),
        requested_participant_sid: Some("PA_reconnect".to_string()),
        selected_room_node_id: "node-remote".to_string(),
        subscriber_primary: false,
        can_publish: true,
        can_subscribe: true,
        can_publish_data: true,
        can_update_metadata: false,
        hidden: false,
        metadata: String::new(),
        attributes: HashMap::new(),
        api_key: String::new(),
        kind: String::new(),
        kind_details: Vec::new(),
        destination_room: String::new(),
        room_config: None,
    };

    let receipt = mailbox
        .dispatch_intent(&intent)
        .expect("relay intent dispatch should store intent");

    let response = NonLocalRelayJoinResponse::Accepted {
        participant_sid: "PA_remote".to_string(),
        server_version: "0.1.0".to_string(),
        ping_interval: 5,
        ping_timeout: 15,
    };
    mailbox
        .store_response(&receipt, &response)
        .expect("relay response should store");

    let roundtrip = mailbox
        .fetch_response(&receipt)
        .expect("response fetch should succeed")
        .expect("response should be present");
    assert_eq!(roundtrip, response);

    let taken = mailbox
        .take_response(&receipt)
        .expect("response take should succeed")
        .expect("response should still be available");
    assert_eq!(taken, response);
    assert!(
        mailbox
            .fetch_response(&receipt)
            .expect("response fetch after take should succeed")
            .is_none()
    );
}

#[test]
fn redis_relay_mailbox_claims_next_intent_for_selected_node() {
    let mailbox = RedisRelayMailbox::with_store(InMemoryHashStore::default());

    let local = NonLocalRelayJoinIntent {
        room: "relay-room".to_string(),
        identity: "alice".to_string(),
        name: "Alice".to_string(),
        requested_participant_sid: None,
        selected_room_node_id: "node-local".to_string(),
        subscriber_primary: false,
        can_publish: true,
        can_subscribe: true,
        can_publish_data: true,
        can_update_metadata: false,
        hidden: false,
        metadata: String::new(),
        attributes: HashMap::new(),
        api_key: String::new(),
        kind: String::new(),
        kind_details: Vec::new(),
        destination_room: String::new(),
        room_config: None,
    };
    let remote_1 = NonLocalRelayJoinIntent {
        room: "relay-room".to_string(),
        identity: "bob".to_string(),
        name: "Bob".to_string(),
        requested_participant_sid: None,
        selected_room_node_id: "node-remote".to_string(),
        subscriber_primary: false,
        can_publish: true,
        can_subscribe: true,
        can_publish_data: true,
        can_update_metadata: false,
        hidden: false,
        metadata: String::new(),
        attributes: HashMap::new(),
        api_key: String::new(),
        kind: String::new(),
        kind_details: Vec::new(),
        destination_room: String::new(),
        room_config: None,
    };
    let remote_2 = NonLocalRelayJoinIntent {
        room: "relay-room".to_string(),
        identity: "charlie".to_string(),
        name: "Charlie".to_string(),
        requested_participant_sid: None,
        selected_room_node_id: "node-remote".to_string(),
        subscriber_primary: false,
        can_publish: true,
        can_subscribe: true,
        can_publish_data: true,
        can_update_metadata: false,
        hidden: false,
        metadata: String::new(),
        attributes: HashMap::new(),
        api_key: String::new(),
        kind: String::new(),
        kind_details: Vec::new(),
        destination_room: String::new(),
        room_config: None,
    };

    mailbox
        .dispatch_intent(&local)
        .expect("local-target intent should store");
    mailbox
        .dispatch_intent(&remote_1)
        .expect("first remote-target intent should store");
    mailbox
        .dispatch_intent(&remote_2)
        .expect("second remote-target intent should store");

    let first_claim = mailbox
        .claim_next_intent_for_node("node-remote")
        .expect("claim should succeed")
        .expect("expected first remote-target intent");
    assert_eq!(first_claim.1.identity, "bob");

    let second_claim = mailbox
        .claim_next_intent_for_node("node-remote")
        .expect("claim should succeed")
        .expect("expected second remote-target intent");
    assert_eq!(second_claim.1.identity, "charlie");

    assert!(
        mailbox
            .claim_next_intent_for_node("node-remote")
            .expect("claim should succeed")
            .is_none()
    );
}

#[test]
fn redis_relay_mailbox_concurrent_claim_consumes_each_intent_once() {
    let mailbox = Arc::new(RedisRelayMailbox::with_store(InMemoryHashStore::default()));

    let total = 64usize;
    for idx in 0..total {
        mailbox
            .dispatch_intent(&NonLocalRelayJoinIntent {
                room: "relay-room".to_string(),
                identity: format!("worker-{idx}"),
                name: "Worker".to_string(),
                requested_participant_sid: None,
                selected_room_node_id: "node-remote".to_string(),
                subscriber_primary: false,
                can_publish: true,
                can_subscribe: true,
                can_publish_data: true,
                can_update_metadata: false,
                hidden: false,
                metadata: String::new(),
                attributes: HashMap::new(),
                api_key: String::new(),
                kind: String::new(),
                kind_details: Vec::new(),
                destination_room: String::new(),
                room_config: None,
            })
            .expect("intent should store");
    }

    let claimed_identities = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let mailbox = mailbox.clone();
        let claimed = claimed_identities.clone();
        workers.push(std::thread::spawn(move || {
            loop {
                let next = mailbox
                    .claim_next_intent_for_node("node-remote")
                    .expect("claim should succeed");
                let Some((_, intent)) = next else {
                    break;
                };
                claimed
                    .lock()
                    .expect("claimed identities lock should not be poisoned")
                    .push(intent.identity);
            }
        }));
    }

    for worker in workers {
        worker
            .join()
            .expect("concurrent claim worker should complete");
    }

    let claimed = claimed_identities
        .lock()
        .expect("claimed identities lock should not be poisoned")
        .clone();

    let unique: std::collections::HashSet<_> = claimed.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        total,
        "concurrent consumers should observe every queued intent identity"
    );
    assert!(
        mailbox
            .claim_next_intent_for_node("node-remote")
            .expect("final claim should succeed")
            .is_none(),
        "intent queue should be drained after concurrent consumption"
    );
}

#[test]
fn redis_mailbox_dispatcher_returns_remote_response_after_driver_executes_intent() {
    let mailbox = RedisRelayMailbox::with_store(InMemoryHashStore::default());
    let dispatcher = RedisMailboxRelayDispatcher::with_mailbox_and_driver(
        mailbox,
        Arc::new(AcceptingMailboxDriver),
    );

    let response = dispatcher
        .dispatch_non_local_join(NonLocalRelayJoinIntent {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            name: "Alice".to_string(),
            requested_participant_sid: Some("PA_reconnect".to_string()),
            selected_room_node_id: "node-remote".to_string(),
            subscriber_primary: false,
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_update_metadata: false,
            hidden: false,
            metadata: String::new(),
            attributes: HashMap::new(),
            api_key: String::new(),
            kind: String::new(),
            kind_details: Vec::new(),
            destination_room: String::new(),
            room_config: None,
        })
        .expect("dispatch should succeed");

    assert_eq!(
        response,
        Some(NonLocalRelayJoinResponse::Accepted {
            participant_sid: "PA_remote_mailbox".to_string(),
            server_version: "relay-mailbox".to_string(),
            ping_interval: 6,
            ping_timeout: 12,
        })
    );
}

#[test]
fn redis_mailbox_dispatcher_retries_transient_dispatch_failure_and_succeeds() {
    let store = FlakyHsetHashStore {
        inner: InMemoryHashStore::default(),
        remaining_hset_failures: Arc::new(std::sync::atomic::AtomicU8::new(1)),
    };
    let mailbox = RedisRelayMailbox::with_store(store);
    let dispatcher = RedisMailboxRelayDispatcher::with_mailbox_and_policy(
        mailbox,
        Arc::new(AcceptingFlakyMailboxDriver),
        Duration::ZERO,
        Duration::from_millis(100),
        2,
        Duration::from_millis(1),
        None,
    );

    let response = dispatcher
        .dispatch_non_local_join(NonLocalRelayJoinIntent {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            name: "Alice".to_string(),
            requested_participant_sid: Some("PA_reconnect".to_string()),
            selected_room_node_id: "node-remote".to_string(),
            subscriber_primary: false,
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_update_metadata: false,
            hidden: false,
            metadata: String::new(),
            attributes: HashMap::new(),
            api_key: String::new(),
            kind: String::new(),
            kind_details: Vec::new(),
            destination_room: String::new(),
            room_config: None,
        })
        .expect("dispatch should succeed after one retry");

    assert!(matches!(
        response,
        Some(NonLocalRelayJoinResponse::Accepted { .. })
    ));
}

#[test]
fn redis_mailbox_dispatcher_applies_backpressure_limit_before_dispatch() {
    let mailbox = RedisRelayMailbox::with_store(InMemoryHashStore::default());
    mailbox
        .dispatch_intent(&NonLocalRelayJoinIntent {
            room: "relay-room".to_string(),
            identity: "seed".to_string(),
            name: "Seed".to_string(),
            requested_participant_sid: None,
            selected_room_node_id: "node-remote".to_string(),
            subscriber_primary: false,
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_update_metadata: false,
            hidden: false,
            metadata: String::new(),
            attributes: HashMap::new(),
            api_key: String::new(),
            kind: String::new(),
            kind_details: Vec::new(),
            destination_room: String::new(),
            room_config: None,
        })
        .expect("seed pending intent should store");

    let dispatcher = RedisMailboxRelayDispatcher::with_mailbox_and_policy(
        mailbox,
        Arc::new(AcceptingMailboxDriver),
        Duration::ZERO,
        Duration::from_millis(1),
        0,
        Duration::ZERO,
        Some(1),
    );

    let error = dispatcher
        .dispatch_non_local_join(NonLocalRelayJoinIntent {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            name: "Alice".to_string(),
            requested_participant_sid: None,
            selected_room_node_id: "node-remote".to_string(),
            subscriber_primary: false,
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_update_metadata: false,
            hidden: false,
            metadata: String::new(),
            attributes: HashMap::new(),
            api_key: String::new(),
            kind: String::new(),
            kind_details: Vec::new(),
            destination_room: String::new(),
            room_config: None,
        })
        .expect_err("dispatch should fail when mailbox is backpressured");

    assert!(error.contains("backpressure"));
}

#[test]
fn redis_relay_mailbox_claims_persistent_outbound_signal_responses_for_session() {
    let mailbox = RedisRelayMailbox::with_store(InMemoryHashStore::default());
    let room = "relay-outbound-room";
    let identity = "alice";
    let node = "node-remote";

    mailbox
        .store_outbound_signal_response(room, identity, node, b"first".to_vec())
        .expect("first outbound response should store");
    mailbox
        .store_outbound_signal_response(room, "bob", node, b"wrong-identity".to_vec())
        .expect("unrelated outbound response should store");
    mailbox
        .store_outbound_signal_response(room, identity, node, b"second".to_vec())
        .expect("second outbound response should store");

    let drained = mailbox
        .claim_outbound_signal_responses(&NonLocalRelayOutboundSignalQuery {
            room: room.to_string(),
            identity: identity.to_string(),
            selected_room_node_id: node.to_string(),
            max_events: 8,
        })
        .expect("outbound responses should drain");
    assert_eq!(drained, vec![b"first".to_vec(), b"second".to_vec()]);

    let remaining = mailbox
        .claim_outbound_signal_responses(&NonLocalRelayOutboundSignalQuery {
            room: room.to_string(),
            identity: "bob".to_string(),
            selected_room_node_id: node.to_string(),
            max_events: 8,
        })
        .expect("unrelated outbound response should remain");
    assert_eq!(remaining, vec![b"wrong-identity".to_vec()]);
}

#[test]
fn redis_mailbox_dispatcher_drains_persistent_outbound_signal_responses() {
    let mailbox = RedisRelayMailbox::with_store(InMemoryHashStore::default());
    mailbox
        .store_outbound_signal_response("relay-room", "alice", "node-remote", b"event".to_vec())
        .expect("outbound response should store");

    let dispatcher = RedisMailboxRelayDispatcher::with_mailbox(mailbox);
    let drained = dispatcher
        .drain_non_local_outbound_signal_responses(NonLocalRelayOutboundSignalQuery {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            selected_room_node_id: "node-remote".to_string(),
            max_events: 8,
        })
        .expect("dispatcher should drain outbound responses");
    assert_eq!(drained, vec![b"event".to_vec()]);
}

#[test]
fn redis_mailbox_dispatcher_dispatches_termination_and_driver_executes_intent() {
    let mailbox = RedisRelayMailbox::with_store(InMemoryHashStore::default());
    let driver = Arc::new(RecordingTerminationMailboxDriver::default());
    let recorded = driver.seen.clone();
    let dispatcher = RedisMailboxRelayDispatcher::with_mailbox_and_driver(mailbox, driver);

    dispatcher
        .dispatch_non_local_termination(NonLocalRelaySessionTerminationIntent {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            participant_sid: "PA_alice".to_string(),
            selected_room_node_id: "node-remote".to_string(),
        })
        .expect("termination dispatch should succeed");

    let seen = recorded
        .lock()
        .expect("termination recording lock should not be poisoned")
        .clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].room, "relay-room");
    assert_eq!(seen[0].identity, "alice");
    assert_eq!(seen[0].selected_room_node_id, "node-remote");
}

#[test]
fn dispatch_non_local_relay_intent_records_target_in_non_strict_mode() {
    let dispatcher = Arc::new(RecordingRelayDispatcher::default());
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let state = state_with_room_nodes_placement_and_relay_dispatcher(
        room_nodes,
        Some("node-local".to_string()),
        false,
        dispatcher.clone(),
    );
    let outcome = room_node_placement_outcome(&state, "relay-dispatch-room");

    let auth = AuthContext {
        api_key: API_KEY.to_string(),
        claims: Claims {
            sub: "alice".to_string(),
            name: "Alice".to_string(),
            ..Default::default()
        },
    };
    let request = proto::JoinRequest {
        participant_sid: "PA_reconnect".to_string(),
        ..Default::default()
    };

    dispatch_non_local_relay_intent(
        &state,
        "relay-dispatch-room",
        &auth,
        &request,
        &outcome,
        false,
    );

    let intents = dispatcher.take();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].room, "relay-dispatch-room");
    assert_eq!(intents[0].selected_room_node_id, "node-remote");
}

#[test]
fn dispatch_non_local_relay_intent_is_skipped_in_strict_mode() {
    let dispatcher = Arc::new(RecordingRelayDispatcher::default());
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let state = state_with_room_nodes_placement_and_relay_dispatcher(
        room_nodes,
        Some("node-local".to_string()),
        true,
        dispatcher.clone(),
    );
    let outcome = room_node_placement_outcome(&state, "relay-dispatch-strict-room");

    let auth = AuthContext {
        api_key: API_KEY.to_string(),
        claims: Claims {
            sub: "alice".to_string(),
            name: "Alice".to_string(),
            ..Default::default()
        },
    };
    let request = proto::JoinRequest {
        participant_sid: "PA_reconnect".to_string(),
        ..Default::default()
    };

    dispatch_non_local_relay_intent(
        &state,
        "relay-dispatch-strict-room",
        &auth,
        &request,
        &outcome,
        false,
    );

    assert!(dispatcher.take().is_empty());
}

#[tokio::test]
async fn rtc_v1_websocket_uses_remote_relay_accept_response_when_available() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_placement_and_relay_dispatcher(
                room_nodes,
                Some("node-local".to_string()),
                false,
                Arc::new(AcceptingRelayDispatcher),
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let first = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");
    let Message::Binary(bytes) = first else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    assert_eq!(
        join.participant.expect("participant should be present").sid,
        "PA_remote"
    );
    assert_eq!(join.server_version, "relay-proxy");
    assert_eq!(join.ping_interval, 9);
    assert_eq!(join.ping_timeout, 19);

    let ping_timestamp = 42_i64;
    let ping = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: ping_timestamp,
            ..Default::default()
        })),
    };
    socket
        .send(Message::Binary(ping.encode_to_vec().into()))
        .await
        .expect("ping request should send over relayed websocket");

    let pong_message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("socket should remain open after relayed join")
        .expect("pong message should arrive")
        .expect("pong message should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong_response =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong should decode");
    let Some(proto::signal_response::Message::PongResp(pong)) = pong_response.message else {
        panic!("expected pong response message");
    };
    assert_eq!(pong.last_ping_timestamp, ping_timestamp);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_relayed_session_forwards_remote_outbound_signal_responses() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_placement_and_relay_dispatcher(
                room_nodes,
                Some("node-local".to_string()),
                false,
                Arc::new(RelaySignalBackhaulDispatcher),
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join response should arrive")
        .expect("join response should be ok");

    let track_setting = proto::SignalRequest {
        message: Some(proto::signal_request::Message::TrackSetting(
            proto::UpdateTrackSettings {
                track_sids: vec!["TR_remote_backhaul".to_string()],
                disabled: true,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(track_setting.encode_to_vec().into()))
        .await
        .expect("track setting should send over relayed websocket");

    let first = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("direct relay response should arrive")
        .expect("direct relay response should be present")
        .expect("direct relay response should be ok");
    let Message::Binary(first_bytes) = first else {
        panic!("expected direct binary response");
    };
    let first_response =
        proto::SignalResponse::decode(first_bytes.as_ref()).expect("direct response should decode");
    assert!(matches!(
        first_response.message,
        Some(proto::signal_response::Message::Pong(1234))
    ));

    let second = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("outbound relay response should arrive")
        .expect("outbound relay response should be present")
        .expect("outbound relay response should be ok");
    let Message::Binary(second_bytes) = second else {
        panic!("expected outbound binary response");
    };
    let second_response = proto::SignalResponse::decode(second_bytes.as_ref())
        .expect("outbound response should decode");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) =
        second_response.message
    else {
        panic!("expected subscribed quality update from relayed outbound backhaul");
    };
    assert_eq!(update.track_sid, "TR_remote_backhaul");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_relayed_session_polls_and_forwards_persistent_outbound_signal_responses() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_placement_and_relay_dispatcher(
                room_nodes,
                Some("node-local".to_string()),
                false,
                Arc::new(PersistentOutboundRelayDispatcher::default()),
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join response should arrive")
        .expect("join response should be ok");

    let track_setting = proto::SignalRequest {
        message: Some(proto::signal_request::Message::TrackSetting(
            proto::UpdateTrackSettings {
                track_sids: vec!["TR_remote_persistent".to_string()],
                disabled: true,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(track_setting.encode_to_vec().into()))
        .await
        .expect("track setting should send over relayed websocket");

    let outbound = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("persistent outbound relay response should arrive")
        .expect("persistent outbound relay response should be present")
        .expect("persistent outbound relay response should be ok");
    let Message::Binary(outbound_bytes) = outbound else {
        panic!("expected outbound binary response");
    };
    let outbound_response = proto::SignalResponse::decode(outbound_bytes.as_ref())
        .expect("outbound response should decode");
    let Some(proto::signal_response::Message::SubscribedQualityUpdate(update)) =
        outbound_response.message
    else {
        panic!("expected subscribed quality update from persistent outbound poll");
    };
    assert_eq!(update.track_sid, "TR_remote_persistent");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_relayed_session_leave_dispatches_remote_termination_intent() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let dispatcher = Arc::new(AcceptingAndRecordingTerminationDispatcher::default());
    let dispatcher_for_state = dispatcher.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_placement_and_relay_dispatcher(
                room_nodes,
                Some("node-local".to_string()),
                false,
                dispatcher_for_state,
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let first = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");
    let Message::Binary(bytes) = first else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    assert!(
        matches!(
            response.message,
            Some(proto::signal_response::Message::Join(_))
        ),
        "expected join response"
    );

    let leave = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Leave(
            proto::LeaveRequest::default(),
        )),
    };
    socket
        .send(Message::Binary(leave.encode_to_vec().into()))
        .await
        .expect("leave should send");

    let _ = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("socket should close after leave");

    let terminations = dispatcher.take_terminations();
    assert_eq!(terminations.len(), 1);
    assert_eq!(terminations[0].room, "test-room");
    assert_eq!(terminations[0].identity, "alice");
    assert_eq!(terminations[0].selected_room_node_id, "node-remote");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_relayed_stale_socket_close_does_not_dispatch_remote_termination() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let dispatcher = Arc::new(AcceptingAndRecordingTerminationDispatcher::default());
    let dispatcher_for_state = dispatcher.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_placement_and_relay_dispatcher(
                room_nodes,
                Some("node-local".to_string()),
                false,
                dispatcher_for_state,
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("first request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first join response should arrive")
        .expect("first join response should be ok");

    let mut second_request = url
        .into_client_request()
        .expect("second request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect");
    let _second_join = second_socket
        .next()
        .await
        .expect("second join response should arrive")
        .expect("second join response should be ok");

    let _ = first_socket.send(Message::Close(None)).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), first_socket.next()).await;

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        dispatcher.take_terminations().is_empty(),
        "stale relayed socket close should not dispatch remote termination"
    );

    let _ = second_socket.send(Message::Close(None)).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), second_socket.next()).await;

    assert!(
        dispatcher.take_terminations().is_empty(),
        "non-leave relayed socket closes should not dispatch remote termination"
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_relayed_close_then_fast_reconnect_skips_old_remote_termination() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let dispatcher = Arc::new(AcceptingAndRecordingTerminationDispatcher::default());
    let dispatcher_for_state = dispatcher.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_placement_and_relay_dispatcher(
                room_nodes,
                Some("node-local".to_string()),
                false,
                dispatcher_for_state,
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("first request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first join response should arrive")
        .expect("first join response should be ok");

    let _ = first_socket.send(Message::Close(None)).await;

    let mut second_request = url
        .into_client_request()
        .expect("second request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect");
    let _second_join = second_socket
        .next()
        .await
        .expect("second join response should arrive")
        .expect("second join response should be ok");

    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        dispatcher.take_terminations().is_empty(),
        "old relayed socket should not dispatch remote termination when fast reconnect establishes a new active session"
    );

    let _ = tokio::time::timeout(Duration::from_secs(2), first_socket.next()).await;

    let _ = second_socket.send(Message::Close(None)).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), second_socket.next()).await;

    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        dispatcher.take_terminations().is_empty(),
        "non-leave relayed reconnect churn should not dispatch remote termination intents"
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_closes_when_remote_relay_rejects() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_placement_and_relay_dispatcher(
                room_nodes,
                Some("node-local".to_string()),
                false,
                Arc::new(RejectingRelayDispatcher),
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let first = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("socket should close promptly on relay rejection");
    assert!(
        matches!(first, None | Some(Ok(Message::Close(_))) | Some(Err(_))),
        "relay rejection should close socket without local join response"
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_falls_back_to_local_join_when_mailbox_dispatch_unavailable() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let dispatcher = Arc::new(RedisMailboxRelayDispatcher::with_mailbox(
        RedisRelayMailbox::with_store(FailingHashStore),
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_placement_and_relay_dispatcher(
                room_nodes,
                Some("node-local".to_string()),
                false,
                dispatcher,
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let first = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");
    let Message::Binary(bytes) = first else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected local fallback join response");
    };

    assert_eq!(join.room.expect("room should exist").name, "test-room");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_rejects_non_local_room_placement_when_strict() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-remote".to_string(),
            region: "remote-region".to_string(),
        })
        .expect("remote node should register");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_and_placement(
                room_nodes,
                Some("node-local".to_string()),
                true,
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");

    let first = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("socket should close or respond promptly");

    match first {
        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {}
        Some(Ok(Message::Binary(bytes))) => {
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("signal response should decode");
            assert!(
                !matches!(
                    response.message,
                    Some(proto::signal_response::Message::Join(_))
                ),
                "strict non-local placement should not emit join response"
            );
        }
        Some(Ok(other)) => panic!("unexpected websocket message: {other:?}"),
    }

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_allows_local_room_placement_when_strict() {
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-local".to_string(),
            region: "local-region".to_string(),
        })
        .expect("local node should register");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state_with_room_nodes_and_placement(
                room_nodes,
                Some("node-local".to_string()),
                true,
            )),
        )
        .await
        .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    assert_eq!(join.room.expect("room should exist").name, "test-room");

    server.abort();
}

#[tokio::test]
async fn rtc_v0_and_rtc_v1_join_responses_are_compatible_for_shared_fields() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let v1_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut v1_request = v1_url.into_client_request().expect("request should build");
    v1_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );
    let (mut v1_socket, _) = connect_async(v1_request)
        .await
        .expect("v1 websocket should connect");
    let v1_message = v1_socket
        .next()
        .await
        .expect("v1 join should arrive")
        .expect("v1 join should be ok");
    let Message::Binary(v1_bytes) = v1_message else {
        panic!("expected v1 binary join response");
    };
    let v1_response =
        proto::SignalResponse::decode(v1_bytes.as_ref()).expect("v1 response should decode");
    let Some(proto::signal_response::Message::Join(v1_join)) = v1_response.message else {
        panic!("expected v1 join response");
    };

    let v0_request = format!("ws://{addr}/rtc?access_token={}", token("test-room"))
        .into_client_request()
        .expect("request should build");
    let (mut v0_socket, _) = connect_async(v0_request)
        .await
        .expect("v0 websocket should connect");
    let v0_message = v0_socket
        .next()
        .await
        .expect("v0 join should arrive")
        .expect("v0 join should be ok");
    let Message::Binary(v0_bytes) = v0_message else {
        panic!("expected v0 binary join response");
    };
    let v0_response =
        proto::SignalResponse::decode(v0_bytes.as_ref()).expect("v0 response should decode");
    let Some(proto::signal_response::Message::Join(v0_join)) = v0_response.message else {
        panic!("expected v0 join response");
    };

    let v1_room = v1_join.room.expect("v1 room should exist");
    let v0_room = v0_join.room.expect("v0 room should exist");
    assert_eq!(v1_room.name, v0_room.name);

    let v1_participant = v1_join.participant.expect("v1 participant should exist");
    let v0_participant = v0_join.participant.expect("v0 participant should exist");
    assert_eq!(v1_participant.identity, v0_participant.identity);
    assert_eq!(v1_participant.name, v0_participant.name);
    assert_eq!(v1_participant.kind, v0_participant.kind);

    assert_eq!(v1_join.ping_interval, v0_join.ping_interval);
    assert_eq!(v1_join.ping_timeout, v0_join.ping_timeout);
    assert!(!v1_join.ice_servers.is_empty());
    assert!(!v0_join.ice_servers.is_empty());

    server.abort();
}

#[tokio::test]
async fn rtc_v0_websocket_sends_join_response_without_join_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc?access_token={}", token("test-room"));
    let request = url.into_client_request().expect("request should build");

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    assert_eq!(join.room.expect("room should exist").name, "test-room");
    let participant = join.participant.expect("participant should exist");
    assert_eq!(participant.identity, "alice");
    assert_eq!(participant.name, "Alice");
    assert_eq!(participant.metadata, "");
    assert!(join.subscriber_primary);

    server.abort();
}

#[tokio::test]
async fn rtc_v0_websocket_without_subscribe_permission_disables_subscriber_primary() {
    let state = state();
    let state_for_server = state.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state_for_server))
            .await
            .expect("test server should run");
    });

    let token = token_for_without_subscribe("test-room", "alice", "Alice");
    let url = format!("ws://{addr}/rtc?access_token={token}");
    let request = url.into_client_request().expect("request should build");

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    assert!(!join.subscriber_primary);
    assert!(
        state.participant_uses_subscriber_primary("test-room", "alice"),
        "a denied join must retain the requested v0 dual-PC topology for a later permission grant"
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v0_websocket_join_request_disables_subscriber_primary() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let message = socket
        .next()
        .await
        .expect("first websocket message should arrive")
        .expect("first websocket message should be ok");

    let Message::Binary(bytes) = message else {
        panic!("expected binary protobuf signal response");
    };
    let response =
        proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    assert!(!join.subscriber_primary);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_responds_to_ping_req() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let ping_timestamp = 1234;
    let request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: ping_timestamp,
            rtt: 7,
        })),
    };
    socket
        .send(Message::Binary(request.encode_to_vec().into()))
        .await
        .expect("ping request should send");

    let pong_message = socket
        .next()
        .await
        .expect("pong should arrive")
        .expect("pong should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    let Some(proto::signal_response::Message::PongResp(pong)) = pong.message else {
        panic!("expected pong_resp response");
    };
    assert_eq!(pong.last_ping_timestamp, ping_timestamp);
    assert!(pong.timestamp >= ping_timestamp);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_accepts_json_text_ping_req() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let ping_timestamp = 5678;
    let ping_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: ping_timestamp,
            rtt: 3,
        })),
    };
    let ping_json = serde_json::to_string(&ping_request).expect("ping request should serialize");
    socket
        .send(Message::Text(ping_json.into()))
        .await
        .expect("json ping request should send");

    let pong_message = socket
        .next()
        .await
        .expect("pong should arrive")
        .expect("pong should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    let Some(proto::signal_response::Message::PongResp(pong)) = pong.message else {
        panic!("expected pong_resp response");
    };
    assert_eq!(pong.last_ping_timestamp, ping_timestamp);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_responds_to_legacy_ping() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Ping(1234)),
    };
    socket
        .send(Message::Binary(request.encode_to_vec().into()))
        .await
        .expect("legacy ping should send");

    let pong_message = socket
        .next()
        .await
        .expect("pong should arrive")
        .expect("pong should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    let Some(proto::signal_response::Message::Pong(timestamp)) = pong.message else {
        panic!("expected legacy pong response");
    };
    assert!(timestamp > 0);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_update_data_subscription_with_can_subscribe_returns_handles() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let publisher_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut publisher_request = publisher_url
        .into_client_request()
        .expect("publisher request should build");
    publisher_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "publisher", "Publisher")
        ))
        .expect("publisher auth header should parse"),
    );

    let (mut publisher_socket, _) = connect_async(publisher_request)
        .await
        .expect("publisher websocket should connect");
    let _publisher_join = publisher_socket
        .next()
        .await
        .expect("publisher join should arrive")
        .expect("publisher join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 61,
                name: "allowed-telemetry".to_string(),
                ..Default::default()
            },
        )),
    };
    publisher_socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let published_track = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = publisher_socket
                .next()
                .await
                .expect("publish response should arrive")
                .expect("publish response should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(response)) =
                response.message
            {
                break response.info.expect("published info should be present");
            }
        }
    })
    .await
    .expect("publish response should arrive before timeout");

    let subscriber_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut subscriber_request = subscriber_url
        .into_client_request()
        .expect("subscriber request should build");
    subscriber_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions("test-room", "subscriber", "Subscriber", false, true)
        ))
        .expect("subscriber auth header should parse"),
    );

    let (mut subscriber_socket, _) = connect_async(subscriber_request)
        .await
        .expect("subscriber websocket should connect");
    let _subscriber_join = subscriber_socket
        .next()
        .await
        .expect("subscriber join should arrive")
        .expect("subscriber join should be ok");

    let subscribe_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::UpdateDataSubscription(
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published_track.sid.clone(),
                    subscribe: true,
                    options: Some(proto::DataTrackSubscriptionOptions {
                        target_fps: Some(24),
                    }),
                }],
            },
        )),
    };
    subscriber_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("update data subscription request should send");

    let handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = subscriber_socket
                .next()
                .await
                .expect("data-track handles should arrive")
                .expect("data-track handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("data-track handles should arrive before timeout");

    assert_eq!(handles.sub_handles.len(), 1);
    let published_mapping = handles
        .sub_handles
        .values()
        .next()
        .expect("subscriber handle mapping should exist");
    assert_eq!(published_mapping.track_sid, published_track.sid);
    assert_eq!(published_mapping.publisher_identity, "publisher");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_update_data_subscription_reuses_handle_on_option_update() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let publisher_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut publisher_request = publisher_url
        .into_client_request()
        .expect("publisher request should build");
    publisher_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "publisher2", "Publisher2")
        ))
        .expect("publisher auth header should parse"),
    );

    let (mut publisher_socket, _) = connect_async(publisher_request)
        .await
        .expect("publisher websocket should connect");
    let _ = publisher_socket
        .next()
        .await
        .expect("publisher join should arrive")
        .expect("publisher join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 71,
                name: "optioned-telemetry".to_string(),
                ..Default::default()
            },
        )),
    };
    publisher_socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let published_track = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = publisher_socket
                .next()
                .await
                .expect("publish response should arrive")
                .expect("publish response should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(response)) =
                response.message
            {
                break response.info.expect("published info should be present");
            }
        }
    })
    .await
    .expect("publish response should arrive before timeout");

    let subscriber_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut subscriber_request = subscriber_url
        .into_client_request()
        .expect("subscriber request should build");
    subscriber_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions("test-room", "subscriber2", "Subscriber2", false, true)
        ))
        .expect("subscriber auth header should parse"),
    );

    let (mut subscriber_socket, _) = connect_async(subscriber_request)
        .await
        .expect("subscriber websocket should connect");
    let _ = subscriber_socket
        .next()
        .await
        .expect("subscriber join should arrive")
        .expect("subscriber join should be ok");

    let make_request = |target_fps: u32| proto::SignalRequest {
        message: Some(proto::signal_request::Message::UpdateDataSubscription(
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published_track.sid.clone(),
                    subscribe: true,
                    options: Some(proto::DataTrackSubscriptionOptions {
                        target_fps: Some(target_fps),
                    }),
                }],
            },
        )),
    };

    subscriber_socket
        .send(Message::Binary(make_request(12).encode_to_vec().into()))
        .await
        .expect("initial update data subscription request should send");

    let first_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = subscriber_socket
                .next()
                .await
                .expect("initial handles should arrive")
                .expect("initial handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("initial handles should arrive before timeout");

    let first_handle = *first_handles
        .sub_handles
        .keys()
        .next()
        .expect("initial subscriber handle should exist");

    subscriber_socket
        .send(Message::Binary(make_request(30).encode_to_vec().into()))
        .await
        .expect("second update data subscription request should send");

    let second_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = subscriber_socket
                .next()
                .await
                .expect("second handles should arrive")
                .expect("second handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("second handles should arrive before timeout");

    assert_eq!(second_handles.sub_handles.len(), 1);
    assert!(second_handles.sub_handles.contains_key(&first_handle));

    server.abort();
}

#[tokio::test]
async fn rtc_v1_update_data_subscription_without_can_subscribe_returns_empty_handles() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let publisher_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut publisher_request = publisher_url
        .into_client_request()
        .expect("publisher request should build");
    publisher_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "publisher", "Publisher")
        ))
        .expect("publisher auth header should parse"),
    );

    let (mut publisher_socket, _) = connect_async(publisher_request)
        .await
        .expect("publisher websocket should connect");
    let _publisher_join = publisher_socket
        .next()
        .await
        .expect("publisher join should arrive")
        .expect("publisher join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 51,
                name: "restricted-telemetry".to_string(),
                ..Default::default()
            },
        )),
    };
    publisher_socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let published_track = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = publisher_socket
                .next()
                .await
                .expect("publish response should arrive")
                .expect("publish response should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(response)) =
                response.message
            {
                break response.info.expect("published info should be present");
            }
        }
    })
    .await
    .expect("publish response should arrive before timeout");

    let subscriber_url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut subscriber_request = subscriber_url
        .into_client_request()
        .expect("subscriber request should build");
    subscriber_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_without_subscribe("test-room", "subscriber", "Subscriber")
        ))
        .expect("subscriber auth header should parse"),
    );

    let (mut subscriber_socket, _) = connect_async(subscriber_request)
        .await
        .expect("subscriber websocket should connect");
    let _subscriber_join = subscriber_socket
        .next()
        .await
        .expect("subscriber join should arrive")
        .expect("subscriber join should be ok");

    let subscribe_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::UpdateDataSubscription(
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published_track.sid,
                    subscribe: true,
                    options: Some(proto::DataTrackSubscriptionOptions {
                        target_fps: Some(24),
                    }),
                }],
            },
        )),
    };
    subscriber_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("update data subscription request should send");

    let handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = subscriber_socket
                .next()
                .await
                .expect("data-track handles should arrive")
                .expect("data-track handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("data-track handles should arrive before timeout");

    assert!(handles.sub_handles.is_empty());

    server.abort();
}

#[tokio::test]
async fn rtc_v1_disconnect_and_rejoin_can_resubscribe_data_track() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut publisher_request = url
        .clone()
        .into_client_request()
        .expect("publisher request should build");
    publisher_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data(
                "test-room",
                "publisher-disconnect",
                "Publisher Disconnect"
            )
        ))
        .expect("publisher auth header should parse"),
    );
    let (mut publisher_socket, _) = connect_async(publisher_request)
        .await
        .expect("publisher websocket should connect");
    let _ = publisher_socket
        .next()
        .await
        .expect("publisher join should arrive")
        .expect("publisher join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 83,
                name: "disconnect-track".to_string(),
                ..Default::default()
            },
        )),
    };
    publisher_socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let published_track = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = publisher_socket
                .next()
                .await
                .expect("publish response should arrive")
                .expect("publish response should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(response)) =
                response.message
            {
                break response.info.expect("published info should be present");
            }
        }
    })
    .await
    .expect("publish response should arrive before timeout");

    let mut subscriber_request = url
        .clone()
        .into_client_request()
        .expect("subscriber request should build");
    subscriber_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions(
                "test-room",
                "disconnect-rejoiner",
                "Disconnect Rejoiner",
                false,
                true
            )
        ))
        .expect("subscriber auth header should parse"),
    );

    let (mut subscriber_socket, _) = connect_async(subscriber_request)
        .await
        .expect("subscriber websocket should connect");
    let _ = subscriber_socket
        .next()
        .await
        .expect("subscriber join should arrive")
        .expect("subscriber join should be ok");

    let subscribe_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::UpdateDataSubscription(
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published_track.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
        )),
    };
    subscriber_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("first subscribe request should send");

    let first_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = subscriber_socket
                .next()
                .await
                .expect("first handles should arrive")
                .expect("first handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("first signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("first handles should arrive before timeout");
    assert_eq!(first_handles.sub_handles.len(), 1);

    let _ = subscriber_socket.close(None).await;

    let mut subscriber_rejoin_request = url
        .into_client_request()
        .expect("subscriber rejoin request should build");
    subscriber_rejoin_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions(
                "test-room",
                "disconnect-rejoiner",
                "Disconnect Rejoiner",
                false,
                true
            )
        ))
        .expect("subscriber rejoin auth header should parse"),
    );

    let (mut subscriber_rejoin_socket, _) = connect_async(subscriber_rejoin_request)
        .await
        .expect("subscriber rejoin websocket should connect");
    let _ = subscriber_rejoin_socket
        .next()
        .await
        .expect("subscriber rejoin join should arrive")
        .expect("subscriber rejoin join should be ok");

    subscriber_rejoin_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("second subscribe request should send");

    let second_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = subscriber_rejoin_socket
                .next()
                .await
                .expect("second handles should arrive")
                .expect("second handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("second signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("second handles should arrive before timeout");
    assert_eq!(second_handles.sub_handles.len(), 1);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_disconnect_clears_subscribe_permission_override() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut publisher_request = url
        .clone()
        .into_client_request()
        .expect("publisher request should build");
    publisher_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data(
                "test-room",
                "publisher-disconnect-perm",
                "Publisher Disconnect Perm"
            )
        ))
        .expect("publisher auth header should parse"),
    );
    let (mut publisher_socket, _) = connect_async(publisher_request)
        .await
        .expect("publisher websocket should connect");
    let _ = publisher_socket
        .next()
        .await
        .expect("publisher join should arrive")
        .expect("publisher join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 84,
                name: "disconnect-permission-track".to_string(),
                ..Default::default()
            },
        )),
    };
    publisher_socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let published_track = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = publisher_socket
                .next()
                .await
                .expect("publish response should arrive")
                .expect("publish response should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(response)) =
                response.message
            {
                break response.info.expect("published info should be present");
            }
        }
    })
    .await
    .expect("publish response should arrive before timeout");

    let subscribe_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::UpdateDataSubscription(
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published_track.sid,
                    subscribe: true,
                    options: None,
                }],
            },
        )),
    };

    let mut denied_request = url
        .clone()
        .into_client_request()
        .expect("denied request should build");
    denied_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_without_subscribe(
                "test-room",
                "disconnect-perm-rejoiner",
                "Disconnect Perm Rejoiner"
            )
        ))
        .expect("denied auth header should parse"),
    );
    let (mut denied_socket, _) = connect_async(denied_request)
        .await
        .expect("denied websocket should connect");
    let _ = denied_socket
        .next()
        .await
        .expect("denied join should arrive")
        .expect("denied join should be ok");

    denied_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("denied subscribe request should send");

    let denied_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = denied_socket
                .next()
                .await
                .expect("denied handles should arrive")
                .expect("denied handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("denied signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("denied handles should arrive before timeout");
    assert!(denied_handles.sub_handles.is_empty());

    let _ = denied_socket.close(None).await;

    let mut allowed_request = url
        .into_client_request()
        .expect("allowed request should build");
    allowed_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions(
                "test-room",
                "disconnect-perm-rejoiner",
                "Disconnect Perm Rejoiner",
                false,
                true
            )
        ))
        .expect("allowed auth header should parse"),
    );
    let (mut allowed_socket, _) = connect_async(allowed_request)
        .await
        .expect("allowed websocket should connect");
    let _ = allowed_socket
        .next()
        .await
        .expect("allowed join should arrive")
        .expect("allowed join should be ok");

    allowed_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("allowed subscribe request should send");

    let allowed_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = allowed_socket
                .next()
                .await
                .expect("allowed handles should arrive")
                .expect("allowed handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("allowed signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("allowed handles should arrive before timeout");
    assert_eq!(allowed_handles.sub_handles.len(), 1);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_leave_and_rejoin_can_resubscribe_data_track() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut publisher_request = url
        .clone()
        .into_client_request()
        .expect("publisher request should build");
    publisher_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "publisher-rejoin", "Publisher Rejoin")
        ))
        .expect("publisher auth header should parse"),
    );
    let (mut publisher_socket, _) = connect_async(publisher_request)
        .await
        .expect("publisher websocket should connect");
    let _ = publisher_socket
        .next()
        .await
        .expect("publisher join should arrive")
        .expect("publisher join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 81,
                name: "rejoin-track".to_string(),
                ..Default::default()
            },
        )),
    };
    publisher_socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let published_track = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = publisher_socket
                .next()
                .await
                .expect("publish response should arrive")
                .expect("publish response should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(response)) =
                response.message
            {
                break response.info.expect("published info should be present");
            }
        }
    })
    .await
    .expect("publish response should arrive before timeout");

    let mut subscriber_request = url
        .clone()
        .into_client_request()
        .expect("subscriber request should build");
    subscriber_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions("test-room", "rejoiner", "Rejoiner", false, true)
        ))
        .expect("subscriber auth header should parse"),
    );

    let (mut subscriber_socket, _) = connect_async(subscriber_request)
        .await
        .expect("subscriber websocket should connect");
    let _ = subscriber_socket
        .next()
        .await
        .expect("subscriber join should arrive")
        .expect("subscriber join should be ok");

    let subscribe_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::UpdateDataSubscription(
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published_track.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
        )),
    };
    subscriber_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("first subscribe request should send");

    let first_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = subscriber_socket
                .next()
                .await
                .expect("first handles should arrive")
                .expect("first handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("first signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("first handles should arrive before timeout");
    assert_eq!(first_handles.sub_handles.len(), 1);

    let leave = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Leave(
            proto::LeaveRequest::default(),
        )),
    };
    subscriber_socket
        .send(Message::Binary(leave.encode_to_vec().into()))
        .await
        .expect("leave request should send");
    let _ = subscriber_socket.close(None).await;

    let mut subscriber_rejoin_request = url
        .into_client_request()
        .expect("subscriber rejoin request should build");
    subscriber_rejoin_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions("test-room", "rejoiner", "Rejoiner", false, true)
        ))
        .expect("subscriber rejoin auth header should parse"),
    );

    let (mut subscriber_rejoin_socket, _) = connect_async(subscriber_rejoin_request)
        .await
        .expect("subscriber rejoin websocket should connect");
    let _ = subscriber_rejoin_socket
        .next()
        .await
        .expect("subscriber rejoin join should arrive")
        .expect("subscriber rejoin join should be ok");

    subscriber_rejoin_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("second subscribe request should send");

    let second_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = subscriber_rejoin_socket
                .next()
                .await
                .expect("second handles should arrive")
                .expect("second handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("second signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("second handles should arrive before timeout");
    assert_eq!(second_handles.sub_handles.len(), 1);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_leave_clears_subscribe_permission_override() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut publisher_request = url
        .clone()
        .into_client_request()
        .expect("publisher request should build");
    publisher_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "publisher-perm", "Publisher Perm")
        ))
        .expect("publisher auth header should parse"),
    );
    let (mut publisher_socket, _) = connect_async(publisher_request)
        .await
        .expect("publisher websocket should connect");
    let _ = publisher_socket
        .next()
        .await
        .expect("publisher join should arrive")
        .expect("publisher join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 82,
                name: "permission-track".to_string(),
                ..Default::default()
            },
        )),
    };
    publisher_socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let published_track = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = publisher_socket
                .next()
                .await
                .expect("publish response should arrive")
                .expect("publish response should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(response)) =
                response.message
            {
                break response.info.expect("published info should be present");
            }
        }
    })
    .await
    .expect("publish response should arrive before timeout");

    let subscribe_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::UpdateDataSubscription(
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published_track.sid,
                    subscribe: true,
                    options: None,
                }],
            },
        )),
    };

    let mut denied_request = url
        .clone()
        .into_client_request()
        .expect("denied request should build");
    denied_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_without_subscribe("test-room", "perm-rejoiner", "Perm Rejoiner")
        ))
        .expect("denied auth header should parse"),
    );
    let (mut denied_socket, _) = connect_async(denied_request)
        .await
        .expect("denied websocket should connect");
    let _ = denied_socket
        .next()
        .await
        .expect("denied join should arrive")
        .expect("denied join should be ok");

    denied_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("denied subscribe request should send");

    let denied_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = denied_socket
                .next()
                .await
                .expect("denied handles should arrive")
                .expect("denied handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("denied signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("denied handles should arrive before timeout");
    assert!(denied_handles.sub_handles.is_empty());

    let leave = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Leave(
            proto::LeaveRequest::default(),
        )),
    };
    denied_socket
        .send(Message::Binary(leave.encode_to_vec().into()))
        .await
        .expect("leave request should send");
    let _ = denied_socket.close(None).await;

    let mut allowed_request = url
        .into_client_request()
        .expect("allowed request should build");
    allowed_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions("test-room", "perm-rejoiner", "Perm Rejoiner", false, true)
        ))
        .expect("allowed auth header should parse"),
    );
    let (mut allowed_socket, _) = connect_async(allowed_request)
        .await
        .expect("allowed websocket should connect");
    let _ = allowed_socket
        .next()
        .await
        .expect("allowed join should arrive")
        .expect("allowed join should be ok");

    allowed_socket
        .send(Message::Binary(subscribe_request.encode_to_vec().into()))
        .await
        .expect("allowed subscribe request should send");

    let allowed_handles = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = allowed_socket
                .next()
                .await
                .expect("allowed handles should arrive")
                .expect("allowed handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("allowed signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("allowed handles should arrive before timeout");
    assert_eq!(allowed_handles.sub_handles.len(), 1);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_publish_data_track_requires_can_publish_data_permission() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions("test-room", "alice", "Alice", false, true)
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 23,
                name: "telemetry".to_string(),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let response_message = socket
        .next()
        .await
        .expect("request response should arrive")
        .expect("request response should be ok");
    let Message::Binary(response_bytes) = response_message else {
        panic!("expected binary request response");
    };
    let response = proto::SignalResponse::decode(response_bytes.as_ref())
        .expect("request response should decode");
    let Some(proto::signal_response::Message::RequestResponse(request_response)) = response.message
    else {
        panic!("expected request_response message");
    };
    assert_eq!(
        request_response.reason,
        proto::request_response::Reason::NotAllowed as i32
    );
    let Some(proto::request_response::Request::PublishDataTrack(request)) =
        request_response.request
    else {
        panic!("expected publish data track request in request_response");
    };
    assert_eq!(request.pub_handle, 23);
    assert_eq!(request.name, "telemetry");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_publish_data_track_rejects_empty_name() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 24,
                name: String::new(),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let response_message = socket
        .next()
        .await
        .expect("request response should arrive")
        .expect("request response should be ok");
    let Message::Binary(response_bytes) = response_message else {
        panic!("expected binary request response");
    };
    let response = proto::SignalResponse::decode(response_bytes.as_ref())
        .expect("request response should decode");
    let Some(proto::signal_response::Message::RequestResponse(request_response)) = response.message
    else {
        panic!("expected request_response message");
    };
    assert_eq!(
        request_response.reason,
        proto::request_response::Reason::InvalidName as i32
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_publish_data_track_rejects_invalid_handle() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 0,
                name: "invalid-handle".to_string(),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let response_message = socket
        .next()
        .await
        .expect("request response should arrive")
        .expect("request response should be ok");
    let Message::Binary(response_bytes) = response_message else {
        panic!("expected binary request response");
    };
    let response = proto::SignalResponse::decode(response_bytes.as_ref())
        .expect("request response should decode");
    let Some(proto::signal_response::Message::RequestResponse(request_response)) = response.message
    else {
        panic!("expected request_response message");
    };
    assert_eq!(
        request_response.reason,
        proto::request_response::Reason::InvalidHandle as i32
    );
    let Some(proto::request_response::Request::PublishDataTrack(request)) =
        request_response.request
    else {
        panic!("expected publish data track request in request_response");
    };
    assert_eq!(request.pub_handle, 0);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_publish_data_track_rejects_duplicate_handle() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let first_publish = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 42,
                name: "sensor-1".to_string(),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(first_publish.encode_to_vec().into()))
        .await
        .expect("first publish should send");

    let published = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = socket
                .next()
                .await
                .expect("first publish response should arrive")
                .expect("first publish response should be ok");
            let Message::Binary(response_bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(response_bytes.as_ref())
                .expect("first publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(published)) =
                response.message
            {
                break published;
            }
        }
    })
    .await
    .expect("publish data track response should arrive before timeout");
    assert_eq!(
        published
            .info
            .expect("published info should be present")
            .pub_handle,
        42
    );

    let second_publish = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 42,
                name: "sensor-2".to_string(),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(second_publish.encode_to_vec().into()))
        .await
        .expect("second publish should send");

    let request_response = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = socket
                .next()
                .await
                .expect("second publish response should arrive")
                .expect("second publish response should be ok");
            let Message::Binary(response_bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(response_bytes.as_ref())
                .expect("second publish response should decode");
            if let Some(proto::signal_response::Message::RequestResponse(request_response)) =
                response.message
            {
                break request_response;
            }
        }
    })
    .await
    .expect("request_response should arrive before timeout");
    assert_eq!(
        request_response.reason,
        proto::request_response::Reason::DuplicateHandle as i32
    );
    let Some(proto::request_response::Request::PublishDataTrack(request)) =
        request_response.request
    else {
        panic!("expected publish data track request in request_response");
    };
    assert_eq!(request.pub_handle, 42);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_publish_data_track_rejects_duplicate_name() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let first_publish = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 50,
                name: "duplicate-name-track".to_string(),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(first_publish.encode_to_vec().into()))
        .await
        .expect("first publish should send");

    let published = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = socket
                .next()
                .await
                .expect("first publish response should arrive")
                .expect("first publish response should be ok");
            let Message::Binary(response_bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(response_bytes.as_ref())
                .expect("first publish response should decode");
            if let Some(proto::signal_response::Message::PublishDataTrackResponse(published)) =
                response.message
            {
                break published;
            }
        }
    })
    .await
    .expect("publish data track response should arrive before timeout");
    assert_eq!(
        published
            .info
            .expect("published info should be present")
            .pub_handle,
        50
    );

    let second_publish = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 51,
                name: "duplicate-name-track".to_string(),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(second_publish.encode_to_vec().into()))
        .await
        .expect("second publish should send");

    let request_response = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = socket
                .next()
                .await
                .expect("second publish response should arrive")
                .expect("second publish response should be ok");
            let Message::Binary(response_bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(response_bytes.as_ref())
                .expect("second publish response should decode");
            if let Some(proto::signal_response::Message::RequestResponse(request_response)) =
                response.message
            {
                break request_response;
            }
        }
    })
    .await
    .expect("request_response should arrive before timeout");
    assert_eq!(
        request_response.reason,
        proto::request_response::Reason::DuplicateName as i32
    );
    let Some(proto::request_response::Request::PublishDataTrack(request)) =
        request_response.request
    else {
        panic!("expected publish data track request in request_response");
    };
    assert_eq!(request.pub_handle, 51);

    server.abort();
}

#[tokio::test]
async fn rtc_v1_publish_data_track_rejects_too_large_handle() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: u16::MAX as u32 + 1,
                name: "too-large-handle".to_string(),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let response_message = socket
        .next()
        .await
        .expect("request response should arrive")
        .expect("request response should be ok");
    let Message::Binary(response_bytes) = response_message else {
        panic!("expected binary request response");
    };
    let response = proto::SignalResponse::decode(response_bytes.as_ref())
        .expect("request response should decode");
    let Some(proto::signal_response::Message::RequestResponse(request_response)) = response.message
    else {
        panic!("expected request_response message");
    };
    assert_eq!(
        request_response.reason,
        proto::request_response::Reason::InvalidHandle as i32
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_publish_data_track_rejects_too_long_name() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_publish_data("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let publish_request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PublishDataTrackRequest(
            proto::PublishDataTrackRequest {
                pub_handle: 99,
                name: "x".repeat(257),
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(publish_request.encode_to_vec().into()))
        .await
        .expect("publish request should send");

    let response_message = socket
        .next()
        .await
        .expect("request response should arrive")
        .expect("request response should be ok");
    let Message::Binary(response_bytes) = response_message else {
        panic!("expected binary request response");
    };
    let response = proto::SignalResponse::decode(response_bytes.as_ref())
        .expect("request response should decode");
    let Some(proto::signal_response::Message::RequestResponse(request_response)) = response.message
    else {
        panic!("expected request_response message");
    };
    assert_eq!(
        request_response.reason,
        proto::request_response::Reason::InvalidName as i32
    );

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_answers_offer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
        panic!("expected answer response");
    };
    assert_eq!(answer.r#type, "answer");
    assert_eq!(answer.id, 7);
    assert!(answer.sdp.starts_with("v=0"));
    assert!(answer.sdp.contains("m=application"));

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
async fn rtc_v1_answer_for_recvonly_audio_offer_is_not_recvonly() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_audio_offer()
        .await
        .expect("audio offer should create");
    assert!(offer_sdp.contains("m=audio"));
    assert!(offer_sdp.contains("a=recvonly"));

    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 11,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
        panic!("expected answer response");
    };

    let mut in_audio_section = false;
    let mut audio_direction: Option<String> = None;
    for line in answer.sdp.lines() {
        if line.starts_with("m=") {
            in_audio_section = line.starts_with("m=audio ");
            continue;
        }
        if in_audio_section
            && matches!(
                line,
                "a=sendrecv" | "a=sendonly" | "a=recvonly" | "a=inactive"
            )
        {
            audio_direction = Some(line.trim_start_matches("a=").to_string());
            break;
        }
    }

    let audio_direction = audio_direction.expect("audio media direction should be present");
    assert_ne!(
        audio_direction, "recvonly",
        "answering a recvonly offer must not produce recvonly on the answer side"
    );

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_sends_trickle_after_offer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(_answer)) = answer.message else {
        panic!("expected answer response");
    };

    let trickle_message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("trickle should arrive before timeout")
        .expect("trickle stream should not close")
        .expect("trickle should be ok");
    let Message::Binary(trickle_bytes) = trickle_message else {
        panic!("expected binary trickle response");
    };
    let trickle = proto::SignalResponse::decode(trickle_bytes.as_ref())
        .expect("trickle response should decode");
    let Some(proto::signal_response::Message::Trickle(trickle)) = trickle.message else {
        panic!("expected trickle response");
    };
    assert_eq!(trickle.target, proto::SignalTarget::Publisher as i32);
    assert!(!trickle.r#final);
    assert!(trickle.candidate_init.contains("candidate:"));
    assert!(trickle.candidate_init.contains("sdpMLineIndex"));

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
#[allow(deprecated)]
async fn rtc_v1_user_data_packet_reaches_oxidesfu() {
    let signal_state = state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let (client_peer, mut client_events) = oxidesfu_rtc::create_peer_connection_with_events()
        .await
        .expect("client peer connection should create");
    let data_channel = client_peer
        .create_data_channel("data")
        .await
        .expect("client data channel should create");
    let offer_sdp = client_peer
        .create_offer()
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
        panic!("expected answer response");
    };
    client_peer
        .set_remote_answer(answer.sdp)
        .await
        .expect("answer should apply to client peer");

    let open_channel = data_channel.clone();
    let open_task = tokio::spawn(async move { open_channel.wait_open().await });
    tokio::pin!(open_task);
    let mut sent = false;
    let received = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = client_events.ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            let trickle = proto::SignalRequest {
                                message: Some(proto::signal_request::Message::Trickle(
                                    proto::TrickleRequest {
                                        candidate_init: candidate.candidate_init_json,
                                        target: proto::SignalTarget::Publisher as i32,
                                        r#final: candidate.is_final,
                                    },
                                )),
                            };
                            socket
                                .send(Message::Binary(trickle.encode_to_vec().into()))
                                .await
                                .expect("client trickle should send");
                        }
                    }
                    message = socket.next() => {
                        let Some(Ok(Message::Binary(bytes))) = message else {
                            continue;
                        };
                        let response = proto::SignalResponse::decode(bytes.as_ref())
                            .expect("signal response should decode");
                        if let Some(proto::signal_response::Message::Trickle(trickle)) = response.message {
                            client_peer
                                .add_ice_candidate_json(&trickle.candidate_init)
                                .await
                                .expect("server trickle should add to client peer");
                        }
                    }
                    result = &mut open_task, if !sent => {
                        result
                            .expect("open task should not panic")
                            .expect("client data channel should open");
                        let packet = proto::DataPacket {
                            kind: proto::data_packet::Kind::Reliable as i32,
                            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                                payload: b"hello ferrite packet".to_vec(),
                                topic: Some("test-topic".to_string()),
                                ..Default::default()
                            })),
                            ..Default::default()
                        };
                        data_channel
                            .send_bytes(&packet.encode_to_vec())
                            .await
                            .expect("data packet should send");
                        sent = true;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)), if sent => {
                        if let Some(message) = signal_state.data_messages.last() {
                            break message;
                        }
                    }
                }
            }
        })
        .await
        .expect("OxideSFU should record data channel message before timeout");

    assert_eq!(received.room, "test-room");
    assert_eq!(received.identity, "alice");
    assert_eq!(received.text, "hello ferrite packet");
    assert_eq!(
        received.user_payload.as_deref(),
        Some(&b"hello ferrite packet"[..])
    );
    assert_eq!(received.topic.as_deref(), Some("test-topic"));
    assert_eq!(received.kind, proto::data_packet::Kind::Reliable as i32);

    client_peer.close().await.expect("client peer should close");
    server.abort();
}

#[tokio::test]
#[allow(deprecated)]
async fn rtc_v1_user_data_packet_requires_can_publish_data() {
    let signal_state = state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_permissions("test-room", "alice", "Alice", false, true)
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let (client_peer, mut client_events) = oxidesfu_rtc::create_peer_connection_with_events()
        .await
        .expect("client peer connection should create");
    let data_channel = client_peer
        .create_data_channel("data")
        .await
        .expect("client data channel should create");
    let offer_sdp = client_peer
        .create_offer()
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 17,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
        panic!("expected answer response");
    };
    client_peer
        .set_remote_answer(answer.sdp)
        .await
        .expect("answer should apply to client peer");

    let open_channel = data_channel.clone();
    let open_task = tokio::spawn(async move { open_channel.wait_open().await });
    tokio::pin!(open_task);
    let mut sent = false;
    let _ = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            tokio::select! {
                candidate = client_events.ice_candidates.recv() => {
                    if let Some(candidate) = candidate {
                        let trickle = proto::SignalRequest {
                            message: Some(proto::signal_request::Message::Trickle(
                                proto::TrickleRequest {
                                    candidate_init: candidate.candidate_init_json,
                                    target: proto::SignalTarget::Publisher as i32,
                                    r#final: candidate.is_final,
                                },
                            )),
                        };
                        socket
                            .send(Message::Binary(trickle.encode_to_vec().into()))
                            .await
                            .expect("client trickle should send");
                    }
                }
                message = socket.next() => {
                    let Some(Ok(Message::Binary(bytes))) = message else {
                        continue;
                    };
                    let response = proto::SignalResponse::decode(bytes.as_ref())
                        .expect("signal response should decode");
                    if let Some(proto::signal_response::Message::Trickle(trickle)) = response.message {
                        client_peer
                            .add_ice_candidate_json(&trickle.candidate_init)
                            .await
                            .expect("server trickle should add to client peer");
                    }
                }
                result = &mut open_task, if !sent => {
                    result
                        .expect("open task should not panic")
                        .expect("client data channel should open");
                    let packet = proto::DataPacket {
                        kind: proto::data_packet::Kind::Reliable as i32,
                        value: Some(proto::data_packet::Value::User(proto::UserPacket {
                            payload: b"denied-payload".to_vec(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    };
                    data_channel
                        .send_bytes(&packet.encode_to_vec())
                        .await
                        .expect("data packet should send");
                    sent = true;
                }
                _ = tokio::time::sleep(Duration::from_millis(300)), if sent => {
                    break;
                }
            }
        }
    })
    .await;

    assert!(
        signal_state.data_messages.last().is_none(),
        "packet should be ignored when can_publish_data is false"
    );

    client_peer.close().await.expect("client peer should close");
    server.abort();
}

#[tokio::test]
#[allow(deprecated)]
async fn rtc_v1_server_user_data_packet_reaches_client() {
    let signal_state = state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let (client_peer, mut client_events) = oxidesfu_rtc::create_peer_connection_with_events()
        .await
        .expect("client peer connection should create");
    let data_channel = client_peer
        .create_data_channel("data")
        .await
        .expect("client data channel should create");
    let offer_sdp = client_peer
        .create_offer()
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");

    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
        panic!("expected answer response");
    };
    client_peer
        .set_remote_answer(answer.sdp)
        .await
        .expect("answer should apply to client peer");

    let (open_tx, mut open_rx) = mpsc::unbounded_channel();
    let recv_task = tokio::spawn(async move {
        data_channel.wait_open().await?;
        let _ = open_tx.send(());
        data_channel.recv_bytes().await
    });
    tokio::pin!(recv_task);
    let mut sent = false;
    let received = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = client_events.ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            let trickle = proto::SignalRequest {
                                message: Some(proto::signal_request::Message::Trickle(
                                    proto::TrickleRequest {
                                        candidate_init: candidate.candidate_init_json,
                                        target: proto::SignalTarget::Publisher as i32,
                                        r#final: candidate.is_final,
                                    },
                                )),
                            };
                            socket
                                .send(Message::Binary(trickle.encode_to_vec().into()))
                                .await
                                .expect("client trickle should send");
                        }
                    }
                    message = socket.next() => {
                        let Some(Ok(Message::Binary(bytes))) = message else {
                            continue;
                        };
                        let response = proto::SignalResponse::decode(bytes.as_ref())
                            .expect("signal response should decode");
                        if let Some(proto::signal_response::Message::Trickle(trickle)) = response.message {
                            client_peer
                                .add_ice_candidate_json(&trickle.candidate_init)
                                .await
                                .expect("server trickle should add to client peer");
                        }
                    }
                    Some(()) = open_rx.recv(), if !sent => {
                        wait_for_reliable_data_channel_registration(
                            &signal_state,
                            "test-room",
                            "alice",
                        )
                        .await;
                        let packet = proto::DataPacket {
                            kind: proto::data_packet::Kind::Reliable as i32,
                            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                                payload: b"hello from ferrite".to_vec(),
                                topic: Some("server-topic".to_string()),
                                ..Default::default()
                            })),
                            ..Default::default()
                        };
                        signal_state
                            .data_channels
                            .send_bytes_to_identities("test-room", &["alice".to_string()], &packet.encode_to_vec())
                            .await
                            .expect("server data packet should send");
                        sent = true;
                    }
                    result = &mut recv_task => {
                        break result
                            .expect("recv task should not panic")
                            .expect("client should receive bytes");
                    }
                }
            }
        })
        .await
        .expect("client should receive server data packet before timeout");

    let packet =
        proto::DataPacket::decode(received.as_slice()).expect("server data packet should decode");
    assert_eq!(packet.kind, proto::data_packet::Kind::Reliable as i32);
    let Some(proto::data_packet::Value::User(user)) = packet.value else {
        panic!("expected user packet");
    };
    assert_eq!(user.payload, b"hello from ferrite");
    assert_eq!(user.topic.as_deref(), Some("server-topic"));

    client_peer.close().await.expect("client peer should close");
    server.abort();
}

#[tokio::test]
async fn rtc_v1_leave_removes_participant_data_channel() {
    let signal_state = state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let (client_peer, mut client_events) = oxidesfu_rtc::create_peer_connection_with_events()
        .await
        .expect("client peer connection should create");
    let data_channel = client_peer
        .create_data_channel("data")
        .await
        .expect("client data channel should create");
    let offer_sdp = client_peer
        .create_offer()
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 11,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");
    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
        panic!("expected answer response");
    };
    client_peer
        .set_remote_answer(answer.sdp)
        .await
        .expect("answer should apply to client peer");

    let open_channel = data_channel.clone();
    let open_task = tokio::spawn(async move { open_channel.wait_open().await });
    tokio::pin!(open_task);
    tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = client_events.ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            let trickle = proto::SignalRequest {
                                message: Some(proto::signal_request::Message::Trickle(
                                    proto::TrickleRequest {
                                        candidate_init: candidate.candidate_init_json,
                                        target: proto::SignalTarget::Publisher as i32,
                                        r#final: candidate.is_final,
                                    },
                                )),
                            };
                            socket
                                .send(Message::Binary(trickle.encode_to_vec().into()))
                                .await
                                .expect("client trickle should send");
                        }
                    }
                    message = socket.next() => {
                        let Some(Ok(Message::Binary(bytes))) = message else {
                            continue;
                        };
                        let response = proto::SignalResponse::decode(bytes.as_ref())
                            .expect("signal response should decode");
                        if let Some(proto::signal_response::Message::Trickle(trickle)) = response.message {
                            client_peer
                                .add_ice_candidate_json(&trickle.candidate_init)
                                .await
                                .expect("server trickle should add to client peer");
                        }
                    }
                    result = &mut open_task => {
                        result
                            .expect("open task should not panic")
                            .expect("client data channel should open");
                        break;
                    }
                }
            }
        })
        .await
        .expect("data channel should open before timeout");

    wait_for_reliable_data_channel_registration(&signal_state, "test-room", "alice").await;
    let leave = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Leave(proto::LeaveRequest {
            reason: proto::DisconnectReason::ClientInitiated as i32,
            ..Default::default()
        })),
    };
    socket
        .send(Message::Binary(leave.encode_to_vec().into()))
        .await
        .expect("leave should send");

    let close_frame = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(frame))) => return frame,
                Some(Ok(_)) => continue,
                Some(Err(error)) => panic!("socket should close cleanly after leave: {error}"),
                None => panic!("socket stream ended before close frame after leave"),
            }
        }
    })
    .await
    .expect("close frame should arrive after leave");
    assert!(
        close_frame.is_none(),
        "leave close frame should not carry custom reason"
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if signal_state
                .data_channels
                .get("test-room", "alice")
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("data channel should be removed after leave");

    client_peer.close().await.expect("client peer should close");
    server.abort();
}

#[tokio::test]
async fn rtc_v1_disconnect_removes_participant_data_channel() {
    let signal_state = state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(server_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let (client_peer, mut client_events) = oxidesfu_rtc::create_peer_connection_with_events()
        .await
        .expect("client peer connection should create");
    let data_channel = client_peer
        .create_data_channel("data")
        .await
        .expect("client data channel should create");
    let offer_sdp = client_peer
        .create_offer()
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 12,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");
    let answer_message = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");
    let Message::Binary(answer_bytes) = answer_message else {
        panic!("expected binary answer response");
    };
    let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
        .expect("answer response should decode");
    let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
        panic!("expected answer response");
    };
    client_peer
        .set_remote_answer(answer.sdp)
        .await
        .expect("answer should apply to client peer");

    let open_channel = data_channel.clone();
    let open_task = tokio::spawn(async move { open_channel.wait_open().await });
    tokio::pin!(open_task);
    tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = client_events.ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            let trickle = proto::SignalRequest {
                                message: Some(proto::signal_request::Message::Trickle(
                                    proto::TrickleRequest {
                                        candidate_init: candidate.candidate_init_json,
                                        target: proto::SignalTarget::Publisher as i32,
                                        r#final: candidate.is_final,
                                    },
                                )),
                            };
                            socket
                                .send(Message::Binary(trickle.encode_to_vec().into()))
                                .await
                                .expect("client trickle should send");
                        }
                    }
                    message = socket.next() => {
                        let Some(Ok(Message::Binary(bytes))) = message else {
                            continue;
                        };
                        let response = proto::SignalResponse::decode(bytes.as_ref())
                            .expect("signal response should decode");
                        if let Some(proto::signal_response::Message::Trickle(trickle)) = response.message {
                            client_peer
                                .add_ice_candidate_json(&trickle.candidate_init)
                                .await
                                .expect("server trickle should add to client peer");
                        }
                    }
                    result = &mut open_task => {
                        result
                            .expect("open task should not panic")
                            .expect("client data channel should open");
                        break;
                    }
                }
            }
        })
        .await
        .expect("data channel should open before timeout");

    wait_for_reliable_data_channel_registration(&signal_state, "test-room", "alice").await;

    drop(socket);
    client_peer.close().await.expect("client peer should close");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if signal_state
                .data_channels
                .get("test-room", "alice")
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("data channel should be removed after disconnect");

    server.abort();
}

#[tokio::test]
async fn rtc_v1_websocket_accepts_trickle_after_offer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("test-room")))
            .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let offerer = oxidesfu_rtc::create_peer_connection()
        .await
        .expect("offerer peer connection should create");
    let offer_sdp = offerer
        .create_data_channel_offer("data")
        .await
        .expect("offer should create");
    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: 7,
                ..Default::default()
            },
        )),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("offer request should send");
    let _answer = socket
        .next()
        .await
        .expect("answer should arrive")
        .expect("answer should be ok");

    let trickle = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Trickle(
                proto::TrickleRequest {
                    candidate_init: r#"{"candidate":"candidate:0 1 UDP 2122252543 127.0.0.1 12345 typ host","sdpMid":"0","sdpMLineIndex":0}"#.to_string(),
                    target: proto::SignalTarget::Publisher as i32,
                    ..Default::default()
                },
            )),
        };
    socket
        .send(Message::Binary(trickle.encode_to_vec().into()))
        .await
        .expect("trickle request should send");

    let ping_timestamp = 4321;
    let ping = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: ping_timestamp,
            rtt: 3,
        })),
    };
    socket
        .send(Message::Binary(ping.encode_to_vec().into()))
        .await
        .expect("ping request should send after trickle");
    let mut pong = None;
    for _ in 0..5 {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("response should arrive after trickle")
            .expect("response stream should stay open after trickle")
            .expect("response should be ok after trickle");
        let Message::Binary(bytes) = message else {
            continue;
        };
        let response =
            proto::SignalResponse::decode(bytes.as_ref()).expect("response should decode");
        if let Some(proto::signal_response::Message::PongResp(response_pong)) = response.message {
            pong = Some(response_pong);
            break;
        }
    }
    let pong = pong.expect("expected pong_resp response");
    assert_eq!(pong.last_ping_timestamp, ping_timestamp);

    offerer.close().await.expect("offerer should close");
    server.abort();
}

#[tokio::test]
async fn leave_removes_participant_and_updates_remaining_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let rooms = signal_state.rooms.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first join should arrive")
        .expect("first join should be ok");

    let mut second_request = url.into_client_request().expect("request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token_for("test-room", "bob", "Bob")))
            .expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect");
    let _second_join = second_socket
        .next()
        .await
        .expect("second join should arrive")
        .expect("second join should be ok");
    let _first_sees_bob_join = first_socket
        .next()
        .await
        .expect("first should see bob join")
        .expect("bob join update should be ok");

    let leave = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Leave(
            proto::LeaveRequest::default(),
        )),
    };
    second_socket
        .send(Message::Binary(leave.encode_to_vec().into()))
        .await
        .expect("leave request should send");

    let update_message = first_socket
        .next()
        .await
        .expect("participant leave update should arrive")
        .expect("participant leave update should be ok");
    let Message::Binary(update_bytes) = update_message else {
        panic!("expected binary participant leave update");
    };
    let update = proto::SignalResponse::decode(update_bytes.as_ref())
        .expect("participant leave update should decode");
    let Some(proto::signal_response::Message::Update(update)) = update.message else {
        panic!("expected participant update");
    };
    assert_eq!(update.participants.len(), 1);
    assert_eq!(update.participants[0].identity, "bob");
    assert_eq!(
        update.participants[0].state,
        proto::participant_info::State::Disconnected as i32
    );
    assert_eq!(
        rooms.get_participant("test-room", "bob"),
        Err(RoomStoreError::ParticipantNotFound)
    );

    server.abort();
}

#[tokio::test]
async fn late_trickle_after_explicit_leave_does_not_restore_participant_state() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let rooms = signal_state.rooms.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    let leave = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Leave(
            proto::LeaveRequest::default(),
        )),
    };
    socket
        .send(Message::Binary(leave.encode_to_vec().into()))
        .await
        .expect("leave request should send");

    let _ = tokio::time::timeout(Duration::from_secs(2), socket.next()).await;

    let late_trickle = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Trickle(proto::TrickleRequest {
            candidate_init: r#"{"candidate":"candidate:0 1 UDP 2122252543 127.0.0.1 12345 typ host","sdpMid":"0","sdpMLineIndex":0}"#.to_string(),
            target: proto::SignalTarget::Publisher as i32,
            ..Default::default()
        })),
    };
    if socket
        .send(Message::Binary(late_trickle.encode_to_vec().into()))
        .await
        .is_ok()
    {
        let _ = tokio::time::timeout(Duration::from_millis(200), socket.next()).await;
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if rooms.get_participant("test-room", "alice")
                == Err(RoomStoreError::ParticipantNotFound)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("participant should remain removed after leave and late trickle");

    server.abort();
}

#[tokio::test]
async fn duplicate_identity_join_sends_duplicate_identity_leave_to_old_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first join should arrive")
        .expect("first join should be ok");

    let mut second_request = url.into_client_request().expect("request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice Duplicate")
        ))
        .expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect");
    let _second_join = second_socket
        .next()
        .await
        .expect("second join should arrive")
        .expect("second join should be ok");

    let leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = first_socket.next().await {
            let message = message.expect("old duplicate socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("duplicate identity signal response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("old duplicate socket closed before duplicate identity leave");
    })
    .await
    .expect("old duplicate socket should receive leave");
    assert_eq!(
        leave.reason,
        proto::DisconnectReason::DuplicateIdentity as i32
    );
    assert_eq!(
        leave.action,
        proto::leave_request::Action::Disconnect as i32
    );

    server.abort();
}

#[tokio::test]
async fn duplicate_identity_join_keeps_new_socket_active() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice One")
        ))
        .expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first join should arrive")
        .expect("first join should be ok");

    let mut second_request = url.into_client_request().expect("request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice Two")
        ))
        .expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect");
    let _second_join = second_socket
        .next()
        .await
        .expect("second join should arrive")
        .expect("second join should be ok");

    let _old_leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = first_socket.next().await {
            let message = message.expect("old duplicate socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("duplicate identity signal response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("old duplicate socket closed before duplicate identity leave");
    })
    .await
    .expect("old duplicate socket should receive leave");

    let ping = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: 5150,
            rtt: 0,
        })),
    };
    second_socket
        .send(Message::Binary(ping.encode_to_vec().into()))
        .await
        .expect("new duplicate socket ping should send");
    let pong_message = tokio::time::timeout(Duration::from_secs(5), second_socket.next())
        .await
        .expect("pong should arrive on new duplicate socket")
        .expect("new duplicate socket should stay open")
        .expect("new duplicate socket pong should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    let Some(proto::signal_response::Message::PongResp(pong)) = pong.message else {
        panic!("expected pong_resp response");
    };
    assert_eq!(pong.last_ping_timestamp, 5150);

    server.abort();
}

#[tokio::test]
async fn duplicate_identity_join_rtc_v0_closes_old_socket_after_post_leave_ping() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc?join_request={}", join_request_param());

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice One")
        ))
        .expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first join should arrive")
        .expect("first join should be ok");

    let mut second_request = url.into_client_request().expect("request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice Two")
        ))
        .expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect");
    let _second_join = second_socket
        .next()
        .await
        .expect("second join should arrive")
        .expect("second join should be ok");

    let _old_leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = first_socket.next().await {
            let message = message.expect("old duplicate socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("duplicate identity signal response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("old duplicate socket closed before duplicate identity leave");
    })
    .await
    .expect("old duplicate socket should receive leave");

    first_socket
        .send(Message::Binary(
            proto::SignalRequest {
                message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                    timestamp: 9001,
                    rtt: 0,
                })),
            }
            .encode_to_vec()
            .into(),
        ))
        .await
        .expect("old duplicate socket ping should send");

    let close_or_drop = tokio::time::timeout(Duration::from_secs(2), first_socket.next())
        .await
        .expect("old duplicate socket should close after stale ping")
        .expect("old duplicate socket should emit close or drop");
    match close_or_drop {
        Ok(Message::Close(_)) => {}
        Ok(other) => panic!("expected close frame on stale duplicate socket, got: {other:?}"),
        Err(_) => {}
    }

    let ping = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: 5150,
            rtt: 0,
        })),
    };
    second_socket
        .send(Message::Binary(ping.encode_to_vec().into()))
        .await
        .expect("new duplicate socket ping should send");
    let pong_message = tokio::time::timeout(Duration::from_secs(5), second_socket.next())
        .await
        .expect("pong should arrive on new duplicate socket")
        .expect("new duplicate socket should stay open")
        .expect("new duplicate socket pong should be ok");
    let Message::Binary(pong_bytes) = pong_message else {
        panic!("expected binary pong response");
    };
    let pong =
        proto::SignalResponse::decode(pong_bytes.as_ref()).expect("pong response should decode");
    let Some(proto::signal_response::Message::PongResp(pong)) = pong.message else {
        panic!("expected pong_resp response");
    };
    assert_eq!(pong.last_ping_timestamp, 5150);

    server.abort();
}

#[tokio::test]
async fn duplicate_identity_is_scoped_per_room() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let join_request = join_request_param();
    let mut room_a_request = format!("ws://{addr}/rtc/v1?join_request={join_request}")
        .into_client_request()
        .expect("request should build");
    room_a_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("room-a", "same-identity", "Alice A")
        ))
        .expect("auth header should parse"),
    );
    let (mut room_a_socket, _) = connect_async(room_a_request)
        .await
        .expect("room-a websocket should connect");
    let room_a_join_message = room_a_socket
        .next()
        .await
        .expect("room-a join should arrive")
        .expect("room-a join should be ok");
    let Message::Binary(room_a_bytes) = room_a_join_message else {
        panic!("expected room-a binary join response");
    };
    let room_a_response = proto::SignalResponse::decode(room_a_bytes.as_ref())
        .expect("room-a response should decode");
    let Some(proto::signal_response::Message::Join(room_a_join)) = room_a_response.message else {
        panic!("expected room-a join response");
    };
    assert_eq!(room_a_join.room.expect("room should exist").name, "room-a");

    let mut room_b_request = format!("ws://{addr}/rtc/v1?join_request={join_request}")
        .into_client_request()
        .expect("request should build");
    room_b_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("room-b", "same-identity", "Alice B")
        ))
        .expect("auth header should parse"),
    );
    let (mut room_b_socket, _) = connect_async(room_b_request)
        .await
        .expect("room-b websocket should connect");
    let room_b_join_message = room_b_socket
        .next()
        .await
        .expect("room-b join should arrive")
        .expect("room-b join should be ok");
    let Message::Binary(room_b_bytes) = room_b_join_message else {
        panic!("expected room-b binary join response");
    };
    let room_b_response = proto::SignalResponse::decode(room_b_bytes.as_ref())
        .expect("room-b response should decode");
    let Some(proto::signal_response::Message::Join(room_b_join)) = room_b_response.message else {
        panic!("expected room-b join response");
    };
    assert_eq!(room_b_join.room.expect("room should exist").name, "room-b");

    assert!(
        tokio::time::timeout(Duration::from_millis(100), room_a_socket.next())
            .await
            .is_err(),
        "cross-room same identity should not force a duplicate-identity leave"
    );

    server.abort();
}

#[tokio::test]
async fn hidden_participant_is_not_announced_or_returned_as_other_participant() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut alice_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    alice_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut alice_socket, _) = connect_async(alice_request)
        .await
        .expect("alice websocket should connect");
    let _alice_join = alice_socket
        .next()
        .await
        .expect("alice join should arrive")
        .expect("alice join should be ok");

    let mut hidden_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    hidden_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_grants(
                "test-room",
                "hidden-bot",
                "Hidden Bot",
                VideoGrants {
                    room_join: true,
                    room: "test-room".to_string(),
                    hidden: true,
                    ..Default::default()
                },
            )
        ))
        .expect("auth header should parse"),
    );
    let (mut hidden_socket, _) = connect_async(hidden_request)
        .await
        .expect("hidden websocket should connect");
    let _hidden_join = hidden_socket
        .next()
        .await
        .expect("hidden join should arrive")
        .expect("hidden join should be ok");

    assert!(
        tokio::time::timeout(Duration::from_millis(100), alice_socket.next())
            .await
            .is_err(),
        "visible participant should not receive hidden participant join update"
    );

    let mut bob_request = url.into_client_request().expect("request should build");
    bob_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token_for("test-room", "bob", "Bob")))
            .expect("auth header should parse"),
    );
    let (mut bob_socket, _) = connect_async(bob_request)
        .await
        .expect("bob websocket should connect");
    let bob_join_message = bob_socket
        .next()
        .await
        .expect("bob join should arrive")
        .expect("bob join should be ok");
    let Message::Binary(bob_join_bytes) = bob_join_message else {
        panic!("expected bob binary join response");
    };
    let bob_response = proto::SignalResponse::decode(bob_join_bytes.as_ref())
        .expect("bob join response should decode");
    let Some(proto::signal_response::Message::Join(bob_join)) = bob_response.message else {
        panic!("expected bob join response");
    };
    assert_eq!(bob_join.other_participants.len(), 1);
    assert_eq!(bob_join.other_participants[0].identity, "alice");

    server.abort();
}

#[tokio::test]
async fn hidden_participant_still_receives_visible_participant_updates() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut hidden_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    hidden_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for_with_grants(
                "test-room",
                "hidden-bot",
                "Hidden Bot",
                VideoGrants {
                    room_join: true,
                    room: "test-room".to_string(),
                    hidden: true,
                    ..Default::default()
                }
            )
        ))
        .expect("auth header should parse"),
    );
    let (mut hidden_socket, _) = connect_async(hidden_request)
        .await
        .expect("hidden websocket should connect");
    let _hidden_join = hidden_socket
        .next()
        .await
        .expect("hidden join should arrive")
        .expect("hidden join should be ok");

    let mut visible_request = url.into_client_request().expect("request should build");
    visible_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "visible", "Visible")
        ))
        .expect("auth header should parse"),
    );
    let (mut visible_socket, _) = connect_async(visible_request)
        .await
        .expect("visible websocket should connect");
    let _visible_join = visible_socket
        .next()
        .await
        .expect("visible join should arrive")
        .expect("visible join should be ok");

    let hidden_update_message = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = hidden_socket.next().await {
            let message = message.expect("hidden socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("hidden update response should decode");
            if let Some(proto::signal_response::Message::Update(update)) = response.message {
                return update;
            }
        }
        panic!("hidden socket closed before visible participant update");
    })
    .await
    .expect("hidden participant should receive visible participant updates");

    assert_eq!(hidden_update_message.participants.len(), 1);
    assert_eq!(hidden_update_message.participants[0].identity, "visible");

    server.abort();
}

#[tokio::test]
async fn service_delete_room_disconnects_all_active_sockets_with_room_deleted_reason() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let runtime_state = signal_state.clone();
    let rooms = signal_state.rooms.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut alice_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    alice_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut alice_socket, _) = connect_async(alice_request)
        .await
        .expect("alice websocket should connect");
    let _alice_join = alice_socket
        .next()
        .await
        .expect("alice join should arrive")
        .expect("alice join should be ok");

    let mut bob_request = url.into_client_request().expect("request should build");
    bob_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token_for("test-room", "bob", "Bob")))
            .expect("auth header should parse"),
    );
    let (mut bob_socket, _) = connect_async(bob_request)
        .await
        .expect("bob websocket should connect");
    let _bob_join = bob_socket
        .next()
        .await
        .expect("bob join should arrive")
        .expect("bob join should be ok");

    let _alice_update = alice_socket
        .next()
        .await
        .expect("alice should receive bob update")
        .expect("alice update should be ok");

    let participants = rooms
        .list_participants("test-room")
        .expect("participants should list before room delete");
    assert_eq!(participants.len(), 2);
    for participant in participants {
        runtime_state
            .disconnect_participant_from_service(
                "test-room",
                &participant.identity,
                proto::DisconnectReason::RoomDeleted,
            )
            .await
            .expect("room-delete disconnect should succeed");
    }

    let alice_leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = alice_socket.next().await {
            let message = message.expect("alice socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("alice response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("alice socket closed before room-deleted leave");
    })
    .await
    .expect("alice leave should arrive");
    assert_eq!(
        alice_leave.reason,
        proto::DisconnectReason::RoomDeleted as i32
    );

    let bob_leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = bob_socket.next().await {
            let message = message.expect("bob socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response =
                proto::SignalResponse::decode(bytes.as_ref()).expect("bob response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("bob socket closed before room-deleted leave");
    })
    .await
    .expect("bob leave should arrive");
    assert_eq!(
        bob_leave.reason,
        proto::DisconnectReason::RoomDeleted as i32
    );

    assert!(
        matches!(rooms.list_participants("test-room"), Ok(participants) if participants.is_empty()),
        "room-delete disconnect should remove all participants from room state"
    );

    server.abort();
}

#[tokio::test]
async fn service_remove_participant_sends_participant_removed_leave_to_active_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let runtime_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    runtime_state
        .disconnect_participant_from_service(
            "test-room",
            "alice",
            proto::DisconnectReason::ParticipantRemoved,
        )
        .await
        .expect("service removal should succeed");

    let leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = socket.next().await {
            let message = message.expect("removed participant socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("participant removed signal response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("removed participant socket closed before participant removed leave");
    })
    .await
    .expect("removed participant should receive leave");
    assert_eq!(
        leave.reason,
        proto::DisconnectReason::ParticipantRemoved as i32
    );
    assert_eq!(
        leave.action,
        proto::leave_request::Action::Disconnect as i32
    );

    server.abort();
}

#[tokio::test]
async fn service_remove_participant_does_not_disconnect_same_identity_in_other_room() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let runtime_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let join_request = join_request_param();

    let mut room_a_request = format!("ws://{addr}/rtc/v1?join_request={join_request}")
        .into_client_request()
        .expect("request should build");
    room_a_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("room-a", "same-identity", "Alice A")
        ))
        .expect("auth header should parse"),
    );
    let (mut room_a_socket, _) = connect_async(room_a_request)
        .await
        .expect("room-a websocket should connect");
    let _room_a_join = room_a_socket
        .next()
        .await
        .expect("room-a join should arrive")
        .expect("room-a join should be ok");

    let mut room_b_request = format!("ws://{addr}/rtc/v1?join_request={join_request}")
        .into_client_request()
        .expect("request should build");
    room_b_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("room-b", "same-identity", "Alice B")
        ))
        .expect("auth header should parse"),
    );
    let (mut room_b_socket, _) = connect_async(room_b_request)
        .await
        .expect("room-b websocket should connect");
    let _room_b_join = room_b_socket
        .next()
        .await
        .expect("room-b join should arrive")
        .expect("room-b join should be ok");

    runtime_state
        .disconnect_participant_from_service(
            "room-a",
            "same-identity",
            proto::DisconnectReason::ParticipantRemoved,
        )
        .await
        .expect("service removal should succeed");

    let leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = room_a_socket.next().await {
            let message = message.expect("room-a socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("room-a leave response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("room-a socket closed before leave");
    })
    .await
    .expect("room-a leave should arrive");
    assert_eq!(
        leave.reason,
        proto::DisconnectReason::ParticipantRemoved as i32
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(100), room_b_socket.next())
            .await
            .is_err(),
        "room-b same identity should remain connected"
    );

    server.abort();
}

#[tokio::test]
async fn late_ping_after_service_remove_participant_does_not_restore_participant_state() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let runtime_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let _join = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");

    runtime_state
        .disconnect_participant_from_service(
            "test-room",
            "alice",
            proto::DisconnectReason::ParticipantRemoved,
        )
        .await
        .expect("service removal should succeed");

    let _leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = socket.next().await {
            let message = message.expect("removed participant socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("participant removed signal response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("removed participant socket closed before participant removed leave");
    })
    .await
    .expect("removed participant should receive leave");

    let ping = proto::SignalRequest {
        message: Some(proto::signal_request::Message::PingReq(proto::Ping {
            timestamp: 404,
            rtt: 0,
        })),
    };
    if socket
        .send(Message::Binary(ping.encode_to_vec().into()))
        .await
        .is_ok()
    {
        let _ = tokio::time::timeout(Duration::from_millis(200), socket.next()).await;
    }

    assert_eq!(
        runtime_state.rooms.get_participant("test-room", "alice"),
        Err(RoomStoreError::ParticipantNotFound),
        "late messages after service removal must not restore room participant state"
    );

    server.abort();
}

#[tokio::test]
async fn removed_participant_can_rejoin_with_same_still_valid_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let runtime_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let token = token_for("test-room", "alice", "Alice");
    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let _first_join = first_socket
        .next()
        .await
        .expect("first join should arrive")
        .expect("first join should be ok");

    runtime_state
        .disconnect_participant_from_service(
            "test-room",
            "alice",
            proto::DisconnectReason::ParticipantRemoved,
        )
        .await
        .expect("service removal should succeed");

    let _leave = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = first_socket.next().await {
            let message = message.expect("removed participant socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("participant removed signal response should decode");
            if let Some(proto::signal_response::Message::Leave(leave)) = response.message {
                return leave;
            }
        }
        panic!("removed participant socket closed before participant removed leave");
    })
    .await
    .expect("removed participant should receive leave");

    let mut second_request = url.into_client_request().expect("request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect with same token");

    let second_join_message = second_socket
        .next()
        .await
        .expect("rejoin should arrive")
        .expect("rejoin should be ok");
    let Message::Binary(second_join_bytes) = second_join_message else {
        panic!("expected rejoin binary response");
    };
    let second_response = proto::SignalResponse::decode(second_join_bytes.as_ref())
        .expect("rejoin response should decode");
    let Some(proto::signal_response::Message::Join(join)) = second_response.message else {
        panic!("expected rejoin response");
    };
    assert_eq!(
        join.participant.expect("participant should exist").identity,
        "alice"
    );

    server.abort();
}

#[tokio::test]
async fn first_join_has_no_other_participants_and_includes_sid_identity_and_kind() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("websocket should connect");
    let join_message = socket
        .next()
        .await
        .expect("join should arrive")
        .expect("join should be ok");
    let Message::Binary(join_bytes) = join_message else {
        panic!("expected binary join response");
    };
    let response =
        proto::SignalResponse::decode(join_bytes.as_ref()).expect("join response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };

    let participant = join.participant.expect("participant should exist");
    assert!(!participant.sid.is_empty());
    assert!(participant.sid.starts_with("PA_"));
    assert_eq!(participant.identity, "alice");
    assert_eq!(
        participant.kind,
        proto::participant_info::Kind::Standard as i32
    );
    assert_eq!(
        participant.state,
        proto::participant_info::State::Joined as i32
    );
    assert!(join.other_participants.is_empty());

    server.abort();
}

#[tokio::test]
async fn second_join_sends_other_participants_and_updates_existing_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let join_request = wrapped_join_request_param(proto::JoinRequest {
        metadata: String::new(),
        ..Default::default()
    });
    let url = format!("ws://{addr}/rtc/v1?join_request={join_request}");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let alice_token = token_for_claims(Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        sub: "alice".to_string(),
        name: "Alice".to_string(),
        metadata: "alice-metadata".to_string(),
        attributes: HashMap::from([("tier".to_string(), "gold".to_string())]),
        video: VideoGrants {
            room_join: true,
            room: "test-room".to_string(),
            ..Default::default()
        },
        ..Default::default()
    });

    let mut first_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    first_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {alice_token}")).expect("auth header should parse"),
    );
    let (mut first_socket, _) = connect_async(first_request)
        .await
        .expect("first websocket should connect");
    let first_join_message = first_socket
        .next()
        .await
        .expect("first join should arrive")
        .expect("first join should be ok");
    assert!(matches!(first_join_message, Message::Binary(_)));

    let mut second_request = url.into_client_request().expect("request should build");
    second_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token_for("test-room", "bob", "Bob")))
            .expect("auth header should parse"),
    );
    let (mut second_socket, _) = connect_async(second_request)
        .await
        .expect("second websocket should connect");
    let second_message = second_socket
        .next()
        .await
        .expect("second join should arrive")
        .expect("second join should be ok");
    let Message::Binary(second_bytes) = second_message else {
        panic!("expected second binary join response");
    };
    let second_response = proto::SignalResponse::decode(second_bytes.as_ref())
        .expect("second signal response should decode");
    let Some(proto::signal_response::Message::Join(second_join)) = second_response.message else {
        panic!("expected second join response");
    };
    assert_eq!(second_join.other_participants.len(), 1);
    assert_eq!(second_join.other_participants[0].identity, "alice");
    assert_eq!(second_join.other_participants[0].name, "Alice");
    assert_eq!(second_join.other_participants[0].metadata, "alice-metadata");
    assert_eq!(
        second_join.other_participants[0].attributes.get("tier"),
        Some(&"gold".to_string())
    );

    let update_message = first_socket
        .next()
        .await
        .expect("participant update should arrive")
        .expect("participant update should be ok");
    let Message::Binary(update_bytes) = update_message else {
        panic!("expected binary participant update");
    };
    let update = proto::SignalResponse::decode(update_bytes.as_ref())
        .expect("participant update should decode");
    let Some(proto::signal_response::Message::Update(update)) = update.message else {
        panic!("expected participant update");
    };
    assert_eq!(update.participants.len(), 1);
    assert_eq!(update.participants[0].identity, "bob");

    server.abort();
}

#[tokio::test]
async fn third_join_receives_all_visible_existing_participants() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state()))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
    let mut existing_sockets = Vec::new();

    for (identity, name) in [("alice", "Alice"), ("bob", "Bob")] {
        let mut request = url
            .clone()
            .into_client_request()
            .expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!(
                "Bearer {}",
                token_for("test-room", identity, name)
            ))
            .expect("auth header should parse"),
        );
        let (mut socket, _) = connect_async(request)
            .await
            .expect("websocket should connect");
        let _join = socket
            .next()
            .await
            .expect("join should arrive")
            .expect("join should be ok");
        existing_sockets.push(socket);
    }

    let mut carol_request = url.into_client_request().expect("request should build");
    carol_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "carol", "Carol")
        ))
        .expect("auth header should parse"),
    );
    let (mut carol_socket, _) = connect_async(carol_request)
        .await
        .expect("carol websocket should connect");
    let carol_join_message = carol_socket
        .next()
        .await
        .expect("carol join should arrive")
        .expect("carol join should be ok");
    let Message::Binary(carol_join_bytes) = carol_join_message else {
        panic!("expected carol binary join response");
    };
    let carol_response = proto::SignalResponse::decode(carol_join_bytes.as_ref())
        .expect("carol join response should decode");
    let Some(proto::signal_response::Message::Join(carol_join)) = carol_response.message else {
        panic!("expected carol join response");
    };

    let mut identities = carol_join
        .other_participants
        .iter()
        .map(|participant| participant.identity.clone())
        .collect::<Vec<_>>();
    identities.sort();
    assert_eq!(identities, vec!["alice".to_string(), "bob".to_string()]);

    server.abort();
}

#[tokio::test]
async fn service_remove_participant_notifies_remaining_participants_with_disconnected_update() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let signal_state = state();
    let runtime_state = signal_state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(signal_state))
            .await
            .expect("test server should run");
    });

    let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());

    let mut alice_request = url
        .clone()
        .into_client_request()
        .expect("request should build");
    alice_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!(
            "Bearer {}",
            token_for("test-room", "alice", "Alice")
        ))
        .expect("auth header should parse"),
    );
    let (mut alice_socket, _) = connect_async(alice_request)
        .await
        .expect("alice websocket should connect");
    let _alice_join = alice_socket
        .next()
        .await
        .expect("alice join should arrive")
        .expect("alice join should be ok");

    let mut bob_request = url.into_client_request().expect("request should build");
    bob_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token_for("test-room", "bob", "Bob")))
            .expect("auth header should parse"),
    );
    let (mut bob_socket, _) = connect_async(bob_request)
        .await
        .expect("bob websocket should connect");
    let _bob_join = bob_socket
        .next()
        .await
        .expect("bob join should arrive")
        .expect("bob join should be ok");

    let _alice_receives_bob_join = alice_socket
        .next()
        .await
        .expect("alice should receive bob join update")
        .expect("alice update should be ok");

    runtime_state
        .disconnect_participant_from_service(
            "test-room",
            "bob",
            proto::DisconnectReason::ParticipantRemoved,
        )
        .await
        .expect("service removal should succeed");

    let alice_update_message = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = alice_socket.next().await {
            let message = message.expect("alice socket message should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("participant update should decode");
            if let Some(proto::signal_response::Message::Update(update)) = response.message {
                return update;
            }
        }
        panic!("alice socket closed before removal update");
    })
    .await
    .expect("alice should receive participant removal update");

    assert_eq!(alice_update_message.participants.len(), 1);
    assert_eq!(alice_update_message.participants[0].identity, "bob");
    assert_eq!(
        alice_update_message.participants[0].state,
        proto::participant_info::State::Disconnected as i32
    );

    // Drain Bob leave to avoid a dropped-future warning path in some runtimes.
    let _ = tokio::time::timeout(Duration::from_secs(1), bob_socket.next()).await;

    server.abort();
}

#[tokio::test]
async fn validate_v1_rejects_missing_join_request() {
    let response = router(state())
        .oneshot(
            Request::builder()
                .uri("/rtc/v1/validate")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", token("test-room")),
                )
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

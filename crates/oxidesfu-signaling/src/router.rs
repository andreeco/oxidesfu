// LiveKit-compatible signalling routes for OxideSFU.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message as WsMessage, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};

#[cfg(test)]
use axum::http::header;
use futures_util::{SinkExt, StreamExt};
use livekit_protocol::{self as proto, ReconnectReason};
use oxidesfu_auth::AuthContext;
use oxidesfu_room::RoomStore;

#[cfg(test)]
use oxidesfu_auth::TokenVerifier;

#[cfg(test)]
use oxidesfu_room::{RoomNodeDirectory, RoomStoreError};
use prost::Message;
use tokio::sync::mpsc;

use crate::{
    data::{
        DataChannelMessageStore, data_track_packet_handle, publish_data_track_request_response,
        rewrite_data_track_packet_handle,
    },
    join::{
        RelayDispatchOutcome, RoomNodePlacementOutcome, dispatch_non_local_relay_intent,
        non_local_relay_initial_signal_responses, non_local_relay_join_response_shape,
        room_node_placement_outcome,
    },
    media::{
        LocalForwardTrackRtpSink, LocalForwardTrackSenderReportSink, ReceiveSection,
        ReceiveSectionKind, RemoteTrackFeedbackSink, accepted_media_mids_from_answer_sdp,
        active_publisher_mids_from_offer, build_rtcp_outbound_effects, derive_rtcp_forward_actions,
        execute_rtcp_outbound_effects, find_media_track_publisher, mid_to_track_id_from_answer_sdp,
        mid_to_track_id_from_offer_sdp, offer_media_sections_from_sdp,
        receive_supported_video_mime_types_from_offer, requested_media_track_sids,
    },
    metrics::current_unix_millis,
    socket::{handle_non_local_relay_socket_message, handle_socket_message, outbound_relay_query},
    stores::{
        DataTrackPublishError, DataTrackSubscriptionStore, ForwardTrackStore, MediaForwardingStore,
        MediaSubscriptionStore, PendingMediaSectionRequestStore, SignalConnectionStore,
    },
    validate::{SignalQuery, authenticate, signal_auth_error, validate_join},
};

pub(crate) use crate::media::RtpForwardingStore;
pub use crate::state::{IceServerProvider, SignalState};

use crate::relay::{
    NonLocalRelayJoinResponse, NonLocalRelaySessionTerminationIntent, RelayOutboundSignalSender,
    inc_fallback_to_local, inc_responses_accepted, inc_responses_rejected,
};

#[cfg(test)]
use crate::join::{non_local_relay_intent_from_outcome, non_local_relay_rejection_details};

#[cfg(test)]
use crate::media::{
    KeyFrameRequestKind, KeyframeFeedbackRequest, MappedSenderReport,
    RTP_RETRANSMISSION_CACHE_SIZE, RecommendedVideoQuality, RtcpFeedbackSink, RtcpForwardAction,
    RtcpOutboundEffects, RtpRetransmitSink, SenderReportSink, build_keyframe_feedback_packet,
    build_rtcp_execution_plan, receive_section_counts_from_offer, rewrite_sender_report_packet,
    subscribed_quality_updates_from_track_settings,
};

#[cfg(test)]
use std::{future::Future, pin::Pin};

#[cfg(test)]
use crate::signal_request::signal_response_for_request;

#[cfg(test)]
use crate::stores::SubscribePermissionStore;

#[cfg(test)]
use crate::relay::{
    NonLocalRelayDispatcher, NonLocalRelayJoinIntent, NonLocalRelayOutboundSignalQuery,
    NonLocalRelaySignalRequestIntent, NonLocalRelaySignalRequestResponse,
    RedisMailboxRelayDispatcher, RedisRelayMailbox, RelayIntentExecutionDriver,
};

#[cfg(test)]
use oxidesfu_room::RedisHashStore;

const PING_INTERVAL_SECONDS: i32 = 5;
const PING_TIMEOUT_SECONDS: i32 = 15;
const CURRENT_SIGNAL_PROTOCOL: i32 = 17;
const NON_LOCAL_TERMINATION_GRACE: Duration = Duration::from_millis(800);
static NEXT_WEBHOOK_EVENT_ID: AtomicU64 = AtomicU64::new(1);

fn room_create_request_from_token_room_config(
    room_name: &str,
    room_config: &serde_json::Value,
) -> Option<proto::CreateRoomRequest> {
    let serde_json::Value::Object(map) = room_config else {
        return None;
    };

    let empty_timeout = map
        .get("emptyTimeout")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default();
    let departure_timeout = map
        .get("departureTimeout")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default();
    let max_participants = map
        .get("maxParticipants")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default();
    let metadata = map
        .get("metadata")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();

    Some(proto::CreateRoomRequest {
        name: room_name.to_string(),
        empty_timeout,
        departure_timeout,
        max_participants,
        metadata,
        ..Default::default()
    })
}

fn apply_room_config_on_first_room_creation_from_token(
    state: &SignalState,
    room_name: &str,
    room_config: Option<&serde_json::Value>,
) {
    if state.rooms.room_exists(room_name).unwrap_or(false) {
        return;
    }
    let Some(room_config) = room_config else {
        return;
    };
    let Some(request) = room_create_request_from_token_room_config(room_name, room_config) else {
        return;
    };
    let _ = state.rooms.create_room(request);
}

fn next_webhook_event_id() -> String {
    format!(
        "EV_{:016x}",
        NEXT_WEBHOOK_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn webhook_created_at_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
        .unwrap_or_default()
}

fn reduced_room_for_track_event(room: &proto::Room) -> proto::Room {
    proto::Room {
        sid: room.sid.clone(),
        name: room.name.clone(),
        ..Default::default()
    }
}

fn reduced_participant_for_track_event(
    participant: &proto::ParticipantInfo,
) -> proto::ParticipantInfo {
    proto::ParticipantInfo {
        sid: participant.sid.clone(),
        identity: participant.identity.clone(),
        name: participant.name.clone(),
        ..Default::default()
    }
}

fn emit_join_webhook_events(
    state: &SignalState,
    room: &proto::Room,
    participant: &proto::ParticipantInfo,
    first_participant_in_room: bool,
) {
    if first_participant_in_room {
        state.emit_webhook_event(proto::WebhookEvent {
            event: "room_started".to_string(),
            room: Some(room.clone()),
            id: next_webhook_event_id(),
            created_at: webhook_created_at_unix_seconds(),
            ..Default::default()
        });
    }

    state.emit_webhook_event(proto::WebhookEvent {
        event: "participant_joined".to_string(),
        room: Some(room.clone()),
        participant: Some(participant.clone()),
        id: next_webhook_event_id(),
        created_at: webhook_created_at_unix_seconds(),
        ..Default::default()
    });
}

fn join_server_info() -> proto::ServerInfo {
    proto::ServerInfo {
        edition: proto::server_info::Edition::Standard as i32,
        version: env!("CARGO_PKG_VERSION").to_string(),
        protocol: CURRENT_SIGNAL_PROTOCOL,
        ..Default::default()
    }
}

fn compare_client_version(client_info: &proto::ClientInfo, version: &str) -> i32 {
    fn parse_component(value: Option<&str>) -> i32 {
        value
            .and_then(|component| component.parse::<i32>().ok())
            .unwrap_or(0)
    }

    let current_parts: Vec<&str> = client_info.version.split('.').collect();
    let expected_parts: Vec<&str> = version.split('.').collect();

    for i in 0..3 {
        let current = parse_component(current_parts.get(i).copied());
        let expected = parse_component(expected_parts.get(i).copied());
        if current > expected {
            return 1;
        }
        if current < expected {
            return -1;
        }
    }

    0
}

fn client_supports_ice_tcp(client_info: Option<&proto::ClientInfo>) -> bool {
    let Some(client_info) = client_info else {
        return false;
    };

    let Ok(sdk) = proto::client_info::Sdk::try_from(client_info.sdk) else {
        return false;
    };

    match sdk {
        proto::client_info::Sdk::Go => false,
        proto::client_info::Sdk::Swift => compare_client_version(client_info, "1.0.5") >= 0,
        _ => true,
    }
}

fn effective_rtc_transport_for_join_request(
    state: &SignalState,
    request: &proto::JoinRequest,
) -> oxidesfu_rtc::RtcTransportConfig {
    let mut transport = state.rtc_transport_config();

    if !client_supports_ice_tcp(request.client_info.as_ref()) {
        transport.tcp_addrs.clear();
    }

    if request.reconnect
        && request.reconnect_reason == ReconnectReason::RrSwitchCandidate as i32
        && !transport.tcp_addrs.is_empty()
    {
        transport.udp_addrs.clear();
    }
    transport
}

fn client_configuration_for_participant(
    state: &SignalState,
    room_name: &str,
    identity: &str,
) -> Option<proto::ClientConfiguration> {
    let force_relay = state
        .candidate_protocol_preference(room_name, identity)
        .filter(|protocol| *protocol == proto::CandidateProtocol::Tls as i32)
        .map(|_| proto::ClientConfigSetting::Enabled as i32)
        .unwrap_or_default();

    let disabled_codecs = state
        .participant_client_info(room_name, identity)
        .and_then(|client_info| disabled_codecs_for_client_info(&client_info));

    if force_relay == 0 && disabled_codecs.is_none() {
        return None;
    }

    Some(proto::ClientConfiguration {
        disabled_codecs,
        force_relay,
        ..Default::default()
    })
}

fn disabled_codecs_for_client_info(
    client_info: &proto::ClientInfo,
) -> Option<proto::DisabledCodecs> {
    crate::client_configuration::static_configuration_for_client_info(client_info)
        .and_then(|config| config.disabled_codecs)
}

type PeerConnectionKey = (String, String, SignalConnectionTarget);
pub(crate) type SharedPeerConnection = Arc<oxidesfu_rtc::PeerConnection>;
pub(crate) type OutboundSignalSender = RelayOutboundSignalSender;
pub(crate) type DataChannelStore = oxidesfu_rtc::DataChannelStore;
pub(crate) type DataChannelKind = oxidesfu_rtc::DataChannelKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SignalConnectionTarget {
    Publisher,
    Subscriber,
}

impl SignalConnectionTarget {
    pub(crate) fn from_signal_target(target: i32) -> Self {
        if target == proto::SignalTarget::Subscriber as i32 {
            Self::Subscriber
        } else {
            Self::Publisher
        }
    }

    pub(crate) fn as_proto(self) -> i32 {
        match self {
            Self::Publisher => proto::SignalTarget::Publisher as i32,
            Self::Subscriber => proto::SignalTarget::Subscriber as i32,
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct PeerConnectionStore {
    connections: Arc<Mutex<HashMap<PeerConnectionKey, SharedPeerConnection>>>,
}

impl std::fmt::Debug for PeerConnectionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConnectionStore")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MediaForwardingConnectionKind {
    SinglePcPublisher,
    DualPcSubscriber,
}

impl PeerConnectionStore {
    fn insert(
        &self,
        room: &str,
        identity: &str,
        target: SignalConnectionTarget,
        peer_connection: oxidesfu_rtc::PeerConnection,
    ) -> SharedPeerConnection {
        let peer_connection = Arc::new(peer_connection);
        if let Ok(mut connections) = self.connections.lock() {
            connections.insert(
                (room.to_string(), identity.to_string(), target),
                peer_connection.clone(),
            );
        }
        peer_connection
    }

    pub(crate) fn get(
        &self,
        room: &str,
        identity: &str,
        target: SignalConnectionTarget,
    ) -> Option<SharedPeerConnection> {
        self.connections.lock().ok().and_then(|connections| {
            connections
                .get(&(room.to_string(), identity.to_string(), target))
                .cloned()
        })
    }

    pub(crate) fn remove(
        &self,
        room: &str,
        identity: &str,
        target: SignalConnectionTarget,
    ) -> Option<SharedPeerConnection> {
        self.connections.lock().ok().and_then(|mut connections| {
            connections.remove(&(room.to_string(), identity.to_string(), target))
        })
    }

    pub(crate) fn remove_all(&self, room: &str, identity: &str) -> Vec<SharedPeerConnection> {
        self.connections
            .lock()
            .map(|mut connections| {
                [
                    SignalConnectionTarget::Publisher,
                    SignalConnectionTarget::Subscriber,
                ]
                .into_iter()
                .filter_map(|target| {
                    connections.remove(&(room.to_string(), identity.to_string(), target))
                })
                .collect()
            })
            .unwrap_or_default()
    }

    fn has_any(&self, room: &str, identity: &str) -> bool {
        self.get(room, identity, SignalConnectionTarget::Publisher)
            .is_some()
            || self
                .get(room, identity, SignalConnectionTarget::Subscriber)
                .is_some()
    }

    fn media_receiver_for_identity(
        &self,
        room: &str,
        identity: &str,
    ) -> Option<(SharedPeerConnection, MediaForwardingConnectionKind)> {
        self.connections.lock().ok().and_then(|connections| {
            let subscriber_pc = connections
                .get(&(
                    room.to_string(),
                    identity.to_string(),
                    SignalConnectionTarget::Subscriber,
                ))
                .cloned();
            if let Some(subscriber_pc) = subscriber_pc {
                return Some((
                    subscriber_pc,
                    MediaForwardingConnectionKind::DualPcSubscriber,
                ));
            }

            connections
                .get(&(
                    room.to_string(),
                    identity.to_string(),
                    SignalConnectionTarget::Publisher,
                ))
                .cloned()
                .map(|publisher_pc| {
                    (
                        publisher_pc,
                        MediaForwardingConnectionKind::SinglePcPublisher,
                    )
                })
        })
    }

    fn media_receivers_in_room_except(
        &self,
        room: &str,
        excluded_identity: &str,
    ) -> Vec<(String, SharedPeerConnection, MediaForwardingConnectionKind)> {
        self.connections
            .lock()
            .map(|connections| {
                let mut by_identity: HashMap<
                    String,
                    (Option<SharedPeerConnection>, Option<SharedPeerConnection>),
                > = HashMap::new();
                for ((candidate_room, identity, target), peer_connection) in connections.iter() {
                    if candidate_room != room || identity == excluded_identity {
                        continue;
                    }
                    let entry = by_identity.entry(identity.clone()).or_default();
                    match target {
                        SignalConnectionTarget::Publisher => {
                            entry.0 = Some(peer_connection.clone());
                        }
                        SignalConnectionTarget::Subscriber => {
                            entry.1 = Some(peer_connection.clone());
                        }
                    }
                }

                by_identity
                    .into_iter()
                    .filter_map(|(identity, (publisher_pc, subscriber_pc))| {
                        if let Some(subscriber_pc) = subscriber_pc {
                            Some((
                                identity,
                                subscriber_pc,
                                MediaForwardingConnectionKind::DualPcSubscriber,
                            ))
                        } else {
                            publisher_pc.map(|publisher_pc| {
                                (
                                    identity,
                                    publisher_pc,
                                    MediaForwardingConnectionKind::SinglePcPublisher,
                                )
                            })
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ParticipantUpdateHub {
    rooms: Arc<Mutex<HashMap<String, Vec<mpsc::UnboundedSender<proto::SignalResponse>>>>>,
    latest_versions: Arc<Mutex<HashMap<String, HashMap<String, u32>>>>,
}

fn participant_is_hidden(participant: &proto::ParticipantInfo) -> bool {
    participant
        .permission
        .as_ref()
        .map(|permission| permission.hidden)
        .unwrap_or(false)
}

fn participant_update_version_key(participant: &proto::ParticipantInfo) -> Option<String> {
    if !participant.sid.is_empty() {
        return Some(format!("sid:{}", participant.sid));
    }
    if !participant.identity.is_empty() {
        return Some(format!("identity:{}", participant.identity));
    }
    None
}

impl ParticipantUpdateHub {
    pub(crate) fn register(&self, room: &str) -> mpsc::UnboundedReceiver<proto::SignalResponse> {
        let (tx, rx) = mpsc::unbounded_channel();
        if let Ok(mut rooms) = self.rooms.lock() {
            let senders = rooms.entry(room.to_string()).or_default();
            senders.retain(|sender| !sender.is_closed());
            senders.push(tx);
        }
        rx
    }

    pub(crate) fn register_and_broadcast_join(
        &self,
        room: &str,
        participant: proto::ParticipantInfo,
    ) -> mpsc::UnboundedReceiver<proto::SignalResponse> {
        let (tx, rx) = mpsc::unbounded_channel();
        let update = (!participant_is_hidden(&participant)).then(|| proto::SignalResponse {
            message: Some(proto::signal_response::Message::Update(
                proto::ParticipantUpdate {
                    participants: vec![participant],
                },
            )),
        });

        if let Ok(mut rooms) = self.rooms.lock() {
            let senders = rooms.entry(room.to_string()).or_default();
            if let Some(update) = update {
                senders.retain(|sender| sender.send(update.clone()).is_ok());
            } else {
                senders.retain(|sender| !sender.is_closed());
            }
            senders.push(tx);
        }

        rx
    }

    fn should_broadcast_update(&self, room: &str, participant: &proto::ParticipantInfo) -> bool {
        let Some(participant_key) = participant_update_version_key(participant) else {
            return true;
        };

        let Ok(mut latest_versions) = self.latest_versions.lock() else {
            return true;
        };

        let room_versions = latest_versions.entry(room.to_string()).or_default();
        let should_broadcast = room_versions
            .get(&participant_key)
            .is_none_or(|seen_version| participant.version > *seen_version);

        if should_broadcast {
            room_versions.insert(participant_key, participant.version);
        }

        should_broadcast
    }

    pub(crate) fn broadcast_update(&self, room: &str, participant: proto::ParticipantInfo) {
        if participant_is_hidden(&participant)
            && participant.state != proto::participant_info::State::Disconnected as i32
        {
            return;
        }

        if !self.should_broadcast_update(room, &participant) {
            return;
        }

        let update = proto::SignalResponse {
            message: Some(proto::signal_response::Message::Update(
                proto::ParticipantUpdate {
                    participants: vec![participant],
                },
            )),
        };

        if let Ok(mut rooms) = self.rooms.lock()
            && let Some(senders) = rooms.get_mut(room)
        {
            senders.retain(|sender| sender.send(update.clone()).is_ok());
        }
    }
}

#[cfg(test)]
mod participant_update_hub_tests {
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;

    fn participant_with_version(version: u32, metadata: &str) -> proto::ParticipantInfo {
        proto::ParticipantInfo {
            sid: "PA_alice".to_string(),
            identity: "alice".to_string(),
            metadata: metadata.to_string(),
            version,
            state: proto::participant_info::State::Joined as i32,
            ..Default::default()
        }
    }

    #[test]
    fn broadcast_update_suppresses_stale_out_of_order_participant_version() {
        let hub = ParticipantUpdateHub::default();
        let mut updates = hub.register("room");

        hub.broadcast_update("room", participant_with_version(2, "second update"));
        hub.broadcast_update("room", participant_with_version(1, "first update"));

        let update = updates
            .try_recv()
            .expect("latest participant update should be broadcast");
        let Some(proto::signal_response::Message::Update(update)) = update.message else {
            panic!("expected participant update message");
        };

        assert_eq!(update.participants.len(), 1);
        assert_eq!(update.participants[0].metadata, "second update");
        assert_eq!(update.participants[0].version, 2);

        let stale_result = updates.try_recv();
        assert!(matches!(stale_result, Err(TryRecvError::Empty)));
    }

    #[test]
    fn broadcast_update_allows_newer_participant_versions() {
        let hub = ParticipantUpdateHub::default();
        let mut updates = hub.register("room");

        hub.broadcast_update("room", participant_with_version(1, "first update"));
        hub.broadcast_update("room", participant_with_version(2, "second update"));

        let first = updates
            .try_recv()
            .expect("first participant update should be broadcast");
        let second = updates
            .try_recv()
            .expect("second participant update should be broadcast");

        let Some(proto::signal_response::Message::Update(first_update)) = first.message else {
            panic!("expected first participant update message");
        };
        let Some(proto::signal_response::Message::Update(second_update)) = second.message else {
            panic!("expected second participant update message");
        };

        assert_eq!(first_update.participants[0].version, 1);
        assert_eq!(second_update.participants[0].version, 2);
    }
}

/// Builds LiveKit-compatible signalling routes.
pub fn router(state: SignalState) -> Router {
    Router::new()
        .route("/rtc/v1/validate", get(validate_v1))
        .route("/rtc/validate", get(validate_v0))
        .route("/rtc/v1", get(rtc_v1))
        .route("/rtc", get(rtc_v0))
        .with_state(state)
}

fn with_validate_cors_header(mut response: Response) -> Response {
    response.headers_mut().insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        axum::http::HeaderValue::from_static("*"),
    );
    response
}

async fn validate_v1(
    State(state): State<SignalState>,
    headers: HeaderMap,
    Query(query): Query<SignalQuery>,
) -> Response {
    let response = match validate_join(&state, &headers, &query, true) {
        Ok(_) => (StatusCode::OK, "success").into_response(),
        Err(response) => *response,
    };
    with_validate_cors_header(response)
}

async fn validate_v0(
    State(state): State<SignalState>,
    headers: HeaderMap,
    Query(query): Query<SignalQuery>,
) -> Response {
    let response = match authenticate(&state, &headers, &query).and_then(|auth| {
        auth.ensure_join_permission()?;
        Ok(auth)
    }) {
        Ok(_) => (StatusCode::OK, "success").into_response(),
        Err(err) => signal_auth_error(err),
    };
    with_validate_cors_header(response)
}

async fn rtc_v1(
    State(state): State<SignalState>,
    headers: HeaderMap,
    Query(query): Query<SignalQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let join = match validate_join(&state, &headers, &query, true) {
        Ok(join) => join,
        Err(response) => return *response,
    };
    if !state.room_auto_create_on_join()
        && join
            .auth
            .ensure_join_permission()
            .ok()
            .is_some_and(|room| !state.rooms.room_exists(room).unwrap_or(false))
    {
        return StatusCode::NOT_FOUND.into_response();
    }

    ws.on_upgrade(move |socket| async move {
        run_join_socket(socket, state, join.auth, join.request, false, true).await;
    })
}

async fn rtc_v0(
    State(state): State<SignalState>,
    headers: HeaderMap,
    Query(query): Query<SignalQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let has_join_request = query.join_request.is_some();
    let join = match validate_join(&state, &headers, &query, false) {
        Ok(join) => join,
        Err(response) => return *response,
    };
    if !state.room_auto_create_on_join()
        && join
            .auth
            .ensure_join_permission()
            .ok()
            .is_some_and(|room| !state.rooms.room_exists(room).unwrap_or(false))
    {
        return StatusCode::NOT_FOUND.into_response();
    }

    ws.on_upgrade(move |socket| async move {
        run_join_socket(
            socket,
            state,
            join.auth,
            join.request,
            !has_join_request,
            false,
        )
        .await;
    })
}

async fn dispatch_non_local_relay_intent_with_runtime(
    state: SignalState,
    room_name: String,
    auth: AuthContext,
    request: proto::JoinRequest,
    outcome: RoomNodePlacementOutcome,
    subscriber_primary: bool,
) -> RelayDispatchOutcome {
    tokio::task::spawn_blocking(move || {
        dispatch_non_local_relay_intent(
            &state,
            &room_name,
            &auth,
            &request,
            &outcome,
            subscriber_primary,
        )
    })
    .await
    .unwrap_or_else(|err| {
        RelayDispatchOutcome::Failed(format!("relay dispatch task join failure: {err}"))
    })
}

async fn run_join_socket(
    mut socket: WebSocket,
    state: SignalState,
    auth: AuthContext,
    request: proto::JoinRequest,
    subscriber_primary: bool,
    is_rtc_v1: bool,
) {
    let room_name = match auth.ensure_join_permission() {
        Ok(room) => room.to_string(),
        Err(_) => return,
    };
    let identity = auth.participant_identity().to_string();

    let placement_outcome = room_node_placement_outcome(&state, &room_name);
    let relay_outcome = dispatch_non_local_relay_intent_with_runtime(
        state.clone(),
        room_name.clone(),
        auth.clone(),
        request.clone(),
        placement_outcome.clone(),
        subscriber_primary,
    )
    .await;

    match placement_outcome {
        RoomNodePlacementOutcome::LocalHandling => {}
        RoomNodePlacementOutcome::NonLocalNeedsRelay {
            selected_room_node_id,
        } if state.reject_non_local_room_placement => {
            tracing::warn!(
                room = %room_name,
                local_room_node_id = ?state.local_room_node_id,
                selected_room_node_id,
                "room_node_assignment_non_local_relay_needed_but_rejected"
            );
            let _ = socket.close().await;
            return;
        }
        RoomNodePlacementOutcome::NonLocalNeedsRelay {
            selected_room_node_id,
        } => match relay_outcome {
            RelayDispatchOutcome::Responded(NonLocalRelayJoinResponse::Rejected { code, msg }) => {
                inc_responses_rejected();
                tracing::warn!(
                    room = %room_name,
                    local_room_node_id = ?state.local_room_node_id,
                    selected_room_node_id,
                    code,
                    msg,
                    "room_node_assignment_non_local_relay_rejected"
                );
                let _ = socket.close().await;
                return;
            }
            RelayDispatchOutcome::Responded(
                response @ NonLocalRelayJoinResponse::Accepted { .. },
            )
            | RelayDispatchOutcome::Responded(
                response @ NonLocalRelayJoinResponse::AcceptedWithJoin { .. },
            )
            | RelayDispatchOutcome::Responded(
                response @ NonLocalRelayJoinResponse::AcceptedWithJoinAndSignals { .. },
            ) => {
                inc_responses_accepted();
                let initial_signal_responses = non_local_relay_initial_signal_responses(&response);
                if let Some(join) = non_local_relay_join_response_shape(&response) {
                    run_non_local_relay_accepted_socket_loop(
                        socket,
                        state,
                        room_name,
                        identity.clone(),
                        selected_room_node_id,
                        join,
                        initial_signal_responses,
                        request.publisher_offer.clone(),
                    )
                    .await;
                    return;
                }
            }
            RelayDispatchOutcome::Failed(error) => {
                inc_fallback_to_local();
                tracing::warn!(
                    room = %room_name,
                    local_room_node_id = ?state.local_room_node_id,
                    selected_room_node_id,
                    error,
                    "room_node_assignment_non_local_relay_dispatch_failed_falling_back_to_local_join"
                );
            }
            RelayDispatchOutcome::NoResponse | RelayDispatchOutcome::NotAttempted => {
                inc_fallback_to_local();
                tracing::warn!(
                    room = %room_name,
                    local_room_node_id = ?state.local_room_node_id,
                    selected_room_node_id,
                    "room_node_assignment_non_local_relay_needed_falling_back_to_local_join"
                );
            }
        },
        RoomNodePlacementOutcome::DirectorySelectionFailed { error } => {
            tracing::warn!(
                room = %room_name,
                error,
                "room_node_assignment_failed_falling_back_to_local_join"
            );
        }
    }
    let auto_subscribe = request
        .connection_settings
        .as_ref()
        .map(|settings| settings.auto_subscribe)
        .unwrap_or(true);
    let auto_subscribe_data_track = request
        .connection_settings
        .as_ref()
        .and_then(|settings| settings.auto_subscribe_data_track)
        .unwrap_or(true);

    let reconnect_participant_sid = if request.reconnect {
        let participant = state.rooms.get_participant(&room_name, &identity).ok();
        let participant_sid = participant
            .as_ref()
            .map(|participant| participant.sid.as_str());
        let participant_sid_matches = participant_sid == Some(request.participant_sid.as_str());
        let has_resumable_peer_connections = state.peer_connections.has_any(&room_name, &identity);
        if !participant_sid_matches {
            let leave = proto::SignalResponse {
                message: Some(proto::signal_response::Message::Leave(
                    proto::LeaveRequest {
                        reason: proto::DisconnectReason::StateMismatch as i32,
                        action: proto::leave_request::Action::Disconnect as i32,
                        ..Default::default()
                    },
                )),
            };
            let _ = socket
                .send(WsMessage::Binary(leave.encode_to_vec().into()))
                .await;
            let _ = socket.send(WsMessage::Close(None)).await;
            return;
        }

        if !has_resumable_peer_connections {
            let leave = proto::SignalResponse {
                message: Some(proto::signal_response::Message::Leave(
                    proto::LeaveRequest {
                        reason: proto::DisconnectReason::StateMismatch as i32,
                        action: proto::leave_request::Action::Reconnect as i32,
                        ..Default::default()
                    },
                )),
            };
            let _ = socket
                .send(WsMessage::Binary(leave.encode_to_vec().into()))
                .await;
            let _ = socket.send(WsMessage::Close(None)).await;
            return;
        }

        participant_sid.unwrap_or(identity.as_str()).to_string()
    } else {
        String::new()
    };

    let effective_subscriber_primary = subscriber_primary && auth.claims.video.get_can_subscribe();
    let reconnecting = request.reconnect;
    if !reconnecting {
        // Keep the requested transport topology even when the initial token denies
        // subscriptions. A later RoomService permission grant must reconcile on the
        // same topology and may need to create its deferred subscriber transport.
        state.remember_participant_subscriber_primary(&room_name, &identity, subscriber_primary);
    }
    if !reconnecting {
        apply_room_config_on_first_room_creation_from_token(
            &state,
            &room_name,
            auth.claims.room_config.as_ref(),
        );
    }
    state.remember_participant_client_info(&room_name, &identity, request.client_info.clone());
    let effective_transport = effective_rtc_transport_for_join_request(&state, &request);
    let add_track_requests = request.add_track_requests;
    let publisher_offer = request.publisher_offer;
    if let Some(offer) = publisher_offer.as_ref() {
        // A v0 client sends its publisher offer in the join request. Record the
        // receive codecs before constructing the server-offered subscriber PC,
        // so its initial forwarding tracks use a codec the client accepts.
        state.remember_participant_subscribe_video_mime_types(
            &room_name,
            &identity,
            &receive_supported_video_mime_types_from_offer(&offer.sdp),
        );
    }
    let response = if reconnecting {
        proto::SignalResponse {
            message: Some(proto::signal_response::Message::Reconnect(
                proto::ReconnectResponse {
                    ice_servers: state.ice_servers(&reconnect_participant_sid),
                    client_configuration: client_configuration_for_participant(
                        &state, &room_name, &identity,
                    ),
                    server_info: Some(join_server_info()),
                    ..Default::default()
                },
            )),
        }
    } else {
        if state.rooms.get_participant(&room_name, &identity).is_ok() {
            let _ = state
                .disconnect_participant_from_service(
                    &room_name,
                    &identity,
                    proto::DisconnectReason::DuplicateIdentity,
                )
                .await;
        }

        let metadata = if request.metadata.is_empty() {
            auth.claims.metadata.clone()
        } else {
            request.metadata
        };
        let mut attributes = auth.claims.attributes.clone();
        attributes.extend(request.participant_attributes);

        let can_publish_sources = auth
            .claims
            .video
            .can_publish_sources
            .iter()
            .filter_map(|source| match source.to_ascii_lowercase().as_str() {
                "camera" => Some(proto::TrackSource::Camera as i32),
                "microphone" => Some(proto::TrackSource::Microphone as i32),
                "screen_share" => Some(proto::TrackSource::ScreenShare as i32),
                "screen_share_audio" => Some(proto::TrackSource::ScreenShareAudio as i32),
                _ => None,
            })
            .collect::<Vec<_>>();
        let permissions = proto::ParticipantPermission {
            can_subscribe: auth.claims.video.get_can_subscribe(),
            can_publish: auth.claims.video.get_can_publish(),
            can_publish_data: auth.claims.video.get_can_publish_data(),
            can_publish_sources,
            can_update_metadata: auth.claims.video.get_can_update_own_metadata(),
            hidden: auth.claims.video.hidden,
            can_subscribe_metrics: false,
            can_manage_agent_session: false,
            ..Default::default()
        };

        if !state.room_auto_create_on_join()
            && !state.rooms.room_exists(&room_name).unwrap_or(false)
        {
            tracing::debug!(
                room = %room_name,
                identity = %identity,
                "join_rejected_room_not_created_and_auto_create_disabled"
            );
            return;
        }

        let Ok((room, participant, other_participants)) =
            state.rooms.join_participant_with_permission(
                &room_name,
                &identity,
                &auth.claims.name,
                metadata,
                attributes,
                Some(permissions),
            )
        else {
            return;
        };

        let participant_kind = match auth.claims.kind.to_ascii_uppercase().as_str() {
            "INGRESS" => proto::participant_info::Kind::Ingress as i32,
            "EGRESS" => proto::participant_info::Kind::Egress as i32,
            "SIP" => proto::participant_info::Kind::Sip as i32,
            "AGENT" => proto::participant_info::Kind::Agent as i32,
            "CONNECTOR" => proto::participant_info::Kind::Connector as i32,
            "BRIDGE" => proto::participant_info::Kind::Bridge as i32,
            _ => proto::participant_info::Kind::Standard as i32,
        };
        let participant_kind_details = auth
            .claims
            .kind_details
            .iter()
            .filter_map(|detail| match detail.to_ascii_uppercase().as_str() {
                "CLOUD_AGENT" => Some(proto::participant_info::KindDetail::CloudAgent as i32),
                "FORWARDED" => Some(proto::participant_info::KindDetail::Forwarded as i32),
                "CONNECTOR_WHATSAPP" => {
                    Some(proto::participant_info::KindDetail::ConnectorWhatsapp as i32)
                }
                "CONNECTOR_TWILIO" => {
                    Some(proto::participant_info::KindDetail::ConnectorTwilio as i32)
                }
                "BRIDGE_RTSP" => Some(proto::participant_info::KindDetail::BridgeRtsp as i32),
                _ => None,
            })
            .collect::<Vec<_>>();

        let participant = state
            .rooms
            .set_participant_kind(
                &room_name,
                &identity,
                participant_kind,
                participant_kind_details,
            )
            .unwrap_or(participant);

        if !auto_subscribe {
            for existing in &other_participants {
                for track in &existing.tracks {
                    if track.sid.is_empty() {
                        continue;
                    }
                    state.media_subscriptions.set_subscribed(
                        &room_name,
                        &existing.identity,
                        &track.sid,
                        &identity,
                        false,
                    );
                }
            }
        }

        let first_participant_in_room = other_participants.is_empty();
        emit_join_webhook_events(&state, &room, &participant, first_participant_in_room);

        let other_participants = other_participants
            .into_iter()
            .filter(|participant| !participant_is_hidden(participant))
            .collect();

        proto::SignalResponse {
            message: Some(proto::signal_response::Message::Join(proto::JoinResponse {
                room: Some(room),
                participant: Some(participant.clone()),
                other_participants,
                server_version: env!("CARGO_PKG_VERSION").to_string(),
                ping_interval: PING_INTERVAL_SECONDS,
                ping_timeout: PING_TIMEOUT_SECONDS,
                subscriber_primary: effective_subscriber_primary,
                ice_servers: state.ice_servers(&participant.sid),
                client_configuration: client_configuration_for_participant(
                    &state, &room_name, &identity,
                ),
                server_info: Some(join_server_info()),
                ..Default::default()
            })),
        }
    };

    let (mut sender, mut receiver) = socket.split();
    if sender
        .send(WsMessage::Binary(response.encode_to_vec().into()))
        .await
        .is_err()
    {
        return;
    }

    let mut updates = if reconnecting {
        state.updates.register(&room_name)
    } else {
        let participant = match state.rooms.get_participant(&room_name, &identity) {
            Ok(participant) => participant,
            Err(_) => return,
        };
        state
            .updates
            .register_and_broadcast_join(&room_name, participant)
    };
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<proto::SignalResponse>();
    state
        .signal_connections
        .insert(&room_name, &identity, outbound_tx.clone());

    state.remember_participant_auth_context(&room_name, &identity, &auth);
    state.publish_permissions.set_can_publish_media(
        &room_name,
        &identity,
        auth.claims.video.get_can_publish(),
    );
    state.publish_permissions.set_can_publish_data(
        &room_name,
        &identity,
        auth.claims.video.get_can_publish_data(),
    );
    state.publish_permissions.set_can_publish_sources(
        &room_name,
        &identity,
        &auth.claims.video.can_publish_sources,
    );
    state.subscribe_permissions.set_can_subscribe(
        &room_name,
        &identity,
        auth.claims.video.get_can_subscribe(),
    );
    state.set_auto_subscribe_preference(&room_name, &identity, auto_subscribe);
    state.set_auto_subscribe_data_track_preference(
        &room_name,
        &identity,
        auto_subscribe_data_track,
    );

    let has_existing_subscriber_pc = state
        .peer_connections
        .get(&room_name, &identity, SignalConnectionTarget::Subscriber)
        .is_some();
    let should_create_subscriber_offer =
        effective_subscriber_primary && (!reconnecting || !has_existing_subscriber_pc);
    if !should_create_subscriber_offer {
        crate::router::session::reconcile_subscriber_data_track_subscriptions(
            &state, &room_name, &identity,
        );
    }
    if should_create_subscriber_offer {
        match create_subscriber_offer(
            &state,
            &room_name,
            &identity,
            &outbound_tx,
            &effective_transport,
        )
        .await
        {
            Ok(response) => {
                if sender
                    .send(WsMessage::Binary(response.encode_to_vec().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(error) => {
                tracing::warn!(
                    room = %room_name,
                    identity = %identity,
                    reconnect = reconnecting,
                    has_existing_subscriber_pc,
                    error = %error,
                    "failed_to_create_subscriber_offer"
                );
                return;
            }
        }
    }

    for add_track_request in add_track_requests {
        let track_published =
            add_track_response(&state, &room_name, &identity, add_track_request).await;
        if sender
            .send(WsMessage::Binary(track_published.encode_to_vec().into()))
            .await
            .is_err()
        {
            return;
        }
    }

    if let Some(offer) = publisher_offer {
        match answer_publisher_offer(
            offer,
            &state,
            &room_name,
            &identity,
            &outbound_tx,
            &effective_transport,
        )
        .await
        {
            Ok(response) => {
                if sender
                    .send(WsMessage::Binary(response.encode_to_vec().into()))
                    .await
                    .is_err()
                {
                    return;
                }
                activate_forward_tracks_after_sent_response(
                    &state, &room_name, &identity, &response,
                );
            }
            Err(error) => {
                tracing::warn!(
                    room = %room_name,
                    identity = %identity,
                    error = %error,
                    "failed_to_answer_initial_publisher_offer"
                );
                return;
            }
        }
    }

    ensure_existing_media_forwarding_for_subscriber(&state, &room_name, &identity).await;

    let mut saw_explicit_leave = false;
    let loop_exit_reason = loop {
        tokio::select! {
            Some(update) = updates.recv() => {
                if sender
                    .send(WsMessage::Binary(update.encode_to_vec().into()))
                    .await
                    .is_err()
                {
                    break "failed_to_send_participant_update";
                }
            }
            Some(response) = outbound_rx.recv() => {
                if let Some(proto::signal_response::Message::SubscriptionResponse(subscription_response)) = response.message.as_ref()
                    && subscription_response.err == proto::SubscriptionError::SeCodecUnsupported as i32
                {
                    tracing::debug!(
                        room = %room_name,
                        identity = %identity,
                        track_sid = %subscription_response.track_sid,
                        "socket_sending_subscription_unsupported_codec_response"
                    );
                }
                let mut failed_to_send_update = false;
                while let Ok(update) = updates.try_recv() {
                    if sender
                        .send(WsMessage::Binary(update.encode_to_vec().into()))
                        .await
                        .is_err()
                    {
                        failed_to_send_update = true;
                        break;
                    }
                }
                if failed_to_send_update {
                    break "failed_to_send_participant_update";
                }
                if sender
                    .send(WsMessage::Binary(response.encode_to_vec().into()))
                    .await
                    .is_err()
                {
                    tracing::debug!(
                        room = %room_name,
                        identity = %identity,
                        "socket_send_failed_closing_join_loop"
                    );
                    break "failed_to_send_outbound_response";
                }
            }
            next = receiver.next() => {
                let Some(next) = next else {
                    tracing::debug!(
                        room = %room_name,
                        identity = %identity,
                        "socket_receiver_stream_ended"
                    );
                    break "socket_receiver_stream_ended";
                };
                let message = match next {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::warn!(
                            room = %room_name,
                            identity = %identity,
                            error = %error,
                            "socket_receiver_error_closing_join_loop"
                        );
                        break "socket_receiver_error";
                    }
                };
                if !state
                    .signal_connections
                    .is_same(&room_name, &identity, &outbound_tx)
                {
                    tracing::debug!(
                        room = %room_name,
                        identity = %identity,
                        "ignoring_message_from_stale_socket_session"
                    );
                    if is_ping_req_signal_request_message(&message) {
                        let _ = sender.send(WsMessage::Close(None)).await;
                        break "stale_socket_ping_after_replacement";
                    }
                    continue;
                }
                if is_leave_signal_request_message(&message) || matches!(message, WsMessage::Close(_)) {
                    saw_explicit_leave = true;
                }
                if reconnecting && !is_rtc_v1 && is_ping_req_signal_request_message(&message) {
                    continue;
                }
                match handle_socket_message(message, &state, &room_name, &identity, &outbound_tx).await {
                    Ok(Some(response)) => {
                        let typed_response = response.typed().cloned();
                        if sender
                            .send(WsMessage::Binary(response.encode().into()))
                            .await
                            .is_err()
                        {
                            break "failed_to_send_direct_signal_response";
                        }
                        if let Some(response) = typed_response.as_ref() {
                            activate_forward_tracks_after_sent_response(&state, &room_name, &identity, response);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if error.is_participant_left() {
                            tracing::info!(
                                room = %room_name,
                                identity = %identity,
                                "signal_leave_request_handled_closing_socket"
                            );
                            let _ = sender.send(WsMessage::Close(None)).await;
                            break "signal_request_participant_left";
                        }

                        if error.is_terminal_for_socket_loop() {
                            tracing::warn!(
                                room = %room_name,
                                identity = %identity,
                                error = %error,
                                "signal_request_handling_failed_closing_socket"
                            );
                            break "signal_request_handling_failed";
                        }

                        tracing::warn!(
                            room = %room_name,
                            identity = %identity,
                            error = %error,
                            "signal_request_handling_failed_ignoring"
                        );
                    }
                }
            }
        }
    };

    cleanup_if_active_session(
        &state,
        &room_name,
        &identity,
        &outbound_tx,
        saw_explicit_leave,
    )
    .await;

    tracing::info!(
        room = %room_name,
        identity = %identity,
        reconnect = reconnecting,
        saw_explicit_leave,
        loop_exit_reason,
        "join_socket_closed"
    );
}

async fn cleanup_if_active_session(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    outbound_tx: &OutboundSignalSender,
    remove_participant_from_room: bool,
) {
    let is_active_session =
        state
            .signal_connections
            .remove_if_same(room_name, identity, outbound_tx);
    if !is_active_session {
        return;
    }

    // Data channels are bound to the active websocket session and should be
    // removed as soon as that signaling session closes, even if we retain
    // peer connections for reconnect grace behavior.
    state.data_channels.remove(room_name, identity);

    if remove_participant_from_room {
        cleanup_participant_runtime_state(state, room_name, identity, true).await;
        return;
    }

    if !state.peer_connections.has_any(room_name, identity) {
        cleanup_participant_runtime_state(state, room_name, identity, true).await;
        return;
    }

    let departing_participant_sid = state
        .rooms
        .get_participant(room_name, identity)
        .ok()
        .map(|participant| participant.sid)
        .filter(|sid| !sid.is_empty());
    // Retain peer connections through the reconnect grace period, but remove
    // published tracks immediately so subscribers receive unpublish updates.
    // A reconnect can publish fresh tracks using the retained transport.
    unpublish_participant_tracks_for_reconnect_grace(state, room_name, identity).await;

    let state = state.clone();
    let room_name = room_name.to_string();
    let identity = identity.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(state.reconnect_participant_retention_grace()).await;
        if state
            .signal_connections
            .get(&room_name, &identity)
            .is_some()
        {
            return;
        }

        if let Some(departing_participant_sid) = departing_participant_sid {
            cleanup_participant_runtime_state_for_participant_sid(
                &state,
                &room_name,
                &identity,
                &departing_participant_sid,
                true,
            )
            .await;
        }
    });
}

async fn unpublish_participant_tracks_for_reconnect_grace(
    state: &SignalState,
    room_name: &str,
    identity: &str,
) {
    let departing_participant = state.rooms.get_participant(room_name, identity).ok();
    let departing_publisher_sid = departing_participant
        .as_ref()
        .map(|participant| participant.sid.clone())
        .filter(|sid| !sid.is_empty());
    let departing_tracks = departing_participant
        .as_ref()
        .map(|participant| participant.tracks.clone())
        .unwrap_or_default();

    if departing_tracks.is_empty() {
        remove_orphaned_forward_tracks_for_departing_publisher(
            state,
            room_name,
            identity,
            departing_publisher_sid.as_deref(),
        )
        .await;
        return;
    }

    let subscribers = state.rooms.list_participants(room_name).unwrap_or_default();
    let mut subscribers_needing_renegotiation = HashSet::new();
    for subscriber in &subscribers {
        if subscriber.identity == identity {
            continue;
        }
        for track in &departing_tracks {
            if session::remove_subscriber_media_forwarding_for_track_without_negotiation(
                state,
                room_name,
                identity,
                &subscriber.identity,
                track,
            )
            .await
            .unwrap_or(false)
            {
                subscribers_needing_renegotiation.insert(subscriber.identity.clone());
            }
        }
    }
    signal_consolidated_forwarding_cleanup_negotiation(
        state,
        room_name,
        subscribers_needing_renegotiation,
    )
    .await;

    let room_snapshot = state
        .rooms
        .list_rooms(&[room_name.to_string()])
        .ok()
        .and_then(|mut rooms| rooms.pop());

    for track in departing_tracks {
        state
            .media_track_cids
            .remove_track_sid(room_name, identity, &track.sid);

        if let Ok(participant) = state
            .rooms
            .remove_participant_track(room_name, identity, &track.sid)
        {
            if let Some(room) = room_snapshot.as_ref() {
                state.emit_webhook_event(proto::WebhookEvent {
                    event: "track_unpublished".to_string(),
                    room: Some(reduced_room_for_track_event(room)),
                    participant: Some(reduced_participant_for_track_event(&participant)),
                    track: Some(track.clone()),
                    id: next_webhook_event_id(),
                    created_at: webhook_created_at_unix_seconds(),
                    ..Default::default()
                });
            }

            state.updates.broadcast_update(room_name, participant);
        }
    }

    remove_orphaned_forward_tracks_for_departing_publisher(
        state,
        room_name,
        identity,
        departing_publisher_sid.as_deref(),
    )
    .await;
}

async fn signal_consolidated_forwarding_cleanup_negotiation(
    state: &SignalState,
    room_name: &str,
    subscribers: HashSet<String>,
) {
    for subscriber_identity in subscribers {
        let Some((subscriber_pc, connection_kind)) = state
            .peer_connections
            .media_receiver_for_identity(room_name, &subscriber_identity)
        else {
            continue;
        };
        if connection_kind == MediaForwardingConnectionKind::SinglePcPublisher {
            continue;
        }
        let Some(subscriber_outbound_tx) = state
            .signal_connections
            .get(room_name, &subscriber_identity)
        else {
            continue;
        };

        let _ = session::signal_media_forwarding_negotiation_with_offer_id(
            state,
            &state.subscriber_offer_ids,
            room_name,
            &subscriber_identity,
            &subscriber_pc,
            connection_kind,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
            &subscriber_outbound_tx,
        )
        .await;
    }
}

async fn remove_orphaned_forward_tracks_for_departing_publisher(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    publisher_sid: Option<&str>,
) {
    let removed_tracks = state
        .forward_tracks
        .remove_all_for_publisher(room_name, publisher_identity);

    let mut subscribers_needing_renegotiation = HashSet::new();

    for (track_sid, subscriber_identity, local_forward_track) in removed_tracks {
        state.media_forwarding.remove(
            room_name,
            publisher_identity,
            &track_sid,
            &subscriber_identity,
        );
        state.rtp_forwarding.remove(
            room_name,
            publisher_identity,
            &track_sid,
            &subscriber_identity,
        );

        let _ = state.rooms.set_media_track_subscribed(
            room_name,
            publisher_identity,
            &track_sid,
            &subscriber_identity,
            false,
        );

        let Some((subscriber_pc, connection_kind)) = state
            .peer_connections
            .media_receiver_for_identity(room_name, &subscriber_identity)
        else {
            continue;
        };

        let _ = subscriber_pc
            .remove_forwarding_track(&local_forward_track)
            .await;

        if connection_kind != MediaForwardingConnectionKind::SinglePcPublisher {
            subscribers_needing_renegotiation.insert(subscriber_identity);
        }
    }

    if let Some(publisher_sid) = publisher_sid.filter(|sid| !sid.is_empty()) {
        for subscriber in state.rooms.list_participants(room_name).unwrap_or_default() {
            if subscriber.identity == publisher_identity {
                continue;
            }

            let Some((subscriber_pc, connection_kind)) = state
                .peer_connections
                .media_receiver_for_identity(room_name, &subscriber.identity)
            else {
                continue;
            };

            let removed = subscriber_pc
                .remove_forwarding_tracks_for_publisher(publisher_sid)
                .await
                .unwrap_or_default();

            if removed > 0 && connection_kind != MediaForwardingConnectionKind::SinglePcPublisher {
                subscribers_needing_renegotiation.insert(subscriber.identity.clone());
            }
        }
    }

    for subscriber_identity in subscribers_needing_renegotiation {
        let Some((subscriber_pc, connection_kind)) = state
            .peer_connections
            .media_receiver_for_identity(room_name, &subscriber_identity)
        else {
            continue;
        };

        if connection_kind == MediaForwardingConnectionKind::SinglePcPublisher {
            continue;
        }

        let Some(subscriber_outbound_tx) = state
            .signal_connections
            .get(room_name, &subscriber_identity)
        else {
            continue;
        };

        for track_kind in [
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
        ] {
            let _ = session::signal_media_forwarding_negotiation_with_offer_id(
                state,
                &state.subscriber_offer_ids,
                room_name,
                &subscriber_identity,
                &subscriber_pc,
                connection_kind,
                track_kind,
                &subscriber_outbound_tx,
            )
            .await;
        }
    }
}

async fn cleanup_participant_runtime_state_for_participant_sid(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    participant_sid: &str,
    remove_participant_from_room: bool,
) -> bool {
    let is_current_participant = state
        .rooms
        .get_participant(room_name, identity)
        .map(|participant| participant.sid == participant_sid)
        .unwrap_or(false);
    if !is_current_participant {
        tracing::debug!(
            room = room_name,
            identity,
            cleanup_participant_sid = participant_sid,
            current_participant_sid = ?state
                .rooms
                .get_participant(room_name, identity)
                .ok()
                .map(|participant| participant.sid),
            "skipping_stale_participant_runtime_cleanup"
        );
        return false;
    }

    cleanup_participant_runtime_state(state, room_name, identity, remove_participant_from_room)
        .await;
    true
}

pub(crate) async fn cleanup_participant_runtime_state(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    remove_participant_from_room: bool,
) {
    let departing_participant = if remove_participant_from_room {
        state.rooms.get_participant(room_name, identity).ok()
    } else {
        None
    };

    let departing_tracks = departing_participant
        .as_ref()
        .map(|participant| participant.tracks.clone())
        .unwrap_or_default();
    let departing_track_settings_sids = state
        .track_settings
        .track_sids_for_participant(room_name, identity);
    let departing_publisher_sid = departing_participant
        .as_ref()
        .map(|participant| participant.sid.as_str());

    let mut subscribers_needing_renegotiation = HashSet::new();
    if !departing_tracks.is_empty() {
        let subscribers = state.rooms.list_participants(room_name).unwrap_or_default();
        for subscriber in subscribers {
            if subscriber.identity == identity {
                continue;
            }
            for track in &departing_tracks {
                if session::remove_subscriber_media_forwarding_for_track_without_negotiation(
                    state,
                    room_name,
                    identity,
                    &subscriber.identity,
                    track,
                )
                .await
                .unwrap_or(false)
                {
                    subscribers_needing_renegotiation.insert(subscriber.identity.clone());
                }
            }
        }
    }
    signal_consolidated_forwarding_cleanup_negotiation(
        state,
        room_name,
        subscribers_needing_renegotiation,
    )
    .await;

    remove_orphaned_forward_tracks_for_departing_publisher(
        state,
        room_name,
        identity,
        departing_publisher_sid,
    )
    .await;

    state.data_channels.remove(room_name, identity);
    state.data_tracks.remove_participant(room_name, identity);
    state
        .data_track_subscriptions
        .remove_participant(room_name, identity);
    for subscriber in state.rooms.list_participants(room_name).unwrap_or_default() {
        if subscriber.identity != identity {
            session::reconcile_subscriber_data_track_subscriptions(
                state,
                room_name,
                &subscriber.identity,
            );
        }
    }
    state
        .publish_permissions
        .remove_participant(room_name, identity);
    state
        .subscribe_permissions
        .remove_participant(room_name, identity);
    state
        .auto_subscribe_preferences
        .remove_participant(room_name, identity);
    state.track_settings.remove_participant(room_name, identity);
    state
        .media_forwarding
        .remove_participant(room_name, identity);
    state
        .pending_media_section_requests
        .remove_participant(room_name, identity);
    state
        .subscriber_offer_ids
        .remove_participant(room_name, identity);
    state
        .subscriber_offer_negotiations
        .remove_participant(room_name, identity);
    state.forget_subscriber_offer_mid_track_ids(room_name, identity);
    state.forget_participant_subscriber_primary(room_name, identity);
    state
        .single_pc_offer_media_kinds
        .remove_participant(room_name, identity);
    state
        .media_subscriptions
        .remove_participant(room_name, identity);
    state.clear_publisher_subscription_active_for_participant(room_name, identity);

    for track_sid in departing_track_settings_sids {
        if let Some((publisher_identity, track)) =
            find_media_track_publisher(state, room_name, &track_sid)
        {
            session::emit_aggregate_subscribed_quality_update_for_track(
                state,
                room_name,
                &publisher_identity,
                &track,
            );
        }
    }

    state
        .media_track_cids
        .remove_participant(room_name, identity);
    state
        .pending_remote_tracks
        .remove_participant(room_name, identity);
    state.forward_tracks.remove_participant(room_name, identity);
    state.rtp_forwarding.remove_participant(room_name, identity);
    for peer_connection in state.peer_connections.remove_all(room_name, identity) {
        let _ = peer_connection.close().await;
    }

    if remove_participant_from_room {
        state.forget_participant_auth_context(room_name, identity);
        state.forget_participant_client_info(room_name, identity);
        state.forget_participant_subscribe_video_mime_types(room_name, identity);
        state.forget_participant_data_blobs(room_name, identity);
    }

    if remove_participant_from_room
        && let Ok(participant) = state.rooms.remove_participant(room_name, identity)
    {
        let room_snapshot = state
            .rooms
            .list_rooms(&[room_name.to_string()])
            .ok()
            .and_then(|mut rooms| rooms.pop());

        if let Some(room) = room_snapshot.as_ref() {
            for track in &participant.tracks {
                state.emit_webhook_event(proto::WebhookEvent {
                    event: "track_unpublished".to_string(),
                    room: Some(reduced_room_for_track_event(room)),
                    participant: Some(reduced_participant_for_track_event(&participant)),
                    track: Some(track.clone()),
                    id: next_webhook_event_id(),
                    created_at: webhook_created_at_unix_seconds(),
                    ..Default::default()
                });
            }
            state.emit_webhook_event(proto::WebhookEvent {
                event: "participant_left".to_string(),
                room: Some(room.clone()),
                participant: Some(participant.clone()),
                id: next_webhook_event_id(),
                created_at: webhook_created_at_unix_seconds(),
                ..Default::default()
            });
        }

        state.updates.broadcast_update(room_name, participant);
    }
}

fn response_activates_forward_tracks(response: &proto::SignalResponse) -> bool {
    matches!(
        response.message,
        Some(proto::signal_response::Message::Answer(_))
    )
}

const FORWARD_TRACK_ACTIVATION_AFTER_ANSWER_DELAY: Duration = Duration::from_millis(50);

fn activate_forward_tracks_after_sent_response(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    response: &proto::SignalResponse,
) {
    if !response_activates_forward_tracks(response) {
        return;
    }

    let (answer_id, answer_mid_to_track_id, track_sids_in_answer, accepted_mids_in_answer) =
        match &response.message {
            Some(proto::signal_response::Message::Answer(answer)) => {
                let mapping = mid_to_track_id_from_answer_sdp(&answer.sdp);
                let mut track_sids = mapping.values().cloned().collect::<HashSet<_>>();
                let accepted_mids = accepted_media_mids_from_answer_sdp(&answer.sdp);
                if track_sids.is_empty() {
                    track_sids = state
                        .forward_tracks
                        .subscriber_track_sids_for_forwarding_mids(
                            room_name,
                            identity,
                            &accepted_mids,
                        );
                }
                (answer.id, mapping, track_sids, accepted_mids)
            }
            _ => (0, HashMap::new(), HashSet::new(), HashSet::new()),
        };

    tracing::debug!(
        room = room_name,
        identity,
        answer_id,
        answer_mid_to_track_id = ?answer_mid_to_track_id,
        accepted_mids_in_answer = ?accepted_mids_in_answer,
        track_sids_in_answer = ?track_sids_in_answer,
        delay_ms = FORWARD_TRACK_ACTIVATION_AFTER_ANSWER_DELAY.as_millis(),
        "forward_track_activation_after_answer_scheduled"
    );

    let forward_tracks = state.forward_tracks.clone();
    let room_name = room_name.to_string();
    let identity = identity.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(FORWARD_TRACK_ACTIVATION_AFTER_ANSWER_DELAY).await;
        tracing::debug!(
            room = %room_name,
            identity = %identity,
            answer_id,
            answer_mid_to_track_id = ?answer_mid_to_track_id,
            accepted_mids_in_answer = ?accepted_mids_in_answer,
            track_sids_in_answer = ?track_sids_in_answer,
            scoped = !track_sids_in_answer.is_empty(),
            "forward_track_activation_after_answer_firing"
        );
        if track_sids_in_answer.is_empty() {
            tracing::debug!(
                room = %room_name,
                identity = %identity,
                answer_id,
                "forward_track_activation_after_answer_skipped_empty_track_sid_set"
            );
            return;
        }

        forward_tracks.activate_subscriber_track_sids(&room_name, &identity, &track_sids_in_answer);
    });
}

fn is_leave_signal_request_message(message: &WsMessage) -> bool {
    let WsMessage::Binary(bytes) = message else {
        return false;
    };
    let Ok(request) = proto::SignalRequest::decode(bytes.as_ref()) else {
        return false;
    };
    matches!(
        request.message,
        Some(proto::signal_request::Message::Leave(_))
    )
}

fn is_ping_req_signal_request_message(message: &WsMessage) -> bool {
    let WsMessage::Binary(bytes) = message else {
        return false;
    };
    let Ok(request) = proto::SignalRequest::decode(bytes.as_ref()) else {
        return false;
    };
    matches!(
        request.message,
        Some(proto::signal_request::Message::PingReq(_))
    )
}

async fn run_non_local_relay_accepted_socket_loop(
    socket: WebSocket,
    state: SignalState,
    room_name: String,
    identity: String,
    selected_room_node_id: String,
    join: proto::JoinResponse,
    initial_signal_responses: Vec<Vec<u8>>,
    initial_publisher_offer: Option<proto::SessionDescription>,
) {
    let participant_sid = join
        .participant
        .as_ref()
        .map(|participant| participant.sid.clone())
        .unwrap_or_default();
    let (mut sender, mut receiver) = socket.split();
    let join_response = proto::SignalResponse {
        message: Some(proto::signal_response::Message::Join(join)),
    };

    if sender
        .send(WsMessage::Binary(join_response.encode_to_vec().into()))
        .await
        .is_err()
    {
        return;
    }
    for response in initial_signal_responses {
        if sender
            .send(WsMessage::Binary(response.into()))
            .await
            .is_err()
        {
            return;
        }
    }

    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<proto::SignalResponse>();
    state
        .signal_connections
        .insert(&room_name, &identity, outbound_tx.clone());

    if let Some(offer) = initial_publisher_offer {
        let initial_offer_request = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Offer(offer)),
        };
        match handle_non_local_relay_socket_message(
            WsMessage::Binary(initial_offer_request.encode_to_vec().into()),
            &state,
            &room_name,
            &identity,
            &selected_room_node_id,
        )
        .await
        {
            Ok(responses) => {
                for response in responses {
                    if sender.send(response).await.is_err() {
                        state.signal_connections.remove_if_same(
                            &room_name,
                            &identity,
                            &outbound_tx,
                        );
                        return;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    room = %room_name,
                    identity = %identity,
                    error = %error,
                    "non_local_relay_initial_publisher_offer_failed"
                );
                state
                    .signal_connections
                    .remove_if_same(&room_name, &identity, &outbound_tx);
                let _ = sender.close().await;
                return;
            }
        }
    }

    let mut outbound_relay_poll = tokio::time::interval(Duration::from_millis(25));
    outbound_relay_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut termination_requested_by_leave = false;

    'socket_loop: loop {
        tokio::select! {
            Some(response) = outbound_rx.recv() => {
                if sender
                    .send(WsMessage::Binary(response.encode_to_vec().into()))
                    .await
                    .is_err()
                {
                    tracing::debug!(
                        room = %room_name,
                        identity = %identity,
                        "socket_send_failed_closing_non_local_relay_loop"
                    );
                    break;
                }
            }
            _ = outbound_relay_poll.tick() => {
                let dispatcher = state.non_local_relay_dispatcher.clone();
                let query = outbound_relay_query(&room_name, &identity, &selected_room_node_id);
                match tokio::task::spawn_blocking(move || {
                    dispatcher.drain_non_local_outbound_signal_responses(query)
                }).await {
                    Ok(Ok(events)) => {
                        if !events.is_empty() {
                            tracing::debug!(
                                room = %room_name,
                                identity = %identity,
                                event_count = events.len(),
                                "non_local_relay_outbound_signal_responses_drained"
                            );
                        }
                        for event in events {
                            if sender.send(WsMessage::Binary(event.into())).await.is_err() {
                                break 'socket_loop;
                            }
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(room = %room_name, identity = %identity, error, "non_local_relay_outbound_drain_failed");
                    }
                    Err(error) => {
                        tracing::warn!(room = %room_name, identity = %identity, error = %error, "non_local_relay_outbound_drain_join_failed");
                    }
                }
            }
            next = receiver.next() => {
                let Some(next) = next else {
                    tracing::debug!(
                        room = %room_name,
                        identity = %identity,
                        "socket_receiver_stream_ended_non_local_relay_loop"
                    );
                    break;
                };
                let message = match next {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::warn!(
                            room = %room_name,
                            identity = %identity,
                            error = %error,
                            "socket_receiver_error_closing_non_local_relay_loop"
                        );
                        break;
                    }
                };
                if is_leave_signal_request_message(&message) {
                    termination_requested_by_leave = true;
                }
                match handle_non_local_relay_socket_message(message, &state, &room_name, &identity, &selected_room_node_id).await {
                    Ok(responses) => {
                        for response in responses {
                            if sender.send(response).await.is_err() {
                                break 'socket_loop;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    let is_active_session =
        state
            .signal_connections
            .remove_if_same(&room_name, &identity, &outbound_tx);
    if !is_active_session {
        return;
    }

    if !termination_requested_by_leave {
        tracing::debug!(
            room = %room_name,
            identity = %identity,
            "non_local_relay_session_closed_without_leave_skipping_remote_termination"
        );
        return;
    }

    tokio::time::sleep(NON_LOCAL_TERMINATION_GRACE).await;
    if state
        .signal_connections
        .get(&room_name, &identity)
        .is_some()
    {
        tracing::debug!(
            room = %room_name,
            identity = %identity,
            "non_local_relay_termination_skipped_due_to_reconnected_active_session"
        );
        return;
    }

    let dispatcher = state.non_local_relay_dispatcher.clone();
    let termination_intent = NonLocalRelaySessionTerminationIntent {
        room: room_name.clone(),
        identity: identity.clone(),
        participant_sid,
        selected_room_node_id,
    };
    match tokio::task::spawn_blocking(move || {
        dispatcher.dispatch_non_local_termination(termination_intent)
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(room = %room_name, identity = %identity, error, "non_local_relay_termination_dispatch_failed");
        }
        Err(error) => {
            tracing::warn!(room = %room_name, identity = %identity, error = %error, "non_local_relay_termination_dispatch_join_failed");
        }
    }
    state.data_channels.remove(&room_name, &identity);
    state
        .data_track_subscriptions
        .remove_participant(&room_name, &identity);
    state
        .publish_permissions
        .remove_participant(&room_name, &identity);
    state
        .subscribe_permissions
        .remove_participant(&room_name, &identity);
    state
        .media_forwarding
        .remove_participant(&room_name, &identity);
    state
        .pending_media_section_requests
        .remove_participant(&room_name, &identity);
    state
        .media_subscriptions
        .remove_participant(&room_name, &identity);
    state
        .media_track_cids
        .remove_participant(&room_name, &identity);
    state
        .pending_remote_tracks
        .remove_participant(&room_name, &identity);
    state
        .forward_tracks
        .remove_participant(&room_name, &identity);
    state
        .rtp_forwarding
        .remove_participant(&room_name, &identity);
    for peer_connection in state.peer_connections.remove_all(&room_name, &identity) {
        let _ = peer_connection.close().await;
    }
}

pub(crate) mod session;

use session::create_subscriber_offer;
#[cfg(test)]
use session::reconcile_publisher_media_tracks_after_answer;
pub(crate) use session::{
    add_track_response, answer_publisher_offer, ensure_existing_media_forwarding_for_subscriber,
    handle_media_subscription_request, publish_data_track_response,
    reject_unaccepted_video_tracks_from_subscriber_answer, unpublish_data_track_response,
    update_data_subscription_response,
};

pub(crate) async fn ensure_subscriber_transport_after_permission_grant(
    state: &SignalState,
    room_name: &str,
    identity: &str,
) {
    if !state.participant_uses_subscriber_primary(room_name, identity) {
        return;
    }

    if state
        .peer_connections
        .get(room_name, identity, SignalConnectionTarget::Subscriber)
        .is_some()
    {
        return;
    }

    let Some(outbound_tx) = state.signal_connections.get(room_name, identity) else {
        return;
    };

    match create_subscriber_offer(
        state,
        room_name,
        identity,
        &outbound_tx,
        &state.rtc_transport_config(),
    )
    .await
    {
        Ok(response) => {
            let _ = outbound_tx.send(response);
        }
        Err(error) => {
            tracing::warn!(
                room = %room_name,
                identity = %identity,
                error = %error,
                "failed_to_create_subscriber_offer_after_permission_grant"
            );
        }
    }
}

#[cfg(test)]
pub(crate) use session::{
    classify_single_pc_offer_sections, should_force_recvonly_for_single_pc_receive_sections,
    should_forward_media_for_subscriber,
};

#[cfg(test)]
mod tests;

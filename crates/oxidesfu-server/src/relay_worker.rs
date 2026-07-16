use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use oxidesfu_auth::{AuthContext, Claims, VideoGrants};
use oxidesfu_room::{RedisHashStore, RoomStore};
use prost::Message;

use crate::readiness::RelayBackendReadiness;

/// Default relay worker tick interval.
pub const DEFAULT_RELAY_WORKER_INTERVAL: Duration = Duration::from_millis(25);
/// Default time between relay mailbox response polls.
pub const DEFAULT_RELAY_RESPONSE_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Default max wait for relay mailbox response before local fallback.
///
/// In multi-process deployments the relay worker may need multiple polling cycles
/// before claiming an intent and writing a response, so sub-second values can
/// cause false local fallbacks and split room ownership.
pub const DEFAULT_RELAY_RESPONSE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared readiness state updated by relay worker runtime.
#[derive(Debug, Default)]
pub struct RelayWorkerReadiness {
    ready: AtomicBool,
}

impl RelayWorkerReadiness {
    /// Creates readiness initialized to `ready`.
    pub fn new(ready: bool) -> Self {
        Self {
            ready: AtomicBool::new(ready),
        }
    }

    /// Marks relay worker/backend as ready.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
    }

    /// Marks relay worker/backend as not ready.
    pub fn mark_not_ready(&self) {
        self.ready.store(false, Ordering::Relaxed);
    }
}

impl RelayBackendReadiness for RelayWorkerReadiness {
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
}

/// Executes relay intents claimed by a local node worker.
pub trait RelayJoinIntentExecutor: Send + Sync {
    /// Returns an accepted/rejected relay response for the claimed join intent.
    fn execute_join(
        &self,
        intent: &oxidesfu_signaling::NonLocalRelayJoinIntent,
    ) -> oxidesfu_signaling::NonLocalRelayJoinResponse;

    /// Executes a relayed join with a persistent outbound response channel.
    fn execute_join_with_outbound<'a>(
        &'a self,
        intent: &'a oxidesfu_signaling::NonLocalRelayJoinIntent,
        _outbound_tx: oxidesfu_signaling::RelayOutboundSignalSender,
    ) -> Pin<Box<dyn Future<Output = oxidesfu_signaling::NonLocalRelayJoinResponse> + Send + 'a>>
    {
        Box::pin(async move { self.execute_join(intent) })
    }

    /// Executes a relayed session termination intent on the remote owner.
    fn execute_termination(
        &self,
        intent: &oxidesfu_signaling::NonLocalRelaySessionTerminationIntent,
    );

    /// Executes a relayed long-lived signal request on the remote owner.
    fn execute_signal_request<'a>(
        &'a self,
        _intent: &'a oxidesfu_signaling::NonLocalRelaySignalRequestIntent,
    ) -> Pin<
        Box<
            dyn Future<Output = oxidesfu_signaling::NonLocalRelaySignalRequestResponse> + Send + 'a,
        >,
    > {
        Box::pin(async {
            oxidesfu_signaling::NonLocalRelaySignalRequestResponse::Error {
                message: "relay signal request execution is not configured".to_string(),
            }
        })
    }

    /// Executes a relayed signal request with a persistent outbound response channel.
    fn execute_signal_request_with_outbound<'a>(
        &'a self,
        intent: &'a oxidesfu_signaling::NonLocalRelaySignalRequestIntent,
        _outbound_tx: oxidesfu_signaling::RelayOutboundSignalSender,
    ) -> Pin<
        Box<
            dyn Future<Output = oxidesfu_signaling::NonLocalRelaySignalRequestResponse> + Send + 'a,
        >,
    > {
        self.execute_signal_request(intent)
    }

    /// Executes a relayed RoomService request on the remote owner.
    fn execute_room_service<'a>(
        &'a self,
        _intent: &'a oxidesfu_signaling::NonLocalRelayRoomServiceIntent,
    ) -> Pin<
        Box<dyn Future<Output = oxidesfu_signaling::NonLocalRelayRoomServiceResponse> + Send + 'a>,
    > {
        Box::pin(async {
            oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                status: 501,
                code: "unimplemented".to_string(),
                msg: "relay room service execution is not configured".to_string(),
            }
        })
    }
}

/// Default relay join executor backed by OxideSFU room state.
#[derive(Clone)]
pub struct RoomStoreRelayJoinIntentExecutor {
    rooms: RoomStore,
    signal_state: Option<oxidesfu_signaling::SignalState>,
}

impl RoomStoreRelayJoinIntentExecutor {
    /// Creates a room-store-backed relay join executor.
    pub fn new(rooms: RoomStore) -> Self {
        Self {
            rooms,
            signal_state: None,
        }
    }

    /// Creates a relay executor backed by both room state and full signalling state.
    pub fn with_signal_state(signal_state: oxidesfu_signaling::SignalState) -> Self {
        Self {
            rooms: signal_state.rooms.clone(),
            signal_state: Some(signal_state),
        }
    }
}

impl RoomStoreRelayJoinIntentExecutor {
    fn effective_subscriber_primary(intent: &oxidesfu_signaling::NonLocalRelayJoinIntent) -> bool {
        intent.subscriber_primary && intent.can_subscribe
    }

    fn default_ice_servers() -> Vec<livekit_protocol::IceServer> {
        vec![livekit_protocol::IceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        }]
    }
}

impl RelayJoinIntentExecutor for RoomStoreRelayJoinIntentExecutor {
    fn execute_join(
        &self,
        intent: &oxidesfu_signaling::NonLocalRelayJoinIntent,
    ) -> oxidesfu_signaling::NonLocalRelayJoinResponse {
        let requested_permission = livekit_protocol::ParticipantPermission {
            can_subscribe: intent.can_subscribe,
            can_publish: intent.can_publish,
            can_publish_data: intent.can_publish_data,
            can_update_metadata: intent.can_update_metadata,
            hidden: intent.hidden,
            can_subscribe_metrics: false,
            can_manage_agent_session: false,
            ..Default::default()
        };

        let apply_permission = |participant: livekit_protocol::ParticipantInfo|
         -> Result<livekit_protocol::ParticipantInfo, String> {
            let updated = self
                .rooms
                .update_participant(
                    &intent.room,
                    &intent.identity,
                    "",
                    "",
                    Some(requested_permission.clone()),
                    std::collections::HashMap::new(),
                )
                .map_err(|err| format!("relay join update_participant failed: {err}"))?;

            if let Some(signal_state) = self.signal_state.as_ref() {
                signal_state.apply_service_participant_update(
                    &intent.room,
                    Some(&participant),
                    updated.clone(),
                );
            }

            Ok(updated)
        };

        let apply_kind = |participant: livekit_protocol::ParticipantInfo|
         -> Result<livekit_protocol::ParticipantInfo, String> {
            let participant_kind = match intent.kind.to_ascii_uppercase().as_str() {
                "INGRESS" => livekit_protocol::participant_info::Kind::Ingress as i32,
                "EGRESS" => livekit_protocol::participant_info::Kind::Egress as i32,
                "SIP" => livekit_protocol::participant_info::Kind::Sip as i32,
                "AGENT" => livekit_protocol::participant_info::Kind::Agent as i32,
                "CONNECTOR" => livekit_protocol::participant_info::Kind::Connector as i32,
                "BRIDGE" => livekit_protocol::participant_info::Kind::Bridge as i32,
                _ => livekit_protocol::participant_info::Kind::Standard as i32,
            };

            let participant_kind_details = intent
                .kind_details
                .iter()
                .filter_map(|detail| match detail.to_ascii_uppercase().as_str() {
                    "CLOUD_AGENT" => {
                        Some(livekit_protocol::participant_info::KindDetail::CloudAgent as i32)
                    }
                    "FORWARDED" => {
                        Some(livekit_protocol::participant_info::KindDetail::Forwarded as i32)
                    }
                    "CONNECTOR_WHATSAPP" => Some(
                        livekit_protocol::participant_info::KindDetail::ConnectorWhatsapp as i32,
                    ),
                    "CONNECTOR_TWILIO" => {
                        Some(livekit_protocol::participant_info::KindDetail::ConnectorTwilio as i32)
                    }
                    "BRIDGE_RTSP" => {
                        Some(livekit_protocol::participant_info::KindDetail::BridgeRtsp as i32)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();

            self.rooms
                .set_participant_kind(
                    &intent.room,
                    &intent.identity,
                    participant_kind,
                    participant_kind_details,
                )
                .map_err(|err| format!("relay join set_participant_kind failed: {err}"))
                .or(Ok(participant))
        };

        let remember_auth_context = |participant: &livekit_protocol::ParticipantInfo| {
            if let Some(signal_state) = self.signal_state.as_ref() {
                signal_state.remember_participant_subscriber_primary(
                    &intent.room,
                    &intent.identity,
                    intent.subscriber_primary,
                );
                signal_state.remember_participant_auth_context(
                    &intent.room,
                    &intent.identity,
                    &AuthContext {
                        api_key: intent.api_key.clone(),
                        claims: Claims {
                            iss: intent.api_key.clone(),
                            sub: intent.identity.clone(),
                            identity: intent.identity.clone(),
                            name: participant.name.clone(),
                            kind: intent.kind.clone(),
                            kind_details: intent.kind_details.clone(),
                            video: VideoGrants {
                                room_join: true,
                                room: intent.room.clone(),
                                destination_room: intent.destination_room.clone(),
                                can_publish: intent.can_publish,
                                can_subscribe: intent.can_subscribe,
                                can_publish_data: intent.can_publish_data,
                                can_update_own_metadata: intent.can_update_metadata,
                                hidden: intent.hidden,
                                ..Default::default()
                            },
                            metadata: participant.metadata.clone(),
                            attributes: participant.attributes.clone(),
                            room_config: intent.room_config.clone(),
                            ..Default::default()
                        },
                    },
                );
            }
        };

        let build_accepted_with_join = |room: livekit_protocol::Room,
                                        participant: livekit_protocol::ParticipantInfo,
                                        other_participants: Vec<
            livekit_protocol::ParticipantInfo,
        >| {
            let join = livekit_protocol::JoinResponse {
                room: Some(room),
                participant: Some(participant),
                other_participants,
                server_version: env!("CARGO_PKG_VERSION").to_string(),
                ping_interval: 5,
                ping_timeout: 15,
                ice_servers: Self::default_ice_servers(),
                subscriber_primary: Self::effective_subscriber_primary(intent),
                fast_publish: intent.can_publish,
                server_info: Some(livekit_protocol::ServerInfo {
                    edition: livekit_protocol::server_info::Edition::Standard as i32,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    protocol: 17,
                    ..Default::default()
                }),
                ..Default::default()
            };
            let response = oxidesfu_signaling::NonLocalRelayJoinResponse::AcceptedWithJoin {
                join_response: join.encode_to_vec(),
            };
            tracing::debug!(room = %intent.room, identity = %intent.identity, "relay_join_executor_returning_accepted_with_join");
            response
        };

        if let Some(requested_sid) = intent.requested_participant_sid.as_deref() {
            return match self.rooms.get_participant(&intent.room, &intent.identity) {
                Ok(participant) if participant.sid == requested_sid => {
                    let room = match self.rooms.list_rooms(std::slice::from_ref(&intent.room)) {
                        Ok(mut rooms) => rooms.pop(),
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                                code: "internal".to_string(),
                                msg: format!("relay reconnect room lookup failed: {err}"),
                            };
                        }
                    };
                    let Some(room) = room else {
                        return oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                            code: "not_found".to_string(),
                            msg: "relay reconnect room not found".to_string(),
                        };
                    };
                    let other_participants = match self.rooms.list_participants(&intent.room) {
                        Ok(participants) => participants
                            .into_iter()
                            .filter(|candidate| candidate.identity != intent.identity)
                            .collect(),
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                                code: "internal".to_string(),
                                msg: format!(
                                    "relay reconnect participant list lookup failed: {err}"
                                ),
                            };
                        }
                    };
                    let participant = match apply_permission(participant).and_then(apply_kind) {
                        Ok(participant) => participant,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                                code: "internal".to_string(),
                                msg: err,
                            };
                        }
                    };
                    remember_auth_context(&participant);
                    if let Some(signal_state) = self.signal_state.as_ref() {
                        signal_state.apply_service_participant_update(
                            &intent.room,
                            None,
                            participant.clone(),
                        );
                    }
                    build_accepted_with_join(room, participant, other_participants)
                }
                Ok(participant) => oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                    code: "failed_precondition".to_string(),
                    msg: format!(
                        "relay reconnect SID mismatch: requested {requested_sid}, found {}",
                        participant.sid
                    ),
                },
                Err(err) => oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                    code: "not_found".to_string(),
                    msg: format!("relay reconnect participant not found: {err}"),
                },
            };
        }

        match self.rooms.join_participant(
            &intent.room,
            &intent.identity,
            &intent.name,
            intent.metadata.clone(),
            intent.attributes.clone(),
        ) {
            Ok((room, participant, other_participants)) => {
                let participant = match apply_permission(participant).and_then(apply_kind) {
                    Ok(participant) => participant,
                    Err(err) => {
                        return oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                            code: "internal".to_string(),
                            msg: err,
                        };
                    }
                };
                remember_auth_context(&participant);
                if let Some(signal_state) = self.signal_state.as_ref() {
                    signal_state.apply_service_participant_update(
                        &intent.room,
                        None,
                        participant.clone(),
                    );
                }
                build_accepted_with_join(room, participant, other_participants)
            }
            Err(err) => oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                code: "internal".to_string(),
                msg: format!("relay join execution failed: {err}"),
            },
        }
    }

    fn execute_join_with_outbound<'a>(
        &'a self,
        intent: &'a oxidesfu_signaling::NonLocalRelayJoinIntent,
        outbound_tx: oxidesfu_signaling::RelayOutboundSignalSender,
    ) -> Pin<Box<dyn Future<Output = oxidesfu_signaling::NonLocalRelayJoinResponse> + Send + 'a>>
    {
        Box::pin(async move {
            let response = self.execute_join(intent);
            let oxidesfu_signaling::NonLocalRelayJoinResponse::AcceptedWithJoin { join_response } =
                response
            else {
                return response;
            };
            if !Self::effective_subscriber_primary(intent) {
                return oxidesfu_signaling::NonLocalRelayJoinResponse::AcceptedWithJoin {
                    join_response,
                };
            }
            let Some(signal_state) = self.signal_state.as_ref() else {
                return oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                    code: "internal".to_string(),
                    msg: "relay owner missing signaling state for v0 subscriber offer".to_string(),
                };
            };
            match signal_state
                .create_relay_subscriber_offer(
                    &intent.room,
                    &intent.identity,
                    intent.can_subscribe,
                    &outbound_tx,
                )
                .await
            {
                Ok(offer) => {
                    oxidesfu_signaling::NonLocalRelayJoinResponse::AcceptedWithJoinAndSignals {
                        join_response,
                        initial_signal_responses: vec![offer.encode_to_vec()],
                    }
                }
                Err(err) => oxidesfu_signaling::NonLocalRelayJoinResponse::Rejected {
                    code: "internal".to_string(),
                    msg: format!("relay owner failed to create v0 subscriber offer: {err}"),
                },
            }
        })
    }

    fn execute_termination(
        &self,
        intent: &oxidesfu_signaling::NonLocalRelaySessionTerminationIntent,
    ) {
        let Ok(current) = self.rooms.get_participant(&intent.room, &intent.identity) else {
            return;
        };
        if current.sid != intent.participant_sid {
            tracing::debug!(
                room = %intent.room,
                identity = %intent.identity,
                terminating_sid = %intent.participant_sid,
                current_sid = %current.sid,
                "relay_termination_ignored_for_rejoined_participant"
            );
            return;
        }

        if let Ok(participant) = self.rooms.remove_participant_with_reason(
            &intent.room,
            &intent.identity,
            livekit_protocol::DisconnectReason::ClientInitiated,
        ) && let Some(signal_state) = self.signal_state.as_ref()
        {
            signal_state.broadcast_participant_update_from_service(&intent.room, participant);
        }
    }

    fn execute_signal_request<'a>(
        &'a self,
        intent: &'a oxidesfu_signaling::NonLocalRelaySignalRequestIntent,
    ) -> Pin<
        Box<
            dyn Future<Output = oxidesfu_signaling::NonLocalRelaySignalRequestResponse> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(signal_state) = self.signal_state.as_ref() else {
                return oxidesfu_signaling::NonLocalRelaySignalRequestResponse::Error {
                    message: "relay signal executor missing signal state".to_string(),
                };
            };

            signal_state
                .handle_relayed_signal_request_bytes(
                    &intent.room,
                    &intent.identity,
                    &intent.signal_request,
                )
                .await
        })
    }

    fn execute_signal_request_with_outbound<'a>(
        &'a self,
        intent: &'a oxidesfu_signaling::NonLocalRelaySignalRequestIntent,
        outbound_tx: oxidesfu_signaling::RelayOutboundSignalSender,
    ) -> Pin<
        Box<
            dyn Future<Output = oxidesfu_signaling::NonLocalRelaySignalRequestResponse> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(signal_state) = self.signal_state.as_ref() else {
                return oxidesfu_signaling::NonLocalRelaySignalRequestResponse::Error {
                    message: "relay signal executor missing signal state".to_string(),
                };
            };

            signal_state
                .handle_relayed_signal_request_bytes_with_outbound_sender(
                    &intent.room,
                    &intent.identity,
                    &intent.signal_request,
                    outbound_tx,
                )
                .await
        })
    }

    fn execute_room_service<'a>(
        &'a self,
        intent: &'a oxidesfu_signaling::NonLocalRelayRoomServiceIntent,
    ) -> Pin<
        Box<dyn Future<Output = oxidesfu_signaling::NonLocalRelayRoomServiceResponse> + Send + 'a>,
    > {
        Box::pin(async move {
            use livekit_protocol as proto;

            let success = |response: Vec<u8>| {
                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success { response }
            };
            let room_error = |err: oxidesfu_room::RoomStoreError| {
                let (status, code, msg) = match err {
                    oxidesfu_room::RoomStoreError::RoomNotFound => {
                        (404, "not_found", "room not found".to_string())
                    }
                    oxidesfu_room::RoomStoreError::ParticipantNotFound => {
                        (404, "not_found", "participant not found".to_string())
                    }
                    oxidesfu_room::RoomStoreError::AgentDispatchNotFound => {
                        (404, "not_found", "agent dispatch not found".to_string())
                    }
                    oxidesfu_room::RoomStoreError::SipTrunkNotFound => {
                        (404, "not_found", "sip trunk not found".to_string())
                    }
                    oxidesfu_room::RoomStoreError::SipDispatchRuleNotFound => {
                        (404, "not_found", "sip dispatch rule not found".to_string())
                    }
                    oxidesfu_room::RoomStoreError::IngressNotFound => {
                        (404, "not_found", "ingress not found".to_string())
                    }
                    oxidesfu_room::RoomStoreError::EgressNotFound => {
                        (404, "not_found", "egress not found".to_string())
                    }
                    oxidesfu_room::RoomStoreError::MaxParticipantsExceeded => (
                        429,
                        "resource_exhausted",
                        "room has exceeded its max participants".to_string(),
                    ),
                    oxidesfu_room::RoomStoreError::InvalidArgument(message) => {
                        (400, "invalid_argument", message)
                    }
                    oxidesfu_room::RoomStoreError::LockPoisoned => {
                        (500, "internal", "internal room store error".to_string())
                    }
                };
                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                    status,
                    code: code.to_string(),
                    msg,
                }
            };

            match intent.method.as_str() {
                "DeleteRoom" => {
                    let request = match proto::DeleteRoomRequest::decode(intent.request.as_slice())
                    {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };

                    if let Some(signal_state) = self.signal_state.as_ref()
                        && let Ok(participants) = self.rooms.list_participants(&request.room)
                    {
                        for participant in participants {
                            let _ = signal_state
                                .disconnect_participant_from_service(
                                    &request.room,
                                    &participant.identity,
                                    proto::DisconnectReason::RoomDeleted,
                                )
                                .await;
                        }
                    }

                    match self.rooms.delete_room_with_snapshot(&request.room) {
                        Ok(_) => success(proto::DeleteRoomResponse {}.encode_to_vec()),
                        Err(err) => room_error(err),
                    }
                }
                "UpdateRoomMetadata" => {
                    let request = match proto::UpdateRoomMetadataRequest::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 400,
                                    code: "malformed".to_string(),
                                    msg: format!("failed to decode request: {err}"),
                                };
                        }
                    };

                    match self
                        .rooms
                        .update_room_metadata(&request.room, request.metadata)
                    {
                        Ok(room) => success(room.encode_to_vec()),
                        Err(err) => room_error(err),
                    }
                }
                "ListRooms" => {
                    let request = match proto::ListRoomsRequest::decode(intent.request.as_slice()) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };

                    match self.rooms.list_rooms(&request.names) {
                        Ok(rooms) => success(proto::ListRoomsResponse { rooms }.encode_to_vec()),
                        Err(err) => room_error(err),
                    }
                }
                "GetParticipant" => {
                    let request = match proto::RoomParticipantIdentity::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };
                    match self.rooms.get_participant(&request.room, &request.identity) {
                        Ok(participant) => success(participant.encode_to_vec()),
                        Err(err) => room_error(err),
                    }
                }
                "ListParticipants" => {
                    let request = match proto::ListParticipantsRequest::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };
                    match self.rooms.list_participants(&request.room) {
                        Ok(participants) => success(
                            proto::ListParticipantsResponse { participants }.encode_to_vec(),
                        ),
                        Err(err) => room_error(err),
                    }
                }
                "UpdateParticipant" => {
                    let request = match proto::UpdateParticipantRequest::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 400,
                                    code: "malformed".to_string(),
                                    msg: format!("failed to decode request: {err}"),
                                };
                        }
                    };

                    let previous = self
                        .rooms
                        .get_participant(&request.room, &request.identity)
                        .ok();

                    match self.rooms.update_participant(
                        &request.room,
                        &request.identity,
                        &request.metadata,
                        &request.name,
                        request.permission,
                        request.attributes,
                    ) {
                        Ok(participant) => {
                            if let Some(signal_state) = self.signal_state.as_ref() {
                                signal_state.apply_service_participant_update(
                                    &request.room,
                                    previous.as_ref(),
                                    participant.clone(),
                                );
                            }
                            success(participant.encode_to_vec())
                        }
                        Err(err) => room_error(err),
                    }
                }
                "MutePublishedTrack" => {
                    let request = match proto::MuteRoomTrackRequest::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };

                    match self.rooms.set_participant_track_muted(
                        &request.room,
                        &request.identity,
                        &request.track_sid,
                        request.muted,
                    ) {
                        Ok(track) => {
                            if let Some(signal_state) = self.signal_state.as_ref()
                                && let Ok(participant) =
                                    self.rooms.get_participant(&request.room, &request.identity)
                            {
                                signal_state.broadcast_participant_update_from_service(
                                    &request.room,
                                    participant,
                                );
                            }
                            success(
                                proto::MuteRoomTrackResponse { track: Some(track) }.encode_to_vec(),
                            )
                        }
                        Err(err) => room_error(err),
                    }
                }
                "UpdateSubscriptions" => {
                    let request = match proto::UpdateSubscriptionsRequest::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };

                    if let Err(err) = self.rooms.get_participant(&request.room, &request.identity) {
                        return room_error(err);
                    }

                    let mut applied_any = false;
                    for track_sid in &request.track_sids {
                        match self.rooms.set_media_track_subscribed_by_track_sid(
                            &request.room,
                            &request.identity,
                            track_sid,
                            request.subscribe,
                        ) {
                            Ok(applied) => {
                                applied_any |= applied;
                            }
                            Err(err) => return room_error(err),
                        }
                    }

                    for participant_tracks in &request.participant_tracks {
                        for track_sid in &participant_tracks.track_sids {
                            match self.rooms.set_media_track_subscribed_by_publisher_sid(
                                &request.room,
                                &participant_tracks.participant_sid,
                                track_sid,
                                &request.identity,
                                request.subscribe,
                            ) {
                                Ok(applied) => {
                                    applied_any |= applied;
                                }
                                Err(err) => return room_error(err),
                            }
                        }
                    }

                    if applied_any && let Some(signal_state) = self.signal_state.as_ref() {
                        signal_state
                            .apply_twirp_update_subscriptions(
                                &request.room,
                                &request.identity,
                                &request.track_sids,
                                &request.participant_tracks,
                                request.subscribe,
                            )
                            .await;
                    }

                    success(proto::UpdateSubscriptionsResponse {}.encode_to_vec())
                }
                "RemoveParticipant" => {
                    let request = match proto::RoomParticipantIdentity::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };

                    if let Some(signal_state) = self.signal_state.as_ref() {
                        if let Err(err) = signal_state
                            .disconnect_participant_from_service(
                                &request.room,
                                &request.identity,
                                proto::DisconnectReason::ParticipantRemoved,
                            )
                            .await
                        {
                            return room_error(err);
                        }
                        return success(proto::RemoveParticipantResponse {}.encode_to_vec());
                    }

                    match self.rooms.remove_participant_with_reason(
                        &request.room,
                        &request.identity,
                        proto::DisconnectReason::ParticipantRemoved,
                    ) {
                        Ok(_) => success(proto::RemoveParticipantResponse {}.encode_to_vec()),
                        Err(err) => room_error(err),
                    }
                }
                "PerformRpc" => {
                    let request = match proto::PerformRpcRequest::decode(intent.request.as_slice())
                    {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };

                    let Some(signal_state) = self.signal_state.as_ref() else {
                        return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                            status: 501,
                            code: "unimplemented".to_string(),
                            msg: "perform rpc relay execution requires signal state".to_string(),
                        };
                    };

                    if let Err(err) = self.rooms.ensure_room_exists(&request.room) {
                        return room_error(err);
                    }

                    match signal_state
                        .perform_rpc_from_service(
                            &request.room,
                            &request.destination_identity,
                            &request.method,
                            &request.payload,
                            request.response_timeout_ms,
                        )
                        .await
                    {
                        Ok(response) => success(response.encode_to_vec()),
                        Err(err) => room_error(err),
                    }
                }
                "SendData" => {
                    let request = match proto::SendDataRequest::decode(intent.request.as_slice()) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 400,
                                code: "malformed".to_string(),
                                msg: format!("failed to decode request: {err}"),
                            };
                        }
                    };

                    let Some(signal_state) = self.signal_state.as_ref() else {
                        return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                            status: 501,
                            code: "unimplemented".to_string(),
                            msg: "send data relay execution requires signal state".to_string(),
                        };
                    };

                    match signal_state.send_data_from_service(&request).await {
                        Ok(()) => success(proto::SendDataResponse {}.encode_to_vec()),
                        Err(message) => {
                            oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                status: 500,
                                code: "internal".to_string(),
                                msg: format!("data send failed: {message}"),
                            }
                        }
                    }
                }
                "CreateDispatch" => {
                    let request = match proto::CreateAgentDispatchRequest::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 400,
                                    code: "malformed".to_string(),
                                    msg: format!("failed to decode request: {err}"),
                                };
                        }
                    };

                    match self.rooms.create_agent_dispatch(request) {
                        Ok(dispatch) => success(dispatch.encode_to_vec()),
                        Err(err) => room_error(err),
                    }
                }
                "DeleteDispatch" => {
                    let request = match proto::DeleteAgentDispatchRequest::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 400,
                                    code: "malformed".to_string(),
                                    msg: format!("failed to decode request: {err}"),
                                };
                        }
                    };

                    match self
                        .rooms
                        .delete_agent_dispatch(&request.room, &request.dispatch_id)
                    {
                        Ok(dispatch) => success(dispatch.encode_to_vec()),
                        Err(err) => room_error(err),
                    }
                }
                "ListDispatch" => {
                    let request = match proto::ListAgentDispatchRequest::decode(
                        intent.request.as_slice(),
                    ) {
                        Ok(request) => request,
                        Err(err) => {
                            return oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 400,
                                    code: "malformed".to_string(),
                                    msg: format!("failed to decode request: {err}"),
                                };
                        }
                    };

                    match self
                        .rooms
                        .list_agent_dispatches(&request.room, &request.dispatch_id)
                    {
                        Ok(agent_dispatches) => success(
                            proto::ListAgentDispatchResponse { agent_dispatches }.encode_to_vec(),
                        ),
                        Err(err) => room_error(err),
                    }
                }
                other => oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                    status: 501,
                    code: "unimplemented".to_string(),
                    msg: format!("relay room service method not implemented: {other}"),
                },
            }
        })
    }
}

/// Spawns a relay worker that claims intents for `local_room_node_id` and stores responses.
pub fn spawn_relay_intent_worker<S>(
    mailbox: oxidesfu_signaling::RedisRelayMailbox<S>,
    local_room_node_id: String,
    executor: Arc<dyn RelayJoinIntentExecutor>,
    interval: Duration,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    readiness: Arc<RelayWorkerReadiness>,
) -> tokio::task::JoinHandle<()>
where
    S: RedisHashStore + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                }
                _ = ticker.tick() => {
                    let mut had_error = false;
                    loop {
                        let claimed = mailbox.claim_next_intent_for_node(&local_room_node_id);
                        let Some((receipt, intent)) = (match claimed {
                            Ok(claimed) => claimed,
                            Err(err) => {
                                had_error = true;
                                tracing::warn!(error = %err, local_room_node_id, "relay_worker_claim_failed");
                                break;
                            }
                        }) else {
                            break;
                        };

                        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
                        let outbound_mailbox = mailbox.clone();
                        let outbound_room = intent.room.clone();
                        let outbound_identity = intent.identity.clone();
                        let outbound_node = intent.selected_room_node_id.clone();
                        tokio::spawn(async move {
                            while let Some(response) = outbound_rx.recv().await {
                                if let Err(err) = outbound_mailbox.store_outbound_signal_response(
                                    &outbound_room,
                                    &outbound_identity,
                                    &outbound_node,
                                    oxidesfu_signaling::encode_relay_signal_response(&response),
                                ) {
                                    tracing::warn!(error = %err, room = %outbound_room, identity = %outbound_identity, "relay_worker_store_join_outbound_signal_response_failed");
                                    break;
                                }
                            }
                        });
                        let response = executor.execute_join_with_outbound(&intent, outbound_tx).await;
                        if let Err(err) = mailbox.store_response(&receipt, &response) {
                            had_error = true;
                            tracing::warn!(error = %err, local_room_node_id, "relay_worker_store_response_failed");
                            break;
                        }
                    }

                    loop {
                        let claimed = mailbox
                            .claim_next_termination_intent_for_node(&local_room_node_id);
                        let Some(intent) = (match claimed {
                            Ok(claimed) => claimed,
                            Err(err) => {
                                had_error = true;
                                tracing::warn!(error = %err, local_room_node_id, "relay_worker_claim_termination_failed");
                                break;
                            }
                        }) else {
                            break;
                        };

                        executor.execute_termination(&intent);
                    }

                    loop {
                        let claimed = mailbox
                            .claim_next_signal_request_intent_for_node(&local_room_node_id);
                        let Some((receipt, intent)) = (match claimed {
                            Ok(claimed) => claimed,
                            Err(err) => {
                                had_error = true;
                                tracing::warn!(error = %err, local_room_node_id, "relay_worker_claim_signal_request_failed");
                                break;
                            }
                        }) else {
                            break;
                        };

                        tracing::debug!(
                            local_room_node_id,
                            room = %intent.room,
                            identity = %intent.identity,
                            selected_room_node_id = %intent.selected_room_node_id,
                            "relay_worker_claimed_signal_request_intent"
                        );
                        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
                        let outbound_mailbox = mailbox.clone();
                        let outbound_room = intent.room.clone();
                        let outbound_identity = intent.identity.clone();
                        let outbound_node = intent.selected_room_node_id.clone();
                        tokio::spawn(async move {
                            while let Some(response) = outbound_rx.recv().await {
                                if let Err(err) = outbound_mailbox.store_outbound_signal_response(
                                    &outbound_room,
                                    &outbound_identity,
                                    &outbound_node,
                                    oxidesfu_signaling::encode_relay_signal_response(&response),
                                ) {
                                    tracing::warn!(error = %err, room = %outbound_room, identity = %outbound_identity, "relay_worker_store_outbound_signal_response_failed");
                                    break;
                                }
                                tracing::debug!(
                                    room = %outbound_room,
                                    identity = %outbound_identity,
                                    selected_room_node_id = %outbound_node,
                                    "relay_worker_stored_outbound_signal_response"
                                );
                            }
                        });

                        let response = executor
                            .execute_signal_request_with_outbound(&intent, outbound_tx)
                            .await;
                        tracing::debug!(
                            local_room_node_id,
                            room = %intent.room,
                            identity = %intent.identity,
                            selected_room_node_id = %intent.selected_room_node_id,
                            response = ?response,
                            "relay_worker_executed_signal_request_intent"
                        );
                        if let Err(err) = mailbox.store_signal_response(&receipt, &response) {
                            had_error = true;
                            tracing::warn!(error = %err, local_room_node_id, "relay_worker_store_signal_response_failed");
                            break;
                        }
                    }

                    loop {
                        let claimed = mailbox
                            .claim_next_room_service_intent_for_node(&local_room_node_id);
                        let Some((receipt, intent)) = (match claimed {
                            Ok(claimed) => claimed,
                            Err(err) => {
                                had_error = true;
                                tracing::warn!(error = %err, local_room_node_id, "relay_worker_claim_room_service_request_failed");
                                break;
                            }
                        }) else {
                            break;
                        };

                        let response = executor.execute_room_service(&intent).await;
                        if let Err(err) = mailbox.store_room_service_response(&receipt, &response) {
                            had_error = true;
                            tracing::warn!(error = %err, local_room_node_id, "relay_worker_store_room_service_response_failed");
                            break;
                        }
                    }

                    if had_error {
                        readiness.mark_not_ready();
                    } else {
                        readiness.mark_ready();
                    }
                }
            }
        }
    })
}

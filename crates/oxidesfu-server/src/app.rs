// OxideSFU HTTP/WebSocket server composition.

use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use async_trait::async_trait;
use axum::{
    Router,
    extract::Request,
    http::{Uri, header, uri::PathAndQuery},
    middleware::{self, Next},
    response::Response,
    routing::get,
};
use oxidesfu_api::{
    ApiState, ForwardedRoomServiceResponse, MediaSubscriptionRuntime, RoomServiceForwarder,
    RoomServiceMethod,
};
use oxidesfu_core::ServerConfig;
use oxidesfu_room::{RoomNodeDirectory, RoomStoreError};

pub use crate::cleanup::{
    DEFAULT_EMPTY_ROOM_MAX_AGE, DEFAULT_ROOM_CLEANUP_INTERVAL, spawn_room_cleanup_task,
    spawn_room_cleanup_task_with_room_finished_handler,
};
pub use crate::config::{
    api_state_from_config, register_local_room_node, resolve_rtc_external_ip_from_config,
    room_node_directory_from_config, rtc_transport_config_from_server_config,
    rtc_transport_config_with_tcp_mux_from_server_config, set_local_room_node_draining,
    signal_ice_servers_for_participant, signal_ice_servers_from_config,
    spawn_room_node_registration_task, validate_turn_runtime_from_config,
};
pub use crate::health::HealthResponse;
pub use crate::logging::DEFAULT_TRACING_ENV_FILTER;
pub use crate::readiness::{
    AlwaysReadyRelayBackendReadiness, ReadinessResponse, RelayBackendReadiness,
};
pub use crate::relay_worker::{
    DEFAULT_RELAY_RESPONSE_POLL_INTERVAL, DEFAULT_RELAY_RESPONSE_WAIT_TIMEOUT,
    DEFAULT_RELAY_WORKER_INTERVAL, RelayJoinIntentExecutor, RelayWorkerReadiness,
    RoomStoreRelayJoinIntentExecutor, spawn_relay_intent_worker,
};
pub use crate::shutdown::begin_graceful_shutdown;
pub use crate::turn_runtime::{TurnRuntime, start_turn_runtime};

#[cfg(test)]
pub(crate) use crate::config::room_node_directory_from_config_with_factory;
#[cfg(test)]
pub(crate) use crate::logging::{X_REQUEST_ID, log_request_completion, request_id_from_headers};

#[derive(Clone)]
struct SignalStateMediaSubscriptionRuntime {
    signal_state: oxidesfu_signaling::SignalState,
    webhook_dispatcher: Option<Arc<crate::webhook::WebhookDispatcher>>,
}

#[derive(Clone)]
struct SignalStateRoomServiceForwarder {
    signal_state: oxidesfu_signaling::SignalState,
}

fn collapse_consecutive_slashes(path: &str) -> Option<String> {
    if !path.contains("//") {
        return None;
    }

    let mut normalized = String::with_capacity(path.len());
    let mut previous_was_slash = false;

    for ch in path.chars() {
        if ch == '/' {
            if previous_was_slash {
                continue;
            }
            previous_was_slash = true;
        } else {
            previous_was_slash = false;
        }
        normalized.push(ch);
    }

    if normalized.is_empty() {
        normalized.push('/');
    }

    Some(normalized)
}

fn normalized_path_and_query(uri: &Uri) -> Option<PathAndQuery> {
    let normalized_path = collapse_consecutive_slashes(uri.path())?;
    let normalized = if let Some(query) = uri.query() {
        format!("{normalized_path}?{query}")
    } else {
        normalized_path
    };
    PathAndQuery::from_str(&normalized).ok()
}

async fn normalize_path_and_reflect_origin(mut request: Request, next: Next) -> Response {
    let origin = request.headers().get(header::ORIGIN).cloned();

    if let Some(path_and_query) = normalized_path_and_query(request.uri()) {
        let mut parts = request.uri().clone().into_parts();
        parts.path_and_query = Some(path_and_query);

        if let Ok(uri) = Uri::from_parts(parts) {
            *request.uri_mut() = uri;
        }
    }

    let mut response = next.run(request).await;
    if let Some(origin) = origin {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }

    response
}

fn room_service_method_name(method: RoomServiceMethod) -> &'static str {
    match method {
        RoomServiceMethod::DeleteRoom => "DeleteRoom",
        RoomServiceMethod::UpdateRoomMetadata => "UpdateRoomMetadata",
        RoomServiceMethod::ListParticipants => "ListParticipants",
        RoomServiceMethod::GetParticipant => "GetParticipant",
        RoomServiceMethod::RemoveParticipant => "RemoveParticipant",
        RoomServiceMethod::UpdateParticipant => "UpdateParticipant",
        RoomServiceMethod::MutePublishedTrack => "MutePublishedTrack",
        RoomServiceMethod::UpdateSubscriptions => "UpdateSubscriptions",
        RoomServiceMethod::SendData => "SendData",
        RoomServiceMethod::PerformRpc => "PerformRpc",
        RoomServiceMethod::CreateDispatch => "CreateDispatch",
        RoomServiceMethod::DeleteDispatch => "DeleteDispatch",
        RoomServiceMethod::ListDispatch => "ListDispatch",
    }
}

#[async_trait]
impl RoomServiceForwarder for SignalStateRoomServiceForwarder {
    async fn forward_if_non_local(
        &self,
        room: &str,
        method: RoomServiceMethod,
        request: Vec<u8>,
    ) -> Option<ForwardedRoomServiceResponse> {
        let selected_room_node_id = match self
            .signal_state
            .non_local_room_service_target_for_room(room)
        {
            Ok(selected) => selected,
            Err(error) => {
                return Some(ForwardedRoomServiceResponse::TwirpError {
                    status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal".to_string(),
                    msg: format!("room node directory lookup failed: {error}"),
                });
            }
        }?;

        let intent = oxidesfu_signaling::NonLocalRelayRoomServiceIntent {
            room: room.to_string(),
            selected_room_node_id,
            method: room_service_method_name(method).to_string(),
            request,
        };

        let signal_state = self.signal_state.clone();
        let dispatched = tokio::task::spawn_blocking(move || {
            signal_state.dispatch_non_local_room_service(intent)
        })
        .await;

        let response = match dispatched {
            Ok(Ok(Some(response))) => response,
            Ok(Ok(None)) => {
                return Some(ForwardedRoomServiceResponse::TwirpError {
                    status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    code: "unavailable".to_string(),
                    msg: "remote room service relay response unavailable".to_string(),
                });
            }
            Ok(Err(error)) => {
                return Some(ForwardedRoomServiceResponse::TwirpError {
                    status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal".to_string(),
                    msg: format!("remote room service relay dispatch failed: {error}"),
                });
            }
            Err(error) => {
                return Some(ForwardedRoomServiceResponse::TwirpError {
                    status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal".to_string(),
                    msg: format!("remote room service relay task failed: {error}"),
                });
            }
        };

        Some(match response {
            oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success { response } => {
                ForwardedRoomServiceResponse::Protobuf(response)
            }
            oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                status,
                code,
                msg,
            } => ForwardedRoomServiceResponse::TwirpError {
                status: axum::http::StatusCode::from_u16(status)
                    .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
                code,
                msg,
            },
        })
    }

    async fn list_rooms_cluster(&self, request: Vec<u8>) -> Option<ForwardedRoomServiceResponse> {
        use livekit_protocol as proto;
        use prost::Message;

        let request_proto = match proto::ListRoomsRequest::decode(request.as_slice()) {
            Ok(request) => request,
            Err(err) => {
                return Some(ForwardedRoomServiceResponse::TwirpError {
                    status: axum::http::StatusCode::BAD_REQUEST,
                    code: "malformed".to_string(),
                    msg: format!("failed to decode request: {err}"),
                });
            }
        };

        let mut rooms_by_name = BTreeMap::<String, proto::Room>::new();
        match self.signal_state.rooms.list_rooms(&request_proto.names) {
            Ok(local_rooms) => {
                for room in local_rooms {
                    rooms_by_name.insert(room.name.clone(), room);
                }
            }
            Err(err) => {
                return Some(ForwardedRoomServiceResponse::TwirpError {
                    status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal".to_string(),
                    msg: format!("local list rooms failed: {err}"),
                });
            }
        }

        let local_room_node_id = self
            .signal_state
            .local_room_node_id()
            .map(ToOwned::to_owned);
        let node_ids = match self.signal_state.list_registered_room_node_ids() {
            Ok(node_ids) => node_ids,
            Err(error) => {
                return Some(ForwardedRoomServiceResponse::TwirpError {
                    status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal".to_string(),
                    msg: format!("room node directory lookup failed: {error}"),
                });
            }
        };

        for node_id in node_ids {
            if local_room_node_id.as_deref() == Some(node_id.as_str()) {
                continue;
            }

            let intent = oxidesfu_signaling::NonLocalRelayRoomServiceIntent {
                room: String::new(),
                selected_room_node_id: node_id,
                method: "ListRooms".to_string(),
                request: request.clone(),
            };
            let signal_state = self.signal_state.clone();
            let dispatched = tokio::task::spawn_blocking(move || {
                signal_state.dispatch_non_local_room_service(intent)
            })
            .await;

            let response = match dispatched {
                Ok(Ok(Some(response))) => response,
                Ok(Ok(None)) => {
                    return Some(ForwardedRoomServiceResponse::TwirpError {
                        status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        code: "unavailable".to_string(),
                        msg: "remote room service relay response unavailable".to_string(),
                    });
                }
                Ok(Err(error)) => {
                    return Some(ForwardedRoomServiceResponse::TwirpError {
                        status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        code: "internal".to_string(),
                        msg: format!("remote room service relay dispatch failed: {error}"),
                    });
                }
                Err(error) => {
                    return Some(ForwardedRoomServiceResponse::TwirpError {
                        status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        code: "internal".to_string(),
                        msg: format!("remote room service relay task failed: {error}"),
                    });
                }
            };

            match response {
                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success { response } => {
                    let decoded = match proto::ListRoomsResponse::decode(response.as_slice()) {
                        Ok(decoded) => decoded,
                        Err(err) => {
                            return Some(ForwardedRoomServiceResponse::TwirpError {
                                status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                code: "internal".to_string(),
                                msg: format!(
                                    "failed to decode forwarded list rooms response: {err}"
                                ),
                            });
                        }
                    };
                    for room in decoded.rooms {
                        rooms_by_name.entry(room.name.clone()).or_insert(room);
                    }
                }
                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                    status,
                    code,
                    msg,
                } => {
                    return Some(ForwardedRoomServiceResponse::TwirpError {
                        status: axum::http::StatusCode::from_u16(status)
                            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
                        code,
                        msg,
                    });
                }
            }
        }

        let response = proto::ListRoomsResponse {
            rooms: rooms_by_name.into_values().collect(),
        };
        Some(ForwardedRoomServiceResponse::Protobuf(
            response.encode_to_vec(),
        ))
    }
}

#[async_trait]
impl MediaSubscriptionRuntime for SignalStateMediaSubscriptionRuntime {
    async fn apply_update_subscriptions(
        &self,
        room: &str,
        identity: &str,
        track_sids: &[String],
        participant_tracks: &[livekit_protocol::ParticipantTracks],
        subscribe: bool,
    ) {
        self.signal_state
            .apply_twirp_update_subscriptions(
                room,
                identity,
                track_sids,
                participant_tracks,
                subscribe,
            )
            .await;
    }

    async fn disconnect_participant(
        &self,
        room: &str,
        identity: &str,
        reason: livekit_protocol::DisconnectReason,
    ) -> Result<(), RoomStoreError> {
        self.signal_state
            .disconnect_participant_from_service(room, identity, reason)
            .await
    }

    async fn disconnect_room_participants(
        &self,
        room: &str,
        reason: livekit_protocol::DisconnectReason,
    ) -> Result<(), RoomStoreError> {
        let participants = self.signal_state.rooms.list_participants(room)?;
        for participant in participants {
            let _ = self
                .signal_state
                .disconnect_participant_from_service(room, &participant.identity, reason)
                .await;
        }
        Ok(())
    }

    async fn broadcast_participant_update(
        &self,
        room: &str,
        participant: livekit_protocol::ParticipantInfo,
    ) {
        self.signal_state
            .broadcast_participant_update_from_service(room, participant);
    }

    async fn apply_participant_update_from_service(
        &self,
        room: &str,
        previous: Option<livekit_protocol::ParticipantInfo>,
        participant: livekit_protocol::ParticipantInfo,
    ) {
        self.signal_state
            .apply_service_participant_update(room, previous.as_ref(), participant);
    }

    async fn perform_rpc(
        &self,
        room: &str,
        request: &livekit_protocol::PerformRpcRequest,
    ) -> Result<livekit_protocol::PerformRpcResponse, RoomStoreError> {
        self.signal_state
            .perform_rpc_from_service(
                room,
                &request.destination_identity,
                &request.method,
                &request.payload,
                request.response_timeout_ms,
            )
            .await
    }

    async fn room_deleted(&self, room: livekit_protocol::Room) {
        if let Some(dispatcher) = self.webhook_dispatcher.as_ref() {
            dispatcher.emit(crate::webhook::room_finished_webhook_event(room));
        }
    }
}

/// Builds the OxideSFU HTTP router with development configuration wiring.
///
/// This path applies the same config-derived signaling transport/ICE settings
/// as production server startup, but with `ServerConfig::development()` values.
pub fn app() -> Router {
    let config = ServerConfig::development();
    let api_state = api_state_from_config(&config);
    let room_nodes = room_node_directory_from_config(&config)
        .expect("development room node directory should construct");
    let registered = register_local_room_node(&room_nodes, &config)
        .expect("development local room node registration should succeed");

    app_with_api_room_nodes_from_config(api_state, Some(room_nodes), Some(registered.id), &config)
}

/// Builds the OxideSFU HTTP router with the provided API state.
pub fn app_with_api(api_state: ApiState) -> Router {
    app_with_api_and_room_nodes(api_state, None)
}

/// Builds the OxideSFU HTTP router with API state and optional room-node directory.
pub fn app_with_api_and_room_nodes(
    api_state: ApiState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
) -> Router {
    app_with_api_room_nodes_and_placement(api_state, room_nodes, None, false)
}

/// Builds the OxideSFU HTTP router with API state, optional room-node directory, and placement controls.
pub fn app_with_api_room_nodes_and_placement(
    api_state: ApiState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    local_room_node_id: Option<String>,
    reject_non_local_room_placement: bool,
) -> Router {
    app_with_api_room_nodes_placement_and_relay_dispatcher(
        api_state,
        room_nodes,
        local_room_node_id,
        reject_non_local_room_placement,
        Arc::new(oxidesfu_signaling::NoopNonLocalRelayDispatcher),
    )
}

/// Builds the OxideSFU HTTP router with API state, placement controls, and non-local relay dispatcher.
pub fn app_with_api_room_nodes_placement_and_relay_dispatcher(
    api_state: ApiState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    local_room_node_id: Option<String>,
    reject_non_local_room_placement: bool,
    non_local_relay_dispatcher: Arc<dyn oxidesfu_signaling::NonLocalRelayDispatcher>,
) -> Router {
    app_with_api_room_nodes_relay_dispatcher_and_readiness(
        api_state,
        room_nodes,
        local_room_node_id,
        reject_non_local_room_placement,
        non_local_relay_dispatcher,
        Arc::new(AlwaysReadyRelayBackendReadiness),
    )
}

/// Builds the OxideSFU HTTP router with API state, placement controls, relay dispatcher, and relay readiness probe.
pub fn app_with_api_room_nodes_relay_dispatcher_and_readiness(
    api_state: ApiState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    local_room_node_id: Option<String>,
    reject_non_local_room_placement: bool,
    non_local_relay_dispatcher: Arc<dyn oxidesfu_signaling::NonLocalRelayDispatcher>,
    relay_backend_readiness: Arc<dyn RelayBackendReadiness>,
) -> Router {
    let signal_state = oxidesfu_signaling::SignalState::with_data_channels_room_nodes_placement_and_relay_dispatcher(
        api_state.rooms.clone(),
        api_state.auth.clone(),
        api_state.data_channels.clone(),
        room_nodes.clone(),
        local_room_node_id,
        reject_non_local_room_placement,
        non_local_relay_dispatcher,
    );

    app_with_api_signal_state_and_readiness(
        api_state,
        signal_state,
        room_nodes,
        relay_backend_readiness,
    )
}

/// Builds the OxideSFU HTTP router with a prebuilt signalling state and relay readiness probe.
pub fn app_with_api_signal_state_and_readiness(
    api_state: ApiState,
    signal_state: oxidesfu_signaling::SignalState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    relay_backend_readiness: Arc<dyn RelayBackendReadiness>,
) -> Router {
    app_with_api_signal_state_readiness_webhooks_and_agent_relay(
        api_state,
        signal_state,
        room_nodes,
        relay_backend_readiness,
        None,
        None,
    )
}

pub fn app_with_api_signal_state_readiness_and_webhooks(
    api_state: ApiState,
    signal_state: oxidesfu_signaling::SignalState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    relay_backend_readiness: Arc<dyn RelayBackendReadiness>,
    webhook_dispatcher: Option<Arc<crate::webhook::WebhookDispatcher>>,
) -> Router {
    app_with_api_signal_state_readiness_webhooks_and_agent_relay(
        api_state,
        signal_state,
        room_nodes,
        relay_backend_readiness,
        webhook_dispatcher,
        None,
    )
}

pub fn app_with_api_signal_state_readiness_webhooks_and_agent_relay(
    api_state: ApiState,
    signal_state: oxidesfu_signaling::SignalState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    relay_backend_readiness: Arc<dyn RelayBackendReadiness>,
    webhook_dispatcher: Option<Arc<crate::webhook::WebhookDispatcher>>,
    agent_redis_url: Option<String>,
) -> Router {
    let mut api_state = api_state;
    let mut agent_state =
        oxidesfu_agent::AgentState::new(api_state.auth.clone(), api_state.rooms.clone());
    if let Some(redis_url) = agent_redis_url {
        agent_state = agent_state.with_redis_webhook_relay(redis_url);
    }
    let signal_state =
        signal_state.with_additional_webhook_event_handler(agent_state.signal_webhook_handler());
    api_state.media_subscription_runtime = Some(Arc::new(SignalStateMediaSubscriptionRuntime {
        signal_state: signal_state.clone(),
        webhook_dispatcher,
    }));
    api_state.room_service_forwarder = Some(Arc::new(SignalStateRoomServiceForwarder {
        signal_state: signal_state.clone(),
    }));

    let readiness_room_nodes = room_nodes;
    let readiness_relay_backend = relay_backend_readiness;
    let app = Router::new()
        .route("/healthz", get(crate::health::healthz))
        .route(
            "/readyz",
            get(move || {
                crate::readiness::readinessz(
                    readiness_room_nodes.clone(),
                    readiness_relay_backend.clone(),
                )
            }),
        )
        .route("/metrics", get(crate::metrics::metrics))
        .route(
            "/debug/forwarding-snapshots",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/x-ndjson")],
                    oxidesfu_signaling::forwarding_snapshot_json_lines(),
                )
            }),
        )
        .merge(oxidesfu_api::router(api_state))
        .merge(oxidesfu_signaling::router(signal_state))
        .merge(oxidesfu_agent::router(agent_state))
        .layer(middleware::from_fn(crate::logging::attach_request_id));

    Router::new()
        .fallback_service(app)
        .layer(middleware::from_fn(normalize_path_and_reflect_origin))
}

/// Builds the OxideSFU HTTP router with placement controls sourced from server configuration.
pub fn app_with_api_room_nodes_from_config(
    api_state: ApiState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    local_room_node_id: Option<String>,
    config: &ServerConfig,
) -> Router {
    app_with_api_room_nodes_from_config_and_relay_dispatcher(
        api_state,
        room_nodes,
        local_room_node_id,
        config,
        Arc::new(oxidesfu_signaling::NoopNonLocalRelayDispatcher),
    )
}

/// Builds the OxideSFU HTTP router with placement controls and relay dispatcher sourced from config.
pub fn app_with_api_room_nodes_from_config_and_relay_dispatcher(
    api_state: ApiState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    local_room_node_id: Option<String>,
    config: &ServerConfig,
    non_local_relay_dispatcher: Arc<dyn oxidesfu_signaling::NonLocalRelayDispatcher>,
) -> Router {
    app_with_api_room_nodes_from_config_relay_dispatcher_and_readiness(
        api_state,
        room_nodes,
        local_room_node_id,
        config,
        non_local_relay_dispatcher,
        Arc::new(AlwaysReadyRelayBackendReadiness),
    )
}

/// Builds the OxideSFU router with placement controls, relay dispatcher, and relay readiness sourced from config.
pub fn app_with_api_room_nodes_from_config_relay_dispatcher_and_readiness(
    api_state: ApiState,
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    local_room_node_id: Option<String>,
    config: &ServerConfig,
    non_local_relay_dispatcher: Arc<dyn oxidesfu_signaling::NonLocalRelayDispatcher>,
    relay_backend_readiness: Arc<dyn RelayBackendReadiness>,
) -> Router {
    let signal_state = oxidesfu_signaling::SignalState::with_data_channels_room_nodes_placement_and_relay_dispatcher(
        api_state.rooms.clone(),
        api_state.auth.clone(),
        api_state.data_channels.clone(),
        room_nodes.clone(),
        local_room_node_id,
        config.reject_non_local_room_placement,
        non_local_relay_dispatcher,
    )
    .with_ice_servers(signal_ice_servers_from_config(config))
        .with_room_auto_create(config.room_auto_create)
        .with_rtc_transport_config(rtc_transport_config_from_server_config(config))
        .with_datachannel_slow_threshold_bytes(config.datachannel_slow_threshold)
        .with_participant_data_blob_enabled(config.participant_data_blob_enabled);

    app_with_api_signal_state_and_readiness(
        api_state,
        signal_state,
        room_nodes,
        relay_backend_readiness,
    )
}

#[cfg(test)]
mod tests;

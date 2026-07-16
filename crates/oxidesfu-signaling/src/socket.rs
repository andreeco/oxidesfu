use std::time::Duration;

use axum::extract::ws::Message as WsMessage;
use livekit_protocol as proto;
use prost::Message;
use tokio::sync::mpsc;

use crate::{
    errors::{SignalError, SignalResult},
    metrics::current_unix_millis,
    relay::{
        NonLocalRelayOutboundSignalQuery, NonLocalRelaySignalRequestIntent,
        NonLocalRelaySignalRequestResponse,
    },
    signal_request::signal_response_for_request,
    state::SignalState,
};

pub(crate) async fn drain_relay_outbound_responses(
    mut outbound_rx: mpsc::UnboundedReceiver<proto::SignalResponse>,
) -> Vec<Vec<u8>> {
    const MAX_RELAY_OUTBOUND_RESPONSES: usize = 16;
    const RELAY_OUTBOUND_DRAIN_WINDOW: Duration = Duration::from_millis(200);
    const RELAY_OUTBOUND_IDLE_STEP: Duration = Duration::from_millis(20);

    let deadline = tokio::time::Instant::now() + RELAY_OUTBOUND_DRAIN_WINDOW;
    let mut responses = Vec::new();

    while responses.len() < MAX_RELAY_OUTBOUND_RESPONSES {
        while let Ok(response) = outbound_rx.try_recv() {
            responses.push(response.encode_to_vec());
            if responses.len() >= MAX_RELAY_OUTBOUND_RESPONSES {
                return responses;
            }
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }

        match tokio::time::timeout(RELAY_OUTBOUND_IDLE_STEP, outbound_rx.recv()).await {
            Ok(Some(response)) => responses.push(response.encode_to_vec()),
            Ok(None) => break,
            Err(_) => {}
        }
    }

    responses
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum SocketSignalResponse {
    Typed(proto::SignalResponse),
    // TODO(protocol-upgrade): remove this raw response escape hatch once the
    // generated livekit-protocol crate exposes DataBlob response variants.
    Raw(Vec<u8>),
}

impl SocketSignalResponse {
    pub(crate) fn encode(self) -> Vec<u8> {
        match self {
            Self::Typed(response) => response.encode_to_vec(),
            Self::Raw(bytes) => bytes,
        }
    }

    pub(crate) fn typed(&self) -> Option<&proto::SignalResponse> {
        match self {
            Self::Typed(response) => Some(response),
            Self::Raw(_) => None,
        }
    }
}

pub(crate) async fn handle_socket_message(
    message: WsMessage,
    state: &SignalState,
    room_name: &str,
    identity: &str,
    outbound_tx: &crate::router::OutboundSignalSender,
) -> SignalResult<Option<SocketSignalResponse>> {
    match message {
        WsMessage::Binary(bytes) => {
            if let Some(response_bytes) = crate::signal_request::raw_data_blob_response_bytes(
                bytes.as_ref(),
                state,
                room_name,
                identity,
            ) {
                return Ok(Some(SocketSignalResponse::Raw(response_bytes)));
            }
            signal_response_for_request(
                proto::SignalRequest::decode(bytes.as_ref())?,
                state,
                room_name,
                identity,
                outbound_tx,
            )
            .await
            .map(|response| response.map(SocketSignalResponse::Typed))
        }
        WsMessage::Text(text) => {
            let request: proto::SignalRequest = serde_json::from_str(text.as_ref())?;
            signal_response_for_request(request, state, room_name, identity, outbound_tx)
                .await
                .map(|response| response.map(SocketSignalResponse::Typed))
        }
        WsMessage::Close(_) => Err(SignalError::WebSocketClosed),
        WsMessage::Ping(_) | WsMessage::Pong(_) => Ok(None),
    }
}

pub(crate) async fn handle_non_local_relay_socket_message(
    message: WsMessage,
    state: &SignalState,
    room_name: &str,
    identity: &str,
    selected_room_node_id: &str,
) -> SignalResult<Vec<WsMessage>> {
    let request_bytes = match message {
        WsMessage::Binary(bytes) => bytes.to_vec(),
        WsMessage::Text(text) => {
            let request: proto::SignalRequest = serde_json::from_str(text.as_ref())?;
            request.encode_to_vec()
        }
        WsMessage::Close(_) => return Err(SignalError::WebSocketClosed),
        WsMessage::Ping(_) | WsMessage::Pong(_) => return Ok(Vec::new()),
    };

    handle_non_local_relay_signal_request_bytes(
        request_bytes,
        state,
        room_name,
        identity,
        selected_room_node_id,
    )
    .await
}

async fn handle_non_local_relay_signal_request_bytes(
    request_bytes: Vec<u8>,
    state: &SignalState,
    room_name: &str,
    identity: &str,
    selected_room_node_id: &str,
) -> SignalResult<Vec<WsMessage>> {
    let decoded_request = proto::SignalRequest::decode(request_bytes.as_slice()).ok();

    let local_response = match decoded_request
        .as_ref()
        .and_then(|request| request.message.as_ref())
    {
        Some(proto::signal_request::Message::Ping(_)) => Some(proto::SignalResponse {
            message: Some(proto::signal_response::Message::Pong(current_unix_millis())),
        }),
        Some(proto::signal_request::Message::PingReq(ping)) => Some(proto::SignalResponse {
            message: Some(proto::signal_response::Message::PongResp(proto::Pong {
                last_ping_timestamp: ping.timestamp,
                timestamp: current_unix_millis(),
            })),
        }),
        _ => None,
    };
    if let Some(response) = local_response {
        return Ok(vec![WsMessage::Binary(response.encode_to_vec().into())]);
    }

    let should_close_if_unhandled = matches!(
        decoded_request
            .as_ref()
            .and_then(|request| request.message.as_ref()),
        Some(proto::signal_request::Message::Leave(_))
    );
    let intent = NonLocalRelaySignalRequestIntent {
        room: room_name.to_string(),
        identity: identity.to_string(),
        selected_room_node_id: selected_room_node_id.to_string(),
        signal_request: request_bytes,
    };

    let dispatcher = state.non_local_relay_dispatcher.clone();
    let dispatch =
        tokio::task::spawn_blocking(move || dispatcher.dispatch_non_local_signal_request(intent))
            .await
            .map_err(|err| SignalError::RequestHandling {
                message: err.to_string(),
            })?;

    match dispatch.map_err(|err| SignalError::RequestHandling { message: err })? {
        Some(NonLocalRelaySignalRequestResponse::Response {
            signal_response,
            outbound_signal_responses,
        }) => {
            let mut responses = vec![WsMessage::Binary(signal_response.into())];
            responses.extend(
                outbound_signal_responses
                    .into_iter()
                    .map(|response| WsMessage::Binary(response.into())),
            );
            Ok(responses)
        }
        Some(NonLocalRelaySignalRequestResponse::Outbound {
            outbound_signal_responses,
        }) => Ok(outbound_signal_responses
            .into_iter()
            .map(|response| WsMessage::Binary(response.into()))
            .collect()),
        Some(NonLocalRelaySignalRequestResponse::NoResponse) | None
            if should_close_if_unhandled =>
        {
            Err(SignalError::RemoteRelayedSessionClosed)
        }
        Some(NonLocalRelaySignalRequestResponse::NoResponse) | None => Ok(Vec::new()),
        Some(NonLocalRelaySignalRequestResponse::Closed) => {
            Err(SignalError::RemoteRelayedSessionClosed)
        }
        Some(NonLocalRelaySignalRequestResponse::Error { message }) => {
            Err(SignalError::RequestHandling { message })
        }
    }
}

pub(crate) fn outbound_relay_query(
    room_name: &str,
    identity: &str,
    selected_room_node_id: &str,
) -> NonLocalRelayOutboundSignalQuery {
    NonLocalRelayOutboundSignalQuery {
        room: room_name.to_string(),
        identity: identity.to_string(),
        selected_room_node_id: selected_room_node_id.to_string(),
        max_events: 32,
    }
}

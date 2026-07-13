use std::collections::HashSet;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use livekit_protocol as proto;
use prost::Message;

use crate::{
    errors::{auth_error, authenticate, room_store_error},
    router::maybe_forward_room_service,
    state::{ApiState, RoomServiceMethod},
    twirp::{decode, encode, request_codec, twirp_error},
};

pub(crate) async fn send_data(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::SendDataRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_admin_permission(&request.room) {
        return auth_error(err);
    }

    // LiveKit compatibility: nonce is either absent or exactly 16 bytes.
    if !request.nonce.is_empty() && request.nonce.len() != 16 {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            &format!(
                "nonce should be 16-bytes or not present, got: {} bytes",
                request.nonce.len()
            ),
        );
    }

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::SendData,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    match state.rooms.ensure_room_exists(&request.room) {
        Ok(()) => {
            if let Err(err) = send_data_packet(&state, &request).await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("data send failed: {err}"),
                )
                    .into_response();
            }
            encode(codec, &proto::SendDataResponse {})
        }
        Err(err) => room_store_error(err),
    }
}

#[allow(deprecated)]
async fn send_data_packet(
    state: &ApiState,
    request: &proto::SendDataRequest,
) -> oxidesfu_rtc::RtcResult<usize> {
    let has_explicit_destinations =
        !request.destination_identities.is_empty() || !request.destination_sids.is_empty();
    let resolved_destination_identities =
        resolve_destination_identities(state, &request.room, request)?;

    let packet = proto::DataPacket {
        kind: request.kind,
        destination_identities: request.destination_identities.clone(),
        value: Some(proto::data_packet::Value::User(proto::UserPacket {
            payload: request.data.clone(),
            destination_sids: request.destination_sids.clone(),
            destination_identities: request.destination_identities.clone(),
            topic: request.topic.clone(),
            nonce: request.nonce.clone(),
            ..Default::default()
        })),
        ..Default::default()
    };
    let channel_kind = if request.kind == proto::data_packet::Kind::Lossy as i32 {
        oxidesfu_rtc::DataChannelKind::Lossy
    } else {
        oxidesfu_rtc::DataChannelKind::Reliable
    };
    if has_explicit_destinations {
        if resolved_destination_identities.is_empty() {
            return Ok(0);
        }

        return state
            .data_channels
            .send_bytes_to_identities_with_kind(
                &request.room,
                &resolved_destination_identities,
                channel_kind,
                &packet.encode_to_vec(),
            )
            .await;
    }

    state
        .data_channels
        .send_bytes_to_room_with_kind(&request.room, channel_kind, &packet.encode_to_vec())
        .await
}

#[allow(deprecated)]
fn resolve_destination_identities(
    state: &ApiState,
    room: &str,
    request: &proto::SendDataRequest,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    if request.destination_sids.is_empty() {
        return Ok(request.destination_identities.clone());
    }

    let participants = state.rooms.list_participants(room)?;
    let identities_by_sid = participants
        .into_iter()
        .map(|participant| (participant.sid, participant.identity))
        .collect::<std::collections::HashMap<_, _>>();

    let mut visited = HashSet::new();
    let mut resolved = Vec::new();

    for identity in &request.destination_identities {
        if visited.insert(identity.clone()) {
            resolved.push(identity.clone());
        }
    }

    for sid in &request.destination_sids {
        if let Some(identity) = identities_by_sid.get(sid)
            && visited.insert(identity.clone())
        {
            resolved.push(identity.clone());
        }
    }

    Ok(resolved)
}

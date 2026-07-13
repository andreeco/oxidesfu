use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::post,
};
use livekit_protocol as proto;
use prost::Message;

use crate::{
    errors::{auth_error, authenticate, room_store_error},
    send_data,
    state::{ApiState, ForwardedRoomServiceResponse, RoomServiceMethod},
    twirp::{
        AGENT_DISPATCH_SERVICE_PREFIX, EGRESS_SERVICE_PREFIX, INGRESS_SERVICE_PREFIX,
        ROOM_SERVICE_PREFIX, decode, encode, protobuf_bytes, request_codec, twirp_error,
        twirp_error_owned,
    },
};

const MAX_ROOM_METADATA_BYTES: usize = 512 * 1024;
const MAX_RPC_METHOD_BYTES: usize = 64;
const MAX_RPC_PAYLOAD_BYTES: usize = 15 * 1024;

pub(crate) async fn maybe_forward_room_service(
    state: &ApiState,
    room: &str,
    method: RoomServiceMethod,
    request: &[u8],
) -> Option<Response> {
    let forwarder = state.room_service_forwarder.as_ref()?;
    let forwarded = forwarder
        .forward_if_non_local(room, method, request.to_vec())
        .await?;

    Some(match forwarded {
        ForwardedRoomServiceResponse::Protobuf(bytes) => protobuf_bytes(bytes),
        ForwardedRoomServiceResponse::TwirpError { status, code, msg } => {
            twirp_error_owned(status, code, msg)
        }
    })
}

/// Builds the LiveKit-compatible Twirp API router.
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/CreateRoom"),
            post(create_room),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/ListRooms"),
            post(list_rooms),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/DeleteRoom"),
            post(delete_room),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/UpdateRoomMetadata"),
            post(update_room_metadata),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/ListParticipants"),
            post(list_participants),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/GetParticipant"),
            post(get_participant),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/RemoveParticipant"),
            post(remove_participant),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/UpdateParticipant"),
            post(update_participant),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/MutePublishedTrack"),
            post(mute_published_track),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/UpdateSubscriptions"),
            post(update_subscriptions),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/ForwardParticipant"),
            post(forward_participant),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/MoveParticipant"),
            post(move_participant),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/SendData"),
            post(send_data::send_data),
        )
        .route(
            &format!("{ROOM_SERVICE_PREFIX}/PerformRpc"),
            post(perform_rpc),
        )
        .route(
            &format!("{AGENT_DISPATCH_SERVICE_PREFIX}/CreateDispatch"),
            post(create_agent_dispatch),
        )
        .route(
            &format!("{AGENT_DISPATCH_SERVICE_PREFIX}/DeleteDispatch"),
            post(delete_agent_dispatch),
        )
        .route(
            &format!("{AGENT_DISPATCH_SERVICE_PREFIX}/ListDispatch"),
            post(list_agent_dispatch),
        )
        // Ingress/Egress support is incremental. Methods without runtime backing stay
        // explicitly unimplemented to preserve deterministic Twirp envelopes.
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/StartEgress"),
            post(start_egress),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/UpdateLayout"),
            post(update_layout),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/UpdateStream"),
            post(update_stream),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/ListEgress"),
            post(list_egress),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/StopEgress"),
            post(stop_egress),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/StartRoomCompositeEgress"),
            post(start_room_composite_egress),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/StartWebEgress"),
            post(start_web_egress),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/StartParticipantEgress"),
            post(start_participant_egress),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/StartTrackCompositeEgress"),
            post(start_track_composite_egress),
        )
        .route(
            &format!("{EGRESS_SERVICE_PREFIX}/StartTrackEgress"),
            post(start_track_egress),
        )
        .route(
            &format!("{INGRESS_SERVICE_PREFIX}/CreateIngress"),
            post(create_ingress),
        )
        .route(
            &format!("{INGRESS_SERVICE_PREFIX}/UpdateIngress"),
            post(update_ingress),
        )
        .route(
            &format!("{INGRESS_SERVICE_PREFIX}/ListIngress"),
            post(list_ingress),
        )
        .route(
            &format!("{INGRESS_SERVICE_PREFIX}/DeleteIngress"),
            post(delete_ingress),
        )
        .with_state(state)
}

async fn create_room(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_create_permission() {
        return auth_error(err);
    }

    let request = match decode::<proto::CreateRoomRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    if request.name.is_empty() {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "room name cannot be empty",
        );
    }

    if request.metadata.len() > MAX_ROOM_METADATA_BYTES {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "metadata exceeds 512KiB limit",
        );
    }

    match state.rooms.create_room(request) {
        Ok(room) => encode(codec, &room),
        Err(err) => room_store_error(err),
    }
}

async fn list_rooms(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_list_permission() {
        return auth_error(err);
    }

    let request = match decode::<proto::ListRoomsRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    if let Some(forwarder) = state.room_service_forwarder.as_ref()
        && let Some(response) = forwarder.list_rooms_cluster(request.encode_to_vec()).await
    {
        return match response {
            ForwardedRoomServiceResponse::Protobuf(bytes) => {
                if codec == crate::twirp::TwirpCodec::Json {
                    match proto::ListRoomsResponse::decode(bytes.as_slice()) {
                        Ok(list_rooms_response) => encode(codec, &list_rooms_response),
                        Err(_) => twirp_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal",
                            "failed to decode forwarded list rooms response",
                        ),
                    }
                } else {
                    protobuf_bytes(bytes)
                }
            }
            ForwardedRoomServiceResponse::TwirpError { status, code, msg } => {
                twirp_error_owned(status, code, msg)
            }
        };
    }

    match state.rooms.list_rooms(&request.names) {
        Ok(rooms) => encode(codec, &proto::ListRoomsResponse { rooms }),
        Err(err) => room_store_error(err),
    }
}

async fn delete_room(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_create_permission() {
        return auth_error(err);
    }

    let request = match decode::<proto::DeleteRoomRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::DeleteRoom,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    if let Err(err) = state.rooms.ensure_room_exists(&request.room) {
        return room_store_error(err);
    }
    if let Some(runtime) = &state.media_subscription_runtime
        && let Err(err) = runtime
            .disconnect_room_participants(&request.room, proto::DisconnectReason::RoomDeleted)
            .await
    {
        return room_store_error(err);
    }

    match state.rooms.delete_room_with_snapshot(&request.room) {
        Ok(room) => {
            if let Some(runtime) = &state.media_subscription_runtime {
                runtime.room_deleted(room).await;
            }
            encode(codec, &proto::DeleteRoomResponse {})
        }
        Err(err) => room_store_error(err),
    }
}

async fn update_room_metadata(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::UpdateRoomMetadataRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    if request.metadata.len() > MAX_ROOM_METADATA_BYTES {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "metadata exceeds 512KiB limit",
        );
    }

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_admin_permission(&request.room) {
        return auth_error(err);
    }

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::UpdateRoomMetadata,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    match state
        .rooms
        .update_room_metadata(&request.room, request.metadata)
    {
        Ok(room) => encode(codec, &room),
        Err(err) => room_store_error(err),
    }
}

async fn list_participants(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::ListParticipantsRequest>(codec, &body) {
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

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::ListParticipants,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    match state.rooms.list_participants(&request.room) {
        Ok(participants) => encode(codec, &proto::ListParticipantsResponse { participants }),
        Err(err) => room_store_error(err),
    }
}

async fn get_participant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::RoomParticipantIdentity>(codec, &body) {
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

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::GetParticipant,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    match state
        .rooms
        .get_participant(&request.room, &request.identity)
    {
        Ok(participant) => encode(codec, &participant),
        Err(err) => room_store_error(err),
    }
}

async fn remove_participant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::RoomParticipantIdentity>(codec, &body) {
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

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::RemoveParticipant,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    if let Some(runtime) = &state.media_subscription_runtime {
        match runtime
            .disconnect_participant(
                &request.room,
                &request.identity,
                proto::DisconnectReason::ParticipantRemoved,
            )
            .await
        {
            Ok(()) => return encode(codec, &proto::RemoveParticipantResponse {}),
            Err(err) => return room_store_error(err),
        }
    }

    match state.rooms.remove_participant_with_reason(
        &request.room,
        &request.identity,
        proto::DisconnectReason::ParticipantRemoved,
    ) {
        Ok(_) => encode(codec, &proto::RemoveParticipantResponse {}),
        Err(err) => room_store_error(err),
    }
}

async fn update_participant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::UpdateParticipantRequest>(codec, &body) {
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

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::UpdateParticipant,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    let previous = state
        .rooms
        .get_participant(&request.room, &request.identity)
        .ok();

    match state.rooms.update_participant(
        &request.room,
        &request.identity,
        &request.metadata,
        &request.name,
        request.permission,
        request.attributes,
    ) {
        Ok(participant) => {
            if let Some(runtime) = &state.media_subscription_runtime {
                runtime
                    .apply_participant_update_from_service(
                        &request.room,
                        previous.clone(),
                        participant.clone(),
                    )
                    .await;
            }
            encode(codec, &participant)
        }
        Err(err) => room_store_error(err),
    }
}

async fn mute_published_track(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::MuteRoomTrackRequest>(codec, &body) {
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
    if !request.muted && !state.enable_remote_unmute {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "cannot unmute track, remote unmute is disabled",
        );
    }

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::MutePublishedTrack,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    match state.rooms.set_participant_track_muted(
        &request.room,
        &request.identity,
        &request.track_sid,
        request.muted,
    ) {
        Ok(track) => {
            if let Some(runtime) = &state.media_subscription_runtime
                && let Ok(participant) = state
                    .rooms
                    .get_participant(&request.room, &request.identity)
            {
                runtime
                    .broadcast_participant_update(&request.room, participant)
                    .await;
            }
            encode(codec, &proto::MuteRoomTrackResponse { track: Some(track) })
        }
        Err(err) => room_store_error(err),
    }
}

async fn update_subscriptions(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::UpdateSubscriptionsRequest>(codec, &body) {
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

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::UpdateSubscriptions,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    if let Err(err) = state
        .rooms
        .get_participant(&request.room, &request.identity)
    {
        return room_store_error(err);
    }

    let mut applied_any = false;
    for track_sid in &request.track_sids {
        match state.rooms.set_media_track_subscribed_by_track_sid(
            &request.room,
            &request.identity,
            track_sid,
            request.subscribe,
        ) {
            Ok(applied) => {
                applied_any |= applied;
            }
            Err(err) => return room_store_error(err),
        }
    }

    for participant_tracks in &request.participant_tracks {
        for track_sid in &participant_tracks.track_sids {
            match state.rooms.set_media_track_subscribed_by_publisher_sid(
                &request.room,
                &participant_tracks.participant_sid,
                track_sid,
                &request.identity,
                request.subscribe,
            ) {
                Ok(applied) => {
                    applied_any |= applied;
                }
                Err(err) => return room_store_error(err),
            }
        }
    }

    if applied_any && let Some(runtime) = &state.media_subscription_runtime {
        runtime
            .apply_update_subscriptions(
                &request.room,
                &request.identity,
                &request.track_sids,
                &request.participant_tracks,
                request.subscribe,
            )
            .await;
    }

    encode(codec, &proto::UpdateSubscriptionsResponse {})
}

async fn forward_participant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::ForwardParticipantRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) =
        auth.ensure_destination_room_permission(&request.room, &request.destination_room)
    {
        return auth_error(err);
    }
    if request.room == request.destination_room {
        return crate::twirp::twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "destination room cannot be same as source room",
        );
    }

    match state.rooms.forward_participant(
        &request.room,
        &request.identity,
        &request.destination_room,
    ) {
        Ok(()) => encode(codec, &proto::ForwardParticipantResponse {}),
        Err(err) => room_store_error(err),
    }
}

async fn move_participant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::MoveParticipantRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) =
        auth.ensure_destination_room_permission(&request.room, &request.destination_room)
    {
        return auth_error(err);
    }
    if request.room == request.destination_room {
        return crate::twirp::twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "destination room cannot be same as source room",
        );
    }

    match state
        .rooms
        .move_participant(&request.room, &request.identity, &request.destination_room)
    {
        Ok(_) => encode(codec, &proto::MoveParticipantResponse {}),
        Err(err) => room_store_error(err),
    }
}

async fn list_egress(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::ListEgressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state
        .rooms
        .list_egress_infos(&request.room_name, request.active)
    {
        Ok(mut items) => {
            if !request.egress_id.is_empty() {
                items.retain(|item| item.egress_id == request.egress_id);
            }
            encode(
                codec,
                &proto::ListEgressResponse {
                    items,
                    next_page_token: None,
                },
            )
        }
        Err(err) => room_store_error(err),
    }
}

async fn list_ingress(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::ListIngressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_ingress_admin_permission() {
        return auth_error(err);
    }

    match state.rooms.list_ingress_infos(&request.room_name) {
        Ok(mut items) => {
            if !request.ingress_id.is_empty() {
                items.retain(|item| item.ingress_id == request.ingress_id);
            }
            encode(
                codec,
                &proto::ListIngressResponse {
                    items,
                    next_page_token: None,
                },
            )
        }
        Err(err) => room_store_error(err),
    }
}

async fn start_egress(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::StartEgressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state.rooms.start_egress_info(&request) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn start_room_composite_egress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::RoomCompositeEgressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state.rooms.start_room_composite_egress_info(&request) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn start_web_egress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::WebEgressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state.rooms.start_web_egress_info(&request) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn start_participant_egress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::ParticipantEgressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state.rooms.start_participant_egress_info(&request) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn start_track_composite_egress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::TrackCompositeEgressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state.rooms.start_track_composite_egress_info(&request) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn start_track_egress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::TrackEgressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state.rooms.start_track_egress_info(&request) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn update_layout(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::UpdateLayoutRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state
        .rooms
        .update_egress_layout(&request.egress_id, &request.layout)
    {
        Ok(info) => encode(codec, &info),
        Err(oxidesfu_room::RoomStoreError::InvalidArgument(message))
            if message.starts_with("egress with status ")
                && message.ends_with(" cannot be updated") =>
        {
            twirp_error(
                StatusCode::PRECONDITION_FAILED,
                "failed_precondition",
                &message,
            )
        }
        Err(err) => room_store_error(err),
    }
}

async fn update_stream(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::UpdateStreamRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state.rooms.update_egress_stream_urls(
        &request.egress_id,
        &request.add_output_urls,
        &request.remove_output_urls,
    ) {
        Ok(info) => encode(codec, &info),
        Err(oxidesfu_room::RoomStoreError::InvalidArgument(message))
            if message.starts_with("egress with status ")
                && message.ends_with(" cannot be updated") =>
        {
            twirp_error(
                StatusCode::PRECONDITION_FAILED,
                "failed_precondition",
                &message,
            )
        }
        Err(err) => room_store_error(err),
    }
}

async fn stop_egress(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::StopEgressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_record_permission() {
        return auth_error(err);
    }

    match state.rooms.stop_egress_info(&request.egress_id) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn create_ingress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::CreateIngressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_ingress_admin_permission() {
        return auth_error(err);
    }

    match state.rooms.create_ingress_info(&request) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn update_ingress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::UpdateIngressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_ingress_admin_permission() {
        return auth_error(err);
    }

    match state.rooms.update_ingress_from_request(&request) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn delete_ingress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::DeleteIngressRequest>(codec, &body) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    let auth = match authenticate(&state, &headers) {
        Ok(auth) => auth,
        Err(err) => return auth_error(err),
    };
    if let Err(err) = auth.ensure_ingress_admin_permission() {
        return auth_error(err);
    }

    match state.rooms.delete_ingress_by_id(&request.ingress_id) {
        Ok(info) => encode(codec, &info),
        Err(err) => room_store_error(err),
    }
}

async fn perform_rpc(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::PerformRpcRequest>(codec, &body) {
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
    if request.destination_identity.is_empty() {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "destination identity is required",
        );
    }
    if request.method.as_bytes().len() > MAX_RPC_METHOD_BYTES {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "rpc method must be at most 64 bytes",
        );
    }
    if request.payload.as_bytes().len() > MAX_RPC_PAYLOAD_BYTES {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "rpc payload must be at most 15KiB",
        );
    }

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::PerformRpc,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    if state.media_subscription_runtime.is_none() {
        return twirp_error(
            StatusCode::NOT_IMPLEMENTED,
            "unimplemented",
            "perform rpc is not implemented",
        );
    }

    if let Err(err) = state.rooms.ensure_room_exists(&request.room) {
        return room_store_error(err);
    }

    let Some(runtime) = state.media_subscription_runtime.as_ref() else {
        return twirp_error(
            StatusCode::NOT_IMPLEMENTED,
            "unimplemented",
            "perform rpc is not implemented",
        );
    };

    match runtime.perform_rpc(&request.room, &request).await {
        Ok(response) => encode(codec, &response),
        Err(err) => room_store_error(err),
    }
}

async fn create_agent_dispatch(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::CreateAgentDispatchRequest>(codec, &body) {
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

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::CreateDispatch,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    match state.rooms.create_agent_dispatch(request) {
        Ok(dispatch) => encode(codec, &dispatch),
        Err(err) => room_store_error(err),
    }
}

async fn delete_agent_dispatch(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::DeleteAgentDispatchRequest>(codec, &body) {
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

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::DeleteDispatch,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    match state
        .rooms
        .delete_agent_dispatch(&request.room, &request.dispatch_id)
    {
        Ok(dispatch) => encode(codec, &dispatch),
        Err(err) => room_store_error(err),
    }
}

async fn list_agent_dispatch(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let codec = request_codec(&headers, &body);
    let request = match decode::<proto::ListAgentDispatchRequest>(codec, &body) {
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

    if let Some(response) = maybe_forward_room_service(
        &state,
        &request.room,
        RoomServiceMethod::ListDispatch,
        &request.encode_to_vec(),
    )
    .await
    {
        return response;
    }

    match state
        .rooms
        .list_agent_dispatches(&request.room, &request.dispatch_id)
    {
        Ok(agent_dispatches) => encode(
            codec,
            &proto::ListAgentDispatchResponse { agent_dispatches },
        ),
        Err(err) => room_store_error(err),
    }
}

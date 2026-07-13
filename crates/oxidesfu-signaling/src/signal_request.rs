use livekit_protocol as proto;
use prost::Message;

use crate::{
    errors::{SignalError, SignalResult},
    metrics::current_unix_millis,
    state::SignalState,
    stores::DataTrackPublishError,
};

const MAX_PARTICIPANT_DATA_BLOB_BYTES: usize = 50 * 1024;
// protocol/protobufs/livekit_rtc.proto::RequestResponse.Reason::INVALID_REQUEST
// is currently not exposed by the crates.io livekit-protocol release used by this workspace.
// Keep wire-compat by emitting numeric reason code directly.
const REQUEST_RESPONSE_REASON_INVALID_REQUEST: i32 = 11;

// TODO(protocol-upgrade): remove these Compat* wire types when the crates.io
// livekit-protocol release includes SignalRequest tags 22/23 and SignalResponse
// tags 30/31 from protocol/protobufs/livekit_rtc.proto. At that point, handle
// StoreDataBlobRequest/GetDataBlobRequest through the generated
// proto::signal_request::Message variants and emit generated SignalResponse
// variants instead of raw protobuf bytes.
#[derive(Clone, PartialEq, Message)]
struct CompatSignalRequest {
    #[prost(message, optional, tag = "22")]
    store_data_blob_request: Option<CompatStoreDataBlobRequest>,
    #[prost(message, optional, tag = "23")]
    get_data_blob_request: Option<CompatGetDataBlobRequest>,
}

#[derive(Clone, PartialEq, Message)]
struct CompatSignalResponse {
    #[prost(message, optional, tag = "30")]
    store_data_blob_response: Option<CompatStoreDataBlobResponse>,
    #[prost(message, optional, tag = "31")]
    get_data_blob_response: Option<CompatGetDataBlobResponse>,
}

#[derive(Clone, PartialEq, Message)]
struct CompatStoreDataBlobRequest {
    #[prost(uint32, tag = "1")]
    request_id: u32,
    #[prost(message, optional, tag = "2")]
    blob: Option<CompatDataBlob>,
}

#[derive(Clone, PartialEq, Message)]
struct CompatStoreDataBlobResponse {
    #[prost(uint32, tag = "1")]
    request_id: u32,
    #[prost(message, optional, tag = "2")]
    key: Option<CompatDataBlobKey>,
}

#[derive(Clone, PartialEq, Message)]
struct CompatGetDataBlobRequest {
    #[prost(uint32, tag = "1")]
    request_id: u32,
    #[prost(string, tag = "2")]
    participant_identity: String,
    #[prost(message, optional, tag = "3")]
    key: Option<CompatDataBlobKey>,
}

#[derive(Clone, PartialEq, Message)]
struct CompatGetDataBlobResponse {
    #[prost(uint32, tag = "1")]
    request_id: u32,
    #[prost(message, optional, tag = "2")]
    blob: Option<CompatDataBlob>,
}

#[derive(Clone, PartialEq, Message)]
struct CompatDataBlob {
    #[prost(message, optional, tag = "1")]
    key: Option<CompatDataBlobKey>,
    #[prost(bytes = "vec", tag = "2")]
    contents: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CompatDataBlobKey {
    #[prost(oneof = "compat_data_blob_key::Key", tags = "1")]
    key: Option<compat_data_blob_key::Key>,
}

mod compat_data_blob_key {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub(super) enum Key {
        #[prost(string, tag = "1")]
        Generic(String),
    }
}

fn request_response_bytes(request_id: u32, reason: i32, message: impl Into<String>) -> Vec<u8> {
    proto::SignalResponse {
        message: Some(proto::signal_response::Message::RequestResponse(
            proto::RequestResponse {
                request_id,
                reason,
                message: message.into(),
                ..Default::default()
            },
        )),
    }
    .encode_to_vec()
}

fn data_blob_key_bytes(key: &CompatDataBlobKey) -> Vec<u8> {
    key.encode_to_vec()
}

fn data_blob_key_generic_len(key: &CompatDataBlobKey) -> usize {
    match key.key.as_ref() {
        Some(compat_data_blob_key::Key::Generic(value)) => value.len(),
        None => 0,
    }
}

pub(crate) fn raw_data_blob_response_bytes(
    request_bytes: &[u8],
    state: &SignalState,
    room_name: &str,
    identity: &str,
) -> Option<Vec<u8>> {
    let request = CompatSignalRequest::decode(request_bytes).ok()?;
    if let Some(request) = request.store_data_blob_request {
        return Some(handle_store_data_blob_request(
            request, state, room_name, identity,
        ));
    }
    request
        .get_data_blob_request
        .map(|request| handle_get_data_blob_request(request, state, room_name))
}

fn handle_store_data_blob_request(
    request: CompatStoreDataBlobRequest,
    state: &SignalState,
    room_name: &str,
    identity: &str,
) -> Vec<u8> {
    if !state.participant_data_blob_enabled() {
        return request_response_bytes(
            request.request_id,
            proto::request_response::Reason::NotAllowed as i32,
            "participant data blob is not enabled",
        );
    }

    let Some(blob) = request.blob else {
        return request_response_bytes(
            request.request_id,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST,
            "data blob is required",
        );
    };
    let Some(key) = blob.key.clone() else {
        return request_response_bytes(
            request.request_id,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST,
            "data blob key is required",
        );
    };
    if key.key.is_none() || key.encode_to_vec().is_empty() {
        return request_response_bytes(
            request.request_id,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST,
            "data blob key is required",
        );
    }
    if state.participant_data_blob_max_key_length() > 0
        && data_blob_key_generic_len(&key) > state.participant_data_blob_max_key_length()
    {
        return request_response_bytes(
            request.request_id,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST,
            "data blob key exceeds maximum length",
        );
    }
    if blob.contents.is_empty() {
        return request_response_bytes(
            request.request_id,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST,
            "data blob is empty",
        );
    }

    if blob.contents.len() > MAX_PARTICIPANT_DATA_BLOB_BYTES {
        return request_response_bytes(
            request.request_id,
            proto::request_response::Reason::LimitExceeded as i32,
            "data blob exceeds maximum size",
        );
    }

    state.store_participant_data_blob(
        room_name,
        identity,
        data_blob_key_bytes(&key),
        blob.encode_to_vec(),
    );

    CompatSignalResponse {
        store_data_blob_response: Some(CompatStoreDataBlobResponse {
            request_id: request.request_id,
            key: Some(key),
        }),
        get_data_blob_response: None,
    }
    .encode_to_vec()
}

fn handle_get_data_blob_request(
    request: CompatGetDataBlobRequest,
    state: &SignalState,
    room_name: &str,
) -> Vec<u8> {
    if !state.participant_data_blob_enabled() {
        return request_response_bytes(
            request.request_id,
            proto::request_response::Reason::NotAllowed as i32,
            "participant data blob is not enabled",
        );
    }

    let Some(key) = request.key else {
        return request_response_bytes(
            request.request_id,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST,
            "data blob key is required",
        );
    };

    if state
        .rooms
        .get_participant(room_name, &request.participant_identity)
        .is_err()
    {
        return request_response_bytes(
            request.request_id,
            proto::request_response::Reason::NotFound as i32,
            "participant not found",
        );
    }

    let key_bytes = data_blob_key_bytes(&key);
    let Some(encoded_blob) =
        state.participant_data_blob(room_name, &request.participant_identity, &key_bytes)
    else {
        return request_response_bytes(
            request.request_id,
            proto::request_response::Reason::NotFound as i32,
            "data blob not found",
        );
    };

    let Ok(blob) = CompatDataBlob::decode(encoded_blob.as_slice()) else {
        return request_response_bytes(
            request.request_id,
            proto::request_response::Reason::UnclassifiedError as i32,
            "stored data blob could not be decoded",
        );
    };

    CompatSignalResponse {
        store_data_blob_response: None,
        get_data_blob_response: Some(CompatGetDataBlobResponse {
            request_id: request.request_id,
            blob: Some(blob),
        }),
    }
    .encode_to_vec()
}

async fn nudge_muted_track_subscriptions(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
) {
    let Ok(participants) = state.rooms.list_participants(room_name) else {
        return;
    };

    for publisher in participants {
        if publisher.identity == subscriber_identity {
            continue;
        }

        for track in publisher.tracks {
            if track.sid.is_empty() || !track.muted {
                continue;
            }

            for (key, forward_track) in
                state
                    .forward_tracks
                    .list_for_track(room_name, &publisher.identity, &track.sid)
            {
                let (_, _, _, key_subscriber_identity) = key;
                if key_subscriber_identity != subscriber_identity {
                    continue;
                }

                let Some(ssrc) = forward_track.primary_ssrc().await else {
                    continue;
                };

                let mime = track.mime_type.to_ascii_lowercase();
                let is_audio = track.r#type == proto::TrackType::Audio as i32;
                let payload_type = if is_audio {
                    if mime.contains("pcmu") {
                        0
                    } else if mime.contains("pcma") {
                        8
                    } else {
                        111
                    }
                } else if mime.contains("h264") {
                    102
                } else {
                    96
                };
                let mut sequence_number = 1_u16;
                let mut timestamp = 1_u32;
                let timestamp_step = if is_audio { 960_u32 } else { 3000_u32 };

                let mut write_failures = 0_u32;
                for _ in 0..250 {
                    let result = forward_track
                        .write_rtp(rtc::rtp::Packet {
                            header: rtc::rtp::header::Header {
                                version: 2,
                                marker: true,
                                payload_type,
                                sequence_number,
                                timestamp,
                                ssrc,
                                ..Default::default()
                            },
                            payload: vec![0].into(),
                        })
                        .await;
                    if result.is_err() {
                        write_failures += 1;
                    }
                    sequence_number = sequence_number.wrapping_add(1);
                    timestamp = timestamp.wrapping_add(timestamp_step);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }

                if write_failures > 0 {
                    tracing::debug!(
                        room = room_name,
                        publisher_identity = %publisher.identity,
                        subscriber_identity,
                        track_sid = %track.sid,
                        write_failures,
                        "muted_track_subscription_nudge_write_failures"
                    );
                }
            }
        }
    }
}

fn is_ignorable_stale_answer_transition_error(error_message: &str) -> bool {
    error_message.contains("invalid proposed signaling state transition")
        && error_message.contains("applying false answer")
}

fn update_metadata_response(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    request: proto::UpdateParticipantMetadata,
) -> proto::SignalResponse {
    let request_id = request.request_id;
    let request_for_response = request.clone();

    let Some(permission) = state
        .rooms
        .get_participant(room_name, identity)
        .ok()
        .and_then(|participant| participant.permission)
    else {
        return proto::SignalResponse {
            message: Some(proto::signal_response::Message::RequestResponse(
                proto::RequestResponse {
                    request_id,
                    reason: proto::request_response::Reason::NotFound as i32,
                    request: Some(proto::request_response::Request::UpdateMetadata(
                        request_for_response,
                    )),
                    ..Default::default()
                },
            )),
        };
    };

    if !permission.can_update_metadata {
        return proto::SignalResponse {
            message: Some(proto::signal_response::Message::RequestResponse(
                proto::RequestResponse {
                    request_id,
                    reason: proto::request_response::Reason::NotAllowed as i32,
                    message: "does not have permission to update own metadata".to_string(),
                    request: Some(proto::request_response::Request::UpdateMetadata(
                        request_for_response,
                    )),
                    ..Default::default()
                },
            )),
        };
    }

    match state.rooms.update_participant(
        room_name,
        identity,
        &request.metadata,
        &request.name,
        None,
        request.attributes,
    ) {
        Ok(participant) => {
            if let Some(refresh_token) = state.maybe_issue_refresh_token(room_name, &participant)
                && let Some(outbound_tx) = state.signal_connections.get(room_name, identity)
            {
                let _ = outbound_tx.send(proto::SignalResponse {
                    message: Some(proto::signal_response::Message::RefreshToken(refresh_token)),
                });
            }
            state.updates.broadcast_update(room_name, participant);
            proto::SignalResponse {
                message: Some(proto::signal_response::Message::RequestResponse(
                    proto::RequestResponse {
                        request_id,
                        request: Some(proto::request_response::Request::UpdateMetadata(
                            request_for_response,
                        )),
                        ..Default::default()
                    },
                )),
            }
        }
        Err(_) => proto::SignalResponse {
            message: Some(proto::signal_response::Message::RequestResponse(
                proto::RequestResponse {
                    request_id,
                    reason: proto::request_response::Reason::UnclassifiedError as i32,
                    request: Some(proto::request_response::Request::UpdateMetadata(
                        request_for_response,
                    )),
                    ..Default::default()
                },
            )),
        },
    }
}

pub(crate) async fn signal_response_for_request(
    request: proto::SignalRequest,
    state: &SignalState,
    room_name: &str,
    identity: &str,
    outbound_tx: &crate::router::OutboundSignalSender,
) -> SignalResult<Option<proto::SignalResponse>> {
    let Some(message) = request.message else {
        return Ok(None);
    };

    let response = match message {
        proto::signal_request::Message::Ping(_) => Some(proto::SignalResponse {
            message: Some(proto::signal_response::Message::Pong(current_unix_millis())),
        }),
        proto::signal_request::Message::PingReq(ping) => Some(proto::SignalResponse {
            message: Some(proto::signal_response::Message::PongResp(proto::Pong {
                last_ping_timestamp: ping.timestamp,
                timestamp: current_unix_millis(),
            })),
        }),
        proto::signal_request::Message::Offer(offer) => Some(
            crate::router::answer_publisher_offer(
                offer,
                state,
                room_name,
                identity,
                outbound_tx,
                &state.rtc_transport_config(),
            )
            .await
            .map_err(|err| SignalError::RequestHandling {
                message: err.to_string(),
            })?,
        ),
        proto::signal_request::Message::Answer(answer) => {
            let answer_id = answer.id;
            let answer_mid_to_track_id = crate::media::mid_to_track_id_from_answer_sdp(&answer.sdp);
            let answer_sdp_len = answer.sdp.len();
            tracing::debug!(
                room = room_name,
                identity,
                answer_id,
                answer_sdp_len,
                answer_mid_to_track_id = ?answer_mid_to_track_id,
                "signal_answer_received"
            );
            if let Some(peer_connection) = state.peer_connections.get(
                room_name,
                identity,
                crate::router::SignalConnectionTarget::Subscriber,
            ) {
                let expected_offer_id = state
                    .subscriber_offer_ids
                    .current_offer_id(room_name, identity);
                tracing::debug!(
                    room = room_name,
                    identity,
                    answer_id,
                    expected_offer_id = ?expected_offer_id,
                    answer_mid_to_track_id = ?answer_mid_to_track_id,
                    "subscriber_answer_offer_id_check"
                );
                if let Some(expected_offer_id) = expected_offer_id
                    && answer.id != 0
                    && answer.id != expected_offer_id
                {
                    tracing::warn!(
                        room = room_name,
                        identity,
                        expected_offer_id,
                        received_offer_id = answer.id,
                        answer_mid_to_track_id = ?answer_mid_to_track_id,
                        "ignoring_stale_or_mismatched_subscriber_answer_id"
                    );
                    return Ok(None);
                }

                let answer_sdp = answer.sdp.clone();
                if let Err(error) = peer_connection.set_remote_answer(answer.sdp).await {
                    let error_message = error.to_string();
                    if is_ignorable_stale_answer_transition_error(&error_message) {
                        tracing::warn!(
                            room = room_name,
                            identity,
                            answer_id,
                            answer_mid_to_track_id = ?answer_mid_to_track_id,
                            error = %error_message,
                            "ignoring_stale_subscriber_answer_in_stable_state"
                        );
                        return Ok(None);
                    }
                    return Err(SignalError::RequestHandling {
                        message: error_message,
                    });
                }

                tracing::debug!(
                    room = room_name,
                    identity,
                    answer_id,
                    answer_mid_to_track_id = ?answer_mid_to_track_id,
                    "subscriber_answer_set_remote_answer_ok_activating_forward_tracks"
                );
                let supported_video_mime_types =
                    crate::media::receive_supported_video_mime_types_from_offer(&answer_sdp);
                state.merge_participant_subscribe_video_mime_types(
                    room_name,
                    identity,
                    &supported_video_mime_types,
                );
                crate::router::reject_unaccepted_video_tracks_from_subscriber_answer(
                    state,
                    room_name,
                    identity,
                    expected_offer_id.unwrap_or(answer_id),
                    &answer_sdp,
                )
                .await;

                let mut offered_track_sids = state
                    .subscriber_offer_mid_track_ids(
                        room_name,
                        identity,
                        expected_offer_id.unwrap_or(answer_id),
                    )
                    .into_values()
                    .collect::<std::collections::HashSet<_>>();
                if offered_track_sids.is_empty() {
                    offered_track_sids = state
                        .forward_tracks
                        .subscriber_track_sids_for_forwarding_mids(
                            room_name,
                            identity,
                            &crate::media::accepted_media_mids_from_answer_sdp(&answer_sdp),
                        );
                }
                crate::router::session::activate_tracks_with_compatible_bind_results(
                    state,
                    room_name,
                    identity,
                    &offered_track_sids,
                )
                .await;

                crate::router::ensure_existing_media_forwarding_for_subscriber(
                    state, room_name, identity,
                )
                .await;

                nudge_muted_track_subscriptions(state, room_name, identity).await;

                let completed_offer_id = expected_offer_id.unwrap_or(answer_id);
                if state.subscriber_offer_negotiations.finish_answer(
                    room_name,
                    identity,
                    completed_offer_id,
                ) {
                    crate::router::session::signal_media_forwarding_negotiation_with_offer_id(
                        state,
                        &state.subscriber_offer_ids,
                        room_name,
                        identity,
                        &peer_connection,
                        crate::router::MediaForwardingConnectionKind::DualPcSubscriber,
                        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
                        outbound_tx,
                    )
                    .await
                    .map_err(|err| SignalError::RequestHandling {
                        message: err.to_string(),
                    })?;
                }
            } else if let Some(peer_connection) = state.peer_connections.get(
                room_name,
                identity,
                crate::router::SignalConnectionTarget::Publisher,
            ) {
                tracing::debug!(
                    room = room_name,
                    identity,
                    answer_id,
                    answer_mid_to_track_id = ?answer_mid_to_track_id,
                    "publisher_answer_set_remote_answer_start"
                );
                if let Err(error) = peer_connection.set_remote_answer(answer.sdp).await {
                    let error_message = error.to_string();
                    if is_ignorable_stale_answer_transition_error(&error_message) {
                        tracing::warn!(
                            room = room_name,
                            identity,
                            answer_id,
                            answer_mid_to_track_id = ?answer_mid_to_track_id,
                            error = %error_message,
                            "ignoring_stale_publisher_answer_in_stable_state"
                        );
                        return Ok(None);
                    }
                    return Err(SignalError::RequestHandling {
                        message: error_message,
                    });
                }
                tracing::debug!(
                    room = room_name,
                    identity,
                    answer_id,
                    answer_mid_to_track_id = ?answer_mid_to_track_id,
                    "publisher_answer_set_remote_answer_ok"
                );
            } else {
                tracing::warn!(
                    room = room_name,
                    identity,
                    answer_id,
                    answer_mid_to_track_id = ?answer_mid_to_track_id,
                    "signal_answer_no_matching_peer_connection"
                );
            }
            None
        }
        proto::signal_request::Message::Trickle(trickle) => {
            let target = crate::router::SignalConnectionTarget::from_signal_target(trickle.target);
            tracing::debug!(
                room = room_name,
                identity,
                target = ?target,
                is_final = trickle.r#final,
                candidate_len = trickle.candidate_init.len(),
                "signal_trickle_received"
            );
            if let Some(peer_connection) = state.peer_connections.get(room_name, identity, target)
                && let Err(error) = peer_connection
                    .add_ice_candidate_json(&trickle.candidate_init)
                    .await
            {
                tracing::warn!(
                    room = room_name,
                    identity,
                    target = ?target,
                    error = %error,
                    "failed_to_add_remote_ice_candidate_ignoring"
                );
            }
            None
        }
        proto::signal_request::Message::AddTrack(request) => {
            Some(crate::router::add_track_response(state, room_name, identity, request).await)
        }
        proto::signal_request::Message::Mute(request) => {
            if state
                .rooms
                .set_participant_track_muted(room_name, identity, &request.sid, request.muted)
                .is_ok()
                && let Ok(participant) = state.rooms.get_participant(room_name, identity)
            {
                state.updates.broadcast_update(room_name, participant);
            }
            None
        }
        proto::signal_request::Message::Subscription(request) => {
            crate::router::handle_media_subscription_request(
                state, room_name, identity, request, false,
            )
            .await;
            None
        }
        proto::signal_request::Message::UpdateMetadata(request) => Some(update_metadata_response(
            state, room_name, identity, request,
        )),
        proto::signal_request::Message::TrackSetting(request) => {
            state
                .track_settings
                .upsert_from_request(room_name, identity, &request);

            for track_sid in &request.track_sids {
                let Some((publisher_identity, track)) =
                    crate::media::find_media_track_publisher(state, room_name, track_sid)
                else {
                    continue;
                };
                crate::router::session::emit_aggregate_subscribed_quality_update_for_track(
                    state,
                    room_name,
                    &publisher_identity,
                    &track,
                );
            }
            None
        }
        proto::signal_request::Message::Simulate(simulate) => {
            if let Some(scenario) = simulate.scenario {
                match scenario {
                    proto::simulate_scenario::Scenario::NodeFailure(true)
                    | proto::simulate_scenario::Scenario::LeaveRequestFullReconnect(true) => {
                        return Ok(Some(proto::SignalResponse {
                            message: Some(proto::signal_response::Message::Leave(
                                proto::LeaveRequest {
                                    action: proto::leave_request::Action::Reconnect as i32,
                                    reason: proto::DisconnectReason::UnknownReason as i32,
                                    ..Default::default()
                                },
                            )),
                        }));
                    }
                    proto::simulate_scenario::Scenario::ServerLeave(true) => {
                        return Ok(Some(proto::SignalResponse {
                            message: Some(proto::signal_response::Message::Leave(
                                proto::LeaveRequest {
                                    action: proto::leave_request::Action::Disconnect as i32,
                                    reason: proto::DisconnectReason::UnknownReason as i32,
                                    ..Default::default()
                                },
                            )),
                        }));
                    }
                    proto::simulate_scenario::Scenario::SwitchCandidateProtocol(protocol) => {
                        state.set_candidate_protocol_preference(room_name, identity, protocol);
                        return Ok(Some(proto::SignalResponse {
                            message: Some(proto::signal_response::Message::Leave(
                                proto::LeaveRequest {
                                    action: proto::leave_request::Action::Reconnect as i32,
                                    reason: proto::DisconnectReason::ClientInitiated as i32,
                                    ..Default::default()
                                },
                            )),
                        }));
                    }
                    _ => {}
                }
            }
            None
        }
        proto::signal_request::Message::Leave(leave) => {
            tracing::info!(
                room = room_name,
                identity,
                reason = leave.reason,
                action = leave.action,
                "signal_leave_request_received"
            );
            crate::router::cleanup_participant_runtime_state(state, room_name, identity, true)
                .await;
            return Err(SignalError::ParticipantLeft);
        }
        proto::signal_request::Message::PublishDataTrackRequest(request) => Some(
            crate::router::publish_data_track_response(state, room_name, identity, request),
        ),
        proto::signal_request::Message::UnpublishDataTrackRequest(request) => Some(
            crate::router::unpublish_data_track_response(state, room_name, identity, request),
        ),
        proto::signal_request::Message::UpdateDataSubscription(request) => Some(
            crate::router::update_data_subscription_response(state, room_name, identity, request),
        ),
        _ => None,
    };

    Ok(response)
}

#[allow(dead_code)]
pub(crate) fn _data_track_error_reason(
    error: DataTrackPublishError,
) -> proto::request_response::Reason {
    match error {
        DataTrackPublishError::DuplicateHandle => proto::request_response::Reason::DuplicateHandle,
        DataTrackPublishError::DuplicateName => proto::request_response::Reason::DuplicateName,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use livekit_protocol as proto;
    use oxidesfu_auth::{ApiKeyStore, AuthContext, Claims, TokenVerifier, VideoGrants};
    use oxidesfu_room::RoomStore;
    use prost::Message;

    use super::{
        CompatDataBlob, CompatDataBlobKey, CompatGetDataBlobRequest, CompatSignalRequest,
        CompatSignalResponse, CompatStoreDataBlobRequest, REQUEST_RESPONSE_REASON_INVALID_REQUEST,
        compat_data_blob_key, is_ignorable_stale_answer_transition_error,
        raw_data_blob_response_bytes, update_metadata_response,
    };
    use crate::state::SignalState;

    const API_KEY: &str = "devkey";
    const API_SECRET: &str = "secret";

    fn state() -> SignalState {
        let mut keys = ApiKeyStore::new();
        keys.insert(API_KEY, API_SECRET);
        SignalState::new(RoomStore::default(), TokenVerifier::new(keys))
    }

    fn auth_for(room: &str, identity: &str, name: &str) -> AuthContext {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be after unix epoch")
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
                can_update_own_metadata: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(API_SECRET.as_bytes()),
        )
        .expect("test token should encode");
        TokenVerifier::new({
            let mut keys = ApiKeyStore::new();
            keys.insert(API_KEY, API_SECRET);
            keys
        })
        .verify_token(&token)
        .expect("token should verify")
    }

    #[test]
    fn classifies_stale_answer_state_transition_error_as_ignorable() {
        let message =
            "invalid proposed signaling state transition: from stable applying false answer";
        assert!(is_ignorable_stale_answer_transition_error(message));
    }

    #[test]
    fn does_not_classify_other_errors_as_ignorable_stale_answer() {
        let message = "failed to parse remote session description";
        assert!(!is_ignorable_stale_answer_transition_error(message));
    }

    fn generic_blob_key(value: &str) -> CompatDataBlobKey {
        CompatDataBlobKey {
            key: Some(compat_data_blob_key::Key::Generic(value.to_string())),
        }
    }

    fn decode_request_response(bytes: &[u8]) -> proto::RequestResponse {
        let signal = proto::SignalResponse::decode(bytes).expect("signal response should decode");
        let Some(proto::signal_response::Message::RequestResponse(request_response)) =
            signal.message
        else {
            panic!("expected request-response signal message");
        };
        request_response
    }

    fn decode_compat_response(bytes: &[u8]) -> CompatSignalResponse {
        CompatSignalResponse::decode(bytes).expect("compat response should decode")
    }

    fn build_get_blob_request(
        request_id: u32,
        participant_identity: &str,
        key: Option<CompatDataBlobKey>,
    ) -> Vec<u8> {
        CompatSignalRequest {
            store_data_blob_request: None,
            get_data_blob_request: Some(CompatGetDataBlobRequest {
                request_id,
                key,
                participant_identity: participant_identity.to_string(),
            }),
        }
        .encode_to_vec()
    }

    fn build_store_blob_request(
        request_id: u32,
        key: Option<CompatDataBlobKey>,
        contents: Vec<u8>,
    ) -> Vec<u8> {
        CompatSignalRequest {
            store_data_blob_request: Some(CompatStoreDataBlobRequest {
                request_id,
                blob: Some(CompatDataBlob { key, contents }),
            }),
            get_data_blob_request: None,
        }
        .encode_to_vec()
    }

    #[test]
    fn handle_get_data_blob_request_requires_key_and_accepts_present_key() {
        let state = state().with_participant_data_blob_enabled(true);
        let room = "room-data-blob";
        let publisher = "publisher-a";

        let missing_key_response = raw_data_blob_response_bytes(
            &build_get_blob_request(7, publisher, None),
            &state,
            room,
            "subscriber-a",
        )
        .expect("compat get-data-blob request should be handled");
        let request_response = decode_request_response(missing_key_response.as_slice());
        assert_eq!(
            request_response.reason,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST
        );

        let present_key_response = raw_data_blob_response_bytes(
            &build_get_blob_request(8, publisher, Some(generic_blob_key("blob-1"))),
            &state,
            room,
            "subscriber-a",
        )
        .expect("compat get-data-blob request should be handled");
        let request_response = decode_request_response(present_key_response.as_slice());
        assert_eq!(
            request_response.reason,
            proto::request_response::Reason::NotFound as i32
        );
    }

    #[test]
    fn handle_store_data_blob_request_covers_core_branches() {
        let room = "room-data-blob-store";
        let identity = "publisher-a";

        let disabled_state = state().with_participant_data_blob_enabled(false);
        let disabled_response = raw_data_blob_response_bytes(
            &build_store_blob_request(10, Some(generic_blob_key("k")), b"v".to_vec()),
            &disabled_state,
            room,
            identity,
        )
        .expect("compat store request should be handled");
        let request_response = decode_request_response(disabled_response.as_slice());
        assert_eq!(
            request_response.reason,
            proto::request_response::Reason::NotAllowed as i32
        );

        let enabled_state = state().with_participant_data_blob_enabled(true);

        let missing_key_response = raw_data_blob_response_bytes(
            &build_store_blob_request(11, None, b"v".to_vec()),
            &enabled_state,
            room,
            identity,
        )
        .expect("compat store request should be handled");
        let request_response = decode_request_response(missing_key_response.as_slice());
        assert_eq!(
            request_response.reason,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST
        );

        let max_key_len_state = state()
            .with_participant_data_blob_enabled(true)
            .with_participant_data_blob_max_key_length(5);
        let over_limit_key_response = raw_data_blob_response_bytes(
            &build_store_blob_request(12, Some(generic_blob_key("123456")), b"v".to_vec()),
            &max_key_len_state,
            room,
            identity,
        )
        .expect("compat store request should be handled");
        let request_response = decode_request_response(over_limit_key_response.as_slice());
        assert_eq!(
            request_response.reason,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST
        );

        let zero_limit_disabled_state = state()
            .with_participant_data_blob_enabled(true)
            .with_participant_data_blob_max_key_length(0);
        let zero_limit_success_response = raw_data_blob_response_bytes(
            &build_store_blob_request(121, Some(generic_blob_key("123456")), b"v".to_vec()),
            &zero_limit_disabled_state,
            room,
            identity,
        )
        .expect("compat store request should be handled");
        let compat = decode_compat_response(zero_limit_success_response.as_slice());
        let store = compat
            .store_data_blob_response
            .expect("store response should be present");
        assert_eq!(store.request_id, 121);
        assert_eq!(store.key, Some(generic_blob_key("123456")));

        let empty_contents_response = raw_data_blob_response_bytes(
            &build_store_blob_request(122, Some(generic_blob_key("k")), Vec::new()),
            &enabled_state,
            room,
            identity,
        )
        .expect("compat store request should be handled");
        let request_response = decode_request_response(empty_contents_response.as_slice());
        assert_eq!(
            request_response.reason,
            REQUEST_RESPONSE_REASON_INVALID_REQUEST
        );

        let too_large_response = raw_data_blob_response_bytes(
            &build_store_blob_request(
                13,
                Some(generic_blob_key("k")),
                vec![0; super::MAX_PARTICIPANT_DATA_BLOB_BYTES + 1],
            ),
            &enabled_state,
            room,
            identity,
        )
        .expect("compat store request should be handled");
        let request_response = decode_request_response(too_large_response.as_slice());
        assert_eq!(
            request_response.reason,
            proto::request_response::Reason::LimitExceeded as i32
        );

        let success_response = raw_data_blob_response_bytes(
            &build_store_blob_request(14, Some(generic_blob_key("k")), b"definition".to_vec()),
            &enabled_state,
            room,
            identity,
        )
        .expect("compat store request should be handled");
        let compat = decode_compat_response(success_response.as_slice());
        let store = compat
            .store_data_blob_response
            .expect("store response should be present");
        assert_eq!(store.request_id, 14);
        assert_eq!(store.key, Some(generic_blob_key("k")));
    }

    #[test]
    fn process_get_data_blob_request_branches_match_upstream_behavior() {
        let state = state().with_participant_data_blob_enabled(true);
        let room = "room-data-blob-process";
        let subscriber = "subscriber-a";
        let publisher = "publisher-a";

        state
            .rooms
            .join_participant(room, publisher, "Publisher", String::new(), HashMap::new())
            .expect("publisher should join");

        let participant_not_found_response = raw_data_blob_response_bytes(
            &build_get_blob_request(20, "missing-publisher", Some(generic_blob_key("k"))),
            &state,
            room,
            subscriber,
        )
        .expect("compat get request should be handled");
        let request_response = decode_request_response(participant_not_found_response.as_slice());
        assert_eq!(
            request_response.reason,
            proto::request_response::Reason::NotFound as i32
        );

        let blob_not_found_response = raw_data_blob_response_bytes(
            &build_get_blob_request(21, publisher, Some(generic_blob_key("k"))),
            &state,
            room,
            subscriber,
        )
        .expect("compat get request should be handled");
        let request_response = decode_request_response(blob_not_found_response.as_slice());
        assert_eq!(
            request_response.reason,
            proto::request_response::Reason::NotFound as i32
        );

        state.store_participant_data_blob(
            room,
            publisher,
            generic_blob_key("k").encode_to_vec(),
            CompatDataBlob {
                key: Some(generic_blob_key("k")),
                contents: b"definition".to_vec(),
            }
            .encode_to_vec(),
        );

        let success_response = raw_data_blob_response_bytes(
            &build_get_blob_request(22, publisher, Some(generic_blob_key("k"))),
            &state,
            room,
            subscriber,
        )
        .expect("compat get request should be handled");
        let compat = decode_compat_response(success_response.as_slice());
        let get = compat
            .get_data_blob_response
            .expect("get response should be present");
        assert_eq!(get.request_id, 22);
        let blob = get.blob.expect("blob should be present");
        assert_eq!(blob.key, Some(generic_blob_key("k")));
        assert_eq!(blob.contents, b"definition".to_vec());
    }

    #[tokio::test]
    async fn update_metadata_request_updates_attributes_when_participant_can_update_metadata() {
        let state = state();
        let room = "room-a";
        let identity = "alice";
        state
            .rooms
            .join_participant_with_permission(
                room,
                identity,
                "Alice",
                String::new(),
                HashMap::from([("mykey".to_string(), "au2".to_string())]),
                Some(proto::ParticipantPermission {
                    can_update_metadata: true,
                    can_publish: true,
                    can_publish_data: true,
                    can_subscribe: true,
                    ..Default::default()
                }),
            )
            .expect("participant should join");
        state.remember_participant_auth_context(room, identity, &auth_for(room, identity, "Alice"));

        let mut updates = state.updates.register(room);

        let response = update_metadata_response(
            &state,
            room,
            identity,
            proto::UpdateParticipantMetadata {
                request_id: 7,
                attributes: HashMap::from([("secondkey".to_string(), "au2".to_string())]),
                ..Default::default()
            },
        );

        let Some(proto::signal_response::Message::RequestResponse(request_response)) =
            response.message
        else {
            panic!("expected request response");
        };
        assert_eq!(request_response.request_id, 7);
        assert_eq!(request_response.reason, 0);

        let update = tokio::time::timeout(Duration::from_secs(1), updates.recv())
            .await
            .expect("update should be broadcast before timeout")
            .expect("update channel should produce a value");
        let Some(proto::signal_response::Message::Update(update)) = update.message else {
            panic!("expected participant update message");
        };
        let participant = update
            .participants
            .into_iter()
            .find(|participant| participant.identity == identity)
            .expect("participant update should include alice");
        assert_eq!(
            participant.attributes.get("mykey"),
            Some(&"au2".to_string())
        );
        assert_eq!(
            participant.attributes.get("secondkey"),
            Some(&"au2".to_string())
        );
    }

    #[tokio::test]
    async fn update_metadata_request_is_rejected_without_metadata_permission() {
        let state = state();
        let room = "room-b";
        let identity = "bob";
        state
            .rooms
            .join_participant_with_permission(
                room,
                identity,
                "Bob",
                String::new(),
                HashMap::new(),
                Some(proto::ParticipantPermission {
                    can_update_metadata: false,
                    can_publish: true,
                    can_publish_data: true,
                    can_subscribe: true,
                    ..Default::default()
                }),
            )
            .expect("participant should join");

        let response = update_metadata_response(
            &state,
            room,
            identity,
            proto::UpdateParticipantMetadata {
                request_id: 11,
                attributes: HashMap::from([("denied".to_string(), "true".to_string())]),
                ..Default::default()
            },
        );

        let Some(proto::signal_response::Message::RequestResponse(request_response)) =
            response.message
        else {
            panic!("expected request response");
        };
        assert_eq!(request_response.request_id, 11);
        assert_eq!(
            request_response.reason,
            proto::request_response::Reason::NotAllowed as i32
        );

        let participant = state
            .rooms
            .get_participant(room, identity)
            .expect("participant should still exist");
        assert!(!participant.attributes.contains_key("denied"));
    }
}

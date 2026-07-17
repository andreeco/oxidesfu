use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};

use super::*;
use crate::media::{
    DependencyDescriptorDti, DependencyDescriptorForwardingDecision,
    DependencyDescriptorForwardingSelector, DependencyDescriptorFrame,
    DependencyDescriptorLayerPolicy, DependencyDescriptorTargetLayer, ForwardTrackKey,
    LayerAcquisitionState, LayerPacketMetadata, LayerPolicy, SpatialLayer,
    SubscriberVideoLayerSelector, VideoIngressDecision, VideoSourceKind,
};
use forwarding_snapshot::{
    DownstreamFeedbackSnapshot, ForwardingResultSnapshot, ForwardingSnapshot,
    ForwardingTargetSnapshot, RtpWindowSnapshot, SelectorPliSnapshot, SpatialSelectionSnapshot,
    TemporalSelectionSnapshot,
};

#[path = "../media/forwarding_snapshot.rs"]
mod forwarding_snapshot;

type PublisherSubscriptionActiveKey = (String, String, String);
type PublisherSubscriptionActivePairs =
    std::sync::Arc<std::sync::Mutex<HashSet<PublisherSubscriptionActiveKey>>>;

static NEXT_TRACK_SID_COUNTER: AtomicU64 = AtomicU64::new(0);
static NEXT_FORWARDING_SNAPSHOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const RTCP_EFFECTS_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

const REMOTE_TRACK_EVENT_IDLE_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

fn next_track_sid() -> String {
    let millis = current_unix_millis().max(0) as u64;
    let counter = NEXT_TRACK_SID_COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFFF;
    let composite = (millis << 16) | counter;
    format!("TR_{composite:016x}")
}

#[allow(deprecated)]
pub(crate) async fn add_track_response(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    request: proto::AddTrackRequest,
) -> proto::SignalResponse {
    if !state
        .publish_permissions
        .can_publish_media(room_name, identity)
    {
        tracing::debug!(
            room = room_name,
            identity,
            "rejecting_add_track_without_can_publish_media"
        );
        return proto::SignalResponse {
            message: Some(proto::signal_response::Message::RequestResponse(
                proto::RequestResponse {
                    reason: proto::request_response::Reason::NotAllowed as i32,
                    request: Some(proto::request_response::Request::AddTrack(request)),
                    ..Default::default()
                },
            )),
        };
    }

    let source = proto::TrackSource::try_from(request.source)
        .unwrap_or(proto::TrackSource::Unknown)
        .as_str_name()
        .to_ascii_lowercase();
    if !state
        .publish_permissions
        .can_publish_source(room_name, identity, &source)
    {
        tracing::debug!(
            room = room_name,
            identity,
            source,
            "rejecting_add_track_source_not_allowed"
        );
        return proto::SignalResponse {
            message: Some(proto::signal_response::Message::RequestResponse(
                proto::RequestResponse {
                    reason: proto::request_response::Reason::NotAllowed as i32,
                    request: Some(proto::request_response::Request::AddTrack(request)),
                    ..Default::default()
                },
            )),
        };
    }

    let simulcast_layers = request.layers.clone();
    let mut simulcast_codecs = if request.simulcast_codecs.is_empty() {
        vec![proto::SimulcastCodecInfo {
            mime_type: default_track_mime_type(request.r#type).to_string(),
            mid: String::new(),
            cid: request.cid.clone(),
            layers: simulcast_layers.clone(),
            video_layer_mode: proto::video_layer::Mode::OneSpatialLayerPerStream as i32,
            sdp_cid: String::new(),
        }]
    } else {
        request
            .simulcast_codecs
            .iter()
            .map(|codec| proto::SimulcastCodecInfo {
                mime_type: normalize_track_mime_type(request.r#type, &codec.codec),
                mid: String::new(),
                cid: codec.cid.clone(),
                layers: codec.layers.clone(),
                video_layer_mode: codec.video_layer_mode,
                sdp_cid: String::new(),
            })
            .collect::<Vec<_>>()
    };

    let disabled_publish_codecs =
        disabled_publish_codecs_for_participant(state, room_name, identity);
    if !disabled_publish_codecs.is_empty() {
        simulcast_codecs.retain(|codec| {
            !disabled_publish_codecs.contains(&codec.mime_type.trim().to_ascii_lowercase())
        });
    }

    if simulcast_codecs.is_empty() {
        let fallback_cid = request
            .simulcast_codecs
            .first()
            .map(|codec| codec.cid.clone())
            .filter(|cid| !cid.is_empty())
            .unwrap_or_else(|| request.cid.clone());
        simulcast_codecs.push(proto::SimulcastCodecInfo {
            mime_type: default_track_mime_type(request.r#type).to_string(),
            mid: String::new(),
            cid: fallback_cid,
            layers: simulcast_layers.clone(),
            video_layer_mode: proto::video_layer::Mode::OneSpatialLayerPerStream as i32,
            sdp_cid: String::new(),
        });
    }

    let mime_type = simulcast_codecs
        .first()
        .map(|codec| codec.mime_type.clone())
        .unwrap_or_else(|| default_track_mime_type(request.r#type).to_string());

    let track = proto::TrackInfo {
        sid: next_track_sid(),
        r#type: request.r#type,
        name: request.name,
        muted: request.muted,
        width: request.width,
        height: request.height,
        source: request.source,
        mime_type,
        simulcast: !simulcast_layers.is_empty() || request.simulcast_codecs.len() > 1,
        layers: simulcast_layers,
        codecs: simulcast_codecs,

        disable_red: request.disable_red,
        encryption: request.encryption,
        stream: request.stream,
        audio_features: request.audio_features,
        backup_codec_policy: request.backup_codec_policy,
        packet_trailer_features: request.packet_trailer_features,
        ..Default::default()
    };

    // Keep the client-provided `cid` opaque and stable: the upstream LiveKit
    // conformance clients commonly use literal track IDs like "audio"/"video"
    // as CIDs for AddTrack correlation. Normalizing or re-keying here can break
    // pending-track matching during reconnect/publish churn.
    state
        .media_track_cids
        .insert(room_name, identity, &request.cid, &track.sid);

    if let Ok(participant) = state
        .rooms
        .add_participant_track(room_name, identity, track.clone())
    {
        tracing::info!(
            room = %room_name,
            identity = %identity,
            track_sid = %track.sid,
            track_name = %track.name,
            track_mime_type = %track.mime_type,
            track_count = participant.tracks.len(),
            "publisher_track_registered"
        );
        if let Some(room) = state
            .rooms
            .list_rooms(&[room_name.to_string()])
            .ok()
            .and_then(|mut rooms| rooms.pop())
        {
            state.emit_webhook_event(proto::WebhookEvent {
                event: "track_published".to_string(),
                room: Some(super::reduced_room_for_track_event(&room)),
                participant: Some(super::reduced_participant_for_track_event(&participant)),
                track: Some(track.clone()),
                id: super::next_webhook_event_id(),
                created_at: super::webhook_created_at_unix_seconds(),
                ..Default::default()
            });
        }
        state.updates.broadcast_update(room_name, participant);
    }

    let _ = ensure_subscriber_forwarding_for_track(state, room_name, identity, &track).await;
    retry_pending_remote_tracks_after_track_published(state, room_name, identity);

    proto::SignalResponse {
        message: Some(proto::signal_response::Message::TrackPublished(
            proto::TrackPublishedResponse {
                cid: request.cid,
                track: Some(track),
            },
        )),
    }
}

fn retry_pending_remote_tracks_after_track_published(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
) {
    let pending_remote_tracks = state
        .pending_remote_tracks
        .take_for_publisher(room_name, publisher_identity);
    if pending_remote_tracks.is_empty() {
        return;
    }

    tracing::info!(
        room = room_name,
        publisher_identity,
        count = pending_remote_tracks.len(),
        "retrying_pending_remote_tracks_after_track_published"
    );

    for pending_remote_track in pending_remote_tracks {
        let state = state.clone();
        let event = PublisherRemoteTrackEvent {
            remote_track: pending_remote_track.remote_track,
            remote_mid: pending_remote_track.remote_mid,
            room_name: room_name.to_string(),
            publisher_identity: publisher_identity.to_string(),
            publisher_sid: pending_remote_track.publisher_sid,
        };

        tokio::spawn(async move {
            let _ = forward_publisher_remote_track(state, event).await;
        });
    }
}

fn disabled_publish_codecs_for_participant(
    state: &SignalState,
    room_name: &str,
    identity: &str,
) -> HashSet<String> {
    let mut disabled = HashSet::new();

    let Some(client_info) = state.participant_client_info(room_name, identity) else {
        return disabled;
    };

    if participant_should_disable_h264_publish(&client_info) {
        disabled.insert("video/h264".to_string());
    }

    disabled
}

fn normalize_track_mime_type(track_type: i32, codec: &str) -> String {
    let codec = codec.trim().to_ascii_lowercase();
    if codec.contains('/') || codec.is_empty() {
        return codec;
    }

    if track_type == proto::TrackType::Audio as i32 {
        return format!("audio/{codec}");
    }
    if track_type == proto::TrackType::Video as i32 {
        return format!("video/{codec}");
    }

    codec
}

fn default_track_mime_type(track_type: i32) -> &'static str {
    if track_type == proto::TrackType::Video as i32 {
        "video/vp8"
    } else {
        "audio/opus"
    }
}

fn pending_media_section_kind_from_track_kind(
    track_kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
) -> crate::stores::PendingMediaSectionKind {
    if track_kind == rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video {
        crate::stores::PendingMediaSectionKind::Video
    } else {
        crate::stores::PendingMediaSectionKind::Audio
    }
}

fn advertised_track_mime_types(track: &proto::TrackInfo) -> Vec<String> {
    let mut mime_types = track
        .codecs
        .iter()
        .map(|codec| codec.mime_type.trim().to_ascii_lowercase())
        .filter(|mime| !mime.is_empty())
        .collect::<Vec<_>>();

    if mime_types.is_empty() {
        let fallback = track.mime_type.trim().to_ascii_lowercase();
        if !fallback.is_empty() {
            mime_types.push(fallback);
        }
    }

    if mime_types.is_empty() {
        mime_types.push(default_track_mime_type(track.r#type).to_string());
    }

    mime_types
}

fn selected_forwarding_mime_type_for_subscriber(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    track: &proto::TrackInfo,
    existing_forwarding_count: usize,
) -> Option<String> {
    let mime_types = advertised_track_mime_types(track);

    if track.r#type != proto::TrackType::Video as i32 {
        return mime_types
            .get(existing_forwarding_count.min(mime_types.len().saturating_sub(1)))
            .cloned();
    }

    mime_types
        .iter()
        .find(|mime_type| {
            state.participant_supports_video_mime_type(room_name, subscriber_identity, mime_type)
        })
        .cloned()
        .or_else(|| mime_types.first().cloned())
}

fn subscriber_supports_any_track_mime_type(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    track: &proto::TrackInfo,
) -> bool {
    if track.r#type != proto::TrackType::Video as i32 {
        return true;
    }

    advertised_track_mime_types(track)
        .into_iter()
        .any(|mime_type| {
            state.participant_supports_video_mime_type(room_name, subscriber_identity, &mime_type)
        })
}

fn emit_subscription_unsupported_codec_response(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    track_sid: &str,
) {
    let Some(subscriber_outbound_tx) = state.signal_connections.get(room_name, subscriber_identity)
    else {
        tracing::debug!(
            room = room_name,
            subscriber_identity,
            track_sid,
            "subscription_unsupported_codec_response_no_active_signal_connection"
        );
        return;
    };

    let response = proto::SignalResponse {
        message: Some(proto::signal_response::Message::SubscriptionResponse(
            proto::SubscriptionResponse {
                track_sid: track_sid.to_string(),
                err: proto::SubscriptionError::SeCodecUnsupported as i32,
            },
        )),
    };
    if subscriber_outbound_tx.send(response).is_err() {
        tracing::debug!(
            room = room_name,
            subscriber_identity,
            track_sid,
            "subscription_unsupported_codec_response_send_failed"
        );
    } else {
        tracing::debug!(
            room = room_name,
            subscriber_identity,
            track_sid,
            "subscription_unsupported_codec_response_sent"
        );
    }
}

pub(crate) fn reject_unsupported_video_subscription_if_needed(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    subscriber_identity: &str,
    track: &proto::TrackInfo,
    enforce_advertised_codec_support: bool,
) -> bool {
    if !enforce_advertised_codec_support
        || subscriber_supports_any_track_mime_type(state, room_name, subscriber_identity, track)
    {
        return false;
    }

    let already_rejected = state.media_subscriptions.explicit_subscription(
        room_name,
        publisher_identity,
        &track.sid,
        subscriber_identity,
    ) == Some(false);

    state.media_subscriptions.set_subscribed(
        room_name,
        publisher_identity,
        &track.sid,
        subscriber_identity,
        false,
    );
    let _ = state.rooms.set_media_track_subscribed(
        room_name,
        publisher_identity,
        &track.sid,
        subscriber_identity,
        false,
    );
    let _ = state.forward_tracks.remove(
        room_name,
        publisher_identity,
        &track.sid,
        subscriber_identity,
    );
    clear_publisher_subscription_active_if_no_remaining_tracks(
        state,
        room_name,
        publisher_identity,
        subscriber_identity,
    );

    if !already_rejected {
        emit_subscription_unsupported_codec_response(
            state,
            room_name,
            subscriber_identity,
            &track.sid,
        );
    }

    tracing::debug!(
        room = %room_name,
        publisher_identity = %publisher_identity,
        subscriber_identity = %subscriber_identity,
        track_sid = %track.sid,
        track_mime_types = ?advertised_track_mime_types(track),
        already_rejected,
        "subscription_rejected_codec_unsupported"
    );

    true
}

pub(crate) async fn activate_tracks_with_compatible_bind_results(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    track_sids: &HashSet<String>,
) -> HashSet<String> {
    let participants = state.rooms.list_participants(room_name).unwrap_or_default();
    let mut compatible_track_sids = HashSet::new();
    let mut rejected_forward_tracks = Vec::new();
    let mut unsupported_response_track_sids = Vec::new();
    let mut rejected_track_sids = HashSet::new();

    for track_sid in track_sids {
        let Some((publisher_identity, track)) = participants.iter().find_map(|participant| {
            participant
                .tracks
                .iter()
                .find(|track| track.sid == *track_sid)
                .map(|track| (participant.identity.as_str(), track))
        }) else {
            continue;
        };

        match state
            .forward_tracks
            .bind_result_for_subscriber_track(
                room_name,
                publisher_identity,
                &track.sid,
                subscriber_identity,
            )
            .await
        {
            Some(oxidesfu_rtc::ForwardTrackBindResult::Compatible { .. }) => {
                let _ = state.rooms.set_media_track_subscribed(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                    true,
                );
                emit_aggregate_subscribed_quality_update_for_track(
                    state,
                    room_name,
                    publisher_identity,
                    track,
                    false,
                );
                compatible_track_sids.insert(track.sid.clone());
            }
            Some(oxidesfu_rtc::ForwardTrackBindResult::UnsupportedCodec)
                if track.r#type == proto::TrackType::Video as i32
                    && subscriber_supports_any_track_mime_type(
                        state,
                        room_name,
                        subscriber_identity,
                        track,
                    ) =>
            {
                // The initial server offer may have selected the publisher's default
                // codec before the subscriber answer revealed a compatible alternative.
                // Rebuild the forwarding sender in the next offer using that codec.
                let _ = state.forward_tracks.remove(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                );
                state.rtp_forwarding.remove(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                );
                state.media_forwarding.remove(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                );
                tracing::debug!(
                    room = room_name,
                    publisher_identity,
                    subscriber_identity,
                    track_sid = %track.sid,
                    "rebuilding_forward_track_with_subscriber_supported_codec"
                );
            }
            Some(oxidesfu_rtc::ForwardTrackBindResult::UnsupportedCodec)
                if track.r#type == proto::TrackType::Video as i32 =>
            {
                let already_rejected = state.media_subscriptions.explicit_subscription(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                ) == Some(false);
                state.media_subscriptions.set_subscribed(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                    false,
                );
                let _ = state.rooms.set_media_track_subscribed(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                    false,
                );
                if let Some(forward_track) = state.forward_tracks.remove(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                ) {
                    rejected_forward_tracks.push((track.sid.clone(), forward_track));
                }
                state.rtp_forwarding.remove(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    subscriber_identity,
                );
                rejected_track_sids.insert(track.sid.clone());
                if !already_rejected {
                    unsupported_response_track_sids.push(track.sid.clone());
                }
            }
            Some(oxidesfu_rtc::ForwardTrackBindResult::Pending) | None => {}
            Some(oxidesfu_rtc::ForwardTrackBindResult::UnsupportedCodec) => {}
        }
    }

    if let Some((subscriber_pc, connection_kind)) = state
        .peer_connections
        .media_receiver_for_identity(room_name, subscriber_identity)
    {
        for (track_sid, forward_track) in &rejected_forward_tracks {
            if let Err(error) = subscriber_pc.remove_forwarding_track(forward_track).await {
                tracing::warn!(
                    room = room_name,
                    subscriber_identity,
                    track_sid,
                    error = %error,
                    "failed_to_detach_unsupported_codec_forwarding_sender"
                );
            }
        }

        for track_sid in &unsupported_response_track_sids {
            emit_subscription_unsupported_codec_response(
                state,
                room_name,
                subscriber_identity,
                track_sid,
            );
        }

        if connection_kind == MediaForwardingConnectionKind::DualPcSubscriber
            && !rejected_forward_tracks.is_empty()
            && let Some(subscriber_outbound_tx) =
                state.signal_connections.get(room_name, subscriber_identity)
            && let Err(error) = signal_media_forwarding_negotiation_with_offer_id(
                state,
                &state.subscriber_offer_ids,
                room_name,
                subscriber_identity,
                &subscriber_pc,
                connection_kind,
                rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
                &subscriber_outbound_tx,
            )
            .await
        {
            tracing::warn!(
                room = room_name,
                subscriber_identity,
                error = %error,
                "failed_to_signal_unsupported_codec_cleanup_offer"
            );
        }
    } else {
        for track_sid in &unsupported_response_track_sids {
            emit_subscription_unsupported_codec_response(
                state,
                room_name,
                subscriber_identity,
                track_sid,
            );
        }
    }

    state.forward_tracks.activate_subscriber_track_sids(
        room_name,
        subscriber_identity,
        &compatible_track_sids,
    );

    rejected_track_sids
}

pub(crate) async fn reject_unaccepted_video_tracks_from_subscriber_answer(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    answer_id: u32,
    answer_sdp: &str,
) {
    let offered_mid_to_track_id =
        state.subscriber_offer_mid_track_ids(room_name, subscriber_identity, answer_id);
    if offered_mid_to_track_id.is_empty() {
        return;
    }

    let accepted_mids = crate::media::accepted_media_mids_from_answer_sdp(answer_sdp);
    let participants = state.rooms.list_participants(room_name).unwrap_or_default();
    let mut removed_forward_tracks = Vec::new();

    for (mid, track_sid) in offered_mid_to_track_id {
        if accepted_mids.contains(&mid) {
            continue;
        }

        let Some((publisher_identity, track)) = participants.iter().find_map(|participant| {
            participant
                .tracks
                .iter()
                .find(|track| track.sid == track_sid)
                .map(|track| (participant.identity.clone(), track.clone()))
        }) else {
            continue;
        };

        if track.r#type != proto::TrackType::Video as i32 {
            continue;
        }

        let already_rejected = state.media_subscriptions.explicit_subscription(
            room_name,
            &publisher_identity,
            &track.sid,
            subscriber_identity,
        ) == Some(false);

        state.media_subscriptions.set_subscribed(
            room_name,
            &publisher_identity,
            &track.sid,
            subscriber_identity,
            false,
        );
        let _ = state.rooms.set_media_track_subscribed(
            room_name,
            &publisher_identity,
            &track.sid,
            subscriber_identity,
            false,
        );
        if let Some(forward_track) = state.forward_tracks.remove(
            room_name,
            &publisher_identity,
            &track.sid,
            subscriber_identity,
        ) {
            removed_forward_tracks.push(forward_track);
        }
        clear_publisher_subscription_active_if_no_remaining_tracks(
            state,
            room_name,
            &publisher_identity,
            subscriber_identity,
        );

        if !already_rejected {
            emit_subscription_unsupported_codec_response(
                state,
                room_name,
                subscriber_identity,
                &track.sid,
            );
        }

        tracing::debug!(
            room = %room_name,
            publisher_identity = %publisher_identity,
            subscriber_identity = %subscriber_identity,
            track_sid = %track.sid,
            answer_id,
            rejected_mid = %mid,
            accepted_mids = ?accepted_mids,
            track_mime_types = ?advertised_track_mime_types(&track),
            already_rejected,
            "subscription_rejected_codec_unsupported_after_answer"
        );
    }

    if removed_forward_tracks.is_empty() {
        return;
    }

    let Some((subscriber_pc, connection_kind)) = state
        .peer_connections
        .media_receiver_for_identity(room_name, subscriber_identity)
    else {
        return;
    };

    for forward_track in &removed_forward_tracks {
        if let Err(error) = subscriber_pc.remove_forwarding_track(forward_track).await {
            tracing::warn!(
                room = room_name,
                subscriber_identity,
                error = %error,
                "failed_to_detach_rejected_forwarding_sender_after_answer"
            );
        }
    }

    if connection_kind == MediaForwardingConnectionKind::DualPcSubscriber
        && let Some(subscriber_outbound_tx) =
            state.signal_connections.get(room_name, subscriber_identity)
        && let Err(error) = signal_media_forwarding_negotiation_with_offer_id(
            state,
            &state.subscriber_offer_ids,
            room_name,
            subscriber_identity,
            &subscriber_pc,
            connection_kind,
            rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
            &subscriber_outbound_tx,
        )
        .await
    {
        tracing::warn!(
            room = room_name,
            subscriber_identity,
            error = %error,
            "failed_to_signal_rejected_forwarding_cleanup_offer_after_answer"
        );
    }
}

fn signal_track_subscribed_to_publisher(
    publisher_subscription_active_pairs: &PublisherSubscriptionActivePairs,
    signal_connections: &SignalConnectionStore,
    room_name: &str,
    publisher_identity: &str,
    subscriber_identity: &str,
    track_sid: &str,
) {
    let was_new_pair = publisher_subscription_active_pairs
        .lock()
        .map(|mut pairs| {
            pairs.insert((
                room_name.to_string(),
                publisher_identity.to_string(),
                subscriber_identity.to_string(),
            ))
        })
        .unwrap_or(false);
    if !was_new_pair {
        tracing::debug!(
            room = %room_name,
            publisher_identity = %publisher_identity,
            subscriber_identity = %subscriber_identity,
            track_sid = %track_sid,
            "track_subscribed_signal_suppressed_already_active_for_pair"
        );
        return;
    }

    let Some(publisher_outbound_tx) = signal_connections.get(room_name, publisher_identity) else {
        return;
    };

    let _ = publisher_outbound_tx.send(proto::SignalResponse {
        message: Some(proto::signal_response::Message::TrackSubscribed(
            proto::TrackSubscribed {
                track_sid: track_sid.to_string(),
            },
        )),
    });
    tracing::debug!(
        room = %room_name,
        publisher_identity = %publisher_identity,
        subscriber_identity = %subscriber_identity,
        track_sid = %track_sid,
        "track_subscribed_signal_sent_to_publisher"
    );
}

fn clear_publisher_subscription_active_if_no_remaining_tracks(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    subscriber_identity: &str,
) {
    let has_active_track = state
        .rooms
        .get_participant(room_name, publisher_identity)
        .map(|publisher| {
            publisher
                .tracks
                .into_iter()
                .filter(|track| !track.sid.is_empty())
                .any(|track| {
                    state.rooms.is_media_track_subscribed(
                        room_name,
                        publisher_identity,
                        &track.sid,
                        subscriber_identity,
                    ) && state.media_subscriptions.is_subscribed(
                        room_name,
                        publisher_identity,
                        &track.sid,
                        subscriber_identity,
                    )
                })
        })
        .unwrap_or(false);

    if !has_active_track {
        state.clear_publisher_subscription_active(
            room_name,
            publisher_identity,
            subscriber_identity,
        );
    }
}

pub(crate) fn should_emit_track_subscribed_for_subscriber(
    rooms: &RoomStore,
    room_name: &str,
    publisher_identity: &str,
    subscriber_identity: &str,
) -> bool {
    if subscriber_identity == publisher_identity {
        return false;
    }

    let Ok(subscriber) = rooms.get_participant(room_name, subscriber_identity) else {
        return false;
    };

    !super::participant_is_hidden(&subscriber)
}

#[allow(dead_code)]
pub(crate) fn should_forward_media_for_subscriber(
    media_subscriptions: &MediaSubscriptionStore,
    rooms: &RoomStore,
    room_name: &str,
    publisher_identity: &str,
    track_sid: &str,
    subscriber_identity: &str,
) -> bool {
    media_subscriptions.is_subscribed(
        room_name,
        publisher_identity,
        track_sid,
        subscriber_identity,
    ) && rooms.is_media_track_subscribed(
        room_name,
        publisher_identity,
        track_sid,
        subscriber_identity,
    )
}

#[derive(Clone, Copy)]
struct SubscriptionLimits {
    audio: Option<usize>,
    video: Option<usize>,
}

struct ForwardingDecisionContext<'a> {
    media_subscriptions: &'a MediaSubscriptionStore,
    auto_subscribe_preferences: &'a crate::stores::AutoSubscribePreferenceStore,
    track_settings: &'a crate::media::TrackSettingsStore,
    track_allocations: &'a crate::media::TrackAllocationStore,
    rooms: &'a RoomStore,
    subscription_limits: SubscriptionLimits,
}

impl ForwardingDecisionContext<'_> {
    fn settings_for(&self, key: &ForwardTrackKey) -> Option<proto::UpdateTrackSettings> {
        let (room_name, _, track_sid, subscriber_identity) = key;
        self.track_settings
            .get_for_track(room_name, subscriber_identity, track_sid)
    }
}

#[allow(clippy::too_many_arguments, dead_code)]
pub(crate) fn should_forward_media_for_subscriber_with_track_settings(
    media_subscriptions: &MediaSubscriptionStore,
    auto_subscribe_preferences: &crate::stores::AutoSubscribePreferenceStore,
    track_settings: &crate::media::TrackSettingsStore,
    rooms: &RoomStore,
    room_name: &str,
    publisher_identity: &str,
    track_sid: &str,
    subscriber_identity: &str,
) -> bool {
    let track_allocations = crate::media::TrackAllocationStore::default();
    let context = ForwardingDecisionContext {
        media_subscriptions,
        auto_subscribe_preferences,
        track_settings,
        track_allocations: &track_allocations,
        rooms,
        subscription_limits: SubscriptionLimits {
            audio: None,
            video: None,
        },
    };
    let key = (
        room_name.to_string(),
        publisher_identity.to_string(),
        track_sid.to_string(),
        subscriber_identity.to_string(),
    );
    should_forward_media_for_subscriber_with_track_settings_in(&context, &key)
}

fn should_forward_media_for_subscriber_with_track_settings_in(
    context: &ForwardingDecisionContext<'_>,
    key: &ForwardTrackKey,
) -> bool {
    let settings = context.settings_for(key);
    should_forward_media_for_subscriber_with_settings_value(context, key, settings.as_ref())
}

fn should_forward_media_for_subscriber_with_settings_value(
    context: &ForwardingDecisionContext<'_>,
    key: &ForwardTrackKey,
    settings: Option<&proto::UpdateTrackSettings>,
) -> bool {
    if settings.is_some_and(|settings| settings.disabled) {
        return false;
    }

    let (room_name, publisher_identity, track_sid, subscriber_identity) = key;
    let default_subscribed = context
        .auto_subscribe_preferences
        .auto_subscribe_enabled(room_name, subscriber_identity);
    context.media_subscriptions.is_subscribed_with_default(
        room_name,
        publisher_identity,
        track_sid,
        subscriber_identity,
        default_subscribed,
    ) && context.rooms.can_subscribe_to_media_track(
        room_name,
        publisher_identity,
        track_sid,
        subscriber_identity,
    )
}

fn subscriber_within_track_type_subscription_limit(
    context: &ForwardingDecisionContext<'_>,
    key: &ForwardTrackKey,
    track_info: Option<&proto::TrackInfo>,
) -> bool {
    let Some(track_info) = track_info else {
        return true;
    };

    let limit = if track_info.r#type == proto::TrackType::Audio as i32 {
        context.subscription_limits.audio
    } else if track_info.r#type == proto::TrackType::Video as i32 {
        context.subscription_limits.video
    } else {
        None
    };

    let Some(limit) = limit else {
        return true;
    };

    if limit == 0 {
        return false;
    }

    let (room_name, publisher_identity, track_sid, subscriber_identity) = key;
    let default_subscribed = context
        .auto_subscribe_preferences
        .auto_subscribe_enabled(room_name, subscriber_identity);
    let mut candidates = context
        .rooms
        .list_participants(room_name)
        .unwrap_or_default()
        .into_iter()
        .filter(|participant| participant.identity != *subscriber_identity)
        .flat_map(|participant| {
            let publisher = participant.identity;
            participant
                .tracks
                .into_iter()
                .filter(|track| !track.sid.is_empty() && track.r#type == track_info.r#type)
                .map(move |track| (publisher.clone(), track.sid))
        })
        .filter(|(candidate_publisher, candidate_track_sid)| {
            context.media_subscriptions.is_subscribed_with_default(
                room_name,
                candidate_publisher,
                candidate_track_sid,
                subscriber_identity,
                default_subscribed,
            ) && context.rooms.can_subscribe_to_media_track(
                room_name,
                candidate_publisher,
                candidate_track_sid,
                subscriber_identity,
            )
        })
        .map(|(candidate_publisher, candidate_track_sid)| {
            let currently_forwarded = context.rooms.is_media_track_subscribed(
                room_name,
                &candidate_publisher,
                &candidate_track_sid,
                subscriber_identity,
            );
            (
                candidate_publisher,
                candidate_track_sid,
                currently_forwarded,
            )
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|a, b| {
        b.2.cmp(&a.2)
            .then_with(|| a.0.cmp(&b.0))
            .then_with(|| a.1.cmp(&b.1))
    });

    candidates
        .into_iter()
        .take(limit)
        .any(|(candidate_publisher, candidate_track_sid, _)| {
            candidate_publisher == *publisher_identity && candidate_track_sid == *track_sid
        })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ForwardingDecisionRevisions {
    media_subscription: u64,
    auto_subscribe: u64,
    track_settings: u64,
    track_allocation: u64,
    room_media_subscription: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct CachedForwardingDecision {
    should_forward_media: bool,
    requested_max_quality: Option<proto::VideoQuality>,
    desired_quality: Option<proto::VideoQuality>,
    desired_temporal_layer: Option<u8>,
    requested_fps: Option<u32>,
    revisions: ForwardingDecisionRevisions,
}

#[derive(Debug, Default)]
struct ForwardingRtpWindow {
    packets: u64,
    wire_bytes: u64,
}

impl ForwardingRtpWindow {
    fn record_successful_write(&mut self, wire_bytes: u64) {
        self.packets = self.packets.saturating_add(1);
        self.wire_bytes = self.wire_bytes.saturating_add(wire_bytes);
    }

    fn finish_window(&mut self, duration: std::time::Duration) -> RtpWindowSnapshot {
        let duration_millis = duration.as_millis().max(1) as u64;
        let snapshot = RtpWindowSnapshot {
            packets: self.packets,
            wire_bytes: self.wire_bytes,
            packets_per_second: self.packets.saturating_mul(1_000) / duration_millis,
            wire_bytes_per_second: self.wire_bytes.saturating_mul(1_000) / duration_millis,
        };
        *self = Self::default();
        snapshot
    }
}

#[derive(Debug, Default)]
struct DownstreamFeedbackCounters {
    pli_received: u64,
    pli_sent: u64,
    pli_suppressed: u64,
    fir_received: u64,
    fir_sent: u64,
    fir_suppressed: u64,
}

const MAX_PREPARED_VIDEO_RTP_BATCH_SIZE: usize = 64;

const fn uses_prepared_video_batching(is_video_track: bool) -> bool {
    is_video_track
}

/// Latches only a known-good prepared sender binding.
///
/// A forwarding target can observe RTP before its negotiated sender is bound. A temporary pending
/// binding must therefore remain retryable rather than permanently disabling batching.
fn prepared_video_batching_is_ready(
    cached_compatible: &mut Option<bool>,
    forwarding_mid: Option<&str>,
    binding_compatible: bool,
) -> bool {
    if *cached_compatible == Some(true) {
        return true;
    }

    let ready = forwarding_mid.is_some_and(|mid| !mid.is_empty()) && binding_compatible;
    if ready {
        *cached_compatible = Some(true);
    }
    ready
}

/// Target-local, frame-bounded output retained until one prepared driver write.
///
/// Packets have already received their final RTP rewrite before entering this queue. This keeps
/// retransmission caching independent from when the prepared batch reaches the WebRTC driver.
#[derive(Default)]
struct PendingVideoRtpBatch {
    packets: Vec<rtc::rtp::Packet>,
    timestamp: Option<u32>,
    payload_bytes: u64,
    wire_bytes: u64,
    first_incoming_payload_type: Option<u8>,
    first_forwarded_payload_type: Option<u8>,
}

impl PendingVideoRtpBatch {
    fn needs_flush_before(&self, timestamp: u32) -> bool {
        self.timestamp.is_some_and(|current| current != timestamp)
            || self.packets.len() >= MAX_PREPARED_VIDEO_RTP_BATCH_SIZE
    }

    fn push(
        &mut self,
        packet: rtc::rtp::Packet,
        incoming_payload_type: u8,
        wire_bytes: u64,
    ) -> bool {
        debug_assert!(self.packets.len() < MAX_PREPARED_VIDEO_RTP_BATCH_SIZE);
        if self.packets.is_empty() {
            self.timestamp = Some(packet.header.timestamp);
            self.first_incoming_payload_type = Some(incoming_payload_type);
            self.first_forwarded_payload_type = Some(packet.header.payload_type);
        }
        self.payload_bytes = self
            .payload_bytes
            .saturating_add(packet.payload.len() as u64);
        self.wire_bytes = self.wire_bytes.saturating_add(wire_bytes);
        let flush_after =
            packet.header.marker || self.packets.len() + 1 == MAX_PREPARED_VIDEO_RTP_BATCH_SIZE;
        self.packets.push(packet);
        flush_after
    }

    fn take(&mut self) -> Option<CompletedVideoRtpBatch> {
        (!self.packets.is_empty()).then(|| CompletedVideoRtpBatch {
            packets: std::mem::take(&mut self.packets),
            timestamp: self
                .timestamp
                .take()
                .expect("non-empty video batch has a timestamp"),
            payload_bytes: std::mem::take(&mut self.payload_bytes),
            wire_bytes: std::mem::take(&mut self.wire_bytes),
            incoming_payload_type: self
                .first_incoming_payload_type
                .take()
                .expect("non-empty video batch has an incoming payload type"),
            forwarded_payload_type: self
                .first_forwarded_payload_type
                .take()
                .expect("non-empty video batch has a forwarded payload type"),
        })
    }
}

struct CompletedVideoRtpBatch {
    packets: Vec<rtc::rtp::Packet>,
    timestamp: u32,
    payload_bytes: u64,
    wire_bytes: u64,
    incoming_payload_type: u8,
    forwarded_payload_type: u8,
}

#[derive(Debug, Default)]
struct VideoForwardingCounters {
    layer_switches: u64,
    drop_waiting_for_keyframe: u64,
    drop_non_selected_ssrc: u64,
    drop_above_maximum: u64,
    drop_unknown_layer: u64,
    drop_temporal_above_maximum: u64,
    drop_temporal_above_desired: u64,
    drop_temporal_timestamp_cap: u64,
    selector_pli_requests: u64,
    selector_pli_suppressed_stable: u64,
    selector_pli_suppressed_fallback_locked: u64,
    selector_pli_suppressed_budget_exhausted: u64,
    selector_pli_suppressed_retry_or_no_target: u64,
    rewrite_drops: u64,
    successful_rtp_packets: u64,
    successful_rtp_payload_bytes: u64,
}

/// Reader-owned state for one subscriber forwarding target.
///
/// A publisher reader is single-threaded, so policy and packet-processing state belongs here
/// rather than in compound-key maps. The RTP forwarder remains shared with RTCP through
/// `RtpForwardingStore`.
struct ForwardTarget {
    key: ForwardTrackKey,
    local_forward_track: oxidesfu_rtc::LocalRtpTrack,
    rtp_forwarder: crate::media::SubscriberRtpForwarder,
    decision: Option<CachedForwardingDecision>,
    settings_revision: Option<u64>,
    allocation_revision: Option<u64>,
    video_layer_selector: SubscriberVideoLayerSelector,
    dependency_descriptor_forwarding_selector: Option<(
        DependencyDescriptorLayerPolicy,
        DependencyDescriptorForwardingSelector,
    )>,
    video_temporal_controller: SubscriberVideoTemporalController,
    video_counters: VideoForwardingCounters,
    downstream_feedback_counters: DownstreamFeedbackCounters,
    rtp_window: ForwardingRtpWindow,
    target_primary_ssrc: Option<Option<u32>>,
    target_payload_type: Option<Option<u8>>,
    target_dependency_descriptor_extension_id: Option<Option<u8>>,
    prepared_video_batching_compatible: Option<bool>,
    pending_video_rtp_batch: PendingVideoRtpBatch,

    forwarded_once: bool,
    logged_video_rewrite_drop: bool,
    logged_audio_rewrite_drop: bool,
    video_write_errors: u64,
    audio_write_errors: u64,
}

async fn flush_pending_video_rtp_batch(
    target: &mut ForwardTarget,
) -> oxidesfu_rtc::RtcResult<Option<CompletedVideoRtpBatch>> {
    let Some(batch) = target.pending_video_rtp_batch.take() else {
        return Ok(None);
    };
    let packet_count = batch.packets.len() as u64;
    let local_forward_track = target.local_forward_track.clone();
    let CompletedVideoRtpBatch {
        packets,
        timestamp,
        payload_bytes,
        wire_bytes,
        incoming_payload_type,
        forwarded_payload_type,
    } = batch;
    match local_forward_track
        .write_rtp_batch_with_cached_mid_preserving_extensions(packets)
        .await
    {
        Ok(()) => {
            target.video_counters.successful_rtp_packets = target
                .video_counters
                .successful_rtp_packets
                .saturating_add(packet_count);
            target.video_counters.successful_rtp_payload_bytes = target
                .video_counters
                .successful_rtp_payload_bytes
                .saturating_add(payload_bytes);
            target.rtp_window.record_successful_write(wire_bytes);
            Ok(Some(CompletedVideoRtpBatch {
                packets: Vec::new(),
                timestamp,
                payload_bytes,
                wire_bytes,
                incoming_payload_type,
                forwarded_payload_type,
            }))
        }
        Err(error) => {
            target.video_write_errors = target.video_write_errors.saturating_add(packet_count);
            Err(error)
        }
    }
}

impl ForwardTarget {
    fn new(
        key: ForwardTrackKey,
        local_forward_track: oxidesfu_rtc::LocalRtpTrack,
        rtp_forwarder: crate::media::SubscriberRtpForwarder,
    ) -> Self {
        Self {
            key,
            local_forward_track,
            rtp_forwarder,
            decision: None,
            settings_revision: None,
            allocation_revision: None,
            video_layer_selector: SubscriberVideoLayerSelector::default(),
            dependency_descriptor_forwarding_selector: None,
            video_temporal_controller: SubscriberVideoTemporalController::default(),
            video_counters: VideoForwardingCounters::default(),
            downstream_feedback_counters: DownstreamFeedbackCounters::default(),
            rtp_window: ForwardingRtpWindow::default(),
            target_primary_ssrc: None,
            target_payload_type: None,
            target_dependency_descriptor_extension_id: None,
            prepared_video_batching_compatible: None,
            pending_video_rtp_batch: PendingVideoRtpBatch::default(),

            forwarded_once: false,
            logged_video_rewrite_drop: false,
            logged_audio_rewrite_drop: false,
            video_write_errors: 0,
            audio_write_errors: 0,
        }
    }

    fn replace_transport(
        &mut self,
        local_forward_track: oxidesfu_rtc::LocalRtpTrack,
        rtp_forwarder: crate::media::SubscriberRtpForwarder,
    ) {
        self.local_forward_track = local_forward_track;
        self.rtp_forwarder = rtp_forwarder;
        self.target_dependency_descriptor_extension_id = None;
        self.prepared_video_batching_compatible = None;
    }

    fn subscriber_identity(&self) -> &str {
        &self.key.3
    }
}

fn dependency_descriptor_dti(
    indication: rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication,
) -> DependencyDescriptorDti {
    match indication {
        rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication::NotPresent => DependencyDescriptorDti::NotPresent,
        rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication::Discardable => DependencyDescriptorDti::Discardable,
        rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication::Switch => DependencyDescriptorDti::Switch,
        rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication::Required => DependencyDescriptorDti::Required,
    }
}

/// Removes entries that cannot be represented by RFC 8285 one-byte extension headers.
///
/// A zero-length one-byte extension would underflow the WebRTC RTP serializer. Such an entry has
/// no valid wire representation (ID zero is padding), so removing it preserves the serializable
/// final-wire packet used for both batching and retransmission caching.
fn remove_unencodable_one_byte_extensions(packet: &mut rtc::rtp::Packet) {
    if packet.header.extension_profile == rtc::rtp::header::EXTENSION_PROFILE_ONE_BYTE {
        packet
            .header
            .extensions
            .retain(|extension| extension.id != 0 && !extension.payload.is_empty());
        if packet.header.extensions.is_empty() {
            packet.header.extension = false;
            packet.header.extension_profile = 0;
            packet.header.extensions_padding = 0;
        }
    }
}

fn rewrite_dependency_descriptor_for_target(
    packet: &mut rtc::rtp::Packet,
    snapshot: &oxidesfu_rtc::DependencyDescriptorMetadataSnapshot,
    decision: DependencyDescriptorForwardingDecision,
    destination_extension_id: Option<u8>,
) -> bool {
    let DependencyDescriptorForwardingDecision::Forward { target } = decision else {
        return false;
    };

    let Some(source_payload) = packet
        .header
        .extensions
        .iter()
        .find(|extension| extension.id == snapshot.source_extension_id)
        .map(|extension| extension.payload.clone())
    else {
        return false;
    };

    packet
        .header
        .extensions
        .retain(|extension| extension.id != snapshot.source_extension_id);

    let num_decode_targets = u8::try_from(snapshot.decode_target_indications.len()).ok();
    let rewritten_payload = num_decode_targets.and_then(|num_decode_targets| {
        (target < num_decode_targets).then(|| {
            rtc::rtp::extension::dependency_descriptor_extension::replace_or_inject_active_decode_target_mask(
                &source_payload,
                num_decode_targets,
                1_u32 << target,
            )
        })?
    });
    if let (Some(destination_extension_id), Some(rewritten_payload)) =
        (destination_extension_id, rewritten_payload)
    {
        let _ = packet
            .header
            .set_extension(destination_extension_id, rewritten_payload.into());
    }

    if packet.header.extensions.is_empty() {
        packet.header.extension = false;
        packet.header.extension_profile = 0;
        packet.header.extensions_padding = 0;
    }
    true
}

/// Packet-local dependency-descriptor view shared by every subscriber target.
///
/// Conversion from RTC descriptor metadata happens once before the forwarding target loop so a
/// fan-out does not allocate equivalent target-layer and DTI vectors for every subscriber.
struct PacketDependencyDescriptorFrame {
    snapshot: oxidesfu_rtc::DependencyDescriptorMetadataSnapshot,
    target_layers: Vec<DependencyDescriptorTargetLayer>,
    dtis: Vec<DependencyDescriptorDti>,
}

impl PacketDependencyDescriptorFrame {
    fn new(snapshot: oxidesfu_rtc::DependencyDescriptorMetadataSnapshot) -> Self {
        let target_layers = snapshot
            .target_layers
            .iter()
            .map(|layer| DependencyDescriptorTargetLayer {
                target: layer.target,
                spatial: match layer.spatial_id {
                    0 => SpatialLayer::Low,
                    1 => SpatialLayer::Medium,
                    _ => SpatialLayer::High,
                },
                temporal: layer.temporal_id,
            })
            .collect();
        let dtis = snapshot
            .decode_target_indications
            .iter()
            .copied()
            .map(dependency_descriptor_dti)
            .collect();
        Self {
            snapshot,
            target_layers,
            dtis,
        }
    }
}

fn select_dependency_descriptor_frame(
    target: &mut ForwardTarget,
    frame: &PacketDependencyDescriptorFrame,
    spatial_policy: LayerPolicy,
    temporal_policy: Option<TemporalLayerPolicy>,
) -> DependencyDescriptorForwardingDecision {
    let temporal_policy = temporal_policy.unwrap_or(TemporalLayerPolicy {
        max: TemporalLayer::T2,
        desired: TemporalLayer::T2,
    });
    let policy = DependencyDescriptorLayerPolicy {
        max_spatial: spatial_policy.max,
        desired_spatial: spatial_policy.desired,
        max_temporal: temporal_policy.max as u8,
        desired_temporal: temporal_policy.desired as u8,
    };
    if target
        .dependency_descriptor_forwarding_selector
        .as_ref()
        .is_none_or(|(current_policy, _)| *current_policy != policy)
    {
        target.dependency_descriptor_forwarding_selector =
            Some((policy, DependencyDescriptorForwardingSelector::new(policy)));
    }
    let Some((_, selector)) = target.dependency_descriptor_forwarding_selector.as_mut() else {
        return DependencyDescriptorForwardingDecision::DropInvalidMetadata;
    };
    selector.select(DependencyDescriptorFrame {
        frame_number: frame.snapshot.frame_number,
        active_decode_targets: frame.snapshot.active_decode_targets,
        target_layers: &frame.target_layers,
        dtis: &frame.dtis,
        frame_diffs: &frame.snapshot.frame_diffs,
        chain_diffs: &frame.snapshot.chain_diffs,
        target_protected_by_chain: &frame.snapshot.target_protected_by_chain,
    })
}

const FORWARDING_SNAPSHOT_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);

const fn spatial_layer_name(layer: SpatialLayer) -> &'static str {
    match layer {
        SpatialLayer::Low => "low",
        SpatialLayer::Medium => "medium",
        SpatialLayer::High => "high",
    }
}

const fn video_source_kind_name(source_kind: VideoSourceKind) -> &'static str {
    match source_kind {
        VideoSourceKind::Simulcast => "simulcast",
        VideoSourceKind::SingleScalable => "single_scalable",
    }
}

const fn temporal_layer_name(layer: TemporalLayer) -> &'static str {
    match layer {
        TemporalLayer::T0 => "t0",
        TemporalLayer::T1 => "t1",
        TemporalLayer::T2 => "t2",
    }
}

const fn acquisition_state_name(state: LayerAcquisitionState) -> &'static str {
    match state {
        LayerAcquisitionState::Stable => "stable",
        LayerAcquisitionState::WaitingForDesired => "waiting_for_desired",
        LayerAcquisitionState::WaitingForFallback => "waiting_for_fallback",
        LayerAcquisitionState::FallbackLocked => "fallback_locked",
    }
}

fn outgoing_rtp_wire_bytes(packet: &rtc::rtp::Packet) -> u64 {
    let header = &packet.header;
    let mut header_bytes = 12 + header.csrc.len().saturating_mul(4);
    if header.extension {
        let extension_bytes = header
            .get_extension_payload_len()
            .saturating_add(header.extensions_padding);
        header_bytes = header_bytes.saturating_add(4 + extension_bytes.div_ceil(4) * 4);
    }
    let padding_bytes = if header.padding {
        let remainder = packet.payload.len() % 4;
        if remainder == 0 { 4 } else { 4 - remainder }
    } else {
        0
    };
    (header_bytes
        .saturating_add(packet.payload.len())
        .saturating_add(padding_bytes)) as u64
}

/// Returns the bounded JSON-lines heartbeat tail for profiler collection.
#[allow(dead_code)] // Coordinator-owned profiler/API integration consumes this internal path.
/// Returns bounded forwarding diagnostics for local profiler collection.
#[doc(hidden)]
pub fn forwarding_snapshot_json_lines() -> String {
    forwarding_snapshot::forwarding_snapshot_json_lines()
}

fn record_forwarding_snapshot(
    room_name: &str,
    publisher_identity: &str,
    track_sid: &str,
    targets: &mut [ForwardTarget],
    video_ssrc_rids: &HashMap<u32, Option<String>>,
) {
    let targets = targets
        .iter_mut()
        .map(|target| {
            let spatial_policy = target.video_layer_selector.policy();
            let temporal_policy = target.video_temporal_controller.policy();
            let selected_ssrc = target.video_layer_selector.selected_ssrc();
            ForwardingTargetSnapshot {
                subscriber_identity: target.subscriber_identity().to_string(),
                spatial: SpatialSelectionSnapshot {
                    source_kind: video_source_kind_name(target.video_layer_selector.source_kind()),
                    maximum: spatial_layer_name(spatial_policy.max),
                    desired: spatial_layer_name(spatial_policy.desired),
                    current: target
                        .video_layer_selector
                        .current_spatial()
                        .map(spatial_layer_name),
                    selected_ssrc,
                    selected_rid: selected_ssrc
                        .and_then(|ssrc| video_ssrc_rids.get(&ssrc))
                        .and_then(|rid| rid.clone()),
                    acquisition_state: acquisition_state_name(
                        target.video_layer_selector.acquisition_state(),
                    ),
                    waiting_for: spatial_layer_name(target.video_layer_selector.waiting_for()),
                    acquisition_ticks: target.video_layer_selector.acquisition_ticks(),
                    remaining_pli_requests: target.video_layer_selector.remaining_pli_requests(),
                    transitions: target.video_counters.layer_switches,
                },
                temporal: TemporalSelectionSnapshot {
                    maximum: temporal_policy.map(|policy| temporal_layer_name(policy.max)),
                    desired: temporal_policy.map(|policy| temporal_layer_name(policy.desired)),
                    current: target
                        .video_temporal_controller
                        .current()
                        .map(temporal_layer_name),
                },
                rtp_window: target.rtp_window.finish_window(FORWARDING_SNAPSHOT_WINDOW),
                selector_pli: SelectorPliSnapshot {
                    sent: target.video_counters.selector_pli_requests,
                    suppressed_stable: target.video_counters.selector_pli_suppressed_stable,
                    suppressed_fallback_locked: target
                        .video_counters
                        .selector_pli_suppressed_fallback_locked,
                    suppressed_budget_exhausted: target
                        .video_counters
                        .selector_pli_suppressed_budget_exhausted,
                    suppressed_retry_or_no_target: target
                        .video_counters
                        .selector_pli_suppressed_retry_or_no_target,
                },
                downstream_feedback: DownstreamFeedbackSnapshot {
                    pli_received: target.downstream_feedback_counters.pli_received,
                    pli_sent: target.downstream_feedback_counters.pli_sent,
                    pli_suppressed: target.downstream_feedback_counters.pli_suppressed,
                    fir_received: target.downstream_feedback_counters.fir_received,
                    fir_sent: target.downstream_feedback_counters.fir_sent,
                    fir_suppressed: target.downstream_feedback_counters.fir_suppressed,
                },
                forwarding: ForwardingResultSnapshot {
                    rewrite_drops: target.video_counters.rewrite_drops,
                    write_errors: target.video_write_errors,
                    drop_waiting_for_keyframe: target.video_counters.drop_waiting_for_keyframe,
                    drop_non_selected_ssrc: target.video_counters.drop_non_selected_ssrc,
                    drop_above_maximum: target.video_counters.drop_above_maximum,
                    drop_unknown_layer: target.video_counters.drop_unknown_layer,
                    drop_temporal_above_maximum: target.video_counters.drop_temporal_above_maximum,
                    drop_temporal_above_desired: target.video_counters.drop_temporal_above_desired,
                    drop_temporal_timestamp_cap: target.video_counters.drop_temporal_timestamp_cap,
                },
            }
        })
        .collect();

    forwarding_snapshot::record_snapshot(ForwardingSnapshot {
        schema_version: 1,
        sequence: NEXT_FORWARDING_SNAPSHOT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        window_duration_ms: FORWARDING_SNAPSHOT_WINDOW.as_millis() as u64,
        room: room_name.to_string(),
        publisher_identity: publisher_identity.to_string(),
        track_sid: track_sid.to_string(),
        targets,
    });
}

/// Returns whether a timer requested one diagnostic scan, consuming that request.
pub(super) fn take_forwarding_debug_heartbeat(heartbeat_due: &mut bool) -> bool {
    std::mem::take(heartbeat_due)
}

async fn refresh_forward_targets_for_track(
    targets: &mut Vec<ForwardTarget>,
    forward_tracks: &ForwardTrackStore,
    rtp_forwarding: &RtpForwardingStore,
    room_name: &str,
    publisher_identity: &str,
    track_sid: &str,
) {
    for target in targets.iter_mut() {
        if let Err(error) = flush_pending_video_rtp_batch(target).await {
            tracing::warn!(
                subscriber_identity = %target.subscriber_identity(),
                write_errors = target.video_write_errors,
                error = %error,
                "video_forwarding_write_failed_before_target_transport_replacement"
            );
        }
    }
    let mut previous_targets = std::mem::take(targets)
        .into_iter()
        .map(|target| (target.key.clone(), target))
        .collect::<HashMap<_, _>>();

    for (key, local_forward_track) in
        forward_tracks.list_for_track(room_name, publisher_identity, track_sid)
    {
        let Some(rtp_forwarder) = rtp_forwarding.forwarder_for(&key) else {
            continue;
        };
        if let Some(mut target) = previous_targets.remove(&key) {
            target.replace_transport(local_forward_track, rtp_forwarder);
            targets.push(target);
        } else {
            targets.push(ForwardTarget::new(key, local_forward_track, rtp_forwarder));
        }
    }
}

#[allow(deprecated)]
fn advertised_video_bitrate_for_quality(
    track: &proto::TrackInfo,
    quality: proto::VideoQuality,
) -> Option<u64> {
    track
        .layers
        .iter()
        .chain(track.codecs.iter().flat_map(|codec| codec.layers.iter()))
        .filter(|layer| {
            normalized_video_quality_from_i32(layer.quality) == quality && layer.bitrate > 0
        })
        .map(|layer| layer.bitrate as u64)
        .min()
}

#[allow(deprecated)]
fn allocation_quality_for_budget(
    track: &proto::TrackInfo,
    per_track_budget_bps: u64,
) -> Option<proto::VideoQuality> {
    [
        proto::VideoQuality::High,
        proto::VideoQuality::Medium,
        proto::VideoQuality::Low,
    ]
    .into_iter()
    .find(|quality| {
        advertised_video_bitrate_for_quality(track, *quality)
            .is_some_and(|bitrate| bitrate <= per_track_budget_bps)
    })
    .or_else(|| {
        advertised_video_bitrate_for_quality(track, proto::VideoQuality::Low)
            .map(|_| proto::VideoQuality::Low)
    })
}

#[allow(deprecated)]
fn allocation_temporal_layer_for_budget(
    track: &proto::TrackInfo,
    quality: proto::VideoQuality,
    per_track_budget_bps: u64,
) -> Option<u8> {
    let bitrate = advertised_video_bitrate_for_quality(track, quality)?;
    Some(
        if per_track_budget_bps.saturating_mul(100) >= bitrate.saturating_mul(90) {
            2
        } else if per_track_budget_bps.saturating_mul(100) >= bitrate.saturating_mul(60) {
            1
        } else {
            0
        },
    )
}

fn allocation_available_outgoing_bitrate_bps(
    test_support_override: Option<u64>,
    rtc_estimate: Option<u64>,
) -> Option<u64> {
    test_support_override.or(rtc_estimate)
}

#[allow(deprecated)]
fn allocation_layout_weight(
    track: &proto::TrackInfo,
    settings: Option<&proto::UpdateTrackSettings>,
) -> u64 {
    let requested_area = settings
        .filter(|settings| settings.width > 0 && settings.height > 0)
        .map(|settings| u64::from(settings.width) * u64::from(settings.height));
    requested_area.unwrap_or_else(|| {
        track
            .layers
            .iter()
            .chain(track.codecs.iter().flat_map(|codec| codec.layers.iter()))
            .map(|layer| u64::from(layer.width) * u64::from(layer.height))
            .max()
            .unwrap_or(1)
            .max(1)
    })
}

fn eligible_video_layout_weight_for_subscriber(
    rooms: &RoomStore,
    media_subscriptions: &MediaSubscriptionStore,
    track_settings: &crate::media::TrackSettingsStore,
    room_name: &str,
    subscriber_identity: &str,
) -> u64 {
    rooms
        .list_participants(room_name)
        .unwrap_or_default()
        .into_iter()
        .filter(|publisher| publisher.identity != subscriber_identity)
        .flat_map(|publisher| {
            publisher.tracks.into_iter().filter_map(move |track| {
                (track.r#type == proto::TrackType::Video as i32
                    && media_subscriptions.is_subscribed(
                        room_name,
                        &publisher.identity,
                        &track.sid,
                        subscriber_identity,
                    )
                    && rooms.can_subscribe_to_media_track(
                        room_name,
                        &publisher.identity,
                        &track.sid,
                        subscriber_identity,
                    ))
                .then(|| {
                    allocation_layout_weight(
                        &track,
                        track_settings
                            .get_for_track(room_name, subscriber_identity, &track.sid)
                            .as_ref(),
                    )
                })
            })
        })
        .sum::<u64>()
        .max(1)
}

#[cfg(test)]
fn cached_forwarding_decision_for_subscriber(
    cache: &mut HashMap<ForwardTrackKey, CachedForwardingDecision>,
    revisions: ForwardingDecisionRevisions,
    context: &ForwardingDecisionContext<'_>,
    track_info: Option<&proto::TrackInfo>,
    key: &ForwardTrackKey,
) -> CachedForwardingDecision {
    if let Some(cached) = cache.get(key)
        && cached.revisions == revisions
    {
        return *cached;
    }

    let (room_name, _, track_sid, subscriber_identity) = key;
    let settings = context.settings_for(key);
    let requested_max_quality =
        requested_video_quality_from_settings(settings.as_ref(), track_info);
    let allocated_desired_quality = context.track_allocations.get_desired_quality_for_track(
        room_name,
        subscriber_identity,
        track_sid,
    );
    let allocated_desired_temporal_layer = context
        .track_allocations
        .get_desired_temporal_layer_for_track(room_name, subscriber_identity, track_sid);
    let should_forward_media =
        should_forward_media_for_subscriber_with_settings_value(context, key, settings.as_ref())
            && subscriber_within_track_type_subscription_limit(context, key, track_info);
    let decision = CachedForwardingDecision {
        should_forward_media,
        requested_max_quality,
        desired_quality: desired_video_quality_from_allocation(
            requested_max_quality,
            allocated_desired_quality,
        ),
        desired_temporal_layer: allocated_desired_temporal_layer,
        requested_fps: requested_video_fps_from_settings(settings.as_ref()),
        revisions,
    };

    cache.insert(key.clone(), decision);
    decision
}

fn cached_forwarding_decision_for_target(
    target: &mut ForwardTarget,
    revisions: ForwardingDecisionRevisions,
    context: &ForwardingDecisionContext<'_>,
    track_info: Option<&proto::TrackInfo>,
) -> CachedForwardingDecision {
    if let Some(decision) = target.decision
        && decision.revisions == revisions
    {
        return decision;
    }

    let (room_name, _, track_sid, subscriber_identity) = &target.key;
    let settings = context.settings_for(&target.key);
    let requested_max_quality =
        requested_video_quality_from_settings(settings.as_ref(), track_info);
    let allocated_desired_quality = context.track_allocations.get_desired_quality_for_track(
        room_name,
        subscriber_identity,
        track_sid,
    );
    let allocated_desired_temporal_layer = context
        .track_allocations
        .get_desired_temporal_layer_for_track(room_name, subscriber_identity, track_sid);
    let decision = CachedForwardingDecision {
        should_forward_media: should_forward_media_for_subscriber_with_settings_value(
            context,
            &target.key,
            settings.as_ref(),
        ) && subscriber_within_track_type_subscription_limit(
            context,
            &target.key,
            track_info,
        ),
        requested_max_quality,
        desired_quality: desired_video_quality_from_allocation(
            requested_max_quality,
            allocated_desired_quality,
        ),
        desired_temporal_layer: allocated_desired_temporal_layer,
        requested_fps: requested_video_fps_from_settings(settings.as_ref()),
        revisions,
    };
    target.decision = Some(decision);
    decision
}

/// Applies one effective settings revision to its reader-owned forwarding target.
fn apply_target_settings_revision(target: &mut ForwardTarget, settings_revision: u64) -> bool {
    let changed = target.settings_revision != Some(settings_revision);
    target.settings_revision = Some(settings_revision);
    if changed {
        target.decision = None;
        target.video_temporal_controller = SubscriberVideoTemporalController::default();
    }
    changed
}

/// Applies one effective allocation revision to its reader-owned forwarding target.
fn apply_target_allocation_revision(target: &mut ForwardTarget, allocation_revision: u64) -> bool {
    let changed = target.allocation_revision != Some(allocation_revision);
    target.allocation_revision = Some(allocation_revision);
    if changed {
        target.decision = None;
    }
    changed
}

/// Applies an effective mutation only to its matching reader target.
fn apply_effective_track_settings_change(
    targets: &mut [ForwardTarget],
    change: &crate::media::EffectiveTrackSettingsChange,
) -> usize {
    targets
        .iter_mut()
        .map(|target| {
            (target.key.0 == change.room
                && target.key.2 == change.track_sid
                && target.key.3 == change.subscriber_identity)
                && apply_target_settings_revision(target, change.revision)
        })
        .filter(|changed| *changed)
        .count()
}

/// Applies an allocation mutation only to its matching reader target.
fn apply_effective_track_allocation_change(
    targets: &mut [ForwardTarget],
    change: &crate::media::EffectiveTrackAllocationChange,
) -> usize {
    targets
        .iter_mut()
        .map(|target| {
            (target.key.0 == change.room
                && target.key.2 == change.track_sid
                && target.key.3 == change.subscriber_identity)
                && apply_target_allocation_revision(target, change.revision)
        })
        .filter(|changed| *changed)
        .count()
}

/// Recovers from a bounded notification receiver lag without returning to packet polling.
fn resync_target_settings_revisions(
    targets: &mut [ForwardTarget],
    track_settings: &crate::media::TrackSettingsStore,
) -> usize {
    targets
        .iter_mut()
        .map(|target| {
            let revision =
                track_settings.revision_for_track(&target.key.0, &target.key.3, &target.key.2);
            apply_target_settings_revision(target, revision)
        })
        .filter(|changed| *changed)
        .count()
}

/// Recovers allocation revisions from bounded receiver lag.
fn resync_target_allocation_revisions(
    targets: &mut [ForwardTarget],
    track_allocations: &crate::media::TrackAllocationStore,
) -> usize {
    targets
        .iter_mut()
        .map(|target| {
            let revision =
                track_allocations.revision_for_track(&target.key.0, &target.key.3, &target.key.2);
            apply_target_allocation_revision(target, revision)
        })
        .filter(|changed| *changed)
        .count()
}

#[allow(deprecated)]
fn track_supports_layer_quality_control(track: &proto::TrackInfo) -> bool {
    track.simulcast
        || track.layers.len() > 1
        || track.codecs.iter().any(|codec| codec.layers.len() > 1)
}

#[allow(deprecated)]
fn min_layer_dimension(layer: &proto::VideoLayer) -> Option<u32> {
    match (layer.width, layer.height) {
        (0, 0) => None,
        (0, h) => Some(h),
        (w, 0) => Some(w),
        (w, h) => Some(w.min(h)),
    }
}

#[allow(deprecated)]
fn requested_quality_from_dimensions_for_track(
    settings: &proto::UpdateTrackSettings,
    track: &proto::TrackInfo,
) -> Option<proto::VideoQuality> {
    if settings.width == 0 || settings.height == 0 {
        return None;
    }

    let requested_size = settings.width.min(settings.height) as f32;
    let requested_size_with_tolerance = requested_size * 0.8;

    let mut quality_size_pairs = std::collections::HashMap::<proto::VideoQuality, u32>::new();
    let mut ingest_layer = |layer: &proto::VideoLayer| {
        let size = min_layer_dimension(layer).unwrap_or(0);
        if size == 0 {
            return;
        }
        let quality = normalized_video_quality_from_i32(layer.quality);
        quality_size_pairs
            .entry(quality)
            .and_modify(|current| *current = (*current).max(size))
            .or_insert(size);
    };

    for layer in &track.layers {
        ingest_layer(layer);
    }
    for codec in &track.codecs {
        for layer in &codec.layers {
            ingest_layer(layer);
        }
    }

    if quality_size_pairs.is_empty() {
        let orig_size = match (track.width, track.height) {
            (0, 0) => 0,
            (0, h) => h,
            (w, 0) => w,
            (w, h) => w.min(h),
        };
        if orig_size > 0 {
            quality_size_pairs.insert(proto::VideoQuality::Low, 180);
            quality_size_pairs.insert(proto::VideoQuality::Medium, 360);
            quality_size_pairs.insert(proto::VideoQuality::High, orig_size);
        }
    }

    if quality_size_pairs.is_empty() {
        return None;
    }

    let mut ordered = quality_size_pairs.into_iter().collect::<Vec<_>>();
    ordered.sort_by_key(|(quality, _)| *quality as i32);

    let mut selected = ordered
        .last()
        .map(|(quality, _)| *quality)
        .unwrap_or(proto::VideoQuality::High);
    for (quality, size) in &ordered {
        if (*size as f32) >= requested_size_with_tolerance {
            selected = *quality;
            break;
        }
    }

    Some(selected)
}

#[allow(deprecated)]
pub(crate) fn requested_video_quality_for_track(
    track_settings: &crate::media::TrackSettingsStore,
    room_name: &str,
    subscriber_identity: &str,
    track_sid: &str,
    track: Option<&proto::TrackInfo>,
) -> Option<proto::VideoQuality> {
    let settings = track_settings.get_for_track(room_name, subscriber_identity, track_sid);
    requested_video_quality_from_settings(settings.as_ref(), track)
}

#[allow(deprecated)]
fn requested_video_quality_from_settings(
    settings: Option<&proto::UpdateTrackSettings>,
    track: Option<&proto::TrackInfo>,
) -> Option<proto::VideoQuality> {
    let settings = settings?;
    if settings.disabled {
        return None;
    }

    if let Some(track) = track
        && let Some(derived) = requested_quality_from_dimensions_for_track(settings, track)
    {
        return Some(derived);
    }

    Some(proto::VideoQuality::try_from(settings.quality).unwrap_or(proto::VideoQuality::High))
}

#[allow(deprecated)]
fn desired_video_quality_from_allocation(
    requested_max_quality: Option<proto::VideoQuality>,
    allocated_desired_quality: Option<proto::VideoQuality>,
) -> Option<proto::VideoQuality> {
    match (requested_max_quality, allocated_desired_quality) {
        (None, None) => None,
        (None, Some(desired)) => Some(desired),
        (Some(maximum), None) => Some(maximum),
        (Some(maximum), Some(desired)) => Some(if (desired as i32) <= (maximum as i32) {
            desired
        } else {
            maximum
        }),
    }
}

#[allow(deprecated)]
pub(crate) fn infer_video_quality_from_rid(rid: Option<&str>) -> Option<proto::VideoQuality> {
    let rid = rid?.trim().to_ascii_lowercase();
    if rid.is_empty() {
        return None;
    }

    // LiveKit-compatible simulcast RID sets commonly use either q/h/f or 2/1/0.
    if rid == "q" || rid == "l" || rid == "low" || rid == "2" || rid.contains("low") {
        return Some(proto::VideoQuality::Low);
    }
    if rid == "h"
        || rid == "m"
        || rid == "mid"
        || rid == "medium"
        || rid == "1"
        || rid.contains("mid")
    {
        return Some(proto::VideoQuality::Medium);
    }
    if rid == "f" || rid == "high" || rid == "0" || rid.contains("high") {
        return Some(proto::VideoQuality::High);
    }

    None
}

#[cfg(test)]
#[allow(deprecated)]
pub(crate) fn should_forward_video_packet_for_requested_quality(
    requested_max_quality: Option<proto::VideoQuality>,
    packet_quality: Option<proto::VideoQuality>,
) -> bool {
    let Some(requested_max_quality) = requested_max_quality else {
        return true;
    };

    let Some(packet_quality) = packet_quality else {
        return true;
    };

    (packet_quality as i32) <= (requested_max_quality as i32)
}

#[allow(deprecated)]
fn normalized_video_quality_from_i32(quality: i32) -> proto::VideoQuality {
    proto::VideoQuality::try_from(quality).unwrap_or(proto::VideoQuality::High)
}

#[allow(deprecated)]
pub(crate) fn layer_quality_maps_for_track(
    track: &proto::TrackInfo,
) -> (
    std::collections::HashMap<u32, proto::VideoQuality>,
    std::collections::HashMap<String, proto::VideoQuality>,
) {
    let mut ssrc_quality = std::collections::HashMap::new();
    let mut rid_quality = std::collections::HashMap::new();

    let mut ingest_layer = |layer: &proto::VideoLayer| {
        let quality = normalized_video_quality_from_i32(layer.quality);
        if layer.ssrc != 0 {
            ssrc_quality.entry(layer.ssrc).or_insert(quality);
        }
        let rid = layer.rid.trim().to_ascii_lowercase();
        if !rid.is_empty() {
            rid_quality.entry(rid).or_insert(quality);
        }
    };

    if !track.codecs.is_empty() {
        for codec in &track.codecs {
            for layer in &codec.layers {
                ingest_layer(layer);
            }
        }
    }

    if !track.layers.is_empty() {
        for layer in &track.layers {
            ingest_layer(layer);
        }
    }

    (ssrc_quality, rid_quality)
}

#[allow(deprecated)]
pub(crate) fn packet_video_quality_for_track(
    incoming_ssrc: u32,
    incoming_rid: Option<&str>,
    layer_quality_by_ssrc: &std::collections::HashMap<u32, proto::VideoQuality>,
    layer_quality_by_rid: &std::collections::HashMap<String, proto::VideoQuality>,
) -> Option<proto::VideoQuality> {
    if let Some(quality) = layer_quality_by_ssrc.get(&incoming_ssrc).copied() {
        return Some(quality);
    }

    if let Some(rid) = incoming_rid {
        let normalized = rid.trim().to_ascii_lowercase();
        if !normalized.is_empty() {
            if let Some(quality) = layer_quality_by_rid.get(&normalized).copied() {
                return Some(quality);
            }
            return infer_video_quality_from_rid(Some(&normalized));
        }
    }

    None
}

/// Returns whether a known scalable codec has one source stream rather than an advertised
/// simulcast SSRC/RID catalog.
///
/// Spatial packets within this source are decoder targets, not alternate source streams. Their
/// filtering therefore belongs to scalable decode-target policy, not the simulcast source selector.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn is_single_scalable_source(
    codec_mime: Option<&str>,
    has_advertised_layer_mapping: bool,
) -> bool {
    is_single_scalable_source_for_codec_class(
        video_codec_class_from_mime(codec_mime),
        has_advertised_layer_mapping,
    )
}

fn is_single_scalable_source_for_codec_class(
    codec_class: VideoCodecClass,
    has_advertised_layer_mapping: bool,
) -> bool {
    if has_advertised_layer_mapping {
        return false;
    }

    matches!(codec_class, VideoCodecClass::Vp9 | VideoCodecClass::Av1)
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FpsForwardingState {
    current_timestamp: Option<u32>,
    forward_current_timestamp: bool,
    last_forwarded_timestamp: Option<u32>,
}

impl FpsForwardingState {
    pub(crate) fn should_forward_packet(
        &mut self,
        packet_timestamp: u32,
        requested_fps: u32,
    ) -> bool {
        if requested_fps == 0 {
            return true;
        }

        let is_new_frame = self.current_timestamp != Some(packet_timestamp);
        if is_new_frame {
            self.current_timestamp = Some(packet_timestamp);

            // RTP clocks are nominal: a 30 FPS source driven at a 33 ms application cadence
            // advances by 2_970 ticks rather than exactly 3_000. Apply the same 10% tolerance
            // used for temporal-layer selection so a request at source cadence is not decimated.
            let min_timestamp_delta = (90_000_u32
                .saturating_mul(9)
                .saturating_div(10_u32.saturating_mul(requested_fps.max(1))))
            .max(1);
            let allow = match self.last_forwarded_timestamp {
                None => true,
                Some(last) => packet_timestamp.wrapping_sub(last) >= min_timestamp_delta,
            };

            self.forward_current_timestamp = allow;
            if allow {
                self.last_forwarded_timestamp = Some(packet_timestamp);
            }
        }

        self.forward_current_timestamp
    }
}

/// A temporal scalability layer, ordered from base to highest enhancement layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TemporalLayer {
    T0 = 0,
    T1 = 1,
    T2 = 2,
}

impl TemporalLayer {
    const fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::T0),
            1 => Some(Self::T1),
            2 => Some(Self::T2),
            _ => None,
        }
    }
}

/// The temporal policy derived from a subscriber FPS limit and observed source cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TemporalLayerPolicy {
    pub(crate) max: TemporalLayer,
    pub(crate) desired: TemporalLayer,
}

/// The target-local outcome for a single video packet's temporal decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TemporalIngressDecision {
    Forward,
    DropAboveMaximum,
    DropAboveDesired,
    DropTimestampCap,
}

/// Target-local temporal selection state.
///
/// Temporal metadata identifies enhancement packets that cannot be forwarded above the target.
/// When either the source cadence or packet temporal ID is unavailable, the controller retains the
/// deterministic RTP-timestamp gate used before temporal metadata was available.
#[derive(Debug, Default)]
pub(crate) struct SubscriberVideoTemporalController {
    policy: Option<TemporalLayerPolicy>,
    current: Option<TemporalLayer>,
    timestamp_fallback: FpsForwardingState,
    timestamp_cap_required: bool,
}

impl SubscriberVideoTemporalController {
    pub(crate) const fn policy(&self) -> Option<TemporalLayerPolicy> {
        self.policy
    }

    pub(crate) const fn current(&self) -> Option<TemporalLayer> {
        self.current
    }

    /// Updates target policy without resetting timestamp continuity. `None` explicitly selects
    /// the metadata-poor timestamp fallback path rather than guessing a temporal layer.
    #[cfg(test)]
    pub(crate) fn set_requested_fps(
        &mut self,
        requested_fps: u32,
        receiver_temporal_layer_fps: Option<[Option<f32>; 3]>,
    ) {
        self.set_requested_fps_with_desired_temporal_layer(
            requested_fps,
            receiver_temporal_layer_fps,
            None,
        );
    }

    /// Applies an allocator temporal target while preserving the subscriber FPS ceiling.
    pub(crate) fn set_requested_fps_with_desired_temporal_layer(
        &mut self,
        requested_fps: u32,
        receiver_temporal_layer_fps: Option<[Option<f32>; 3]>,
        desired_temporal_layer: Option<u8>,
    ) {
        let Some(receiver_temporal_layer_fps) = receiver_temporal_layer_fps else {
            self.policy = None;
            self.current = None;
            self.timestamp_cap_required = false;
            return;
        };
        let Some(max) = max_temporal_layer_for_requested_fps_from_receiver(
            requested_fps,
            &receiver_temporal_layer_fps,
        )
        .and_then(TemporalLayer::from_id) else {
            self.policy = None;
            self.current = None;
            self.timestamp_cap_required = false;
            return;
        };

        let desired = desired_temporal_layer
            .and_then(TemporalLayer::from_id)
            .map(|desired| desired.min(max))
            .unwrap_or(max);
        let policy = TemporalLayerPolicy { max, desired };
        if self.policy != Some(policy) {
            self.policy = Some(policy);
            self.current = self.current.map(|current| current.min(desired));
        }
        self.timestamp_cap_required = should_apply_timestamp_fps_cap_for_selected_temporal_layer(
            requested_fps,
            &receiver_temporal_layer_fps,
            desired as u8,
        );
    }

    pub(crate) fn observe_packet(
        &mut self,
        packet_timestamp: u32,
        requested_fps: u32,
        temporal_id: Option<u8>,
    ) -> TemporalIngressDecision {
        if requested_fps == 0 {
            return TemporalIngressDecision::Forward;
        }

        if let (Some(policy), Some(temporal)) =
            (self.policy, temporal_id.and_then(TemporalLayer::from_id))
        {
            if temporal > policy.max {
                return TemporalIngressDecision::DropAboveMaximum;
            }
            if temporal > policy.desired {
                return TemporalIngressDecision::DropAboveDesired;
            }
            if self.timestamp_cap_required
                && !self
                    .timestamp_fallback
                    .should_forward_packet(packet_timestamp, requested_fps)
            {
                return TemporalIngressDecision::DropTimestampCap;
            }
            self.current = Some(
                self.current
                    .map_or(temporal, |current| current.max(temporal)),
            );
            return TemporalIngressDecision::Forward;
        }

        if self
            .timestamp_fallback
            .should_forward_packet(packet_timestamp, requested_fps)
        {
            TemporalIngressDecision::Forward
        } else {
            TemporalIngressDecision::DropTimestampCap
        }
    }
}

const LAYER_SELECTION_TOLERANCE: f32 = 0.9;

fn should_apply_timestamp_fps_cap_for_selected_temporal_layer(
    requested_fps: u32,
    receiver_temporal_layer_fps: &[Option<f32>; 3],
    selected_temporal_layer: u8,
) -> bool {
    if requested_fps == 0 {
        return false;
    }

    let Some(selected_layer_fps) = receiver_temporal_layer_fps
        .get(selected_temporal_layer as usize)
        .and_then(|fps| *fps)
    else {
        return false;
    };

    let requested = requested_fps as f32;
    let relaxed_requested = requested * (1.0 / LAYER_SELECTION_TOLERANCE);
    selected_layer_fps > relaxed_requested
}

pub(crate) fn max_temporal_layer_for_requested_fps_from_receiver(
    requested_fps: u32,
    receiver_temporal_layer_fps: &[Option<f32>; 3],
) -> Option<u8> {
    let request_fps = requested_fps as f32 * LAYER_SELECTION_TOLERANCE;
    for (temporal, layer_fps) in receiver_temporal_layer_fps.iter().enumerate() {
        if let Some(layer_fps) = layer_fps
            && request_fps <= *layer_fps
        {
            return Some(temporal as u8);
        }
    }
    None
}

pub(crate) fn vp8_temporal_layer_id_from_payload(payload: &[u8]) -> Option<u8> {
    rtc::rtp::codec::vp8::temporal_layer_id_from_payload(payload)
}

/// Returns whether this VP8 RTP payload begins partition zero of a keyframe.
///
/// A simulcast source switch must begin at this boundary. The parser only reads the RTP payload
/// descriptor and frame tag, so it does not allocate or depend on packet history.
pub(crate) fn vp8_is_keyframe_start(payload: &[u8]) -> bool {
    let Some(descriptor) = payload.first().copied() else {
        return false;
    };
    if descriptor & 0x10 == 0 || descriptor & 0x0f != 0 {
        return false;
    }

    let mut offset = 1usize;
    if descriptor & 0x80 != 0 {
        let Some(extension) = payload.get(offset).copied() else {
            return false;
        };
        offset += 1;
        if extension & 0x80 != 0 {
            let Some(picture_id) = payload.get(offset).copied() else {
                return false;
            };
            offset += 1 + usize::from(picture_id & 0x80 != 0);
        }
        if extension & 0x40 != 0 {
            offset += 1;
        }
        if extension & 0x20 != 0 || extension & 0x10 != 0 {
            offset += 1;
        }
    }

    payload
        .get(offset)
        .is_some_and(|frame_tag| frame_tag & 1 == 0)
}

/// Returns whether a VP9 RTP packet starts a non-predicted frame.
pub(crate) fn vp9_is_keyframe_start(payload: &[u8]) -> bool {
    let Some(descriptor) = payload.first().copied() else {
        return false;
    };
    // RFC draft-ietf-payload-vp9: B marks the start of a frame and P marks an
    // inter-picture predicted frame. A switch may begin only at B with P clear.
    descriptor & 0x08 != 0 && descriptor & 0x40 == 0
}

/// Returns whether an H264 RTP packet begins an IDR NAL unit.
pub(crate) fn h264_is_keyframe_start(payload: &[u8]) -> bool {
    let Some(indicator) = payload.first().copied() else {
        return false;
    };
    match indicator & 0x1f {
        5 => true, // Single NAL unit IDR.
        24 => {
            // STAP-A: inspect the contained NAL units without allocating.
            let mut offset = 1usize;
            while let Some(length_bytes) = payload.get(offset..offset + 2) {
                let length = u16::from_be_bytes([length_bytes[0], length_bytes[1]]) as usize;
                offset += 2;
                let Some(nal) = payload.get(offset..offset + length) else {
                    return false;
                };
                if nal.first().is_some_and(|header| header & 0x1f == 5) {
                    return true;
                }
                offset += length;
            }
            false
        }
        28 | 29 => payload
            .get(1)
            .is_some_and(|header| header & 0x80 != 0 && header & 0x1f == 5),
        _ => false,
    }
}

/// Returns whether an AV1 RTP packet begins a new decodable coded-video sequence.
pub(crate) fn av1_is_keyframe_start(payload: &[u8]) -> bool {
    let Some(aggregation_header) = payload.first().copied() else {
        return false;
    };
    // RFC 9364 N marks a new coded video sequence. This is the same boundary used by the local
    // AV1 depacketizer before it accepts decoded frames.
    aggregation_header & 0x08 != 0
}

/// Determines whether a packet may start a decodable spatial simulcast source after a switch.
/// Codec formats without a verified detector deliberately return false rather than allowing an
/// arbitrary delta frame to become the new source.
fn video_is_decodable_switch_point_for_codec_class(
    codec_class: VideoCodecClass,
    payload: &[u8],
) -> bool {
    match codec_class {
        VideoCodecClass::Vp8 => vp8_is_keyframe_start(payload),
        VideoCodecClass::Vp9 => vp9_is_keyframe_start(payload),
        VideoCodecClass::H264 => h264_is_keyframe_start(payload),
        VideoCodecClass::Av1 => av1_is_keyframe_start(payload),
        _ => false,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn video_is_decodable_switch_point(codec_mime: Option<&str>, payload: &[u8]) -> bool {
    video_is_decodable_switch_point_for_codec_class(
        video_codec_class_from_mime(codec_mime),
        payload,
    )
}

/// Applies dependency-descriptor source-switch semantics for scalable VP9/AV1 when available.
/// A parsed descriptor that is not a switch point must not fall back to a payload heuristic.
fn video_is_decodable_switch_point_with_dependency_descriptor_for_codec_class(
    codec_class: VideoCodecClass,
    payload: &[u8],
    descriptor_switch_point: Option<bool>,
) -> bool {
    if matches!(codec_class, VideoCodecClass::Vp9 | VideoCodecClass::Av1) {
        return descriptor_switch_point.unwrap_or_else(|| {
            video_is_decodable_switch_point_for_codec_class(codec_class, payload)
        });
    }

    video_is_decodable_switch_point_for_codec_class(codec_class, payload)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn video_is_decodable_switch_point_with_dependency_descriptor(
    codec_mime: Option<&str>,
    payload: &[u8],
    descriptor_switch_point: Option<bool>,
) -> bool {
    video_is_decodable_switch_point_with_dependency_descriptor_for_codec_class(
        video_codec_class_from_mime(codec_mime),
        payload,
        descriptor_switch_point,
    )
}

pub(crate) fn vp9_temporal_layer_id_from_payload(payload: &[u8]) -> Option<u8> {
    rtc::rtp::codec::vp9::temporal_layer_id_from_payload(payload)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn vp9_spatial_layer_id_from_payload(payload: &[u8]) -> Option<u8> {
    rtc::rtp::codec::vp9::layer_ids_from_payload(payload).map(|ids| ids.spatial_id)
}

pub(crate) fn h265_temporal_layer_id_from_payload(payload: &[u8]) -> Option<u8> {
    rtc::rtp::codec::h265::temporal_layer_id_from_payload(payload)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VideoCodecClass {
    Unknown,
    Vp8,
    Vp9,
    H264,
    H265,
    Av1,
    Rtx,
    Other,
}

fn video_codec_class_from_mime(codec_mime: Option<&str>) -> VideoCodecClass {
    let Some(codec_mime) = codec_mime else {
        return VideoCodecClass::Unknown;
    };

    let mime = codec_mime.trim().to_ascii_lowercase();
    if mime.is_empty() {
        VideoCodecClass::Unknown
    } else if mime.contains("rtx") {
        VideoCodecClass::Rtx
    } else if mime.contains("vp8") {
        VideoCodecClass::Vp8
    } else if mime.contains("vp9") {
        VideoCodecClass::Vp9
    } else if mime.contains("h264") {
        VideoCodecClass::H264
    } else if mime.contains("h265") {
        VideoCodecClass::H265
    } else if mime.contains("av1") {
        VideoCodecClass::Av1
    } else {
        VideoCodecClass::Other
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VideoTemporalCodecHint {
    Vp8,
    Vp9,
    H265,
    Unknown,
}

fn video_temporal_codec_hint_from_class(codec_class: VideoCodecClass) -> VideoTemporalCodecHint {
    match codec_class {
        VideoCodecClass::Vp8 => VideoTemporalCodecHint::Vp8,
        VideoCodecClass::Vp9 => VideoTemporalCodecHint::Vp9,
        VideoCodecClass::H265 => VideoTemporalCodecHint::H265,
        _ => VideoTemporalCodecHint::Unknown,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn video_temporal_codec_hint_from_mime(
    codec_mime: Option<&str>,
) -> VideoTemporalCodecHint {
    video_temporal_codec_hint_from_class(video_codec_class_from_mime(codec_mime))
}

#[cfg(test)]
pub(crate) fn should_forward_video_packet_for_requested_fps(
    requested_fps: u32,
    codec_mime: Option<&str>,
    receiver_temporal_layer_fps: Option<[Option<f32>; 3]>,
    packet_timestamp: u32,
    payload: &[u8],
    packet_temporal_layer_hint: Option<u8>,
    fps_state: &mut FpsForwardingState,
) -> bool {
    should_forward_video_packet_for_requested_fps_with_codec_hint(
        requested_fps,
        video_temporal_codec_hint_from_mime(codec_mime),
        receiver_temporal_layer_fps,
        packet_timestamp,
        payload,
        packet_temporal_layer_hint,
        fps_state,
    )
}

pub(crate) fn temporal_layer_id_from_packet(
    codec_hint: VideoTemporalCodecHint,
    payload: &[u8],
    packet_temporal_layer_hint: Option<u8>,
) -> Option<u8> {
    packet_temporal_layer_hint.or_else(|| match codec_hint {
        VideoTemporalCodecHint::Vp8 => vp8_temporal_layer_id_from_payload(payload),
        VideoTemporalCodecHint::Vp9 => vp9_temporal_layer_id_from_payload(payload),
        VideoTemporalCodecHint::H265 => h265_temporal_layer_id_from_payload(payload),
        VideoTemporalCodecHint::Unknown => None,
    })
}

#[cfg(test)]
fn should_forward_video_packet_for_requested_fps_with_codec_hint(
    requested_fps: u32,
    codec_hint: VideoTemporalCodecHint,
    receiver_temporal_layer_fps: Option<[Option<f32>; 3]>,
    packet_timestamp: u32,
    payload: &[u8],
    packet_temporal_layer_hint: Option<u8>,
    fps_state: &mut FpsForwardingState,
) -> bool {
    if requested_fps == 0 {
        return true;
    }

    let temporal_id =
        temporal_layer_id_from_packet(codec_hint, payload, packet_temporal_layer_hint);

    if let Some(temporal_id) = temporal_id
        && let Some(receiver_temporal_layer_fps) = receiver_temporal_layer_fps.as_ref()
        && let Some(max_temporal) = max_temporal_layer_for_requested_fps_from_receiver(
            requested_fps,
            receiver_temporal_layer_fps,
        )
    {
        if temporal_id > max_temporal {
            return false;
        }

        if should_apply_timestamp_fps_cap_for_selected_temporal_layer(
            requested_fps,
            receiver_temporal_layer_fps,
            max_temporal,
        ) {
            return fps_state.should_forward_packet(packet_timestamp, requested_fps);
        }

        return true;
    }

    fps_state.should_forward_packet(packet_timestamp, requested_fps)
}

pub(crate) fn forwarding_target_revision_changed(
    cached_revision: Option<u64>,
    current_revision: u64,
) -> bool {
    cached_revision != Some(current_revision)
}

#[cfg(test)]
pub(crate) fn retain_forwarding_state_for_current_targets<T>(
    states: &mut std::collections::HashMap<(String, String, String, String), T>,
    current_forward_keys: &std::collections::HashSet<(String, String, String, String)>,
) {
    states.retain(|key, _| current_forward_keys.contains(key));
}

#[cfg(test)]
pub(crate) fn retain_fps_forwarding_state_for_current_targets(
    fps_states: &mut std::collections::HashMap<
        (String, String, String, String),
        FpsForwardingState,
    >,
    current_forward_keys: &std::collections::HashSet<(String, String, String, String)>,
) {
    retain_forwarding_state_for_current_targets(fps_states, current_forward_keys);
}

fn requested_video_fps_from_settings(settings: Option<&proto::UpdateTrackSettings>) -> Option<u32> {
    let settings = settings?;
    (settings.fps > 0).then_some(settings.fps)
}

#[allow(deprecated)]
fn has_active_media_subscriber_for_track(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    track_sid: &str,
) -> bool {
    let Ok(participants) = state.rooms.list_participants(room_name) else {
        return false;
    };

    participants.into_iter().any(|participant| {
        let subscriber_identity = participant.identity;
        let explicit_subscription = state.media_subscriptions.explicit_subscription(
            room_name,
            publisher_identity,
            track_sid,
            &subscriber_identity,
        );

        subscriber_identity != publisher_identity
            && explicit_subscription != Some(false)
            && (explicit_subscription.is_some()
                || state
                    .signal_connections
                    .get(room_name, &subscriber_identity)
                    .is_some())
            && state.rooms.is_media_track_subscribed(
                room_name,
                publisher_identity,
                track_sid,
                &subscriber_identity,
            )
    })
}

pub(crate) fn aggregate_requested_quality_for_track(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    track_sid: &str,
) -> Option<proto::VideoQuality> {
    let Ok(participants) = state.rooms.list_participants(room_name) else {
        return None;
    };

    let publisher_track = state
        .rooms
        .get_participant(room_name, publisher_identity)
        .ok()
        .and_then(|participant| {
            participant
                .tracks
                .into_iter()
                .find(|track| track.sid == track_sid)
        });

    let mut max_quality: Option<proto::VideoQuality> = None;
    for participant in participants {
        let subscriber_identity = participant.identity;
        if subscriber_identity == publisher_identity {
            continue;
        }

        let explicit_subscription = state.media_subscriptions.explicit_subscription(
            room_name,
            publisher_identity,
            track_sid,
            &subscriber_identity,
        );
        if explicit_subscription == Some(false) {
            continue;
        }
        if explicit_subscription.is_none()
            && state
                .signal_connections
                .get(room_name, &subscriber_identity)
                .is_none()
        {
            continue;
        }

        if !state.rooms.is_media_track_subscribed(
            room_name,
            publisher_identity,
            track_sid,
            &subscriber_identity,
        ) {
            continue;
        }

        let settings =
            state
                .track_settings
                .get_for_track(room_name, &subscriber_identity, track_sid);
        if settings.is_some_and(|settings| settings.disabled) {
            continue;
        }

        let requested = requested_video_quality_for_track(
            &state.track_settings,
            room_name,
            &subscriber_identity,
            track_sid,
            publisher_track.as_ref(),
        )
        .unwrap_or(proto::VideoQuality::High);

        max_quality = Some(match max_quality {
            Some(current) if (current as i32) >= (requested as i32) => current,
            _ => requested,
        });
    }

    max_quality
}

#[allow(deprecated)]
pub(crate) fn emit_aggregate_subscribed_quality_update_for_track(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    track: &proto::TrackInfo,
    emit_when_no_active_receiver: bool,
) {
    if track.r#type != proto::TrackType::Video as i32 {
        return;
    }

    let track_is_still_published = state
        .rooms
        .get_participant(room_name, publisher_identity)
        .ok()
        .is_some_and(|publisher| {
            publisher
                .tracks
                .iter()
                .any(|current| current.sid == track.sid)
        });
    if !track_is_still_published {
        tracing::debug!(
            room = %room_name,
            publisher_identity,
            track_sid = %track.sid,
            "skipping_subscribed_quality_update_for_unpublished_track"
        );
        return;
    }

    let Some(publisher_outbound_tx) = state.signal_connections.get(room_name, publisher_identity)
    else {
        return;
    };

    let has_active_receiver =
        has_active_media_subscriber_for_track(state, room_name, publisher_identity, &track.sid);
    if !has_active_receiver && !emit_when_no_active_receiver {
        return;
    }

    let codec_mime_types = crate::media::codec_mime_types_for_track(track);
    let aggregate_max_quality =
        aggregate_requested_quality_for_track(state, room_name, publisher_identity, &track.sid);

    let update = crate::media::subscribed_quality_update_for_track_with_codecs(
        &track.sid,
        &codec_mime_types,
        aggregate_max_quality,
    );

    let _ = publisher_outbound_tx.send(proto::SignalResponse {
        message: Some(proto::signal_response::Message::SubscribedQualityUpdate(
            update,
        )),
    });
}

#[cfg(test)]
pub(crate) fn should_force_recvonly_for_single_pc_receive_sections(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
) -> bool {
    let Ok(participants) = state.rooms.list_participants(room_name) else {
        return false;
    };

    if state
        .pending_media_section_requests
        .has_for_subscriber(room_name, subscriber_identity)
    {
        return true;
    }

    participants
        .into_iter()
        .filter(|participant| participant.identity != *subscriber_identity)
        .flat_map(|participant| participant.tracks.into_iter())
        .any(|track| !track.sid.is_empty())
}

fn offer_advertises_ice_trickle(offer_sdp: &str) -> bool {
    offer_sdp
        .lines()
        .any(|line| line.trim() == "a=ice-options:trickle")
}

fn sdp_line_without_ending(raw_line: &str) -> &str {
    raw_line.trim_end_matches(['\r', '\n'])
}

fn sdp_line_ending(raw_line: &str) -> &'static str {
    if raw_line.ends_with("\r\n") {
        "\r\n"
    } else if raw_line.ends_with('\n') {
        "\n"
    } else {
        ""
    }
}

fn sdp_line_with_ending(line: &str, ending: &str) -> String {
    let mut out = String::with_capacity(line.len() + ending.len());
    out.push_str(line);
    out.push_str(ending);
    out
}

fn is_sdp_direction_line(line: &str) -> bool {
    matches!(
        line,
        "a=sendrecv" | "a=sendonly" | "a=recvonly" | "a=inactive"
    )
}

fn force_answer_mids_sendonly(
    answer_sdp: &str,
    mids_to_force: &std::collections::HashSet<&str>,
) -> String {
    if mids_to_force.is_empty() {
        return answer_sdp.to_string();
    }

    let mut rewritten = String::with_capacity(answer_sdp.len());
    let mut current_mid: Option<String> = None;
    for raw_line in answer_sdp.split_inclusive('\n') {
        let line = sdp_line_without_ending(raw_line);
        if line.starts_with("m=") {
            current_mid = None;
        } else if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(mid.to_string());
        }

        let should_force = current_mid
            .as_deref()
            .is_some_and(|mid| mids_to_force.contains(mid));
        if should_force && is_sdp_direction_line(line) {
            rewritten.push_str(&sdp_line_with_ending(
                "a=sendonly",
                sdp_line_ending(raw_line),
            ));
        } else {
            rewritten.push_str(raw_line);
        }
    }
    rewritten
}

#[allow(dead_code)]
fn force_answer_mids_inactive(
    answer_sdp: &str,
    mids_to_force: &std::collections::HashSet<&str>,
) -> String {
    if mids_to_force.is_empty() {
        return answer_sdp.to_string();
    }

    let mut rewritten = String::with_capacity(answer_sdp.len());
    let mut current_mid: Option<String> = None;
    for raw_line in answer_sdp.split_inclusive('\n') {
        let line = sdp_line_without_ending(raw_line);
        if line.starts_with("m=") {
            current_mid = None;
        } else if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(mid.to_string());
        }

        let should_force = current_mid
            .as_deref()
            .is_some_and(|mid| mids_to_force.contains(mid));
        if should_force && is_sdp_direction_line(line) {
            rewritten.push_str(&sdp_line_with_ending(
                "a=inactive",
                sdp_line_ending(raw_line),
            ));
        } else {
            rewritten.push_str(raw_line);
        }
    }

    rewritten
}

pub(crate) fn force_sendonly_sections_without_msid_recvonly(
    answer_sdp: &str,
    attached_mids: &HashSet<&str>,
) -> String {
    fn rewrite_section_if_needed(section: &mut [String], attached_mids: &HashSet<&str>) {
        let is_media_section = section.first().is_some_and(|line| {
            let line = sdp_line_without_ending(line);
            line.starts_with("m=audio ") || line.starts_with("m=video ")
        });
        if !is_media_section {
            return;
        }

        let mut direction_line_index: Option<usize> = None;
        let mut has_msid = false;
        let mut mid = None;

        for (index, raw_line) in section.iter().enumerate() {
            let line = sdp_line_without_ending(raw_line);
            if let Some(section_mid) = line.strip_prefix("a=mid:") {
                mid = Some(section_mid);
            }
            if line == "a=sendonly" {
                direction_line_index = Some(index);
            }
            if line.starts_with("a=msid:")
                || (line.starts_with("a=ssrc:") && line.contains(" msid:"))
            {
                has_msid = true;
            }
        }

        if !has_msid
            && !mid.is_some_and(|mid| attached_mids.contains(mid))
            && let Some(index) = direction_line_index
        {
            let ending = sdp_line_ending(&section[index]);
            section[index] = sdp_line_with_ending("a=inactive", ending);
        }
    }

    let mut rewritten = Vec::new();
    let mut current_section = Vec::new();

    for raw_line in answer_sdp.split_inclusive('\n') {
        if raw_line.starts_with("m=") && !current_section.is_empty() {
            rewrite_section_if_needed(&mut current_section, attached_mids);
            rewritten.append(&mut current_section);
        }
        current_section.push(raw_line.to_string());
    }

    if !current_section.is_empty() {
        rewrite_section_if_needed(&mut current_section, attached_mids);
        rewritten.append(&mut current_section);
    }

    rewritten.concat()
}

fn push_normalized_answer_section_direction(
    section: &mut Vec<String>,
    normalized: &mut Vec<String>,
) {
    let Some(direction_index) = section
        .iter()
        .position(|line| is_sdp_direction_line(sdp_line_without_ending(line)))
    else {
        normalized.append(section);
        return;
    };

    let direction_line = section.remove(direction_index);
    let insertion_index = section
        .iter()
        .position(|line| {
            let line = sdp_line_without_ending(line);
            line.starts_with("a=msid:") || line.starts_with("a=ssrc:")
        })
        .unwrap_or(section.len());
    section.insert(insertion_index, direction_line);
    normalized.append(section);
}

fn normalize_answer_direction_order(answer_sdp: &str) -> String {
    let mut normalized = Vec::new();
    let mut current_section = Vec::new();

    for raw_line in answer_sdp.split_inclusive('\n') {
        if raw_line.starts_with("m=") && !current_section.is_empty() {
            push_normalized_answer_section_direction(&mut current_section, &mut normalized);
        }
        current_section.push(raw_line.to_string());
    }

    if !current_section.is_empty() {
        push_normalized_answer_section_direction(&mut current_section, &mut normalized);
    }

    normalized.concat()
}

fn participant_should_disable_h264_publish(client_info: &proto::ClientInfo) -> bool {
    let os = client_info.os.trim().to_ascii_lowercase();
    let device_model = client_info.device_model.trim().to_ascii_lowercase();
    let browser = client_info.browser.trim().to_ascii_lowercase();

    (device_model == "xiaomi 2201117ti" && os == "android")
        || ((browser == "firefox" || browser == "firefox mobile")
            && (os == "linux" || os == "android"))
}

fn effective_offer_mid_to_track_id(
    offer_sdp: &str,
    offer_proto_mid_to_track_id: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut effective = mid_to_track_id_from_offer_sdp(offer_sdp);
    for (mid, track_id) in offer_proto_mid_to_track_id {
        effective.insert(mid.clone(), track_id.clone());
    }
    effective
}

fn codec_name_from_mime(mime_type: &str) -> Option<String> {
    let normalized = mime_type.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    Some(
        normalized
            .split('/')
            .nth(1)
            .unwrap_or(normalized.as_str())
            .to_string(),
    )
}

fn preferred_codec_mime_for_participant_track(track: &proto::TrackInfo) -> Option<String> {
    let first_codec = track.codecs.first().and_then(|codec| {
        let normalized = codec.mime_type.trim().to_ascii_lowercase();
        (!normalized.is_empty()).then_some(normalized)
    });

    if track.r#type == proto::TrackType::Audio as i32 && !track.disable_red {
        if let Some(first_codec) = first_codec.as_ref() {
            if first_codec != "audio/opus" {
                return Some(first_codec.clone());
            }
        } else {
            let normalized = track.mime_type.trim().to_ascii_lowercase();
            if !normalized.is_empty() && normalized != "audio/opus" {
                return Some(normalized);
            }
        }
        return Some("audio/red".to_string());
    }

    first_codec.or_else(|| {
        let normalized = track.mime_type.trim().to_ascii_lowercase();
        (!normalized.is_empty()).then_some(normalized)
    })
}

fn reorder_section_media_line_payloads_for_preferred_codec(
    section: &mut [String],
    preferred_mime: &str,
) {
    let Some(preferred_codec) = codec_name_from_mime(preferred_mime) else {
        return;
    };

    let Some(media_line_index) = section.iter().position(|line| {
        let line = sdp_line_without_ending(line);
        line.starts_with("m=audio ") || line.starts_with("m=video ")
    }) else {
        return;
    };

    let preferred_payload_types = section
        .iter()
        .filter_map(|line| {
            let line = sdp_line_without_ending(line);
            let value = line.strip_prefix("a=rtpmap:")?;
            let payload_type = value.split_whitespace().next()?;
            let codec = value.split_whitespace().nth(1)?.split('/').next()?;
            (codec.to_ascii_lowercase() == preferred_codec).then_some(payload_type.to_string())
        })
        .collect::<Vec<_>>();

    if preferred_payload_types.is_empty() {
        return;
    }

    let media_line = sdp_line_without_ending(&section[media_line_index]).to_string();
    let ending = sdp_line_ending(&section[media_line_index]);
    let mut parts = media_line
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if parts.len() < 4 {
        return;
    }

    let payloads = parts.drain(3..).collect::<Vec<_>>();
    let preferred = payloads
        .iter()
        .filter(|payload| preferred_payload_types.contains(*payload))
        .cloned()
        .collect::<Vec<_>>();

    if preferred.is_empty() {
        return;
    }

    let mut reordered = preferred;
    reordered.extend(
        payloads
            .into_iter()
            .filter(|payload| !preferred_payload_types.contains(payload)),
    );

    let mut updated_media_line = parts;
    updated_media_line.extend(reordered);
    section[media_line_index] = sdp_line_with_ending(&updated_media_line.join(" "), ending);
}

fn apply_publisher_codec_preferences_to_answer(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    answer_sdp: &str,
    offer_mid_to_track_id: &HashMap<String, String>,
) -> String {
    if offer_mid_to_track_id.is_empty() {
        return answer_sdp.to_string();
    }

    let Ok(participant) = state.rooms.get_participant(room_name, identity) else {
        return answer_sdp.to_string();
    };

    let preferred_by_track_sid = participant
        .tracks
        .iter()
        .filter_map(|track| {
            let preferred = preferred_codec_mime_for_participant_track(track)?;
            Some((track.sid.clone(), preferred))
        })
        .collect::<HashMap<_, _>>();

    if preferred_by_track_sid.is_empty() {
        return answer_sdp.to_string();
    }

    let preferred_by_mid = offer_mid_to_track_id
        .iter()
        .filter_map(|(mid, track_id_or_cid)| {
            let track_sid = state
                .media_track_cids
                .find_track_sid(room_name, identity, track_id_or_cid)
                .or_else(|| {
                    track_id_or_cid
                        .starts_with("TR_")
                        .then(|| track_id_or_cid.clone())
                })?;
            let preferred = preferred_by_track_sid.get(&track_sid)?;
            Some((mid.clone(), preferred.clone()))
        })
        .collect::<HashMap<_, _>>();

    if preferred_by_mid.is_empty() {
        return answer_sdp.to_string();
    }

    let mut rewritten = Vec::new();
    let mut current_section = Vec::new();

    let flush_section = |section: &mut Vec<String>, out: &mut Vec<String>| {
        let mid = section.iter().find_map(|line| {
            sdp_line_without_ending(line)
                .strip_prefix("a=mid:")
                .map(str::to_string)
        });
        if let Some(mid) = mid
            && let Some(preferred) = preferred_by_mid.get(&mid)
        {
            reorder_section_media_line_payloads_for_preferred_codec(section, preferred);
        }
        out.append(section);
    };

    for raw_line in answer_sdp.split_inclusive('\n') {
        if raw_line.starts_with("m=") && !current_section.is_empty() {
            flush_section(&mut current_section, &mut rewritten);
        }
        current_section.push(raw_line.to_string());
    }

    if !current_section.is_empty() {
        flush_section(&mut current_section, &mut rewritten);
    }

    rewritten.concat()
}

fn filter_h264_from_publisher_answer_for_client(
    answer_sdp: &str,
    client_info: &proto::ClientInfo,
) -> String {
    if !participant_should_disable_h264_publish(client_info) {
        return answer_sdp.to_string();
    }

    let mut rewritten = Vec::new();
    let mut current_section = Vec::new();

    for raw_line in answer_sdp.split_inclusive('\n') {
        if raw_line.starts_with("m=") && !current_section.is_empty() {
            filter_h264_from_video_section(&mut current_section, &mut rewritten);
        }
        current_section.push(raw_line.to_string());
    }

    if !current_section.is_empty() {
        filter_h264_from_video_section(&mut current_section, &mut rewritten);
    }

    rewritten.concat()
}

fn filter_h264_from_video_section(section: &mut Vec<String>, rewritten: &mut Vec<String>) {
    let Some(media_line_index) = section
        .iter()
        .position(|line| sdp_line_without_ending(line).starts_with("m=video "))
    else {
        rewritten.append(section);
        return;
    };

    let direction = section.iter().find_map(|line| {
        let line = sdp_line_without_ending(line);
        line.strip_prefix("a=").and_then(|value| {
            matches!(value, "sendrecv" | "sendonly" | "recvonly" | "inactive").then_some(value)
        })
    });

    // H264 disablement for problematic clients should only apply to publish/recv-side
    // answer sections. Keep send-side sections intact so subscribe/downtrack codec
    // advertisement remains available in single-PC mode.
    if matches!(direction, Some("sendonly")) {
        rewritten.append(section);
        return;
    }

    let h264_payload_types = section
        .iter()
        .filter_map(|line| {
            let line = sdp_line_without_ending(line);
            let value = line.strip_prefix("a=rtpmap:")?;
            if !value.to_ascii_lowercase().contains("h264/") {
                return None;
            }
            value.split_whitespace().next().map(str::to_string)
        })
        .collect::<std::collections::HashSet<_>>();

    if h264_payload_types.is_empty() {
        rewritten.append(section);
        return;
    }

    let media_line = sdp_line_without_ending(&section[media_line_index]);
    let ending = sdp_line_ending(&section[media_line_index]);
    let mut parts = media_line.split_whitespace().collect::<Vec<_>>();
    if parts.len() <= 3 {
        rewritten.append(section);
        return;
    }

    let retained_payloads = parts
        .drain(3..)
        .filter(|payload| !h264_payload_types.contains(*payload))
        .collect::<Vec<_>>();
    if retained_payloads.is_empty() {
        rewritten.append(section);
        return;
    }

    let prefix = parts.join(" ");
    section[media_line_index] =
        sdp_line_with_ending(&format!("{prefix} {}", retained_payloads.join(" ")), ending);

    section.retain(|line| {
        let line_without_ending = sdp_line_without_ending(line);
        for payload_type in &h264_payload_types {
            if line_without_ending.starts_with(&format!("a=rtpmap:{payload_type} "))
                || line_without_ending.starts_with(&format!("a=fmtp:{payload_type} "))
                || line_without_ending.starts_with(&format!("a=rtcp-fb:{payload_type} "))
            {
                return false;
            }
        }
        true
    });

    rewritten.append(section);
}

fn supports_auto_recommended_quality_updates(track: &proto::TrackInfo) -> bool {
    #[allow(deprecated)]
    {
        track.simulcast || track.layers.len() > 1
    }
}

struct RecommendedQualityUpdateContext<'a> {
    signal_connections: &'a SignalConnectionStore,
    track_settings: &'a crate::media::TrackSettingsStore,
    rooms: &'a RoomStore,
}

struct RecommendedQualityUpdate<'a> {
    room_name: &'a str,
    publisher_identity: &'a str,
    track_sid: &'a str,
    subscriber_identity: &'a str,
    recommended_quality: crate::media::RecommendedVideoQuality,
}

#[allow(clippy::too_many_arguments, dead_code)]
pub(crate) fn maybe_emit_recommended_subscribed_quality_update(
    signal_connections: &SignalConnectionStore,
    track_settings: &crate::media::TrackSettingsStore,
    rooms: &RoomStore,
    room_name: &str,
    publisher_identity: &str,
    track_sid: &str,
    subscriber_identity: &str,
    recommended_quality: crate::media::RecommendedVideoQuality,
) {
    maybe_emit_recommended_subscribed_quality_update_in(
        &RecommendedQualityUpdateContext {
            signal_connections,
            track_settings,
            rooms,
        },
        RecommendedQualityUpdate {
            room_name,
            publisher_identity,
            track_sid,
            subscriber_identity,
            recommended_quality,
        },
    );
}

fn maybe_emit_recommended_subscribed_quality_update_in(
    context: &RecommendedQualityUpdateContext<'_>,
    update: RecommendedQualityUpdate<'_>,
) {
    let RecommendedQualityUpdate {
        room_name,
        publisher_identity,
        track_sid,
        subscriber_identity,
        recommended_quality,
    } = update;
    if context
        .track_settings
        .get_for_track(room_name, subscriber_identity, track_sid)
        .is_some()
    {
        return;
    }

    let Some(publisher_outbound_tx) = context
        .signal_connections
        .get(room_name, publisher_identity)
    else {
        return;
    };

    #[allow(deprecated)]
    let quality = match recommended_quality {
        crate::media::RecommendedVideoQuality::Low => proto::VideoQuality::Low,
        crate::media::RecommendedVideoQuality::Medium => proto::VideoQuality::Medium,
        crate::media::RecommendedVideoQuality::High => proto::VideoQuality::High,
    };

    let Some(track) = context
        .rooms
        .get_participant(room_name, publisher_identity)
        .ok()
        .and_then(|participant| {
            participant
                .tracks
                .into_iter()
                .find(|track| track.sid == track_sid)
        })
    else {
        tracing::debug!(
            room = %room_name,
            publisher_identity = %publisher_identity,
            subscriber_identity = %subscriber_identity,
            track_sid = %track_sid,
            "skipping_recommended_quality_update_unknown_publisher_track"
        );
        return;
    };

    if !supports_auto_recommended_quality_updates(&track) {
        tracing::debug!(
            room = %room_name,
            publisher_identity = %publisher_identity,
            subscriber_identity = %subscriber_identity,
            track_sid = %track_sid,
            "skipping_recommended_quality_update_non_simulcast_track"
        );
        return;
    }

    let codec_mime_types = crate::media::codec_mime_types_for_track(&track);

    let update = crate::media::subscribed_quality_update_for_track_with_codecs(
        track_sid,
        &codec_mime_types,
        Some(quality),
    );
    let _ = publisher_outbound_tx.send(proto::SignalResponse {
        message: Some(proto::signal_response::Message::SubscribedQualityUpdate(
            update,
        )),
    });
}

pub(crate) async fn handle_media_subscription_request(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    request: proto::UpdateSubscription,
    allow_without_subscribe_permission: bool,
) {
    let track_sids = requested_media_track_sids(&request);
    tracing::debug!(
        room = %room_name,
        subscriber_identity = %subscriber_identity,
        subscribe = request.subscribe,
        track_sids = ?track_sids,
        "media_subscription_request_received"
    );

    for track_sid in track_sids {
        if request.subscribe {
            if !allow_without_subscribe_permission
                && !state
                    .subscribe_permissions
                    .can_subscribe(room_name, subscriber_identity)
            {
                tracing::debug!(
                    room = %room_name,
                    subscriber_identity = %subscriber_identity,
                    track_sid = %track_sid,
                    "media_subscription_subscribe_blocked_by_can_subscribe_permission"
                );
                continue;
            }
            let Some((publisher_identity, track)) =
                find_media_track_publisher(state, room_name, &track_sid)
            else {
                tracing::debug!(
                    room = %room_name,
                    subscriber_identity = %subscriber_identity,
                    track_sid = %track_sid,
                    "media_subscription_subscribe_track_not_found"
                );
                continue;
            };
            tracing::debug!(
                room = %room_name,
                subscriber_identity = %subscriber_identity,
                publisher_identity = %publisher_identity,
                track_sid = %track_sid,
                track_name = %track.name,
                "media_subscription_subscribe_track_resolved"
            );

            if reject_unsupported_video_subscription_if_needed(
                state,
                room_name,
                &publisher_identity,
                subscriber_identity,
                &track,
                true,
            ) {
                continue;
            }

            let prior_tracks_same_logical_stream = state
                .rooms
                .get_participant(room_name, &publisher_identity)
                .map(|publisher| {
                    publisher
                        .tracks
                        .into_iter()
                        .filter(|candidate| {
                            candidate.sid != track.sid
                                && candidate.r#type == track.r#type
                                && candidate.name == track.name
                                && state.media_subscriptions.is_subscribed(
                                    room_name,
                                    &publisher_identity,
                                    &candidate.sid,
                                    subscriber_identity,
                                )
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            for prior_track in prior_tracks_same_logical_stream {
                tracing::debug!(
                    room = %room_name,
                    subscriber_identity = %subscriber_identity,
                    publisher_identity = %publisher_identity,
                    old_track_sid = %prior_track.sid,
                    old_track_name = %prior_track.name,
                    new_track_sid = %track_sid,
                    "media_subscription_auto_unsubscribing_prior_logical_track"
                );
                state.media_subscriptions.set_subscribed(
                    room_name,
                    &publisher_identity,
                    &prior_track.sid,
                    subscriber_identity,
                    false,
                );
                state.track_settings.remove_for_track(
                    room_name,
                    subscriber_identity,
                    &prior_track.sid,
                );
                state.track_allocations.remove_for_track(
                    room_name,
                    subscriber_identity,
                    &prior_track.sid,
                );
                let _ = remove_subscriber_media_forwarding_for_track(
                    state,
                    room_name,
                    &publisher_identity,
                    subscriber_identity,
                    &prior_track,
                )
                .await;
                emit_aggregate_subscribed_quality_update_for_track(
                    state,
                    room_name,
                    &publisher_identity,
                    &prior_track,
                    true,
                );
            }

            state.media_subscriptions.set_subscribed(
                room_name,
                &publisher_identity,
                &track_sid,
                subscriber_identity,
                true,
            );
            let _ = ensure_subscriber_forwarding_for_track(
                state,
                room_name,
                &publisher_identity,
                &track,
            )
            .await;
            emit_aggregate_subscribed_quality_update_for_track(
                state,
                room_name,
                &publisher_identity,
                &track,
                false,
            );
            continue;
        }

        if let Some((publisher_identity, track)) =
            find_media_track_publisher(state, room_name, &track_sid)
        {
            tracing::debug!(
                room = %room_name,
                subscriber_identity = %subscriber_identity,
                publisher_identity = %publisher_identity,
                track_sid = %track_sid,
                track_name = %track.name,
                "media_subscription_unsubscribe_track_resolved"
            );
            state.media_subscriptions.set_subscribed(
                room_name,
                &publisher_identity,
                &track_sid,
                subscriber_identity,
                false,
            );
            state
                .track_settings
                .remove_for_track(room_name, subscriber_identity, &track_sid);
            state
                .track_allocations
                .remove_for_track(room_name, subscriber_identity, &track_sid);
            let _ = remove_subscriber_media_forwarding_for_track(
                state,
                room_name,
                &publisher_identity,
                subscriber_identity,
                &track,
            )
            .await;
            emit_aggregate_subscribed_quality_update_for_track(
                state,
                room_name,
                &publisher_identity,
                &track,
                true,
            );
            continue;
        }

        let participants = state.rooms.list_participants(room_name).unwrap_or_default();
        for participant in participants {
            state.media_subscriptions.set_subscribed(
                room_name,
                &participant.identity,
                &track_sid,
                subscriber_identity,
                false,
            );
            state
                .track_settings
                .remove_for_track(room_name, subscriber_identity, &track_sid);
            state
                .track_allocations
                .remove_for_track(room_name, subscriber_identity, &track_sid);
            state.media_forwarding.remove(
                room_name,
                &participant.identity,
                &track_sid,
                subscriber_identity,
            );
            let _ = state.forward_tracks.remove(
                room_name,
                &participant.identity,
                &track_sid,
                subscriber_identity,
            );
            state.rtp_forwarding.remove(
                room_name,
                &participant.identity,
                &track_sid,
                subscriber_identity,
            );
            clear_publisher_subscription_active_if_no_remaining_tracks(
                state,
                room_name,
                &participant.identity,
                subscriber_identity,
            );
        }
    }
}

pub(super) async fn remove_subscriber_media_forwarding_for_track(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    subscriber_identity: &str,
    track: &proto::TrackInfo,
) -> oxidesfu_rtc::RtcResult<()> {
    remove_subscriber_media_forwarding_for_track_with_negotiation(
        state,
        room_name,
        publisher_identity,
        subscriber_identity,
        track,
        true,
    )
    .await
    .map(|_| ())
}

pub(super) async fn remove_subscriber_media_forwarding_for_track_without_negotiation(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    subscriber_identity: &str,
    track: &proto::TrackInfo,
) -> oxidesfu_rtc::RtcResult<bool> {
    remove_subscriber_media_forwarding_for_track_with_negotiation(
        state,
        room_name,
        publisher_identity,
        subscriber_identity,
        track,
        false,
    )
    .await
}

async fn remove_subscriber_media_forwarding_for_track_with_negotiation(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    subscriber_identity: &str,
    track: &proto::TrackInfo,
    signal_renegotiation: bool,
) -> oxidesfu_rtc::RtcResult<bool> {
    state.media_forwarding.remove(
        room_name,
        publisher_identity,
        &track.sid,
        subscriber_identity,
    );
    let _ = state.rooms.set_media_track_subscribed(
        room_name,
        publisher_identity,
        &track.sid,
        subscriber_identity,
        false,
    );
    clear_publisher_subscription_active_if_no_remaining_tracks(
        state,
        room_name,
        publisher_identity,
        subscriber_identity,
    );

    let Some(forward_track) = state.forward_tracks.remove(
        room_name,
        publisher_identity,
        &track.sid,
        subscriber_identity,
    ) else {
        return Ok(false);
    };
    state.rtp_forwarding.remove(
        room_name,
        publisher_identity,
        &track.sid,
        subscriber_identity,
    );

    let Some((subscriber_pc, connection_kind)) = state
        .peer_connections
        .media_receiver_for_identity(room_name, subscriber_identity)
    else {
        return Ok(false);
    };

    let track_kind = if track.r#type == proto::TrackType::Video as i32 {
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video
    } else {
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio
    };

    if track_kind == rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio {
        match subscriber_pc.debug_transceiver_summary().await {
            Ok(summary) => tracing::debug!(
                room = %room_name,
                publisher_identity = %publisher_identity,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                transceivers = ?summary,
                "single_pc_audio_unsubscribe_transceivers_before_remove"
            ),
            Err(error) => tracing::warn!(
                room = %room_name,
                publisher_identity = %publisher_identity,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                error = %error,
                "single_pc_audio_unsubscribe_transceivers_before_remove_failed"
            ),
        }
    }

    subscriber_pc
        .remove_forwarding_track(&forward_track)
        .await?;

    if track_kind == rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio {
        match subscriber_pc.debug_transceiver_summary().await {
            Ok(summary) => tracing::debug!(
                room = %room_name,
                publisher_identity = %publisher_identity,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                transceivers = ?summary,
                "single_pc_audio_unsubscribe_transceivers_after_remove"
            ),
            Err(error) => tracing::warn!(
                room = %room_name,
                publisher_identity = %publisher_identity,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                error = %error,
                "single_pc_audio_unsubscribe_transceivers_after_remove_failed"
            ),
        }
    }

    if !signal_renegotiation {
        return Ok(true);
    }

    let Some(subscriber_outbound_tx) = state.signal_connections.get(room_name, subscriber_identity)
    else {
        return Ok(true);
    };

    if connection_kind == MediaForwardingConnectionKind::SinglePcPublisher {
        signal_single_pc_sender_removal_negotiation(
            room_name,
            subscriber_identity,
            &subscriber_outbound_tx,
        )
        .await
        .map(|_| true)
    } else {
        signal_media_forwarding_negotiation_with_offer_id(
            state,
            &state.subscriber_offer_ids,
            room_name,
            subscriber_identity,
            &subscriber_pc,
            connection_kind,
            track_kind,
            &subscriber_outbound_tx,
        )
        .await
        .map(|_| true)
    }
}

pub(crate) fn update_data_subscription_response(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    mut request: proto::UpdateDataSubscription,
) -> proto::SignalResponse {
    if !state
        .subscribe_permissions
        .can_subscribe(room_name, identity)
    {
        request.updates.retain(|update| !update.subscribe);
    }

    let handles = state.data_track_subscriptions.update(
        room_name,
        identity,
        request,
        &state.data_tracks,
        &state.rooms,
    );
    proto::SignalResponse {
        message: Some(proto::signal_response::Message::DataTrackSubscriberHandles(
            handles,
        )),
    }
}

pub(crate) fn reconcile_subscriber_data_track_subscriptions(
    state: &SignalState,
    room_name: &str,
    identity: &str,
) {
    if !state.auto_subscribe_data_track_enabled(room_name, identity)
        || !state
            .subscribe_permissions
            .can_subscribe(room_name, identity)
    {
        return;
    }

    let Ok(participants) = state.rooms.list_participants(room_name) else {
        return;
    };

    let updates = participants
        .into_iter()
        .filter(|participant| participant.identity != identity)
        .flat_map(|participant| participant.data_tracks.into_iter())
        .filter(|track| !track.sid.is_empty())
        .map(|track| proto::update_data_subscription::Update {
            track_sid: track.sid,
            subscribe: true,
            options: None,
        })
        .collect::<Vec<_>>();

    if updates.is_empty() {
        return;
    }

    let response = update_data_subscription_response(
        state,
        room_name,
        identity,
        proto::UpdateDataSubscription { updates },
    );

    if let Some(outbound_tx) = state.signal_connections.get(room_name, identity) {
        let _ = outbound_tx.send(response);
    }
}

pub(crate) fn publish_data_track_response(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    request: proto::PublishDataTrackRequest,
) -> proto::SignalResponse {
    if !state
        .publish_permissions
        .can_publish_data(room_name, identity)
    {
        return publish_data_track_request_response(
            proto::request_response::Reason::NotAllowed,
            "does not have permission to publish data",
            request,
        );
    }

    if request.pub_handle == 0 || request.pub_handle > u16::MAX as u32 {
        return publish_data_track_request_response(
            proto::request_response::Reason::InvalidHandle,
            "handle should be > 0 AND < 65536",
            request,
        );
    }

    if request.name.is_empty() || request.name.chars().count() > 256 {
        return publish_data_track_request_response(
            proto::request_response::Reason::InvalidName,
            "name should not be empty and should not exceed 256 characters",
            request,
        );
    }

    match state.data_tracks.publish(room_name, identity, &request) {
        Ok(info) => {
            if let Ok(participant) =
                state
                    .rooms
                    .add_participant_data_track(room_name, identity, info.clone())
            {
                state.updates.broadcast_update(room_name, participant);

                if let Ok(subscribers) = state.rooms.list_participants(room_name) {
                    for subscriber in subscribers {
                        if subscriber.identity == identity {
                            continue;
                        }
                        reconcile_subscriber_data_track_subscriptions(
                            state,
                            room_name,
                            &subscriber.identity,
                        );
                    }
                }
            }
            proto::SignalResponse {
                message: Some(proto::signal_response::Message::PublishDataTrackResponse(
                    proto::PublishDataTrackResponse { info: Some(info) },
                )),
            }
        }
        Err(err) => {
            let (reason, message) = match err {
                DataTrackPublishError::DuplicateHandle => (
                    proto::request_response::Reason::DuplicateHandle,
                    "a data track with same handle already exists",
                ),
                DataTrackPublishError::DuplicateName => (
                    proto::request_response::Reason::DuplicateName,
                    "a data track with same name already exists",
                ),
            };
            publish_data_track_request_response(reason, message, request)
        }
    }
}

pub(crate) fn unpublish_data_track_response(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    request: proto::UnpublishDataTrackRequest,
) -> proto::SignalResponse {
    let Some(info) = state
        .data_tracks
        .unpublish(room_name, identity, request.pub_handle)
    else {
        return proto::SignalResponse {
            message: Some(proto::signal_response::Message::RequestResponse(
                proto::RequestResponse {
                    reason: proto::request_response::Reason::NotFound as i32,
                    request: Some(proto::request_response::Request::UnpublishDataTrack(
                        request,
                    )),
                    ..Default::default()
                },
            )),
        };
    };

    state
        .data_track_subscriptions
        .remove_published_track(room_name, identity, request.pub_handle);

    if let Ok(participant) =
        state
            .rooms
            .remove_participant_data_track(room_name, identity, request.pub_handle)
    {
        state.updates.broadcast_update(room_name, participant);
    }

    proto::SignalResponse {
        message: Some(proto::signal_response::Message::UnpublishDataTrackResponse(
            proto::UnpublishDataTrackResponse { info: Some(info) },
        )),
    }
}

pub(crate) async fn ensure_existing_media_forwarding_for_subscriber(
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
            if let Err(error) = ensure_subscriber_forwarding_for_track(
                state,
                room_name,
                &publisher.identity,
                &track,
            )
            .await
            {
                tracing::warn!(
                    room = %room_name,
                    publisher_identity = %publisher.identity,
                    subscriber_identity = %subscriber_identity,
                    track_sid = %track.sid,
                    error = %error,
                    "failed_to_ensure_existing_media_forwarding_for_subscriber"
                );
            }
        }
    }
}

async fn attach_existing_media_to_subscriber_pc(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    subscriber_pc: &oxidesfu_rtc::PeerConnection,
) -> oxidesfu_rtc::RtcResult<()> {
    let Ok(participants) = state.rooms.list_participants(room_name) else {
        return Ok(());
    };

    for publisher in participants {
        if publisher.identity == subscriber_identity {
            continue;
        }
        for track in publisher.tracks {
            if track.sid.is_empty() {
                continue;
            }
            if reject_unsupported_video_subscription_if_needed(
                state,
                room_name,
                &publisher.identity,
                subscriber_identity,
                &track,
                false,
            ) {
                continue;
            }
            if !state.media_forwarding.insert_once(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
            ) {
                continue;
            }
            let track_kind = if track.r#type == proto::TrackType::Video as i32 {
                rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video
            } else {
                rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio
            };
            let existing_forwarding_count = state
                .forward_tracks
                .list_for_track(room_name, &publisher.identity, &track.sid)
                .len();
            let forwarding_mime = selected_forwarding_mime_type_for_subscriber(
                state,
                room_name,
                subscriber_identity,
                &track,
                existing_forwarding_count,
            );
            let forward_track = subscriber_pc
                .add_forwarding_track_with_mime(
                    &publisher.sid,
                    &track.sid,
                    track_kind,
                    forwarding_mime.as_deref(),
                )
                .await?;
            state.forward_tracks.insert_inactive(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
                forward_track,
            );
        }
    }

    Ok(())
}

async fn attach_receive_section_forwarding_to_single_pc(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    subscriber_pc: &oxidesfu_rtc::PeerConnection,
    mut receive_sections: std::collections::VecDeque<ReceiveSection>,
    prefer_newest_receive_section: bool,
) -> oxidesfu_rtc::RtcResult<std::collections::HashMap<String, String>> {
    if receive_sections.is_empty() {
        tracing::debug!(
            room = %room_name,
            subscriber_identity = %subscriber_identity,
            "single_pc_attach_receive_sections_empty"
        );
        return Ok(std::collections::HashMap::new());
    }

    let initial_receive_sections = receive_sections.iter().cloned().collect::<Vec<_>>();
    tracing::debug!(
        room = %room_name,
        subscriber_identity = %subscriber_identity,
        receive_sections = ?initial_receive_sections,
        prefer_newest_receive_section,
        "single_pc_attach_receive_sections_start"
    );

    let Ok(participants) = state.rooms.list_participants(room_name) else {
        tracing::warn!(
            room = %room_name,
            subscriber_identity = %subscriber_identity,
            "single_pc_attach_receive_sections_list_participants_failed"
        );
        return Ok(std::collections::HashMap::new());
    };
    let offered_audio_only = receive_sections
        .iter()
        .all(|section| section.kind == ReceiveSectionKind::Audio);

    let mut attached_mid_to_track_id = std::collections::HashMap::new();

    for publisher in participants {
        if publisher.identity == subscriber_identity {
            continue;
        }

        let mut publisher_tracks = publisher.tracks;
        publisher_tracks.sort_by(|left, right| {
            let left_existing_mid = state
                .forward_tracks
                .forwarding_mid_for_subscriber_track(
                    room_name,
                    &publisher.identity,
                    &left.sid,
                    subscriber_identity,
                )
                .is_some();
            let right_existing_mid = state
                .forward_tracks
                .forwarding_mid_for_subscriber_track(
                    room_name,
                    &publisher.identity,
                    &right.sid,
                    subscriber_identity,
                )
                .is_some();

            let left_pending = state.pending_media_section_requests.contains(
                room_name,
                &publisher.identity,
                &left.sid,
                subscriber_identity,
            );
            let right_pending = state.pending_media_section_requests.contains(
                room_name,
                &publisher.identity,
                &right.sid,
                subscriber_identity,
            );

            (!left_existing_mid, !left_pending, left.sid.as_str()).cmp(&(
                !right_existing_mid,
                !right_pending,
                right.sid.as_str(),
            ))
        });

        for track in publisher_tracks {
            if track.sid.is_empty() {
                continue;
            }
            let (track_kind, receive_kind) = if track.r#type == proto::TrackType::Video as i32 {
                (
                    rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
                    ReceiveSectionKind::Video,
                )
            } else {
                (
                    rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
                    ReceiveSectionKind::Audio,
                )
            };

            if reject_unsupported_video_subscription_if_needed(
                state,
                room_name,
                &publisher.identity,
                subscriber_identity,
                &track,
                false,
            ) {
                state.pending_media_section_requests.remove(
                    room_name,
                    &publisher.identity,
                    &track.sid,
                    subscriber_identity,
                );
                continue;
            }

            let has_pending_request = state.pending_media_section_requests.contains(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
            );

            let existing_forwarding_mid = state.forward_tracks.forwarding_mid_for_subscriber_track(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
            );

            let already_forwarding = state.media_forwarding.contains(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
            ) || existing_forwarding_mid.is_some();

            let section_index = existing_forwarding_mid
                .as_deref()
                .and_then(|mid| {
                    receive_sections
                        .iter()
                        .position(|section| section.mid == mid && section.kind == receive_kind)
                })
                .or_else(|| {
                    if already_forwarding && !has_pending_request {
                        return None;
                    }
                    if prefer_newest_receive_section {
                        receive_sections
                            .iter()
                            .rposition(|section| section.kind == receive_kind)
                    } else {
                        receive_sections
                            .iter()
                            .position(|section| section.kind == receive_kind)
                    }
                });
            let Some(section_index) = section_index else {
                tracing::debug!(
                    room = %room_name,
                    publisher_identity = %publisher.identity,
                    subscriber_identity = %subscriber_identity,
                    track_sid = %track.sid,
                    track_kind = ?track_kind,
                    receive_kind = ?receive_kind,
                    existing_forwarding_mid = ?existing_forwarding_mid,
                    remaining_receive_sections = ?receive_sections,
                    "single_pc_attach_no_matching_receive_section_for_track"
                );
                continue;
            };

            tracing::debug!(
                room = %room_name,
                publisher_identity = %publisher.identity,
                publisher_sid = %publisher.sid,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                track_type = track.r#type,
                section_index,
                selected_receive_kind = ?receive_kind,
                has_pending_request,
                already_forwarding,
                remaining_receive_sections_before_consume = ?receive_sections,
                "single_pc_attach_track_candidate"
            );

            if !has_pending_request && !already_forwarding {
                tracing::debug!(
                    room = %room_name,
                    publisher_identity = %publisher.identity,
                    subscriber_identity = %subscriber_identity,
                    track_sid = %track.sid,
                    "single_pc_attach_skipping_track_without_pending_or_forwarding"
                );
                continue;
            }

            let section = receive_sections
                .remove(section_index)
                .expect("matched receive section should exist");

            tracing::debug!(
                room = %room_name,
                publisher_identity = %publisher.identity,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                mid = %section.mid,
                section_kind = ?section.kind,
                "single_pc_attach_consumed_receive_section"
            );

            let reclaimed_mid_tracks = state.forward_tracks.remove_subscriber_mid(
                room_name,
                subscriber_identity,
                &section.mid,
            );
            tracing::debug!(
                room = %room_name,
                subscriber_identity = %subscriber_identity,
                mid = %section.mid,
                reclaimed = ?reclaimed_mid_tracks
                    .iter()
                    .map(|(publisher, track_sid, track)| format!(
                        "publisher={publisher} track={track_sid} forwarding_mid={:?}",
                        track.forwarding_mid()
                    ))
                    .collect::<Vec<_>>(),
                "single_pc_attach_reclaimed_mid_tracks"
            );
            let mut reclaimed_current_track = false;
            for (reclaimed_publisher_identity, reclaimed_track_sid, _reclaimed_track) in
                reclaimed_mid_tracks
            {
                let reclaimed_is_current_track = reclaimed_publisher_identity == publisher.identity
                    && reclaimed_track_sid == track.sid;
                if reclaimed_is_current_track {
                    reclaimed_current_track = true;
                }

                let reclaimed_track_info = state
                    .rooms
                    .get_participant(room_name, &reclaimed_publisher_identity)
                    .ok()
                    .and_then(|participant| {
                        participant
                            .tracks
                            .into_iter()
                            .find(|published| published.sid == reclaimed_track_sid)
                    });
                let should_requeue_reclaimed_track =
                    !reclaimed_is_current_track && reclaimed_track_info.is_some();

                state.media_forwarding.remove(
                    room_name,
                    &reclaimed_publisher_identity,
                    &reclaimed_track_sid,
                    subscriber_identity,
                );
                state.pending_media_section_requests.remove(
                    room_name,
                    &reclaimed_publisher_identity,
                    &reclaimed_track_sid,
                    subscriber_identity,
                );
                state.rtp_forwarding.remove(
                    room_name,
                    &reclaimed_publisher_identity,
                    &reclaimed_track_sid,
                    subscriber_identity,
                );

                if should_requeue_reclaimed_track {
                    if let Some(reclaimed_track) = reclaimed_track_info.as_ref() {
                        let pending_kind = pending_media_section_kind_from_track_kind(
                            if reclaimed_track.r#type == proto::TrackType::Video as i32 {
                                rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video
                            } else {
                                rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio
                            },
                        );
                        state.pending_media_section_requests.insert_once(
                            room_name,
                            &reclaimed_publisher_identity,
                            &reclaimed_track_sid,
                            subscriber_identity,
                            pending_kind,
                        );
                    }
                } else {
                    state.media_subscriptions.set_subscribed(
                        room_name,
                        &reclaimed_publisher_identity,
                        &reclaimed_track_sid,
                        subscriber_identity,
                        false,
                    );
                    let _ = state.rooms.set_media_track_subscribed(
                        room_name,
                        &reclaimed_publisher_identity,
                        &reclaimed_track_sid,
                        subscriber_identity,
                        false,
                    );
                }

                tracing::info!(
                    room = %room_name,
                    publisher_identity = %reclaimed_publisher_identity,
                    subscriber_identity = %subscriber_identity,
                    track_sid = %reclaimed_track_sid,
                    mid = %section.mid,
                    requeued = should_requeue_reclaimed_track,
                    "single_pc_forward_track_mid_reclaimed"
                );
            }

            let already_forwarding = if reclaimed_current_track {
                false
            } else {
                already_forwarding
            };

            if already_forwarding && !has_pending_request {
                tracing::debug!(
                    room = %room_name,
                    publisher_identity = %publisher.identity,
                    subscriber_identity = %subscriber_identity,
                    track_sid = %track.sid,
                    mid = %section.mid,
                    "single_pc_forward_track_reused_for_existing_attachment"
                );
                attached_mid_to_track_id.insert(section.mid, track.sid.clone());
                continue;
            }

            if !state.media_forwarding.insert_once(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
            ) {
                tracing::debug!(
                    room = %room_name,
                    publisher_identity = %publisher.identity,
                    subscriber_identity = %subscriber_identity,
                    track_sid = %track.sid,
                    mid = %section.mid,
                    "single_pc_forward_track_already_attached"
                );
                state.pending_media_section_requests.remove(
                    room_name,
                    &publisher.identity,
                    &track.sid,
                    subscriber_identity,
                );
                attached_mid_to_track_id.insert(section.mid, track.sid.clone());
                continue;
            }
            state.media_subscriptions.set_subscribed(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
                true,
            );
            let _ = state.rooms.set_media_track_subscribed(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
                true,
            );
            let existing_forwarding_count = state
                .forward_tracks
                .list_for_track(room_name, &publisher.identity, &track.sid)
                .len();
            let forwarding_mime = selected_forwarding_mime_type_for_subscriber(
                state,
                room_name,
                subscriber_identity,
                &track,
                existing_forwarding_count,
            );
            let forward_track = subscriber_pc
                .add_forwarding_track_to_mid_with_mime(
                    &section.mid,
                    &publisher.sid,
                    &track.sid,
                    track_kind,
                    forwarding_mime.as_deref(),
                )
                .await?;
            tracing::info!(
                room = %room_name,
                publisher_identity = %publisher.identity,
                publisher_sid = %publisher.sid,
                subscriber_identity = %subscriber_identity,
                mid = %section.mid,
                track_sid = %track.sid,
                "single_pc_forward_track_attached"
            );
            state.forward_tracks.insert_inactive(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
                forward_track,
            );
            state.pending_media_section_requests.remove(
                room_name,
                &publisher.identity,
                &track.sid,
                subscriber_identity,
            );
            attached_mid_to_track_id.insert(section.mid, track.sid.clone());
        }
    }

    if offered_audio_only {
        let mut video_track_states = Vec::new();
        if let Ok(participants) = state.rooms.list_participants(room_name) {
            for publisher in participants {
                if publisher.identity == subscriber_identity {
                    continue;
                }
                for track in publisher.tracks {
                    if track.r#type != proto::TrackType::Video as i32 || track.sid.is_empty() {
                        continue;
                    }
                    video_track_states.push(format!(
                        "publisher={} track_sid={} media_forwarding={} signaling_subscribed={} room_subscribed={}",
                        publisher.identity,
                        track.sid,
                        state.media_forwarding.contains(
                            room_name,
                            &publisher.identity,
                            &track.sid,
                            subscriber_identity,
                        ),
                        state.media_subscriptions.is_subscribed(
                            room_name,
                            &publisher.identity,
                            &track.sid,
                            subscriber_identity,
                        ),
                        state.rooms.is_media_track_subscribed(
                            room_name,
                            &publisher.identity,
                            &track.sid,
                            subscriber_identity,
                        ),
                    ));
                }
            }
        }
        tracing::debug!(
            room = %room_name,
            subscriber_identity = %subscriber_identity,
            attached_mid_to_track_id = ?attached_mid_to_track_id,
            video_track_states = ?video_track_states,
            "single_pc_audio_only_offer_video_forwarding_state"
        );
    }

    tracing::debug!(
        room = %room_name,
        subscriber_identity = %subscriber_identity,
        attached_mid_to_track_id = ?attached_mid_to_track_id,
        leftover_receive_sections = ?receive_sections,
        "single_pc_attach_receive_sections_complete"
    );

    Ok(attached_mid_to_track_id)
}

pub(crate) fn classify_single_pc_offer_sections(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    offer_sdp: &str,
    offer_proto_mid_to_track_id: &std::collections::HashMap<String, String>,
) -> (
    std::collections::HashSet<String>,
    std::collections::VecDeque<ReceiveSection>,
) {
    let sections = offer_media_sections_from_sdp(offer_sdp);

    let mut remote_track_sids = std::collections::HashSet::new();
    if let Ok(participants) = state.rooms.list_participants(room_name) {
        for participant in participants {
            if participant.identity == identity {
                continue;
            }
            for track in participant.tracks {
                if !track.sid.is_empty() {
                    remote_track_sids.insert(track.sid);
                }
            }
        }
    }

    let mut publisher_mids = active_publisher_mids_from_offer(offer_sdp);
    let mut mids_referencing_remote_tracks = std::collections::HashSet::new();
    for (mid, track_id) in offer_proto_mid_to_track_id {
        let resolves_local_cid = state
            .media_track_cids
            .find_track_sid(room_name, identity, track_id)
            .is_some();
        let references_remote_track_sid = remote_track_sids.contains(track_id);

        if references_remote_track_sid {
            mids_referencing_remote_tracks.insert(mid.clone());
            publisher_mids.remove(mid);
            continue;
        }

        if resolves_local_cid {
            publisher_mids.insert(mid.clone());
        }
    }

    let receive_sections = sections
        .into_iter()
        .filter(|section| !section.is_rejected)
        .filter(|section| !publisher_mids.contains(&section.mid))
        .filter_map(|section| {
            let can_receive = matches!(section.direction.as_str(), "recvonly")
                || (matches!(section.direction.as_str(), "sendrecv")
                    && (!section.has_msid
                        || mids_referencing_remote_tracks.contains(&section.mid)));
            if !can_receive {
                return None;
            }
            section.kind.map(|kind| ReceiveSection {
                mid: section.mid,
                kind,
            })
        })
        .collect::<std::collections::VecDeque<_>>();

    tracing::info!(
        room = %room_name,
        identity = %identity,
        publisher_mids = ?publisher_mids,
        receive_sections = ?receive_sections,
        "single_pc_offer_section_classification"
    );

    (publisher_mids, receive_sections)
}

pub(crate) async fn create_subscriber_offer(
    state: &SignalState,
    room_name: &str,
    identity: &str,
    outbound_tx: &OutboundSignalSender,
    rtc_transport_config: &oxidesfu_rtc::RtcTransportConfig,
) -> Result<proto::SignalResponse, prost::DecodeError> {
    let data_channel_block_write = state.datachannel_slow_threshold_bytes().is_some();
    let (peer_connection, events) =
        oxidesfu_rtc::create_peer_connection_with_events_with_transport_and_data_channel_block_write(
            rtc_transport_config,
            data_channel_block_write,
        )
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let reliable = peer_connection
        .create_data_channel("_reliable")
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let lossy = peer_connection
        .create_data_channel_with_options(
            "_lossy",
            oxidesfu_rtc::DataChannelOptions {
                ordered: false,
                max_retransmits: Some(0),
            },
        )
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    if let Some(threshold) = state.datachannel_slow_threshold_bytes() {
        reliable.set_slow_reader_bitrate_threshold_bps(threshold);
        let _ = reliable
            .set_buffered_amount_low_threshold(threshold.saturating_div(2))
            .await;
        let _ = reliable.set_buffered_amount_high_threshold(threshold).await;
    }

    let data_track = peer_connection
        .create_data_channel_with_options(
            "_data_track",
            oxidesfu_rtc::DataChannelOptions {
                ordered: false,
                max_retransmits: Some(0),
            },
        )
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    state.data_channels.insert_with_kind_for_target(
        room_name,
        identity,
        oxidesfu_rtc::DataChannelTransportTarget::Subscriber,
        DataChannelKind::Reliable,
        reliable,
    );
    state.data_channels.insert_with_kind_for_target(
        room_name,
        identity,
        oxidesfu_rtc::DataChannelTransportTarget::Subscriber,
        DataChannelKind::Lossy,
        lossy,
    );
    let data_track_for_ready = data_track.clone();
    state.data_channels.insert_with_kind_for_target(
        room_name,
        identity,
        oxidesfu_rtc::DataChannelTransportTarget::Subscriber,
        DataChannelKind::DataTrack,
        data_track,
    );

    attach_existing_media_to_subscriber_pc(state, room_name, identity, &peer_connection)
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let sdp = peer_connection
        .create_offer()
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let offer_id = state
        .subscriber_offer_ids
        .next_offer_id(room_name, identity);
    state.remember_subscriber_offer_mid_track_ids(
        room_name,
        identity,
        offer_id,
        crate::media::mid_to_track_id_from_offer_sdp(&sdp),
    );
    state
        .subscriber_offer_negotiations
        .mark_offer_in_flight(room_name, identity, offer_id);
    tracing::debug!(
        room = %room_name,
        identity = %identity,
        offer_id,
        sdp = %sdp,
        "subscriber_initial_offer_created"
    );
    forward_peer_connection_events(
        events,
        PeerConnectionEventForwardingContext {
            outbound_tx: outbound_tx.clone(),
            data_messages: state.data_messages.clone(),
            data_channels: state.data_channels.clone(),
            data_track_subscriptions: state.data_track_subscriptions.clone(),
            peer_connections: state.peer_connections.clone(),
            rooms: state.rooms.clone(),
            media_forwarding: state.media_forwarding.clone(),
            pending_media_section_requests: state.pending_media_section_requests.clone(),
            media_subscriptions: state.media_subscriptions.clone(),
            subscribe_permissions: state.subscribe_permissions.clone(),
            auto_subscribe_preferences: state.auto_subscribe_preferences.clone(),
            track_settings: state.track_settings.clone(),
            track_allocations: state.track_allocations.clone(),
            pending_remote_tracks: state.pending_remote_tracks.clone(),
            forward_tracks: state.forward_tracks.clone(),
            rtp_forwarding: state.rtp_forwarding.clone(),
            signal_connections: state.signal_connections.clone(),
            media_track_cids: state.media_track_cids.clone(),
            publish_permissions: state.publish_permissions.clone(),
            state: state.clone(),
            room_name: room_name.to_string(),
            identity: identity.to_string(),
            target: SignalConnectionTarget::Subscriber,
        },
    );
    state.peer_connections.insert(
        room_name,
        identity,
        SignalConnectionTarget::Subscriber,
        peer_connection,
    );

    // Reconcile immediately after subscriber signal wiring so late joiners
    // receive existing published data-track handles without waiting for
    // `_data_track` channel open timing.
    reconcile_subscriber_data_track_subscriptions(state, room_name, identity);

    let state_for_data_track_reconcile = state.clone();
    let room_name_for_data_track_reconcile = room_name.to_string();
    let identity_for_data_track_reconcile = identity.to_string();
    tokio::spawn(async move {
        if data_track_for_ready.wait_open().await.is_ok() {
            reconcile_subscriber_data_track_subscriptions(
                &state_for_data_track_reconcile,
                &room_name_for_data_track_reconcile,
                &identity_for_data_track_reconcile,
            );
        }
    });

    Ok(proto::SignalResponse {
        message: Some(proto::signal_response::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp,
                id: offer_id,
                ..Default::default()
            },
        )),
    })
}

fn single_pc_offer_media_kinds(
    offer_sdp: &str,
) -> HashMap<String, crate::media::ReceiveSectionKind> {
    offer_media_sections_from_sdp(offer_sdp)
        .into_iter()
        .filter_map(|section| section.kind.map(|kind| (section.mid, kind)))
        .collect()
}

async fn reclaim_single_pc_forwarding_on_publisher_mids(
    state: &SignalState,
    room_name: &str,
    subscriber_identity: &str,
    subscriber_pc: &oxidesfu_rtc::PeerConnection,
    publisher_mids: &HashSet<String>,
) -> oxidesfu_rtc::RtcResult<()> {
    for publisher_mid in publisher_mids {
        let reclaimed_tracks = state.forward_tracks.remove_subscriber_mid(
            room_name,
            subscriber_identity,
            publisher_mid,
        );

        for (publisher_identity, track_sid, forward_track) in reclaimed_tracks {
            state.media_forwarding.remove(
                room_name,
                &publisher_identity,
                &track_sid,
                subscriber_identity,
            );
            state.pending_media_section_requests.remove(
                room_name,
                &publisher_identity,
                &track_sid,
                subscriber_identity,
            );
            state.rtp_forwarding.remove(
                room_name,
                &publisher_identity,
                &track_sid,
                subscriber_identity,
            );

            subscriber_pc
                .remove_forwarding_track(&forward_track)
                .await?;

            let track = state
                .rooms
                .get_participant(room_name, &publisher_identity)
                .ok()
                .and_then(|participant| {
                    participant
                        .tracks
                        .into_iter()
                        .find(|track| track.sid == track_sid)
                });
            let Some(track) = track else {
                continue;
            };

            let track_kind = if track.r#type == proto::TrackType::Video as i32 {
                rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video
            } else {
                rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio
            };
            state.pending_media_section_requests.insert_once(
                room_name,
                &publisher_identity,
                &track_sid,
                subscriber_identity,
                pending_media_section_kind_from_track_kind(track_kind),
            );
            tracing::info!(
                room = %room_name,
                subscriber_identity,
                publisher_identity,
                track_sid,
                publisher_mid,
                "single_pc_forwarding_reclaimed_for_local_publisher_mid"
            );
        }
    }

    Ok(())
}

pub(crate) async fn answer_publisher_offer(
    offer: proto::SessionDescription,
    state: &SignalState,
    room_name: &str,
    identity: &str,
    outbound_tx: &OutboundSignalSender,
    rtc_transport_config: &oxidesfu_rtc::RtcTransportConfig,
) -> Result<proto::SignalResponse, prost::DecodeError> {
    let offer_id = offer.id;
    // v0 clients use a separate server-offered subscriber transport. Only v1
    // single-PC clients add receive sections to their publisher offer and can
    // act on MediaSectionsRequirement.
    let single_pc_mode = !state.participant_uses_subscriber_primary(room_name, identity);
    let offer_proto_mid_to_track_id = offer.mid_to_track_id.clone();
    let offer_mid_to_track_id_for_codec_preferences =
        effective_offer_mid_to_track_id(&offer.sdp, &offer_proto_mid_to_track_id);
    let offered_media_kinds = single_pc_offer_media_kinds(&offer.sdp);
    let offer_has_sctp = offer
        .sdp
        .lines()
        .any(|line| line.trim_start().starts_with("m=application "));
    tracing::debug!(
        room = %room_name,
        identity = %identity,
        offer_id,
        single_pc_mode,
        offer_has_sctp,
        offer_sdp_len = offer.sdp.len(),
        "publisher_offer_received"
    );
    state.remember_participant_subscribe_video_mime_types(
        room_name,
        identity,
        &receive_supported_video_mime_types_from_offer(&offer.sdp),
    );

    if single_pc_mode
        && !state
            .single_pc_offer_media_kinds
            .is_compatible_with_previous(room_name, identity, &offered_media_kinds)
    {
        tracing::info!(
            room = %room_name,
            identity = %identity,
            offer_id,
            offered_media_kinds = ?offered_media_kinds,
            "single_pc_offer_mid_kind_changed_rebuilding_publisher_pc"
        );
        state.data_channels.remove(room_name, identity);
        if let Some(peer_connection) =
            state
                .peer_connections
                .remove(room_name, identity, SignalConnectionTarget::Publisher)
        {
            let _ = peer_connection.close().await;
        }
    }

    if let Some(existing_peer_connection) =
        state
            .peer_connections
            .get(room_name, identity, SignalConnectionTarget::Publisher)
    {
        let offer_sdp = offer.sdp;
        existing_peer_connection
            .set_remote_offer(offer_sdp.clone())
            .await
            .map_err(|err| prost::DecodeError::new(err.to_string()))?;
        let (publisher_mids, receive_sections) = if single_pc_mode {
            classify_single_pc_offer_sections(
                state,
                room_name,
                identity,
                &offer_sdp,
                &offer_proto_mid_to_track_id,
            )
        } else {
            (Default::default(), Default::default())
        };
        tracing::debug!(
            room = %room_name,
            identity = %identity,
            offer_id,
            offer_mid_to_track_id = ?offer_proto_mid_to_track_id,
            publisher_mids = ?publisher_mids,
            receive_sections = ?receive_sections,
            "single_pc_existing_pc_offer_classified"
        );
        let offer_uses_ice_trickle = offer_advertises_ice_trickle(&offer_sdp);
        reclaim_single_pc_forwarding_on_publisher_mids(
            state,
            room_name,
            identity,
            &existing_peer_connection,
            &publisher_mids,
        )
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
        let receive_mids = receive_sections
            .iter()
            .map(|section| section.mid.clone())
            .collect::<Vec<_>>();
        existing_peer_connection
            .set_transceivers_recvonly_by_mid(publisher_mids.iter().map(String::as_str))
            .await
            .map_err(|err| prost::DecodeError::new(err.to_string()))?;
        let attached_mid_to_track_id = attach_receive_section_forwarding_to_single_pc(
            state,
            room_name,
            identity,
            &existing_peer_connection,
            receive_sections,
            !offer_uses_ice_trickle,
        )
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
        let mut sdp = existing_peer_connection
            .create_answer()
            .await
            .map_err(|err| prost::DecodeError::new(err.to_string()))?;
        let attached_mids = attached_mid_to_track_id
            .keys()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        let unattached_receive_mids = receive_mids
            .iter()
            .map(String::as_str)
            .filter(|mid| !attached_mid_to_track_id.contains_key(*mid))
            .collect::<std::collections::HashSet<_>>();
        sdp = force_answer_mids_sendonly(&sdp, &attached_mids);
        sdp = force_answer_mids_inactive(&sdp, &unattached_receive_mids);
        sdp = force_sendonly_sections_without_msid_recvonly(&sdp, &attached_mids);
        sdp = normalize_answer_direction_order(&sdp);
        if let Some(client_info) = state.participant_client_info(room_name, identity) {
            sdp = filter_h264_from_publisher_answer_for_client(&sdp, &client_info);
        }
        sdp = apply_publisher_codec_preferences_to_answer(
            state,
            room_name,
            identity,
            &sdp,
            &offer_mid_to_track_id_for_codec_preferences,
        );
        let answer_mid_to_track_id = if attached_mid_to_track_id.is_empty() {
            mid_to_track_id_from_answer_sdp(&sdp)
        } else {
            attached_mid_to_track_id
        };
        let offer_mid_to_track_id = mid_to_track_id_from_offer_sdp(&offer_sdp);
        let reconcile_mid_to_track_id = if offer_mid_to_track_id.is_empty() {
            answer_mid_to_track_id.clone()
        } else {
            offer_mid_to_track_id.clone()
        };
        reconcile_publisher_media_tracks_after_answer(
            state,
            room_name,
            identity,
            &offer_sdp,
            &reconcile_mid_to_track_id,
            &offer_proto_mid_to_track_id,
            single_pc_mode,
        )
        .await;
        ensure_existing_media_forwarding_for_subscriber(state, room_name, identity).await;
        schedule_pending_media_section_requirement_after_answer(
            state.clone(),
            room_name.to_string(),
            identity.to_string(),
        );
        state
            .pending_media_section_requests
            .clear_negotiation_if_no_pending(room_name, identity);
        retry_pending_remote_tracks_after_track_published(state, room_name, identity);
        state
            .single_pc_offer_media_kinds
            .set(room_name, identity, offered_media_kinds);
        let rejected_track_sids = activate_tracks_with_compatible_bind_results(
            state,
            room_name,
            identity,
            &answer_mid_to_track_id
                .values()
                .cloned()
                .collect::<HashSet<_>>(),
        )
        .await;
        let rejected_mids = answer_mid_to_track_id
            .iter()
            .filter_map(|(mid, track_sid)| {
                rejected_track_sids
                    .contains(track_sid)
                    .then_some(mid.as_str())
            })
            .collect::<HashSet<_>>();
        sdp = force_answer_mids_inactive(&sdp, &rejected_mids);
        tracing::debug!(
            room = %room_name,
            identity = %identity,
            offer_id,
            offer_has_sctp,
            answer_has_sctp = sdp.lines().any(|line| line.trim_start().starts_with("m=application ")),
            answer_sdp = %sdp,
            "publisher_existing_pc_answer_created"
        );
        tracing::info!(
            room = %room_name,
            identity = %identity,
            offer_id,
            answer_mid_to_track_id = ?answer_mid_to_track_id,
            "single_pc_answer_mid_to_track_id"
        );
        return Ok(proto::SignalResponse {
            message: Some(proto::signal_response::Message::Answer(
                proto::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp,
                    id: offer_id,
                    mid_to_track_id: answer_mid_to_track_id,
                },
            )),
        });
    }

    let data_channel_block_write = state.datachannel_slow_threshold_bytes().is_some();
    let (peer_connection, events) =
        oxidesfu_rtc::create_peer_connection_with_events_with_transport_and_data_channel_block_write(
            rtc_transport_config,
            data_channel_block_write,
        )
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let offer_sdp = offer.sdp;
    peer_connection
        .set_remote_offer(offer_sdp.clone())
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let (publisher_mids, receive_sections) = if single_pc_mode {
        classify_single_pc_offer_sections(
            state,
            room_name,
            identity,
            &offer_sdp,
            &offer_proto_mid_to_track_id,
        )
    } else {
        (Default::default(), Default::default())
    };
    tracing::debug!(
        room = %room_name,
        identity = %identity,
        offer_id,
        offer_mid_to_track_id = ?offer_proto_mid_to_track_id,
        publisher_mids = ?publisher_mids,
        receive_sections = ?receive_sections,
        "single_pc_new_pc_offer_classified"
    );
    let offer_uses_ice_trickle = offer_advertises_ice_trickle(&offer_sdp);
    reclaim_single_pc_forwarding_on_publisher_mids(
        state,
        room_name,
        identity,
        &peer_connection,
        &publisher_mids,
    )
    .await
    .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let receive_mids = receive_sections
        .iter()
        .map(|section| section.mid.clone())
        .collect::<Vec<_>>();
    peer_connection
        .set_transceivers_recvonly_by_mid(publisher_mids.iter().map(String::as_str))
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let attached_mid_to_track_id = attach_receive_section_forwarding_to_single_pc(
        state,
        room_name,
        identity,
        &peer_connection,
        receive_sections,
        !offer_uses_ice_trickle,
    )
    .await
    .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let mut sdp = peer_connection
        .create_answer()
        .await
        .map_err(|err| prost::DecodeError::new(err.to_string()))?;
    let attached_mids = attached_mid_to_track_id
        .keys()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let unattached_receive_mids = receive_mids
        .iter()
        .map(String::as_str)
        .filter(|mid| !attached_mid_to_track_id.contains_key(*mid))
        .collect::<std::collections::HashSet<_>>();
    sdp = force_answer_mids_sendonly(&sdp, &attached_mids);
    sdp = force_answer_mids_inactive(&sdp, &unattached_receive_mids);
    sdp = force_sendonly_sections_without_msid_recvonly(&sdp, &attached_mids);
    sdp = normalize_answer_direction_order(&sdp);
    if let Some(client_info) = state.participant_client_info(room_name, identity) {
        sdp = filter_h264_from_publisher_answer_for_client(&sdp, &client_info);
    }
    sdp = apply_publisher_codec_preferences_to_answer(
        state,
        room_name,
        identity,
        &sdp,
        &offer_mid_to_track_id_for_codec_preferences,
    );
    forward_peer_connection_events(
        events,
        PeerConnectionEventForwardingContext {
            outbound_tx: outbound_tx.clone(),
            data_messages: state.data_messages.clone(),
            data_channels: state.data_channels.clone(),
            data_track_subscriptions: state.data_track_subscriptions.clone(),
            peer_connections: state.peer_connections.clone(),
            rooms: state.rooms.clone(),
            media_forwarding: state.media_forwarding.clone(),
            pending_media_section_requests: state.pending_media_section_requests.clone(),
            media_subscriptions: state.media_subscriptions.clone(),
            subscribe_permissions: state.subscribe_permissions.clone(),
            auto_subscribe_preferences: state.auto_subscribe_preferences.clone(),
            track_settings: state.track_settings.clone(),
            track_allocations: state.track_allocations.clone(),
            pending_remote_tracks: state.pending_remote_tracks.clone(),
            forward_tracks: state.forward_tracks.clone(),
            rtp_forwarding: state.rtp_forwarding.clone(),
            signal_connections: state.signal_connections.clone(),
            media_track_cids: state.media_track_cids.clone(),
            publish_permissions: state.publish_permissions.clone(),
            state: state.clone(),
            room_name: room_name.to_string(),
            identity: identity.to_string(),
            target: SignalConnectionTarget::Publisher,
        },
    );
    state.peer_connections.insert(
        room_name,
        identity,
        SignalConnectionTarget::Publisher,
        peer_connection,
    );

    let answer_mid_to_track_id = if attached_mid_to_track_id.is_empty() {
        mid_to_track_id_from_answer_sdp(&sdp)
    } else {
        attached_mid_to_track_id
    };
    let offer_mid_to_track_id = mid_to_track_id_from_offer_sdp(&offer_sdp);
    let reconcile_mid_to_track_id = if offer_mid_to_track_id.is_empty() {
        answer_mid_to_track_id.clone()
    } else {
        offer_mid_to_track_id.clone()
    };
    reconcile_publisher_media_tracks_after_answer(
        state,
        room_name,
        identity,
        &offer_sdp,
        &reconcile_mid_to_track_id,
        &offer_proto_mid_to_track_id,
        single_pc_mode,
    )
    .await;
    ensure_existing_media_forwarding_for_subscriber(state, room_name, identity).await;
    schedule_pending_media_section_requirement_after_answer(
        state.clone(),
        room_name.to_string(),
        identity.to_string(),
    );
    state
        .pending_media_section_requests
        .clear_negotiation_if_no_pending(room_name, identity);
    retry_pending_remote_tracks_after_track_published(state, room_name, identity);
    state
        .single_pc_offer_media_kinds
        .set(room_name, identity, offered_media_kinds);
    let rejected_track_sids = activate_tracks_with_compatible_bind_results(
        state,
        room_name,
        identity,
        &answer_mid_to_track_id
            .values()
            .cloned()
            .collect::<HashSet<_>>(),
    )
    .await;
    let rejected_mids = answer_mid_to_track_id
        .iter()
        .filter_map(|(mid, track_sid)| {
            rejected_track_sids
                .contains(track_sid)
                .then_some(mid.as_str())
        })
        .collect::<HashSet<_>>();
    sdp = force_answer_mids_inactive(&sdp, &rejected_mids);
    tracing::debug!(
        room = %room_name,
        identity = %identity,
        offer_id,
        offer_has_sctp,
        answer_has_sctp = sdp.lines().any(|line| line.trim_start().starts_with("m=application ")),
        answer_sdp = %sdp,
        "publisher_new_pc_answer_created"
    );
    tracing::info!(
        room = %room_name,
        identity = %identity,
        offer_id,
        answer_mid_to_track_id = ?answer_mid_to_track_id,
        "single_pc_answer_mid_to_track_id"
    );
    Ok(proto::SignalResponse {
        message: Some(proto::signal_response::Message::Answer(
            proto::SessionDescription {
                r#type: "answer".to_string(),
                sdp,
                id: offer_id,
                mid_to_track_id: answer_mid_to_track_id,
            },
        )),
    })
}

#[allow(dead_code)]
struct PeerConnectionEventForwardingContext {
    state: SignalState,
    outbound_tx: OutboundSignalSender,
    data_messages: DataChannelMessageStore,
    data_channels: DataChannelStore,
    data_track_subscriptions: DataTrackSubscriptionStore,
    peer_connections: PeerConnectionStore,
    publish_permissions: crate::stores::PublishPermissionStore,
    rooms: RoomStore,
    media_forwarding: MediaForwardingStore,
    pending_media_section_requests: PendingMediaSectionRequestStore,
    media_subscriptions: MediaSubscriptionStore,
    subscribe_permissions: crate::stores::SubscribePermissionStore,
    auto_subscribe_preferences: crate::stores::AutoSubscribePreferenceStore,
    media_track_cids: crate::stores::MediaTrackCidStore,
    pending_remote_tracks: crate::stores::PendingPublisherRemoteTrackStore,
    track_settings: crate::media::TrackSettingsStore,
    track_allocations: crate::media::TrackAllocationStore,
    forward_tracks: ForwardTrackStore,
    rtp_forwarding: RtpForwardingStore,
    signal_connections: SignalConnectionStore,
    room_name: String,
    identity: String,
    target: SignalConnectionTarget,
}

fn data_channel_kind_for_label(label: &str) -> Option<DataChannelKind> {
    match label {
        "data" | "_reliable" | "pubraw" | "subraw" => Some(DataChannelKind::Reliable),
        "_lossy" => Some(DataChannelKind::Lossy),
        "_data_track" => Some(DataChannelKind::DataTrack),
        _ => None,
    }
}

fn reliable_channel_label_rank(label: &str) -> u8 {
    match label {
        "_reliable" | "data" => 2,
        "pubraw" | "subraw" => 1,
        _ => 0,
    }
}

fn forward_peer_connection_events(
    mut events: oxidesfu_rtc::PeerConnectionEvents,
    context: PeerConnectionEventForwardingContext,
) {
    tokio::spawn(async move {
        let PeerConnectionEventForwardingContext {
            state,
            outbound_tx,
            data_messages,
            data_channels,
            data_track_subscriptions,
            peer_connections: _,
            publish_permissions,
            rooms,
            media_forwarding: _,
            pending_media_section_requests: _,
            media_subscriptions: _,
            subscribe_permissions: _,
            auto_subscribe_preferences: _,
            media_track_cids: _,
            pending_remote_tracks: _,
            track_settings: _,
            track_allocations: _,
            forward_tracks: _,
            rtp_forwarding: _,
            signal_connections: _,
            room_name,
            identity,
            target,
        } = context;
        loop {
            tokio::select! {
                candidate = events.ice_candidates.recv() => {
                    let Some(candidate) = candidate else {
                        break;
                    };
                    tracing::debug!(
                        room = %room_name,
                        identity = %identity,
                        target = ?target,
                        is_final = candidate.is_final,
                        candidate_len = candidate.candidate_init_json.len(),
                        "signal_trickle_sending"
                    );
                    let response = proto::SignalResponse {
                        message: Some(proto::signal_response::Message::Trickle(
                            proto::TrickleRequest {
                                candidate_init: candidate.candidate_init_json,
                                target: target.as_proto(),
                                r#final: candidate.is_final,
                            },
                        )),
                    };
                    if outbound_tx.send(response).is_err() {
                        break;
                    }
                }
                remote_track = events.remote_tracks.recv() => {
                    let Some(remote_track) = remote_track else {
                        break;
                    };
                    if target != SignalConnectionTarget::Publisher {
                        continue;
                    }
                    let state = state.clone();
                    let room_name = room_name.clone();
                    let publisher_identity = identity.clone();
                    let Ok(publisher) = state.rooms.get_participant(&room_name, &publisher_identity)
                    else {
                        continue;
                    };
                    let publisher_sid = publisher.sid;
                    tokio::spawn(async move {
                        if let Err(error) = forward_publisher_remote_track(
                            state,
                            PublisherRemoteTrackEvent {
                                remote_track,
                                remote_mid: None,
                                room_name: room_name.clone(),
                                publisher_identity: publisher_identity.clone(),
                                publisher_sid,
                            },
                        )
                        .await
                        {
                            tracing::warn!(
                                room = %room_name,
                                publisher_identity = %publisher_identity,
                                error = %error,
                                "failed_to_forward_publisher_remote_track"
                            );
                        }
                    });
                }
                data_channel = events.data_channels.recv() => {
                    let Some(data_channel) = data_channel else {
                        break;
                    };
                    let label = data_channel.label().await.unwrap_or_default();
                    let channel_kind = data_channel_kind_for_label(label.as_str());
                    tracing::debug!(
                        room = %room_name,
                        identity = %identity,
                        target = ?target,
                        label = %label,
                        channel_kind = ?channel_kind,
                        "peer_connection_data_channel_received"
                    );
                    if let Some(kind) = channel_kind {
                        if kind == DataChannelKind::Reliable
                            && let Some(threshold) = state.datachannel_slow_threshold_bytes()
                        {
                            data_channel.set_slow_reader_bitrate_threshold_bps(threshold);
                            let _ = data_channel
                                .set_buffered_amount_low_threshold(threshold.saturating_div(2))
                                .await;
                            let _ = data_channel
                                .set_buffered_amount_high_threshold(threshold)
                                .await;
                        }

                        let transport_target = match target {
                            SignalConnectionTarget::Publisher => {
                                oxidesfu_rtc::DataChannelTransportTarget::Publisher
                            }
                            SignalConnectionTarget::Subscriber => {
                                oxidesfu_rtc::DataChannelTransportTarget::Subscriber
                            }
                        };
                        let should_insert = if kind == DataChannelKind::Reliable {
                            if let Some(existing) = data_channels.get_with_kind_for_target(
                                &room_name,
                                &identity,
                                transport_target,
                                DataChannelKind::Reliable,
                            ) {
                                let existing_label = existing.label().await.unwrap_or_default();
                                reliable_channel_label_rank(&label)
                                    >= reliable_channel_label_rank(&existing_label)
                            } else {
                                true
                            }
                        } else {
                            true
                        };

                        if should_insert {
                            data_channels.insert_with_kind_for_target(
                                &room_name,
                                &identity,
                                transport_target,
                                kind,
                                data_channel.clone(),
                            );
                        }
                    }
                    let data_messages = data_messages.clone();
                    let data_channels_for_task = data_channels.clone();
                    let data_track_subscriptions = data_track_subscriptions.clone();
                    let publish_permissions_for_task = publish_permissions.clone();
                    let rooms_for_task = rooms.clone();
                    let state_for_task = state.clone();
                    let room_name = room_name.clone();
                    let identity = identity.clone();
                    tokio::spawn(async move {
                        if channel_kind == Some(DataChannelKind::DataTrack) {
                            while let Ok(bytes) = data_channel.recv_bytes().await {
                                if !publish_permissions_for_task.can_publish_data(&room_name, &identity) {
                                    continue;
                                }
                                if relay_data_track_packet_with_rooms(
                                    &data_channels_for_task,
                                    &data_track_subscriptions,
                                    &rooms_for_task,
                                    &room_name,
                                    &identity,
                                    bytes,
                                )
                                .await
                                .is_err()
                                {
                                    break;
                                }
                            }
                            return;
                        }
                        let relay_channel_kind = channel_kind.unwrap_or(DataChannelKind::Reliable);
                        while let Ok(bytes) = data_channel.recv_bytes().await {
                            if !publish_permissions_for_task.can_publish_data(&room_name, &identity) {
                                continue;
                            }
                            match proto::DataPacket::decode(bytes.as_slice()) {
                                Ok(mut packet) => {
                                    normalize_incoming_data_packet(
                                        &mut packet,
                                        &rooms_for_task,
                                        &room_name,
                                        &identity,
                                        relay_channel_kind,
                                    );

                                    if state_for_task.consume_service_rpc_data_packet(&packet) {
                                        continue;
                                    }
                                    data_messages.push_data_packet(&room_name, &identity, packet.clone());
                                    let destination_identities = resolved_destination_identities_for_packet(&packet);
                                    let encoded = packet.encode_to_vec();
                                    let relay_result = relay_data_packet_after_channel_convergence(
                                        &data_channels_for_task,
                                        &rooms_for_task,
                                        &room_name,
                                        &identity,
                                        &destination_identities,
                                        relay_channel_kind,
                                        &encoded,
                                    )
                                    .await;
                                    if relay_result.is_err() {
                                        break;
                                    }
                                }
                                Err(_) => {
                                    if let Ok(text) = String::from_utf8(bytes.clone()) {
                                        data_messages.push_text(&room_name, &identity, text);
                                    }
                                    let packet = raw_reliable_payload_data_packet(
                                        bytes,
                                        &rooms_for_task,
                                        &room_name,
                                        &identity,
                                    );
                                    let encoded = packet.encode_to_vec();
                                    if relay_data_packet_after_channel_convergence(
                                        &data_channels_for_task,
                                        &rooms_for_task,
                                        &room_name,
                                        &identity,
                                        &[],
                                        DataChannelKind::Reliable,
                                        &encoded,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }
    });
}

fn proto_kind_from_channel_kind(kind: DataChannelKind) -> i32 {
    match kind {
        DataChannelKind::Lossy => proto::data_packet::Kind::Lossy as i32,
        DataChannelKind::Reliable | DataChannelKind::DataTrack => {
            proto::data_packet::Kind::Reliable as i32
        }
    }
}

fn sender_hidden(rooms: &RoomStore, room_name: &str, sender_identity: &str) -> bool {
    rooms
        .get_participant(room_name, sender_identity)
        .ok()
        .and_then(|participant| participant.permission)
        .is_some_and(|permission| permission.hidden)
}

#[allow(deprecated)]
pub(crate) fn raw_reliable_payload_data_packet(
    bytes: Vec<u8>,
    rooms: &RoomStore,
    room_name: &str,
    sender_identity: &str,
) -> proto::DataPacket {
    let participant_sid = rooms
        .get_participant(room_name, sender_identity)
        .map(|participant| participant.sid)
        .unwrap_or_default();

    proto::DataPacket {
        kind: proto::data_packet::Kind::Reliable as i32,
        participant_identity: sender_identity.to_string(),
        value: Some(proto::data_packet::Value::User(proto::UserPacket {
            payload: bytes,
            participant_sid,
            participant_identity: sender_identity.to_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[allow(deprecated)]
fn resolved_destination_identities_for_packet(packet: &proto::DataPacket) -> Vec<String> {
    if !packet.destination_identities.is_empty() {
        return packet.destination_identities.clone();
    }

    match packet.value.as_ref() {
        Some(proto::data_packet::Value::User(user)) if !user.destination_identities.is_empty() => {
            user.destination_identities.clone()
        }
        _ => Vec::new(),
    }
}

#[allow(deprecated)]
fn normalize_incoming_data_packet(
    packet: &mut proto::DataPacket,
    rooms: &RoomStore,
    room_name: &str,
    sender_identity: &str,
    channel_kind: DataChannelKind,
) {
    packet.kind = proto_kind_from_channel_kind(channel_kind);
    let hidden_sender = sender_hidden(rooms, room_name, sender_identity);

    if packet.participant_identity.is_empty() || hidden_sender {
        packet.participant_identity = if hidden_sender {
            String::new()
        } else {
            sender_identity.to_string()
        };
    }

    if let Some(proto::data_packet::Value::User(user)) = packet.value.as_mut() {
        if user.participant_sid.is_empty() || hidden_sender {
            user.participant_sid = if hidden_sender {
                String::new()
            } else {
                rooms
                    .get_participant(room_name, sender_identity)
                    .map(|participant| participant.sid)
                    .unwrap_or_default()
            };
        }
        if user.participant_identity.is_empty() || hidden_sender {
            user.participant_identity = if hidden_sender {
                String::new()
            } else {
                sender_identity.to_string()
            };
        }
        if user.destination_identities.is_empty() && !packet.destination_identities.is_empty() {
            user.destination_identities = packet.destination_identities.clone();
        }
    }
}

fn relay_target_identities(
    rooms: &RoomStore,
    room_name: &str,
    sender_identity: &str,
    destination_identities: &[String],
) -> Vec<String> {
    if destination_identities.is_empty() {
        return rooms
            .list_participants(room_name)
            .unwrap_or_default()
            .into_iter()
            .filter(|participant| participant.identity != sender_identity)
            .map(|participant| participant.identity)
            .collect();
    }

    destination_identities
        .iter()
        .filter(|identity| identity.as_str() != sender_identity)
        .cloned()
        .collect()
}

fn is_slow_reader_backpressure_error(error: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
        return io_error.kind() == std::io::ErrorKind::WouldBlock;
    }

    error
        .source()
        .is_some_and(is_slow_reader_backpressure_error)
}

async fn relay_data_packet_after_channel_convergence(
    data_channels: &DataChannelStore,
    rooms: &RoomStore,
    room_name: &str,
    sender_identity: &str,
    destination_identities: &[String],
    channel_kind: DataChannelKind,
    encoded_packet: &[u8],
) -> oxidesfu_rtc::RtcResult<()> {
    let target_identities =
        relay_target_identities(rooms, room_name, sender_identity, destination_identities);
    if target_identities.is_empty() {
        return Ok(());
    }

    let mut channels = Vec::new();
    for target_identity in target_identities {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(channel) = data_channels.get_with_kind_for_downstream(
                room_name,
                &target_identity,
                channel_kind,
            ) && channel.is_open().await.unwrap_or(false)
            {
                channels.push((target_identity, channel));
                break;
            }

            if channel_kind != DataChannelKind::Reliable || tokio::time::Instant::now() >= deadline
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    let available_channels = channels.len();
    let send_results = futures_util::future::join_all(channels.iter().map(
        |(target_identity, channel)| async move {
            (target_identity, channel.send_bytes(encoded_packet).await)
        },
    ))
    .await;
    let mut delivered_count = 0usize;

    for (target_identity, send_result) in send_results {
        match send_result {
            Ok(()) => {
                delivered_count += 1;
            }
            Err(error) if is_slow_reader_backpressure_error(error.as_ref()) => {
                tracing::debug!(
                    room = %room_name,
                    sender_identity = %sender_identity,
                    target_identity = %target_identity,
                    channel_kind = ?channel_kind,
                    error = %error,
                    "data_packet_relay_dropped_for_slow_reader"
                );
            }
            Err(error) => {
                tracing::debug!(
                    room = %room_name,
                    sender_identity = %sender_identity,
                    target_identity = %target_identity,
                    channel_kind = ?channel_kind,
                    error = %error,
                    "data_packet_relay_send_failed"
                );
            }
        }
    }

    if delivered_count > 0 {
        tracing::debug!(
            room = %room_name,
            sender_identity = %sender_identity,
            destination_identities = ?destination_identities,
            channel_kind = ?channel_kind,
            delivered_count,
            "data_packet_relay_sent"
        );
        return Ok(());
    }

    if available_channels == 0 {
        tracing::debug!(
            room = %room_name,
            sender_identity = %sender_identity,
            destination_identities = ?destination_identities,
            channel_kind = ?channel_kind,
            "data_packet_relay_no_recipients_available"
        );
        return Ok(());
    }

    tracing::debug!(
        room = %room_name,
        sender_identity = %sender_identity,
        destination_identities = ?destination_identities,
        channel_kind = ?channel_kind,
        "data_packet_relay_dropped_for_all_available_targets"
    );
    Ok(())
}

async fn ensure_subscriber_forwarding_for_track(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    track: &proto::TrackInfo,
) -> oxidesfu_rtc::RtcResult<()> {
    if track.sid.is_empty() {
        return Ok(());
    }
    if state
        .signal_connections
        .get(room_name, publisher_identity)
        .is_none()
    {
        tracing::debug!(
            room = %room_name,
            publisher_identity = %publisher_identity,
            track_sid = %track.sid,
            "skipping_forwarding_for_disconnected_publisher"
        );
        return Ok(());
    }
    let Ok(publisher) = state.rooms.get_participant(room_name, publisher_identity) else {
        return Ok(());
    };
    let track_kind = if track.r#type == proto::TrackType::Video as i32 {
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video
    } else {
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio
    };

    let subscribers = state
        .peer_connections
        .media_receivers_in_room_except(room_name, publisher_identity);
    for (subscriber_identity, subscriber_pc, connection_kind) in subscribers {
        if !state
            .subscribe_permissions
            .can_subscribe(room_name, &subscriber_identity)
        {
            continue;
        }

        let default_subscribed = state.auto_subscribe_enabled(room_name, &subscriber_identity);
        let subscribed = state.media_subscriptions.is_subscribed_with_default(
            room_name,
            publisher_identity,
            &track.sid,
            &subscriber_identity,
            default_subscribed,
        );
        if !subscribed {
            tracing::debug!(
                room = %room_name,
                publisher_identity = %publisher_identity,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                default_subscribed,
                explicit_subscription = ?state
                    .media_subscriptions
                    .explicit_subscription(
                        room_name,
                        publisher_identity,
                        &track.sid,
                        &subscriber_identity,
                    ),
                rooms_subscribed = state.rooms.is_media_track_subscribed(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    &subscriber_identity,
                ),
                "single_pc_forwarding_not_subscribed"
            );
            if state
                .media_subscriptions
                .explicit_subscription(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    &subscriber_identity,
                )
                .is_none()
            {
                state.media_subscriptions.set_subscribed(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    &subscriber_identity,
                    false,
                );
            }
            continue;
        }

        if reject_unsupported_video_subscription_if_needed(
            state,
            room_name,
            publisher_identity,
            &subscriber_identity,
            track,
            false,
        ) {
            continue;
        }

        if connection_kind == MediaForwardingConnectionKind::SinglePcPublisher {
            if state.media_forwarding.contains(
                room_name,
                publisher_identity,
                &track.sid,
                &subscriber_identity,
            ) {
                continue;
            }
            let had_pending_request = state
                .pending_media_section_requests
                .has_for_subscriber(room_name, &subscriber_identity);
            let inserted = state.pending_media_section_requests.insert_once(
                room_name,
                publisher_identity,
                &track.sid,
                &subscriber_identity,
                pending_media_section_kind_from_track_kind(track_kind),
            );
            if inserted
                && !had_pending_request
                && state
                    .pending_media_section_requests
                    .begin_negotiation_if_idle(room_name, &subscriber_identity)
                && let Some(subscriber_outbound_tx) = state
                    .signal_connections
                    .get(room_name, &subscriber_identity)
            {
                signal_pending_media_section_requirement(
                    &state.pending_media_section_requests,
                    room_name,
                    &subscriber_identity,
                    &subscriber_outbound_tx,
                )
                .await?;
            }
            continue;
        }
        if !state.media_forwarding.insert_once(
            room_name,
            publisher_identity,
            &track.sid,
            &subscriber_identity,
        ) {
            continue;
        }
        let existing_forwarding_count = state
            .forward_tracks
            .list_for_track(room_name, publisher_identity, &track.sid)
            .len();
        let forwarding_mime = selected_forwarding_mime_type_for_subscriber(
            state,
            room_name,
            &subscriber_identity,
            track,
            existing_forwarding_count,
        );
        let forward_track = subscriber_pc
            .add_forwarding_track_with_mime(
                &publisher.sid,
                &track.sid,
                track_kind,
                forwarding_mime.as_deref(),
            )
            .await?;
        state.forward_tracks.insert_inactive(
            room_name,
            publisher_identity,
            &track.sid,
            &subscriber_identity,
            forward_track,
        );
        if let Some(subscriber_outbound_tx) = state
            .signal_connections
            .get(room_name, &subscriber_identity)
        {
            signal_media_forwarding_negotiation_with_offer_id(
                state,
                &state.subscriber_offer_ids,
                room_name,
                &subscriber_identity,
                &subscriber_pc,
                connection_kind,
                track_kind,
                &subscriber_outbound_tx,
            )
            .await?;
        }
    }

    Ok(())
}

pub(crate) async fn resolve_forward_track_info(
    rooms: &RoomStore,
    media_track_cids: &crate::stores::MediaTrackCidStore,
    room_name: &str,
    publisher_identity: &str,
    remote_track_id: &str,
    remote_mid: Option<&str>,
    remote_track_kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
) -> Option<proto::TrackInfo> {
    const RESOLUTION_ATTEMPTS: usize = 250;
    const RESOLUTION_SLEEP_MS: u64 = 20;

    let expected_track_type =
        if remote_track_kind == rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video {
            proto::TrackType::Video as i32
        } else {
            proto::TrackType::Audio as i32
        };

    for _ in 0..RESOLUTION_ATTEMPTS {
        let Ok(publisher) = rooms.get_participant(room_name, publisher_identity) else {
            return None;
        };

        if let Some(track_sid) =
            media_track_cids.find_track_sid(room_name, publisher_identity, remote_track_id)
            && let Some(track) = publisher
                .tracks
                .iter()
                .find(|track| track.sid == track_sid)
                .cloned()
        {
            return Some(track);
        }

        if remote_track_id.starts_with("TR_")
            && let Some(track) = publisher
                .tracks
                .iter()
                .find(|track| track.sid == remote_track_id)
                .cloned()
        {
            return Some(track);
        }

        if let Some(remote_mid) = remote_mid
            && let Some(track) = publisher
                .tracks
                .iter()
                .find(|track| track.mid == remote_mid)
                .cloned()
        {
            return Some(track);
        }

        // Browser renegotiation can leave the protocol and RTP MID views out
        // of sync. As a last resort, use only an unambiguous same-kind match.
        let mut compatible_tracks = publisher
            .tracks
            .iter()
            .filter(|track| track.r#type == expected_track_type);
        if let Some(track) = compatible_tracks.next().cloned()
            && compatible_tracks.next().is_none()
        {
            return Some(track);
        }

        tokio::time::sleep(std::time::Duration::from_millis(RESOLUTION_SLEEP_MS)).await;
    }

    match rooms.get_participant(room_name, publisher_identity) {
        Ok(publisher) => tracing::warn!(
            room = room_name,
            publisher_identity,
            remote_track_id,
            remote_mid = ?remote_mid,
            expected_track_type,
            tracks = ?publisher.tracks.iter().map(|track| (
                &track.sid,
                track.r#type,
                &track.mid,
                track.codecs.iter().map(|codec| (&codec.cid, &codec.sdp_cid)).collect::<Vec<_>>(),
            )).collect::<Vec<_>>(),
            "remote_track_resolution_timed_out"
        ),
        Err(error) => tracing::warn!(
            room = room_name,
            publisher_identity,
            remote_track_id,
            error = %error,
            "remote_track_resolution_publisher_missing"
        ),
    }
    None
}

pub(crate) fn publisher_session_is_current(
    rooms: &RoomStore,
    room_name: &str,
    publisher_identity: &str,
    publisher_sid: &str,
) -> bool {
    rooms
        .get_participant(room_name, publisher_identity)
        .is_ok_and(|participant| participant.sid == publisher_sid)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn rebuild_forwarding_tracks_after_runtime_codec_change(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    track_sid: &str,
) {
    state
        .forward_tracks
        .revoke_track_reader(room_name, publisher_identity, track_sid);
    let stale_forward_tracks =
        state
            .forward_tracks
            .remove_all_for_track(room_name, publisher_identity, track_sid);

    for (subscriber_identity, forward_track) in stale_forward_tracks {
        state.rtp_forwarding.remove(
            room_name,
            publisher_identity,
            track_sid,
            &subscriber_identity,
        );
        state.media_forwarding.remove(
            room_name,
            publisher_identity,
            track_sid,
            &subscriber_identity,
        );

        let Some((subscriber_pc, _connection_kind)) = state
            .peer_connections
            .media_receiver_for_identity(room_name, &subscriber_identity)
        else {
            continue;
        };
        if let Err(error) = subscriber_pc.remove_forwarding_track(&forward_track).await {
            tracing::warn!(
                room = room_name,
                publisher_identity,
                subscriber_identity,
                track_sid,
                error = %error,
                "failed_to_detach_runtime_codec_changed_forwarding_sender"
            );
        }
    }
}

struct PublisherRemoteTrackEvent {
    remote_track: oxidesfu_rtc::RemoteTrack,
    remote_mid: Option<String>,
    room_name: String,
    publisher_identity: String,
    publisher_sid: String,
}

async fn forward_publisher_remote_track(
    state: SignalState,
    event: PublisherRemoteTrackEvent,
) -> oxidesfu_rtc::RtcResult<()> {
    let PublisherRemoteTrackEvent {
        remote_track,
        remote_mid,
        room_name: event_room_name,
        publisher_identity: event_publisher_identity,
        publisher_sid: event_publisher_sid,
    } = event;
    let room_name = event_room_name.as_str();
    let publisher_identity = event_publisher_identity.as_str();
    let publisher_sid = event_publisher_sid.as_str();
    let peer_connections = &state.peer_connections;
    let rooms = &state.rooms;
    let media_forwarding = &state.media_forwarding;
    let pending_media_section_requests = &state.pending_media_section_requests;
    let media_subscriptions = &state.media_subscriptions;
    let subscribe_permissions = &state.subscribe_permissions;
    let auto_subscribe_preferences = &state.auto_subscribe_preferences;
    let track_settings = &state.track_settings;
    let track_allocations = &state.track_allocations;
    let media_track_cids = &state.media_track_cids;
    let pending_remote_tracks = &state.pending_remote_tracks;
    let subscriber_offer_ids = &state.subscriber_offer_ids;
    let forward_tracks = &state.forward_tracks;
    let rtp_forwarding = &state.rtp_forwarding;
    let signal_connections = &state.signal_connections;

    if !publisher_session_is_current(rooms, room_name, publisher_identity, publisher_sid) {
        tracing::debug!(
            room = room_name,
            publisher_identity,
            publisher_sid,
            "skipping_forwarding_for_stale_publisher_session"
        );
        return Ok(());
    }

    let remote_track_id = remote_track.track_id().await;
    let remote_mid = match remote_mid {
        Some(remote_mid) => Some(remote_mid),
        None => remote_track.mid().await,
    };
    let remote_track_kind = remote_track.kind().await;
    let Some(mut track_info) = resolve_forward_track_info(
        rooms,
        media_track_cids,
        room_name,
        publisher_identity,
        &remote_track_id,
        remote_mid.as_deref(),
        remote_track_kind,
    )
    .await
    else {
        pending_remote_tracks.enqueue(
            room_name,
            publisher_identity,
            crate::stores::PendingPublisherRemoteTrack {
                publisher_sid: publisher_sid.to_string(),
                remote_track_id: remote_track_id.clone(),
                remote_mid: remote_mid.clone(),
                remote_track,
            },
        );
        tracing::warn!(
            room = room_name,
            publisher_identity,
            remote_track_id,
            remote_mid = ?remote_mid,
            remote_track_kind = ?remote_track_kind,
            "queued_unresolved_remote_track_for_retry"
        );
        return Ok(());
    };

    if !publisher_session_is_current(rooms, room_name, publisher_identity, publisher_sid) {
        tracing::debug!(
            room = room_name,
            publisher_identity,
            publisher_sid,
            "discarding_remote_track_resolved_for_stale_publisher_session"
        );
        return Ok(());
    }

    let needs_mid_update = remote_mid
        .as_deref()
        .is_some_and(|mid| track_info.mid != mid);
    let needs_sdp_cid_update = track_info
        .codecs
        .iter()
        .any(|codec| codec.sdp_cid != remote_track_id);
    media_track_cids.insert(
        room_name,
        publisher_identity,
        &remote_track_id,
        &track_info.sid,
    );
    if needs_mid_update || needs_sdp_cid_update {
        let mut participant_update = None;
        if needs_mid_update && let Some(mid) = remote_mid.as_deref() {
            participant_update = rooms
                .set_participant_track_mid(room_name, publisher_identity, &track_info.sid, mid)
                .ok();
        }
        if needs_sdp_cid_update {
            participant_update = rooms
                .set_participant_track_sdp_cid(
                    room_name,
                    publisher_identity,
                    &track_info.sid,
                    &remote_track_id,
                )
                .ok()
                .or(participant_update);
        }
        if let Some(participant) = participant_update {
            if let Some(updated_track) = participant
                .tracks
                .iter()
                .find(|track| track.sid == track_info.sid)
                .cloned()
            {
                track_info = updated_track;
            }
            state.updates.broadcast_update(room_name, participant);
        }
    }

    let expected_mime_prefix = match track_info.r#type {
        kind if kind == proto::TrackType::Audio as i32 => "audio/",
        kind if kind == proto::TrackType::Video as i32 => "video/",
        _ => "",
    };
    if !expected_mime_prefix.is_empty() {
        for ssrc in remote_track.ssrcs().await {
            let Some(codec_mime) = remote_track.codec_mime_for_ssrc(ssrc).await else {
                continue;
            };
            let normalized_mime = codec_mime.trim().to_ascii_lowercase();
            let is_primary_media_codec = normalized_mime.starts_with(expected_mime_prefix)
                && !normalized_mime.contains("rtx")
                && !normalized_mime.contains("red")
                && !normalized_mime.contains("ulpfec")
                && !normalized_mime.contains("telephone-event");
            if !is_primary_media_codec {
                continue;
            }
            let runtime_mime_changed = !track_info.mime_type.eq_ignore_ascii_case(&normalized_mime);
            if let Ok(participant) = rooms.set_participant_track_mime_type(
                room_name,
                publisher_identity,
                &track_info.sid,
                &normalized_mime,
            ) {
                state.updates.broadcast_update(room_name, participant);
            }
            track_info.mime_type = normalized_mime.clone();
            track_info.codecs = vec![proto::SimulcastCodecInfo {
                mime_type: normalized_mime,
                ..Default::default()
            }];
            if runtime_mime_changed {
                rebuild_forwarding_tracks_after_runtime_codec_change(
                    &state,
                    room_name,
                    publisher_identity,
                    &track_info.sid,
                )
                .await;
            }
            break;
        }
    }

    tracing::debug!(
        room = room_name,
        publisher_identity,
        remote_track_id,
        track_sid = %track_info.sid,
        track_mime_type = %track_info.mime_type,
        "forwarding_remote_track_with_resolved_track_sid"
    );

    if !publisher_session_is_current(rooms, room_name, publisher_identity, publisher_sid) {
        return Ok(());
    }

    // Ensure forwarding tracks exist for current subscribers before copying RTP.
    ensure_subscriber_forwarding_from_parts(
        &state,
        peer_connections,
        rooms,
        media_forwarding,
        pending_media_section_requests,
        media_subscriptions,
        subscribe_permissions,
        auto_subscribe_preferences,
        forward_tracks,
        subscriber_offer_ids,
        signal_connections,
        room_name,
        publisher_identity,
        &track_info,
    )
    .await?;

    let Some(reader_lease) =
        forward_tracks.acquire_track_reader(room_name, publisher_identity, &track_info.sid)
    else {
        tracing::debug!(
            room = room_name,
            publisher_identity,
            track_sid = %track_info.sid,
            "forward_track_reader_already_started"
        );
        return Ok(());
    };

    let forward_tracks = forward_tracks.clone();
    let media_forwarding = media_forwarding.clone();
    let pending_media_section_requests = pending_media_section_requests.clone();
    let media_subscriptions = media_subscriptions.clone();
    let peer_connections = peer_connections.clone();
    let subscribe_permissions = subscribe_permissions.clone();
    let auto_subscribe_preferences = auto_subscribe_preferences.clone();
    let track_settings = track_settings.clone();
    let track_allocations = track_allocations.clone();
    let rooms = rooms.clone();
    let rtp_forwarding = rtp_forwarding.clone();
    let subscriber_offer_ids = subscriber_offer_ids.clone();
    let signal_connections = signal_connections.clone();
    let publisher_subscription_active_pairs = state.publisher_subscription_active_pairs();
    let state = state.clone();
    let room_name = room_name.to_string();
    let publisher_identity = publisher_identity.to_string();
    let publisher_sid = publisher_sid.to_string();
    let is_video_track = track_info.r#type == proto::TrackType::Video as i32;
    let track_supports_quality_control =
        is_video_track && track_supports_layer_quality_control(&track_info);
    let (layer_quality_by_ssrc, layer_quality_by_rid) = layer_quality_maps_for_track(&track_info);
    let track_sid = track_info.sid.clone();
    let audio_subscription_limit = state.media_subscription_limit_audio;
    let video_subscription_limit = state.media_subscription_limit_video;
    tokio::spawn(async move {
        let forwarding_decision_context = ForwardingDecisionContext {
            media_subscriptions: &media_subscriptions,
            auto_subscribe_preferences: &auto_subscribe_preferences,
            track_settings: &track_settings,
            track_allocations: &track_allocations,
            rooms: &rooms,
            subscription_limits: SubscriptionLimits {
                audio: audio_subscription_limit,
                video: video_subscription_limit,
            },
        };
        let mut logged_no_forward_tracks = false;
        let mut logged_first_rtp = false;
        let mut keyframe_retry_tick = tokio::time::interval_at(
            tokio::time::Instant::now() + std::time::Duration::from_millis(250),
            std::time::Duration::from_millis(250),
        );
        keyframe_retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut forwarding_debug_tick = tokio::time::interval_at(
            tokio::time::Instant::now() + std::time::Duration::from_secs(3),
            std::time::Duration::from_secs(3),
        );
        forwarding_debug_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut allocation_tick = tokio::time::interval_at(
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        );
        allocation_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut video_ssrc_rids = HashMap::<u32, Option<String>>::new();
        let mut video_ssrc_codec_mime = HashMap::<u32, Option<String>>::new();
        let mut video_ssrc_codec_class = HashMap::<u32, VideoCodecClass>::new();
        let mut video_rid_primary_ssrc = HashMap::<String, u32>::new();
        let mut dropped_repair_video_ssrc_count: u64 = 0;
        let mut had_forward_targets: Option<bool> = None;
        let mut track_subscribed_signaled_to_publisher = false;
        let mut forwarding_debug_heartbeat_due = false;
        let mut last_video_media_ssrc: Option<u32> = None;
        let mut cached_forward_tracks_revision: Option<u64> = None;
        let mut track_settings_changes = track_settings.subscribe_effective_changes();
        let mut track_allocation_changes = track_allocations.subscribe_effective_changes();
        let mut cached_forward_targets = Vec::<ForwardTarget>::new();

        let reader_lease_generation = reader_lease.generation();
        loop {
            if !forward_tracks.owns_track_reader(&reader_lease) {
                tracing::debug!(
                    room = %room_name,
                    publisher_identity = %publisher_identity,
                    track_sid = %track_sid,
                    "stopping_forward_track_reader_after_lease_revocation"
                );
                break;
            }
            if !publisher_session_is_current(
                &rooms,
                &room_name,
                &publisher_identity,
                &publisher_sid,
            ) {
                tracing::debug!(
                    room = %room_name,
                    publisher_identity = %publisher_identity,
                    publisher_sid,
                    track_sid = %track_sid,
                    "stopping_forward_track_reader_for_stale_publisher_session"
                );
                break;
            }

            let recv_event_result = if is_video_track {
                tokio::select! {
                    result = tokio::time::timeout(
                        REMOTE_TRACK_EVENT_IDLE_LOG_INTERVAL,
                        remote_track.recv_event(),
                    ) => result,
                    _ = keyframe_retry_tick.tick() => {
                        let keyframe_requests = cached_forward_targets
                            .iter_mut()
                            .filter_map(|target| {
                                let state_before = target.video_layer_selector.acquisition_state();
                                let remaining_before = target.video_layer_selector.remaining_pli_requests();
                                let request = target.video_layer_selector.on_timer();
                                if request.is_some() {
                                    target.video_counters.selector_pli_requests += 1;
                                } else {
                                    match state_before {
                                        LayerAcquisitionState::Stable => {
                                            target.video_counters.selector_pli_suppressed_stable += 1;
                                        }
                                        LayerAcquisitionState::FallbackLocked => {
                                            target.video_counters.selector_pli_suppressed_fallback_locked += 1;
                                        }
                                        LayerAcquisitionState::WaitingForDesired
                                        | LayerAcquisitionState::WaitingForFallback
                                            if remaining_before == 0 =>
                                        {
                                            target.video_counters.selector_pli_suppressed_budget_exhausted += 1;
                                        }
                                        LayerAcquisitionState::WaitingForDesired
                                        | LayerAcquisitionState::WaitingForFallback => {
                                            target.video_counters.selector_pli_suppressed_retry_or_no_target += 1;
                                        }
                                    }
                                }
                                request
                            })
                            .map(|request| Box::new(
                                rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                                    sender_ssrc: 0,
                                    media_ssrc: request.media_ssrc,
                                },
                            ) as Box<dyn rtc::rtcp::Packet>)
                            .collect::<Vec<_>>();
                        if !keyframe_requests.is_empty() {
                            let _ = remote_track.write_rtcp_packets(keyframe_requests).await;
                        }
                        continue;
                    }
                    _ = allocation_tick.tick() => {
                        for target in &cached_forward_targets {
                            let subscriber_identity = target.subscriber_identity();
                            let Some((receiver, _)) = peer_connections
                                .media_receiver_for_identity(&room_name, subscriber_identity)
                            else {
                                track_allocations.remove_for_track(
                                    &room_name,
                                    subscriber_identity,
                                    &track_sid,
                                );
                                continue;
                            };
                            let test_support_override = state
                                .test_support_available_outgoing_bitrate_bps(
                                    &room_name,
                                    subscriber_identity,
                                );
                            let rtc_estimate = if test_support_override.is_none() {
                                receiver.available_outgoing_bitrate_bps().await
                            } else {
                                None
                            };
                            let Some(available_bitrate_bps) = allocation_available_outgoing_bitrate_bps(
                                test_support_override,
                                rtc_estimate,
                            ) else {
                                track_allocations.remove_for_track(
                                    &room_name,
                                    subscriber_identity,
                                    &track_sid,
                                );
                                continue;
                            };
                            let total_layout_weight = eligible_video_layout_weight_for_subscriber(
                                &rooms,
                                &media_subscriptions,
                                &track_settings,
                                &room_name,
                                subscriber_identity,
                            );
                            let target_layout_weight = allocation_layout_weight(
                                &track_info,
                                track_settings
                                    .get_for_track(&room_name, subscriber_identity, &track_sid)
                                    .as_ref(),
                            );
                            let budget_bps = available_bitrate_bps
                                .saturating_mul(target_layout_weight)
                                / total_layout_weight;
                            let quality = allocation_quality_for_budget(&track_info, budget_bps);
                            let temporal = quality.and_then(|quality| {
                                allocation_temporal_layer_for_budget(
                                    &track_info,
                                    quality,
                                    budget_bps,
                                )
                            });
                            track_allocations.set_desired_quality_for_track(
                                &room_name,
                                subscriber_identity,
                                &track_sid,
                                quality,
                            );
                            track_allocations.set_desired_temporal_layer_for_track(
                                &room_name,
                                subscriber_identity,
                                &track_sid,
                                temporal,
                            );
                        }
                        continue;
                    }
                    _ = forwarding_debug_tick.tick() => {
                        let current_revision = forward_tracks.revision();
                        if cached_forward_tracks_revision != Some(current_revision) {
                            refresh_forward_targets_for_track(
                                &mut cached_forward_targets,
                                &forward_tracks,
                                &rtp_forwarding,
                                &room_name,
                                &publisher_identity,
                                &track_sid,
                            )
                            .await;
                            cached_forward_tracks_revision = Some(current_revision);
                        }
                        record_forwarding_snapshot(
                            &room_name,
                            &publisher_identity,
                            &track_sid,
                            &mut cached_forward_targets,
                            &video_ssrc_rids,
                        );
                        forwarding_debug_heartbeat_due = true;
                        // This is track-level recovery, distinct from target-layer acquisition.
                        // It preserves the prior periodic keyframe behavior for an already
                        // locked stream without adding time checks to the RTP packet path.
                        if let Some(media_ssrc) = last_video_media_ssrc
                            && !cached_forward_targets.is_empty()
                        {
                            let _ = remote_track.write_rtcp_packets(vec![Box::new(
                                rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                                    sender_ssrc: 0,
                                    media_ssrc,
                                },
                            )]).await;
                        }
                        continue;
                    }
                    change = track_settings_changes.recv() => {
                        match change {
                            Ok(change) => {
                                let changed_targets = apply_effective_track_settings_change(
                                    &mut cached_forward_targets,
                                    &change,
                                );
                                if changed_targets > 0 {
                                    tracing::debug!(
                                        room = %room_name,
                                        publisher_identity = %publisher_identity,
                                        track_sid = %track_sid,
                                        subscriber_identity = %change.subscriber_identity,
                                        settings_revision = change.revision,
                                        changed_targets,
                                        "subscriber_track_settings_applied_to_forwarding_target"
                                    );
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                resync_target_settings_revisions(
                                    &mut cached_forward_targets,
                                    &track_settings,
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                        continue;
                    }
                    change = track_allocation_changes.recv() => {
                        match change {
                            Ok(change) => {
                                let changed_targets = apply_effective_track_allocation_change(
                                    &mut cached_forward_targets,
                                    &change,
                                );
                                if changed_targets > 0 {
                                    tracing::debug!(
                                        room = %room_name,
                                        publisher_identity = %publisher_identity,
                                        track_sid = %track_sid,
                                        subscriber_identity = %change.subscriber_identity,
                                        allocation_revision = change.revision,
                                        changed_targets,
                                        "subscriber_track_allocation_applied_to_forwarding_target"
                                    );
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                resync_target_allocation_revisions(
                                    &mut cached_forward_targets,
                                    &track_allocations,
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                        continue;
                    }
                }
            } else {
                tokio::select! {
                    result = tokio::time::timeout(
                        REMOTE_TRACK_EVENT_IDLE_LOG_INTERVAL,
                        remote_track.recv_event(),
                    ) => result,
                    change = track_settings_changes.recv() => {
                        match change {
                            Ok(change) => {
                                apply_effective_track_settings_change(
                                    &mut cached_forward_targets,
                                    &change,
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                resync_target_settings_revisions(
                                    &mut cached_forward_targets,
                                    &track_settings,
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                        continue;
                    }
                    change = track_allocation_changes.recv() => {
                        match change {
                            Ok(change) => {
                                apply_effective_track_allocation_change(
                                    &mut cached_forward_targets,
                                    &change,
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                resync_target_allocation_revisions(
                                    &mut cached_forward_targets,
                                    &track_allocations,
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                        continue;
                    }
                }
            };
            let recv_event = match recv_event_result {
                Ok(recv_event) => recv_event,
                Err(_elapsed) => {
                    if is_video_track {
                        let current_revision = forward_tracks.revision();
                        if cached_forward_tracks_revision != Some(current_revision) {
                            refresh_forward_targets_for_track(
                                &mut cached_forward_targets,
                                &forward_tracks,
                                &rtp_forwarding,
                                &room_name,
                                &publisher_identity,
                                &track_sid,
                            )
                            .await;
                            cached_forward_tracks_revision = Some(current_revision);
                        }
                        let total_forward_targets = cached_forward_targets.len();
                        let mut filtered_out_targets = 0usize;
                        let mut media_forwarding_true_targets = 0usize;
                        let mut signaling_subscribed_targets = 0usize;
                        let mut room_subscribed_targets = 0usize;
                        let media_subscription_revision = media_subscriptions.revision();
                        let auto_subscribe_revision = auto_subscribe_preferences.revision();
                        let room_media_subscription_revision = rooms.media_subscription_revision();
                        for target in &mut cached_forward_targets {
                            let decision_revisions = ForwardingDecisionRevisions {
                                media_subscription: media_subscription_revision,
                                auto_subscribe: auto_subscribe_revision,
                                track_settings: target.settings_revision.unwrap_or_default(),
                                track_allocation: target.allocation_revision.unwrap_or_default(),
                                room_media_subscription: room_media_subscription_revision,
                            };
                            let (key_room, key_publisher, key_track_sid, key_subscriber) =
                                &target.key;
                            let has_media_forwarding = media_forwarding.contains(
                                key_room,
                                key_publisher,
                                key_track_sid,
                                key_subscriber,
                            );
                            let signaling_subscribed = media_subscriptions.is_subscribed(
                                key_room,
                                key_publisher,
                                key_track_sid,
                                key_subscriber,
                            );
                            let room_subscribed = rooms.is_media_track_subscribed(
                                key_room,
                                key_publisher,
                                key_track_sid,
                                key_subscriber,
                            );
                            if has_media_forwarding {
                                media_forwarding_true_targets += 1;
                            }
                            if signaling_subscribed {
                                signaling_subscribed_targets += 1;
                            }
                            if room_subscribed {
                                room_subscribed_targets += 1;
                            }
                            let decision = cached_forwarding_decision_for_target(
                                target,
                                decision_revisions,
                                &forwarding_decision_context,
                                Some(&track_info),
                            );
                            if !decision.should_forward_media {
                                filtered_out_targets += 1;
                            }
                        }

                        tracing::warn!(
                            room = %room_name,
                            publisher_identity = %publisher_identity,
                            track_sid = %track_sid,
                            total_forward_targets,
                            filtered_out_targets,
                            media_forwarding_true_targets,
                            signaling_subscribed_targets,
                            room_subscribed_targets,
                            idle_ms = REMOTE_TRACK_EVENT_IDLE_LOG_INTERVAL.as_millis(),
                            "video_forwarding_remote_track_event_idle"
                        );
                    }
                    continue;
                }
            };

            match recv_event {
                Ok(oxidesfu_rtc::RemoteTrackEvent::RtpPacket(incoming)) => {
                    let oxidesfu_rtc::IncomingRtpPacket {
                        packet,
                        metadata: packet_metadata,
                    } = incoming;
                    let mut packet_video_quality: Option<proto::VideoQuality> = None;
                    let mut packet_source_kind = VideoSourceKind::Simulcast;
                    let mut effective_video_ssrc = packet.header.ssrc;
                    if is_video_track {
                        let incoming_ssrc = packet.header.ssrc;

                        if let std::collections::hash_map::Entry::Vacant(entry) =
                            video_ssrc_rids.entry(incoming_ssrc)
                        {
                            let rid = remote_track.rid_for_ssrc(incoming_ssrc).await;
                            entry.insert(rid);
                        }
                        if !video_ssrc_codec_mime.contains_key(&incoming_ssrc) {
                            let codec_mime = remote_track.codec_mime_for_ssrc(incoming_ssrc).await;
                            if let Some(codec_mime) = codec_mime.as_deref() {
                                let normalized_mime = codec_mime.trim().to_ascii_lowercase();
                                let primary_video_codec = normalized_mime.starts_with("video/")
                                    && !normalized_mime.contains("rtx")
                                    && !normalized_mime.contains("red")
                                    && !normalized_mime.contains("ulpfec");
                                if primary_video_codec
                                    && !track_info.mime_type.eq_ignore_ascii_case(&normalized_mime)
                                {
                                    match rooms.set_participant_track_mime_type(
                                        &room_name,
                                        &publisher_identity,
                                        &track_sid,
                                        &normalized_mime,
                                    ) {
                                        Ok(participant) => {
                                            if let Some(updated_track) = participant
                                                .tracks
                                                .iter()
                                                .find(|track| track.sid == track_sid)
                                                .cloned()
                                            {
                                                track_info = updated_track;
                                            }
                                            state.updates.broadcast_update(&room_name, participant);
                                            rebuild_forwarding_tracks_after_runtime_codec_change(
                                                &state,
                                                &room_name,
                                                &publisher_identity,
                                                &track_sid,
                                            )
                                            .await;
                                            if let Err(error) =
                                                ensure_subscriber_forwarding_from_parts(
                                                    &state,
                                                    &peer_connections,
                                                    &rooms,
                                                    &media_forwarding,
                                                    &pending_media_section_requests,
                                                    &media_subscriptions,
                                                    &subscribe_permissions,
                                                    &auto_subscribe_preferences,
                                                    &forward_tracks,
                                                    &subscriber_offer_ids,
                                                    &signal_connections,
                                                    &room_name,
                                                    &publisher_identity,
                                                    &track_info,
                                                )
                                                .await
                                            {
                                                tracing::warn!(
                                                    room = %room_name,
                                                    publisher_identity = %publisher_identity,
                                                    track_sid = %track_sid,
                                                    codec_mime = %normalized_mime,
                                                    error = %error,
                                                    "failed_to_rebuild_forwarding_after_runtime_codec_change"
                                                );
                                            }
                                            // The next RTP packet observes the replacement target
                                            // set. The triggering packet must not be sent through a
                                            // stale sender negotiated for the prior codec.
                                            video_ssrc_codec_mime.insert(
                                                incoming_ssrc,
                                                Some(codec_mime.to_string()),
                                            );
                                            video_ssrc_codec_class.insert(
                                                incoming_ssrc,
                                                video_codec_class_from_mime(Some(codec_mime)),
                                            );
                                            continue;
                                        }
                                        Err(error) => tracing::warn!(
                                            room = %room_name,
                                            publisher_identity = %publisher_identity,
                                            track_sid = %track_sid,
                                            codec_mime = %normalized_mime,
                                            error = %error,
                                            "failed_to_reconcile_runtime_track_codec"
                                        ),
                                    }
                                }
                            }
                            video_ssrc_codec_class.insert(
                                incoming_ssrc,
                                video_codec_class_from_mime(codec_mime.as_deref()),
                            );
                            video_ssrc_codec_mime.insert(incoming_ssrc, codec_mime);
                        }

                        if matches!(
                            video_ssrc_codec_class
                                .get(&incoming_ssrc)
                                .copied()
                                .unwrap_or(VideoCodecClass::Unknown),
                            VideoCodecClass::Rtx
                        ) {
                            dropped_repair_video_ssrc_count =
                                dropped_repair_video_ssrc_count.saturating_add(1);
                            if dropped_repair_video_ssrc_count.is_multiple_of(300) {
                                tracing::debug!(
                                    room = %room_name,
                                    publisher_identity = %publisher_identity,
                                    track_sid = %track_sid,
                                    incoming_ssrc,
                                    dropped_repair_video_ssrc_count,
                                    "video_packet_dropped_repair_ssrc"
                                );
                            }
                            continue;
                        }

                        effective_video_ssrc =
                            if let Some(Some(rid)) = video_ssrc_rids.get(&incoming_ssrc) {
                                let primary = video_rid_primary_ssrc
                                    .entry(rid.clone())
                                    .or_insert(incoming_ssrc);
                                *primary
                            } else {
                                incoming_ssrc
                            };

                        let incoming_codec_mime = video_ssrc_codec_mime
                            .get(&incoming_ssrc)
                            .and_then(|mime| mime.as_deref());
                        let incoming_codec_class = video_ssrc_codec_class
                            .get(&incoming_ssrc)
                            .copied()
                            .unwrap_or_else(|| video_codec_class_from_mime(incoming_codec_mime));
                        packet_source_kind = if is_single_scalable_source_for_codec_class(
                            incoming_codec_class,
                            !layer_quality_by_ssrc.is_empty() || !layer_quality_by_rid.is_empty(),
                        ) {
                            VideoSourceKind::SingleScalable
                        } else {
                            VideoSourceKind::Simulcast
                        };
                        packet_video_quality = packet_video_quality_for_track(
                            incoming_ssrc,
                            video_ssrc_rids
                                .get(&incoming_ssrc)
                                .and_then(|rid| rid.as_deref()),
                            &layer_quality_by_ssrc,
                            &layer_quality_by_rid,
                        );
                    }

                    if !logged_first_rtp {
                        tracing::debug!(
                            room = %room_name,
                            publisher_identity = %publisher_identity,
                            track_sid = %track_sid,
                            "forward_track_reader_received_first_rtp_packet"
                        );
                        logged_first_rtp = true;
                    }
                    let current_revision = forward_tracks.revision();
                    let forwarding_targets_changed = forwarding_target_revision_changed(
                        cached_forward_tracks_revision,
                        current_revision,
                    );
                    if forwarding_targets_changed {
                        refresh_forward_targets_for_track(
                            &mut cached_forward_targets,
                            &forward_tracks,
                            &rtp_forwarding,
                            &room_name,
                            &publisher_identity,
                            &track_sid,
                        )
                        .await;
                        cached_forward_tracks_revision = Some(current_revision);
                    }
                    let had_targets_now = !cached_forward_targets.is_empty();
                    if had_forward_targets != Some(had_targets_now) {
                        tracing::debug!(
                            room = %room_name,
                            publisher_identity = %publisher_identity,
                            track_sid = %track_sid,
                            had_forward_targets = ?had_forward_targets,
                            has_forward_targets_now = had_targets_now,
                            "forward_track_reader_forward_targets_transition"
                        );
                        had_forward_targets = Some(had_targets_now);
                    }

                    if is_video_track {
                        last_video_media_ssrc = Some(packet.header.ssrc);
                    }

                    if cached_forward_targets.is_empty() {
                        if !logged_no_forward_tracks {
                            tracing::debug!(
                                room = %room_name,
                                publisher_identity = %publisher_identity,
                                track_sid = %track_sid,
                                "forward_track_reader_has_no_subscriber_forward_tracks"
                            );
                            logged_no_forward_tracks = true;
                        }
                        continue;
                    }
                    logged_no_forward_tracks = false;

                    if is_video_track && forwarding_targets_changed {
                        tracing::debug!(
                            room = %room_name,
                            publisher_identity = %publisher_identity,
                            track_sid = %track_sid,
                            media_ssrc = packet.header.ssrc,
                            "requesting_keyframe_for_new_video_forwarding_targets"
                        );
                        let _ = remote_track
                            .write_rtcp_packets(vec![Box::new(
                                rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                                    sender_ssrc: 0,
                                    media_ssrc: packet.header.ssrc,
                                },
                            )])
                            .await;
                    }

                    let total_forward_targets = cached_forward_targets.len();
                    let mut filtered_out_targets = 0usize;
                    let collect_forwarding_debug_counts = is_video_track
                        && take_forwarding_debug_heartbeat(&mut forwarding_debug_heartbeat_due);
                    let mut media_forwarding_true_targets = 0usize;
                    let mut signaling_subscribed_targets = 0usize;
                    let mut room_subscribed_targets = 0usize;
                    let media_subscription_revision = media_subscriptions.revision();
                    let auto_subscribe_revision = auto_subscribe_preferences.revision();
                    let room_media_subscription_revision = rooms.media_subscription_revision();
                    let packet_codec_class = video_ssrc_codec_class
                        .get(&packet.header.ssrc)
                        .copied()
                        .unwrap_or(VideoCodecClass::Unknown);
                    let packet_temporal_codec_hint =
                        video_temporal_codec_hint_from_class(packet_codec_class);
                    let packet_temporal_layer_hint = is_video_track
                        .then_some(packet_metadata.temporal_layer)
                        .flatten();
                    let packet_descriptor_switch_point = is_video_track
                        .then_some(packet_metadata.dependency_descriptor_switch_point)
                        .flatten();
                    let packet_descriptor_metadata = is_video_track
                        .then_some(packet_metadata.dependency_descriptor)
                        .flatten();
                    let packet_descriptor_frame = (packet_source_kind
                        == VideoSourceKind::SingleScalable)
                        .then(|| {
                            packet_descriptor_metadata.map(PacketDependencyDescriptorFrame::new)
                        })
                        .flatten();

                    let packet_receiver_temporal_layer_fps = is_video_track
                        .then_some(packet_metadata.temporal_layer_fps)
                        .flatten();
                    let packet_temporal_layer = temporal_layer_id_from_packet(
                        packet_temporal_codec_hint,
                        &packet.payload,
                        packet_temporal_layer_hint,
                    );
                    macro_rules! record_completed_video_batch {
                        ($target:expr, $batch:expr) => {{
                            let batch = $batch;
                            if !$target.forwarded_once {
                                $target.forwarded_once = true;
                                if !track_subscribed_signaled_to_publisher
                                    && should_emit_track_subscribed_for_subscriber(
                                        &rooms,
                                        &room_name,
                                        &publisher_identity,
                                        $target.subscriber_identity(),
                                    )
                                {
                                    signal_track_subscribed_to_publisher(
                                        &publisher_subscription_active_pairs,
                                        &signal_connections,
                                        &room_name,
                                        &publisher_identity,
                                        $target.subscriber_identity(),
                                        &track_sid,
                                    );
                                    track_subscribed_signaled_to_publisher = true;
                                }
                                tracing::info!(
                                    room = %room_name,
                                    publisher_identity = %publisher_identity,
                                    track_sid = %track_sid,
                                    subscriber_identity = %$target.subscriber_identity(),
                                    incoming_payload_type = batch.incoming_payload_type,
                                    forwarded_payload_type = batch.forwarded_payload_type,
                                    batch_packets = $target.video_counters.successful_rtp_packets,
                                    "video_forwarding_first_packet_forwarded"
                                );
                            }
                        }};
                    }
                    macro_rules! record_video_batch_error {
                        ($target:expr, $error:expr) => {{
                            if $target.video_write_errors % 50 == 0 {
                                tracing::warn!(
                                    room = %room_name,
                                    publisher_identity = %publisher_identity,
                                    track_sid = %track_sid,
                                    subscriber_identity = %$target.subscriber_identity(),
                                    write_errors = $target.video_write_errors,
                                    error = %$error,
                                    "video_forwarding_write_failed"
                                );
                            }
                        }};
                    }

                    for target in &mut cached_forward_targets {
                        let mut packet_descriptor_forwarding_decision = None;
                        let settings_revision = target.settings_revision.unwrap_or_default();
                        let decision_revisions = ForwardingDecisionRevisions {
                            media_subscription: media_subscription_revision,
                            auto_subscribe: auto_subscribe_revision,
                            track_settings: settings_revision,
                            track_allocation: target.allocation_revision.unwrap_or_default(),
                            room_media_subscription: room_media_subscription_revision,
                        };
                        let (key_room, key_publisher, key_track_sid, key_subscriber) = &target.key;
                        if collect_forwarding_debug_counts {
                            if media_forwarding.contains(
                                key_room,
                                key_publisher,
                                key_track_sid,
                                key_subscriber,
                            ) {
                                media_forwarding_true_targets += 1;
                            }
                            if media_subscriptions.is_subscribed(
                                key_room,
                                key_publisher,
                                key_track_sid,
                                key_subscriber,
                            ) {
                                signaling_subscribed_targets += 1;
                            }
                            if rooms.is_media_track_subscribed(
                                key_room,
                                key_publisher,
                                key_track_sid,
                                key_subscriber,
                            ) {
                                room_subscribed_targets += 1;
                            }
                        }

                        let decision = cached_forwarding_decision_for_target(
                            target,
                            decision_revisions,
                            &forwarding_decision_context,
                            Some(&track_info),
                        );
                        if !decision.should_forward_media {
                            filtered_out_targets += 1;
                            continue;
                        }

                        if is_video_track && track_supports_quality_control {
                            let requested_quality = decision
                                .requested_max_quality
                                .unwrap_or(proto::VideoQuality::High);
                            let desired_quality =
                                decision.desired_quality.unwrap_or(requested_quality);
                            let policy = LayerPolicy {
                                max: SpatialLayer::from_quality(requested_quality),
                                desired: SpatialLayer::from_quality(desired_quality),
                            };
                            target.video_layer_selector.set_policy(policy);
                            let layer_selection = target
                                .video_layer_selector
                                .observe_packet_with_dependency_descriptor_metadata(
                                LayerPacketMetadata {
                                    ssrc: effective_video_ssrc,
                                    spatial: packet_video_quality.map(SpatialLayer::from_quality),
                                    source_kind: packet_source_kind,
                                    is_decodable_switch_point:
                                        video_is_decodable_switch_point_with_dependency_descriptor_for_codec_class(
                                            packet_codec_class,
                                            &packet.payload,
                                            packet_descriptor_switch_point,
                                        ),
                                },
                                packet_descriptor_switch_point.is_some(),
                            );
                            match layer_selection {
                                VideoIngressDecision::Forward {
                                    selected_ssrc_changed,
                                } => {
                                    if selected_ssrc_changed {
                                        target.video_counters.layer_switches += 1;
                                        tracing::debug!(
                                            room = %room_name,
                                            publisher_identity = %publisher_identity,
                                            track_sid = %track_sid,
                                            subscriber_identity = %target.subscriber_identity(),
                                            settings_revision,
                                            requested_max_quality = ?decision.requested_max_quality,
                                            desired_quality = ?decision.desired_quality,
                                            selected_incoming_ssrc = effective_video_ssrc,
                                            selected_spatial = ?target.video_layer_selector.current_spatial(),
                                            "subscriber_video_layer_switched"
                                        );
                                    }
                                }
                                VideoIngressDecision::DropNonSelectedSsrc => {
                                    target.video_counters.drop_non_selected_ssrc += 1;
                                    filtered_out_targets += 1;
                                    continue;
                                }
                                VideoIngressDecision::DropWaitingForKeyframe => {
                                    target.video_counters.drop_waiting_for_keyframe += 1;
                                    filtered_out_targets += 1;
                                    continue;
                                }
                                VideoIngressDecision::DropAboveMaximum => {
                                    target.video_counters.drop_above_maximum += 1;
                                    filtered_out_targets += 1;
                                    continue;
                                }
                                VideoIngressDecision::DropUnknownLayer => {
                                    target.video_counters.drop_unknown_layer += 1;
                                    filtered_out_targets += 1;
                                    continue;
                                }
                            }
                        }

                        if is_video_track
                            && packet_source_kind == VideoSourceKind::SingleScalable
                            && let Some(descriptor_frame) = packet_descriptor_frame.as_ref()
                        {
                            if let Some(requested_fps) = decision.requested_fps {
                                target
                                    .video_temporal_controller
                                    .set_requested_fps_with_desired_temporal_layer(
                                        requested_fps,
                                        packet_receiver_temporal_layer_fps,
                                        decision.desired_temporal_layer,
                                    );
                            }
                            let descriptor_decision = select_dependency_descriptor_frame(
                                target,
                                descriptor_frame,
                                target.video_layer_selector.policy(),
                                target.video_temporal_controller.policy(),
                            );
                            packet_descriptor_forwarding_decision = Some(descriptor_decision);
                            if !matches!(
                                descriptor_decision,
                                DependencyDescriptorForwardingDecision::Forward { .. }
                            ) {
                                filtered_out_targets += 1;
                                continue;
                            }
                        }

                        if track_supports_quality_control
                            && let Some(requested_fps) = decision.requested_fps
                        {
                            target
                                .video_temporal_controller
                                .set_requested_fps_with_desired_temporal_layer(
                                    requested_fps,
                                    packet_receiver_temporal_layer_fps,
                                    decision.desired_temporal_layer,
                                );
                            match target.video_temporal_controller.observe_packet(
                                packet.header.timestamp,
                                requested_fps,
                                packet_temporal_layer,
                            ) {
                                TemporalIngressDecision::Forward => {}
                                TemporalIngressDecision::DropAboveMaximum => {
                                    target.video_counters.drop_temporal_above_maximum += 1;
                                    filtered_out_targets += 1;
                                    continue;
                                }
                                TemporalIngressDecision::DropAboveDesired => {
                                    target.video_counters.drop_temporal_above_desired += 1;
                                    filtered_out_targets += 1;
                                    continue;
                                }
                                TemporalIngressDecision::DropTimestampCap => {
                                    target.video_counters.drop_temporal_timestamp_cap += 1;
                                    filtered_out_targets += 1;
                                    continue;
                                }
                            }
                        }

                        let target_ssrc = match target.target_primary_ssrc {
                            Some(target_ssrc) => target_ssrc,
                            None => {
                                let target_ssrc = target.local_forward_track.primary_ssrc().await;
                                target.target_primary_ssrc = Some(target_ssrc);
                                target_ssrc
                            }
                        };
                        let negotiated_payload_type = if is_video_track {
                            None
                        } else {
                            match target.target_payload_type {
                                Some(payload_type) => payload_type,
                                None => {
                                    let payload_type = match target
                                        .local_forward_track
                                        .bind_result()
                                        .await
                                    {
                                        oxidesfu_rtc::ForwardTrackBindResult::Compatible {
                                            payload_type,
                                        } => Some(payload_type),
                                        oxidesfu_rtc::ForwardTrackBindResult::Pending
                                        | oxidesfu_rtc::ForwardTrackBindResult::UnsupportedCodec => {
                                            None
                                        }
                                    };
                                    target.target_payload_type = Some(payload_type);
                                    payload_type
                                }
                            }
                        };
                        let rewritten_packet = target
                            .rtp_forwarder
                            .rewrite_packet_with_target_ssrc_and_payload_type(
                                &packet,
                                target_ssrc,
                                negotiated_payload_type,
                            );
                        let Some(mut rewritten_packet) = rewritten_packet else {
                            if is_video_track {
                                if !target.logged_video_rewrite_drop {
                                    target.logged_video_rewrite_drop = true;
                                    tracing::debug!(
                                        room = %room_name,
                                        publisher_identity = %publisher_identity,
                                        track_sid = %track_sid,
                                        subscriber_identity = %target.subscriber_identity(),
                                        selected_incoming_ssrc = ?target.video_layer_selector.selected_ssrc(),
                                        incoming_ssrc = packet.header.ssrc,
                                        incoming_sequence_number = packet.header.sequence_number,
                                        "video_forwarding_packet_rewrite_dropped"
                                    );
                                }
                            } else if !target.logged_audio_rewrite_drop {
                                target.logged_audio_rewrite_drop = true;
                                tracing::debug!(
                                    room = %room_name,
                                    publisher_identity = %publisher_identity,
                                    track_sid = %track_sid,
                                    subscriber_identity = %target.subscriber_identity(),
                                    incoming_sequence_number = packet.header.sequence_number,
                                    "audio_forwarding_packet_rewrite_dropped"
                                );
                            }
                            if is_video_track {
                                target.video_counters.rewrite_drops += 1;
                            }
                            continue;
                        };

                        if let (Some(descriptor_frame), Some(descriptor_decision)) = (
                            packet_descriptor_frame.as_ref(),
                            packet_descriptor_forwarding_decision,
                        ) {
                            let destination_extension_id =
                                match target.target_dependency_descriptor_extension_id {
                                    Some(extension_id) => extension_id,
                                    None => {
                                        let extension_id = target
                                            .local_forward_track
                                            .dependency_descriptor_extension_id()
                                            .await;
                                        target.target_dependency_descriptor_extension_id =
                                            Some(extension_id);
                                        extension_id
                                    }
                                };
                            if rewrite_dependency_descriptor_for_target(
                                &mut rewritten_packet,
                                &descriptor_frame.snapshot,
                                descriptor_decision,
                                destination_extension_id,
                            ) {
                                target
                                    .rtp_forwarder
                                    .replace_cached_retransmission_packet(&rewritten_packet);
                            }
                        }

                        if is_video_track {
                            remove_unencodable_one_byte_extensions(&mut rewritten_packet);
                        }

                        if uses_prepared_video_batching(is_video_track) {
                            let binding_compatible = matches!(
                                target.local_forward_track.bind_result().await,
                                oxidesfu_rtc::ForwardTrackBindResult::Compatible { .. }
                            );
                            if prepared_video_batching_is_ready(
                                &mut target.prepared_video_batching_compatible,
                                target.local_forward_track.forwarding_mid(),
                                binding_compatible,
                            ) {
                                if target
                                    .pending_video_rtp_batch
                                    .needs_flush_before(rewritten_packet.header.timestamp)
                                {
                                    match flush_pending_video_rtp_batch(target).await {
                                        Ok(Some(batch)) => {
                                            record_completed_video_batch!(target, batch)
                                        }
                                        Ok(None) => {}
                                        Err(error) => record_video_batch_error!(target, error),
                                    }
                                }
                                let rewritten_wire_bytes =
                                    outgoing_rtp_wire_bytes(&rewritten_packet);
                                let flush_after = target.pending_video_rtp_batch.push(
                                    rewritten_packet,
                                    packet.header.payload_type,
                                    rewritten_wire_bytes,
                                );
                                if flush_after {
                                    match flush_pending_video_rtp_batch(target).await {
                                        Ok(Some(batch)) => {
                                            record_completed_video_batch!(target, batch)
                                        }
                                        Ok(None) => {}
                                        Err(error) => record_video_batch_error!(target, error),
                                    }
                                }
                                continue;
                            }
                        }

                        let forwarded_payload_type = rewritten_packet.header.payload_type;
                        let rewritten_payload_bytes = rewritten_packet.payload.len() as u64;
                        let rewritten_wire_bytes = outgoing_rtp_wire_bytes(&rewritten_packet);
                        let write_result = if is_video_track {
                            target
                                .local_forward_track
                                .write_rtp_with_cached_mid_preserving_extensions(rewritten_packet)
                                .await
                        } else {
                            target
                                .local_forward_track
                                .write_rtp_with_cached_mid(rewritten_packet)
                                .await
                        };

                        if let Err(error) = write_result {
                            if is_video_track {
                                target.video_write_errors += 1;
                                if target.video_write_errors % 50 == 0 {
                                    tracing::warn!(
                                        room = %room_name,
                                        publisher_identity = %publisher_identity,
                                        track_sid = %track_sid,
                                        subscriber_identity = %target.subscriber_identity(),
                                        write_errors = target.video_write_errors,
                                        error = %error,
                                        "video_forwarding_write_failed"
                                    );
                                }
                            } else {
                                target.audio_write_errors += 1;
                                if target.audio_write_errors % 50 == 0 {
                                    tracing::warn!(
                                        room = %room_name,
                                        publisher_identity = %publisher_identity,
                                        track_sid = %track_sid,
                                        subscriber_identity = %target.subscriber_identity(),
                                        write_errors = target.audio_write_errors,
                                        error = %error,
                                        "audio_forwarding_write_failed"
                                    );
                                }
                            }
                            continue;
                        }
                        if is_video_track {
                            target.video_counters.successful_rtp_packets += 1;
                            target.video_counters.successful_rtp_payload_bytes = target
                                .video_counters
                                .successful_rtp_payload_bytes
                                .saturating_add(rewritten_payload_bytes);
                            target
                                .rtp_window
                                .record_successful_write(rewritten_wire_bytes);
                        }

                        if !target.forwarded_once {
                            target.forwarded_once = true;
                            if !track_subscribed_signaled_to_publisher
                                && should_emit_track_subscribed_for_subscriber(
                                    &rooms,
                                    &room_name,
                                    &publisher_identity,
                                    target.subscriber_identity(),
                                )
                            {
                                signal_track_subscribed_to_publisher(
                                    &publisher_subscription_active_pairs,
                                    &signal_connections,
                                    &room_name,
                                    &publisher_identity,
                                    target.subscriber_identity(),
                                    &track_sid,
                                );
                                track_subscribed_signaled_to_publisher = true;
                            }
                            if is_video_track {
                                tracing::info!(
                                    room = %room_name,
                                    publisher_identity = %publisher_identity,
                                    track_sid = %track_sid,
                                    subscriber_identity = %target.subscriber_identity(),
                                    incoming_payload_type = packet.header.payload_type,
                                    forwarded_payload_type,
                                    "video_forwarding_first_packet_forwarded"
                                );
                            } else {
                                tracing::info!(
                                    room = %room_name,
                                    publisher_identity = %publisher_identity,
                                    track_sid = %track_sid,
                                    subscriber_identity = %target.subscriber_identity(),
                                    "audio_forwarding_first_packet_forwarded"
                                );
                            }
                        }
                    }

                    if collect_forwarding_debug_counts {
                        for target in &cached_forward_targets {
                            let layer_policy = target.video_layer_selector.policy();
                            let selected_incoming_ssrc =
                                target.video_layer_selector.selected_ssrc();
                            let selected_rid = selected_incoming_ssrc.and_then(|ssrc| {
                                video_ssrc_rids.get(&ssrc).and_then(|rid| rid.as_deref())
                            });
                            tracing::debug!(
                                room = %room_name,
                                publisher_identity = %publisher_identity,
                                track_sid = %track_sid,
                                subscriber_identity = %target.subscriber_identity(),
                                maximum_spatial = ?layer_policy.max,
                                desired_spatial = ?layer_policy.desired,
                                current_spatial = ?target.video_layer_selector.current_spatial(),
                                acquisition_state = ?target.video_layer_selector.acquisition_state(),
                                waiting_for_spatial = ?target.video_layer_selector.waiting_for(),
                                acquisition_ticks = target.video_layer_selector.acquisition_ticks(),
                                selector_pli_remaining = target.video_layer_selector.remaining_pli_requests(),
                                maximum_temporal = ?target.video_temporal_controller.policy().map(|policy| policy.max),
                                desired_temporal = ?target.video_temporal_controller.policy().map(|policy| policy.desired),
                                current_temporal = ?target.video_temporal_controller.current(),
                                selected_incoming_ssrc = ?selected_incoming_ssrc,
                                selected_rid = ?selected_rid,
                                layer_switches = target.video_counters.layer_switches,
                                drop_waiting_for_keyframe = target.video_counters.drop_waiting_for_keyframe,
                                drop_non_selected_ssrc = target.video_counters.drop_non_selected_ssrc,
                                drop_above_maximum = target.video_counters.drop_above_maximum,
                                drop_unknown_layer = target.video_counters.drop_unknown_layer,
                                drop_temporal_above_maximum = target.video_counters.drop_temporal_above_maximum,
                                drop_temporal_above_desired = target.video_counters.drop_temporal_above_desired,
                                drop_temporal_timestamp_cap = target.video_counters.drop_temporal_timestamp_cap,
                                selector_pli_requests = target.video_counters.selector_pli_requests,
                                rewrite_drops = target.video_counters.rewrite_drops,
                                successful_rtp_packets = target.video_counters.successful_rtp_packets,
                                successful_rtp_payload_bytes = target.video_counters.successful_rtp_payload_bytes,
                                video_write_errors = target.video_write_errors,
                                "video_forwarding_target_heartbeat"
                            );
                        }
                        tracing::debug!(
                            room = %room_name,
                            publisher_identity = %publisher_identity,
                            track_sid = %track_sid,
                            last_recv_event_kind = "rtp",
                            total_forward_targets,
                            filtered_out_targets,
                            media_forwarding_true_targets,
                            signaling_subscribed_targets,
                            room_subscribed_targets,
                            "video_forwarding_loop_heartbeat"
                        );
                    }
                }
                Ok(oxidesfu_rtc::RemoteTrackEvent::RtcpPacket(rtcp_packets)) => {
                    let actions = derive_rtcp_forward_actions(&rtcp_packets);
                    if actions.is_empty() {
                        continue;
                    }

                    let downstream_pli_received = rtcp_packets
                        .iter()
                        .filter(|packet| {
                            packet.as_any().is::<
                                rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
                            >()
                        })
                        .count() as u64;
                    let downstream_fir_received = rtcp_packets
                        .iter()
                        .filter(|packet| {
                            packet.as_any().is::<
                                rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest,
                            >()
                        })
                        .count() as u64;
                    let forward_tracks_for_track =
                        forward_tracks.list_for_track(&room_name, &publisher_identity, &track_sid);
                    for (key, local_forward_track) in forward_tracks_for_track {
                        let (key_room, key_publisher, key_track_sid, key_subscriber) = &key;
                        if !should_forward_media_for_subscriber_with_track_settings_in(
                            &forwarding_decision_context,
                            &key,
                        ) || !subscriber_within_track_type_subscription_limit(
                            &forwarding_decision_context,
                            &key,
                            Some(&track_info),
                        ) {
                            continue;
                        }

                        let effects = build_rtcp_outbound_effects(
                            &key,
                            &actions,
                            &rtp_forwarding,
                            current_unix_millis(),
                        );
                        let downstream_pli_sent = effects
                            .feedback_packets
                            .iter()
                            .filter(|packet| {
                                packet.as_any().is::<
                                    rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
                                >()
                            })
                            .count() as u64;
                        let downstream_fir_sent = effects
                            .feedback_packets
                            .iter()
                            .filter(|packet| {
                                packet.as_any().is::<
                                    rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest,
                                >()
                            })
                            .count() as u64;
                        if let Some(target) = cached_forward_targets
                            .iter_mut()
                            .find(|target| target.key == key)
                        {
                            target.downstream_feedback_counters.pli_received = target
                                .downstream_feedback_counters
                                .pli_received
                                .saturating_add(downstream_pli_received);
                            target.downstream_feedback_counters.pli_sent = target
                                .downstream_feedback_counters
                                .pli_sent
                                .saturating_add(downstream_pli_sent);
                            target.downstream_feedback_counters.pli_suppressed = target
                                .downstream_feedback_counters
                                .pli_suppressed
                                .saturating_add(
                                    downstream_pli_received.saturating_sub(downstream_pli_sent),
                                );
                            target.downstream_feedback_counters.fir_received = target
                                .downstream_feedback_counters
                                .fir_received
                                .saturating_add(downstream_fir_received);
                            target.downstream_feedback_counters.fir_sent = target
                                .downstream_feedback_counters
                                .fir_sent
                                .saturating_add(downstream_fir_sent);
                            target.downstream_feedback_counters.fir_suppressed = target
                                .downstream_feedback_counters
                                .fir_suppressed
                                .saturating_add(
                                    downstream_fir_received.saturating_sub(downstream_fir_sent),
                                );
                        }

                        let rtp_sink = LocalForwardTrackRtpSink {
                            track: local_forward_track,
                        };
                        let feedback_sink = RemoteTrackFeedbackSink {
                            track: remote_track.clone(),
                        };
                        let sender_report_sink = LocalForwardTrackSenderReportSink {
                            track: rtp_sink.track.clone(),
                        };
                        let recommended_video_quality = effects.recommended_video_quality;
                        if tokio::time::timeout(
                            RTCP_EFFECTS_TIMEOUT,
                            execute_rtcp_outbound_effects(
                                effects,
                                &rtp_sink,
                                &feedback_sink,
                                &sender_report_sink,
                            ),
                        )
                        .await
                        .is_err()
                        {
                            tracing::warn!(
                                room = %room_name,
                                publisher_identity = %publisher_identity,
                                track_sid = %track_sid,
                                subscriber_identity = %key_subscriber,
                                timeout_ms = RTCP_EFFECTS_TIMEOUT.as_millis(),
                                "rtcp_outbound_effects_timed_out"
                            );
                        }

                        if track_info.r#type == proto::TrackType::Video as i32
                            && let Some(quality) = recommended_video_quality
                        {
                            maybe_emit_recommended_subscribed_quality_update_in(
                                &RecommendedQualityUpdateContext {
                                    signal_connections: &signal_connections,
                                    track_settings: &track_settings,
                                    rooms: &rooms,
                                },
                                RecommendedQualityUpdate {
                                    room_name: key_room,
                                    publisher_identity: key_publisher,
                                    track_sid: key_track_sid,
                                    subscriber_identity: key_subscriber,
                                    recommended_quality: quality,
                                },
                            );
                        }
                    }
                }
                Ok(oxidesfu_rtc::RemoteTrackEvent::Ended) => {
                    tracing::info!(
                        room = %room_name,
                        publisher_identity = %publisher_identity,
                        track_sid = %track_sid,
                        is_video_track,
                        "forward_track_reader_remote_track_ended"
                    );
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        room = %room_name,
                        publisher_identity = %publisher_identity,
                        track_sid = %track_sid,
                        is_video_track,
                        error = %error,
                        "forward_track_reader_remote_track_error"
                    );
                    break;
                }
            }
        }

        if is_video_track {
            for target in &mut cached_forward_targets {
                if let Err(error) = flush_pending_video_rtp_batch(target).await {
                    tracing::warn!(
                        room = %room_name,
                        publisher_identity = %publisher_identity,
                        track_sid = %track_sid,
                        subscriber_identity = %target.subscriber_identity(),
                        write_errors = target.video_write_errors,
                        error = %error,
                        "video_forwarding_write_failed_before_reader_exit"
                    );
                }
            }
        }

        let released = forward_tracks.release_track_reader(reader_lease);
        tracing::debug!(
            room = %room_name,
            publisher_identity = %publisher_identity,
            track_sid = %track_sid,
            reader_lease = reader_lease_generation,
            released,
            "forward_track_reader_lease_released"
        );
    });

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn ensure_subscriber_forwarding_from_parts(
    state: &SignalState,
    peer_connections: &PeerConnectionStore,
    rooms: &RoomStore,
    media_forwarding: &MediaForwardingStore,
    pending_media_section_requests: &PendingMediaSectionRequestStore,
    media_subscriptions: &MediaSubscriptionStore,
    subscribe_permissions: &crate::stores::SubscribePermissionStore,
    auto_subscribe_preferences: &crate::stores::AutoSubscribePreferenceStore,
    forward_tracks: &ForwardTrackStore,
    subscriber_offer_ids: &crate::stores::SubscriberOfferIdStore,
    signal_connections: &SignalConnectionStore,
    room_name: &str,
    publisher_identity: &str,
    track: &proto::TrackInfo,
) -> oxidesfu_rtc::RtcResult<()> {
    if track.sid.is_empty() {
        return Ok(());
    }
    if signal_connections
        .get(room_name, publisher_identity)
        .is_none()
    {
        tracing::debug!(
            room = %room_name,
            publisher_identity = %publisher_identity,
            track_sid = %track.sid,
            "skipping_forwarding_from_rtp_for_disconnected_publisher"
        );
        return Ok(());
    }
    let Ok(publisher) = rooms.get_participant(room_name, publisher_identity) else {
        return Ok(());
    };
    let track_kind = if track.r#type == proto::TrackType::Video as i32 {
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video
    } else {
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio
    };

    let subscribers =
        peer_connections.media_receivers_in_room_except(room_name, publisher_identity);
    for (subscriber_identity, subscriber_pc, connection_kind) in subscribers {
        if !subscribe_permissions.can_subscribe(room_name, &subscriber_identity) {
            continue;
        }

        let default_subscribed =
            auto_subscribe_preferences.auto_subscribe_enabled(room_name, &subscriber_identity);
        let subscribed = media_subscriptions.is_subscribed_with_default(
            room_name,
            publisher_identity,
            &track.sid,
            &subscriber_identity,
            default_subscribed,
        );
        if !subscribed {
            tracing::debug!(
                room = %room_name,
                publisher_identity = %publisher_identity,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                default_subscribed,
                explicit_subscription = ?media_subscriptions.explicit_subscription(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    &subscriber_identity,
                ),
                rooms_subscribed = rooms.is_media_track_subscribed(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    &subscriber_identity,
                ),
                "single_pc_forwarding_from_rtp_not_subscribed"
            );
            if media_subscriptions
                .explicit_subscription(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    &subscriber_identity,
                )
                .is_none()
            {
                media_subscriptions.set_subscribed(
                    room_name,
                    publisher_identity,
                    &track.sid,
                    &subscriber_identity,
                    false,
                );
            }
            continue;
        }

        let forwarding_decision_context = ForwardingDecisionContext {
            media_subscriptions,
            auto_subscribe_preferences,
            track_settings: &state.track_settings,
            track_allocations: &state.track_allocations,
            rooms,
            subscription_limits: SubscriptionLimits {
                audio: state.media_subscription_limit_audio,
                video: state.media_subscription_limit_video,
            },
        };
        let forwarding_key = (
            room_name.to_string(),
            publisher_identity.to_string(),
            track.sid.clone(),
            subscriber_identity.clone(),
        );
        if !subscriber_within_track_type_subscription_limit(
            &forwarding_decision_context,
            &forwarding_key,
            Some(track),
        ) {
            tracing::debug!(
                room = %room_name,
                publisher_identity = %publisher_identity,
                subscriber_identity = %subscriber_identity,
                track_sid = %track.sid,
                audio_subscription_limit = ?state.media_subscription_limit_audio,
                video_subscription_limit = ?state.media_subscription_limit_video,
                "single_pc_forwarding_from_rtp_subscription_limit_reached"
            );
            continue;
        }

        if reject_unsupported_video_subscription_if_needed(
            state,
            room_name,
            publisher_identity,
            &subscriber_identity,
            track,
            false,
        ) {
            continue;
        }

        if connection_kind == MediaForwardingConnectionKind::SinglePcPublisher {
            if media_forwarding.contains(
                room_name,
                publisher_identity,
                &track.sid,
                &subscriber_identity,
            ) {
                continue;
            }
            let had_pending_request =
                pending_media_section_requests.has_for_subscriber(room_name, &subscriber_identity);
            let inserted = pending_media_section_requests.insert_once(
                room_name,
                publisher_identity,
                &track.sid,
                &subscriber_identity,
                pending_media_section_kind_from_track_kind(track_kind),
            );
            if inserted
                && !had_pending_request
                && pending_media_section_requests
                    .begin_negotiation_if_idle(room_name, &subscriber_identity)
                && let Some(subscriber_outbound_tx) =
                    signal_connections.get(room_name, &subscriber_identity)
            {
                signal_pending_media_section_requirement(
                    pending_media_section_requests,
                    room_name,
                    &subscriber_identity,
                    &subscriber_outbound_tx,
                )
                .await?;
            }
            continue;
        }
        if !media_forwarding.insert_once(
            room_name,
            publisher_identity,
            &track.sid,
            &subscriber_identity,
        ) {
            continue;
        }
        let existing_forwarding_count = forward_tracks
            .list_for_track(room_name, publisher_identity, &track.sid)
            .len();
        let forwarding_mime = selected_forwarding_mime_type_for_subscriber(
            state,
            room_name,
            &subscriber_identity,
            track,
            existing_forwarding_count,
        );
        let forward_track = subscriber_pc
            .add_forwarding_track_with_mime(
                &publisher.sid,
                &track.sid,
                track_kind,
                forwarding_mime.as_deref(),
            )
            .await?;
        forward_tracks.insert_inactive(
            room_name,
            publisher_identity,
            &track.sid,
            &subscriber_identity,
            forward_track,
        );
        if let Some(subscriber_outbound_tx) =
            signal_connections.get(room_name, &subscriber_identity)
        {
            signal_media_forwarding_negotiation_with_offer_id(
                state,
                subscriber_offer_ids,
                room_name,
                &subscriber_identity,
                &subscriber_pc,
                connection_kind,
                track_kind,
                &subscriber_outbound_tx,
            )
            .await?;
        }
    }

    Ok(())
}

async fn signal_media_forwarding_negotiation(
    peer_connection: &SharedPeerConnection,
    connection_kind: MediaForwardingConnectionKind,
    track_kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
    outbound_tx: &OutboundSignalSender,
) -> oxidesfu_rtc::RtcResult<()> {
    match connection_kind {
        MediaForwardingConnectionKind::DualPcSubscriber => {
            let offer_sdp = peer_connection.create_offer().await?;
            tracing::debug!(
                connection_kind = ?connection_kind,
                track_kind = ?track_kind,
                sdp = %offer_sdp,
                "media_forwarding_offer_created_without_offer_id"
            );
            let _ = outbound_tx.send(proto::SignalResponse {
                message: Some(proto::signal_response::Message::Offer(
                    proto::SessionDescription {
                        r#type: "offer".to_string(),
                        sdp: offer_sdp,
                        id: 0,
                        ..Default::default()
                    },
                )),
            });
        }
        MediaForwardingConnectionKind::SinglePcPublisher => {
            let (num_audios, num_videos) =
                if track_kind == rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video {
                    (0_u32, 1_u32)
                } else {
                    (1_u32, 0_u32)
                };
            let send_result = outbound_tx.send(proto::SignalResponse {
                message: Some(proto::signal_response::Message::MediaSectionsRequirement(
                    proto::MediaSectionsRequirement {
                        num_audios,
                        num_videos,
                    },
                )),
            });
            tracing::debug!(
                connection_kind = ?connection_kind,
                track_kind = ?track_kind,
                num_audios,
                num_videos,
                sent = send_result.is_ok(),
                "single_pc_media_section_requirement_sent_without_offer_id"
            );
        }
    }
    Ok(())
}

async fn signal_pending_media_section_requirement(
    pending_media_section_requests: &PendingMediaSectionRequestStore,
    room_name: &str,
    subscriber_identity: &str,
    outbound_tx: &OutboundSignalSender,
) -> oxidesfu_rtc::RtcResult<()> {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    pending_media_section_requests.release_requested_for_unresolved(room_name, subscriber_identity);
    let counts =
        pending_media_section_requests.take_unrequested_counts(room_name, subscriber_identity);
    tracing::debug!(
        room = %room_name,
        subscriber_identity = %subscriber_identity,
        counts = ?counts,
        "pending_media_section_requirement_counts_taken"
    );
    if counts.is_empty() {
        return Ok(());
    }
    let send_result = outbound_tx.send(proto::SignalResponse {
        message: Some(proto::signal_response::Message::MediaSectionsRequirement(
            proto::MediaSectionsRequirement {
                num_audios: counts.audios,
                num_videos: counts.videos,
            },
        )),
    });
    tracing::debug!(
        room = %room_name,
        subscriber_identity = %subscriber_identity,
        counts = ?counts,
        sent = send_result.is_ok(),
        "pending_media_section_requirement_sent"
    );
    Ok(())
}

fn schedule_pending_media_section_requirement_after_answer(
    state: SignalState,
    room_name: String,
    subscriber_identity: String,
) {
    if state.participant_uses_subscriber_primary(&room_name, &subscriber_identity) {
        return;
    }
    tracing::debug!(
        room = %room_name,
        subscriber_identity = %subscriber_identity,
        "pending_media_section_requirement_after_answer_scheduled"
    );
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        if !state
            .pending_media_section_requests
            .has_for_subscriber(&room_name, &subscriber_identity)
        {
            tracing::debug!(
                room = %room_name,
                subscriber_identity = %subscriber_identity,
                "pending_media_section_requirement_after_answer_no_pending"
            );
            return;
        }
        let Some(outbound_tx) = state
            .signal_connections
            .get(&room_name, &subscriber_identity)
        else {
            tracing::warn!(
                room = %room_name,
                subscriber_identity = %subscriber_identity,
                "pending_media_section_requirement_after_answer_no_signal_connection"
            );
            return;
        };
        let _ = signal_pending_media_section_requirement(
            &state.pending_media_section_requests,
            &room_name,
            &subscriber_identity,
            &outbound_tx,
        )
        .await;
    });
}

async fn emit_claimed_subscriber_forwarding_offer(
    state: &SignalState,
    subscriber_offer_ids: &crate::stores::SubscriberOfferIdStore,
    room_name: &str,
    subscriber_identity: &str,
    peer_connection: &SharedPeerConnection,
    track_kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
    outbound_tx: &OutboundSignalSender,
) -> oxidesfu_rtc::RtcResult<()> {
    let offer_sdp = match peer_connection.create_offer().await {
        Ok(offer_sdp) => offer_sdp,
        Err(error) => {
            state
                .subscriber_offer_negotiations
                .abort_offer_creation(room_name, subscriber_identity);
            return Err(error);
        }
    };
    let offer_id = subscriber_offer_ids.next_offer_id(room_name, subscriber_identity);
    state.remember_subscriber_offer_mid_track_ids(
        room_name,
        subscriber_identity,
        offer_id,
        crate::media::mid_to_track_id_from_offer_sdp(&offer_sdp),
    );
    state.subscriber_offer_negotiations.mark_offer_in_flight(
        room_name,
        subscriber_identity,
        offer_id,
    );
    tracing::debug!(
        room = %room_name,
        subscriber_identity = %subscriber_identity,
        offer_id,
        track_kind = ?track_kind,
        sdp = %offer_sdp,
        "subscriber_media_forwarding_offer_created"
    );
    let _ = outbound_tx.send(proto::SignalResponse {
        message: Some(proto::signal_response::Message::Offer(
            proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer_sdp,
                id: offer_id,
                ..Default::default()
            },
        )),
    });
    Ok(())
}

pub(crate) async fn signal_single_pc_sender_removal_negotiation(
    room_name: &str,
    subscriber_identity: &str,
    outbound_tx: &OutboundSignalSender,
) -> oxidesfu_rtc::RtcResult<()> {
    let send_result = outbound_tx.send(proto::SignalResponse {
        message: Some(proto::signal_response::Message::MediaSectionsRequirement(
            proto::MediaSectionsRequirement {
                num_audios: 0,
                num_videos: 0,
            },
        )),
    });
    tracing::debug!(
        room = %room_name,
        subscriber_identity,
        sent = send_result.is_ok(),
        "single_pc_sender_removal_renegotiation_requested"
    );
    Ok(())
}

async fn signal_server_offer_with_offer_id(
    state: &SignalState,
    subscriber_offer_ids: &crate::stores::SubscriberOfferIdStore,
    room_name: &str,
    subscriber_identity: &str,
    peer_connection: &SharedPeerConnection,
    track_kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
    outbound_tx: &OutboundSignalSender,
) -> oxidesfu_rtc::RtcResult<()> {
    if state
        .subscriber_offer_negotiations
        .request_offer(room_name, subscriber_identity)
        == crate::stores::SubscriberOfferNegotiationRequest::Coalesced
    {
        return Ok(());
    }

    emit_claimed_subscriber_forwarding_offer(
        state,
        subscriber_offer_ids,
        room_name,
        subscriber_identity,
        peer_connection,
        track_kind,
        outbound_tx,
    )
    .await
}

struct MediaForwardingNegotiationRequest<'a> {
    subscriber_offer_ids: &'a crate::stores::SubscriberOfferIdStore,
    room_name: &'a str,
    subscriber_identity: &'a str,
    peer_connection: &'a SharedPeerConnection,
    connection_kind: MediaForwardingConnectionKind,
    track_kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
    outbound_tx: &'a OutboundSignalSender,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn signal_media_forwarding_negotiation_with_offer_id(
    state: &SignalState,
    subscriber_offer_ids: &crate::stores::SubscriberOfferIdStore,
    room_name: &str,
    subscriber_identity: &str,
    peer_connection: &SharedPeerConnection,
    connection_kind: MediaForwardingConnectionKind,
    track_kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
    outbound_tx: &OutboundSignalSender,
) -> oxidesfu_rtc::RtcResult<()> {
    signal_media_forwarding_negotiation_with_request(
        state,
        MediaForwardingNegotiationRequest {
            subscriber_offer_ids,
            room_name,
            subscriber_identity,
            peer_connection,
            connection_kind,
            track_kind,
            outbound_tx,
        },
    )
    .await
}

async fn signal_media_forwarding_negotiation_with_request(
    state: &SignalState,
    request: MediaForwardingNegotiationRequest<'_>,
) -> oxidesfu_rtc::RtcResult<()> {
    let MediaForwardingNegotiationRequest {
        subscriber_offer_ids,
        room_name,
        subscriber_identity,
        peer_connection,
        connection_kind,
        track_kind,
        outbound_tx,
    } = request;
    match connection_kind {
        MediaForwardingConnectionKind::DualPcSubscriber => {
            signal_server_offer_with_offer_id(
                state,
                subscriber_offer_ids,
                room_name,
                subscriber_identity,
                peer_connection,
                track_kind,
                outbound_tx,
            )
            .await
        }
        MediaForwardingConnectionKind::SinglePcPublisher => {
            signal_media_forwarding_negotiation(
                peer_connection,
                connection_kind,
                track_kind,
                outbound_tx,
            )
            .await
        }
    }
}

pub(super) async fn reconcile_publisher_media_tracks_after_answer(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    offer_sdp: &str,
    mid_to_sdp_track_id: &HashMap<String, String>,
    mid_to_signal_cid: &HashMap<String, String>,
    single_pc_mode: bool,
) {
    let active_mids = active_publisher_mids_from_offer(offer_sdp);
    let offered_mids = publisher_mids_from_offer(offer_sdp);
    let offer_sections = offer_media_sections_from_sdp(offer_sdp);
    let media_kind_by_mid = offer_sections
        .iter()
        .filter_map(|section| section.kind.map(|kind| (section.mid.clone(), kind)))
        .collect::<HashMap<_, _>>();
    let sender_removed_mids = offer_sections
        .iter()
        .filter(|section| {
            section.direction == "inactive" || (!single_pc_mode && section.direction == "recvonly")
        })
        .map(|section| section.mid.clone())
        .collect::<HashSet<_>>();

    for (mid, sdp_track_id) in mid_to_sdp_track_id {
        let track_sid = if sdp_track_id.starts_with("TR_") {
            Some(sdp_track_id.clone())
        } else {
            state
                .media_track_cids
                .find_track_sid(room_name, publisher_identity, sdp_track_id)
                .or_else(|| {
                    mid_to_signal_cid.get(mid).and_then(|signal_cid| {
                        state.media_track_cids.find_track_sid(
                            room_name,
                            publisher_identity,
                            signal_cid,
                        )
                    })
                })
                .or_else(|| {
                    // LiveKit accepts a browser's changed SDP track ID only
                    // when one unbound publication of the same kind exists.
                    // Do not guess when multiple tracks could match.
                    let expected_track_type = match media_kind_by_mid.get(mid) {
                        Some(ReceiveSectionKind::Audio) => proto::TrackType::Audio as i32,
                        Some(ReceiveSectionKind::Video) => proto::TrackType::Video as i32,
                        None => return None,
                    };
                    let participant = state
                        .rooms
                        .get_participant(room_name, publisher_identity)
                        .ok()?;
                    let mut candidates = participant.tracks.iter().filter(|track| {
                        track.r#type == expected_track_type
                            && track.mid.is_empty()
                            && track.codecs.iter().all(|codec| codec.sdp_cid.is_empty())
                    });
                    let track_sid = candidates.next()?.sid.clone();
                    candidates.next().is_none().then_some(track_sid)
                })
        };

        if let Some(track_sid) = track_sid {
            // Browser SDP MSID track IDs are not required to equal the
            // application-signaled AddTrack CID. Preserve both aliases, as
            // LiveKit does with SimulcastCodecInfo.sdp_cid.
            state
                .media_track_cids
                .insert(room_name, publisher_identity, sdp_track_id, &track_sid);
            let mid_updated = state.rooms.set_participant_track_mid(
                room_name,
                publisher_identity,
                &track_sid,
                mid,
            );
            let sdp_cid_updated = state.rooms.set_participant_track_sdp_cid(
                room_name,
                publisher_identity,
                &track_sid,
                sdp_track_id,
            );
            if let Ok(participant) = sdp_cid_updated.or(mid_updated) {
                state.updates.broadcast_update(room_name, participant);
            }
        }
    }

    let Ok(publisher) = state.rooms.get_participant(room_name, publisher_identity) else {
        return;
    };
    let stale_tracks: Vec<proto::TrackInfo> = publisher
        .tracks
        .iter()
        .filter(|track| {
            let is_bound_to_removed_mid = !track.mid.is_empty()
                && track.codecs.iter().any(|codec| !codec.sdp_cid.is_empty())
                && offered_mids.contains(&track.mid)
                && !active_mids.contains(&track.mid)
                && sender_removed_mids.contains(&track.mid);

            let is_uniquely_unbound_dual_pc_track = !single_pc_mode
                && track.mid.is_empty()
                && track.codecs.iter().all(|codec| codec.sdp_cid.is_empty())
                && offer_sections.iter().any(|section| {
                    section.kind
                        == Some(if track.r#type == proto::TrackType::Video as i32 {
                            ReceiveSectionKind::Video
                        } else {
                            ReceiveSectionKind::Audio
                        })
                        && sender_removed_mids.contains(&section.mid)
                })
                && publisher
                    .tracks
                    .iter()
                    .filter(|candidate| {
                        candidate.r#type == track.r#type
                            && candidate.mid.is_empty()
                            && candidate
                                .codecs
                                .iter()
                                .all(|codec| codec.sdp_cid.is_empty())
                    })
                    .take(2)
                    .count()
                    == 1;

            is_bound_to_removed_mid || is_uniquely_unbound_dual_pc_track
        })
        .cloned()
        .collect();

    for track in stale_tracks {
        tracing::warn!(
            room = room_name,
            publisher_identity,
            track_sid = %track.sid,
            track_mid = %track.mid,
            active_mids = ?active_mids,
            offered_mids = ?offered_mids,
            sender_removed_mids = ?sender_removed_mids,
            offer_sections = ?offer_sections.iter().map(|section| (&section.mid, &section.direction, section.is_rejected)).collect::<Vec<_>>(),
            "removing_stale_publisher_track"
        );
        cleanup_publisher_forwarding_for_track(state, room_name, publisher_identity, &track).await;
        state
            .media_track_cids
            .remove_track_sid(room_name, publisher_identity, &track.sid);
        if let Ok(participant) =
            state
                .rooms
                .remove_participant_track(room_name, publisher_identity, &track.sid)
        {
            if let Some(room) = state
                .rooms
                .list_rooms(&[room_name.to_string()])
                .ok()
                .and_then(|mut rooms| rooms.pop())
            {
                state.emit_webhook_event(proto::WebhookEvent {
                    event: "track_unpublished".to_string(),
                    room: Some(super::reduced_room_for_track_event(&room)),
                    participant: Some(super::reduced_participant_for_track_event(&participant)),
                    track: Some(track.clone()),
                    id: super::next_webhook_event_id(),
                    created_at: super::webhook_created_at_unix_seconds(),
                    ..Default::default()
                });
            }
            state.updates.broadcast_update(room_name, participant);
        }
    }
}

fn publisher_mids_from_offer(offer_sdp: &str) -> HashSet<String> {
    offer_sdp
        .lines()
        .filter_map(|line| line.trim().strip_prefix("a=mid:").map(str::to_owned))
        .collect()
}

async fn cleanup_publisher_forwarding_for_track(
    state: &SignalState,
    room_name: &str,
    publisher_identity: &str,
    track: &proto::TrackInfo,
) {
    let removed_tracks =
        state
            .forward_tracks
            .remove_track(room_name, publisher_identity, &track.sid);
    state
        .media_forwarding
        .remove_track(room_name, publisher_identity, &track.sid);
    state
        .media_subscriptions
        .remove_track(room_name, publisher_identity, &track.sid);
    state
        .rtp_forwarding
        .remove_track(room_name, publisher_identity, &track.sid);

    let track_kind = if track.r#type == proto::TrackType::Video as i32 {
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video
    } else {
        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio
    };

    for (subscriber_identity, local_forward_track) in removed_tracks {
        let Some((subscriber_pc, connection_kind)) = state
            .peer_connections
            .media_receiver_for_identity(room_name, &subscriber_identity)
        else {
            continue;
        };
        if subscriber_pc
            .remove_forwarding_track(&local_forward_track)
            .await
            .is_err()
        {
            continue;
        }
        if connection_kind == MediaForwardingConnectionKind::SinglePcPublisher {
            continue;
        }
        let Some(subscriber_outbound_tx) = state
            .signal_connections
            .get(room_name, &subscriber_identity)
        else {
            continue;
        };
        let _ = signal_media_forwarding_negotiation_with_offer_id(
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

async fn relay_data_track_packet_with_rooms(
    data_channels: &DataChannelStore,
    data_track_subscriptions: &DataTrackSubscriptionStore,
    rooms: &RoomStore,
    room_name: &str,
    publisher_identity: &str,
    bytes: Vec<u8>,
) -> oxidesfu_rtc::RtcResult<usize> {
    let Some(pub_handle) = data_track_packet_handle(&bytes) else {
        return Ok(0);
    };
    let publisher_sid = rooms
        .get_participant(room_name, publisher_identity)
        .map(|participant| participant.sid)
        .unwrap_or_default();
    if publisher_sid.is_empty() {
        tracing::trace!(
            room = %room_name,
            publisher_identity,
            pub_handle,
            "data_track_relay_missing_publisher_sid"
        );
        return Ok(0);
    }

    let subscribers = data_track_subscriptions.subscribers_for_packet_with_publisher_sid(
        room_name,
        publisher_identity,
        &publisher_sid,
        pub_handle,
    );
    tracing::trace!(
        room = %room_name,
        publisher_identity,
        pub_handle,
        subscriber_count = subscribers.len(),
        packet_len = bytes.len(),
        "data_track_relay_resolved_subscribers"
    );

    let mut sent = 0;
    for (subscriber_identity, sub_handle) in subscribers {
        let Some(channel) = data_channels.get_with_kind_for_downstream(
            room_name,
            &subscriber_identity,
            DataChannelKind::DataTrack,
        ) else {
            tracing::trace!(
                room = %room_name,
                publisher_identity,
                pub_handle,
                subscriber_identity,
                sub_handle,
                "data_track_relay_missing_subscriber_channel"
            );
            continue;
        };
        if !channel.is_open().await.unwrap_or(false) {
            tracing::trace!(
                room = %room_name,
                publisher_identity,
                pub_handle,
                subscriber_identity,
                sub_handle,
                "data_track_relay_subscriber_channel_not_open"
            );
            continue;
        }
        let Some(packet) = rewrite_data_track_packet_handle(&bytes, sub_handle) else {
            tracing::trace!(
                room = %room_name,
                publisher_identity,
                pub_handle,
                subscriber_identity,
                sub_handle,
                "data_track_relay_handle_rewrite_failed"
            );
            continue;
        };
        match channel.send_bytes(&packet).await {
            Ok(()) => sent += 1,
            Err(error) => {
                tracing::debug!(
                    room = %room_name,
                    publisher_identity,
                    pub_handle,
                    subscriber_identity,
                    sub_handle,
                    error = %error,
                    "data_track_relay_subscriber_send_failed"
                );
            }
        }
    }
    tracing::trace!(
        room = %room_name,
        publisher_identity,
        pub_handle,
        sent,
        "data_track_relay_complete"
    );
    Ok(sent)
}

#[cfg(test)]
async fn relay_data_track_packet(
    data_channels: &DataChannelStore,
    data_track_subscriptions: &DataTrackSubscriptionStore,
    room_name: &str,
    publisher_identity: &str,
    bytes: Vec<u8>,
) -> oxidesfu_rtc::RtcResult<usize> {
    let Some(pub_handle) = data_track_packet_handle(&bytes) else {
        return Ok(0);
    };
    let subscribers =
        data_track_subscriptions.subscribers_for_packet(room_name, publisher_identity, pub_handle);

    let mut sent = 0;
    for (subscriber_identity, sub_handle) in subscribers {
        let Some(channel) = data_channels.get_with_kind_for_downstream(
            room_name,
            &subscriber_identity,
            DataChannelKind::DataTrack,
        ) else {
            continue;
        };
        if !channel.is_open().await.unwrap_or(false) {
            continue;
        }
        let Some(packet) = rewrite_data_track_packet_handle(&bytes, sub_handle) else {
            continue;
        };
        match channel.send_bytes(&packet).await {
            Ok(()) => sent += 1,
            Err(error) => {
                tracing::debug!(
                    room = %room_name,
                    publisher_identity,
                    sub_handle,
                    error = %error,
                    "data_track_relay_subscriber_send_failed"
                );
            }
        }
    }

    Ok(sent)
}

#[cfg(test)]
mod tests {
    #![allow(deprecated)]

    use std::{
        collections::{HashMap, HashSet},
        time::Duration,
    };

    use livekit_protocol as proto;
    use oxidesfu_room::RoomStore;
    use prost::Message;

    use super::{
        DependencyDescriptorForwardingDecision, ForwardingDecisionContext,
        ForwardingDecisionRevisions, ForwardingRtpWindow, FpsForwardingState,
        SubscriberVideoTemporalController, SubscriptionLimits, TemporalIngressDecision,
        TemporalLayer, add_track_response, allocation_available_outgoing_bitrate_bps,
        allocation_layout_weight, allocation_quality_for_budget,
        allocation_temporal_layer_for_budget, apply_publisher_codec_preferences_to_answer,
        av1_is_keyframe_start, cached_forwarding_decision_for_subscriber,
        clear_publisher_subscription_active_if_no_remaining_tracks, data_channel_kind_for_label,
        desired_video_quality_from_allocation, filter_h264_from_publisher_answer_for_client,
        force_sendonly_sections_without_msid_recvonly, h264_is_keyframe_start,
        normalize_incoming_data_packet, outgoing_rtp_wire_bytes,
        preferred_codec_mime_for_participant_track, relay_data_packet_after_channel_convergence,
        relay_data_track_packet, reliable_channel_label_rank,
        remove_unencodable_one_byte_extensions,
        reorder_section_media_line_payloads_for_preferred_codec,
        resolved_destination_identities_for_packet, rewrite_dependency_descriptor_for_target,
        selected_forwarding_mime_type_for_subscriber, signal_track_subscribed_to_publisher,
        video_is_decodable_switch_point,
        video_is_decodable_switch_point_with_dependency_descriptor, vp8_is_keyframe_start,
        vp9_is_keyframe_start,
    };
    use crate::{
        DataChannelStore,
        state::SignalState,
        stores::{DataTrackStore, DataTrackSubscriptionStore, SignalConnectionStore},
    };
    use oxidesfu_auth::{ApiKeyStore, TokenVerifier};
    use oxidesfu_rtc::{DataChannelKind, DependencyDescriptorMetadataSnapshot};

    fn video_batch_packet(sequence_number: u16, timestamp: u32, marker: bool) -> rtc::rtp::Packet {
        rtc::rtp::Packet {
            header: rtc::rtp::header::Header {
                sequence_number,
                timestamp,
                marker,
                payload_type: 98,
                extension: true,
                extension_profile: rtc::rtp::header::EXTENSION_PROFILE_ONE_BYTE,
                ..Default::default()
            },
            payload: vec![sequence_number as u8].into(),
        }
    }

    #[test]
    fn video_prepared_batch_flushes_on_marker_and_preserves_final_packet_fifo() {
        let mut batch = super::PendingVideoRtpBatch::default();
        let mut first = video_batch_packet(10, 9_000, false);
        first.header.extensions.push(rtc::rtp::header::Extension {
            id: 9,
            payload: b"final-target-local-descriptor".to_vec().into(),
        });
        assert!(!batch.push(first, 96, 25));
        assert!(batch.push(video_batch_packet(11, 9_000, true), 96, 13));

        let completed = batch
            .take()
            .expect("marker must complete the prepared batch");
        assert_eq!(completed.timestamp, 9_000);
        assert_eq!(completed.payload_bytes, 2);
        assert_eq!(completed.wire_bytes, 38);
        assert_eq!(
            completed
                .packets
                .iter()
                .map(|packet| packet.header.sequence_number)
                .collect::<Vec<_>>(),
            vec![10, 11]
        );
        assert_eq!(
            completed.packets[0].header.get_extension(9),
            Some(b"final-target-local-descriptor".to_vec().into()),
            "the batch must retain the final rewritten wire packet, not a pre-rewrite cache copy"
        );
    }

    #[test]
    fn video_prepared_batch_flushes_at_capacity_or_timestamp_boundary() {
        let mut batch = super::PendingVideoRtpBatch::default();
        for sequence_number in 0..super::MAX_PREPARED_VIDEO_RTP_BATCH_SIZE as u16 {
            assert_eq!(
                batch.push(video_batch_packet(sequence_number, 9_000, false), 96, 13),
                sequence_number as usize + 1 == super::MAX_PREPARED_VIDEO_RTP_BATCH_SIZE
            );
        }
        assert!(batch.needs_flush_before(12_000));
        assert_eq!(
            batch
                .take()
                .expect("capacity must complete a batch")
                .packets
                .len(),
            64
        );

        assert!(!batch.push(video_batch_packet(64, 12_000, false), 96, 13));
        assert!(batch.needs_flush_before(15_000));
    }

    #[test]
    fn only_video_targets_use_the_prepared_batch_path() {
        assert!(super::uses_prepared_video_batching(true));
        assert!(!super::uses_prepared_video_batching(false));
    }

    #[test]
    fn prepared_video_batching_retries_pending_binding_and_rejects_empty_mid() {
        let mut cached = None;
        assert!(
            !super::prepared_video_batching_is_ready(&mut cached, Some("0"), false),
            "a pending sender binding must not permanently disable batching"
        );
        assert_eq!(cached, None);
        assert!(super::prepared_video_batching_is_ready(
            &mut cached,
            Some("0"),
            true
        ));
        assert_eq!(cached, Some(true));

        let mut empty_mid = None;
        assert!(!super::prepared_video_batching_is_ready(
            &mut empty_mid,
            Some(""),
            true
        ));
        assert_eq!(empty_mid, None);
    }

    #[test]
    fn video_batch_normalization_removes_unencodable_empty_one_byte_extensions() {
        let mut packet = video_batch_packet(10, 9_000, true);
        packet.header.extensions = vec![
            rtc::rtp::header::Extension {
                id: 0,
                payload: Vec::new().into(),
            },
            rtc::rtp::header::Extension {
                id: 4,
                payload: Vec::new().into(),
            },
            rtc::rtp::header::Extension {
                id: 9,
                payload: b"final".to_vec().into(),
            },
        ];

        remove_unencodable_one_byte_extensions(&mut packet);

        assert_eq!(
            packet.header.get_extension(9),
            Some(b"final".to_vec().into())
        );
        assert_eq!(packet.header.extensions.len(), 1);
        assert!(packet.header.extension);
    }

    #[test]
    fn dependency_descriptor_rewrite_is_target_local_and_strips_when_unnegotiated() {
        let source_packet = rtc::rtp::Packet {
            header: rtc::rtp::header::Header {
                extension: true,
                extension_profile: rtc::rtp::header::EXTENSION_PROFILE_ONE_BYTE,
                extensions: vec![
                    rtc::rtp::header::Extension {
                        id: 1,
                        payload: b"other".to_vec().into(),
                    },
                    rtc::rtp::header::Extension {
                        id: 3,
                        payload: vec![0, 0, 0, 0b0100_0110].into(),
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        let source_payload = source_packet
            .header
            .get_extension(3)
            .expect("fixture packet should carry the source descriptor");
        let snapshot = DependencyDescriptorMetadataSnapshot {
            source_extension_id: 3,
            frame_number: 1,
            first_packet_in_frame: true,
            has_switching_decode_target: true,
            spatial_id: 1,
            temporal_id: 0,
            active_decode_targets: 0b11,
            target_layers: Vec::new(),
            decode_target_indications: vec![
                rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication::Required,
                rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication::Switch,
            ],
            frame_diffs: Vec::new(),
            chain_diffs: Vec::new(),
            target_protected_by_chain: Vec::new(),
        };

        let mut target_packet = source_packet.clone();
        rewrite_dependency_descriptor_for_target(
            &mut target_packet,
            &snapshot,
            DependencyDescriptorForwardingDecision::Forward { target: 1 },
            Some(9),
        );
        let expected_payload = rtc::rtp::extension::dependency_descriptor_extension::replace_or_inject_active_decode_target_mask(
            &source_payload,
            2,
            0b10,
        )
        .expect("fixture descriptor should support a target mask rewrite");
        assert_eq!(source_packet.header.extensions[1].payload, source_payload);
        assert!(
            target_packet
                .header
                .extensions
                .iter()
                .all(|extension| extension.id != 3)
        );
        assert_eq!(
            target_packet.header.get_extension(9),
            Some(expected_payload.into())
        );
        assert_eq!(
            target_packet.header.get_extension(1),
            Some(b"other".to_vec().into())
        );

        let mut unnegotiated_packet = source_packet.clone();
        rewrite_dependency_descriptor_for_target(
            &mut unnegotiated_packet,
            &snapshot,
            DependencyDescriptorForwardingDecision::Forward { target: 1 },
            None,
        );
        assert!(
            unnegotiated_packet
                .header
                .extensions
                .iter()
                .all(|extension| extension.id != 3),
            "an unnegotiated destination must not leak the publisher extension ID"
        );
        assert_eq!(
            unnegotiated_packet.header.get_extension(1),
            Some(b"other".to_vec().into())
        );
    }

    #[test]
    fn forwarding_rtp_window_reports_wire_packet_rates_not_payload_totals() {
        let mut window = ForwardingRtpWindow::default();
        window.record_successful_write(1_500);
        window.record_successful_write(900);

        let snapshot = window.finish_window(Duration::from_secs(3));
        assert_eq!(snapshot.packets, 2);
        assert_eq!(snapshot.wire_bytes, 2_400);
        assert_eq!(snapshot.packets_per_second, 0);
        assert_eq!(snapshot.wire_bytes_per_second, 800);
        assert_eq!(window.finish_window(Duration::from_secs(3)).packets, 0);
    }

    #[test]
    fn outgoing_rtp_wire_bytes_includes_the_rtp_header() {
        let packet = rtc::rtp::Packet {
            header: rtc::rtp::header::Header {
                ssrc: 123,
                ..Default::default()
            },
            payload: vec![0; 100].into(),
        };

        assert_eq!(outgoing_rtp_wire_bytes(&packet), 112);
    }

    #[test]
    fn forwarding_snapshot_store_retains_a_bounded_json_lines_tail() {
        let store = super::forwarding_snapshot::ForwardingSnapshotStore::new(2);
        for sequence in 1..=3 {
            store.push(super::forwarding_snapshot::ForwardingSnapshot {
                schema_version: 1,
                sequence,
                window_duration_ms: 3_000,
                room: "room".to_string(),
                publisher_identity: "publisher".to_string(),
                track_sid: "track".to_string(),
                targets: Vec::new(),
            });
        }

        let output = store.json_lines();
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"sequence\":2"));
        assert!(lines[1].contains("\"sequence\":3"));
    }

    #[test]
    fn allocation_bitrate_source_prefers_test_override_and_falls_back_to_rtc_estimate() {
        assert_eq!(
            allocation_available_outgoing_bitrate_bps(Some(150_000), Some(900_000)),
            Some(150_000),
            "test support must override the production RTC estimate when explicitly set"
        );
        assert_eq!(
            allocation_available_outgoing_bitrate_bps(None, Some(900_000)),
            Some(900_000),
            "production allocation must use the RTC estimate when no test override exists"
        );
        assert_eq!(
            allocation_available_outgoing_bitrate_bps(None, None),
            None,
            "an absent override and absent RTC estimate must retain allocation removal behavior"
        );
    }

    #[test]
    fn preferred_codec_mime_for_participant_track_prefers_requested_video_codec() {
        let track = proto::TrackInfo {
            r#type: proto::TrackType::Video as i32,
            mime_type: "video/vp8".to_string(),
            codecs: vec![
                proto::SimulcastCodecInfo {
                    mime_type: "video/h264".to_string(),
                    ..Default::default()
                },
                proto::SimulcastCodecInfo {
                    mime_type: "video/vp8".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            preferred_codec_mime_for_participant_track(&track).as_deref(),
            Some("video/h264")
        );
    }

    #[test]
    fn preferred_codec_mime_for_participant_track_prefers_audio_red_when_enabled_for_opus() {
        let track = proto::TrackInfo {
            r#type: proto::TrackType::Audio as i32,
            mime_type: "audio/opus".to_string(),
            disable_red: false,
            codecs: vec![proto::SimulcastCodecInfo {
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(
            preferred_codec_mime_for_participant_track(&track).as_deref(),
            Some("audio/red")
        );
    }

    #[test]
    fn preferred_codec_mime_for_participant_track_respects_disable_red_and_non_opus_audio() {
        let disable_red_track = proto::TrackInfo {
            r#type: proto::TrackType::Audio as i32,
            mime_type: "audio/opus".to_string(),
            disable_red: true,
            codecs: vec![proto::SimulcastCodecInfo {
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            preferred_codec_mime_for_participant_track(&disable_red_track).as_deref(),
            Some("audio/opus")
        );

        let pcma_track = proto::TrackInfo {
            r#type: proto::TrackType::Audio as i32,
            mime_type: "audio/pcma".to_string(),
            disable_red: false,
            codecs: vec![proto::SimulcastCodecInfo {
                mime_type: "audio/pcma".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            preferred_codec_mime_for_participant_track(&pcma_track).as_deref(),
            Some("audio/pcma")
        );
    }

    #[test]
    fn reorder_section_media_line_payloads_for_preferred_codec_prioritizes_matching_payload() {
        let mut section = vec![
            "m=audio 9 UDP/TLS/RTP/SAVPF 111 63\r\n".to_string(),
            "a=rtpmap:111 opus/48000/2\r\n".to_string(),
            "a=rtpmap:63 red/48000/2\r\n".to_string(),
        ];

        reorder_section_media_line_payloads_for_preferred_codec(&mut section, "audio/red");
        assert_eq!(section[0], "m=audio 9 UDP/TLS/RTP/SAVPF 63 111\r\n");
    }

    fn test_signal_state() -> SignalState {
        let mut keys = ApiKeyStore::new();
        keys.insert("devkey", "secret");
        SignalState::new(RoomStore::default(), TokenVerifier::new(keys))
    }

    #[test]
    fn selected_forwarding_mime_type_for_subscriber_uses_first_published_codec_when_no_preference()
    {
        let state = test_signal_state();
        let track = proto::TrackInfo {
            r#type: proto::TrackType::Video as i32,
            codecs: vec![
                proto::SimulcastCodecInfo {
                    mime_type: "video/vp8".to_string(),
                    ..Default::default()
                },
                proto::SimulcastCodecInfo {
                    mime_type: "video/h264".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let selected =
            selected_forwarding_mime_type_for_subscriber(&state, "room", "subscriber", &track, 0);
        assert_eq!(selected.as_deref(), Some("video/vp8"));
    }

    #[test]
    fn selected_forwarding_mime_type_for_subscriber_rotates_published_audio_codecs() {
        let state = test_signal_state();
        let track = proto::TrackInfo {
            r#type: proto::TrackType::Audio as i32,
            codecs: vec![
                proto::SimulcastCodecInfo {
                    mime_type: "audio/pcmu".to_string(),
                    ..Default::default()
                },
                proto::SimulcastCodecInfo {
                    mime_type: "audio/opus".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let selected =
            selected_forwarding_mime_type_for_subscriber(&state, "room", "subscriber", &track, 1);
        assert_eq!(selected.as_deref(), Some("audio/opus"));
    }

    #[test]
    fn selected_forwarding_mime_type_for_subscriber_prefers_subscriber_supported_video_codec() {
        let state = test_signal_state();
        state.remember_participant_subscribe_video_mime_types(
            "room",
            "subscriber",
            &HashSet::from(["video/h264".to_string()]),
        );

        let track = proto::TrackInfo {
            r#type: proto::TrackType::Video as i32,
            codecs: vec![
                proto::SimulcastCodecInfo {
                    mime_type: "video/vp8".to_string(),
                    ..Default::default()
                },
                proto::SimulcastCodecInfo {
                    mime_type: "video/h264".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let selected =
            selected_forwarding_mime_type_for_subscriber(&state, "room", "subscriber", &track, 0);
        assert_eq!(selected.as_deref(), Some("video/h264"));
    }

    #[tokio::test]
    async fn add_track_response_preserves_muted_flag_in_response_and_room_snapshot() {
        let state = test_signal_state();
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
            .publish_permissions
            .set_can_publish_media("room", "publisher", true);

        let response = add_track_response(
            &state,
            "room",
            "publisher",
            proto::AddTrackRequest {
                cid: "cid-audio".to_string(),
                name: "mic".to_string(),
                r#type: proto::TrackType::Audio as i32,
                source: proto::TrackSource::Microphone as i32,
                muted: true,
                ..Default::default()
            },
        )
        .await;

        let Some(proto::signal_response::Message::TrackPublished(track_published)) =
            response.message
        else {
            panic!("expected TrackPublished response");
        };
        let track = track_published.track.expect("published track should exist");
        assert!(track.muted, "track in add-track response should stay muted");

        let participant = state
            .rooms
            .get_participant("room", "publisher")
            .expect("publisher snapshot should exist");
        assert!(
            participant
                .tracks
                .iter()
                .any(|stored| stored.sid == track.sid && stored.muted),
            "room snapshot should persist muted state for published track"
        );
    }

    #[test]
    fn apply_publisher_codec_preferences_to_answer_prefers_requested_video_and_audio_codecs() {
        let state = test_signal_state();
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
            .add_participant_track(
                "room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_video".to_string(),
                    r#type: proto::TrackType::Video as i32,
                    codecs: vec![
                        proto::SimulcastCodecInfo {
                            mime_type: "video/h264".to_string(),
                            ..Default::default()
                        },
                        proto::SimulcastCodecInfo {
                            mime_type: "video/vp8".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            )
            .expect("video track should add");
        state
            .rooms
            .add_participant_track(
                "room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_audio".to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    codecs: vec![proto::SimulcastCodecInfo {
                        mime_type: "audio/pcma".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .expect("audio track should add");

        state
            .media_track_cids
            .insert("room", "publisher", "cid-video", "TR_video");
        state
            .media_track_cids
            .insert("room", "publisher", "cid-audio", "TR_audio");

        let answer_sdp = concat!(
            "v=0\r\n",
            "o=- 1 2 IN IP4 127.0.0.1\r\n",
            "s=-\r\n",
            "t=0 0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96 102\r\n",
            "a=mid:0\r\n",
            "a=rtpmap:96 VP8/90000\r\n",
            "a=rtpmap:102 H264/90000\r\n",
            "m=audio 9 UDP/TLS/RTP/SAVPF 111 8\r\n",
            "a=mid:1\r\n",
            "a=rtpmap:111 opus/48000/2\r\n",
            "a=rtpmap:8 PCMA/8000\r\n",
        );

        let reordered = apply_publisher_codec_preferences_to_answer(
            &state,
            "room",
            "publisher",
            answer_sdp,
            &HashMap::from([
                ("0".to_string(), "cid-video".to_string()),
                ("1".to_string(), "cid-audio".to_string()),
            ]),
        );

        assert!(
            reordered.contains("m=video 9 UDP/TLS/RTP/SAVPF 102 96\r\n"),
            "video section should prefer H264 first: {reordered}"
        );
        assert!(
            reordered.contains("m=audio 9 UDP/TLS/RTP/SAVPF 8 111\r\n"),
            "audio section should prefer PCMA first: {reordered}"
        );
    }

    #[test]
    fn apply_publisher_codec_preferences_to_answer_prefers_red_when_audio_red_enabled() {
        let state = test_signal_state();
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
            .add_participant_track(
                "room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_red_enabled".to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    disable_red: false,
                    codecs: vec![proto::SimulcastCodecInfo {
                        mime_type: "audio/opus".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .expect("red-enabled audio track should add");
        state
            .rooms
            .add_participant_track(
                "room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_red_disabled".to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    disable_red: true,
                    codecs: vec![proto::SimulcastCodecInfo {
                        mime_type: "audio/opus".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .expect("red-disabled audio track should add");

        let answer_sdp = concat!(
            "v=0\r\n",
            "o=- 1 2 IN IP4 127.0.0.1\r\n",
            "s=-\r\n",
            "t=0 0\r\n",
            "m=audio 9 UDP/TLS/RTP/SAVPF 111 63\r\n",
            "a=mid:0\r\n",
            "a=rtpmap:111 opus/48000/2\r\n",
            "a=rtpmap:63 red/48000/2\r\n",
            "m=audio 9 UDP/TLS/RTP/SAVPF 111 63\r\n",
            "a=mid:1\r\n",
            "a=rtpmap:111 opus/48000/2\r\n",
            "a=rtpmap:63 red/48000/2\r\n",
        );

        let reordered = apply_publisher_codec_preferences_to_answer(
            &state,
            "room",
            "publisher",
            answer_sdp,
            &HashMap::from([
                ("0".to_string(), "TR_red_enabled".to_string()),
                ("1".to_string(), "TR_red_disabled".to_string()),
            ]),
        );

        assert!(
            reordered.contains("m=audio 9 UDP/TLS/RTP/SAVPF 63 111\r\na=mid:0\r\n"),
            "red-enabled section should prefer RED first: {reordered}"
        );
        assert!(
            reordered.contains("m=audio 9 UDP/TLS/RTP/SAVPF 111 63\r\na=mid:1\r\n"),
            "red-disabled section should keep OPUS first: {reordered}"
        );
    }

    #[test]
    fn cached_forwarding_decision_enforces_subscription_limits_and_promotes_pending_track() {
        let rooms = RoomStore::default();
        rooms
            .join_participant(
                "room",
                "publisher",
                "Publisher",
                String::new(),
                HashMap::new(),
            )
            .expect("publisher should join");
        rooms
            .join_participant(
                "room",
                "subscriber",
                "Subscriber",
                String::new(),
                HashMap::new(),
            )
            .expect("subscriber should join");
        rooms
            .add_participant_track(
                "room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_audio_a".to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    ..Default::default()
                },
            )
            .expect("first audio track should add");
        rooms
            .add_participant_track(
                "room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_audio_b".to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    ..Default::default()
                },
            )
            .expect("second audio track should add");

        let media_subscriptions = crate::stores::MediaSubscriptionStore::default();
        let auto_subscribe_preferences = crate::stores::AutoSubscribePreferenceStore::default();
        let track_settings = crate::media::TrackSettingsStore::default();
        let track_allocations = crate::media::TrackAllocationStore::default();
        media_subscriptions.set_subscribed("room", "publisher", "TR_audio_a", "subscriber", true);
        media_subscriptions.set_subscribed("room", "publisher", "TR_audio_b", "subscriber", true);
        let _ =
            rooms.set_media_track_subscribed("room", "publisher", "TR_audio_a", "subscriber", true);
        let _ =
            rooms.set_media_track_subscribed("room", "publisher", "TR_audio_b", "subscriber", true);

        let track_info = proto::TrackInfo {
            sid: "TR_audio_b".to_string(),
            r#type: proto::TrackType::Audio as i32,
            ..Default::default()
        };

        let forwarding_decision_context = ForwardingDecisionContext {
            media_subscriptions: &media_subscriptions,
            auto_subscribe_preferences: &auto_subscribe_preferences,
            track_settings: &track_settings,
            track_allocations: &track_allocations,
            rooms: &rooms,
            subscription_limits: SubscriptionLimits {
                audio: Some(1),
                video: None,
            },
        };
        let mut cache = HashMap::new();
        let key_b = (
            "room".to_string(),
            "publisher".to_string(),
            "TR_audio_b".to_string(),
            "subscriber".to_string(),
        );
        let revisions = ForwardingDecisionRevisions {
            media_subscription: media_subscriptions.revision(),
            auto_subscribe: auto_subscribe_preferences.revision(),
            track_settings: track_settings.revision(),
            track_allocation: track_allocations.revision(),
            room_media_subscription: rooms.media_subscription_revision(),
        };

        let blocked = cached_forwarding_decision_for_subscriber(
            &mut cache,
            revisions,
            &forwarding_decision_context,
            Some(&track_info),
            &key_b,
        );
        assert!(
            !blocked.should_forward_media,
            "second desired audio track should be limited while one audio subscription slot is occupied"
        );

        media_subscriptions.set_subscribed("room", "publisher", "TR_audio_a", "subscriber", false);
        let _ = rooms.set_media_track_subscribed(
            "room",
            "publisher",
            "TR_audio_a",
            "subscriber",
            false,
        );
        let promoted_revisions = ForwardingDecisionRevisions {
            media_subscription: media_subscriptions.revision(),
            auto_subscribe: auto_subscribe_preferences.revision(),
            track_settings: track_settings.revision(),
            track_allocation: track_allocations.revision(),
            room_media_subscription: rooms.media_subscription_revision(),
        };

        let promoted = cached_forwarding_decision_for_subscriber(
            &mut cache,
            promoted_revisions,
            &forwarding_decision_context,
            Some(&track_info),
            &key_b,
        );
        assert!(
            promoted.should_forward_media,
            "pending desired audio track should become forwardable once an occupied slot is released"
        );
    }

    #[test]
    fn cached_forwarding_decision_invalidates_when_revisions_change() {
        let rooms = RoomStore::default();
        rooms
            .join_participant(
                "room",
                "publisher",
                "Publisher",
                String::new(),
                HashMap::new(),
            )
            .expect("publisher should join");
        rooms
            .join_participant(
                "room",
                "subscriber",
                "Subscriber",
                String::new(),
                HashMap::new(),
            )
            .expect("subscriber should join");
        rooms
            .add_participant_track(
                "room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    r#type: proto::TrackType::Video as i32,
                    ..Default::default()
                },
            )
            .expect("track should add");

        let media_subscriptions = crate::stores::MediaSubscriptionStore::default();
        let auto_subscribe_preferences = crate::stores::AutoSubscribePreferenceStore::default();
        let track_settings = crate::media::TrackSettingsStore::default();
        let track_allocations = crate::media::TrackAllocationStore::default();
        let forwarding_decision_context = ForwardingDecisionContext {
            media_subscriptions: &media_subscriptions,
            auto_subscribe_preferences: &auto_subscribe_preferences,
            track_settings: &track_settings,
            track_allocations: &track_allocations,
            rooms: &rooms,
            subscription_limits: SubscriptionLimits {
                audio: None,
                video: None,
            },
        };
        let mut cache = HashMap::new();
        let key = (
            "room".to_string(),
            "publisher".to_string(),
            "TR_test".to_string(),
            "subscriber".to_string(),
        );

        let revisions = ForwardingDecisionRevisions {
            media_subscription: media_subscriptions.revision(),
            auto_subscribe: auto_subscribe_preferences.revision(),
            track_settings: track_settings.revision(),
            track_allocation: track_allocations.revision(),
            room_media_subscription: rooms.media_subscription_revision(),
        };
        let initial = cached_forwarding_decision_for_subscriber(
            &mut cache,
            revisions,
            &forwarding_decision_context,
            None,
            &key,
        );
        assert!(initial.should_forward_media);

        track_settings.upsert_from_request(
            "room",
            "subscriber",
            &proto::UpdateTrackSettings {
                track_sids: vec!["TR_test".to_string()],
                disabled: true,
                ..Default::default()
            },
        );
        let updated_revisions = ForwardingDecisionRevisions {
            media_subscription: media_subscriptions.revision(),
            auto_subscribe: auto_subscribe_preferences.revision(),
            track_settings: track_settings.revision(),
            track_allocation: track_allocations.revision(),
            room_media_subscription: rooms.media_subscription_revision(),
        };
        let updated = cached_forwarding_decision_for_subscriber(
            &mut cache,
            updated_revisions,
            &forwarding_decision_context,
            None,
            &key,
        );
        assert!(!updated.should_forward_media);
    }

    async fn connected_data_channel_pair() -> (
        oxidesfu_rtc::PeerConnection,
        oxidesfu_rtc::PeerConnection,
        oxidesfu_rtc::DataChannel,
        oxidesfu_rtc::DataChannel,
    ) {
        connected_data_channel_pair_with_offer_block_write(false).await
    }

    async fn connected_data_channel_pair_with_offer_block_write(
        offer_block_write: bool,
    ) -> (
        oxidesfu_rtc::PeerConnection,
        oxidesfu_rtc::PeerConnection,
        oxidesfu_rtc::DataChannel,
        oxidesfu_rtc::DataChannel,
    ) {
        let (offerer, offerer_events) = if offer_block_write {
            oxidesfu_rtc::create_peer_connection_with_events_with_data_channel_block_write()
                .await
                .expect("offerer peer connection should create")
        } else {
            oxidesfu_rtc::create_peer_connection_with_events()
                .await
                .expect("offerer peer connection should create")
        };
        let (answerer, answerer_events) = oxidesfu_rtc::create_peer_connection_with_events()
            .await
            .expect("answerer peer connection should create");

        let oxidesfu_rtc::PeerConnectionEvents {
            ice_candidates: mut offerer_ice_candidates,
            data_channels: _,
            remote_tracks: _,
        } = offerer_events;
        let oxidesfu_rtc::PeerConnectionEvents {
            ice_candidates: mut answerer_ice_candidates,
            data_channels: mut answerer_data_channels,
            remote_tracks: _,
        } = answerer_events;

        let offer_channel = offerer
            .create_data_channel("data")
            .await
            .expect("offerer data channel should create");
        let offer_sdp = offerer.create_offer().await.expect("offer should create");
        let answer_sdp = answerer
            .create_answer_for_offer(offer_sdp)
            .await
            .expect("answer should create");
        offerer
            .set_remote_answer(answer_sdp)
            .await
            .expect("answer should apply to offerer");

        let open_channel = offer_channel.clone();
        let mut open_wait = Box::pin(open_channel.wait_open());
        let mut answer_channel_wait = Box::pin(async move {
            answerer_data_channels
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("answerer data channel stream ended"))
        });
        let mut open_completed = false;

        let answer_channel = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = offerer_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            answerer
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("offerer candidate should add to answerer");
                        }
                    }
                    candidate = answerer_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            offerer
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("answerer candidate should add to offerer");
                        }
                    }
                    result = &mut open_wait, if !open_completed => {
                        result.expect("offerer data channel should open");
                        open_completed = true;
                    }
                    result = &mut answer_channel_wait => {
                        break result
                            .expect("answer data channel should be available");
                    }
                }
            }
        })
        .await
        .expect("data channel should connect before timeout");

        (offerer, answerer, offer_channel, answer_channel)
    }

    async fn recv_packet_with_timeout(
        channel: &oxidesfu_rtc::DataChannel,
        timeout: Duration,
    ) -> Option<proto::DataPacket> {
        match tokio::time::timeout(timeout, channel.recv_bytes()).await {
            Ok(Ok(bytes)) => Some(
                proto::DataPacket::decode(bytes.as_slice())
                    .expect("received bytes should decode as data packet"),
            ),
            _ => None,
        }
    }

    async fn recv_bytes_with_timeout(
        channel: &oxidesfu_rtc::DataChannel,
        timeout: Duration,
    ) -> Option<Vec<u8>> {
        match tokio::time::timeout(timeout, channel.recv_bytes()).await {
            Ok(Ok(bytes)) => Some(bytes),
            _ => None,
        }
    }

    fn encode_data_track_packet_with_handle(pub_handle: u16, payload_tag: u8) -> Vec<u8> {
        let mut bytes = vec![0u8; 12];
        bytes[2..4].copy_from_slice(&pub_handle.to_be_bytes());
        bytes.push(payload_tag);
        bytes
    }

    fn encode_data_track_packet_with_sequence(pub_handle: u16, sequence: u32) -> Vec<u8> {
        let mut bytes = vec![0u8; 12];
        bytes[2..4].copy_from_slice(&pub_handle.to_be_bytes());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes
    }

    fn encode_data_track_packet_with_payload(pub_handle: u16, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0u8; 12];
        bytes[2..4].copy_from_slice(&pub_handle.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn packet_handle(bytes: &[u8]) -> u16 {
        u16::from_be_bytes([bytes[2], bytes[3]])
    }

    fn packet_sequence(bytes: &[u8]) -> Option<u32> {
        if bytes.len() < 16 {
            return None;
        }
        Some(u32::from_be_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]))
    }

    fn setup_room_with_participants(room: &str, identities: &[&str]) -> RoomStore {
        let rooms = RoomStore::default();
        for identity in identities {
            rooms
                .join_participant(room, identity, identity, String::new(), HashMap::new())
                .expect("participant should join");
        }
        rooms
    }

    #[tokio::test]
    async fn reliable_data_channel_recovers_after_receiver_starts_draining() {
        let (offerer, answerer, sender, receiver) =
            connected_data_channel_pair_with_offer_block_write(true).await;
        const THRESHOLD: u32 = 1_024;
        const PAYLOAD_SIZE: usize = 100;

        sender.set_slow_reader_bitrate_threshold_bps(THRESHOLD);
        sender
            .set_buffered_amount_low_threshold(THRESHOLD / 2)
            .await
            .expect("low buffered threshold should apply");
        sender
            .set_buffered_amount_high_threshold(THRESHOLD)
            .await
            .expect("high buffered threshold should apply");

        let recovery_payload = vec![7; PAYLOAD_SIZE];
        let recovery_payload_for_receiver = recovery_payload.clone();
        let (detached_tx, detached_rx) = tokio::sync::oneshot::channel();
        let (drain_tx, drain_rx) = tokio::sync::oneshot::channel();
        let receiver_task = tokio::spawn(async move {
            let seed = receiver
                .recv_bytes()
                .await
                .expect("receiver should detach and receive its seed message");
            assert_eq!(seed, b"seed");
            let _ = detached_tx.send(());
            let _ = drain_rx.await;

            loop {
                let bytes = tokio::time::timeout(Duration::from_secs(5), receiver.recv_bytes())
                    .await
                    .expect("receiver should make progress after draining starts")
                    .expect("receiver data channel should remain open");
                if bytes == recovery_payload_for_receiver {
                    break;
                }
            }
        });

        sender
            .send_bytes(b"seed")
            .await
            .expect("seed should send before receiver is stalled");
        tokio::time::timeout(Duration::from_secs(5), detached_rx)
            .await
            .expect("receiver should detach before the measured send loop")
            .expect("receiver task should report detachment");

        let mut saw_slow_reader_drop = false;
        for sequence in 1..=20_000u64 {
            let mut payload = vec![0; PAYLOAD_SIZE];
            payload[(PAYLOAD_SIZE - 8)..].copy_from_slice(&sequence.to_be_bytes());
            match sender.send_bytes(&payload).await {
                Ok(()) => {}
                Err(error) => {
                    let is_slow_reader_drop = error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::WouldBlock);
                    if is_slow_reader_drop {
                        saw_slow_reader_drop = true;
                        break;
                    }
                    panic!("unexpected reliable send failure: {error}");
                }
            }
        }
        assert!(
            saw_slow_reader_drop,
            "a stalled receiver should eventually make the reliable writer report slow-reader backpressure"
        );
        let buffered_before_drain = sender
            .buffered_amount()
            .await
            .expect("sender buffered amount should be observable");
        assert!(
            buffered_before_drain > 0,
            "slow-reader backpressure should correspond to buffered outbound data"
        );
        eprintln!("[recovery-debug] buffered_before_drain={buffered_before_drain}");
        let _ = drain_tx.send(());

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match sender.send_bytes(&recovery_payload).await {
                    Ok(()) => break,
                    Err(error)
                        if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                            error.kind() == std::io::ErrorKind::WouldBlock
                        }) =>
                    {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("unexpected recovery send failure: {error}"),
                }
            }
        })
        .await
        .expect("reliable writer should recover once its receiver drains");

        receiver_task
            .await
            .expect("receiver drain task should join without panic");
        let buffered_after_drain = sender
            .buffered_amount()
            .await
            .expect("sender buffered amount should remain observable");
        eprintln!("[recovery-debug] buffered_after_drain={buffered_after_drain}");
        assert!(
            buffered_after_drain < buffered_before_drain,
            "sender buffered amount should decrease after receiver drains"
        );
        offerer
            .close()
            .await
            .expect("offerer peer connection should close");
        answerer
            .close()
            .await
            .expect("answerer peer connection should close");
    }

    #[tokio::test]
    #[ignore = "isolates a reliable writer against an above-threshold reader"]
    async fn reliable_data_channel_keeps_above_threshold_reader_contiguous() {
        const THRESHOLD_BPS: u32 = 21_024;
        const READER_TARGET_BPS: u32 = THRESHOLD_BPS * 2;
        const PACKETS: u64 = 500;
        const PAYLOAD_SIZE: usize = 100;

        let (offerer, answerer, sender, receiver) =
            connected_data_channel_pair_with_offer_block_write(true).await;
        sender.set_slow_reader_bitrate_threshold_bps(THRESHOLD_BPS);
        sender
            .set_buffered_amount_low_threshold(THRESHOLD_BPS / 2)
            .await
            .expect("low threshold should apply");
        sender
            .set_buffered_amount_high_threshold(THRESHOLD_BPS)
            .await
            .expect("high threshold should apply");

        let receiver_task = tokio::spawn(async move {
            let per_packet =
                Duration::from_secs_f64((PAYLOAD_SIZE as f64 * 8.0) / f64::from(READER_TARGET_BPS));
            let mut last = 0u64;
            for _ in 0..PACKETS {
                let bytes = receiver
                    .recv_bytes()
                    .await
                    .expect("receiver should remain open");
                let mut index = [0u8; 8];
                index.copy_from_slice(&bytes[(PAYLOAD_SIZE - 8)..]);
                let index = u64::from_be_bytes(index);
                assert_eq!(index, last + 1, "receiver should remain contiguous");
                last = index;
                tokio::time::sleep(per_packet).await;
            }
        });

        for index in 1..=PACKETS {
            let mut payload = vec![0; PAYLOAD_SIZE];
            payload[(PAYLOAD_SIZE - 8)..].copy_from_slice(&index.to_be_bytes());
            sender
                .send_bytes(&payload)
                .await
                .expect("above-threshold reader should not make reliable writer drop data");
        }

        receiver_task
            .await
            .expect("receiver task should join without panic");
        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    /// Ensures one slow reliable subscriber does not interrupt fast-subscriber delivery.
    #[tokio::test]
    #[allow(deprecated)]
    async fn relay_data_packet_slow_subscriber_drop_does_not_force_drops_for_above_threshold_subscriber()
     {
        const THRESHOLD: u32 = 1_024;
        const PACKETS: u64 = 200;
        let room = "relay-slow-subscriber-isolation";
        let rooms = setup_room_with_participants(room, &["publisher", "fast", "slow"]);
        let channels = DataChannelStore::default();

        let (fast_offer, fast_answer, fast_server, fast_client) =
            connected_data_channel_pair_with_offer_block_write(true).await;
        let (slow_offer, slow_answer, slow_server, _slow_client) =
            connected_data_channel_pair_with_offer_block_write(true).await;

        for channel in [&fast_server, &slow_server] {
            channel.set_slow_reader_bitrate_threshold_bps(THRESHOLD);
            channel
                .set_buffered_amount_low_threshold(THRESHOLD / 2)
                .await
                .expect("low threshold should apply");
            channel
                .set_buffered_amount_high_threshold(THRESHOLD)
                .await
                .expect("high threshold should apply");
        }
        channels.insert_with_kind(room, "fast", DataChannelKind::Reliable, fast_server);
        channels.insert_with_kind(room, "slow", DataChannelKind::Reliable, slow_server);

        let receive_fast = tokio::spawn(async move {
            let mut last = 0u64;
            for _ in 0..PACKETS {
                let bytes = tokio::time::timeout(Duration::from_secs(10), fast_client.recv_bytes())
                    .await
                    .expect("fast subscriber should receive packet before timeout")
                    .expect("fast data channel should remain open");
                let packet =
                    proto::DataPacket::decode(bytes.as_slice()).expect("fast packet should decode");
                let Some(proto::data_packet::Value::User(user)) = packet.value else {
                    panic!("fast subscriber should receive user packet");
                };
                let tail = user
                    .payload
                    .get(user.payload.len().saturating_sub(8)..)
                    .expect("payload should carry index");
                let mut index = [0u8; 8];
                index.copy_from_slice(tail);
                let index = u64::from_be_bytes(index);
                assert_eq!(index, last + 1, "fast subscriber should stay contiguous");
                last = index;
            }
        });

        tokio::time::timeout(Duration::from_secs(20), async {
            for index in 1..=PACKETS {
                let mut payload = vec![0u8; 100];
                payload[92..].copy_from_slice(&index.to_be_bytes());
                let packet = proto::DataPacket {
                    kind: proto::data_packet::Kind::Reliable as i32,
                    value: Some(proto::data_packet::Value::User(proto::UserPacket {
                        payload,
                        topic: Some("indexed".to_string()),
                        ..Default::default()
                    })),
                    ..Default::default()
                }
                .encode_to_vec();
                relay_data_packet_after_channel_convergence(
                    &channels,
                    &rooms,
                    room,
                    "publisher",
                    &[],
                    DataChannelKind::Reliable,
                    &packet,
                )
                .await
                .expect("relay should keep processing while slow subscriber is backpressured");
            }
        })
        .await
        .expect("relay should not be held indefinitely by slow subscriber");

        receive_fast.await.expect("fast receiver should not panic");
        fast_offer.close().await.expect("fast offer should close");
        fast_answer.close().await.expect("fast answer should close");
        slow_offer.close().await.expect("slow offer should close");
        slow_answer.close().await.expect("slow answer should close");
    }

    /// Reproduces the sustained-pressure phase of the upstream slow-subscriber test.
    ///
    /// Run explicitly while bisecting reliable data-channel backpressure behavior.
    #[tokio::test]
    #[ignore = "sustained slow-subscriber backpressure regression investigation"]
    #[allow(deprecated)]
    async fn relay_data_packet_sustained_slow_subscriber_pressure_regression() {
        const THRESHOLD: u32 = 1_024;
        const PACKETS: u64 = 2_000;
        let room = "relay-sustained-slow-subscriber-pressure";
        let rooms = setup_room_with_participants(room, &["publisher", "fast", "slow"]);
        let channels = DataChannelStore::default();

        let (fast_offer, fast_answer, fast_server, fast_client) =
            connected_data_channel_pair_with_offer_block_write(true).await;
        let (slow_offer, slow_answer, slow_server, _slow_client) =
            connected_data_channel_pair_with_offer_block_write(true).await;
        for channel in [&fast_server, &slow_server] {
            channel.set_slow_reader_bitrate_threshold_bps(THRESHOLD);
            channel
                .set_buffered_amount_low_threshold(THRESHOLD / 2)
                .await
                .expect("low threshold should apply");
            channel
                .set_buffered_amount_high_threshold(THRESHOLD)
                .await
                .expect("high threshold should apply");
        }
        channels.insert_with_kind(room, "fast", DataChannelKind::Reliable, fast_server);
        channels.insert_with_kind(room, "slow", DataChannelKind::Reliable, slow_server);

        let receive_fast = tokio::spawn(async move {
            let mut last = 0u64;
            for _ in 0..PACKETS {
                let bytes = tokio::time::timeout(Duration::from_secs(15), fast_client.recv_bytes())
                    .await
                    .expect("fast subscriber should receive packet before timeout")
                    .expect("fast data channel should remain open");
                let packet =
                    proto::DataPacket::decode(bytes.as_slice()).expect("fast packet should decode");
                let Some(proto::data_packet::Value::User(user)) = packet.value else {
                    panic!("fast subscriber should receive user packet");
                };
                let mut index = [0u8; 8];
                index.copy_from_slice(&user.payload[92..]);
                let index = u64::from_be_bytes(index);
                assert_eq!(index, last + 1, "fast subscriber should stay contiguous");
                last = index;
            }
        });

        tokio::time::timeout(Duration::from_secs(30), async {
            for index in 1..=PACKETS {
                let mut payload = vec![0u8; 100];
                payload[92..].copy_from_slice(&index.to_be_bytes());
                let packet = proto::DataPacket {
                    kind: proto::data_packet::Kind::Reliable as i32,
                    value: Some(proto::data_packet::Value::User(proto::UserPacket {
                        payload,
                        topic: Some("indexed".to_string()),
                        ..Default::default()
                    })),
                    ..Default::default()
                }
                .encode_to_vec();
                relay_data_packet_after_channel_convergence(
                    &channels,
                    &rooms,
                    room,
                    "publisher",
                    &[],
                    DataChannelKind::Reliable,
                    &packet,
                )
                .await
                .expect("relay should keep processing under sustained slow-subscriber pressure");
            }
        })
        .await
        .expect("slow subscriber should not hold relay longer than sustained-pressure bound");

        receive_fast.await.expect("fast receiver should not panic");
        fast_offer.close().await.expect("fast offer should close");
        fast_answer.close().await.expect("fast answer should close");
        slow_offer.close().await.expect("slow offer should close");
        slow_answer.close().await.expect("slow answer should close");
    }

    /// Reproduces the three-subscriber pressure shape used by the upstream compatibility test.
    #[tokio::test]
    #[allow(deprecated)]
    async fn relay_data_packet_above_threshold_subscriber_stays_contiguous_under_slow_drop_pressure()
     {
        const THRESHOLD_BPS: u32 = 21_024;
        const PACKETS: u64 = 500;
        const PAYLOAD_SIZE: usize = 100;
        let room = "relay-three-subscriber-slow-pressure";
        let rooms =
            setup_room_with_participants(room, &["publisher", "fast", "slowNoDrop", "slowDrop"]);
        let channels = DataChannelStore::default();

        let (fast_offer, fast_answer, fast_server, fast_client) =
            connected_data_channel_pair_with_offer_block_write(true).await;
        let (no_drop_offer, no_drop_answer, no_drop_server, no_drop_client) =
            connected_data_channel_pair_with_offer_block_write(true).await;
        let (drop_offer, drop_answer, drop_server, _drop_client) =
            connected_data_channel_pair_with_offer_block_write(true).await;
        for channel in [&fast_server, &no_drop_server, &drop_server] {
            channel.set_slow_reader_bitrate_threshold_bps(THRESHOLD_BPS);
            channel
                .set_buffered_amount_low_threshold(THRESHOLD_BPS / 2)
                .await
                .expect("low threshold should apply");
            channel
                .set_buffered_amount_high_threshold(THRESHOLD_BPS)
                .await
                .expect("high threshold should apply");
        }
        channels.insert_with_kind(room, "fast", DataChannelKind::Reliable, fast_server);
        channels.insert_with_kind(
            room,
            "slowNoDrop",
            DataChannelKind::Reliable,
            no_drop_server,
        );
        channels.insert_with_kind(room, "slowDrop", DataChannelKind::Reliable, drop_server);

        let receive_fast = tokio::spawn(async move {
            let mut last = 0u64;
            for _ in 0..PACKETS {
                let bytes = tokio::time::timeout(Duration::from_secs(15), fast_client.recv_bytes())
                    .await
                    .expect("fast receiver should make progress")
                    .expect("fast channel should stay open");
                let packet =
                    proto::DataPacket::decode(bytes.as_slice()).expect("packet should decode");
                let Some(proto::data_packet::Value::User(user)) = packet.value else {
                    panic!("fast receiver should receive user packet");
                };
                let mut index = [0u8; 8];
                index.copy_from_slice(&user.payload[(PAYLOAD_SIZE - 8)..]);
                let index = u64::from_be_bytes(index);
                assert_eq!(index, last + 1, "fast receiver should stay contiguous");
                last = index;
            }
        });
        let receive_no_drop = tokio::spawn(async move {
            let per_packet =
                Duration::from_secs_f64((PAYLOAD_SIZE as f64 * 8.0) / f64::from(THRESHOLD_BPS * 2));
            let mut last = 0u64;
            for _ in 0..PACKETS {
                let bytes =
                    tokio::time::timeout(Duration::from_secs(15), no_drop_client.recv_bytes())
                        .await
                        .expect("above-threshold receiver should make progress")
                        .expect("above-threshold channel should stay open");
                let packet =
                    proto::DataPacket::decode(bytes.as_slice()).expect("packet should decode");
                let Some(proto::data_packet::Value::User(user)) = packet.value else {
                    panic!("above-threshold receiver should receive user packet");
                };
                let mut index = [0u8; 8];
                index.copy_from_slice(&user.payload[(PAYLOAD_SIZE - 8)..]);
                let index = u64::from_be_bytes(index);
                assert_eq!(
                    index,
                    last + 1,
                    "above-threshold receiver should stay contiguous"
                );
                last = index;
                tokio::time::sleep(per_packet).await;
            }
        });

        tokio::time::timeout(Duration::from_secs(30), async {
            for index in 1..=PACKETS {
                let mut payload = vec![0u8; PAYLOAD_SIZE];
                payload[(PAYLOAD_SIZE - 8)..].copy_from_slice(&index.to_be_bytes());
                let packet = proto::DataPacket {
                    kind: proto::data_packet::Kind::Reliable as i32,
                    value: Some(proto::data_packet::Value::User(proto::UserPacket {
                        payload,
                        topic: Some("indexed".to_string()),
                        ..Default::default()
                    })),
                    ..Default::default()
                }
                .encode_to_vec();
                relay_data_packet_after_channel_convergence(
                    &channels,
                    &rooms,
                    room,
                    "publisher",
                    &[],
                    DataChannelKind::Reliable,
                    &packet,
                )
                .await
                .expect("relay should keep processing");
            }
        })
        .await
        .expect("relay should not be held by slow-drop target");

        receive_fast.await.expect("fast receiver should not panic");
        receive_no_drop
            .await
            .expect("above-threshold receiver should not panic");
        fast_offer.close().await.expect("fast offer should close");
        fast_answer.close().await.expect("fast answer should close");
        no_drop_offer
            .close()
            .await
            .expect("above-threshold offer should close");
        no_drop_answer
            .close()
            .await
            .expect("above-threshold answer should close");
        drop_offer
            .close()
            .await
            .expect("slow-drop offer should close");
        drop_answer
            .close()
            .await
            .expect("slow-drop answer should close");
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn relay_data_packet_empty_destinations_broadcasts_to_all_except_sender() {
        let room = "data-packet-broadcast-room";
        let rooms = setup_room_with_participants(room, &["alice", "bob", "carol"]);
        let channels = DataChannelStore::default();

        let (alice_offer_pc, alice_answer_pc, alice_server_channel, alice_client_channel) =
            connected_data_channel_pair().await;
        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;
        let (carol_offer_pc, carol_answer_pc, carol_server_channel, carol_client_channel) =
            connected_data_channel_pair().await;

        channels.insert_with_kind(
            room,
            "alice",
            DataChannelKind::Reliable,
            alice_server_channel,
        );
        channels.insert_with_kind(room, "bob", DataChannelKind::Reliable, bob_server_channel);
        channels.insert_with_kind(
            room,
            "carol",
            DataChannelKind::Reliable,
            carol_server_channel,
        );

        let packet = proto::DataPacket {
            kind: proto::data_packet::Kind::Reliable as i32,
            participant_identity: "alice".to_string(),
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                payload: b"broadcast".to_vec(),
                topic: Some("topic-all".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec();

        relay_data_packet_after_channel_convergence(
            &channels,
            &rooms,
            room,
            "alice",
            &[],
            DataChannelKind::Reliable,
            &packet,
        )
        .await
        .expect("broadcast relay should succeed");

        assert!(
            recv_packet_with_timeout(&alice_client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "sender should not receive echo packet"
        );

        for recipient in [&bob_client_channel, &carol_client_channel] {
            let received = recv_packet_with_timeout(recipient, Duration::from_secs(3))
                .await
                .expect("non-sender participant should receive broadcast packet");
            let Some(proto::data_packet::Value::User(user)) = received.value else {
                panic!("expected user packet");
            };
            assert_eq!(user.payload, b"broadcast");
            assert_eq!(user.topic.as_deref(), Some("topic-all"));
        }

        alice_offer_pc
            .close()
            .await
            .expect("alice offer pc should close");
        alice_answer_pc
            .close()
            .await
            .expect("alice answer pc should close");
        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
        carol_offer_pc
            .close()
            .await
            .expect("carol offer pc should close");
        carol_answer_pc
            .close()
            .await
            .expect("carol answer pc should close");
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn relay_data_packet_with_destinations_targets_only_named_identities() {
        let room = "data-packet-destination-room";
        let rooms = setup_room_with_participants(room, &["alice", "bob", "carol"]);
        let channels = DataChannelStore::default();

        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;
        let (carol_offer_pc, carol_answer_pc, carol_server_channel, carol_client_channel) =
            connected_data_channel_pair().await;

        channels.insert_with_kind(room, "bob", DataChannelKind::Reliable, bob_server_channel);
        channels.insert_with_kind(
            room,
            "carol",
            DataChannelKind::Reliable,
            carol_server_channel,
        );

        let packet = proto::DataPacket {
            kind: proto::data_packet::Kind::Reliable as i32,
            participant_identity: "alice".to_string(),
            destination_identities: vec!["carol".to_string()],
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                payload: b"targeted".to_vec(),
                topic: Some("topic-target".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec();

        relay_data_packet_after_channel_convergence(
            &channels,
            &rooms,
            room,
            "alice",
            &["carol".to_string()],
            DataChannelKind::Reliable,
            &packet,
        )
        .await
        .expect("targeted relay should succeed");

        assert!(
            recv_packet_with_timeout(&bob_client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "non-destination participant should not receive targeted packet"
        );

        let carol_packet = recv_packet_with_timeout(&carol_client_channel, Duration::from_secs(3))
            .await
            .expect("destination participant should receive targeted packet");
        let Some(proto::data_packet::Value::User(user)) = carol_packet.value else {
            panic!("expected user packet");
        };
        assert_eq!(user.payload, b"targeted");
        assert_eq!(user.topic.as_deref(), Some("topic-target"));

        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
        carol_offer_pc
            .close()
            .await
            .expect("carol offer pc should close");
        carol_answer_pc
            .close()
            .await
            .expect("carol answer pc should close");
    }

    #[tokio::test]
    async fn relay_rpc_response_packet_with_destination_targets_only_named_identity() {
        let room = "rpc-response-destination-room";
        let rooms = setup_room_with_participants(room, &["alice", "bob", "carol"]);
        let channels = DataChannelStore::default();

        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;
        let (carol_offer_pc, carol_answer_pc, carol_server_channel, carol_client_channel) =
            connected_data_channel_pair().await;

        channels.insert_with_kind(room, "bob", DataChannelKind::Reliable, bob_server_channel);
        channels.insert_with_kind(
            room,
            "carol",
            DataChannelKind::Reliable,
            carol_server_channel,
        );

        let request_id = "rpc-123".to_string();
        let packet = proto::DataPacket {
            participant_identity: request_id.clone(),
            destination_identities: vec!["carol".to_string()],
            value: Some(proto::data_packet::Value::RpcResponse(proto::RpcResponse {
                request_id,
                value: Some(proto::rpc_response::Value::Payload("pong".to_string())),
            })),
            ..Default::default()
        }
        .encode_to_vec();

        relay_data_packet_after_channel_convergence(
            &channels,
            &rooms,
            room,
            "alice",
            &["carol".to_string()],
            DataChannelKind::Reliable,
            &packet,
        )
        .await
        .expect("targeted rpc response relay should succeed");

        assert!(
            recv_packet_with_timeout(&bob_client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "non-destination participant should not receive targeted rpc response"
        );

        let carol_packet = recv_packet_with_timeout(&carol_client_channel, Duration::from_secs(3))
            .await
            .expect("destination participant should receive rpc response");
        let Some(proto::data_packet::Value::RpcResponse(response)) = carol_packet.value else {
            panic!("expected rpc response packet");
        };
        assert_eq!(response.request_id, "rpc-123");
        match response.value {
            Some(proto::rpc_response::Value::Payload(payload)) => assert_eq!(payload, "pong"),
            _ => panic!("expected rpc payload response"),
        }

        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
        carol_offer_pc
            .close()
            .await
            .expect("carol offer pc should close");
        carol_answer_pc
            .close()
            .await
            .expect("carol answer pc should close");
    }

    #[tokio::test]
    async fn relay_stream_chunk_packet_broadcasts_to_all_except_sender() {
        let room = "stream-chunk-broadcast-room";
        let rooms = setup_room_with_participants(room, &["alice", "bob", "carol"]);
        let channels = DataChannelStore::default();

        let (alice_offer_pc, alice_answer_pc, alice_server_channel, alice_client_channel) =
            connected_data_channel_pair().await;
        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;
        let (carol_offer_pc, carol_answer_pc, carol_server_channel, carol_client_channel) =
            connected_data_channel_pair().await;

        channels.insert_with_kind(
            room,
            "alice",
            DataChannelKind::Reliable,
            alice_server_channel,
        );
        channels.insert_with_kind(room, "bob", DataChannelKind::Reliable, bob_server_channel);
        channels.insert_with_kind(
            room,
            "carol",
            DataChannelKind::Reliable,
            carol_server_channel,
        );

        let packet = proto::DataPacket {
            participant_identity: "alice".to_string(),
            value: Some(proto::data_packet::Value::StreamChunk(
                proto::data_stream::Chunk {
                    stream_id: "stream-1".to_string(),
                    content: b"chunk".to_vec(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
        .encode_to_vec();

        relay_data_packet_after_channel_convergence(
            &channels,
            &rooms,
            room,
            "alice",
            &[],
            DataChannelKind::Reliable,
            &packet,
        )
        .await
        .expect("stream chunk broadcast relay should succeed");

        assert!(
            recv_packet_with_timeout(&alice_client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "sender should not receive echo stream chunk"
        );

        for recipient in [&bob_client_channel, &carol_client_channel] {
            let received = recv_packet_with_timeout(recipient, Duration::from_secs(3))
                .await
                .expect("recipient should receive stream chunk");
            let Some(proto::data_packet::Value::StreamChunk(chunk)) = received.value else {
                panic!("expected stream chunk packet");
            };
            assert_eq!(chunk.stream_id, "stream-1");
            assert_eq!(chunk.content, b"chunk");
        }

        alice_offer_pc
            .close()
            .await
            .expect("alice offer pc should close");
        alice_answer_pc
            .close()
            .await
            .expect("alice answer pc should close");
        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
        carol_offer_pc
            .close()
            .await
            .expect("carol offer pc should close");
        carol_answer_pc
            .close()
            .await
            .expect("carol answer pc should close");
    }

    #[tokio::test]
    async fn relay_data_packet_missing_destination_identity_is_not_buffered() {
        let room = "data-packet-missing-destination-room";
        let rooms = setup_room_with_participants(room, &["alice", "bob"]);
        let channels = DataChannelStore::default();

        let packet = proto::DataPacket {
            participant_identity: "alice".to_string(),
            destination_identities: vec!["missing".to_string()],
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                payload: b"ignored".to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec();

        relay_data_packet_after_channel_convergence(
            &channels,
            &rooms,
            room,
            "alice",
            &["missing".to_string()],
            DataChannelKind::Reliable,
            &packet,
        )
        .await
        .expect("missing destination should be ignored without error");
    }

    #[tokio::test]
    async fn relay_data_track_packet_staggered_multi_subscriber_reconnect_routes_to_active_subscribers_only()
     {
        let room = "data-track-staggered-reconnect-room";
        let publisher = "alice";
        let bob = "bob";
        let carol = "carol";

        let rooms = setup_room_with_participants(room, &[publisher, bob, carol]);
        let data_tracks = DataTrackStore::default();
        let subscriptions = DataTrackSubscriptionStore::default();
        let channels = DataChannelStore::default();

        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 77,
                    name: "telemetry".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let bob_handles = subscriptions.update(
            room,
            bob,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let carol_handles = subscriptions.update(
            room,
            carol,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );

        let bob_sub_handle = *bob_handles
            .sub_handles
            .keys()
            .next()
            .expect("bob sub handle should exist");
        let carol_sub_handle = *carol_handles
            .sub_handles
            .keys()
            .next()
            .expect("carol sub handle should exist");

        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;
        let (carol_offer_pc, carol_answer_pc, carol_server_channel, carol_client_channel) =
            connected_data_channel_pair().await;

        channels.insert_with_kind(room, bob, DataChannelKind::DataTrack, bob_server_channel);
        channels.insert_with_kind(
            room,
            carol,
            DataChannelKind::DataTrack,
            carol_server_channel,
        );

        let first_packet = encode_data_track_packet_with_handle(77, 0xA1);
        let sent =
            relay_data_track_packet(&channels, &subscriptions, room, publisher, first_packet)
                .await
                .expect("relay to both subscribers should succeed");
        assert_eq!(sent, 2);

        let bob_first = recv_bytes_with_timeout(&bob_client_channel, Duration::from_secs(3))
            .await
            .expect("bob should receive first packet");
        let carol_first = recv_bytes_with_timeout(&carol_client_channel, Duration::from_secs(3))
            .await
            .expect("carol should receive first packet");
        assert_eq!(packet_handle(&bob_first), bob_sub_handle as u16);
        assert_eq!(packet_handle(&carol_first), carol_sub_handle as u16);

        subscriptions.remove_participant(room, bob);
        let second_packet = encode_data_track_packet_with_handle(77, 0xB2);
        let sent =
            relay_data_track_packet(&channels, &subscriptions, room, publisher, second_packet)
                .await
                .expect("relay after bob removal should succeed");
        assert_eq!(
            sent, 1,
            "only carol should receive while bob is disconnected"
        );

        assert!(
            recv_bytes_with_timeout(&bob_client_channel, Duration::from_millis(350))
                .await
                .is_none(),
            "bob should not receive packets while removed"
        );
        let carol_second = recv_bytes_with_timeout(&carol_client_channel, Duration::from_secs(3))
            .await
            .expect("carol should receive second packet");
        assert_eq!(packet_handle(&carol_second), carol_sub_handle as u16);

        let bob_handles_after_reconnect = subscriptions.update(
            room,
            bob,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid,
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let bob_new_sub_handle = *bob_handles_after_reconnect
            .sub_handles
            .keys()
            .next()
            .expect("bob reconnect sub handle should exist");
        assert_ne!(
            bob_new_sub_handle, bob_sub_handle,
            "reconnect should allocate a new subscriber handle"
        );

        let third_packet = encode_data_track_packet_with_handle(77, 0xC3);
        let sent =
            relay_data_track_packet(&channels, &subscriptions, room, publisher, third_packet)
                .await
                .expect("relay after bob reconnect should succeed");
        assert_eq!(sent, 2);

        let bob_third = recv_bytes_with_timeout(&bob_client_channel, Duration::from_secs(3))
            .await
            .expect("bob should receive packet after reconnect");
        let carol_third = recv_bytes_with_timeout(&carol_client_channel, Duration::from_secs(3))
            .await
            .expect("carol should receive packet after bob reconnect");
        assert_eq!(packet_handle(&bob_third), bob_new_sub_handle as u16);
        assert_eq!(packet_handle(&carol_third), carol_sub_handle as u16);

        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
        carol_offer_pc
            .close()
            .await
            .expect("carol offer pc should close");
        carol_answer_pc
            .close()
            .await
            .expect("carol answer pc should close");
    }

    #[tokio::test]
    async fn relay_data_track_packet_packets_before_resubscribe_are_not_buffered_and_only_new_packets_flow()
     {
        let room = "data-track-resubscribe-no-buffer-room";
        let publisher = "alice";
        let subscriber = "bob";

        let rooms = setup_room_with_participants(room, &[publisher, subscriber]);
        let data_tracks = DataTrackStore::default();
        let subscriptions = DataTrackSubscriptionStore::default();
        let channels = DataChannelStore::default();

        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 88,
                    name: "resub-track".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;
        channels.insert_with_kind(
            room,
            subscriber,
            DataChannelKind::DataTrack,
            bob_server_channel,
        );

        let sent = relay_data_track_packet(
            &channels,
            &subscriptions,
            room,
            publisher,
            encode_data_track_packet_with_handle(88, 0x10),
        )
        .await
        .expect("relay without subscription should succeed");
        assert_eq!(sent, 0, "no subscriber should receive before subscribe");

        assert!(
            recv_bytes_with_timeout(&bob_client_channel, Duration::from_millis(350))
                .await
                .is_none(),
            "packet sent before subscribe must not be buffered"
        );

        let subscribe_handles = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let first_sub_handle = *subscribe_handles
            .sub_handles
            .keys()
            .next()
            .expect("sub handle should exist after subscribe");

        let sent = relay_data_track_packet(
            &channels,
            &subscriptions,
            room,
            publisher,
            encode_data_track_packet_with_handle(88, 0x11),
        )
        .await
        .expect("relay after subscribe should succeed");
        assert_eq!(sent, 1);

        let first_live_packet =
            recv_bytes_with_timeout(&bob_client_channel, Duration::from_secs(3))
                .await
                .expect("subscriber should receive first live packet");
        assert_eq!(packet_handle(&first_live_packet), first_sub_handle as u16);
        assert_eq!(
            *first_live_packet
                .last()
                .expect("packet should carry payload tag"),
            0x11
        );

        subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: false,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );

        let sent = relay_data_track_packet(
            &channels,
            &subscriptions,
            room,
            publisher,
            encode_data_track_packet_with_handle(88, 0x12),
        )
        .await
        .expect("relay while unsubscribed should succeed");
        assert_eq!(sent, 0);
        assert!(
            recv_bytes_with_timeout(&bob_client_channel, Duration::from_millis(350))
                .await
                .is_none(),
            "packet sent while unsubscribed must not be buffered"
        );

        let resubscribe_handles = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid,
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let second_sub_handle = *resubscribe_handles
            .sub_handles
            .keys()
            .next()
            .expect("sub handle should exist after resubscribe");
        assert_ne!(
            second_sub_handle, first_sub_handle,
            "resubscribe should allocate new handle"
        );

        let sent = relay_data_track_packet(
            &channels,
            &subscriptions,
            room,
            publisher,
            encode_data_track_packet_with_handle(88, 0x13),
        )
        .await
        .expect("relay after resubscribe should succeed");
        assert_eq!(sent, 1);

        let second_live_packet =
            recv_bytes_with_timeout(&bob_client_channel, Duration::from_secs(3))
                .await
                .expect("subscriber should receive new packet after resubscribe");
        assert_eq!(packet_handle(&second_live_packet), second_sub_handle as u16);
        assert_eq!(
            *second_live_packet
                .last()
                .expect("packet should carry payload tag"),
            0x13
        );

        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
    }

    #[tokio::test]
    async fn relay_data_track_packet_burst_during_disconnect_is_not_replayed_after_reconnect() {
        let room = "data-track-reconnect-burst-no-replay-room";
        let publisher = "alice";
        let subscriber = "bob";

        let rooms = setup_room_with_participants(room, &[publisher, subscriber]);
        let data_tracks = DataTrackStore::default();
        let subscriptions = DataTrackSubscriptionStore::default();
        let channels = DataChannelStore::default();

        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 91,
                    name: "burst-track".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;
        channels.insert_with_kind(
            room,
            subscriber,
            DataChannelKind::DataTrack,
            bob_server_channel,
        );

        let initial_handles = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let initial_sub_handle = *initial_handles
            .sub_handles
            .keys()
            .next()
            .expect("initial handle should exist");

        for tag in [0x21u8, 0x22u8] {
            relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_handle(91, tag),
            )
            .await
            .expect("initial live relay should succeed");
            let packet = recv_bytes_with_timeout(&bob_client_channel, Duration::from_secs(3))
                .await
                .expect("subscriber should receive initial packet");
            assert_eq!(packet_handle(&packet), initial_sub_handle as u16);
            assert_eq!(*packet.last().expect("payload tag should exist"), tag);
        }

        subscriptions.remove_participant(room, subscriber);
        for tag in [0x30u8, 0x31u8, 0x32u8, 0x33u8] {
            let sent = relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_handle(91, tag),
            )
            .await
            .expect("relay during disconnect should succeed");
            assert_eq!(sent, 0);
        }

        assert!(
            recv_bytes_with_timeout(&bob_client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "no packets should arrive while subscriber is disconnected"
        );

        let reconnect_handles = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid,
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let reconnect_sub_handle = *reconnect_handles
            .sub_handles
            .keys()
            .next()
            .expect("reconnect handle should exist");
        assert_ne!(reconnect_sub_handle, initial_sub_handle);

        assert!(
            recv_bytes_with_timeout(&bob_client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "packets sent during disconnect must not be replayed after reconnect"
        );

        for tag in [0x40u8, 0x41u8, 0x42u8] {
            let sent = relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_handle(91, tag),
            )
            .await
            .expect("relay after reconnect should succeed");
            assert_eq!(sent, 1);
            let packet = recv_bytes_with_timeout(&bob_client_channel, Duration::from_secs(3))
                .await
                .expect("subscriber should receive post-reconnect packet");
            assert_eq!(packet_handle(&packet), reconnect_sub_handle as u16);
            assert_eq!(*packet.last().expect("payload tag should exist"), tag);
        }

        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
    }

    #[tokio::test]
    async fn relay_data_track_packet_drops_when_subscriber_channel_not_open() {
        let room = "data-track-drop-unopened-channel-room";
        let publisher = "alice";
        let subscriber = "bob";

        let rooms = setup_room_with_participants(room, &[publisher, subscriber]);
        let data_tracks = DataTrackStore::default();
        let subscriptions = DataTrackSubscriptionStore::default();
        let channels = DataChannelStore::default();

        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 101,
                    name: "unopened-track".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let _handles = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid,
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );

        let subscriber_pc = oxidesfu_rtc::create_peer_connection()
            .await
            .expect("subscriber peer connection should create");
        let unopened_channel = subscriber_pc
            .create_data_channel("_data_track")
            .await
            .expect("data channel should create without negotiation");
        channels.insert_with_kind(
            room,
            subscriber,
            DataChannelKind::DataTrack,
            unopened_channel,
        );

        let sent = relay_data_track_packet(
            &channels,
            &subscriptions,
            room,
            publisher,
            encode_data_track_packet_with_payload(101, &[0xAB; 64]),
        )
        .await
        .expect("relay should drop packet instead of erroring when channel is unopened");
        assert_eq!(sent, 0);

        subscriber_pc
            .close()
            .await
            .expect("subscriber pc should close");
    }

    #[tokio::test]
    async fn relay_data_track_packet_delivers_small_and_large_frames_to_subscriber() {
        let room = "data-track-frame-delivery-room";
        let publisher = "alice";
        let subscriber = "bob";

        let rooms = setup_room_with_participants(room, &[publisher, subscriber]);
        let data_tracks = DataTrackStore::default();
        let subscriptions = DataTrackSubscriptionStore::default();
        let channels = DataChannelStore::default();

        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 102,
                    name: "frame-track".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let handles = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid,
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let sub_handle = *handles
            .sub_handles
            .keys()
            .next()
            .expect("sub handle should exist");

        let (offer_pc, answer_pc, server_channel, client_channel) =
            connected_data_channel_pair().await;
        channels.insert_with_kind(room, subscriber, DataChannelKind::DataTrack, server_channel);

        let small_payload = vec![0xFA; 256];
        let sent = relay_data_track_packet(
            &channels,
            &subscriptions,
            room,
            publisher,
            encode_data_track_packet_with_payload(102, &small_payload),
        )
        .await
        .expect("small frame relay should succeed");
        assert_eq!(sent, 1);
        let small_frame = recv_bytes_with_timeout(&client_channel, Duration::from_secs(3))
            .await
            .expect("small frame should arrive");
        assert_eq!(packet_handle(&small_frame), sub_handle as u16);
        assert_eq!(&small_frame[12..], small_payload.as_slice());

        let large_payload = vec![0xBC; 32_000];
        let sent = relay_data_track_packet(
            &channels,
            &subscriptions,
            room,
            publisher,
            encode_data_track_packet_with_payload(102, &large_payload),
        )
        .await
        .expect("large frame relay should succeed");
        assert_eq!(sent, 1);
        let large_frame = recv_bytes_with_timeout(&client_channel, Duration::from_secs(3))
            .await
            .expect("large frame should arrive");
        assert_eq!(packet_handle(&large_frame), sub_handle as u16);
        assert_eq!(&large_frame[12..], large_payload.as_slice());

        offer_pc.close().await.expect("offer pc should close");
        answer_pc.close().await.expect("answer pc should close");
    }

    #[tokio::test]
    async fn relay_data_track_packet_with_subscription_options_delivers_frame_and_preserves_options()
     {
        let room = "data-track-options-delivery-room";
        let publisher = "alice";
        let subscriber = "bob";

        let rooms = setup_room_with_participants(room, &[publisher, subscriber]);
        let data_tracks = DataTrackStore::default();
        let subscriptions = DataTrackSubscriptionStore::default();
        let channels = DataChannelStore::default();

        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 103,
                    name: "options-track".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let handles = subscriptions.update(
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
            &data_tracks,
            &rooms,
        );
        let sub_handle = *handles
            .sub_handles
            .keys()
            .next()
            .expect("sub handle should exist");
        assert_eq!(
            subscriptions
                .options_for_track(room, subscriber, &published.sid)
                .and_then(|options| options.target_fps),
            Some(30)
        );

        let (offer_pc, answer_pc, server_channel, client_channel) =
            connected_data_channel_pair().await;
        channels.insert_with_kind(room, subscriber, DataChannelKind::DataTrack, server_channel);

        let payload = vec![0x55; 64];
        let sent = relay_data_track_packet(
            &channels,
            &subscriptions,
            room,
            publisher,
            encode_data_track_packet_with_payload(103, &payload),
        )
        .await
        .expect("options frame relay should succeed");
        assert_eq!(sent, 1);
        let frame = recv_bytes_with_timeout(&client_channel, Duration::from_secs(3))
            .await
            .expect("options frame should arrive");
        assert_eq!(packet_handle(&frame), sub_handle as u16);
        assert_eq!(&frame[12..], payload.as_slice());

        offer_pc.close().await.expect("offer pc should close");
        answer_pc.close().await.expect("answer pc should close");
    }

    #[tokio::test]
    async fn relay_data_track_packet_reconnect_burst_reports_delivery_metrics() {
        let room = "data-track-reconnect-metrics-room";
        let publisher = "alice";
        let subscriber = "bob";

        let rooms = setup_room_with_participants(room, &[publisher, subscriber]);
        let data_tracks = DataTrackStore::default();
        let subscriptions = DataTrackSubscriptionStore::default();
        let channels = DataChannelStore::default();

        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 120,
                    name: "metrics-track".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let (offer_pc, answer_pc, server_channel, client_channel) =
            connected_data_channel_pair().await;
        channels.insert_with_kind(room, subscriber, DataChannelKind::DataTrack, server_channel);

        let first_handles = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let first_sub_handle = *first_handles
            .sub_handles
            .keys()
            .next()
            .expect("first sub handle should exist");

        for seq in 1..=12u32 {
            let sent = relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_sequence(120, seq),
            )
            .await
            .expect("warmup relay should succeed");
            assert_eq!(sent, 1);
            let packet = recv_bytes_with_timeout(&client_channel, Duration::from_secs(3))
                .await
                .expect("warmup packet should arrive");
            assert_eq!(packet_handle(&packet), first_sub_handle as u16);
            assert_eq!(packet_sequence(&packet), Some(seq));
        }

        subscriptions.remove_participant(room, subscriber);

        for seq in 13..=28u32 {
            let sent = relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_sequence(120, seq),
            )
            .await
            .expect("disconnected burst relay should succeed");
            assert_eq!(sent, 0);
        }

        let reconnect_handles = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid,
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let reconnect_sub_handle = *reconnect_handles
            .sub_handles
            .keys()
            .next()
            .expect("reconnect sub handle should exist");
        assert_ne!(reconnect_sub_handle, first_sub_handle);

        assert!(
            recv_bytes_with_timeout(&client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "disconnected burst packets must not replay after reconnect"
        );

        let post_start = 29u32;
        let post_end = 52u32;
        let post_sent = (post_end - post_start + 1) as usize;
        let mut received = Vec::new();

        for seq in post_start..=post_end {
            let sent = relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_sequence(120, seq),
            )
            .await
            .expect("post-reconnect relay should succeed");
            assert_eq!(sent, 1);

            if let Some(packet) =
                recv_bytes_with_timeout(&client_channel, Duration::from_secs(3)).await
            {
                assert_eq!(packet_handle(&packet), reconnect_sub_handle as u16);
                if let Some(received_seq) = packet_sequence(&packet) {
                    received.push((seq, received_seq));
                }
            }
        }

        let delivery_ratio = received.len() as f64 / post_sent as f64;
        let max_lag = received
            .iter()
            .map(|(sent_seq, recv_seq)| sent_seq.saturating_sub(*recv_seq))
            .max()
            .unwrap_or(0);

        assert!(
            delivery_ratio >= 0.95,
            "expected delivery ratio >= 0.95, got {delivery_ratio:.2}"
        );
        assert!(max_lag <= 2, "expected max lag <= 2, got {max_lag}");

        offer_pc.close().await.expect("offer pc should close");
        answer_pc.close().await.expect("answer pc should close");
    }

    #[tokio::test]
    async fn relay_data_track_packet_dual_reconnect_burst_reports_delivery_metrics() {
        let room = "data-track-dual-reconnect-metrics-room";
        let publisher = "alice";
        let bob = "bob";
        let charlie = "charlie";

        let rooms = setup_room_with_participants(room, &[publisher, bob, charlie]);
        let data_tracks = DataTrackStore::default();
        let subscriptions = DataTrackSubscriptionStore::default();
        let channels = DataChannelStore::default();

        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 121,
                    name: "dual-metrics-track".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;
        let (charlie_offer_pc, charlie_answer_pc, charlie_server_channel, charlie_client_channel) =
            connected_data_channel_pair().await;

        channels.insert_with_kind(room, bob, DataChannelKind::DataTrack, bob_server_channel);
        channels.insert_with_kind(
            room,
            charlie,
            DataChannelKind::DataTrack,
            charlie_server_channel,
        );

        let bob_handles = subscriptions.update(
            room,
            bob,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let charlie_handles = subscriptions.update(
            room,
            charlie,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );

        let bob_first_handle = *bob_handles
            .sub_handles
            .keys()
            .next()
            .expect("bob first handle should exist");
        let charlie_first_handle = *charlie_handles
            .sub_handles
            .keys()
            .next()
            .expect("charlie first handle should exist");

        for seq in 1..=8u32 {
            let sent = relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_sequence(121, seq),
            )
            .await
            .expect("warmup relay should succeed");
            assert_eq!(sent, 2);
            let bob_packet = recv_bytes_with_timeout(&bob_client_channel, Duration::from_secs(3))
                .await
                .expect("bob warmup packet should arrive");
            let charlie_packet =
                recv_bytes_with_timeout(&charlie_client_channel, Duration::from_secs(3))
                    .await
                    .expect("charlie warmup packet should arrive");
            assert_eq!(packet_handle(&bob_packet), bob_first_handle as u16);
            assert_eq!(packet_handle(&charlie_packet), charlie_first_handle as u16);
            assert_eq!(packet_sequence(&bob_packet), Some(seq));
            assert_eq!(packet_sequence(&charlie_packet), Some(seq));
        }

        subscriptions.remove_participant(room, bob);
        subscriptions.remove_participant(room, charlie);

        for seq in 9..=20u32 {
            let sent = relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_sequence(121, seq),
            )
            .await
            .expect("disconnected burst relay should succeed");
            assert_eq!(sent, 0);
        }

        let bob_reconnect_handles = subscriptions.update(
            room,
            bob,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid.clone(),
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );
        let charlie_reconnect_handles = subscriptions.update(
            room,
            charlie,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: published.sid,
                    subscribe: true,
                    options: None,
                }],
            },
            &data_tracks,
            &rooms,
        );

        let bob_second_handle = *bob_reconnect_handles
            .sub_handles
            .keys()
            .next()
            .expect("bob reconnect handle should exist");
        let charlie_second_handle = *charlie_reconnect_handles
            .sub_handles
            .keys()
            .next()
            .expect("charlie reconnect handle should exist");

        assert_ne!(bob_second_handle, bob_first_handle);
        assert_ne!(charlie_second_handle, charlie_first_handle);

        assert!(
            recv_bytes_with_timeout(&bob_client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "bob must not receive replayed disconnected-burst packets"
        );
        assert!(
            recv_bytes_with_timeout(&charlie_client_channel, Duration::from_millis(400))
                .await
                .is_none(),
            "charlie must not receive replayed disconnected-burst packets"
        );

        let post_start = 21u32;
        let post_end = 48u32;
        let post_sent = (post_end - post_start + 1) as usize;
        let mut bob_received = Vec::new();
        let mut charlie_received = Vec::new();

        for seq in post_start..=post_end {
            let sent = relay_data_track_packet(
                &channels,
                &subscriptions,
                room,
                publisher,
                encode_data_track_packet_with_sequence(121, seq),
            )
            .await
            .expect("post-reconnect relay should succeed");
            assert_eq!(sent, 2);

            if let Some(packet) =
                recv_bytes_with_timeout(&bob_client_channel, Duration::from_secs(3)).await
            {
                assert_eq!(packet_handle(&packet), bob_second_handle as u16);
                if let Some(received_seq) = packet_sequence(&packet) {
                    bob_received.push((seq, received_seq));
                }
            }
            if let Some(packet) =
                recv_bytes_with_timeout(&charlie_client_channel, Duration::from_secs(3)).await
            {
                assert_eq!(packet_handle(&packet), charlie_second_handle as u16);
                if let Some(received_seq) = packet_sequence(&packet) {
                    charlie_received.push((seq, received_seq));
                }
            }
        }

        let bob_ratio = bob_received.len() as f64 / post_sent as f64;
        let charlie_ratio = charlie_received.len() as f64 / post_sent as f64;
        let bob_max_lag = bob_received
            .iter()
            .map(|(sent_seq, recv_seq)| sent_seq.saturating_sub(*recv_seq))
            .max()
            .unwrap_or(0);
        let charlie_max_lag = charlie_received
            .iter()
            .map(|(sent_seq, recv_seq)| sent_seq.saturating_sub(*recv_seq))
            .max()
            .unwrap_or(0);

        assert!(
            bob_ratio >= 0.95,
            "expected bob delivery ratio >= 0.95, got {bob_ratio:.2}"
        );
        assert!(
            charlie_ratio >= 0.95,
            "expected charlie delivery ratio >= 0.95, got {charlie_ratio:.2}"
        );
        assert!(
            bob_max_lag <= 2,
            "expected bob max lag <= 2, got {bob_max_lag}"
        );
        assert!(
            charlie_max_lag <= 2,
            "expected charlie max lag <= 2, got {charlie_max_lag}"
        );

        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
        charlie_offer_pc
            .close()
            .await
            .expect("charlie offer pc should close");
        charlie_answer_pc
            .close()
            .await
            .expect("charlie answer pc should close");
    }

    #[test]
    #[allow(deprecated)]
    fn normalize_incoming_data_packet_sets_kind_sender_and_user_destinations() {
        let room = "normalize-packet-room";
        let rooms = setup_room_with_participants(room, &["alice", "bob"]);

        let mut packet = proto::DataPacket {
            kind: 999,
            destination_identities: vec!["bob".to_string()],
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                payload: b"payload".to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        };

        normalize_incoming_data_packet(
            &mut packet,
            &rooms,
            room,
            "alice",
            DataChannelKind::Reliable,
        );

        assert_eq!(packet.kind, proto::data_packet::Kind::Reliable as i32);
        assert_eq!(packet.participant_identity, "alice");
        let Some(proto::data_packet::Value::User(user)) = packet.value else {
            panic!("expected user packet");
        };
        assert_eq!(user.participant_identity, "alice");
        assert_eq!(user.destination_identities, vec!["bob".to_string()]);
    }

    #[test]
    #[allow(deprecated)]
    fn normalize_incoming_data_packet_clears_sender_identity_for_hidden_participant() {
        let room = "normalize-hidden-room";
        let rooms = RoomStore::default();
        rooms
            .join_participant_with_permission(
                room,
                "hidden",
                "Hidden",
                String::new(),
                HashMap::new(),
                Some(proto::ParticipantPermission {
                    hidden: true,
                    ..Default::default()
                }),
            )
            .expect("hidden participant should join");

        let mut packet = proto::DataPacket {
            participant_identity: "hidden".to_string(),
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                participant_identity: "hidden".to_string(),
                payload: b"payload".to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        };

        normalize_incoming_data_packet(
            &mut packet,
            &rooms,
            room,
            "hidden",
            DataChannelKind::Reliable,
        );

        assert!(packet.participant_identity.is_empty());
        let Some(proto::data_packet::Value::User(user)) = packet.value else {
            panic!("expected user packet");
        };
        assert!(user.participant_identity.is_empty());
    }

    #[test]
    #[allow(deprecated)]
    fn resolved_destination_identities_falls_back_to_user_destination_field() {
        let packet = proto::DataPacket {
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                destination_identities: vec!["bob".to_string()],
                ..Default::default()
            })),
            ..Default::default()
        };

        assert_eq!(
            resolved_destination_identities_for_packet(&packet),
            vec!["bob".to_string()]
        );

        let packet = proto::DataPacket {
            destination_identities: vec!["carol".to_string()],
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                destination_identities: vec!["bob".to_string()],
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            resolved_destination_identities_for_packet(&packet),
            vec!["carol".to_string()]
        );
    }

    #[test]
    fn preserves_sctp_application_section_when_deactivating_unbound_media_sections() {
        let answer_sdp = "v=0\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
            a=mid:0\r\n\
            a=sendonly\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
            a=mid:1\r\n\
            a=sendonly\r\n\
            a=sctp-port:5000\r\n";

        let rewritten = force_sendonly_sections_without_msid_recvonly(answer_sdp, &HashSet::new());
        let video_section = rewritten
            .split("m=application")
            .next()
            .expect("video section should precede the application section");
        let application_section = rewritten
            .split_once("m=application")
            .map(|(_, section)| section)
            .expect("application section should remain present");

        assert!(video_section.contains("a=inactive"));
        assert!(
            application_section.contains("a=sendonly"),
            "SCTP application sections are not media receive sections and must stay enabled"
        );
    }

    #[test]
    fn filter_h264_from_publisher_answer_strips_recvonly_sections_but_keeps_sendonly_sections() {
        let client_info = proto::ClientInfo {
            browser: "firefox".to_string(),
            os: "linux".to_string(),
            ..Default::default()
        };

        let answer_sdp = "v=0\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 96 102\r\n\
            a=mid:1\r\n\
            a=recvonly\r\n\
            a=rtpmap:96 VP8/90000\r\n\
            a=rtpmap:102 H264/90000\r\n\
            a=fmtp:102 profile-level-id=42e01f;packetization-mode=1\r\n\
            m=video 9 UDP/TLS/RTP/SAVPF 97 103\r\n\
            a=mid:2\r\n\
            a=sendonly\r\n\
            a=rtpmap:97 VP8/90000\r\n\
            a=rtpmap:103 H264/90000\r\n\
            a=fmtp:103 profile-level-id=42e01f;packetization-mode=1\r\n";

        let filtered = filter_h264_from_publisher_answer_for_client(answer_sdp, &client_info);

        let section_for_mid = |sdp: &str, target_mid: &str| -> Vec<String> {
            let mut sections = Vec::<Vec<String>>::new();
            let mut current = Vec::new();
            for line in sdp.lines() {
                if line.starts_with("m=") && !current.is_empty() {
                    sections.push(current);
                    current = Vec::new();
                }
                current.push(line.to_string());
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
        };

        let mid1_lines = section_for_mid(&filtered, "1");
        assert!(
            mid1_lines.iter().any(|line| line.contains("VP8/90000")),
            "recvonly section should retain VP8"
        );
        assert!(
            mid1_lines.iter().all(|line| !line.contains("H264/90000")),
            "recvonly section should strip H264"
        );

        let mid2_lines = section_for_mid(&filtered, "2");
        assert!(
            mid2_lines.iter().any(|line| line.contains("H264/90000")),
            "sendonly section should retain H264 for subscribe/downtrack compatibility"
        );
    }

    #[test]
    fn data_channel_kind_for_label_maps_livekit_compatible_labels() {
        assert_eq!(
            data_channel_kind_for_label("data"),
            Some(DataChannelKind::Reliable)
        );
        assert_eq!(
            data_channel_kind_for_label("_reliable"),
            Some(DataChannelKind::Reliable)
        );
        assert_eq!(
            data_channel_kind_for_label("pubraw"),
            Some(DataChannelKind::Reliable)
        );
        assert_eq!(
            data_channel_kind_for_label("subraw"),
            Some(DataChannelKind::Reliable)
        );
        assert_eq!(
            data_channel_kind_for_label("_lossy"),
            Some(DataChannelKind::Lossy)
        );
        assert_eq!(
            data_channel_kind_for_label("_data_track"),
            Some(DataChannelKind::DataTrack)
        );
        assert_eq!(data_channel_kind_for_label("unknown"), None);
    }

    #[test]
    fn reliable_channel_label_rank_prefers_reliable_over_raw() {
        assert!(reliable_channel_label_rank("_reliable") > reliable_channel_label_rank("pubraw"));
        assert!(reliable_channel_label_rank("data") > reliable_channel_label_rank("subraw"));
        assert!(reliable_channel_label_rank("pubraw") > reliable_channel_label_rank("unknown"));
    }

    #[test]
    fn signal_track_subscribed_to_publisher_ignores_closed_sender() {
        let state = test_signal_state();
        let signal_connections = SignalConnectionStore::default();
        let (publisher_tx, publisher_rx) = tokio::sync::mpsc::unbounded_channel();
        signal_connections.insert("room-a", "publisher", publisher_tx);
        drop(publisher_rx);

        signal_track_subscribed_to_publisher(
            &state.publisher_subscription_active_pairs(),
            &signal_connections,
            "room-a",
            "publisher",
            "subscriber",
            "TR_test",
        );
    }

    #[test]
    fn track_subscribed_signal_emits_once_per_active_pair_and_reemits_after_last_unsubscribe() {
        let state = test_signal_state();
        state
            .rooms
            .join_participant(
                "room-a",
                "publisher",
                "Publisher",
                String::new(),
                HashMap::new(),
            )
            .expect("publisher should join");
        state
            .rooms
            .join_participant(
                "room-a",
                "subscriber",
                "Subscriber",
                String::new(),
                HashMap::new(),
            )
            .expect("subscriber should join");
        state
            .rooms
            .add_participant_track(
                "room-a",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_a".to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    ..Default::default()
                },
            )
            .expect("first track should add");
        state
            .rooms
            .add_participant_track(
                "room-a",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_b".to_string(),
                    r#type: proto::TrackType::Audio as i32,
                    ..Default::default()
                },
            )
            .expect("second track should add");

        state
            .media_subscriptions
            .set_subscribed("room-a", "publisher", "TR_a", "subscriber", true);
        state
            .media_subscriptions
            .set_subscribed("room-a", "publisher", "TR_b", "subscriber", true);
        let _ = state.rooms.set_media_track_subscribed(
            "room-a",
            "publisher",
            "TR_a",
            "subscriber",
            true,
        );
        let _ = state.rooms.set_media_track_subscribed(
            "room-a",
            "publisher",
            "TR_b",
            "subscriber",
            true,
        );

        let signal_connections = SignalConnectionStore::default();
        let (publisher_tx, mut publisher_rx) = tokio::sync::mpsc::unbounded_channel();
        signal_connections.insert("room-a", "publisher", publisher_tx);

        signal_track_subscribed_to_publisher(
            &state.publisher_subscription_active_pairs(),
            &signal_connections,
            "room-a",
            "publisher",
            "subscriber",
            "TR_a",
        );
        let first = publisher_rx
            .try_recv()
            .expect("first track subscription signal should be emitted");
        let Some(proto::signal_response::Message::TrackSubscribed(first_track_subscribed)) =
            first.message
        else {
            panic!("expected TrackSubscribed response");
        };
        assert_eq!(first_track_subscribed.track_sid, "TR_a");

        signal_track_subscribed_to_publisher(
            &state.publisher_subscription_active_pairs(),
            &signal_connections,
            "room-a",
            "publisher",
            "subscriber",
            "TR_b",
        );
        assert!(
            publisher_rx.try_recv().is_err(),
            "second track should not emit another TrackSubscribed while pair is already active"
        );

        state.media_subscriptions.set_subscribed(
            "room-a",
            "publisher",
            "TR_a",
            "subscriber",
            false,
        );
        let _ = state.rooms.set_media_track_subscribed(
            "room-a",
            "publisher",
            "TR_a",
            "subscriber",
            false,
        );
        clear_publisher_subscription_active_if_no_remaining_tracks(
            &state,
            "room-a",
            "publisher",
            "subscriber",
        );

        signal_track_subscribed_to_publisher(
            &state.publisher_subscription_active_pairs(),
            &signal_connections,
            "room-a",
            "publisher",
            "subscriber",
            "TR_b",
        );
        assert!(
            publisher_rx.try_recv().is_err(),
            "pair should remain active while at least one track stays subscribed"
        );

        state.media_subscriptions.set_subscribed(
            "room-a",
            "publisher",
            "TR_b",
            "subscriber",
            false,
        );
        let _ = state.rooms.set_media_track_subscribed(
            "room-a",
            "publisher",
            "TR_b",
            "subscriber",
            false,
        );
        clear_publisher_subscription_active_if_no_remaining_tracks(
            &state,
            "room-a",
            "publisher",
            "subscriber",
        );

        state
            .media_subscriptions
            .set_subscribed("room-a", "publisher", "TR_b", "subscriber", true);
        let _ = state.rooms.set_media_track_subscribed(
            "room-a",
            "publisher",
            "TR_b",
            "subscriber",
            true,
        );

        signal_track_subscribed_to_publisher(
            &state.publisher_subscription_active_pairs(),
            &signal_connections,
            "room-a",
            "publisher",
            "subscriber",
            "TR_b",
        );
        let second = publisher_rx.try_recv().expect(
            "track subscription signal should emit again after last-unsubscribe transition",
        );
        let Some(proto::signal_response::Message::TrackSubscribed(second_track_subscribed)) =
            second.message
        else {
            panic!("expected TrackSubscribed response");
        };
        assert_eq!(second_track_subscribed.track_sid, "TR_b");
    }

    #[test]
    fn allocation_desired_quality_is_independent_and_clamped_to_subscription_maximum() {
        assert_eq!(
            desired_video_quality_from_allocation(
                Some(proto::VideoQuality::High),
                Some(proto::VideoQuality::Low),
            ),
            Some(proto::VideoQuality::Low),
            "an allocator may reduce a high-capable target"
        );
        assert_eq!(
            desired_video_quality_from_allocation(
                Some(proto::VideoQuality::Low),
                Some(proto::VideoQuality::High),
            ),
            Some(proto::VideoQuality::Low),
            "an allocator must never exceed the subscriber maximum"
        );
        assert_eq!(
            desired_video_quality_from_allocation(Some(proto::VideoQuality::Medium), None),
            Some(proto::VideoQuality::Medium),
            "without allocation, desired quality defaults to the subscriber maximum"
        );
    }

    #[test]
    fn fps_filter_does_not_decimate_nominal_30_fps_33ms_cadence() {
        let mut state = FpsForwardingState::default();
        assert!(state.should_forward_packet(0, 30));
        assert!(
            state.should_forward_packet(2_970, 30),
            "a nominal 33 ms frame interval must not be dropped for a 30 FPS request"
        );
        assert!(state.should_forward_packet(5_940, 30));
    }

    #[test]
    fn allocation_layout_weight_prefers_subscriber_viewport_over_track_default() {
        let track = proto::TrackInfo {
            layers: vec![proto::VideoLayer {
                width: 1280,
                height: 720,
                ..Default::default()
            }],
            ..Default::default()
        };
        let settings = proto::UpdateTrackSettings {
            width: 320,
            height: 180,
            ..Default::default()
        };
        assert_eq!(allocation_layout_weight(&track, None), 1280 * 720);
        assert_eq!(allocation_layout_weight(&track, Some(&settings)), 320 * 180);
    }

    #[test]
    fn allocation_budget_selects_highest_advertised_layer_and_temporal_target() {
        let track = proto::TrackInfo {
            layers: vec![
                proto::VideoLayer {
                    quality: proto::VideoQuality::Low as i32,
                    bitrate: 150_000,
                    ..Default::default()
                },
                proto::VideoLayer {
                    quality: proto::VideoQuality::Medium as i32,
                    bitrate: 600_000,
                    ..Default::default()
                },
                proto::VideoLayer {
                    quality: proto::VideoQuality::High as i32,
                    bitrate: 1_800_000,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            allocation_quality_for_budget(&track, 700_000),
            Some(proto::VideoQuality::Medium)
        );
        assert_eq!(
            allocation_temporal_layer_for_budget(&track, proto::VideoQuality::Medium, 700_000),
            Some(2)
        );
        assert_eq!(
            allocation_quality_for_budget(&track, 300_000),
            Some(proto::VideoQuality::Low)
        );
    }

    #[test]
    fn temporal_controller_clamps_requested_fps_to_available_maximum_layer() {
        let mut controller = SubscriberVideoTemporalController::default();
        controller.set_requested_fps(8, Some([Some(8.0), Some(16.0), Some(30.0)]));

        assert_eq!(
            controller
                .policy()
                .map(|policy| (policy.max, policy.desired)),
            Some((TemporalLayer::T0, TemporalLayer::T0)),
            "an 8 FPS request must select the base temporal layer"
        );
        assert_eq!(
            controller.observe_packet(3_000, 8, Some(2)),
            TemporalIngressDecision::DropAboveMaximum
        );
        assert_eq!(
            controller.observe_packet(3_000, 8, Some(0)),
            TemporalIngressDecision::Forward
        );
        assert_eq!(controller.current(), Some(TemporalLayer::T0));
    }

    #[test]
    fn temporal_controller_clamps_allocator_target_to_subscriber_maximum() {
        let mut controller = SubscriberVideoTemporalController::default();
        let layers = Some([Some(8.0), Some(16.0), Some(30.0)]);
        controller.set_requested_fps_with_desired_temporal_layer(30, layers, Some(1));

        assert_eq!(
            controller
                .policy()
                .map(|policy| (policy.max, policy.desired)),
            Some((TemporalLayer::T2, TemporalLayer::T1))
        );
        assert_eq!(
            controller.observe_packet(3_000, 30, Some(2)),
            TemporalIngressDecision::DropAboveDesired
        );
        assert_eq!(
            controller.observe_packet(6_000, 30, Some(1)),
            TemporalIngressDecision::Forward
        );

        controller.set_requested_fps_with_desired_temporal_layer(8, layers, Some(2));
        assert_eq!(
            controller
                .policy()
                .map(|policy| (policy.max, policy.desired)),
            Some((TemporalLayer::T0, TemporalLayer::T0))
        );
    }

    #[test]
    fn temporal_controller_downgrade_clamps_current_without_resetting_target_state() {
        let mut controller = SubscriberVideoTemporalController::default();
        let layers = Some([Some(8.0), Some(16.0), Some(30.0)]);
        controller.set_requested_fps(30, layers);
        assert_eq!(
            controller.observe_packet(3_000, 30, Some(2)),
            TemporalIngressDecision::Forward
        );
        assert_eq!(controller.current(), Some(TemporalLayer::T2));

        controller.set_requested_fps(8, layers);
        assert_eq!(controller.current(), Some(TemporalLayer::T0));
        assert_eq!(
            controller.observe_packet(6_000, 8, Some(2)),
            TemporalIngressDecision::DropAboveMaximum
        );
        assert_eq!(
            controller.observe_packet(6_000, 8, Some(0)),
            TemporalIngressDecision::Forward
        );
    }

    #[test]
    fn temporal_controllers_are_isolated_for_identical_packets() {
        let mut low = SubscriberVideoTemporalController::default();
        let mut high = SubscriberVideoTemporalController::default();
        let layers = Some([Some(8.0), Some(16.0), Some(30.0)]);
        low.set_requested_fps(8, layers);
        high.set_requested_fps(30, layers);

        assert_eq!(
            low.observe_packet(3_000, 8, Some(2)),
            TemporalIngressDecision::DropAboveMaximum
        );
        assert_eq!(
            high.observe_packet(3_000, 30, Some(2)),
            TemporalIngressDecision::Forward
        );
        assert_eq!(low.current(), None);
        assert_eq!(high.current(), Some(TemporalLayer::T2));
    }

    #[test]
    fn temporal_controller_uses_timestamp_gate_only_when_metadata_is_unavailable() {
        let mut controller = SubscriberVideoTemporalController::default();
        controller.set_requested_fps(15, None);
        assert_eq!(controller.policy(), None);
        assert_eq!(
            controller.observe_packet(0, 15, None),
            TemporalIngressDecision::Forward
        );
        assert_eq!(
            controller.observe_packet(3_000, 15, None),
            TemporalIngressDecision::DropTimestampCap,
            "metadata-poor packets retain deterministic timestamp gating"
        );
        assert_eq!(
            controller.observe_packet(6_000, 15, None),
            TemporalIngressDecision::Forward
        );
    }

    #[test]
    fn dependency_descriptor_switch_point_overrides_scalable_payload_heuristics() {
        // VP9's payload alone looks like a keyframe start, but a parsed descriptor that lacks
        // DTI Switch must prevent a source transition.
        assert!(!video_is_decodable_switch_point_with_dependency_descriptor(
            Some("video/VP9"),
            &[0x08],
            Some(false),
        ));
        assert!(video_is_decodable_switch_point_with_dependency_descriptor(
            Some("video/VP9"),
            &[0x00],
            Some(true),
        ));
        // No descriptor metadata preserves the codec-specific keyframe fallback.
        assert!(video_is_decodable_switch_point_with_dependency_descriptor(
            Some("video/VP9"),
            &[0x08],
            None,
        ));
        // Descriptor metadata has no meaning for non-scalable H264 simulcast.
        assert!(video_is_decodable_switch_point_with_dependency_descriptor(
            Some("video/H264"),
            &[0x65],
            Some(false),
        ));
    }

    #[test]
    fn vp8_switch_point_requires_partition_zero_keyframe_start() {
        // S=1, partition ID=0, followed by a VP8 keyframe frame tag.
        assert!(vp8_is_keyframe_start(&[0x10, 0x00]));
        // The VP8 frame-tag P bit identifies an interframe.
        assert!(!vp8_is_keyframe_start(&[0x10, 0x01]));
        // A non-zero partition cannot start a decodable frame.
        assert!(!vp8_is_keyframe_start(&[0x11, 0x00]));
        assert!(video_is_decodable_switch_point(
            Some("video/VP8"),
            &[0x10, 0x00]
        ));
        assert!(vp9_is_keyframe_start(&[0x08]));
        assert!(!vp9_is_keyframe_start(&[0x48]));
        assert!(h264_is_keyframe_start(&[0x65]));
        assert!(h264_is_keyframe_start(&[0x7c, 0x85]));
        assert!(!h264_is_keyframe_start(&[0x7c, 0x05]));
        assert!(av1_is_keyframe_start(&[0x08]));
        assert!(!av1_is_keyframe_start(&[0x00]));
        assert!(video_is_decodable_switch_point(Some("video/H264"), &[0x65]));
        assert!(video_is_decodable_switch_point(Some("video/VP9"), &[0x08]));
        assert!(video_is_decodable_switch_point(Some("video/AV1"), &[0x08]));
    }
}

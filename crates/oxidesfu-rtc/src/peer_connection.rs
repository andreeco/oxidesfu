// WebRTC integration boundary for OxideSFU.
//
// This crate intentionally wraps `webrtc-rs` so API churn stays isolated from
// OxideSFU signalling, room, and SFU state management.

use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use rtc::{
    ice::mdns::MulticastDnsMode,
    media_stream::MediaStreamTrack,
    peer_connection::configuration::media_engine::{
        MIME_TYPE_AV1, MIME_TYPE_H264, MIME_TYPE_OPUS, MIME_TYPE_PCMA, MIME_TYPE_PCMU,
        MIME_TYPE_VP8,
    },
    rtp_transceiver::{
        RTCRtpTransceiverDirection, RTCRtpTransceiverInit,
        rtp_sender::{
            RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters,
            RtpCodecKind,
        },
    },
    statistics::{StatsSelector, report::RTCStatsReportEntry},
};
use tokio::sync::mpsc;
use webrtc::data_channel::RTCDataChannelInit;
use webrtc::media_stream::track_local::{
    TrackLocal as WebRtcTrackLocal, static_rtp::TrackLocalStaticRTP,
};
use webrtc::peer_connection::RTCSessionDescription;
use webrtc::peer_connection::{
    MediaEngine, PeerConnection as WebRtcPeerConnection, PeerConnectionBuilder,
    PeerConnectionEventHandler, RTCConfigurationBuilder, RTCIceCandidateInit, RTCIceCandidateType,
    Registry, SettingEngine, register_default_interceptors,
};

use crate::tracks::next_ssrc;
use crate::webrtc_adapter::{EventPeerConnectionHandler, NoopPeerConnectionHandler};
use crate::{DataChannel, DataChannelOptions, LocalRtpTrack, PeerConnectionEvents, RtcResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtcTransportConfig {
    pub udp_addrs: Vec<String>,
    pub tcp_addrs: Vec<String>,
    pub nat_1to1_ips: Vec<String>,
}

impl Default for RtcTransportConfig {
    fn default() -> Self {
        Self {
            udp_addrs: vec!["0.0.0.0:0".to_string()],
            tcp_addrs: Vec::new(),
            nat_1to1_ips: Vec::new(),
        }
    }
}

/// OxideSFU-owned wrapper around a `webrtc-rs` peer connection.
pub struct PeerConnection {
    inner: Box<dyn WebRtcPeerConnection>,
}

impl PeerConnection {
    /// Returns the largest positive congestion-feedback outgoing bitrate estimate in bits/second.
    ///
    /// The value is absent until a nominated ICE candidate pair has produced transport feedback.
    pub async fn available_outgoing_bitrate_bps(&self) -> Option<u64> {
        let report = self
            .inner
            .get_stats(Instant::now(), StatsSelector::None)
            .await;
        report
            .iter()
            .filter_map(|entry| match entry {
                RTCStatsReportEntry::IceCandidatePair(pair)
                    if pair.available_outgoing_bitrate.is_finite()
                        && pair.available_outgoing_bitrate > 0.0 =>
                {
                    Some(pair.available_outgoing_bitrate as u64)
                }
                _ => None,
            })
            .max()
    }

    /// Creates a local data channel with default reliable/ordered options.
    pub async fn create_data_channel(&self, label: &str) -> RtcResult<DataChannel> {
        self.create_data_channel_with_options(label, DataChannelOptions::default())
            .await
    }

    /// Creates a local data channel with explicit delivery options.
    pub async fn create_data_channel_with_options(
        &self,
        label: &str,
        options: DataChannelOptions,
    ) -> RtcResult<DataChannel> {
        let data_channel = self
            .inner
            .create_data_channel(
                label,
                Some(RTCDataChannelInit {
                    ordered: options.ordered,
                    max_retransmits: options.max_retransmits,
                    ..Default::default()
                }),
            )
            .await?;
        Ok(DataChannel::new(data_channel))
    }

    /// Creates an SDP offer and sets it as the local description.
    pub async fn create_offer(&self) -> RtcResult<String> {
        let offer = self.inner.create_offer(None).await?;
        self.inner.set_local_description(offer).await?;
        let local_description = self
            .inner
            .local_description()
            .await
            .ok_or_else(|| std::io::Error::other("local description was not set"))?;
        Ok(local_description.sdp)
    }

    /// Creates an SDP offer with a data channel so the offer has an application media section.
    pub async fn create_data_channel_offer(&self, label: &str) -> RtcResult<String> {
        let _data_channel = self.create_data_channel(label).await?;
        self.create_offer().await
    }

    /// Creates an SDP offer with an audio transceiver for media-negotiation compatibility tests.
    pub async fn create_audio_offer(&self) -> RtcResult<String> {
        let _ = self
            .inner
            .add_transceiver_from_kind(RtpCodecKind::Audio, None)
            .await?;
        self.create_offer().await
    }

    /// Adds receive-only media sections requested by a LiveKit single-peer-connection server.
    pub async fn add_recvonly_transceivers(&self, kind: RtpCodecKind, count: u32) -> RtcResult<()> {
        for _ in 0..count {
            self.inner
                .add_transceiver_from_kind(
                    kind,
                    Some(RTCRtpTransceiverInit {
                        direction: RTCRtpTransceiverDirection::Recvonly,
                        ..Default::default()
                    }),
                )
                .await?;
        }
        Ok(())
    }

    fn forwarding_codec_for(kind: RtpCodecKind, mime_type: Option<&str>) -> RTCRtpCodec {
        let normalized_mime = mime_type
            .map(str::trim)
            .filter(|mime| !mime.is_empty())
            .map(|mime| mime.to_ascii_lowercase());

        match kind {
            RtpCodecKind::Audio => {
                let mime = normalized_mime.as_deref().unwrap_or("audio/opus");
                match mime {
                    "audio/pcmu" => RTCRtpCodec {
                        mime_type: MIME_TYPE_PCMU.to_string(),
                        clock_rate: 8_000,
                        channels: 1,
                        sdp_fmtp_line: String::new(),
                        rtcp_feedback: vec![],
                    },
                    "audio/pcma" => RTCRtpCodec {
                        mime_type: MIME_TYPE_PCMA.to_string(),
                        clock_rate: 8_000,
                        channels: 1,
                        sdp_fmtp_line: String::new(),
                        rtcp_feedback: vec![],
                    },
                    _ => RTCRtpCodec {
                        mime_type: MIME_TYPE_OPUS.to_string(),
                        clock_rate: 48_000,
                        channels: 2,
                        sdp_fmtp_line: "minptime=10;useinbandfec=1".to_string(),
                        rtcp_feedback: vec![],
                    },
                }
            }
            RtpCodecKind::Video | RtpCodecKind::Unspecified => {
                let mime = normalized_mime.as_deref().unwrap_or("video/vp8");
                match mime {
                    "video/h264" => RTCRtpCodec {
                        mime_type: MIME_TYPE_H264.to_string(),
                        clock_rate: 90_000,
                        channels: 0,
                        sdp_fmtp_line:
                            "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                                .to_string(),
                        rtcp_feedback: vec![],
                    },
                    "video/av1" => RTCRtpCodec {
                        mime_type: MIME_TYPE_AV1.to_string(),
                        clock_rate: 90_000,
                        channels: 0,
                        // AV1 RTP infers the Main profile when `profile` is absent.
                        // LiveKit/Pion advertises AV1 this way rather than with `profile-id`.
                        sdp_fmtp_line: String::new(),
                        rtcp_feedback: vec![],
                    },
                    _ => RTCRtpCodec {
                        mime_type: MIME_TYPE_VP8.to_string(),
                        clock_rate: 90_000,
                        channels: 0,
                        sdp_fmtp_line: String::new(),
                        rtcp_feedback: vec![],
                    },
                }
            }
        }
    }

    fn forwarding_video_codec_preferences(mime_type: Option<&str>) -> Vec<RTCRtpCodecParameters> {
        if mime_type.is_some_and(|mime| mime.trim().eq_ignore_ascii_case("video/av1")) {
            return vec![RTCRtpCodecParameters {
                rtp_codec: Self::forwarding_codec_for(RtpCodecKind::Video, Some("video/av1")),
                payload_type: 45,
            }];
        }

        vec![
            RTCRtpCodecParameters {
                rtp_codec: Self::forwarding_codec_for(RtpCodecKind::Video, Some("video/vp8")),
                payload_type: 96,
            },
            RTCRtpCodecParameters {
                rtp_codec: RTCRtpCodec {
                    mime_type: MIME_TYPE_H264.to_string(),
                    clock_rate: 90_000,
                    channels: 0,
                    sdp_fmtp_line:
                        "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f"
                            .to_string(),
                    rtcp_feedback: vec![],
                },
                payload_type: 125,
            },
            RTCRtpCodecParameters {
                rtp_codec: Self::forwarding_codec_for(RtpCodecKind::Video, Some("video/h264")),
                payload_type: 108,
            },
        ]
    }

    fn forwarding_track_local(
        publisher_sid: &str,
        track_sid: &str,
        kind: RtpCodecKind,
        mime_type: Option<&str>,
    ) -> Arc<TrackLocalStaticRTP> {
        let codec = Self::forwarding_codec_for(kind, mime_type);
        Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
            format!("{publisher_sid}|{track_sid}"),
            track_sid.to_string(),
            track_sid.to_string(),
            kind,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(next_ssrc()),
                    ..Default::default()
                },
                codec,
                ..Default::default()
            }],
        )))
    }

    /// Adds a local RTP track for a native client publisher.
    ///
    /// Unlike forwarding tracks, this does not encode an OxideSFU publisher/track
    /// SID into the stream ID and does not configure server-side codec preferences.
    pub async fn add_local_rtp_track_with_mime(
        &self,
        track_id: &str,
        stream_id: &str,
        kind: RtpCodecKind,
        mime_type: &str,
    ) -> RtcResult<LocalRtpTrack> {
        let local_track = Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
            track_id.to_string(),
            stream_id.to_string(),
            stream_id.to_string(),
            kind,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(next_ssrc()),
                    ..Default::default()
                },
                codec: Self::forwarding_codec_for(kind, Some(mime_type)),
                ..Default::default()
            }],
        )));
        let sender = self
            .inner
            .add_track(local_track.clone() as Arc<dyn WebRtcTrackLocal>)
            .await?;
        sender.set_streams(vec![stream_id.to_string()]).await?;
        let sender_id = sender.id();
        let mut mid = None;
        for transceiver in self.inner.get_transceivers().await {
            let Some(candidate_sender) = transceiver.sender().await? else {
                continue;
            };
            if candidate_sender.id() == sender_id {
                mid = transceiver.mid().await?;
                break;
            }
        }
        let forwarding_mid_bytes = mid
            .as_ref()
            .map(|mid| bytes::Bytes::copy_from_slice(mid.as_bytes()));
        Ok(LocalRtpTrack {
            inner: local_track,
            sender,
            forwarding_mid: mid,
            forwarding_mid_bytes,
        })
    }

    /// Adds a local RTP forwarding track for a subscriber offer.
    pub async fn add_forwarding_track(
        &self,
        publisher_sid: &str,
        track_sid: &str,
        kind: RtpCodecKind,
    ) -> RtcResult<LocalRtpTrack> {
        self.add_forwarding_track_with_mime(publisher_sid, track_sid, kind, None)
            .await
    }

    /// Adds a local RTP forwarding track for a subscriber offer with an explicit codec MIME type.
    pub async fn add_forwarding_track_with_mime(
        &self,
        publisher_sid: &str,
        track_sid: &str,
        kind: RtpCodecKind,
        mime_type: Option<&str>,
    ) -> RtcResult<LocalRtpTrack> {
        self.add_forwarding_track_with_mime_and_mid(publisher_sid, track_sid, kind, mime_type, None)
            .await
    }

    async fn add_forwarding_track_with_mime_and_mid(
        &self,
        publisher_sid: &str,
        track_sid: &str,
        kind: RtpCodecKind,
        mime_type: Option<&str>,
        forwarding_mid: Option<&str>,
    ) -> RtcResult<LocalRtpTrack> {
        let local_track = Self::forwarding_track_local(publisher_sid, track_sid, kind, mime_type);
        let sender = self
            .inner
            .add_track(local_track.clone() as Arc<dyn WebRtcTrackLocal>)
            .await?;
        let sender_id = sender.id();
        let mut forwarding_transceiver = None;
        for transceiver in self.inner.get_transceivers().await {
            let Some(candidate_sender) = transceiver.sender().await? else {
                continue;
            };
            if candidate_sender.id() == sender_id {
                forwarding_transceiver = Some(transceiver);
                break;
            }
        }
        let Some(forwarding_transceiver) = forwarding_transceiver else {
            return Err(std::io::Error::other("forwarding sender has no transceiver").into());
        };
        forwarding_transceiver
            .set_direction(RTCRtpTransceiverDirection::Sendonly)
            .await?;
        let requires_explicit_video_codec_preferences = kind == RtpCodecKind::Video
            && !mime_type.is_some_and(|mime| mime.trim().eq_ignore_ascii_case("video/h264"));
        if requires_explicit_video_codec_preferences {
            forwarding_transceiver
                .set_codec_preferences(Self::forwarding_video_codec_preferences(mime_type))
                .await?;
        }
        sender
            .set_streams(vec![format!("{publisher_sid}|{track_sid}")])
            .await?;
        let forwarding_mid = forwarding_mid.map(ToOwned::to_owned);
        Ok(LocalRtpTrack {
            inner: local_track,
            sender,
            forwarding_mid_bytes: forwarding_mid
                .as_ref()
                .map(|mid| bytes::Bytes::copy_from_slice(mid.as_bytes())),
            forwarding_mid,
        })
    }

    /// Adds a local RTP forwarding track to a specific receive-capable transceiver MID.
    pub async fn add_forwarding_track_to_mid(
        &self,
        mid: &str,
        publisher_sid: &str,
        track_sid: &str,
        kind: RtpCodecKind,
    ) -> RtcResult<LocalRtpTrack> {
        self.add_forwarding_track_to_mid_with_mime(mid, publisher_sid, track_sid, kind, None)
            .await
    }

    /// Adds a local RTP forwarding track to a specific receive-capable transceiver MID
    /// with an explicit codec MIME type.
    pub async fn add_forwarding_track_to_mid_with_mime(
        &self,
        mid: &str,
        publisher_sid: &str,
        track_sid: &str,
        kind: RtpCodecKind,
        mime_type: Option<&str>,
    ) -> RtcResult<LocalRtpTrack> {
        let local_track = Self::forwarding_track_local(publisher_sid, track_sid, kind, mime_type);
        for transceiver in self.inner.get_transceivers().await {
            let Some(candidate_mid) = transceiver.mid().await? else {
                continue;
            };
            if candidate_mid != mid {
                continue;
            }
            let sender = if let Some(sender) = transceiver.sender().await? {
                sender
            } else {
                self.inner
                    .add_track_to_mid(mid, local_track.clone() as Arc<dyn WebRtcTrackLocal>)
                    .await?
            };
            transceiver
                .set_direction(RTCRtpTransceiverDirection::Sendonly)
                .await?;
            sender
                .replace_track(local_track.clone() as Arc<dyn WebRtcTrackLocal>)
                .await?;
            sender
                .set_streams(vec![format!("{publisher_sid}|{track_sid}")])
                .await?;
            let forwarding_mid = Some(mid.to_string());
            return Ok(LocalRtpTrack {
                inner: local_track,
                sender,
                forwarding_mid_bytes: forwarding_mid
                    .as_ref()
                    .map(|mid| bytes::Bytes::copy_from_slice(mid.as_bytes())),
                forwarding_mid,
            });
        }

        Err(std::io::Error::other(format!("receive transceiver mid {mid} not found")).into())
    }

    /// Removes a local RTP forwarding track from this peer connection.
    pub async fn remove_forwarding_track(&self, track: &LocalRtpTrack) -> RtcResult<()> {
        self.inner.remove_track(&track.sender).await?;
        Ok(())
    }

    /// Removes all forwarding tracks whose stream ID belongs to the given publisher SID.
    ///
    /// Forwarding tracks created by OxideSFU use a stream-id shape of
    /// `<publisher_sid>|<track_sid>`, which lets cleanup paths aggressively
    /// detach any stale sender bindings for a departing publisher.
    pub async fn remove_forwarding_tracks_for_publisher(
        &self,
        publisher_sid: &str,
    ) -> RtcResult<usize> {
        let stream_prefix = format!("{publisher_sid}|");
        let mut removed = 0usize;

        for transceiver in self.inner.get_transceivers().await {
            let Some(sender) = transceiver.sender().await? else {
                continue;
            };

            if !sender.track().stream_id().await.starts_with(&stream_prefix) {
                continue;
            }

            self.inner.remove_track(&sender).await?;
            removed = removed.saturating_add(1);
        }

        Ok(removed)
    }

    /// Returns whether a transceiver with the given MID has a receiver bound.
    pub async fn has_receiver_for_mid(&self, mid: &str) -> RtcResult<bool> {
        for transceiver in self.inner.get_transceivers().await {
            let Some(candidate_mid) = transceiver.mid().await? else {
                continue;
            };
            if candidate_mid == mid {
                return Ok(transceiver.receiver().await?.is_some());
            }
        }
        Ok(false)
    }

    /// Returns a per-transceiver summary (MID + preferred/current directions) for diagnostics.
    pub async fn debug_transceiver_summary(&self) -> RtcResult<Vec<String>> {
        let mut summary = Vec::new();
        for transceiver in self.inner.get_transceivers().await {
            let mid = transceiver
                .mid()
                .await?
                .unwrap_or_else(|| "<none>".to_string());
            let direction = transceiver.direction().await?;
            let current_direction = transceiver.current_direction().await?;
            let has_sender = transceiver.sender().await?.is_some();
            let has_receiver = transceiver.receiver().await?.is_some();
            summary.push(format!(
                "mid={mid} direction={direction:?} current_direction={current_direction:?} has_sender={has_sender} has_receiver={has_receiver}"
            ));
        }
        Ok(summary)
    }

    /// Applies a remote SDP answer.
    pub async fn set_remote_answer(&self, answer_sdp: String) -> RtcResult<()> {
        self.inner
            .set_remote_description(RTCSessionDescription::answer(answer_sdp)?)
            .await?;
        Ok(())
    }

    /// Applies a remote SDP offer without creating an answer yet.
    pub async fn set_remote_offer(&self, offer_sdp: String) -> RtcResult<()> {
        self.inner
            .set_remote_description(RTCSessionDescription::offer(offer_sdp)?)
            .await?;
        Ok(())
    }

    /// Forces existing transceivers with the provided MIDs to receive-only.
    pub async fn set_transceivers_recvonly_by_mid<'a>(
        &self,
        mids: impl IntoIterator<Item = &'a str>,
    ) -> RtcResult<()> {
        let mids = mids.into_iter().collect::<std::collections::HashSet<_>>();
        if mids.is_empty() {
            return Ok(());
        }

        for transceiver in self.inner.get_transceivers().await {
            let Some(mid) = transceiver.mid().await? else {
                continue;
            };
            if mids.contains(mid.as_str()) {
                transceiver
                    .set_direction(RTCRtpTransceiverDirection::Recvonly)
                    .await?;
            }
        }
        Ok(())
    }

    /// Creates an SDP answer for the current remote offer and sets it as the local description.
    pub async fn create_answer(&self) -> RtcResult<String> {
        let answer = self.inner.create_answer(None).await?;
        self.inner.set_local_description(answer).await?;
        let local_description = self
            .inner
            .local_description()
            .await
            .ok_or_else(|| std::io::Error::other("local description was not set"))?;
        Ok(local_description.sdp)
    }

    /// Creates an SDP answer for a remote SDP offer and sets it as the local description.
    pub async fn create_answer_for_offer(&self, offer_sdp: String) -> RtcResult<String> {
        self.set_remote_offer(offer_sdp).await?;
        self.create_answer().await
    }

    /// Adds a remote ICE candidate encoded as the LiveKit protobuf `candidate_init` JSON string.
    pub async fn add_ice_candidate_json(&self, candidate_init: &str) -> RtcResult<()> {
        if candidate_init.is_empty() {
            return Ok(());
        }
        let candidate: RTCIceCandidateInit = serde_json::from_str(candidate_init)?;
        self.inner.add_ice_candidate(candidate).await?;
        Ok(())
    }

    /// Closes the underlying peer connection.
    pub async fn close(&self) -> RtcResult<()> {
        self.inner.close().await?;
        Ok(())
    }
}

/// Creates a peer connection configured with default codecs and interceptors.
pub async fn create_peer_connection() -> RtcResult<PeerConnection> {
    create_peer_connection_with_handler(
        Arc::new(NoopPeerConnectionHandler),
        &RtcTransportConfig::default(),
        false,
        None,
        PeerConnectionCodecProfile::Default,
    )
    .await
}

pub async fn create_peer_connection_with_transport(
    transport: &RtcTransportConfig,
) -> RtcResult<PeerConnection> {
    create_peer_connection_with_handler(
        Arc::new(NoopPeerConnectionHandler),
        transport,
        false,
        None,
        PeerConnectionCodecProfile::Default,
    )
    .await
}

/// Creates a peer connection and returns OxideSFU-owned event streams for it.
pub async fn create_peer_connection_with_events()
-> RtcResult<(PeerConnection, PeerConnectionEvents)> {
    create_peer_connection_with_events_with_transport(&RtcTransportConfig::default()).await
}

/// Creates an evented peer connection whose media capability excludes H264.
///
/// This is intentionally narrow for LiveKit compatibility tests that need a
/// subscriber to advertise and negotiate VP8 while rejecting H264.
pub async fn create_peer_connection_with_events_without_h264()
-> RtcResult<(PeerConnection, PeerConnectionEvents)> {
    create_peer_connection_with_events_with_transport_and_block_write_and_codec_profile(
        &RtcTransportConfig::default(),
        false,
        None,
        PeerConnectionCodecProfile::WithoutH264,
    )
    .await
}

pub async fn create_peer_connection_with_events_with_transport(
    transport: &RtcTransportConfig,
) -> RtcResult<(PeerConnection, PeerConnectionEvents)> {
    create_peer_connection_with_events_with_transport_and_block_write(transport, false, None).await
}

/// Creates a peer connection and returns OxideSFU-owned event streams for it
/// with explicit SCTP data-channel block-write behavior.
pub async fn create_peer_connection_with_events_with_transport_and_data_channel_block_write(
    transport: &RtcTransportConfig,
    data_channel_block_write: bool,
) -> RtcResult<(PeerConnection, PeerConnectionEvents)> {
    create_peer_connection_with_events_with_transport_and_block_write(
        transport,
        data_channel_block_write,
        None,
    )
    .await
}

/// Creates a peer connection with Pion-style SCTP data-channel block-write enabled.
///
/// This is intended for compatibility tests that need sender-visible backpressure.
pub async fn create_peer_connection_with_events_with_data_channel_block_write()
-> RtcResult<(PeerConnection, PeerConnectionEvents)> {
    create_peer_connection_with_events_with_transport_and_block_write(
        &RtcTransportConfig::default(),
        true,
        None,
    )
    .await
}

/// Creates a peer connection and returns event streams with explicit data-channel
/// block-write and SCTP receive-buffer settings.
pub async fn create_peer_connection_with_events_with_transport_and_data_channel_options(
    transport: &RtcTransportConfig,
    data_channel_block_write: bool,
    sctp_max_receive_buffer_size: Option<u32>,
) -> RtcResult<(PeerConnection, PeerConnectionEvents)> {
    create_peer_connection_with_events_with_transport_and_block_write(
        transport,
        data_channel_block_write,
        sctp_max_receive_buffer_size,
    )
    .await
}

async fn create_peer_connection_with_events_with_transport_and_block_write(
    transport: &RtcTransportConfig,
    data_channel_block_write: bool,
    sctp_max_receive_buffer_size: Option<u32>,
) -> RtcResult<(PeerConnection, PeerConnectionEvents)> {
    create_peer_connection_with_events_with_transport_and_block_write_and_codec_profile(
        transport,
        data_channel_block_write,
        sctp_max_receive_buffer_size,
        PeerConnectionCodecProfile::Default,
    )
    .await
}

#[derive(Clone, Copy)]
enum PeerConnectionCodecProfile {
    Default,
    WithoutH264,
}

async fn create_peer_connection_with_events_with_transport_and_block_write_and_codec_profile(
    transport: &RtcTransportConfig,
    data_channel_block_write: bool,
    sctp_max_receive_buffer_size: Option<u32>,
    codec_profile: PeerConnectionCodecProfile,
) -> RtcResult<(PeerConnection, PeerConnectionEvents)> {
    let (ice_candidate_tx, ice_candidates) = mpsc::unbounded_channel();
    let (data_channel_tx, data_channels) = mpsc::unbounded_channel();
    let (remote_track_tx, remote_tracks) = mpsc::unbounded_channel();
    let peer_connection = create_peer_connection_with_handler(
        Arc::new(EventPeerConnectionHandler {
            ice_candidate_tx,
            data_channel_tx,
            remote_track_tx,
        }),
        transport,
        data_channel_block_write,
        sctp_max_receive_buffer_size,
        codec_profile,
    )
    .await?;

    Ok((
        peer_connection,
        PeerConnectionEvents {
            ice_candidates,
            data_channels,
            remote_tracks,
        },
    ))
}

fn ensure_rustls_crypto_provider() {
    static RUSTLS_PROVIDER_INIT: OnceLock<()> = OnceLock::new();
    let _ = RUSTLS_PROVIDER_INIT.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn register_codecs(
    media_engine: &mut MediaEngine,
    codec_profile: PeerConnectionCodecProfile,
) -> RtcResult<()> {
    if matches!(codec_profile, PeerConnectionCodecProfile::Default) {
        media_engine.register_default_codecs()?;
        return Ok(());
    }

    for (codec, kind) in [
        (
            RTCRtpCodecParameters {
                rtp_codec: RTCRtpCodec {
                    mime_type: MIME_TYPE_OPUS.to_string(),
                    clock_rate: 48_000,
                    channels: 2,
                    sdp_fmtp_line: "minptime=10;useinbandfec=1".to_string(),
                    rtcp_feedback: vec![],
                },
                payload_type: 111,
            },
            RtpCodecKind::Audio,
        ),
        (
            RTCRtpCodecParameters {
                rtp_codec: RTCRtpCodec {
                    mime_type: MIME_TYPE_VP8.to_string(),
                    clock_rate: 90_000,
                    channels: 0,
                    sdp_fmtp_line: String::new(),
                    rtcp_feedback: vec![],
                },
                payload_type: 96,
            },
            RtpCodecKind::Video,
        ),
    ] {
        media_engine.register_codec(codec, kind)?;
    }
    Ok(())
}

fn rotated_pool_addrs(addrs: &[String], cursor: &AtomicUsize) -> Vec<Vec<String>> {
    if addrs.len() <= 1 {
        return vec![addrs.to_vec()];
    }

    let start = cursor.fetch_add(1, Ordering::Relaxed) % addrs.len();
    (0..addrs.len())
        .map(|offset| vec![addrs[(start + offset) % addrs.len()].clone()])
        .collect()
}

fn is_addr_in_use_error(error: &webrtc::error::Error) -> bool {
    matches!(error, webrtc::error::Error::ErrAddressAlreadyInUse)
        || error.to_string().contains("Address already in use")
        || error.to_string().contains("address already in use")
}

async fn create_peer_connection_with_handler(
    handler: Arc<dyn PeerConnectionEventHandler>,
    transport: &RtcTransportConfig,
    data_channel_block_write: bool,
    sctp_max_receive_buffer_size: Option<u32>,
    codec_profile: PeerConnectionCodecProfile,
) -> RtcResult<PeerConnection> {
    ensure_rustls_crypto_provider();

    static UDP_BIND_CURSOR: AtomicUsize = AtomicUsize::new(0);

    let udp_bind_attempts = rotated_pool_addrs(&transport.udp_addrs, &UDP_BIND_CURSOR);
    let mut last_error: Option<webrtc::error::Error> = None;

    for udp_addrs in udp_bind_attempts {
        let mut media_engine = MediaEngine::default();
        register_codecs(&mut media_engine, codec_profile)?;
        let registry = register_default_interceptors(Registry::new(), &mut media_engine)?;
        let config = RTCConfigurationBuilder::new().build();
        let mut setting_engine = SettingEngine::default();
        if data_channel_block_write {
            setting_engine.detach_data_channels();
            setting_engine.set_data_channel_block_write(true);
        }
        if let Some(max_receive_buffer_size) = sctp_max_receive_buffer_size {
            setting_engine.set_sctp_max_receive_buffer_size(max_receive_buffer_size);
        }
        setting_engine.set_multicast_dns_mode(MulticastDnsMode::QueryOnly);
        setting_engine.set_multicast_dns_timeout(Some(Duration::from_secs(10)));
        if !transport.nat_1to1_ips.is_empty() {
            setting_engine
                .set_nat_1to1_ips(transport.nat_1to1_ips.clone(), RTCIceCandidateType::Host);
        }

        match PeerConnectionBuilder::new()
            .with_configuration(config)
            .with_setting_engine(setting_engine)
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .with_handler(handler.clone())
            .with_udp_addrs(udp_addrs.clone())
            .with_tcp_addrs(transport.tcp_addrs.clone())
            .build()
            .await
        {
            Ok(inner) => {
                return Ok(PeerConnection {
                    inner: Box::new(inner),
                });
            }
            Err(error) => {
                if udp_addrs.len() == 1
                    && transport.udp_addrs.len() > 1
                    && is_addr_in_use_error(&error)
                {
                    last_error = Some(error);
                    continue;
                }
                return Err(Box::new(error));
            }
        }
    }

    match last_error {
        Some(error) => Err(Box::new(error)),
        None => Err(std::io::Error::other("no udp bind attempts configured").into()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn local_h264_rtp_track_advertises_requested_codec_in_offer() {
        let peer = create_peer_connection()
            .await
            .expect("publisher peer connection should create");
        let _track = peer
            .add_local_rtp_track_with_mime(
                "h264-client-track",
                "screen",
                RtpCodecKind::Video,
                "video/h264",
            )
            .await
            .expect("local H264 track should add");

        let offer = peer.create_offer().await.expect("offer should create");
        assert!(offer.to_ascii_lowercase().contains("h264/90000"));
        assert!(offer.contains("profile-level-id=42e01f"));
        assert!(offer.contains("packetization-mode=1"));
    }

    #[tokio::test]
    async fn emits_local_ice_candidate_after_offer_answer() {
        let offerer = create_peer_connection()
            .await
            .expect("offerer peer connection should create");
        let (answerer, mut events) = create_peer_connection_with_events()
            .await
            .expect("answerer peer connection should create");

        let offer_sdp = offerer
            .create_data_channel_offer("data")
            .await
            .expect("data channel offer should create");
        answerer
            .create_answer_for_offer(offer_sdp)
            .await
            .expect("answer should create");

        let candidate = tokio::time::timeout(Duration::from_secs(5), events.ice_candidates.recv())
            .await
            .expect("local ICE candidate should arrive before timeout")
            .expect("local ICE candidate stream should stay open");

        assert!(!candidate.is_final);
        assert!(candidate.candidate_init_json.contains("candidate:"));
        assert!(candidate.candidate_init_json.contains("\"sdpMid\":\"0\""));
        assert!(candidate.candidate_init_json.contains("sdpMLineIndex"));

        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    #[tokio::test]
    async fn creates_peer_connection_with_nat_1to1_transport_configuration() {
        let transport = RtcTransportConfig {
            udp_addrs: vec!["0.0.0.0:0".to_string()],
            tcp_addrs: Vec::new(),
            nat_1to1_ips: vec!["203.0.113.10".to_string()],
        };

        let peer_connection = create_peer_connection_with_transport(&transport)
            .await
            .expect("peer connection should create with nat 1:1 transport configuration");
        let offer = peer_connection
            .create_data_channel_offer("nat-transport-check")
            .await
            .expect("offer should be created with nat 1:1 transport configured");
        assert!(offer.contains("m=application"));

        peer_connection
            .close()
            .await
            .expect("peer connection should close");
    }

    #[tokio::test]
    async fn creates_peer_connection_with_explicit_tcp_transport_addresses() {
        let transport = RtcTransportConfig {
            udp_addrs: vec!["0.0.0.0:0".to_string()],
            tcp_addrs: vec!["127.0.0.1:0".to_string()],
            nat_1to1_ips: Vec::new(),
        };

        let peer_connection = create_peer_connection_with_transport(&transport)
            .await
            .expect("peer connection should create with explicit tcp transport addresses");
        let offer = peer_connection
            .create_data_channel_offer("tcp-transport-check")
            .await
            .expect("offer should be created with tcp transport configured");
        assert!(offer.contains("m=application"));

        peer_connection
            .close()
            .await
            .expect("peer connection should close");
    }

    #[tokio::test]
    async fn creates_multiple_peer_connections_with_udp_bind_pool() {
        let reserved_a = std::net::UdpSocket::bind("127.0.0.1:0")
            .expect("first udp reservation socket should bind");
        let reserved_b = std::net::UdpSocket::bind("127.0.0.1:0")
            .expect("second udp reservation socket should bind");
        let addr_a = reserved_a
            .local_addr()
            .expect("first udp reservation should have local addr");
        let addr_b = reserved_b
            .local_addr()
            .expect("second udp reservation should have local addr");
        drop(reserved_a);
        drop(reserved_b);

        let transport = RtcTransportConfig {
            udp_addrs: vec![addr_a.to_string(), addr_b.to_string()],
            tcp_addrs: Vec::new(),
            nat_1to1_ips: Vec::new(),
        };

        let first = create_peer_connection_with_transport(&transport)
            .await
            .expect("first peer connection should bind one pooled udp address");
        let second = create_peer_connection_with_transport(&transport)
            .await
            .expect("second peer connection should bind remaining pooled udp address");

        let first_offer = first
            .create_data_channel_offer("udp-pool-first")
            .await
            .expect("first offer should create");
        let second_offer = second
            .create_data_channel_offer("udp-pool-second")
            .await
            .expect("second offer should create");
        assert!(first_offer.contains("m=application"));
        assert!(second_offer.contains("m=application"));

        first
            .close()
            .await
            .expect("first peer connection should close");
        second
            .close()
            .await
            .expect("second peer connection should close");
    }

    #[tokio::test]
    async fn create_data_channel_with_options_applies_ordering_and_retransmit_policy() {
        let peer_connection = create_peer_connection()
            .await
            .expect("peer connection should create");

        let reliable = peer_connection
            .create_data_channel("reliable")
            .await
            .expect("reliable data channel should create");
        assert!(
            reliable
                .ordered()
                .await
                .expect("reliable ordered should read"),
            "default reliable data channel should be ordered"
        );
        assert_eq!(
            reliable
                .max_retransmits()
                .await
                .expect("reliable retransmits should read"),
            None,
            "default reliable data channel should not cap retransmits"
        );

        let lossy = peer_connection
            .create_data_channel_with_options(
                "lossy",
                DataChannelOptions {
                    ordered: false,
                    max_retransmits: Some(0),
                },
            )
            .await
            .expect("lossy data channel should create");
        assert!(
            !lossy.ordered().await.expect("lossy ordered should read"),
            "lossy data channel should be unordered"
        );
        assert_eq!(
            lossy
                .max_retransmits()
                .await
                .expect("lossy retransmits should read"),
            Some(0),
            "lossy data channel should cap retransmits at zero"
        );

        peer_connection
            .close()
            .await
            .expect("peer connection should close");
    }

    #[tokio::test]
    async fn multiple_server_created_data_channels_open_between_in_process_peers() {
        let (offerer, offerer_events) = create_peer_connection_with_events()
            .await
            .expect("offerer peer connection should create");
        let (answerer, answerer_events) = create_peer_connection_with_events()
            .await
            .expect("answerer peer connection should create");
        let PeerConnectionEvents {
            ice_candidates: mut offerer_ice_candidates,
            data_channels: _,
            remote_tracks: _,
        } = offerer_events;
        let PeerConnectionEvents {
            ice_candidates: mut answerer_ice_candidates,
            data_channels: mut answerer_data_channels,
            remote_tracks: _,
        } = answerer_events;

        let reliable = offerer
            .create_data_channel("_reliable")
            .await
            .expect("offerer reliable channel should create");
        let lossy = offerer
            .create_data_channel_with_options(
                "_lossy",
                DataChannelOptions {
                    ordered: false,
                    max_retransmits: Some(0),
                },
            )
            .await
            .expect("offerer lossy channel should create");
        let _data_track = offerer
            .create_data_channel_with_options(
                "_data_track",
                DataChannelOptions {
                    ordered: false,
                    max_retransmits: Some(0),
                },
            )
            .await
            .expect("offerer data_track channel should create");

        let offer_sdp = offerer.create_offer().await.expect("offer should create");
        let answer_sdp = answerer
            .create_answer_for_offer(offer_sdp)
            .await
            .expect("answer should create");
        offerer
            .set_remote_answer(answer_sdp)
            .await
            .expect("answer should apply to offerer");

        let reliable_for_open = reliable.clone();
        let lossy_for_open = lossy.clone();
        let open_task = tokio::spawn(async move {
            reliable_for_open.wait_open().await?;
            lossy_for_open.wait_open().await
        });

        let receive_task = tokio::spawn(async move {
            let mut labels = std::collections::BTreeSet::new();
            while labels.len() < 3 {
                let channel = answerer_data_channels
                    .recv()
                    .await
                    .ok_or_else(|| std::io::Error::other("answerer data channel stream ended"))?;
                labels.insert(channel.label().await?);
            }
            Ok::<std::collections::BTreeSet<String>, Box<dyn std::error::Error + Send + Sync>>(
                labels,
            )
        });

        tokio::pin!(open_task);
        tokio::pin!(receive_task);

        let labels = tokio::time::timeout(Duration::from_secs(10), async {
            let mut channels_opened = false;
            let mut labels: Option<std::collections::BTreeSet<String>> = None;

            loop {
                if channels_opened && labels.is_some() {
                    break labels.expect("labels should be set once received");
                }

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
                    opened = &mut open_task, if !channels_opened => {
                        opened
                            .expect("open task should not panic")
                            .expect("reliable/lossy channels should open");
                        channels_opened = true;
                    }
                    received = &mut receive_task, if labels.is_none() => {
                        labels = Some(
                            received
                                .expect("receive task should not panic")
                                .expect("answerer should observe channels")
                        );
                    }
                }
            }
        })
        .await
        .expect("data channels should become ready before timeout");

        assert!(labels.contains("_reliable"));
        assert!(labels.contains("_lossy"));
        assert!(labels.contains("_data_track"));

        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    #[tokio::test]
    async fn data_channel_sends_text_between_in_process_peers() {
        let (offerer, offerer_events) = create_peer_connection_with_events()
            .await
            .expect("offerer peer connection should create");
        let (answerer, answerer_events) = create_peer_connection_with_events()
            .await
            .expect("answerer peer connection should create");
        let PeerConnectionEvents {
            ice_candidates: mut offerer_ice_candidates,
            data_channels: _,
            remote_tracks: _,
        } = offerer_events;
        let PeerConnectionEvents {
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
        let open_task = tokio::spawn(async move { open_channel.wait_open().await });
        let send_channel = offer_channel.clone();
        let recv_task = tokio::spawn(async move {
            let answer_channel = answerer_data_channels
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("answerer data channel stream ended"))?;
            answer_channel.recv_text().await
        });
        tokio::pin!(open_task);
        tokio::pin!(recv_task);

        let mut sent = false;
        let received = tokio::time::timeout(Duration::from_secs(10), async {
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
                    result = &mut open_task, if !sent => {
                        result
                            .expect("open task should not panic")
                            .expect("offerer data channel should open");
                        send_channel
                            .send_text("hello ferrite")
                            .await
                            .expect("text should send");
                        sent = true;
                    }
                    result = &mut recv_task => {
                        break result
                            .expect("recv task should not panic")
                            .expect("answerer should receive text");
                    }
                }
            }
        })
        .await
        .expect("data channel message should arrive before timeout");

        assert_eq!(received, "hello ferrite");

        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    #[tokio::test]
    async fn vp8_forwarding_section_advertises_h264_receiver_capabilities() {
        let forwarder = create_peer_connection()
            .await
            .expect("forwarder peer connection should create");
        forwarder
            .add_forwarding_track_with_mime(
                "PA_publisher",
                "TR_vp8",
                RtpCodecKind::Video,
                Some("video/vp8"),
            )
            .await
            .expect("VP8 forwarding track should add");

        let offer_sdp = forwarder.create_offer().await.expect("offer should create");
        let video_section = offer_sdp
            .split("m=video ")
            .nth(1)
            .expect("offer should contain a video section");

        assert!(video_section.contains("a=rtpmap:96 VP8/90000"));
        assert!(video_section.contains("a=rtpmap:125 H264/90000"));
        assert!(video_section.contains(
            "a=fmtp:125 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
        ));
        assert!(video_section.contains("a=rtpmap:108 H264/90000"));
        assert!(video_section.contains(
            "a=fmtp:108 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f"
        ));

        forwarder.close().await.expect("forwarder should close");
    }

    #[tokio::test]
    async fn av1_forwarding_section_advertises_av1_receiver_capabilities() {
        let forwarder = create_peer_connection()
            .await
            .expect("forwarder peer connection should create");
        forwarder
            .add_forwarding_track_with_mime(
                "PA_publisher",
                "TR_av1",
                RtpCodecKind::Video,
                Some("video/av1"),
            )
            .await
            .expect("AV1 forwarding track should add");

        let offer_sdp = forwarder.create_offer().await.expect("offer should create");
        let video_section = offer_sdp
            .split("m=video ")
            .nth(1)
            .expect("offer should contain a video section");

        assert!(
            video_section.contains("a=rtpmap:45 AV1/90000"),
            "AV1 forwarding offer should advertise AV1 rather than silently falling back to VP8"
        );
        assert!(
            !video_section.contains("a=fmtp:41"),
            "AV1 profile 0 is represented by an omitted profile parameter, matching LiveKit/Pion"
        );

        forwarder.close().await.expect("forwarder should close");
    }

    #[tokio::test]
    async fn negotiated_vp8_forward_track_delivers_rtp_to_in_process_subscriber() {
        let (forwarder, forwarder_events) = create_peer_connection_with_events()
            .await
            .expect("forwarder peer connection should create");
        let (subscriber, subscriber_events) = create_peer_connection_with_events()
            .await
            .expect("subscriber peer connection should create");
        let PeerConnectionEvents {
            ice_candidates: mut forwarder_ice_candidates,
            data_channels: _,
            remote_tracks: _,
        } = forwarder_events;
        let PeerConnectionEvents {
            ice_candidates: mut subscriber_ice_candidates,
            data_channels: _,
            remote_tracks: mut subscriber_remote_tracks,
        } = subscriber_events;

        let forward_track = forwarder
            .add_forwarding_track_with_mime(
                "PA_publisher",
                "TR_vp8",
                RtpCodecKind::Video,
                Some("video/vp8"),
            )
            .await
            .expect("VP8 forwarding track should add");
        let offer_sdp = forwarder.create_offer().await.expect("offer should create");
        let answer_sdp = subscriber
            .create_answer_for_offer(offer_sdp)
            .await
            .expect("answer should create");
        forwarder
            .set_remote_answer(answer_sdp)
            .await
            .expect("answer should apply to forwarder");
        assert!(
            matches!(
                forward_track.bind_result().await,
                crate::ForwardTrackBindResult::Compatible { .. }
            ),
            "VP8 forwarding track should bind to the subscriber's negotiated codec context"
        );

        let received_rtp_task = tokio::spawn(async move {
            let remote_track = subscriber_remote_tracks
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("subscriber remote-track stream ended"))?;
            remote_track.recv_rtp_packet().await
        });
        tokio::pin!(received_rtp_task);

        // This matches the Pion `TrackLocalStaticSample` null VP8 sample sent by
        // upstream `TestSubscribeToCodecUnsupported` before it asserts receipt.
        let pion_null_vp8_sample = bytes::Bytes::from_static(&[0x00, 0xff, 0xff, 0xff, 0xff]);
        let mut next_sequence_number = 1u16;
        let received_packet = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = forwarder_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            subscriber
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("forwarder candidate should add to subscriber");
                        }
                    }
                    candidate = subscriber_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            forwarder
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("subscriber candidate should add to forwarder");
                        }
                    }
                    result = &mut received_rtp_task => {
                        break result
                            .expect("subscriber RTP receive task should not panic")
                            .expect("subscriber should receive forwarded VP8 RTP");
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        forward_track
                            .write_rtp(rtc::rtp::packet::Packet {
                                header: rtc::rtp::header::Header {
                                    version: 2,
                                    payload_type: 96,
                                    sequence_number: next_sequence_number,
                                    timestamp: u32::from(next_sequence_number) * 3_000,
                                    ssrc: 42,
                                    ..Default::default()
                                },
                                payload: pion_null_vp8_sample.clone(),
                            })
                            .await
                            .expect("forwarded VP8 RTP should write");
                        next_sequence_number = next_sequence_number.wrapping_add(1);
                    }
                }
            }
        })
        .await
        .expect("subscriber should receive VP8 RTP before timeout");

        assert_eq!(received_packet.payload, pion_null_vp8_sample);
        assert_eq!(received_packet.header.payload_type, 96);

        forwarder
            .close()
            .await
            .expect("forwarder peer connection should close");
        subscriber
            .close()
            .await
            .expect("subscriber peer connection should close");
    }

    #[tokio::test]
    async fn negotiated_av1_forward_track_delivers_rtp_to_in_process_subscriber() {
        let (forwarder, forwarder_events) = create_peer_connection_with_events()
            .await
            .expect("forwarder peer connection should create");
        let (subscriber, subscriber_events) = create_peer_connection_with_events()
            .await
            .expect("subscriber peer connection should create");
        let PeerConnectionEvents {
            ice_candidates: mut forwarder_ice_candidates,
            data_channels: _,
            remote_tracks: _,
        } = forwarder_events;
        let PeerConnectionEvents {
            ice_candidates: mut subscriber_ice_candidates,
            data_channels: _,
            remote_tracks: mut subscriber_remote_tracks,
        } = subscriber_events;

        let forward_track = forwarder
            .add_forwarding_track_with_mime(
                "PA_publisher",
                "TR_av1",
                RtpCodecKind::Video,
                Some("video/av1"),
            )
            .await
            .expect("AV1 forwarding track should add");
        let offer_sdp = forwarder.create_offer().await.expect("offer should create");
        let answer_sdp = subscriber
            .create_answer_for_offer(offer_sdp)
            .await
            .expect("answer should create");
        forwarder
            .set_remote_answer(answer_sdp)
            .await
            .expect("answer should apply to forwarder");
        assert!(
            matches!(
                forward_track.bind_result().await,
                crate::ForwardTrackBindResult::Compatible { .. }
            ),
            "AV1 forwarding track should bind to the subscriber's negotiated codec context"
        );

        let received_rtp_task = tokio::spawn(async move {
            let remote_track = subscriber_remote_tracks
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("subscriber remote-track stream ended"))?;
            remote_track.recv_rtp_packet().await
        });
        tokio::pin!(received_rtp_task);

        // AV1 aggregation header with one OBU element. The forwarding path must preserve
        // AV1 RTP payload bytes; it does not transcode them to VP8.
        let av1_rtp_payload = bytes::Bytes::from_static(&[0x10, 0x00]);
        let mut next_sequence_number = 1u16;
        let received_packet = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = forwarder_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            subscriber
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("forwarder candidate should add to subscriber");
                        }
                    }
                    candidate = subscriber_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            forwarder
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("subscriber candidate should add to forwarder");
                        }
                    }
                    result = &mut received_rtp_task => {
                        break result
                            .expect("subscriber RTP receive task should not panic")
                            .expect("subscriber should receive forwarded AV1 RTP");
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        forward_track
                            .write_rtp(rtc::rtp::packet::Packet {
                                header: rtc::rtp::header::Header {
                                    version: 2,
                                    payload_type: 45,
                                    sequence_number: next_sequence_number,
                                    timestamp: u32::from(next_sequence_number) * 3_000,
                                    ssrc: 42,
                                    ..Default::default()
                                },
                                payload: av1_rtp_payload.clone(),
                            })
                            .await
                            .expect("forwarded AV1 RTP should write");
                        next_sequence_number = next_sequence_number.wrapping_add(1);
                    }
                }
            }
        })
        .await
        .expect("subscriber should receive AV1 RTP before timeout");

        assert_eq!(received_packet.payload, av1_rtp_payload);
        assert_eq!(received_packet.header.payload_type, 45);

        forwarder
            .close()
            .await
            .expect("forwarder peer connection should close");
        subscriber
            .close()
            .await
            .expect("subscriber peer connection should close");
    }

    #[tokio::test]
    async fn forcing_publish_mid_recvonly_prevents_server_send_answer_on_publish_section() {
        let offerer = create_peer_connection()
            .await
            .expect("offerer peer connection should create");
        let answerer = create_peer_connection()
            .await
            .expect("answerer peer connection should create");

        let offer_sdp = offerer
            .create_audio_offer()
            .await
            .expect("audio offer should create");
        assert!(offer_sdp.contains("a=mid:0"));
        assert!(offer_sdp.contains("a=recvonly"));

        answerer
            .set_remote_offer(offer_sdp)
            .await
            .expect("remote offer should set");
        answerer
            .set_transceivers_recvonly_by_mid(["0"])
            .await
            .expect("publish transceiver should force recvonly");
        let answer_sdp = answerer
            .create_answer()
            .await
            .expect("answer should create");

        assert!(answer_sdp.contains("a=mid:0"));
        assert!(answer_sdp.contains("a=recvonly"));
        assert!(!answer_sdp.contains("a=sendonly"));
        assert!(!answer_sdp.contains("a=sendrecv"));

        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    #[tokio::test]
    async fn h264_forwarding_track_reports_unsupported_after_vp8_only_answer() {
        let forwarder = create_peer_connection()
            .await
            .expect("forwarder peer connection should create");
        let subscriber = create_peer_connection()
            .await
            .expect("subscriber peer connection should create");

        let forwarding_track = forwarder
            .add_forwarding_track_with_mime(
                "PA_publisher",
                "TR_h264",
                RtpCodecKind::Video,
                Some("video/h264"),
            )
            .await
            .expect("H264 forwarding track should attach");
        let offer_sdp = forwarder
            .create_offer()
            .await
            .expect("forwarding offer should create");
        subscriber
            .set_remote_offer(offer_sdp)
            .await
            .expect("subscriber should accept forwarding offer");
        let answer_sdp = subscriber
            .create_answer()
            .await
            .expect("subscriber answer should create");

        let h264_payload_types = answer_sdp
            .lines()
            .filter_map(|line| {
                let rest = line.strip_prefix("a=rtpmap:")?;
                let (payload_type, codec) = rest.split_once(' ')?;
                codec
                    .to_ascii_lowercase()
                    .starts_with("h264/")
                    .then_some(payload_type.to_string())
            })
            .collect::<std::collections::HashSet<_>>();
        assert!(
            !h264_payload_types.is_empty(),
            "default subscriber answer must initially contain H264"
        );

        let h264_payload_type = h264_payload_types
            .iter()
            .next()
            .expect("H264 payload type should exist");
        let vp8_only_answer = answer_sdp
            .lines()
            .map(|line| {
                if line.starts_with("m=video ") {
                    return line.replace(h264_payload_type, "96");
                }
                if line.starts_with(&format!("a=rtpmap:{h264_payload_type} ")) {
                    return "a=rtpmap:96 VP8/90000".to_string();
                }
                if line.starts_with(&format!("a=fmtp:{h264_payload_type}")) {
                    return String::new();
                }
                if line.starts_with(&format!("a=rtcp-fb:{h264_payload_type}")) {
                    return line.replacen(h264_payload_type, "96", 1);
                }
                line.to_string()
            })
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join("\r\n");

        forwarder
            .set_remote_answer(format!("{vp8_only_answer}\r\n"))
            .await
            .expect("forwarder should accept VP8-only answer");
        assert_eq!(
            forwarding_track.bind_result().await,
            crate::ForwardTrackBindResult::UnsupportedCodec,
            "an H264 forwarding track must not bind to a VP8-only answer"
        );

        forwarder.close().await.expect("forwarder should close");
        subscriber.close().await.expect("subscriber should close");
    }

    #[tokio::test]
    async fn local_audio_sender_survives_single_pc_style_recvonly_renegotiations() {
        let (offerer, offerer_events) = create_peer_connection_with_events()
            .await
            .expect("offerer peer connection should create");
        let (answerer, answerer_events) = create_peer_connection_with_events()
            .await
            .expect("answerer peer connection should create");
        let PeerConnectionEvents {
            ice_candidates: mut offerer_ice_candidates,
            data_channels: _,
            remote_tracks: _,
        } = offerer_events;
        let PeerConnectionEvents {
            ice_candidates: mut answerer_ice_candidates,
            data_channels: _,
            remote_tracks: mut answerer_remote_tracks,
        } = answerer_events;

        offerer
            .create_data_channel("data")
            .await
            .expect("offerer data channel should create");
        offerer
            .add_recvonly_transceivers(RtpCodecKind::Audio, 2)
            .await
            .expect("audio recvonly sections should add");
        offerer
            .add_recvonly_transceivers(RtpCodecKind::Video, 3)
            .await
            .expect("video recvonly sections should add");

        // Baseline single-PC style negotiation before any local media publication.
        let initial_offer_sdp = offerer
            .create_offer()
            .await
            .expect("initial offer should create");
        answerer
            .set_remote_offer(initial_offer_sdp)
            .await
            .expect("initial offer should set on answerer");
        answerer
            .set_transceivers_recvonly_by_mid(["0", "1", "2", "3", "4"])
            .await
            .expect("answerer media transceivers should force recvonly");
        let initial_answer_sdp = answerer
            .create_answer()
            .await
            .expect("initial answer should create");
        offerer
            .set_remote_answer(initial_answer_sdp)
            .await
            .expect("initial answer should apply");

        let audio_track = offerer
            .add_local_rtp_track_with_mime(
                "audio-local",
                "stream-local",
                RtpCodecKind::Audio,
                "audio/opus",
            )
            .await
            .expect("local audio track should add");

        let offer_sdp = offerer
            .create_offer()
            .await
            .expect("publication offer should create");

        answerer
            .set_remote_offer(offer_sdp)
            .await
            .expect("publication offer should set on answerer");
        answerer
            .set_transceivers_recvonly_by_mid(["0", "1", "2", "3", "4", "6"])
            .await
            .expect("publication answer should keep media recvonly on answerer");
        let answer_sdp = answerer
            .create_answer()
            .await
            .expect("publication answer should create");

        offerer
            .set_remote_answer(answer_sdp)
            .await
            .expect("publication answer should apply");
        assert_eq!(audio_track.forwarding_mid(), Some("0"));
        assert!(
            matches!(
                audio_track.bind_result().await,
                crate::ForwardTrackBindResult::Compatible { payload_type: 111 }
            ),
            "local audio sender should bind to the Opus answer context"
        );

        let first_remote_task = tokio::spawn(async move {
            let remote_track = answerer_remote_tracks
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("answerer remote-track stream ended"))?;
            let packet = remote_track
                .recv_rtp_packet()
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            Ok::<_, std::io::Error>((remote_track, packet))
        });
        tokio::pin!(first_remote_task);

        let mut sequence_number = 1u16;
        let (remote_track, first_packet) = tokio::time::timeout(Duration::from_secs(10), async {
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
                    result = &mut first_remote_task => {
                        let (track, packet) = result
                            .expect("remote-track task should not panic")
                            .expect("answerer should receive first RTP packet");
                        break (track, packet);
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        audio_track
                            .write_rtp_with_cached_mid(rtc::rtp::packet::Packet {
                                header: rtc::rtp::header::Header {
                                    version: 2,
                                    payload_type: 111,
                                    sequence_number,
                                    timestamp: u32::from(sequence_number) * 960,
                                    ssrc: 0x1234_5678,
                                    ..Default::default()
                                },
                                payload: bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe]),
                            })
                            .await
                            .expect("initial RTP should write");
                        sequence_number = sequence_number.wrapping_add(1);
                    }
                }
            }
        })
        .await
        .expect("answerer should receive first RTP before timeout");
        assert_eq!(first_packet.header.payload_type, 111);

        offerer
            .add_recvonly_transceivers(RtpCodecKind::Audio, 1)
            .await
            .expect("additional audio recvonly section should add");
        let reoffer_sdp = offerer
            .create_offer()
            .await
            .expect("renegotiation offer should create");
        answerer
            .set_remote_offer(reoffer_sdp)
            .await
            .expect("renegotiation offer should set on answerer");
        answerer
            .set_transceivers_recvonly_by_mid(["0", "1", "2", "3", "4", "6", "7"])
            .await
            .expect("renegotiation answer should keep media recvonly on answerer");
        let reanswer_sdp = answerer
            .create_answer()
            .await
            .expect("renegotiation answer should create");
        offerer
            .set_remote_answer(reanswer_sdp)
            .await
            .expect("renegotiation answer should apply");

        let second_packet_task = tokio::spawn(async move { remote_track.recv_rtp_packet().await });
        tokio::pin!(second_packet_task);

        let second_packet = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = offerer_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            answerer
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("offerer candidate should add to answerer after renegotiation");
                        }
                    }
                    candidate = answerer_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            offerer
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("answerer candidate should add to offerer after renegotiation");
                        }
                    }
                    result = &mut second_packet_task => {
                        break result
                            .expect("second packet task should not panic")
                            .expect("answerer should receive RTP after renegotiation");
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        audio_track
                            .write_rtp_with_cached_mid(rtc::rtp::packet::Packet {
                                header: rtc::rtp::header::Header {
                                    version: 2,
                                    payload_type: 111,
                                    sequence_number,
                                    timestamp: u32::from(sequence_number) * 960,
                                    ssrc: 0x1234_5678,
                                    ..Default::default()
                                },
                                payload: bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe]),
                            })
                            .await
                            .expect("post-renegotiation RTP should write");
                        sequence_number = sequence_number.wrapping_add(1);
                    }
                }
            }
        })
        .await
        .expect("answerer should receive post-renegotiation RTP before timeout");
        assert_eq!(second_packet.header.payload_type, 111);

        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    #[tokio::test]
    async fn forwarding_track_to_receive_mid_emits_livekit_compatible_msid_in_answer() {
        let offerer = create_peer_connection()
            .await
            .expect("offerer peer connection should create");
        let answerer = create_peer_connection()
            .await
            .expect("answerer peer connection should create");

        let offer_sdp = offerer
            .create_audio_offer()
            .await
            .expect("audio offer should create");
        assert!(offer_sdp.contains("a=mid:0"));
        assert!(offer_sdp.contains("a=recvonly"));

        answerer
            .set_remote_offer(offer_sdp)
            .await
            .expect("remote offer should set");
        answerer
            .set_transceivers_recvonly_by_mid(["0"])
            .await
            .expect("receive MID should be forced recvonly before attachment");
        let forwarding_track = answerer
            .add_forwarding_track_to_mid("0", "PA_test", "TR_test", RtpCodecKind::Audio)
            .await
            .expect("forwarding track should attach to receive mid");
        assert_eq!(forwarding_track.forwarding_mid(), Some("0"));
        let answer_sdp = answerer
            .create_answer()
            .await
            .expect("answer should create");

        assert!(answer_sdp.contains("a=mid:0"));
        assert!(answer_sdp.contains("a=sendonly"));
        assert!(
            answer_sdp.contains("a=msid:PA_test|TR_test TR_test"),
            "single-PC downtrack answer should expose participant/track msid for Go SDK mapping; SDP:\n{answer_sdp}"
        );

        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    #[tokio::test]
    async fn creates_answer_for_data_channel_offer() {
        let offerer = create_peer_connection()
            .await
            .expect("offerer peer connection should create");
        let answerer = create_peer_connection()
            .await
            .expect("answerer peer connection should create");

        let offer_sdp = offerer
            .create_data_channel_offer("data")
            .await
            .expect("data channel offer should create");
        let answer_sdp = answerer
            .create_answer_for_offer(offer_sdp)
            .await
            .expect("answer should create");

        assert!(answer_sdp.starts_with("v=0"));
        assert!(answer_sdp.contains("m=application"));

        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    #[tokio::test]
    async fn adds_ice_candidate_json() {
        let offerer = create_peer_connection()
            .await
            .expect("offerer peer connection should create");
        let answerer = create_peer_connection()
            .await
            .expect("answerer peer connection should create");

        let offer_sdp = offerer
            .create_data_channel_offer("data")
            .await
            .expect("data channel offer should create");
        answerer
            .create_answer_for_offer(offer_sdp)
            .await
            .expect("answer should create");

        answerer
            .add_ice_candidate_json(
                r#"{"candidate":"candidate:0 1 UDP 2122252543 127.0.0.1 12345 typ host","sdpMid":"0","sdpMLineIndex":0}"#,
            )
            .await
            .expect("candidate should add");

        offerer.close().await.expect("offerer should close");
        answerer.close().await.expect("answerer should close");
    }

    #[tokio::test]
    async fn creates_data_channel_offer() {
        let peer_connection = create_peer_connection()
            .await
            .expect("peer connection should create");
        let sdp = peer_connection
            .create_data_channel_offer("data")
            .await
            .expect("data channel offer should create");

        assert!(sdp.starts_with("v=0"));
        assert!(sdp.contains("m=application"));

        peer_connection
            .close()
            .await
            .expect("peer connection should close");
    }

    #[tokio::test]
    async fn local_forwarding_track_accepts_sender_report_rtcp_packets() {
        let peer_connection = create_peer_connection()
            .await
            .expect("peer connection should create");

        let forwarding_track = peer_connection
            .add_forwarding_track("PA_test", "TR_test", RtpCodecKind::Video)
            .await
            .expect("forwarding track should add");

        forwarding_track
            .write_rtcp_packets(vec![Box::new(rtc::rtcp::sender_report::SenderReport {
                ssrc: 0x1234,
                rtp_time: 90_000,
                ..Default::default()
            })])
            .await
            .expect("forwarding track should accept sender report RTCP packets");

        peer_connection
            .close()
            .await
            .expect("peer connection should close");
    }

    #[tokio::test]
    async fn forwarding_offer_contains_livekit_compatible_stream_and_track_ids() {
        let peer_connection = create_peer_connection()
            .await
            .expect("peer connection should create");

        let _forwarding_track = peer_connection
            .add_forwarding_track("PA_test", "TR_test", RtpCodecKind::Audio)
            .await
            .expect("forwarding track should add");

        let offer_sdp = peer_connection
            .create_offer()
            .await
            .expect("offer should create");

        assert!(
            offer_sdp.contains("a=msid:PA_test|TR_test TR_test"),
            "forwarding offer should expose participant/track msid for SDK mapping"
        );
        assert!(
            offer_sdp.contains("a=sendonly"),
            "subscriber forwarding offer must be sendonly"
        );

        peer_connection
            .close()
            .await
            .expect("peer connection should close");
    }

    #[tokio::test]
    async fn local_forwarding_track_accepts_rtcp_packets() {
        let peer_connection = create_peer_connection()
            .await
            .expect("peer connection should create");

        let forwarding_track = peer_connection
            .add_forwarding_track("PA_test", "TR_test", RtpCodecKind::Video)
            .await
            .expect("forwarding track should add");

        forwarding_track
            .write_rtcp_packets(vec![Box::new(
                rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                    sender_ssrc: 0,
                    media_ssrc: 1234,
                },
            )])
            .await
            .expect("forwarding track should accept RTCP packets");

        peer_connection
            .close()
            .await
            .expect("peer connection should close");
    }

    #[test]
    fn forwarding_codec_for_audio_prefers_expected_mime_profiles() {
        let pcmu = PeerConnection::forwarding_codec_for(RtpCodecKind::Audio, Some("audio/pcmu"));
        assert_eq!(pcmu.mime_type, MIME_TYPE_PCMU);
        assert_eq!(pcmu.clock_rate, 8_000);
        assert_eq!(pcmu.channels, 1);

        let pcmu_mixed_case =
            PeerConnection::forwarding_codec_for(RtpCodecKind::Audio, Some("  AuDiO/PcMu  "));
        assert_eq!(pcmu_mixed_case.mime_type, MIME_TYPE_PCMU);

        let pcma = PeerConnection::forwarding_codec_for(RtpCodecKind::Audio, Some("audio/pcma"));
        assert_eq!(pcma.mime_type, MIME_TYPE_PCMA);
        assert_eq!(pcma.clock_rate, 8_000);
        assert_eq!(pcma.channels, 1);

        let opus_default = PeerConnection::forwarding_codec_for(RtpCodecKind::Audio, None);
        assert_eq!(opus_default.mime_type, MIME_TYPE_OPUS);
        assert_eq!(opus_default.clock_rate, 48_000);
        assert_eq!(opus_default.channels, 2);
        assert!(opus_default.sdp_fmtp_line.contains("useinbandfec=1"));
    }

    #[tokio::test]
    async fn creates_audio_offer() {
        let peer_connection = create_peer_connection()
            .await
            .expect("peer connection should create");
        let sdp = peer_connection
            .create_audio_offer()
            .await
            .expect("audio offer should create");

        assert!(sdp.starts_with("v=0"));
        assert!(
            sdp.contains("m=audio"),
            "audio offer should include an audio media section"
        );

        peer_connection
            .close()
            .await
            .expect("peer connection should close");
    }
}

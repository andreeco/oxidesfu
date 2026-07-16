use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use rtc::{rtcp, rtp, rtp_transceiver::rtp_sender::RtpCodecKind};
use webrtc::media_stream::{
    track_local::{
        TrackLocal as WebRtcTrackLocal,
        static_rtp::{TrackLocalStaticRTP, TrackLocalStaticRtpBindResult},
    },
    track_remote::{TrackRemote as WebRtcTrackRemote, TrackRemoteEvent},
};
use webrtc::rtp_transceiver::RtpSender as WebRtcRtpSender;

use crate::{RemoteTrackEvent, RtcResult};

static NEXT_SSRC: AtomicU32 = AtomicU32::new(0x1000_0000);

pub(crate) fn next_ssrc() -> u32 {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos())
        .unwrap_or_default();
    NEXT_SSRC.fetch_add(1, Ordering::Relaxed) ^ now_nanos
}

#[derive(Debug, Clone, Default)]
struct TemporalLayerFpsEstimate {
    last_timestamp_by_tid: [Option<u32>; 3],
    ema_fps_by_tid: [Option<f32>; 3],
}

impl TemporalLayerFpsEstimate {
    fn observe(&mut self, temporal_id: u8, timestamp: u32) {
        let index = temporal_id as usize;
        if index >= self.last_timestamp_by_tid.len() {
            return;
        }

        if let Some(previous) = self.last_timestamp_by_tid[index] {
            let delta = timestamp.wrapping_sub(previous);
            if delta > 0 {
                let instant_fps = 90_000_f32 / delta as f32;
                let smoothed = match self.ema_fps_by_tid[index] {
                    Some(current) => current * 0.7 + instant_fps * 0.3,
                    None => instant_fps,
                };
                self.ema_fps_by_tid[index] = Some(smoothed);
            }
        }

        self.last_timestamp_by_tid[index] = Some(timestamp);
    }

    fn as_array(&self) -> [Option<f32>; 3] {
        self.ema_fps_by_tid
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn dependency_descriptor_is_switch_point(
    metadata: &rtp::extension::dependency_descriptor_extension::DependencyDescriptorPacketMetadata,
) -> bool {
    metadata.first_packet_in_frame && metadata.has_switching_decode_target
}

/// Owned dependency-descriptor target metadata retained for the latest RTP packet on an SSRC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DependencyDescriptorTargetMetadata {
    /// Decode-target index in the descriptor structure.
    pub target: u8,
    /// Maximum temporal layer for this decode target.
    pub temporal_id: u8,
    /// Maximum spatial layer for this decode target.
    pub spatial_id: u8,
}

/// Owned dependency-descriptor metadata retained for the latest RTP packet on an SSRC.
///
/// The snapshot deliberately avoids exposing the RTC parser's mutable state. It is cleared when
/// the latest packet does not yield descriptor metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyDescriptorMetadataSnapshot {
    /// RTP header extension ID that carried the parsed source descriptor.
    pub source_extension_id: u8,
    /// Frame number carried by the descriptor.
    pub frame_number: u64,
    /// Whether this packet starts its descriptor frame.
    pub first_packet_in_frame: bool,
    /// Whether this frame contains an active DTI Switch decode target.
    pub has_switching_decode_target: bool,
    /// Spatial layer carried by this packet.
    pub spatial_id: u8,
    /// Temporal layer carried by this packet.
    pub temporal_id: u8,
    /// Effective active decode-target mask.
    pub active_decode_targets: u32,
    /// Layer bounds for each decode target.
    pub target_layers: Vec<DependencyDescriptorTargetMetadata>,
    /// Per-target decode target indications in wire target order.
    pub decode_target_indications: Vec<
        rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication,
    >,
    /// Frame-number differences to required frames.
    pub frame_diffs: Vec<u16>,
    /// Per-chain frame-number differences.
    pub chain_diffs: Vec<u16>,
    /// Chain protecting each decode target.
    pub target_protected_by_chain: Vec<u8>,
}

/// Metadata derived exactly once from an incoming RTP packet.
///
/// This travels with [`IncomingRtpPacket`] to avoid storing packet-local state in a remote-track
/// map and immediately locking that map again in the forwarding reader.
#[derive(Debug, Clone, Default)]
pub struct IncomingRtpMetadata {
    /// Parsed temporal layer when the codec or descriptor carries one.
    pub temporal_layer: Option<u8>,
    /// Parsed spatial layer when the codec or descriptor carries one.
    pub spatial_layer: Option<u8>,
    /// `Some` when a dependency descriptor was parsed for this packet.
    pub dependency_descriptor_switch_point: Option<bool>,
    /// Owned descriptor metadata for this exact packet.
    pub dependency_descriptor: Option<DependencyDescriptorMetadataSnapshot>,
    /// Receiver-observed temporal cadence for this packet's source and spatial layer.
    pub temporal_layer_fps: Option<[Option<f32>; 3]>,
}

/// An RTP packet and metadata derived while it was received.
pub struct IncomingRtpPacket {
    /// The original incoming RTP packet.
    pub packet: rtp::Packet,
    /// Metadata derived from this exact packet.
    pub metadata: IncomingRtpMetadata,
}

impl DependencyDescriptorMetadataSnapshot {
    fn from_packet_metadata(
        source_extension_id: u8,
        metadata: rtp::extension::dependency_descriptor_extension::DependencyDescriptorPacketMetadata,
    ) -> Self {
        Self {
            source_extension_id,
            frame_number: u64::from(metadata.frame_number),
            first_packet_in_frame: metadata.first_packet_in_frame,
            has_switching_decode_target: metadata.has_switching_decode_target,
            spatial_id: metadata.layer_ids.spatial_id,
            temporal_id: metadata.layer_ids.temporal_id,
            active_decode_targets: metadata.active_decode_targets_mask,
            target_layers: metadata
                .decode_target_layers
                .into_iter()
                .enumerate()
                .filter_map(|(target, layer)| {
                    u8::try_from(target)
                        .ok()
                        .map(|target| DependencyDescriptorTargetMetadata {
                            target,
                            temporal_id: layer.temporal_id,
                            spatial_id: layer.spatial_id,
                        })
                })
                .collect(),
            decode_target_indications: metadata.decode_target_indications,
            frame_diffs: metadata.frame_diffs,
            chain_diffs: metadata.chain_diffs.into_iter().map(u16::from).collect(),
            target_protected_by_chain: metadata.decode_target_protected_by_chain,
        }
    }
}

#[derive(Debug, Default)]
struct RemoteTrackState {
    codec_mime_by_ssrc: Mutex<HashMap<u32, Option<String>>>,
    temporal_fps_by_ssrc_spatial: Mutex<HashMap<(u32, u8), TemporalLayerFpsEstimate>>,
    dd_parser_by_ssrc: Mutex<
        HashMap<u32, rtp::extension::dependency_descriptor_extension::DependencyDescriptorParser>,
    >,
}

impl RemoteTrackState {
    fn observe_dependency_descriptor_packet(
        &self,
        packet: &rtp::Packet,
    ) -> Option<DependencyDescriptorMetadataSnapshot> {
        let ssrc = packet.header.ssrc;
        if !packet.header.extension {
            return None;
        }

        self.dd_parser_by_ssrc.lock().ok().and_then(|mut parsers| {
            let parser = parsers.entry(ssrc).or_default();
            packet.header.extensions.iter().find_map(|extension| {
                parser
                    .parse_packet_metadata(&extension.payload)
                    .map(|metadata| {
                        DependencyDescriptorMetadataSnapshot::from_packet_metadata(
                            extension.id,
                            metadata,
                        )
                    })
            })
        })
    }
}

/// OxideSFU-owned wrapper around a remote media track.
#[derive(Clone)]
pub struct RemoteTrack {
    pub(crate) inner: Arc<dyn WebRtcTrackRemote>,
    state: Arc<RemoteTrackState>,
}

impl RemoteTrack {
    pub(crate) fn new(inner: Arc<dyn WebRtcTrackRemote>) -> Self {
        Self {
            inner,
            state: Arc::new(RemoteTrackState::default()),
        }
    }

    async fn codec_mime_cached_for_ssrc(&self, ssrc: u32) -> Option<String> {
        if let Ok(cache) = self.state.codec_mime_by_ssrc.lock()
            && let Some(existing) = cache.get(&ssrc)
        {
            return existing.clone();
        }

        let resolved = self.inner.codec(ssrc).await.map(|codec| codec.mime_type);
        if let Ok(mut cache) = self.state.codec_mime_by_ssrc.lock() {
            cache.insert(ssrc, resolved.clone());
        }
        resolved
    }

    async fn observe_rtp_metadata(&self, packet: &rtp::Packet) -> IncomingRtpMetadata {
        let ssrc = packet.header.ssrc;
        let dependency_descriptor = self.state.observe_dependency_descriptor_packet(packet);
        let dependency_descriptor_switch_point = dependency_descriptor
            .as_ref()
            .map(|metadata| metadata.first_packet_in_frame && metadata.has_switching_decode_target);

        let (spatial_layer, temporal_layer) = if let Some(metadata) = dependency_descriptor.as_ref()
        {
            (Some(metadata.spatial_id), Some(metadata.temporal_id))
        } else {
            let codec_mime = self.codec_mime_cached_for_ssrc(ssrc).await;
            let mime = codec_mime.as_deref().map(str::to_ascii_lowercase);
            match mime.as_deref() {
                Some(mime) if mime.contains("vp8") => (
                    Some(0),
                    rtp::codec::vp8::temporal_layer_id_from_payload(&packet.payload),
                ),
                Some(mime) if mime.contains("vp9") => {
                    if let Some(layer_ids) =
                        rtp::codec::vp9::layer_ids_from_payload(&packet.payload)
                    {
                        (Some(layer_ids.spatial_id), Some(layer_ids.temporal_id))
                    } else {
                        (
                            Some(0),
                            rtp::codec::vp9::temporal_layer_id_from_payload(&packet.payload),
                        )
                    }
                }
                Some(mime) if mime.contains("h265") => (
                    Some(0),
                    rtp::codec::h265::temporal_layer_id_from_payload(&packet.payload),
                ),
                _ => (None, None),
            }
        };

        let temporal_layer_fps = match (spatial_layer, temporal_layer) {
            (Some(spatial_layer), Some(temporal_layer)) => self
                .state
                .temporal_fps_by_ssrc_spatial
                .lock()
                .ok()
                .map(|mut stats| {
                    let estimate = stats.entry((ssrc, spatial_layer)).or_default();
                    estimate.observe(temporal_layer, packet.header.timestamp);
                    estimate.as_array()
                }),
            _ => None,
        };

        IncomingRtpMetadata {
            temporal_layer,
            spatial_layer,
            dependency_descriptor_switch_point,
            dependency_descriptor,
            temporal_layer_fps,
        }
    }

    /// Returns remote track identifier.
    pub async fn track_id(&self) -> String {
        self.inner.track_id().await.to_string()
    }

    /// Returns the negotiated SDP media-section MID when available.
    pub async fn mid(&self) -> Option<String> {
        self.inner.mid().await
    }

    /// Returns remote stream identifier.
    pub async fn stream_id(&self) -> String {
        self.inner.stream_id().await.to_string()
    }

    /// Returns remote track media kind.
    pub async fn kind(&self) -> RtpCodecKind {
        self.inner.kind().await
    }

    /// Returns SSRCs associated with this remote track.
    pub async fn ssrcs(&self) -> Vec<u32> {
        self.inner.ssrcs().await
    }

    /// Returns RID for an incoming SSRC when available.
    pub async fn rid_for_ssrc(&self, ssrc: u32) -> Option<String> {
        self.inner.rid(ssrc).await.map(|rid| rid.to_string())
    }

    /// Returns codec MIME for an incoming SSRC when available.
    pub async fn codec_mime_for_ssrc(&self, ssrc: u32) -> Option<String> {
        self.codec_mime_cached_for_ssrc(ssrc).await
    }

    /// Writes RTCP feedback packets to the remote sender.
    pub async fn write_rtcp_packets(&self, packets: Vec<Box<dyn rtcp::Packet>>) -> RtcResult<()> {
        self.inner.write_rtcp(packets).await?;
        Ok(())
    }

    /// Receives the next remote-track event.
    pub async fn recv_event(&self) -> RtcResult<RemoteTrackEvent> {
        while let Some(event) = self.inner.poll().await {
            match event {
                TrackRemoteEvent::OnRtpPacket(packet) => {
                    let metadata = self.observe_rtp_metadata(&packet).await;
                    return Ok(RemoteTrackEvent::RtpPacket(IncomingRtpPacket {
                        packet,
                        metadata,
                    }));
                }
                TrackRemoteEvent::OnRtcpPacket(packets) => {
                    return Ok(RemoteTrackEvent::RtcpPacket(packets));
                }
                TrackRemoteEvent::OnEnded => return Ok(RemoteTrackEvent::Ended),
                TrackRemoteEvent::OnError => {
                    return Err(std::io::Error::other("remote track emitted error event").into());
                }
                _ => {}
            }
        }
        Err(std::io::Error::other("remote track event stream ended").into())
    }

    /// Receives the next RTP packet for this remote track.
    pub async fn recv_rtp_packet(&self) -> RtcResult<rtp::Packet> {
        loop {
            match self.recv_event().await? {
                RemoteTrackEvent::RtpPacket(incoming) => return Ok(incoming.packet),
                RemoteTrackEvent::RtcpPacket(_) => continue,
                RemoteTrackEvent::Ended => {
                    return Err(
                        std::io::Error::other("remote track ended before RTP packet").into(),
                    );
                }
            }
        }
    }
}

/// Post-negotiation binding outcome for a forwarding RTP track.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardTrackBindResult {
    /// The track has not received a negotiated sender context.
    Pending,
    /// The subscriber selected a compatible output payload type.
    Compatible { payload_type: u8 },
    /// The subscriber selected no compatible codec.
    UnsupportedCodec,
}

/// OxideSFU-owned wrapper around a local forwarding RTP track.
#[derive(Clone)]
pub struct LocalRtpTrack {
    pub(crate) inner: Arc<TrackLocalStaticRTP>,
    pub(crate) sender: Arc<dyn WebRtcRtpSender>,
    pub(crate) forwarding_mid: Option<String>,
    pub(crate) forwarding_mid_bytes: Option<Bytes>,
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)] // LocalRtpTrack implementation follows its focused tests.
mod tests {
    use super::{
        RemoteTrackState, TemporalLayerFpsEstimate, dependency_descriptor_is_switch_point,
    };
    use bytes::Bytes;
    use rtc::rtp::{
        Packet,
        extension::dependency_descriptor_extension::{
            DependencyDescriptorLayerIds, DependencyDescriptorPacketMetadata,
        },
        header::{Extension, Header},
    };

    #[derive(Default)]
    struct BitWriter {
        bits: Vec<bool>,
    }

    impl BitWriter {
        fn push(&mut self, value: u64, width: usize) {
            for bit in (0..width).rev() {
                self.bits.push((value & (1 << bit)) != 0);
            }
        }

        fn push_bool(&mut self, value: bool) {
            self.bits.push(value);
        }

        fn into_bytes(self) -> Vec<u8> {
            let mut bytes = vec![0; self.bits.len().div_ceil(8)];
            for (index, bit) in self.bits.into_iter().enumerate() {
                if bit {
                    bytes[index / 8] |= 1 << (7 - index % 8);
                }
            }
            bytes
        }
    }

    fn descriptor_packet(ssrc: u32, sequence_number: u16, payload: Vec<u8>) -> Packet {
        Packet {
            header: Header {
                extension: true,
                sequence_number,
                ssrc,
                extensions: vec![Extension {
                    id: 1,
                    payload: Bytes::from(payload),
                }],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn descriptor_with_structure(frame_number: u16) -> Vec<u8> {
        let mut writer = BitWriter::default();
        writer.push_bool(true); // first packet in frame
        writer.push_bool(true); // last packet in frame
        writer.push(0, 6); // frame dependency template id
        writer.push(u64::from(frame_number), 16);
        writer.push_bool(true); // template dependency structure present
        writer.push_bool(false); // active decode targets present
        writer.push_bool(false); // custom DTIs
        writer.push_bool(false); // custom frame differences
        writer.push_bool(false); // custom chains
        writer.push(0, 6); // structure id
        writer.push(0, 5); // one decode target
        writer.push(3, 2); // one template, no layer transition
        writer.push(3, 2); // template DTI is required, not Switch
        writer.push_bool(false); // no frame differences
        writer.push_bool(false); // no chains
        writer.push_bool(false); // no resolutions
        writer.into_bytes()
    }

    fn descriptor_with_switch_target(frame_number: u16) -> Vec<u8> {
        let mut writer = BitWriter::default();
        writer.push_bool(true); // first packet in frame
        writer.push_bool(true); // last packet in frame
        writer.push(0, 6); // cached frame dependency template id
        writer.push(u64::from(frame_number), 16);
        writer.push_bool(false); // template dependency structure present
        writer.push_bool(true); // active decode targets present
        writer.push_bool(true); // custom DTIs
        writer.push_bool(false); // custom frame differences
        writer.push_bool(false); // custom chains
        writer.push(1, 1); // the sole decode target is active
        writer.push(2, 2); // DTI Switch
        writer.into_bytes()
    }

    #[test]
    fn dependency_descriptor_switch_requires_frame_start_and_switch_target() {
        let metadata = DependencyDescriptorPacketMetadata {
            frame_number: 1,
            layer_ids: DependencyDescriptorLayerIds {
                temporal_id: 0,
                spatial_id: 2,
            },
            first_packet_in_frame: true,
            last_packet_in_frame: false,
            active_decode_targets_mask: 1,
            decode_target_indications: vec![
                rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorDecodeTargetIndication::Switch
            ],
            decode_target_layers: vec![DependencyDescriptorLayerIds {
                temporal_id: 0,
                spatial_id: 2,
            }],
            frame_diffs: vec![],
            chain_diffs: vec![],
            decode_target_protected_by_chain: vec![],
            has_switching_decode_target: true,
        };
        assert!(dependency_descriptor_is_switch_point(&metadata));

        assert!(!dependency_descriptor_is_switch_point(
            &DependencyDescriptorPacketMetadata {
                has_switching_decode_target: false,
                ..metadata.clone()
            }
        ));
        assert!(!dependency_descriptor_is_switch_point(
            &DependencyDescriptorPacketMetadata {
                first_packet_in_frame: false,
                ..metadata
            }
        ));
    }

    #[test]
    fn remote_track_state_returns_packet_local_dependency_descriptor_metadata() {
        let state = RemoteTrackState::default();
        let ssrc = 0x1234_5678;

        let non_switch = descriptor_packet(ssrc, 100, descriptor_with_structure(1));
        let non_switch_snapshot = state
            .observe_dependency_descriptor_packet(&non_switch)
            .expect("descriptor packet should return packet-local metadata");
        assert!(non_switch_snapshot.first_packet_in_frame);
        assert!(!non_switch_snapshot.has_switching_decode_target);

        let switch = descriptor_packet(ssrc, 101, descriptor_with_switch_target(2));
        let snapshot = state
            .observe_dependency_descriptor_packet(&switch)
            .expect("later descriptor packet should return its own metadata");
        assert_eq!(snapshot.source_extension_id, 1);
        assert_eq!(snapshot.frame_number, 2);
        assert!(snapshot.first_packet_in_frame);
        assert!(snapshot.has_switching_decode_target);
        assert_eq!(snapshot.active_decode_targets, 1);
        assert_eq!(snapshot.target_layers.len(), 1);
        assert_eq!(snapshot.decode_target_indications.len(), 1);

        let without_descriptor = Packet {
            header: Header {
                sequence_number: 102,
                ssrc,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            state
                .observe_dependency_descriptor_packet(&without_descriptor)
                .is_none(),
            "a descriptor-free packet must not inherit prior packet metadata"
        );
    }

    #[test]
    fn temporal_layer_fps_estimate_tracks_per_temporal_layer_independently() {
        let mut estimate = TemporalLayerFpsEstimate::default();

        // Two observations on T0 and T1 with distinct cadence.
        estimate.observe(0, 0);
        estimate.observe(1, 0);
        estimate.observe(0, 9_000);
        estimate.observe(1, 4_500);

        let fps = estimate.as_array();
        assert!(fps[0].is_some());
        assert!(fps[1].is_some());
        assert!(fps[2].is_none());

        let t0 = fps[0].expect("t0 fps should be tracked");
        let t1 = fps[1].expect("t1 fps should be tracked");
        assert!(t1 > t0, "higher temporal layer cadence should be higher");
    }
}

impl LocalRtpTrack {
    /// Returns the primary SSRC configured for this forwarding track.
    pub async fn primary_ssrc(&self) -> Option<u32> {
        self.inner.track().await.ssrcs().next()
    }

    /// Returns the negotiated MID this forwarding track is bound to, when known.
    pub fn forwarding_mid(&self) -> Option<&str> {
        self.forwarding_mid.as_deref()
    }

    /// Returns the negotiated dependency-descriptor RTP header extension ID, when available.
    pub async fn dependency_descriptor_extension_id(&self) -> Option<u8> {
        self.sender
            .get_parameters()
            .await
            .ok()?
            .rtp_parameters
            .header_extensions
            .iter()
            .find(|extension| {
                extension.uri
                    == rtp::extension::dependency_descriptor_extension::DEPENDENCY_DESCRIPTOR_URI
            })
            .and_then(|extension| u8::try_from(extension.id).ok())
    }

    /// Returns the negotiated codec binding outcome for this forwarding track.
    pub async fn bind_result(&self) -> ForwardTrackBindResult {
        match self.inner.bind_result().await {
            TrackLocalStaticRtpBindResult::Pending => ForwardTrackBindResult::Pending,
            TrackLocalStaticRtpBindResult::Compatible { payload_type } => {
                ForwardTrackBindResult::Compatible { payload_type }
            }
            TrackLocalStaticRtpBindResult::UnsupportedCodec => {
                ForwardTrackBindResult::UnsupportedCodec
            }
        }
    }

    /// Writes RTP packet to this local forwarding track.
    pub async fn write_rtp(&self, packet: rtp::Packet) -> RtcResult<()> {
        self.inner.write_rtp(packet).await?;
        Ok(())
    }

    /// Writes RTP packet with this track's cached SDES MID extension.
    ///
    /// This variant clears incoming header extensions first.
    pub async fn write_rtp_with_cached_mid(&self, packet: rtp::Packet) -> RtcResult<()> {
        let Some(mid) = self.forwarding_mid_bytes.as_ref() else {
            return self.write_rtp(packet).await;
        };
        self.inner
            .write_rtp_with_sdes_mid_bytes(packet, mid.clone(), false)
            .await?;
        Ok(())
    }

    /// Writes RTP packet with an explicit SDES MID extension mapped to the sender's negotiated ext ID
    /// while preserving any existing RTP header extensions.
    pub async fn write_rtp_with_cached_mid_preserving_extensions(
        &self,
        packet: rtp::Packet,
    ) -> RtcResult<()> {
        let Some(mid) = self.forwarding_mid_bytes.as_ref() else {
            return self.write_rtp(packet).await;
        };
        // Hot-path note: use the webrtc fast path that injects MID directly,
        // avoiding per-packet extension trait-object marshal/allocation.
        self.inner
            .write_rtp_with_sdes_mid_bytes(packet, mid.clone(), true)
            .await?;
        Ok(())
    }

    /// Writes a prepared batch of video RTP packets while preserving target-local extensions.
    ///
    /// The WebRTC driver accepts at most 64 packets per batch and requires a compatible sender
    /// binding plus this wrapper's cached forwarding MID. Callers must use this only after
    /// observing [`ForwardTrackBindResult::Compatible`].
    pub async fn write_rtp_batch_with_cached_mid_preserving_extensions(
        &self,
        packets: Vec<rtp::Packet>,
    ) -> RtcResult<()> {
        let Some(mid) = self.forwarding_mid_bytes.clone() else {
            return Err(std::io::Error::other(
                "prepared RTP batch requires a cached forwarding MID",
            )
            .into());
        };
        self.inner
            .write_rtp_batch_with_sdes_mid_bytes(packets, mid, true)
            .await?;
        Ok(())
    }

    /// Writes RTCP packets to this local forwarding track sender.
    pub async fn write_rtcp_packets(&self, packets: Vec<Box<dyn rtcp::Packet>>) -> RtcResult<()> {
        self.inner.write_rtcp(packets).await?;
        Ok(())
    }
}

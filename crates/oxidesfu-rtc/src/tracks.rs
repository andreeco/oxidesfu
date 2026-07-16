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

#[derive(Debug, Clone, Copy, Default)]
struct ObservedLayerIds {
    temporal_id: u8,
    spatial_id: u8,
}

fn dependency_descriptor_is_switch_point(
    metadata: &rtp::extension::dependency_descriptor_extension::DependencyDescriptorPacketMetadata,
) -> bool {
    metadata.first_packet_in_frame && metadata.has_switching_decode_target
}

/// Dependency-descriptor availability observed for the latest RTP packet on an SSRC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyDescriptorLayerAvailability {
    /// RTP was received, but it was not a descriptor-backed decoder-usable frame boundary.
    RtpSeen,
    /// The descriptor reports an active DTI Switch target at a frame boundary.
    DecoderUsable,
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

impl DependencyDescriptorMetadataSnapshot {
    fn from_packet_metadata(
        source_extension_id: u8,
        metadata: rtp::extension::dependency_descriptor_extension::DependencyDescriptorPacketMetadata,
    ) -> Self {
        Self {
            source_extension_id,
            frame_number: u64::from(metadata.frame_number),
            first_packet_in_frame: metadata.first_packet_in_frame,
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
    last_dd_switch_point_by_ssrc: Mutex<HashMap<u32, bool>>,
    last_dd_availability_by_ssrc: Mutex<HashMap<u32, DependencyDescriptorLayerAvailability>>,
    last_dd_metadata_by_ssrc: Mutex<HashMap<u32, DependencyDescriptorMetadataSnapshot>>,
    last_observed_layer_by_ssrc: Mutex<HashMap<u32, ObservedLayerIds>>,
}

impl RemoteTrackState {
    fn observe_dependency_descriptor_packet(
        &self,
        packet: &rtp::Packet,
    ) -> Option<rtp::extension::dependency_descriptor_extension::DependencyDescriptorPacketMetadata>
    {
        let ssrc = packet.header.ssrc;
        let metadata = if packet.header.extension {
            self.dd_parser_by_ssrc.lock().ok().and_then(|mut parsers| {
                let parser = parsers.entry(ssrc).or_default();
                packet.header.extensions.iter().find_map(|extension| {
                    parser
                        .parse_packet_metadata(&extension.payload)
                        .map(|metadata| (extension.id, metadata))
                })
            })
        } else {
            None
        };

        let availability = metadata.as_ref().map(|(_, metadata)| {
            if dependency_descriptor_is_switch_point(metadata) {
                DependencyDescriptorLayerAvailability::DecoderUsable
            } else {
                DependencyDescriptorLayerAvailability::RtpSeen
            }
        });
        if let Ok(mut switch_points) = self.last_dd_switch_point_by_ssrc.lock() {
            match metadata {
                Some((_, ref metadata)) => {
                    switch_points.insert(ssrc, dependency_descriptor_is_switch_point(metadata));
                }
                None => {
                    switch_points.remove(&ssrc);
                }
            }
        }
        if let Ok(mut availability_by_ssrc) = self.last_dd_availability_by_ssrc.lock() {
            match availability {
                Some(availability) => {
                    availability_by_ssrc.insert(ssrc, availability);
                }
                None => {
                    availability_by_ssrc.remove(&ssrc);
                }
            }
        }
        if let Ok(mut metadata_by_ssrc) = self.last_dd_metadata_by_ssrc.lock() {
            match metadata.as_ref() {
                Some((source_extension_id, metadata)) => {
                    metadata_by_ssrc.insert(
                        ssrc,
                        DependencyDescriptorMetadataSnapshot::from_packet_metadata(
                            *source_extension_id,
                            metadata.clone(),
                        ),
                    );
                }
                None => {
                    metadata_by_ssrc.remove(&ssrc);
                }
            }
        }
        metadata.map(|(_, metadata)| metadata)
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

    async fn observe_temporal_layer_fps(&self, packet: &rtp::Packet) {
        let ssrc = packet.header.ssrc;
        let dd_metadata = self.state.observe_dependency_descriptor_packet(packet);

        let Some(codec_mime) = self.codec_mime_cached_for_ssrc(ssrc).await else {
            return;
        };
        let mime = codec_mime.to_ascii_lowercase();

        let (spatial_id, temporal_id) = if let Some(metadata) = dd_metadata {
            (
                metadata.layer_ids.spatial_id,
                Some(metadata.layer_ids.temporal_id),
            )
        } else if mime.contains("vp8") {
            (
                0,
                rtp::codec::vp8::temporal_layer_id_from_payload(&packet.payload),
            )
        } else if mime.contains("vp9") {
            if let Some(layer_ids) = rtp::codec::vp9::layer_ids_from_payload(&packet.payload) {
                (layer_ids.spatial_id, Some(layer_ids.temporal_id))
            } else {
                (
                    0,
                    rtp::codec::vp9::temporal_layer_id_from_payload(&packet.payload),
                )
            }
        } else if mime.contains("h265") {
            (
                0,
                rtp::codec::h265::temporal_layer_id_from_payload(&packet.payload),
            )
        } else {
            (0, None)
        };

        let Some(temporal_id) = temporal_id else {
            if let Ok(mut last_layers) = self.state.last_observed_layer_by_ssrc.lock() {
                last_layers.remove(&ssrc);
            }
            return;
        };

        if let Ok(mut last_layers) = self.state.last_observed_layer_by_ssrc.lock() {
            last_layers.insert(
                ssrc,
                ObservedLayerIds {
                    temporal_id,
                    spatial_id,
                },
            );
        }

        if let Ok(mut stats) = self.state.temporal_fps_by_ssrc_spatial.lock() {
            stats
                .entry((ssrc, spatial_id))
                .or_default()
                .observe(temporal_id, packet.header.timestamp);
        }
    }

    /// Returns estimated temporal-layer FPS values for a specific incoming SSRC.
    ///
    /// Values are `None` until enough packets have been observed for each layer.
    pub fn temporal_layer_fps_for_ssrc(&self, ssrc: u32) -> Option<[Option<f32>; 3]> {
        self.temporal_layer_fps_for_ssrc_and_spatial(ssrc, 0)
    }

    /// Returns estimated temporal-layer FPS values for a specific incoming SSRC and spatial layer.
    pub fn temporal_layer_fps_for_ssrc_and_spatial(
        &self,
        ssrc: u32,
        spatial_id: u8,
    ) -> Option<[Option<f32>; 3]> {
        self.state
            .temporal_fps_by_ssrc_spatial
            .lock()
            .ok()
            .and_then(|stats| {
                stats
                    .get(&(ssrc, spatial_id))
                    .or_else(|| stats.get(&(ssrc, 0)))
                    .map(TemporalLayerFpsEstimate::as_array)
            })
    }

    /// Returns the most recently observed spatial layer for an incoming SSRC.
    pub fn last_observed_spatial_layer_for_ssrc(&self, ssrc: u32) -> Option<u8> {
        self.state
            .last_observed_layer_by_ssrc
            .lock()
            .ok()
            .and_then(|layers| layers.get(&ssrc).map(|ids| ids.spatial_id))
    }

    /// Returns whether the latest packet for an SSRC is a verified dependency-descriptor switch
    /// point. `None` means descriptor metadata was unavailable for that packet.
    pub fn last_observed_dependency_descriptor_switch_point_for_ssrc(
        &self,
        ssrc: u32,
    ) -> Option<bool> {
        self.state
            .last_dd_switch_point_by_ssrc
            .lock()
            .ok()
            .and_then(|switch_points| switch_points.get(&ssrc).copied())
    }

    /// Returns dependency-descriptor availability for the latest packet on an incoming SSRC.
    ///
    /// `None` means the latest packet did not produce dependency-descriptor metadata.
    pub fn last_observed_dependency_descriptor_availability_for_ssrc(
        &self,
        ssrc: u32,
    ) -> Option<DependencyDescriptorLayerAvailability> {
        self.state
            .last_dd_availability_by_ssrc
            .lock()
            .ok()
            .and_then(|availability| availability.get(&ssrc).copied())
    }

    /// Returns the dependency-descriptor metadata for the latest packet on an incoming SSRC.
    ///
    /// `None` means the latest packet did not produce dependency-descriptor metadata.
    pub fn last_observed_dependency_descriptor_metadata_for_ssrc(
        &self,
        ssrc: u32,
    ) -> Option<DependencyDescriptorMetadataSnapshot> {
        self.state
            .last_dd_metadata_by_ssrc
            .lock()
            .ok()
            .and_then(|metadata| metadata.get(&ssrc).cloned())
    }

    /// Returns the most recently observed temporal layer for an incoming SSRC.
    pub fn last_observed_temporal_layer_for_ssrc(&self, ssrc: u32) -> Option<u8> {
        self.state
            .last_observed_layer_by_ssrc
            .lock()
            .ok()
            .and_then(|layers| layers.get(&ssrc).map(|ids| ids.temporal_id))
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
                    self.observe_temporal_layer_fps(&packet).await;
                    return Ok(RemoteTrackEvent::RtpPacket(packet));
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
                RemoteTrackEvent::RtpPacket(packet) => return Ok(packet),
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
        DependencyDescriptorLayerAvailability, RemoteTrackState, TemporalLayerFpsEstimate,
        dependency_descriptor_is_switch_point,
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
    fn remote_track_state_tracks_dependency_descriptor_switch_points_across_rtp_packets() {
        let state = RemoteTrackState::default();
        let ssrc = 0x1234_5678;

        let non_switch = descriptor_packet(ssrc, 100, descriptor_with_structure(1));
        assert!(
            state
                .observe_dependency_descriptor_packet(&non_switch)
                .is_some()
        );
        assert_eq!(
            state
                .last_dd_switch_point_by_ssrc
                .lock()
                .expect("dependency descriptor state lock should not be poisoned")
                .get(&ssrc),
            Some(&false),
            "a frame start without DTI Switch must not authorize a source switch"
        );
        assert_eq!(
            state
                .last_dd_availability_by_ssrc
                .lock()
                .expect("dependency descriptor availability lock should not be poisoned")
                .get(&ssrc),
            Some(&DependencyDescriptorLayerAvailability::RtpSeen),
            "descriptor RTP without a DTI Switch boundary is not decoder-usable"
        );

        let switch = descriptor_packet(ssrc, 101, descriptor_with_switch_target(2));
        assert!(
            state
                .observe_dependency_descriptor_packet(&switch)
                .is_some()
        );
        assert_eq!(
            state
                .last_dd_switch_point_by_ssrc
                .lock()
                .expect("dependency descriptor state lock should not be poisoned")
                .get(&ssrc),
            Some(&true),
            "a later frame start with an active DTI Switch target must authorize a source switch"
        );
        assert_eq!(
            state
                .last_dd_availability_by_ssrc
                .lock()
                .expect("dependency descriptor availability lock should not be poisoned")
                .get(&ssrc),
            Some(&DependencyDescriptorLayerAvailability::DecoderUsable)
        );
        let snapshot = state
            .last_dd_metadata_by_ssrc
            .lock()
            .expect("dependency descriptor metadata lock should not be poisoned")
            .get(&ssrc)
            .cloned()
            .expect("descriptor packet should retain a metadata snapshot");
        assert_eq!(snapshot.source_extension_id, 1);
        assert_eq!(snapshot.frame_number, 2);
        assert!(snapshot.first_packet_in_frame);
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
                .is_none()
        );
        assert!(
            !state
                .last_dd_switch_point_by_ssrc
                .lock()
                .expect("dependency descriptor state lock should not be poisoned")
                .contains_key(&ssrc),
            "a packet without descriptor metadata must not inherit a prior switch point"
        );
        assert!(
            !state
                .last_dd_availability_by_ssrc
                .lock()
                .expect("dependency descriptor availability lock should not be poisoned")
                .contains_key(&ssrc),
            "a packet without descriptor metadata must not inherit prior availability"
        );
        assert!(
            !state
                .last_dd_metadata_by_ssrc
                .lock()
                .expect("dependency descriptor metadata lock should not be poisoned")
                .contains_key(&ssrc),
            "a packet without descriptor metadata must not inherit prior metadata"
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

    /// Writes RTCP packets to this local forwarding track sender.
    pub async fn write_rtcp_packets(&self, packets: Vec<Box<dyn rtcp::Packet>>) -> RtcResult<()> {
        self.inner.write_rtcp(packets).await?;
        Ok(())
    }
}

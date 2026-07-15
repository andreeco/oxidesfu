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

#[derive(Debug, Default)]
struct RemoteTrackState {
    codec_mime_by_ssrc: Mutex<HashMap<u32, Option<String>>>,
    temporal_fps_by_ssrc_spatial: Mutex<HashMap<(u32, u8), TemporalLayerFpsEstimate>>,
    dd_parser_by_ssrc: Mutex<
        HashMap<u32, rtp::extension::dependency_descriptor_extension::DependencyDescriptorParser>,
    >,
    last_observed_layer_by_ssrc: Mutex<HashMap<u32, ObservedLayerIds>>,
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
        let Some(codec_mime) = self.codec_mime_cached_for_ssrc(ssrc).await else {
            return;
        };
        let mime = codec_mime.to_ascii_lowercase();

        let dd_layer_ids = if packet.header.extension {
            self.try_parse_dd_layer_ids(ssrc, packet)
        } else {
            None
        };

        let (spatial_id, temporal_id) = if let Some(ids) = dd_layer_ids {
            (ids.spatial_id, Some(ids.temporal_id))
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

    /// Returns the most recently observed temporal layer for an incoming SSRC.
    pub fn last_observed_temporal_layer_for_ssrc(&self, ssrc: u32) -> Option<u8> {
        self.state
            .last_observed_layer_by_ssrc
            .lock()
            .ok()
            .and_then(|layers| layers.get(&ssrc).map(|ids| ids.temporal_id))
    }

    fn try_parse_dd_layer_ids(
        &self,
        ssrc: u32,
        packet: &rtp::Packet,
    ) -> Option<rtp::extension::dependency_descriptor_extension::DependencyDescriptorLayerIds> {
        let mut parsers = self.state.dd_parser_by_ssrc.lock().ok()?;
        let parser = parsers.entry(ssrc).or_default();

        packet
            .header
            .extensions
            .iter()
            .find_map(|extension| parser.parse_layer_ids(&extension.payload))
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
mod tests {
    use super::TemporalLayerFpsEstimate;

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

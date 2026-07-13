use tokio::sync::mpsc;

use crate::{DataChannel, RemoteTrack};

/// OxideSFU-owned local ICE candidate event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IceCandidate {
    /// JSON-encoded `RTCIceCandidateInit` value used by LiveKit `TrickleRequest.candidate_init`.
    pub candidate_init_json: String,
    /// Whether this event marks end-of-candidates.
    pub is_final: bool,
}

/// Event stream emitted by an OxideSFU peer connection.
pub struct PeerConnectionEvents {
    /// Local ICE candidates gathered by the peer connection.
    pub ice_candidates: mpsc::UnboundedReceiver<IceCandidate>,
    /// Data channels opened by the remote peer.
    pub data_channels: mpsc::UnboundedReceiver<DataChannel>,
    /// Remote media tracks received from the remote peer.
    pub remote_tracks: mpsc::UnboundedReceiver<RemoteTrack>,
}

/// Event emitted by [`RemoteTrack`] polling.
pub enum RemoteTrackEvent {
    /// Received RTP packet.
    RtpPacket(rtc::rtp::Packet),
    /// Received RTCP packets.
    RtcpPacket(Vec<Box<dyn rtc::rtcp::Packet>>),
    /// Track ended.
    Ended,
}

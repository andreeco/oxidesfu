use std::sync::Arc;

use tokio::sync::mpsc;
use webrtc::data_channel::DataChannel as WebRtcDataChannel;
use webrtc::media_stream::track_remote::TrackRemote as WebRtcTrackRemote;
use webrtc::peer_connection::{PeerConnectionEventHandler, RTCPeerConnectionIceEvent};

use crate::{DataChannel, IceCandidate, RemoteTrack};

#[derive(Debug, Clone)]
pub(crate) struct NoopPeerConnectionHandler;

#[async_trait::async_trait]
impl PeerConnectionEventHandler for NoopPeerConnectionHandler {}

#[derive(Debug, Clone)]
pub(crate) struct EventPeerConnectionHandler {
    pub(crate) ice_candidate_tx: mpsc::UnboundedSender<IceCandidate>,
    pub(crate) data_channel_tx: mpsc::UnboundedSender<DataChannel>,
    pub(crate) remote_track_tx: mpsc::UnboundedSender<RemoteTrack>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for EventPeerConnectionHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        let Ok(candidate_init) = event.candidate.to_json() else {
            return;
        };
        if candidate_init.candidate.is_empty() {
            return;
        }
        let Ok(candidate_init_json) = serde_json::to_string(&candidate_init) else {
            return;
        };

        let _ = self.ice_candidate_tx.send(IceCandidate {
            candidate_init_json,
            is_final: false,
        });
    }

    async fn on_data_channel(&self, data_channel: Arc<dyn WebRtcDataChannel>) {
        let _ = self.data_channel_tx.send(DataChannel::new(data_channel));
    }

    async fn on_track(&self, track: Arc<dyn WebRtcTrackRemote>) {
        let _ = self.remote_track_tx.send(RemoteTrack::new(track));
    }
}

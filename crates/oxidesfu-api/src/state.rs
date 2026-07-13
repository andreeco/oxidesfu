use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use livekit_protocol as proto;
use oxidesfu_auth::TokenVerifier;
use oxidesfu_room::{RoomStore, RoomStoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomServiceMethod {
    DeleteRoom,
    UpdateRoomMetadata,
    ListParticipants,
    GetParticipant,
    RemoveParticipant,
    UpdateParticipant,
    MutePublishedTrack,
    UpdateSubscriptions,
    SendData,
    PerformRpc,
    CreateDispatch,
    DeleteDispatch,
    ListDispatch,
}

#[derive(Debug, Clone)]
pub enum ForwardedRoomServiceResponse {
    Protobuf(Vec<u8>),
    TwirpError {
        status: StatusCode,
        code: String,
        msg: String,
    },
}

#[async_trait]
pub trait RoomServiceForwarder: Send + Sync {
    async fn forward_if_non_local(
        &self,
        room: &str,
        method: RoomServiceMethod,
        request: Vec<u8>,
    ) -> Option<ForwardedRoomServiceResponse>;

    async fn list_rooms_cluster(&self, _request: Vec<u8>) -> Option<ForwardedRoomServiceResponse> {
        None
    }
}

#[async_trait]
pub trait MediaSubscriptionRuntime: Send + Sync {
    async fn apply_update_subscriptions(
        &self,
        room: &str,
        identity: &str,
        track_sids: &[String],
        participant_tracks: &[proto::ParticipantTracks],
        subscribe: bool,
    );

    async fn disconnect_participant(
        &self,
        _room: &str,
        _identity: &str,
        _reason: proto::DisconnectReason,
    ) -> Result<(), RoomStoreError> {
        Ok(())
    }

    async fn disconnect_room_participants(
        &self,
        _room: &str,
        _reason: proto::DisconnectReason,
    ) -> Result<(), RoomStoreError> {
        Ok(())
    }

    async fn broadcast_participant_update(
        &self,
        _room: &str,
        _participant: proto::ParticipantInfo,
    ) {
    }

    async fn apply_participant_update_from_service(
        &self,
        room: &str,
        _previous: Option<proto::ParticipantInfo>,
        participant: proto::ParticipantInfo,
    ) {
        self.broadcast_participant_update(room, participant).await;
    }

    async fn perform_rpc(
        &self,
        _room: &str,
        _request: &proto::PerformRpcRequest,
    ) -> Result<proto::PerformRpcResponse, RoomStoreError> {
        Err(RoomStoreError::InvalidArgument(
            "perform rpc is not implemented".to_string(),
        ))
    }

    async fn room_deleted(&self, _room: proto::Room) {}
}

/// Shared state for OxideSFU's Twirp API handlers.
#[derive(Clone)]
pub struct ApiState {
    pub rooms: RoomStore,
    pub auth: TokenVerifier,
    pub data_channels: oxidesfu_rtc::DataChannelStore,
    pub media_subscription_runtime: Option<Arc<dyn MediaSubscriptionRuntime>>,
    pub room_service_forwarder: Option<Arc<dyn RoomServiceForwarder>>,
    pub enable_remote_unmute: bool,
}

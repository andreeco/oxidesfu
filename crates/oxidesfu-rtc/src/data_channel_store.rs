use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{DataChannel, DataChannelKind, RtcResult};

type DataChannelKey = (String, String, DataChannelKind);

/// Shared registry of participant data channels keyed by room, identity, and reliability class.
#[derive(Clone, Default)]
pub struct DataChannelStore {
    channels: Arc<Mutex<HashMap<DataChannelKey, DataChannel>>>,
}

impl std::fmt::Debug for DataChannelStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataChannelStore").finish_non_exhaustive()
    }
}

impl DataChannelStore {
    /// Stores a reliable data channel for a room participant.
    pub fn insert(&self, room: &str, identity: &str, data_channel: DataChannel) {
        self.insert_with_kind(room, identity, DataChannelKind::Reliable, data_channel);
    }

    /// Stores a data channel for a room participant and reliability class.
    pub fn insert_with_kind(
        &self,
        room: &str,
        identity: &str,
        kind: DataChannelKind,
        data_channel: DataChannel,
    ) {
        if let Ok(mut channels) = self.channels.lock() {
            channels.insert((room.to_string(), identity.to_string(), kind), data_channel);
        }
    }

    /// Removes all stored data channels for a room participant.
    pub fn remove(&self, room: &str, identity: &str) -> Option<DataChannel> {
        self.channels.lock().ok().and_then(|mut channels| {
            let room = room.to_string();
            let identity = identity.to_string();
            let reliable =
                channels.remove(&(room.clone(), identity.clone(), DataChannelKind::Reliable));
            let lossy = channels.remove(&(room.clone(), identity.clone(), DataChannelKind::Lossy));
            let data_track = channels.remove(&(room, identity, DataChannelKind::DataTrack));
            reliable.or(lossy).or(data_track)
        })
    }

    /// Gets a reliable data channel for a room participant.
    pub fn get(&self, room: &str, identity: &str) -> Option<DataChannel> {
        self.get_with_kind(room, identity, DataChannelKind::Reliable)
    }

    /// Gets a data channel for a room participant and reliability class.
    pub fn get_with_kind(
        &self,
        room: &str,
        identity: &str,
        kind: DataChannelKind,
    ) -> Option<DataChannel> {
        self.channels.lock().ok().and_then(|channels| {
            channels
                .get(&(room.to_string(), identity.to_string(), kind))
                .cloned()
        })
    }

    /// Sends bytes to all reliable data channels in a room.
    pub async fn send_bytes_to_room(&self, room: &str, bytes: &[u8]) -> RtcResult<usize> {
        self.send_bytes_to_room_with_kind(room, DataChannelKind::Reliable, bytes)
            .await
    }

    /// Sends bytes to all data channels in a room matching the reliability class.
    pub async fn send_bytes_to_room_with_kind(
        &self,
        room: &str,
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        let channels = self.channels_for_room_with_kind(room, kind);
        let count = channels.len();
        for channel in channels {
            channel.send_bytes(bytes).await?;
        }
        Ok(count)
    }

    /// Sends bytes to all reliable data channels in a room except one identity.
    pub async fn send_bytes_to_room_except(
        &self,
        room: &str,
        excluded_identity: &str,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        self.send_bytes_to_room_except_with_kind(
            room,
            excluded_identity,
            DataChannelKind::Reliable,
            bytes,
        )
        .await
    }

    /// Sends bytes to data channels matching the reliability class in a room except one identity.
    pub async fn send_bytes_to_room_except_with_kind(
        &self,
        room: &str,
        excluded_identity: &str,
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        let channels = self.channels_for_room_except_with_kind(room, kind, Some(excluded_identity));
        let count = channels.len();
        for channel in channels {
            channel.send_bytes(bytes).await?;
        }
        Ok(count)
    }

    /// Sends bytes to reliable data channels matching identities in a room.
    pub async fn send_bytes_to_identities(
        &self,
        room: &str,
        identities: &[String],
        bytes: &[u8],
    ) -> RtcResult<usize> {
        self.send_bytes_to_identities_with_kind(room, identities, DataChannelKind::Reliable, bytes)
            .await
    }

    /// Sends bytes to data channels matching identities and reliability class in a room.
    pub async fn send_bytes_to_identities_with_kind(
        &self,
        room: &str,
        identities: &[String],
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        if identities.is_empty() {
            return self.send_bytes_to_room_with_kind(room, kind, bytes).await;
        }
        let channels = identities
            .iter()
            .filter_map(|identity| self.get_with_kind(room, identity, kind))
            .collect::<Vec<_>>();
        let count = channels.len();
        for channel in channels {
            channel.send_bytes(bytes).await?;
        }
        Ok(count)
    }

    fn channels_for_room_with_kind(&self, room: &str, kind: DataChannelKind) -> Vec<DataChannel> {
        self.channels_for_room_except_with_kind(room, kind, None)
    }

    fn channels_for_room_except_with_kind(
        &self,
        room: &str,
        kind: DataChannelKind,
        excluded_identity: Option<&str>,
    ) -> Vec<DataChannel> {
        self.channels
            .lock()
            .map(|channels| {
                channels
                    .iter()
                    .filter(|((channel_room, identity, channel_kind), _)| {
                        channel_room == room
                            && *channel_kind == kind
                            && excluded_identity.is_none_or(|excluded| identity != excluded)
                    })
                    .map(|(_, channel)| channel.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
}

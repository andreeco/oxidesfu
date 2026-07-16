use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{DataChannel, DataChannelKind, RtcResult};

/// Transport owning a participant data channel.
///
/// This mirrors the publisher/subscriber signaling target without making the
/// RTC crate depend on the signaling crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataChannelTransportTarget {
    /// The peer connection used to publish media and application data.
    Publisher,
    /// The peer connection used to receive subscribed media.
    Subscriber,
}

type DataChannelKey = (String, String, DataChannelTransportTarget, DataChannelKind);

/// Shared registry of participant data channels keyed by room, identity, transport, and reliability class.
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
    const LEGACY_TARGET: DataChannelTransportTarget = DataChannelTransportTarget::Publisher;

    /// Stores a reliable data channel for a room participant on the publisher transport.
    pub fn insert(&self, room: &str, identity: &str, data_channel: DataChannel) {
        self.insert_for_target(room, identity, Self::LEGACY_TARGET, data_channel);
    }

    /// Stores a reliable data channel for a room participant on a transport target.
    pub fn insert_for_target(
        &self,
        room: &str,
        identity: &str,
        target: DataChannelTransportTarget,
        data_channel: DataChannel,
    ) {
        self.insert_with_kind_for_target(
            room,
            identity,
            target,
            DataChannelKind::Reliable,
            data_channel,
        );
    }

    /// Stores a data channel for a room participant and reliability class on the publisher transport.
    pub fn insert_with_kind(
        &self,
        room: &str,
        identity: &str,
        kind: DataChannelKind,
        data_channel: DataChannel,
    ) {
        self.insert_with_kind_for_target(room, identity, Self::LEGACY_TARGET, kind, data_channel);
    }

    /// Stores a data channel for a room participant, transport target, and reliability class.
    pub fn insert_with_kind_for_target(
        &self,
        room: &str,
        identity: &str,
        target: DataChannelTransportTarget,
        kind: DataChannelKind,
        data_channel: DataChannel,
    ) {
        if let Ok(mut channels) = self.channels.lock() {
            channels.insert(
                (room.to_string(), identity.to_string(), target, kind),
                data_channel,
            );
        }
    }

    /// Removes all stored data channels for a room participant on both transport targets.
    pub fn remove(&self, room: &str, identity: &str) -> Option<DataChannel> {
        let publisher =
            self.remove_for_target(room, identity, DataChannelTransportTarget::Publisher);
        let subscriber =
            self.remove_for_target(room, identity, DataChannelTransportTarget::Subscriber);
        publisher.or(subscriber)
    }

    /// Removes all stored data channels for a room participant on one transport target.
    pub fn remove_for_target(
        &self,
        room: &str,
        identity: &str,
        target: DataChannelTransportTarget,
    ) -> Option<DataChannel> {
        self.channels.lock().ok().and_then(|mut channels| {
            let room = room.to_string();
            let identity = identity.to_string();
            let reliable = channels.remove(&(
                room.clone(),
                identity.clone(),
                target,
                DataChannelKind::Reliable,
            ));
            let lossy = channels.remove(&(
                room.clone(),
                identity.clone(),
                target,
                DataChannelKind::Lossy,
            ));
            let data_track = channels.remove(&(room, identity, target, DataChannelKind::DataTrack));
            reliable.or(lossy).or(data_track)
        })
    }

    /// Gets a reliable data channel for a room participant on the publisher transport.
    pub fn get(&self, room: &str, identity: &str) -> Option<DataChannel> {
        self.get_for_target(room, identity, Self::LEGACY_TARGET)
    }

    /// Gets a reliable data channel for a room participant on a transport target.
    pub fn get_for_target(
        &self,
        room: &str,
        identity: &str,
        target: DataChannelTransportTarget,
    ) -> Option<DataChannel> {
        self.get_with_kind_for_target(room, identity, target, DataChannelKind::Reliable)
    }

    /// Gets a data channel for a room participant and reliability class on the publisher transport.
    pub fn get_with_kind(
        &self,
        room: &str,
        identity: &str,
        kind: DataChannelKind,
    ) -> Option<DataChannel> {
        self.get_with_kind_for_target(room, identity, Self::LEGACY_TARGET, kind)
    }

    /// Gets the downstream data channel, preferring the subscriber transport.
    ///
    /// Legacy subscriber-primary sessions own downstream writers on the subscriber
    /// peer connection. Falling back to the publisher target retains single-PC and
    /// publisher-primary behavior when no subscriber channel exists.
    pub fn get_with_kind_for_downstream(
        &self,
        room: &str,
        identity: &str,
        kind: DataChannelKind,
    ) -> Option<DataChannel> {
        self.get_with_kind_for_target(room, identity, DataChannelTransportTarget::Subscriber, kind)
            .or_else(|| {
                self.get_with_kind_for_target(
                    room,
                    identity,
                    DataChannelTransportTarget::Publisher,
                    kind,
                )
            })
    }

    /// Gets a data channel for a room participant, transport target, and reliability class.
    pub fn get_with_kind_for_target(
        &self,
        room: &str,
        identity: &str,
        target: DataChannelTransportTarget,
        kind: DataChannelKind,
    ) -> Option<DataChannel> {
        self.channels.lock().ok().and_then(|channels| {
            channels
                .get(&(room.to_string(), identity.to_string(), target, kind))
                .cloned()
        })
    }

    /// Sends bytes to all reliable data channels in a room on the publisher transport.
    pub async fn send_bytes_to_room(&self, room: &str, bytes: &[u8]) -> RtcResult<usize> {
        self.send_bytes_to_room_with_kind(room, DataChannelKind::Reliable, bytes)
            .await
    }

    /// Sends bytes to all data channels in a room matching the reliability class on the publisher transport.
    pub async fn send_bytes_to_room_with_kind(
        &self,
        room: &str,
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        self.send_bytes_to_room_with_kind_for_target(room, Self::LEGACY_TARGET, kind, bytes)
            .await
    }

    /// Sends bytes to all data channels in a room matching one transport target and reliability class.
    pub async fn send_bytes_to_room_with_kind_for_target(
        &self,
        room: &str,
        target: DataChannelTransportTarget,
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        let channels = self.channels_for_room_with_kind_for_target(room, target, kind);
        let count = channels.len();
        for channel in channels {
            channel.send_bytes(bytes).await?;
        }
        Ok(count)
    }

    /// Sends bytes to all reliable data channels in a room except one identity on the publisher transport.
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

    /// Sends bytes to data channels matching the reliability class in a room except one identity on the publisher transport.
    pub async fn send_bytes_to_room_except_with_kind(
        &self,
        room: &str,
        excluded_identity: &str,
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        self.send_bytes_to_room_except_with_kind_for_target(
            room,
            excluded_identity,
            Self::LEGACY_TARGET,
            kind,
            bytes,
        )
        .await
    }

    /// Sends bytes to data channels matching one transport target and reliability class in a room except one identity.
    pub async fn send_bytes_to_room_except_with_kind_for_target(
        &self,
        room: &str,
        excluded_identity: &str,
        target: DataChannelTransportTarget,
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        let channels = self.channels_for_room_except_with_kind_for_target(
            room,
            target,
            kind,
            Some(excluded_identity),
        );
        let count = channels.len();
        for channel in channels {
            channel.send_bytes(bytes).await?;
        }
        Ok(count)
    }

    /// Sends bytes to reliable data channels matching identities in a room on the publisher transport.
    pub async fn send_bytes_to_identities(
        &self,
        room: &str,
        identities: &[String],
        bytes: &[u8],
    ) -> RtcResult<usize> {
        self.send_bytes_to_identities_with_kind(room, identities, DataChannelKind::Reliable, bytes)
            .await
    }

    /// Sends bytes to data channels matching identities and reliability class in a room on the publisher transport.
    pub async fn send_bytes_to_identities_with_kind(
        &self,
        room: &str,
        identities: &[String],
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        self.send_bytes_to_identities_with_kind_for_target(
            room,
            identities,
            Self::LEGACY_TARGET,
            kind,
            bytes,
        )
        .await
    }

    /// Sends bytes to data channels matching identities, one transport target, and reliability class in a room.
    pub async fn send_bytes_to_identities_with_kind_for_target(
        &self,
        room: &str,
        identities: &[String],
        target: DataChannelTransportTarget,
        kind: DataChannelKind,
        bytes: &[u8],
    ) -> RtcResult<usize> {
        if identities.is_empty() {
            return self
                .send_bytes_to_room_with_kind_for_target(room, target, kind, bytes)
                .await;
        }
        let channels = identities
            .iter()
            .filter_map(|identity| self.get_with_kind_for_target(room, identity, target, kind))
            .collect::<Vec<_>>();
        let count = channels.len();
        for channel in channels {
            channel.send_bytes(bytes).await?;
        }
        Ok(count)
    }

    fn channels_for_room_with_kind_for_target(
        &self,
        room: &str,
        target: DataChannelTransportTarget,
        kind: DataChannelKind,
    ) -> Vec<DataChannel> {
        self.channels_for_room_except_with_kind_for_target(room, target, kind, None)
    }

    fn channels_for_room_except_with_kind_for_target(
        &self,
        room: &str,
        target: DataChannelTransportTarget,
        kind: DataChannelKind,
        excluded_identity: Option<&str>,
    ) -> Vec<DataChannel> {
        self.channels
            .lock()
            .map(|channels| {
                channels
                    .iter()
                    .filter(
                        |((channel_room, identity, channel_target, channel_kind), _)| {
                            channel_room == room
                                && *channel_target == target
                                && *channel_kind == kind
                                && excluded_identity.is_none_or(|excluded| identity != excluded)
                        },
                    )
                    .map(|(_, channel)| channel.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::{DataChannelKind, DataChannelStore, DataChannelTransportTarget};

    #[tokio::test]
    async fn stores_same_participant_kind_channels_on_distinct_transport_targets() {
        let publisher = crate::create_peer_connection()
            .await
            .expect("publisher peer connection should create");
        let subscriber = crate::create_peer_connection()
            .await
            .expect("subscriber peer connection should create");
        let publisher_channel = publisher
            .create_data_channel("publisher-reliable")
            .await
            .expect("publisher data channel should create");
        let subscriber_channel = subscriber
            .create_data_channel("subscriber-reliable")
            .await
            .expect("subscriber data channel should create");
        let store = DataChannelStore::default();

        store.insert_with_kind_for_target(
            "room",
            "alice",
            DataChannelTransportTarget::Publisher,
            DataChannelKind::Reliable,
            publisher_channel,
        );
        store.insert_with_kind_for_target(
            "room",
            "alice",
            DataChannelTransportTarget::Subscriber,
            DataChannelKind::Reliable,
            subscriber_channel,
        );

        let publisher_label = store
            .get_with_kind_for_target(
                "room",
                "alice",
                DataChannelTransportTarget::Publisher,
                DataChannelKind::Reliable,
            )
            .expect("publisher channel should remain stored")
            .label()
            .await
            .expect("publisher label should read");
        let subscriber_label = store
            .get_with_kind_for_target(
                "room",
                "alice",
                DataChannelTransportTarget::Subscriber,
                DataChannelKind::Reliable,
            )
            .expect("subscriber channel should remain stored")
            .label()
            .await
            .expect("subscriber label should read");

        assert_eq!(publisher_label, "publisher-reliable");
        assert_eq!(subscriber_label, "subscriber-reliable");
    }
}

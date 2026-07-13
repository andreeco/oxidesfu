use std::collections::HashMap;

use livekit_protocol as proto;

use crate::{RoomStore, RoomStoreError, store::RoomRecord};

fn participant_is_hidden(participant: &proto::ParticipantInfo) -> bool {
    participant
        .permission
        .as_ref()
        .map(|permission| permission.hidden)
        .unwrap_or(false)
}

const MAX_PARTICIPANT_METADATA_BYTES: usize = 512 * 1024;
const MAX_PARTICIPANT_ATTRIBUTES_BYTES: usize = 64 * 1024;

fn visible_participant_count(record: &RoomRecord) -> u32 {
    record
        .participants
        .values()
        .filter(|participant| !participant_is_hidden(participant))
        .count()
        .min(u32::MAX as usize) as u32
}

impl RoomStore {
    /// Joins a participant to an existing room, or creates the room first when missing.
    pub fn join_participant(
        &self,
        room_name: &str,
        identity: &str,
        name: &str,
        metadata: String,
        attributes: HashMap<String, String>,
    ) -> Result<
        (
            proto::Room,
            proto::ParticipantInfo,
            Vec<proto::ParticipantInfo>,
        ),
        RoomStoreError,
    > {
        self.join_participant_with_permission(room_name, identity, name, metadata, attributes, None)
    }

    /// Joins a participant with explicit participant permissions in its snapshot.
    pub fn join_participant_with_permission(
        &self,
        room_name: &str,
        identity: &str,
        name: &str,
        metadata: String,
        attributes: HashMap<String, String>,
        permission: Option<proto::ParticipantPermission>,
    ) -> Result<
        (
            proto::Room,
            proto::ParticipantInfo,
            Vec<proto::ParticipantInfo>,
        ),
        RoomStoreError,
    > {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        if !inner.rooms.contains_key(room_name) {
            inner.next_room_id = inner.next_room_id.saturating_add(1);
            let now_ms = crate::store::now_unix_ms();
            let room = proto::Room {
                sid: format!("RM_{:016x}", inner.next_room_id),
                name: room_name.to_string(),
                empty_timeout: 300,
                departure_timeout: 20,
                creation_time: now_ms / 1000,
                creation_time_ms: now_ms,
                ..Default::default()
            };
            inner.rooms.insert(
                room.name.clone(),
                RoomRecord {
                    room,
                    room_internal: None,
                    participants: HashMap::new(),
                    participant_versions: HashMap::new(),
                    agent_dispatches: Vec::new(),
                    empty_since_unix_ms: Some(now_ms),
                    had_participants: false,
                },
            );
        }

        let existing_record = inner
            .rooms
            .get(room_name)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let replacing_existing_identity = existing_record.participants.contains_key(identity);
        if existing_record.room.max_participants > 0
            && !replacing_existing_identity
            && existing_record.participants.len() >= existing_record.room.max_participants as usize
        {
            return Err(RoomStoreError::MaxParticipantsExceeded);
        }
        let existing = existing_record
            .participants
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let version = existing_record
            .participant_versions
            .get(identity)
            .copied()
            .map(|version| version.saturating_add(1))
            .unwrap_or_default();
        inner.next_participant_id = inner.next_participant_id.saturating_add(1);
        let participant = proto::ParticipantInfo {
            sid: format!("PA_{:016x}", inner.next_participant_id),
            version,
            identity: identity.to_string(),
            name: name.to_string(),
            metadata,
            attributes,
            permission,
            state: proto::participant_info::State::Joined as i32,
            joined_at: crate::store::now_unix_ms() / 1000,
            ..Default::default()
        };

        let record = inner
            .rooms
            .get_mut(room_name)
            .ok_or(RoomStoreError::RoomNotFound)?;
        record
            .participants
            .insert(identity.to_string(), participant.clone());
        record
            .participant_versions
            .insert(identity.to_string(), participant.version);
        record.had_participants = true;
        record.room.num_participants = visible_participant_count(record);
        record.empty_since_unix_ms = None;

        Ok((record.room.clone(), participant, existing))
    }

    /// Removes a participant after a client-initiated leave and returns a disconnected snapshot.
    pub fn remove_participant(
        &self,
        room: &str,
        identity: &str,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        self.remove_participant_with_reason(
            room,
            identity,
            proto::DisconnectReason::ClientInitiated,
        )
    }

    /// Removes a participant and returns a disconnected snapshot with the provided reason.
    pub fn remove_participant_with_reason(
        &self,
        room: &str,
        identity: &str,
        reason: proto::DisconnectReason,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let mut participant = record
            .participants
            .remove(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;
        participant.state = proto::participant_info::State::Disconnected as i32;
        participant.disconnect_reason = reason as i32;
        participant.version = participant.version.saturating_add(1);
        record
            .participant_versions
            .insert(identity.to_string(), participant.version);
        record.room.num_participants = visible_participant_count(record);
        if record.participants.is_empty() {
            record.empty_since_unix_ms = Some(crate::store::now_unix_ms());
        }

        let unsubscribed_before = inner.media_unsubscribed.len();
        inner.media_unsubscribed.retain(
            |(candidate_room, publisher_identity, _track_sid, subscriber_identity)| {
                candidate_room != room
                    || (publisher_identity != identity && subscriber_identity != identity)
            },
        );
        if inner.media_unsubscribed.len() != unsubscribed_before {
            inner.media_subscription_revision = inner.media_subscription_revision.saturating_add(1);
        }

        Ok(participant)
    }

    /// Sets participant kind fields and returns the updated participant snapshot.
    pub fn set_participant_kind(
        &self,
        room: &str,
        identity: &str,
        kind: i32,
        kind_details: Vec<i32>,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let participant = record
            .participants
            .get_mut(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;
        participant.kind = kind;
        participant.kind_details = kind_details;
        participant.version = participant.version.saturating_add(1);
        Ok(participant.clone())
    }

    /// Adds a published media track to a participant and returns the updated participant snapshot.
    pub fn add_participant_track(
        &self,
        room: &str,
        identity: &str,
        track: proto::TrackInfo,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let participant = record
            .participants
            .get_mut(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;
        participant.tracks.push(track);
        participant.version = participant.version.saturating_add(1);
        Ok(participant.clone())
    }

    /// Updates the MIME type for a published media track and returns the updated participant snapshot.
    pub fn set_participant_track_mime_type(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
        mime_type: &str,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let participant = record
            .participants
            .get_mut(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;
        let Some(track) = participant
            .tracks
            .iter_mut()
            .find(|track| track.sid == track_sid)
        else {
            return Err(RoomStoreError::ParticipantNotFound);
        };
        let normalized_mime = mime_type.trim().to_ascii_lowercase();
        if normalized_mime.is_empty() || track.mime_type == normalized_mime {
            return Ok(participant.clone());
        }
        track.mime_type = normalized_mime.clone();
        track.codecs = vec![proto::SimulcastCodecInfo {
            mime_type: normalized_mime,
            ..Default::default()
        }];
        participant.version = participant.version.saturating_add(1);
        Ok(participant.clone())
    }

    /// Updates the MID for a published media track and returns the updated participant snapshot.
    pub fn set_participant_track_mid(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
        mid: &str,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let participant = record
            .participants
            .get_mut(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;
        let Some(track) = participant
            .tracks
            .iter_mut()
            .find(|track| track.sid == track_sid)
        else {
            return Err(RoomStoreError::ParticipantNotFound);
        };
        if track.mid == mid {
            return Ok(participant.clone());
        }
        track.mid = mid.to_string();
        participant.version = participant.version.saturating_add(1);
        Ok(participant.clone())
    }

    /// Sets media subscription preference for a subscriber and published track SID.
    ///
    /// Returns `true` when at least one matching published media track SID was found.
    pub fn set_media_track_subscribed_by_track_sid(
        &self,
        room: &str,
        subscriber_identity: &str,
        track_sid: &str,
        subscribed: bool,
    ) -> Result<bool, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner.rooms.get(room).ok_or(RoomStoreError::RoomNotFound)?;

        if !record.participants.contains_key(subscriber_identity) {
            return Err(RoomStoreError::ParticipantNotFound);
        }

        let publishers = record
            .participants
            .iter()
            .filter(|(_, participant)| {
                participant
                    .tracks
                    .iter()
                    .any(|track| track.sid == track_sid)
            })
            .map(|(publisher_identity, _)| publisher_identity.clone())
            .collect::<Vec<_>>();

        if publishers.is_empty() {
            return Ok(false);
        }

        let mut changed = false;
        for publisher_identity in publishers {
            let key = (
                room.to_string(),
                publisher_identity,
                track_sid.to_string(),
                subscriber_identity.to_string(),
            );
            if subscribed {
                changed |= inner.media_unsubscribed.remove(&key);
            } else {
                changed |= inner.media_unsubscribed.insert(key);
            }
        }
        if changed {
            inner.media_subscription_revision = inner.media_subscription_revision.saturating_add(1);
        }

        Ok(true)
    }

    /// Sets media subscription preference for a subscriber and publisher participant SID.
    pub fn set_media_track_subscribed_by_publisher_sid(
        &self,
        room: &str,
        publisher_sid: &str,
        track_sid: &str,
        subscriber_identity: &str,
        subscribed: bool,
    ) -> Result<bool, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner.rooms.get(room).ok_or(RoomStoreError::RoomNotFound)?;

        if !record.participants.contains_key(subscriber_identity) {
            return Err(RoomStoreError::ParticipantNotFound);
        }

        let publisher_identity = record
            .participants
            .iter()
            .find_map(|(identity, participant)| {
                (participant.sid == publisher_sid).then(|| identity.clone())
            });

        let Some(publisher_identity) = publisher_identity else {
            return Ok(false);
        };

        let key = (
            room.to_string(),
            publisher_identity,
            track_sid.to_string(),
            subscriber_identity.to_string(),
        );
        let changed = if subscribed {
            inner.media_unsubscribed.remove(&key)
        } else {
            inner.media_unsubscribed.insert(key)
        };
        if changed {
            inner.media_subscription_revision = inner.media_subscription_revision.saturating_add(1);
        }

        Ok(true)
    }

    /// Sets media subscription preference for a subscriber and a concrete publisher/track tuple.
    pub fn set_media_track_subscribed(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
        subscribed: bool,
    ) -> Result<(), RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let Some(_record) = inner.rooms.get(room) else {
            return Err(RoomStoreError::RoomNotFound);
        };

        let key = (
            room.to_string(),
            publisher_identity.to_string(),
            track_sid.to_string(),
            subscriber_identity.to_string(),
        );
        let changed = if subscribed {
            inner.media_unsubscribed.remove(&key)
        } else {
            inner.media_unsubscribed.insert(key)
        };
        if changed {
            inner.media_subscription_revision = inner.media_subscription_revision.saturating_add(1);
        }

        Ok(())
    }

    /// Returns whether media should be forwarded for this publisher/track/subscriber tuple.
    pub fn is_media_track_subscribed(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> bool {
        self.inner
            .read()
            .map(|inner| {
                !inner.media_unsubscribed.contains(&(
                    room.to_string(),
                    publisher_identity.to_string(),
                    track_sid.to_string(),
                    subscriber_identity.to_string(),
                ))
            })
            .unwrap_or(true)
    }

    /// Returns a revision counter for media subscription/permission forwarding policy.
    pub fn media_subscription_revision(&self) -> u64 {
        self.inner
            .read()
            .map(|inner| inner.media_subscription_revision)
            .unwrap_or_default()
    }

    /// Returns whether a subscriber may receive media for this publisher/track tuple.
    ///
    /// This combines the participant permission check and media subscription check under one
    /// read lock to avoid cloning participant state in high-rate forwarding paths.
    pub fn can_subscribe_to_media_track(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> bool {
        self.inner
            .read()
            .map(|inner| {
                let Some(record) = inner.rooms.get(room) else {
                    return false;
                };
                let can_subscribe = record
                    .participants
                    .get(subscriber_identity)
                    .and_then(|participant| participant.permission.as_ref())
                    .map(|permission| permission.can_subscribe)
                    .unwrap_or(true);
                can_subscribe
                    && !inner.media_unsubscribed.contains(&(
                        room.to_string(),
                        publisher_identity.to_string(),
                        track_sid.to_string(),
                        subscriber_identity.to_string(),
                    ))
            })
            .unwrap_or(true)
    }

    /// Sets muted state for a published media track and returns the updated track snapshot.
    pub fn set_participant_track_muted(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
        muted: bool,
    ) -> Result<proto::TrackInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let participant = record
            .participants
            .get_mut(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;

        let Some(track) = participant
            .tracks
            .iter_mut()
            .find(|track| track.sid == track_sid)
        else {
            return Err(RoomStoreError::ParticipantNotFound);
        };

        if track.muted != muted {
            track.muted = muted;
            participant.version = participant.version.saturating_add(1);
        }

        Ok(track.clone())
    }

    /// Removes a published media track from a participant and returns the updated participant snapshot.
    pub fn remove_participant_track(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let updated_participant = {
            let participant = record
                .participants
                .get_mut(identity)
                .ok_or(RoomStoreError::ParticipantNotFound)?;
            participant.tracks.retain(|track| track.sid != track_sid);
            participant.version = participant.version.saturating_add(1);
            participant.clone()
        };

        let unsubscribed_before = inner.media_unsubscribed.len();
        inner.media_unsubscribed.retain(
            |(candidate_room, candidate_publisher, candidate_track_sid, _subscriber_identity)| {
                candidate_room != room
                    || candidate_publisher != identity
                    || candidate_track_sid != track_sid
            },
        );
        if inner.media_unsubscribed.len() != unsubscribed_before {
            inner.media_subscription_revision = inner.media_subscription_revision.saturating_add(1);
        }

        Ok(updated_participant)
    }

    /// Adds a published data track to a participant and returns the updated participant snapshot.
    pub fn add_participant_data_track(
        &self,
        room: &str,
        identity: &str,
        data_track: proto::DataTrackInfo,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let participant = record
            .participants
            .get_mut(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;
        participant.data_tracks.push(data_track);
        participant.version = participant.version.saturating_add(1);
        Ok(participant.clone())
    }

    /// Removes a published data track from a participant and returns the updated participant snapshot.
    pub fn remove_participant_data_track(
        &self,
        room: &str,
        identity: &str,
        pub_handle: u32,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let participant = record
            .participants
            .get_mut(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;
        participant
            .data_tracks
            .retain(|track| track.pub_handle != pub_handle);
        participant.version = participant.version.saturating_add(1);
        Ok(participant.clone())
    }

    /// Updates participant metadata, name, permission, and attributes.
    pub fn update_participant(
        &self,
        room: &str,
        identity: &str,
        metadata: &str,
        name: &str,
        permission: Option<proto::ParticipantPermission>,
        attributes: HashMap<String, String>,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        if metadata.len() > MAX_PARTICIPANT_METADATA_BYTES {
            return Err(RoomStoreError::InvalidArgument(
                "metadata exceeds 512KiB limit".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let participant = record
            .participants
            .get_mut(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;

        let mut next_attributes = participant.attributes.clone();
        for (key, value) in &attributes {
            if value.is_empty() {
                next_attributes.remove(key);
            } else {
                next_attributes.insert(key.clone(), value.clone());
            }
        }
        let next_attributes_size: usize = next_attributes
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        if next_attributes_size > MAX_PARTICIPANT_ATTRIBUTES_BYTES {
            return Err(RoomStoreError::InvalidArgument(
                "attributes exceed 64KiB limit".to_string(),
            ));
        }

        let mut changed = false;
        let mut visibility_changed = false;
        let mut media_permission_changed = false;

        if !metadata.is_empty() && participant.metadata != metadata {
            participant.metadata = metadata.to_string();
            changed = true;
        }

        if !name.is_empty() && participant.name != name {
            participant.name = name.to_string();
            changed = true;
        }

        if let Some(permission) = permission
            && participant.permission.as_ref() != Some(&permission)
        {
            if !permission.can_publish {
                participant.tracks.clear();
                participant.data_tracks.clear();
            }
            let was_hidden = participant_is_hidden(participant);
            participant.permission = Some(permission);
            let is_hidden = participant_is_hidden(participant);
            visibility_changed = was_hidden != is_hidden;
            media_permission_changed = true;
            changed = true;
        }

        for (key, value) in attributes {
            if value.is_empty() {
                if participant.attributes.remove(&key).is_some() {
                    changed = true;
                }
                continue;
            }

            match participant.attributes.insert(key, value.clone()) {
                Some(existing) if existing == value => {}
                _ => changed = true,
            }
        }

        if changed {
            participant.version = participant.version.saturating_add(1);
        }

        let updated = participant.clone();
        if visibility_changed {
            record.room.num_participants = visible_participant_count(record);
        }
        if media_permission_changed {
            inner.media_subscription_revision = inner.media_subscription_revision.saturating_add(1);
        }

        Ok(updated)
    }

    /// Validates that a participant can be forwarded to a destination room.
    pub fn forward_participant(
        &self,
        room: &str,
        identity: &str,
        destination_room: &str,
    ) -> Result<(), RoomStoreError> {
        self.get_participant(room, identity)?;
        self.ensure_room_exists(destination_room)?;
        Ok(())
    }

    /// Moves a participant between rooms and returns the moved participant snapshot.
    pub fn move_participant(
        &self,
        room: &str,
        identity: &str,
        destination_room: &str,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        if !inner.rooms.contains_key(destination_room) {
            return Err(RoomStoreError::RoomNotFound);
        }

        let source = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        let mut participant = source
            .participants
            .remove(identity)
            .ok_or(RoomStoreError::ParticipantNotFound)?;
        source.room.num_participants = visible_participant_count(source);
        if source.participants.is_empty() {
            source.empty_since_unix_ms = Some(crate::store::now_unix_ms());
        }

        participant.version = participant.version.saturating_add(1);
        let destination = inner
            .rooms
            .get_mut(destination_room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        destination
            .participants
            .insert(identity.to_string(), participant.clone());
        destination.room.num_participants = visible_participant_count(destination);
        destination.empty_since_unix_ms = None;

        let unsubscribed_before = inner.media_unsubscribed.len();
        inner.media_unsubscribed.retain(
            |(candidate_room, publisher_identity, _track_sid, subscriber_identity)| {
                candidate_room != room
                    || (publisher_identity != identity && subscriber_identity != identity)
            },
        );
        if inner.media_unsubscribed.len() != unsubscribed_before {
            inner.media_subscription_revision = inner.media_subscription_revision.saturating_add(1);
        }

        Ok(participant)
    }

    /// Lists participants for a room.
    pub fn list_participants(
        &self,
        room: &str,
    ) -> Result<Vec<proto::ParticipantInfo>, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner.rooms.get(room).ok_or(RoomStoreError::RoomNotFound)?;
        Ok(record.participants.values().cloned().collect())
    }

    /// Loads a participant by identity.
    pub fn get_participant(
        &self,
        room: &str,
        identity: &str,
    ) -> Result<proto::ParticipantInfo, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner.rooms.get(room).ok_or(RoomStoreError::RoomNotFound)?;
        record
            .participants
            .get(identity)
            .cloned()
            .ok_or(RoomStoreError::ParticipantNotFound)
    }
}

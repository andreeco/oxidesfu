use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use livekit_protocol as proto;

use crate::metrics::current_unix_millis;

type DataTrackKey = (String, String);
type DataTrackMap = HashMap<DataTrackKey, Vec<proto::DataTrackInfo>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct DataTrackStore {
    tracks: Arc<Mutex<DataTrackMap>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataTrackPublishError {
    DuplicateHandle,
    DuplicateName,
}

impl DataTrackStore {
    pub(crate) fn publish(
        &self,
        room: &str,
        identity: &str,
        request: &proto::PublishDataTrackRequest,
    ) -> Result<proto::DataTrackInfo, DataTrackPublishError> {
        let mut tracks = self
            .tracks
            .lock()
            .map_err(|_| DataTrackPublishError::DuplicateHandle)?;
        let participant_tracks = tracks
            .entry((room.to_string(), identity.to_string()))
            .or_default();

        if participant_tracks
            .iter()
            .any(|track| track.pub_handle == request.pub_handle)
        {
            return Err(DataTrackPublishError::DuplicateHandle);
        }
        if participant_tracks
            .iter()
            .any(|track| track.name == request.name)
        {
            return Err(DataTrackPublishError::DuplicateName);
        }

        let info = proto::DataTrackInfo {
            pub_handle: request.pub_handle,
            sid: format!(
                "DTR_{:016x}{:04x}",
                current_unix_millis(),
                request.pub_handle
            ),
            name: request.name.clone(),
            encryption: request.encryption,
        };
        participant_tracks.push(info.clone());
        Ok(info)
    }

    pub(crate) fn unpublish(
        &self,
        room: &str,
        identity: &str,
        pub_handle: u32,
    ) -> Option<proto::DataTrackInfo> {
        let mut tracks = self.tracks.lock().ok()?;
        let participant_tracks = tracks.get_mut(&(room.to_string(), identity.to_string()))?;
        let index = participant_tracks
            .iter()
            .position(|track| track.pub_handle == pub_handle)?;
        Some(participant_tracks.remove(index))
    }

    pub(crate) fn find_by_sid(
        &self,
        room: &str,
        track_sid: &str,
    ) -> Option<(String, proto::DataTrackInfo)> {
        let tracks = self.tracks.lock().ok()?;
        tracks
            .iter()
            .filter(|((track_room, _), _)| track_room == room)
            .find_map(|((_, publisher_identity), participant_tracks)| {
                participant_tracks
                    .iter()
                    .find(|track| track.sid == track_sid)
                    .cloned()
                    .map(|track| (publisher_identity.clone(), track))
            })
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut tracks) = self.tracks.lock() {
            tracks.remove(&(room.to_string(), identity.to_string()));
        }
    }
}

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

type ForwardTrackKey = (String, String, String, String);
type ForwardTrackReaderKey = (String, String, String);

#[derive(Clone, Default)]
pub(crate) struct ForwardTrackStore {
    tracks: Arc<Mutex<HashMap<ForwardTrackKey, oxidesfu_rtc::LocalRtpTrack>>>,
    active: Arc<Mutex<HashSet<ForwardTrackKey>>>,
    active_by_track: Arc<Mutex<HashMap<ForwardTrackReaderKey, HashSet<ForwardTrackKey>>>>,
    started: Arc<Mutex<HashMap<ForwardTrackReaderKey, u64>>>,
    next_reader_lease: Arc<AtomicU64>,
    revision: Arc<AtomicU64>,
}

impl std::fmt::Debug for ForwardTrackStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardTrackStore").finish_non_exhaustive()
    }
}

impl ForwardTrackStore {
    fn bump_revision(&self) {
        self.revision.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    fn reader_key_for_forward_key(key: &ForwardTrackKey) -> ForwardTrackReaderKey {
        (key.0.clone(), key.1.clone(), key.2.clone())
    }

    fn reader_key(room: &str, publisher_identity: &str, track_sid: &str) -> ForwardTrackReaderKey {
        (
            room.to_string(),
            publisher_identity.to_string(),
            track_sid.to_string(),
        )
    }

    fn set_active_index_for_key(&self, key: &ForwardTrackKey, active: bool) {
        if let Ok(mut active_by_track) = self.active_by_track.lock() {
            let reader_key = Self::reader_key_for_forward_key(key);
            if active {
                active_by_track
                    .entry(reader_key)
                    .or_default()
                    .insert(key.clone());
            } else if let Some(keys) = active_by_track.get_mut(&reader_key) {
                keys.remove(key);
                if keys.is_empty() {
                    active_by_track.remove(&reader_key);
                }
            }
        }
    }

    fn set_active_index_for_keys(&self, keys: &[ForwardTrackKey], active: bool) {
        if keys.is_empty() {
            return;
        }
        if let Ok(mut active_by_track) = self.active_by_track.lock() {
            for key in keys {
                let reader_key = Self::reader_key_for_forward_key(key);
                if active {
                    active_by_track
                        .entry(reader_key)
                        .or_default()
                        .insert(key.clone());
                } else if let Some(active_keys) = active_by_track.get_mut(&reader_key) {
                    active_keys.remove(key);
                    if active_keys.is_empty() {
                        active_by_track.remove(&reader_key);
                    }
                }
            }
        }
    }

    fn rebuild_active_index_from_active_set(&self) {
        let Ok(active_tracks) = self.active.lock() else {
            return;
        };
        let mut next_index = HashMap::<ForwardTrackReaderKey, HashSet<ForwardTrackKey>>::new();
        for key in active_tracks.iter() {
            next_index
                .entry(Self::reader_key_for_forward_key(key))
                .or_default()
                .insert(key.clone());
        }
        drop(active_tracks);

        if let Ok(mut active_by_track) = self.active_by_track.lock() {
            *active_by_track = next_index;
        }
    }

    #[allow(dead_code)]
    pub(crate) fn insert(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
        track: oxidesfu_rtc::LocalRtpTrack,
    ) {
        self.insert_with_active(
            room,
            publisher_identity,
            track_sid,
            subscriber_identity,
            track,
            true,
        );
    }

    pub(crate) fn insert_inactive(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
        track: oxidesfu_rtc::LocalRtpTrack,
    ) {
        self.insert_with_active(
            room,
            publisher_identity,
            track_sid,
            subscriber_identity,
            track,
            false,
        );
    }

    fn insert_with_active(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
        track: oxidesfu_rtc::LocalRtpTrack,
        active: bool,
    ) {
        let key = (
            room.to_string(),
            publisher_identity.to_string(),
            track_sid.to_string(),
            subscriber_identity.to_string(),
        );
        let forwarding_mid = track.forwarding_mid().map(str::to_string);
        if let Ok(mut tracks) = self.tracks.lock() {
            tracks.insert(key.clone(), track);
            tracing::debug!(
                room,
                publisher_identity,
                track_sid,
                subscriber_identity,
                active,
                forwarding_mid = ?forwarding_mid,
                total_forward_tracks = tracks.len(),
                "forward_track_store_inserted"
            );
        }
        if let Ok(mut active_tracks) = self.active.lock() {
            if active {
                active_tracks.insert(key.clone());
            } else {
                active_tracks.remove(&key);
            }
            tracing::debug!(
                room,
                publisher_identity,
                track_sid,
                subscriber_identity,
                active,
                active_forward_tracks = active_tracks.len(),
                "forward_track_store_active_state_updated"
            );
        }
        self.set_active_index_for_key(&key, active);
        self.bump_revision();
    }

    #[allow(dead_code)]
    pub(crate) fn activate_subscriber(&self, room: &str, subscriber_identity: &str) {
        let Ok(tracks) = self.tracks.lock() else {
            tracing::warn!(
                room,
                subscriber_identity,
                "forward_track_activate_all_tracks_lock_failed"
            );
            return;
        };
        let keys = tracks
            .keys()
            .filter(
                |(candidate_room, _publisher, _track_sid, candidate_subscriber)| {
                    candidate_room == room && candidate_subscriber == subscriber_identity
                },
            )
            .cloned()
            .collect::<Vec<_>>();
        drop(tracks);

        let key_debug = keys
            .iter()
            .map(|(room, publisher, track_sid, subscriber)| {
                format!(
                    "room={room} publisher={publisher} track={track_sid} subscriber={subscriber}"
                )
            })
            .collect::<Vec<_>>();

        if let Ok(mut active_tracks) = self.active.lock() {
            active_tracks.extend(keys.iter().cloned());
            tracing::debug!(
                room,
                subscriber_identity,
                activated = ?key_debug,
                active_forward_tracks = active_tracks.len(),
                "forward_track_activate_subscriber_all"
            );
        }
        self.set_active_index_for_keys(&keys, true);
        if !keys.is_empty() {
            self.bump_revision();
        }
    }

    pub(crate) fn activate_subscriber_track_sids(
        &self,
        room: &str,
        subscriber_identity: &str,
        track_sids: &HashSet<String>,
    ) {
        if track_sids.is_empty() {
            return;
        }

        let Ok(tracks) = self.tracks.lock() else {
            tracing::warn!(
                room,
                subscriber_identity,
                "forward_track_activate_scoped_tracks_lock_failed"
            );
            return;
        };
        let keys = tracks
            .keys()
            .filter(
                |(candidate_room, _publisher, candidate_track_sid, candidate_subscriber)| {
                    candidate_room == room
                        && candidate_subscriber == subscriber_identity
                        && track_sids.contains(candidate_track_sid)
                },
            )
            .cloned()
            .collect::<Vec<_>>();
        drop(tracks);

        let key_debug = keys
            .iter()
            .map(|(room, publisher, track_sid, subscriber)| {
                format!(
                    "room={room} publisher={publisher} track={track_sid} subscriber={subscriber}"
                )
            })
            .collect::<Vec<_>>();

        if let Ok(mut active_tracks) = self.active.lock() {
            active_tracks.extend(keys.iter().cloned());
            tracing::debug!(
                room,
                subscriber_identity,
                requested_track_sids = ?track_sids,
                activated = ?key_debug,
                active_forward_tracks = active_tracks.len(),
                "forward_track_activate_subscriber_scoped"
            );
        }
        self.set_active_index_for_keys(&keys, true);
        if !keys.is_empty() {
            self.bump_revision();
        }
    }

    pub(crate) fn list_for_track(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
    ) -> Vec<(ForwardTrackKey, oxidesfu_rtc::LocalRtpTrack)> {
        let reader_key = Self::reader_key(room, publisher_identity, track_sid);
        let active_keys = self
            .active_by_track
            .lock()
            .ok()
            .and_then(|active_by_track| active_by_track.get(&reader_key).cloned())
            .unwrap_or_default();

        self.tracks
            .lock()
            .map(|tracks| {
                active_keys
                    .iter()
                    .filter_map(|key| tracks.get(key).map(|track| (key.clone(), track.clone())))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) async fn bind_result_for_subscriber_track(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> Option<oxidesfu_rtc::ForwardTrackBindResult> {
        let track = self.tracks.lock().ok().and_then(|tracks| {
            tracks
                .get(&(
                    room.to_string(),
                    publisher_identity.to_string(),
                    track_sid.to_string(),
                    subscriber_identity.to_string(),
                ))
                .cloned()
        });
        match track {
            Some(track) => Some(track.bind_result().await),
            None => None,
        }
    }

    pub(crate) fn forwarding_mid_for_subscriber_track(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> Option<String> {
        self.tracks.lock().ok().and_then(|tracks| {
            tracks
                .get(&(
                    room.to_string(),
                    publisher_identity.to_string(),
                    track_sid.to_string(),
                    subscriber_identity.to_string(),
                ))
                .and_then(|track| track.forwarding_mid().map(str::to_string))
        })
    }

    pub(crate) fn subscriber_track_sids_for_forwarding_mids(
        &self,
        room: &str,
        subscriber_identity: &str,
        forwarding_mids: &HashSet<String>,
    ) -> HashSet<String> {
        if forwarding_mids.is_empty() {
            return HashSet::new();
        }

        self.tracks
            .lock()
            .map(|tracks| {
                tracks
                    .iter()
                    .filter_map(
                        |(
                            (
                                candidate_room,
                                _publisher_identity,
                                candidate_track_sid,
                                candidate_subscriber,
                            ),
                            track,
                        )| {
                            if candidate_room == room
                                && candidate_subscriber == subscriber_identity
                                && track
                                    .forwarding_mid()
                                    .is_some_and(|mid| forwarding_mids.contains(mid))
                            {
                                Some(candidate_track_sid.clone())
                            } else {
                                None
                            }
                        },
                    )
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default()
    }

    /// Removes every forwarding sender for one publisher track.
    pub(crate) fn remove_all_for_track(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
    ) -> Vec<(String, oxidesfu_rtc::LocalRtpTrack)> {
        let keys = self
            .tracks
            .lock()
            .map(|tracks| {
                tracks
                    .keys()
                    .filter(
                        |(candidate_room, candidate_publisher, candidate_track, _subscriber)| {
                            candidate_room == room
                                && candidate_publisher == publisher_identity
                                && candidate_track == track_sid
                        },
                    )
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        keys.into_iter()
            .filter_map(|(_room, _publisher, _track, subscriber_identity)| {
                self.remove(room, publisher_identity, track_sid, &subscriber_identity)
                    .map(|track| (subscriber_identity, track))
            })
            .collect()
    }

    pub(crate) fn remove(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> Option<oxidesfu_rtc::LocalRtpTrack> {
        let key = (
            room.to_string(),
            publisher_identity.to_string(),
            track_sid.to_string(),
            subscriber_identity.to_string(),
        );
        if let Ok(mut active_tracks) = self.active.lock() {
            let was_active = active_tracks.remove(&key);
            tracing::debug!(
                room,
                publisher_identity,
                track_sid,
                subscriber_identity,
                was_active,
                active_forward_tracks = active_tracks.len(),
                "forward_track_remove_active_state"
            );
        }
        self.set_active_index_for_key(&key, false);
        self.bump_revision();
        self.tracks.lock().ok().and_then(|mut tracks| {
            let removed = tracks.remove(&key);
            tracing::debug!(
                room,
                publisher_identity,
                track_sid,
                subscriber_identity,
                removed = removed.is_some(),
                total_forward_tracks = tracks.len(),
                "forward_track_removed"
            );
            removed
        })
    }

    pub(crate) fn remove_subscriber_mid(
        &self,
        room: &str,
        subscriber_identity: &str,
        forwarding_mid: &str,
    ) -> Vec<(String, String, oxidesfu_rtc::LocalRtpTrack)> {
        let mut removed = Vec::new();
        let mut removed_keys = Vec::new();

        if let Ok(mut tracks) = self.tracks.lock() {
            let keys: Vec<ForwardTrackKey> = tracks
                .iter()
                .filter_map(
                    |(
                        key @ (
                            candidate_room,
                            _candidate_publisher,
                            _candidate_track_sid,
                            candidate_subscriber,
                        ),
                        track,
                    )| {
                        if candidate_room == room
                            && candidate_subscriber == subscriber_identity
                            && track.forwarding_mid() == Some(forwarding_mid)
                        {
                            Some(key.clone())
                        } else {
                            None
                        }
                    },
                )
                .collect();

            for key in keys {
                if let Some(track) = tracks.remove(&key) {
                    tracing::debug!(
                        room,
                        subscriber_identity,
                        publisher_identity = %key.1,
                        track_sid = %key.2,
                        forwarding_mid,
                        total_forward_tracks_after_remove = tracks.len(),
                        "forward_track_remove_subscriber_mid_removed_track"
                    );
                    removed_keys.push(key.clone());
                    removed.push((key.1.clone(), key.2.clone(), track));
                }
            }
        }

        if !removed_keys.is_empty()
            && let Ok(mut active_tracks) = self.active.lock()
        {
            for key in &removed_keys {
                let was_active = active_tracks.remove(key);
                tracing::debug!(
                    room,
                    subscriber_identity,
                    publisher_identity = %key.1,
                    track_sid = %key.2,
                    forwarding_mid,
                    was_active,
                    active_forward_tracks = active_tracks.len(),
                    "forward_track_remove_subscriber_mid_active_state_removed"
                );
            }
        }
        self.set_active_index_for_keys(&removed_keys, false);
        if !removed_keys.is_empty() {
            self.bump_revision();
        }

        removed
    }

    pub(crate) fn remove_track(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
    ) -> Vec<(String, oxidesfu_rtc::LocalRtpTrack)> {
        let mut removed = Vec::new();
        if let Ok(mut tracks) = self.tracks.lock() {
            let keys: Vec<ForwardTrackKey> = tracks
                .keys()
                .filter(
                    |(candidate_room, candidate_publisher, candidate_track_sid, _subscriber)| {
                        candidate_room == room
                            && candidate_publisher == publisher_identity
                            && candidate_track_sid == track_sid
                    },
                )
                .cloned()
                .collect();
            for key in keys {
                if let Some(track) = tracks.remove(&key) {
                    if let Ok(mut active_tracks) = self.active.lock() {
                        let was_active = active_tracks.remove(&key);
                        tracing::debug!(
                            room,
                            publisher_identity,
                            track_sid,
                            subscriber_identity = %key.3,
                            was_active,
                            active_forward_tracks = active_tracks.len(),
                            "forward_track_remove_track_active_state_removed"
                        );
                    }
                    self.set_active_index_for_key(&key, false);
                    tracing::debug!(
                        room,
                        publisher_identity,
                        track_sid,
                        subscriber_identity = %key.3,
                        total_forward_tracks_after_remove = tracks.len(),
                        "forward_track_remove_track_removed"
                    );
                    removed.push((key.3, track));
                }
            }
        }
        if !removed.is_empty() {
            self.bump_revision();
        }
        if let Ok(mut started) = self.started.lock() {
            started.remove(&(
                room.to_string(),
                publisher_identity.to_string(),
                track_sid.to_string(),
            ));
        }
        removed
    }

    /// Acquires exclusive ownership of the reader for one inbound remote-track instance.
    ///
    /// The returned lease must be released when that instance ends. A stale reader cannot
    /// release a lease acquired by a replacement remote track.
    pub(crate) fn acquire_track_reader(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
    ) -> Option<u64> {
        let key = Self::reader_key(room, publisher_identity, track_sid);
        let lease = self.next_reader_lease.fetch_add(1, Ordering::Relaxed);
        self.started
            .lock()
            .ok()
            .and_then(|mut started| match started.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(lease);
                    Some(lease)
                }
                std::collections::hash_map::Entry::Occupied(_) => None,
            })
    }

    /// Returns whether an inbound reader still owns its lease.
    pub(crate) fn owns_track_reader(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        lease: u64,
    ) -> bool {
        let key = Self::reader_key(room, publisher_identity, track_sid);
        self.started
            .lock()
            .is_ok_and(|started| started.get(&key) == Some(&lease))
    }

    /// Revokes the current reader lease so a replacement remote track can take over.
    pub(crate) fn revoke_track_reader(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
    ) -> bool {
        let key = Self::reader_key(room, publisher_identity, track_sid);
        self.started
            .lock()
            .ok()
            .and_then(|mut started| started.remove(&key))
            .is_some()
    }

    /// Releases a reader lease only when it is still owned by that remote-track instance.
    pub(crate) fn release_track_reader(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        lease: u64,
    ) -> bool {
        let key = Self::reader_key(room, publisher_identity, track_sid);
        self.started
            .lock()
            .ok()
            .and_then(|mut started| {
                (started.get(&key) == Some(&lease)).then(|| started.remove(&key))
            })
            .flatten()
            .is_some()
    }

    #[allow(dead_code)]
    pub(crate) fn clear_track_readers_for_publisher(&self, room: &str, publisher_identity: &str) {
        if let Ok(mut started) = self.started.lock() {
            started.retain(
                |(candidate_room, candidate_publisher, _track_sid), _lease| {
                    candidate_room != room || candidate_publisher != publisher_identity
                },
            );
        }
    }

    pub(crate) fn remove_all_for_publisher(
        &self,
        room: &str,
        publisher_identity: &str,
    ) -> Vec<(String, String, oxidesfu_rtc::LocalRtpTrack)> {
        let mut removed = Vec::new();

        if let Ok(mut tracks) = self.tracks.lock() {
            let keys = tracks
                .keys()
                .filter(
                    |(
                        candidate_room,
                        candidate_publisher_identity,
                        _track_sid,
                        _subscriber_identity,
                    )| {
                        candidate_room == room && candidate_publisher_identity == publisher_identity
                    },
                )
                .cloned()
                .collect::<Vec<_>>();

            for key in &keys {
                if let Some(track) = tracks.remove(key) {
                    removed.push((key.2.clone(), key.3.clone(), track));
                }
            }
            self.set_active_index_for_keys(&keys, false);
            if !keys.is_empty() {
                self.bump_revision();
            }

            tracing::debug!(
                room,
                publisher_identity,
                removed = removed.len(),
                remaining = tracks.len(),
                "forward_track_remove_all_for_publisher"
            );
        }

        if let Ok(mut active_tracks) = self.active.lock() {
            active_tracks.retain(
                |(candidate_room, candidate_publisher_identity, _track_sid, _subscriber)| {
                    !(candidate_room == room && candidate_publisher_identity == publisher_identity)
                },
            );
        }
        self.rebuild_active_index_from_active_set();

        if let Ok(mut started) = self.started.lock() {
            started.retain(
                |(candidate_room, candidate_publisher_identity, _track_sid), _lease| {
                    !(candidate_room == room && candidate_publisher_identity == publisher_identity)
                },
            );
        }

        removed
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut tracks) = self.tracks.lock() {
            let before = tracks.len();
            tracks.retain(
                |(candidate_room, publisher_identity, _track_sid, subscriber_identity), _| {
                    candidate_room != room
                        || (publisher_identity != identity && subscriber_identity != identity)
                },
            );
            tracing::debug!(
                room,
                identity,
                removed = before.saturating_sub(tracks.len()),
                remaining = tracks.len(),
                "forward_track_remove_participant_tracks"
            );
        }
        if let Ok(mut active_tracks) = self.active.lock() {
            let before = active_tracks.len();
            active_tracks.retain(
                |(candidate_room, publisher_identity, _track_sid, subscriber_identity)| {
                    candidate_room != room
                        || (publisher_identity != identity && subscriber_identity != identity)
                },
            );
            tracing::debug!(
                room,
                identity,
                removed = before.saturating_sub(active_tracks.len()),
                remaining = active_tracks.len(),
                "forward_track_remove_participant_active_tracks"
            );
        }
        self.rebuild_active_index_from_active_set();
        self.bump_revision();
        if let Ok(mut started) = self.started.lock() {
            let before = started.len();
            started.retain(|(candidate_room, publisher_identity, _track_sid), _lease| {
                candidate_room != room || publisher_identity != identity
            });
            tracing::debug!(
                room,
                identity,
                removed = before.saturating_sub(started.len()),
                remaining = started.len(),
                "forward_track_remove_participant_started_readers"
            );
        }
    }
}

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use livekit_protocol as proto;

type TrackSettingsKey = (String, String, String);

/// A semantic effective-setting mutation for one subscriber forwarding target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveTrackSettingsChange {
    pub(crate) room: String,
    pub(crate) subscriber_identity: String,
    pub(crate) track_sid: String,
    pub(crate) revision: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct TrackSettingsStore {
    settings: Arc<Mutex<HashMap<TrackSettingsKey, proto::UpdateTrackSettings>>>,
    revisions: Arc<Mutex<HashMap<TrackSettingsKey, u64>>>,
    pending: Arc<Mutex<HashMap<TrackSettingsKey, (u64, proto::UpdateTrackSettings)>>>,
    next_pending_generation: Arc<AtomicU64>,
    revision: Arc<AtomicU64>,
    effective_changes: tokio::sync::broadcast::Sender<EffectiveTrackSettingsChange>,
}

impl Default for TrackSettingsStore {
    fn default() -> Self {
        let (effective_changes, _) = tokio::sync::broadcast::channel(256);
        Self {
            settings: Arc::default(),
            revisions: Arc::default(),
            pending: Arc::default(),
            next_pending_generation: Arc::default(),
            revision: Arc::default(),
            effective_changes,
        }
    }
}

impl TrackSettingsStore {
    fn bump_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn mark_changed(&self, key: TrackSettingsKey) -> u64 {
        let revision = self.bump_revision();
        if let Ok(mut revisions) = self.revisions.lock() {
            revisions.insert(key, revision);
        }
        revision
    }

    fn notify_effective_change(&self, key: TrackSettingsKey, revision: u64) {
        let (room, subscriber_identity, track_sid) = key;
        let _ = self.effective_changes.send(EffectiveTrackSettingsChange {
            room,
            subscriber_identity,
            track_sid,
            revision,
        });
    }

    /// Subscribes a forwarding reader to semantic settings changes.
    pub(crate) fn subscribe_effective_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<EffectiveTrackSettingsChange> {
        self.effective_changes.subscribe()
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    /// Returns the semantic-settings revision for one subscriber and publication.
    pub(crate) fn revision_for_track(&self, room: &str, identity: &str, track_sid: &str) -> u64 {
        self.revisions
            .lock()
            .ok()
            .and_then(|revisions| {
                revisions
                    .get(&(
                        room.to_string(),
                        identity.to_string(),
                        track_sid.to_string(),
                    ))
                    .copied()
            })
            .unwrap_or_default()
    }

    /// Queues ordinary settings for debounce while applying disabled-to-enabled immediately.
    pub(crate) fn schedule_from_request(
        &self,
        room: &str,
        identity: &str,
        request: &proto::UpdateTrackSettings,
    ) -> (Vec<String>, Vec<(String, u64)>) {
        let mut immediate = Vec::new();
        let mut deferred = Vec::new();
        for track_sid in &request.track_sids {
            let key = (room.to_string(), identity.to_string(), track_sid.clone());
            let mut setting = request.clone();
            setting.track_sids = vec![track_sid.clone()];
            let current = self.get_for_track(room, identity, track_sid);
            if current.as_ref() == Some(&setting) {
                if let Ok(mut pending) = self.pending.lock() {
                    pending.remove(&key);
                }
                continue;
            }
            if current.is_some_and(|current| current.disabled && !setting.disabled) {
                self.upsert_from_request(room, identity, &setting);
                if let Ok(mut pending) = self.pending.lock() {
                    pending.remove(&key);
                }
                immediate.push(track_sid.clone());
                continue;
            }
            let generation = self.next_pending_generation.fetch_add(1, Ordering::Relaxed) + 1;
            if let Ok(mut pending) = self.pending.lock() {
                pending.insert(key, (generation, setting));
                deferred.push((track_sid.clone(), generation));
            }
        }
        (immediate, deferred)
    }

    /// Applies a debounced setting only when it is still the most recent pending update.
    pub(crate) fn apply_pending_for_track(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
        generation: u64,
    ) -> bool {
        let key = (
            room.to_string(),
            identity.to_string(),
            track_sid.to_string(),
        );
        let setting = self.pending.lock().ok().and_then(|mut pending| {
            pending
                .get(&key)
                .filter(|(candidate_generation, _)| *candidate_generation == generation)
                .cloned()
                .map(|(_, setting)| {
                    pending.remove(&key);
                    setting
                })
        });
        let Some(setting) = setting else {
            return false;
        };
        let before = self.revision_for_track(room, identity, track_sid);
        self.upsert_from_request(room, identity, &setting);
        self.revision_for_track(room, identity, track_sid) != before
    }

    pub(crate) fn upsert_from_request(
        &self,
        room: &str,
        identity: &str,
        request: &proto::UpdateTrackSettings,
    ) {
        let Ok(mut settings) = self.settings.lock() else {
            return;
        };

        let room = room.to_string();
        let identity = identity.to_string();
        let mut changed = Vec::new();
        for track_sid in &request.track_sids {
            let key = (room.clone(), identity.clone(), track_sid.clone());
            let mut setting = request.clone();
            setting.track_sids = vec![track_sid.clone()];
            if settings.get(&key) == Some(&setting) {
                continue;
            }
            settings.insert(key.clone(), setting);
            changed.push((key.clone(), self.mark_changed(key)));
        }
        drop(settings);

        for (key, revision) in changed {
            self.notify_effective_change(key, revision);
        }
    }

    pub(crate) fn remove_for_track(&self, room: &str, identity: &str, track_sid: &str) {
        let key = (
            room.to_string(),
            identity.to_string(),
            track_sid.to_string(),
        );
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&key);
        }
        let changed = self.settings.lock().ok().and_then(|mut settings| {
            settings
                .remove(&key)
                .map(|_| self.mark_changed(key.clone()))
        });
        if let Some(revision) = changed {
            self.notify_effective_change(key, revision);
        }
    }

    pub(crate) fn track_sids_for_participant(&self, room: &str, identity: &str) -> Vec<String> {
        self.settings
            .lock()
            .map(|settings| {
                settings
                    .keys()
                    .filter(|(key_room, key_identity, _)| {
                        key_room == room && key_identity == identity
                    })
                    .map(|(_, _, key_track_sid)| key_track_sid.clone())
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    }

    #[allow(dead_code)]
    pub(crate) fn get_for_track(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
    ) -> Option<proto::UpdateTrackSettings> {
        self.settings.lock().ok().and_then(|settings| {
            settings
                .get(&(
                    room.to_string(),
                    identity.to_string(),
                    track_sid.to_string(),
                ))
                .cloned()
        })
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.retain(|(key_room, key_identity, _), _| {
                key_room != room || key_identity != identity
            });
        }
        let Ok(mut settings) = self.settings.lock() else {
            return;
        };
        let removed = settings
            .keys()
            .filter(|(key_room, key_identity, _)| key_room == room && key_identity == identity)
            .cloned()
            .collect::<Vec<_>>();
        let changed = removed
            .into_iter()
            .filter_map(|key| {
                settings
                    .remove(&key)
                    .map(|_| (key.clone(), self.mark_changed(key)))
            })
            .collect::<Vec<_>>();
        drop(settings);

        for (key, revision) in changed {
            self.notify_effective_change(key, revision);
        }
    }

    pub(crate) fn settings_for_track(
        &self,
        room: &str,
        track_sid: &str,
    ) -> Vec<(String, proto::UpdateTrackSettings)> {
        self.settings
            .lock()
            .map(|settings| {
                settings
                    .iter()
                    .filter(|((key_room, _, key_track_sid), _)| {
                        key_room == room && key_track_sid == track_sid
                    })
                    .map(|((_, key_identity, _), setting)| (key_identity.clone(), setting.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[allow(deprecated)]
pub(crate) fn aggregate_max_quality_from_track_settings(
    settings: impl IntoIterator<Item = proto::UpdateTrackSettings>,
) -> Option<proto::VideoQuality> {
    settings
        .into_iter()
        .filter(|setting| !setting.disabled)
        .map(|setting| {
            proto::VideoQuality::try_from(setting.quality).unwrap_or(proto::VideoQuality::High)
        })
        .max_by_key(|quality| *quality as i32)
}

#[allow(dead_code, deprecated)]
pub(crate) fn subscribed_quality_updates_from_track_settings(
    request: &proto::UpdateTrackSettings,
) -> Vec<proto::SubscribedQualityUpdate> {
    let preferred_quality =
        proto::VideoQuality::try_from(request.quality).unwrap_or(proto::VideoQuality::High);

    request
        .track_sids
        .iter()
        .map(|track_sid| {
            if request.disabled {
                subscribed_quality_update_for_track(track_sid, None)
            } else {
                subscribed_quality_update_for_track(track_sid, Some(preferred_quality))
            }
        })
        .collect()
}

#[allow(dead_code, deprecated)]
pub(crate) fn subscribed_quality_update_for_track(
    track_sid: &str,
    max_quality: Option<proto::VideoQuality>,
) -> proto::SubscribedQualityUpdate {
    subscribed_quality_update_for_track_with_codecs(
        track_sid,
        &["video/vp8".to_string()],
        max_quality,
    )
}

#[allow(deprecated)]
pub(crate) fn subscribed_quality_update_for_track_with_codecs(
    track_sid: &str,
    codec_mime_types: &[String],
    max_quality: Option<proto::VideoQuality>,
) -> proto::SubscribedQualityUpdate {
    let quality_values = [
        proto::VideoQuality::Low,
        proto::VideoQuality::Medium,
        proto::VideoQuality::High,
    ];

    let subscribed_qualities = quality_values
        .iter()
        .map(|quality| {
            let enabled =
                max_quality.is_some_and(|max_quality| (*quality as i32) <= (max_quality as i32));
            proto::SubscribedQuality {
                quality: *quality as i32,
                enabled,
            }
        })
        .collect::<Vec<_>>();

    let subscribed_codecs = if codec_mime_types.is_empty() {
        vec![proto::SubscribedCodec {
            codec: "video/vp8".to_string(),
            qualities: subscribed_qualities.clone(),
        }]
    } else {
        codec_mime_types
            .iter()
            .map(|codec| proto::SubscribedCodec {
                codec: codec.clone(),
                qualities: subscribed_qualities.clone(),
            })
            .collect()
    };

    proto::SubscribedQualityUpdate {
        track_sid: track_sid.to_string(),
        subscribed_qualities,
        subscribed_codecs,
    }
}

pub(crate) fn codec_mime_types_for_track(track: &proto::TrackInfo) -> Vec<String> {
    let mut codec_mime_types = track
        .codecs
        .iter()
        .map(|codec| codec.mime_type.trim().to_ascii_lowercase())
        .filter(|codec| !codec.is_empty())
        .collect::<Vec<_>>();

    if codec_mime_types.is_empty() {
        let fallback = track.mime_type.trim().to_ascii_lowercase();
        if !fallback.is_empty() {
            codec_mime_types.push(fallback);
        }
    }
    if codec_mime_types.is_empty() {
        codec_mime_types.push("video/vp8".to_string());
    }

    codec_mime_types.sort();
    codec_mime_types.dedup();
    codec_mime_types
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(deprecated)]
    #[test]
    fn subscribed_quality_update_for_track_low_enables_only_low_quality() {
        let update = subscribed_quality_update_for_track("TR_test", Some(proto::VideoQuality::Low));

        assert_eq!(update.subscribed_qualities.len(), 3);
        assert_eq!(
            update.subscribed_qualities[0].quality,
            proto::VideoQuality::Low as i32
        );
        assert!(update.subscribed_qualities[0].enabled);
        assert!(!update.subscribed_qualities[1].enabled);
        assert!(!update.subscribed_qualities[2].enabled);
    }

    #[allow(deprecated)]
    #[test]
    fn subscribed_quality_update_for_track_none_disables_all_qualities() {
        let update = subscribed_quality_update_for_track("TR_test", None);
        assert!(
            update
                .subscribed_qualities
                .iter()
                .all(|quality| !quality.enabled)
        );
    }

    #[allow(deprecated)]
    #[test]
    fn subscribed_quality_update_for_track_medium_enables_low_and_medium() {
        let update =
            subscribed_quality_update_for_track("TR_test", Some(proto::VideoQuality::Medium));

        assert_eq!(update.subscribed_qualities.len(), 3);
        assert!(update.subscribed_qualities[0].enabled);
        assert!(update.subscribed_qualities[1].enabled);
        assert!(!update.subscribed_qualities[2].enabled);
    }

    #[allow(deprecated)]
    #[test]
    fn subscribed_quality_update_for_track_high_enables_all_qualities() {
        let update =
            subscribed_quality_update_for_track("TR_test", Some(proto::VideoQuality::High));

        assert_eq!(update.subscribed_qualities.len(), 3);
        assert!(
            update
                .subscribed_qualities
                .iter()
                .all(|quality| quality.enabled)
        );
    }

    #[allow(deprecated)]
    #[test]
    fn aggregate_max_quality_from_track_settings_uses_highest_enabled_quality() {
        let aggregate = aggregate_max_quality_from_track_settings([
            proto::UpdateTrackSettings {
                disabled: false,
                quality: proto::VideoQuality::Low as i32,
                ..Default::default()
            },
            proto::UpdateTrackSettings {
                disabled: true,
                quality: proto::VideoQuality::High as i32,
                ..Default::default()
            },
            proto::UpdateTrackSettings {
                disabled: false,
                quality: proto::VideoQuality::Medium as i32,
                ..Default::default()
            },
        ]);

        assert_eq!(aggregate, Some(proto::VideoQuality::Medium));
    }

    #[allow(deprecated)]
    #[test]
    fn aggregate_max_quality_from_track_settings_returns_none_when_all_disabled() {
        let aggregate = aggregate_max_quality_from_track_settings([
            proto::UpdateTrackSettings {
                disabled: true,
                quality: proto::VideoQuality::Low as i32,
                ..Default::default()
            },
            proto::UpdateTrackSettings {
                disabled: true,
                quality: proto::VideoQuality::High as i32,
                ..Default::default()
            },
        ]);

        assert_eq!(aggregate, None);
    }

    #[allow(deprecated)]
    #[test]
    fn track_settings_store_lists_settings_for_track_across_subscribers() {
        let store = TrackSettingsStore::default();
        store.upsert_from_request(
            "room",
            "alice",
            &proto::UpdateTrackSettings {
                track_sids: vec!["TR_a".to_string()],
                quality: proto::VideoQuality::Low as i32,
                ..Default::default()
            },
        );
        store.upsert_from_request(
            "room",
            "bob",
            &proto::UpdateTrackSettings {
                track_sids: vec!["TR_a".to_string(), "TR_b".to_string()],
                quality: proto::VideoQuality::High as i32,
                ..Default::default()
            },
        );

        let mut identities = store
            .settings_for_track("room", "TR_a")
            .into_iter()
            .map(|(identity, _)| identity)
            .collect::<Vec<_>>();
        identities.sort();

        assert_eq!(identities, vec!["alice", "bob"]);
        assert_eq!(store.settings_for_track("room", "TR_b").len(), 1);
    }

    #[allow(deprecated)]
    #[test]
    fn track_settings_store_remove_for_track_removes_only_that_track() {
        let store = TrackSettingsStore::default();
        store.upsert_from_request(
            "room",
            "alice",
            &proto::UpdateTrackSettings {
                track_sids: vec!["TR_a".to_string(), "TR_b".to_string()],
                quality: proto::VideoQuality::High as i32,
                ..Default::default()
            },
        );

        store.remove_for_track("room", "alice", "TR_a");

        assert!(store.get_for_track("room", "alice", "TR_a").is_none());
        assert!(store.get_for_track("room", "alice", "TR_b").is_some());
    }

    #[allow(deprecated)]
    #[test]
    fn track_settings_store_track_sids_for_participant_lists_unique_tracks() {
        let store = TrackSettingsStore::default();
        store.upsert_from_request(
            "room",
            "alice",
            &proto::UpdateTrackSettings {
                track_sids: vec!["TR_a".to_string()],
                quality: proto::VideoQuality::Low as i32,
                ..Default::default()
            },
        );
        store.upsert_from_request(
            "room",
            "alice",
            &proto::UpdateTrackSettings {
                track_sids: vec!["TR_b".to_string()],
                quality: proto::VideoQuality::Medium as i32,
                ..Default::default()
            },
        );

        let mut track_sids = store.track_sids_for_participant("room", "alice");
        track_sids.sort();
        assert_eq!(track_sids, vec!["TR_a", "TR_b"]);
    }

    #[allow(deprecated)]
    #[test]
    fn subscribed_quality_update_for_track_with_codecs_applies_same_quality_vector_per_codec() {
        let update = subscribed_quality_update_for_track_with_codecs(
            "TR_test",
            &["video/vp8".to_string(), "video/h264".to_string()],
            Some(proto::VideoQuality::Low),
        );

        assert_eq!(update.subscribed_codecs.len(), 2);
        assert_eq!(update.subscribed_codecs[0].qualities.len(), 3);
        assert!(update.subscribed_codecs[0].qualities[0].enabled);
        assert!(!update.subscribed_codecs[0].qualities[1].enabled);
        assert!(!update.subscribed_codecs[0].qualities[2].enabled);
        assert_eq!(update.subscribed_codecs[1].qualities.len(), 3);
        assert!(update.subscribed_codecs[1].qualities[0].enabled);
        assert!(!update.subscribed_codecs[1].qualities[1].enabled);
        assert!(!update.subscribed_codecs[1].qualities[2].enabled);
    }

    #[allow(deprecated)]
    #[test]
    fn track_settings_store_debounces_rapid_dimension_updates_to_the_latest_value() {
        let store = TrackSettingsStore::default();
        let first = proto::UpdateTrackSettings {
            track_sids: vec!["TR_video".to_string()],
            quality: proto::VideoQuality::High as i32,
            width: 468,
            height: 940,
            ..Default::default()
        };
        let second = proto::UpdateTrackSettings {
            height: 121,
            ..first.clone()
        };

        let (_, first_deferred) = store.schedule_from_request("room", "subscriber", &first);
        let (_, second_deferred) = store.schedule_from_request("room", "subscriber", &second);
        let first_generation = first_deferred[0].1;
        let second_generation = second_deferred[0].1;

        assert!(
            !store.apply_pending_for_track("room", "subscriber", "TR_video", first_generation),
            "a superseded dimension update must not become effective"
        );
        assert!(store.apply_pending_for_track("room", "subscriber", "TR_video", second_generation));
        let effective = store
            .get_for_track("room", "subscriber", "TR_video")
            .expect("latest debounced setting should become effective");
        assert_eq!((effective.width, effective.height), (468, 121));
    }

    #[test]
    fn track_settings_store_revisions_advance_only_for_semantic_target_changes() {
        let store = TrackSettingsStore::default();
        let initial = store.revision();
        let initial_a = store.revision_for_track("room", "alice", "TR_a");
        let initial_b = store.revision_for_track("room", "bob", "TR_b");
        let setting = proto::UpdateTrackSettings {
            track_sids: vec!["TR_a".to_string()],
            quality: proto::VideoQuality::Low as i32,
            ..Default::default()
        };

        store.upsert_from_request("room", "alice", &setting);
        let after_upsert = store.revision();
        let after_upsert_a = store.revision_for_track("room", "alice", "TR_a");
        assert!(after_upsert > initial);
        assert!(after_upsert_a > initial_a);
        assert_eq!(store.revision_for_track("room", "bob", "TR_b"), initial_b);

        store.upsert_from_request("room", "alice", &setting);
        assert_eq!(store.revision(), after_upsert);
        assert_eq!(
            store.revision_for_track("room", "alice", "TR_a"),
            after_upsert_a,
            "an identical setting must not restart this target's layer selection"
        );

        store.upsert_from_request(
            "room",
            "bob",
            &proto::UpdateTrackSettings {
                track_sids: vec!["TR_b".to_string()],
                quality: proto::VideoQuality::High as i32,
                ..Default::default()
            },
        );
        assert_eq!(
            store.revision_for_track("room", "alice", "TR_a"),
            after_upsert_a,
            "an unrelated subscriber/track must not restart this target's layer selection"
        );

        store.remove_for_track("room", "alice", "TR_a");
        assert!(store.revision() > after_upsert);
        assert!(store.revision_for_track("room", "alice", "TR_a") > after_upsert_a);
    }

    #[tokio::test]
    async fn effective_track_setting_changes_notify_only_semantic_mutations_and_removals() {
        let store = TrackSettingsStore::default();
        let mut changes = store.subscribe_effective_changes();
        let setting = proto::UpdateTrackSettings {
            track_sids: vec!["TR_video".to_string()],
            quality: proto::VideoQuality::Low as i32,
            width: 320,
            height: 180,
            fps: 15,
            ..Default::default()
        };

        store.upsert_from_request("room", "subscriber", &setting);
        let applied = changes
            .recv()
            .await
            .expect("effective setting should notify reader");
        assert_eq!(applied.room, "room");
        assert_eq!(applied.subscriber_identity, "subscriber");
        assert_eq!(applied.track_sid, "TR_video");
        assert_eq!(
            applied.revision,
            store.revision_for_track("room", "subscriber", "TR_video")
        );

        store.upsert_from_request("room", "subscriber", &setting);
        assert!(matches!(
            changes.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        let deferred = proto::UpdateTrackSettings {
            height: 360,
            fps: 10,
            ..setting.clone()
        };
        let (_, pending) = store.schedule_from_request("room", "subscriber", &deferred);
        assert!(store.apply_pending_for_track("room", "subscriber", "TR_video", pending[0].1));
        let debounced = changes
            .recv()
            .await
            .expect("effective debounced setting should notify reader");
        assert!(debounced.revision > applied.revision);
        assert_eq!(
            store
                .get_for_track("room", "subscriber", "TR_video")
                .expect("debounced setting should be effective")
                .fps,
            10
        );

        let pending_removal = proto::UpdateTrackSettings {
            disabled: true,
            ..deferred
        };
        let (_, pending) = store.schedule_from_request("room", "subscriber", &pending_removal);
        store.remove_for_track("room", "subscriber", "TR_video");
        let removed = changes
            .recv()
            .await
            .expect("effective removal should notify reader");
        assert_eq!(removed.room, "room");
        assert_eq!(removed.subscriber_identity, "subscriber");
        assert_eq!(removed.track_sid, "TR_video");
        assert!(removed.revision > debounced.revision);
        assert!(
            !store.apply_pending_for_track("room", "subscriber", "TR_video", pending[0].1),
            "removal must cancel a pending debounce so it cannot resurrect a subscription"
        );
        assert!(matches!(
            changes.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn codec_mime_types_for_track_prefers_codecs_and_deduplicates() {
        let track = proto::TrackInfo {
            mime_type: "video/vp8".to_string(),
            codecs: vec![
                proto::SimulcastCodecInfo {
                    mime_type: "video/h264".to_string(),
                    ..Default::default()
                },
                proto::SimulcastCodecInfo {
                    mime_type: "video/H264".to_string(),
                    ..Default::default()
                },
                proto::SimulcastCodecInfo {
                    mime_type: "video/av1".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let codecs = codec_mime_types_for_track(&track);
        assert_eq!(codecs, vec!["video/av1", "video/h264"]);
    }
}

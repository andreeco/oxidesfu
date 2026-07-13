use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use livekit_protocol as proto;

type TrackSettingsKey = (String, String, String);

#[derive(Debug, Clone, Default)]
pub(crate) struct TrackSettingsStore {
    settings: Arc<Mutex<HashMap<TrackSettingsKey, proto::UpdateTrackSettings>>>,
    revision: Arc<AtomicU64>,
}

impl TrackSettingsStore {
    fn bump_revision(&self) {
        self.revision.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    pub(crate) fn upsert_from_request(
        &self,
        room: &str,
        identity: &str,
        request: &proto::UpdateTrackSettings,
    ) {
        if let Ok(mut settings) = self.settings.lock() {
            for track_sid in &request.track_sids {
                settings.insert(
                    (room.to_string(), identity.to_string(), track_sid.clone()),
                    request.clone(),
                );
            }
            self.bump_revision();
        }
    }

    pub(crate) fn remove_for_track(&self, room: &str, identity: &str, track_sid: &str) {
        if let Ok(mut settings) = self.settings.lock() {
            if settings
                .remove(&(
                    room.to_string(),
                    identity.to_string(),
                    track_sid.to_string(),
                ))
                .is_some()
            {
                self.bump_revision();
            }
        }
    }

    pub(crate) fn track_sids_for_participant(&self, room: &str, identity: &str) -> Vec<String> {
        self.settings
            .lock()
            .map(|settings| {
                settings
                    .keys()
                    .filter_map(|(key_room, key_identity, key_track_sid)| {
                        (key_room == room && key_identity == identity)
                            .then(|| key_track_sid.clone())
                    })
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
        if let Ok(mut settings) = self.settings.lock() {
            let size_before = settings.len();
            settings.retain(|(key_room, key_identity, _), _| {
                key_room != room || key_identity != identity
            });
            if settings.len() != size_before {
                self.bump_revision();
            }
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
                    .filter_map(|((key_room, key_identity, key_track_sid), setting)| {
                        (key_room == room && key_track_sid == track_sid)
                            .then(|| (key_identity.clone(), setting.clone()))
                    })
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

    #[test]
    fn track_settings_store_revision_advances_on_mutation() {
        let store = TrackSettingsStore::default();
        let initial = store.revision();

        store.upsert_from_request(
            "room",
            "alice",
            &proto::UpdateTrackSettings {
                track_sids: vec!["TR_a".to_string()],
                ..Default::default()
            },
        );
        let after_upsert = store.revision();
        assert!(after_upsert > initial);

        store.remove_for_track("room", "alice", "TR_a");
        assert!(store.revision() > after_upsert);
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

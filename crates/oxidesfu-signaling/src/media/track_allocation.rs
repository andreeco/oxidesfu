use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use livekit_protocol as proto;

type TrackAllocationKey = (String, String, String);

/// A semantic desired-layer allocation mutation for one subscriber forwarding target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveTrackAllocationChange {
    pub(crate) room: String,
    pub(crate) subscriber_identity: String,
    pub(crate) track_sid: String,
    pub(crate) revision: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct TrackAllocationStore {
    desired_quality: Arc<Mutex<HashMap<TrackAllocationKey, proto::VideoQuality>>>,
    desired_temporal_layer: Arc<Mutex<HashMap<TrackAllocationKey, u8>>>,
    revisions: Arc<Mutex<HashMap<TrackAllocationKey, u64>>>,
    revision: Arc<AtomicU64>,
    effective_changes: tokio::sync::broadcast::Sender<EffectiveTrackAllocationChange>,
}

impl Default for TrackAllocationStore {
    fn default() -> Self {
        let (effective_changes, _) = tokio::sync::broadcast::channel(256);
        Self {
            desired_quality: Arc::default(),
            desired_temporal_layer: Arc::default(),
            revisions: Arc::default(),
            revision: Arc::default(),
            effective_changes,
        }
    }
}

impl TrackAllocationStore {
    fn bump_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn mark_changed(&self, key: TrackAllocationKey) -> u64 {
        let revision = self.bump_revision();
        if let Ok(mut revisions) = self.revisions.lock() {
            revisions.insert(key, revision);
        }
        revision
    }

    fn notify_effective_change(&self, key: TrackAllocationKey, revision: u64) {
        let (room, subscriber_identity, track_sid) = key;
        let _ = self.effective_changes.send(EffectiveTrackAllocationChange {
            room,
            subscriber_identity,
            track_sid,
            revision,
        });
    }

    /// Subscribes a forwarding reader to semantic allocation changes.
    pub(crate) fn subscribe_effective_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<EffectiveTrackAllocationChange> {
        self.effective_changes.subscribe()
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

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

    pub(crate) fn get_desired_quality_for_track(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
    ) -> Option<proto::VideoQuality> {
        self.desired_quality.lock().ok().and_then(|allocations| {
            allocations
                .get(&(
                    room.to_string(),
                    identity.to_string(),
                    track_sid.to_string(),
                ))
                .copied()
        })
    }

    /// Returns the allocator's desired temporal layer for one subscriber target.
    ///
    /// `0`, `1`, and `2` correspond to base through highest temporal enhancement layers.
    pub(crate) fn get_desired_temporal_layer_for_track(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
    ) -> Option<u8> {
        self.desired_temporal_layer
            .lock()
            .ok()
            .and_then(|allocations| {
                allocations
                    .get(&(
                        room.to_string(),
                        identity.to_string(),
                        track_sid.to_string(),
                    ))
                    .copied()
            })
    }

    /// Sets or clears the allocator desired quality for one subscriber target.
    pub(crate) fn set_desired_quality_for_track(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
        desired_quality: Option<proto::VideoQuality>,
    ) {
        let key = (
            room.to_string(),
            identity.to_string(),
            track_sid.to_string(),
        );
        let mut changed = false;

        if let Ok(mut allocations) = self.desired_quality.lock() {
            match desired_quality {
                Some(desired_quality) => {
                    changed = allocations.get(&key).copied() != Some(desired_quality);
                    if changed {
                        allocations.insert(key.clone(), desired_quality);
                    }
                }
                None => {
                    changed = allocations.remove(&key).is_some();
                }
            }
        }

        if changed {
            let revision = self.mark_changed(key.clone());
            self.notify_effective_change(key, revision);
        }
    }

    /// Sets or clears the allocator desired temporal layer for one subscriber target.
    /// Values above temporal layer two are ignored rather than becoming an invalid policy.
    pub(crate) fn set_desired_temporal_layer_for_track(
        &self,
        room: &str,
        identity: &str,
        track_sid: &str,
        desired_temporal_layer: Option<u8>,
    ) {
        let desired_temporal_layer = desired_temporal_layer.filter(|layer| *layer <= 2);
        let key = (
            room.to_string(),
            identity.to_string(),
            track_sid.to_string(),
        );
        let mut changed = false;

        if let Ok(mut allocations) = self.desired_temporal_layer.lock() {
            match desired_temporal_layer {
                Some(desired_temporal_layer) => {
                    changed = allocations.get(&key).copied() != Some(desired_temporal_layer);
                    if changed {
                        allocations.insert(key.clone(), desired_temporal_layer);
                    }
                }
                None => {
                    changed = allocations.remove(&key).is_some();
                }
            }
        }

        if changed {
            let revision = self.mark_changed(key.clone());
            self.notify_effective_change(key, revision);
        }
    }

    pub(crate) fn remove_for_track(&self, room: &str, identity: &str, track_sid: &str) {
        self.set_desired_quality_for_track(room, identity, track_sid, None);
        self.set_desired_temporal_layer_for_track(room, identity, track_sid, None);
    }

    pub(crate) fn remove_for_participant(&self, room: &str, identity: &str) {
        let keys = self
            .desired_quality
            .lock()
            .map(|allocations| {
                allocations
                    .keys()
                    .filter(|(candidate_room, candidate_identity, _)| {
                        candidate_room == room && candidate_identity == identity
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        for (_, _, track_sid) in keys {
            self.set_desired_quality_for_track(room, identity, &track_sid, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TrackAllocationStore;
    use livekit_protocol as proto;

    #[tokio::test]
    async fn effective_changes_notify_on_semantic_allocation_changes_only() {
        let store = TrackAllocationStore::default();
        let mut changes = store.subscribe_effective_changes();

        store.set_desired_quality_for_track(
            "room",
            "sub",
            "TR_video",
            Some(proto::VideoQuality::Low),
        );
        let first = changes.recv().await.expect("first change should publish");
        assert_eq!(first.room, "room");
        assert_eq!(first.subscriber_identity, "sub");
        assert_eq!(first.track_sid, "TR_video");

        store.set_desired_quality_for_track(
            "room",
            "sub",
            "TR_video",
            Some(proto::VideoQuality::Low),
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(40), changes.recv())
                .await
                .is_err(),
            "semantic no-op must not emit a change"
        );

        store.set_desired_quality_for_track(
            "room",
            "sub",
            "TR_video",
            Some(proto::VideoQuality::High),
        );
        let second = changes
            .recv()
            .await
            .expect("changed quality should publish");
        assert!(second.revision > first.revision);

        store.remove_for_track("room", "sub", "TR_video");
        let third = changes.recv().await.expect("removal should publish");
        assert!(third.revision > second.revision);
    }

    #[test]
    fn desired_temporal_layer_is_target_scoped_and_validated() {
        let store = TrackAllocationStore::default();
        store.set_desired_temporal_layer_for_track("room", "sub-a", "TR_video", Some(1));
        store.set_desired_temporal_layer_for_track("room", "sub-b", "TR_video", Some(2));
        store.set_desired_temporal_layer_for_track("room", "sub-a", "TR_invalid", Some(3));

        assert_eq!(
            store.get_desired_temporal_layer_for_track("room", "sub-a", "TR_video"),
            Some(1)
        );
        assert_eq!(
            store.get_desired_temporal_layer_for_track("room", "sub-b", "TR_video"),
            Some(2)
        );
        assert_eq!(
            store.get_desired_temporal_layer_for_track("room", "sub-a", "TR_invalid"),
            None
        );
    }

    #[test]
    fn desired_quality_is_stored_per_target_and_revision_advances() {
        let store = TrackAllocationStore::default();
        let initial = store.revision();
        assert_eq!(
            store.get_desired_quality_for_track("room", "sub", "TR_video"),
            None
        );

        store.set_desired_quality_for_track(
            "room",
            "sub",
            "TR_video",
            Some(proto::VideoQuality::Medium),
        );

        assert_eq!(
            store.get_desired_quality_for_track("room", "sub", "TR_video"),
            Some(proto::VideoQuality::Medium)
        );
        assert!(store.revision() > initial);
        assert!(store.revision_for_track("room", "sub", "TR_video") > 0);
    }
}

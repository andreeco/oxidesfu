use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

type PermissionKey = (String, String);

#[derive(Debug, Clone, Default)]
pub(crate) struct PublishPermissionStore {
    can_publish_media: Arc<Mutex<HashMap<PermissionKey, bool>>>,
    can_publish_data: Arc<Mutex<HashMap<PermissionKey, bool>>>,
    can_publish_sources: Arc<Mutex<HashMap<PermissionKey, HashSet<String>>>>,
}

impl PublishPermissionStore {
    pub(crate) fn set_can_publish_media(&self, room: &str, identity: &str, allowed: bool) {
        if let Ok(mut can_publish_media) = self.can_publish_media.lock() {
            can_publish_media.insert((room.to_string(), identity.to_string()), allowed);
        }
    }

    pub(crate) fn can_publish_media(&self, room: &str, identity: &str) -> bool {
        self.can_publish_media
            .lock()
            .ok()
            .and_then(|can_publish_media| {
                can_publish_media
                    .get(&(room.to_string(), identity.to_string()))
                    .copied()
            })
            .unwrap_or(false)
    }

    pub(crate) fn set_can_publish_data(&self, room: &str, identity: &str, allowed: bool) {
        if let Ok(mut can_publish_data) = self.can_publish_data.lock() {
            can_publish_data.insert((room.to_string(), identity.to_string()), allowed);
        }
    }

    pub(crate) fn set_can_publish_sources(&self, room: &str, identity: &str, sources: &[String]) {
        if let Ok(mut can_publish_sources) = self.can_publish_sources.lock() {
            let key = (room.to_string(), identity.to_string());
            if sources.is_empty() {
                can_publish_sources.remove(&key);
                return;
            }
            let normalized = sources
                .iter()
                .map(|source| source.to_ascii_lowercase())
                .collect::<HashSet<_>>();
            can_publish_sources.insert(key, normalized);
        }
    }

    pub(crate) fn can_publish_data(&self, room: &str, identity: &str) -> bool {
        self.can_publish_data
            .lock()
            .ok()
            .and_then(|can_publish_data| {
                can_publish_data
                    .get(&(room.to_string(), identity.to_string()))
                    .copied()
            })
            .unwrap_or(false)
    }

    pub(crate) fn can_publish_source(&self, room: &str, identity: &str, source: &str) -> bool {
        if !self.can_publish_media(room, identity) {
            return false;
        }

        self.can_publish_sources
            .lock()
            .ok()
            .and_then(|can_publish_sources| {
                can_publish_sources
                    .get(&(room.to_string(), identity.to_string()))
                    .map(|allowed| {
                        if allowed.is_empty() {
                            return true;
                        }
                        allowed.contains(&source.to_ascii_lowercase())
                    })
            })
            .unwrap_or(true)
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut can_publish_media) = self.can_publish_media.lock() {
            can_publish_media.remove(&(room.to_string(), identity.to_string()));
        }
        if let Ok(mut can_publish_data) = self.can_publish_data.lock() {
            can_publish_data.remove(&(room.to_string(), identity.to_string()));
        }
        if let Ok(mut can_publish_sources) = self.can_publish_sources.lock() {
            can_publish_sources.remove(&(room.to_string(), identity.to_string()));
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SubscribePermissionStore {
    can_subscribe_data: Arc<Mutex<HashMap<PermissionKey, bool>>>,
}

impl SubscribePermissionStore {
    pub(crate) fn set_can_subscribe(&self, room: &str, identity: &str, allowed: bool) {
        if let Ok(mut can_subscribe_data) = self.can_subscribe_data.lock() {
            can_subscribe_data.insert((room.to_string(), identity.to_string()), allowed);
        }
    }

    pub(crate) fn can_subscribe(&self, room: &str, identity: &str) -> bool {
        self.can_subscribe_data
            .lock()
            .ok()
            .and_then(|can_subscribe_data| {
                can_subscribe_data
                    .get(&(room.to_string(), identity.to_string()))
                    .copied()
            })
            .unwrap_or(true)
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut can_subscribe_data) = self.can_subscribe_data.lock() {
            can_subscribe_data.remove(&(room.to_string(), identity.to_string()));
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AutoSubscribePreferenceStore {
    auto_subscribe: Arc<Mutex<HashMap<PermissionKey, bool>>>,
    auto_subscribe_data_track: Arc<Mutex<HashMap<PermissionKey, bool>>>,
    revision: Arc<AtomicU64>,
}

impl AutoSubscribePreferenceStore {
    fn bump_revision(&self) {
        self.revision.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    pub(crate) fn set_auto_subscribe(&self, room: &str, identity: &str, enabled: bool) {
        if let Ok(mut auto_subscribe) = self.auto_subscribe.lock() {
            auto_subscribe.insert((room.to_string(), identity.to_string()), enabled);
            self.bump_revision();
        }
    }

    pub(crate) fn auto_subscribe_enabled(&self, room: &str, identity: &str) -> bool {
        self.auto_subscribe
            .lock()
            .ok()
            .and_then(|auto_subscribe| {
                auto_subscribe
                    .get(&(room.to_string(), identity.to_string()))
                    .copied()
            })
            .unwrap_or(true)
    }

    pub(crate) fn set_auto_subscribe_data_track(&self, room: &str, identity: &str, enabled: bool) {
        if let Ok(mut auto_subscribe_data_track) = self.auto_subscribe_data_track.lock() {
            auto_subscribe_data_track.insert((room.to_string(), identity.to_string()), enabled);
            self.bump_revision();
        }
    }

    pub(crate) fn auto_subscribe_data_track_enabled(&self, room: &str, identity: &str) -> bool {
        self.auto_subscribe_data_track
            .lock()
            .ok()
            .and_then(|auto_subscribe_data_track| {
                auto_subscribe_data_track
                    .get(&(room.to_string(), identity.to_string()))
                    .copied()
            })
            .unwrap_or(true)
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        let mut changed = false;
        if let Ok(mut auto_subscribe) = self.auto_subscribe.lock()
            && auto_subscribe
                .remove(&(room.to_string(), identity.to_string()))
                .is_some()
        {
            changed = true;
        }
        if let Ok(mut auto_subscribe_data_track) = self.auto_subscribe_data_track.lock()
            && auto_subscribe_data_track
                .remove(&(room.to_string(), identity.to_string()))
                .is_some()
        {
            changed = true;
        }
        if changed {
            self.bump_revision();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AutoSubscribePreferenceStore, SubscribePermissionStore};

    #[test]
    fn subscribe_permission_defaults_to_allowed_and_can_be_overridden() {
        let store = SubscribePermissionStore::default();

        assert!(store.can_subscribe("room", "alice"));

        store.set_can_subscribe("room", "alice", false);
        assert!(!store.can_subscribe("room", "alice"));

        store.set_can_subscribe("room", "alice", true);
        assert!(store.can_subscribe("room", "alice"));
    }

    #[test]
    fn subscribe_permission_remove_participant_restores_default_allow() {
        let store = SubscribePermissionStore::default();

        store.set_can_subscribe("room", "alice", false);
        assert!(!store.can_subscribe("room", "alice"));

        store.remove_participant("room", "alice");
        assert!(store.can_subscribe("room", "alice"));
    }

    #[test]
    fn auto_subscribe_preference_store_revision_advances_on_mutation() {
        let store = AutoSubscribePreferenceStore::default();
        let initial = store.revision();

        store.set_auto_subscribe("room", "alice", false);
        let after_auto_subscribe = store.revision();
        assert!(after_auto_subscribe > initial);

        store.set_auto_subscribe_data_track("room", "alice", false);
        let after_data_track = store.revision();
        assert!(after_data_track > after_auto_subscribe);

        store.remove_participant("room", "alice");
        assert!(store.revision() > after_data_track);
    }
}

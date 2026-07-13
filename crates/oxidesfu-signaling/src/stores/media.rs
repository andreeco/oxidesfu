use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

type MediaForwardingKey = (String, String, String, String);
type PendingMediaSectionSubscriberKey = (String, String);
type MediaCidKey = (String, String, String);
type PendingPublisherRemoteTrackKey = (String, String);
type SubscriberOfferIdKey = (String, String);
type SinglePcOfferMediaKindKey = (String, String);

#[derive(Debug, Clone, Default)]
pub(crate) struct SinglePcOfferMediaKindStore {
    entries: Arc<
        Mutex<
            HashMap<SinglePcOfferMediaKindKey, HashMap<String, crate::media::ReceiveSectionKind>>,
        >,
    >,
}

impl SinglePcOfferMediaKindStore {
    pub(crate) fn is_compatible_with_previous(
        &self,
        room: &str,
        identity: &str,
        offered: &HashMap<String, crate::media::ReceiveSectionKind>,
    ) -> bool {
        self.entries
            .lock()
            .ok()
            .and_then(|entries| {
                entries
                    .get(&(room.to_string(), identity.to_string()))
                    .cloned()
            })
            .is_none_or(|previous| {
                offered.iter().all(|(mid, kind)| {
                    previous
                        .get(mid)
                        .is_none_or(|previous_kind| previous_kind == kind)
                })
            })
    }

    pub(crate) fn set(
        &self,
        room: &str,
        identity: &str,
        offered: HashMap<String, crate::media::ReceiveSectionKind>,
    ) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert((room.to_string(), identity.to_string()), offered);
        }
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(&(room.to_string(), identity.to_string()));
        }
    }
}

#[derive(Clone)]
pub(crate) struct PendingPublisherRemoteTrack {
    pub(crate) remote_track_id: String,
    pub(crate) remote_track: oxidesfu_rtc::RemoteTrack,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MediaSubscriptionStore {
    entries: Arc<Mutex<HashMap<MediaForwardingKey, bool>>>,
    revision: Arc<AtomicU64>,
}

impl MediaSubscriptionStore {
    fn bump_revision(&self) {
        self.revision.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    pub(crate) fn set_subscribed(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
        subscribed: bool,
    ) {
        let key = (
            room.to_string(),
            publisher_identity.to_string(),
            track_sid.to_string(),
            subscriber_identity.to_string(),
        );
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(key, subscribed);
            self.bump_revision();
        }
    }

    pub(crate) fn explicit_subscription(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> Option<bool> {
        self.entries.lock().ok().and_then(|entries| {
            entries
                .get(&(
                    room.to_string(),
                    publisher_identity.to_string(),
                    track_sid.to_string(),
                    subscriber_identity.to_string(),
                ))
                .copied()
        })
    }

    pub(crate) fn is_subscribed_with_default(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
        default_subscribed: bool,
    ) -> bool {
        self.explicit_subscription(room, publisher_identity, track_sid, subscriber_identity)
            .unwrap_or(default_subscribed)
    }

    pub(crate) fn is_subscribed(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> bool {
        self.is_subscribed_with_default(
            room,
            publisher_identity,
            track_sid,
            subscriber_identity,
            true,
        )
    }

    pub(crate) fn remove_track(&self, room: &str, publisher_identity: &str, track_sid: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            let size_before = entries.len();
            entries.retain(
                |(
                    candidate_room,
                    candidate_publisher,
                    candidate_track_sid,
                    _subscriber_identity,
                ),
                 _| {
                    candidate_room != room
                        || candidate_publisher != publisher_identity
                        || candidate_track_sid != track_sid
                },
            );
            if entries.len() != size_before {
                self.bump_revision();
            }
        }
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            let size_before = entries.len();
            entries.retain(
                |(candidate_room, publisher_identity, _track_sid, subscriber_identity), _| {
                    candidate_room != room
                        || (publisher_identity != identity && subscriber_identity != identity)
                },
            );
            if entries.len() != size_before {
                self.bump_revision();
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MediaTrackCidStore {
    track_sids: Arc<Mutex<HashMap<MediaCidKey, String>>>,
}

impl MediaTrackCidStore {
    pub(crate) fn insert(&self, room: &str, publisher_identity: &str, cid: &str, track_sid: &str) {
        if let Ok(mut track_sids) = self.track_sids.lock() {
            track_sids.insert(
                (
                    room.to_string(),
                    publisher_identity.to_string(),
                    cid.to_string(),
                ),
                track_sid.to_string(),
            );
        }
    }

    pub(crate) fn find_track_sid(
        &self,
        room: &str,
        publisher_identity: &str,
        cid: &str,
    ) -> Option<String> {
        self.track_sids.lock().ok().and_then(|track_sids| {
            track_sids
                .get(&(
                    room.to_string(),
                    publisher_identity.to_string(),
                    cid.to_string(),
                ))
                .cloned()
        })
    }

    pub(crate) fn remove_track_sid(&self, room: &str, publisher_identity: &str, track_sid: &str) {
        if let Ok(mut track_sids) = self.track_sids.lock() {
            track_sids.retain(
                |(candidate_room, candidate_publisher, _cid), candidate_sid| {
                    candidate_room != room
                        || candidate_publisher != publisher_identity
                        || candidate_sid != track_sid
                },
            );
        }
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut track_sids) = self.track_sids.lock() {
            track_sids.retain(
                |(candidate_room, candidate_publisher, _cid), _candidate_sid| {
                    candidate_room != room || candidate_publisher != identity
                },
            );
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct PendingPublisherRemoteTrackStore {
    entries: Arc<Mutex<HashMap<PendingPublisherRemoteTrackKey, Vec<PendingPublisherRemoteTrack>>>>,
}

impl PendingPublisherRemoteTrackStore {
    pub(crate) fn enqueue(
        &self,
        room: &str,
        publisher_identity: &str,
        pending: PendingPublisherRemoteTrack,
    ) {
        if let Ok(mut entries) = self.entries.lock() {
            let queue = entries
                .entry((room.to_string(), publisher_identity.to_string()))
                .or_default();
            if queue
                .iter()
                .any(|existing| existing.remote_track_id == pending.remote_track_id)
            {
                return;
            }
            queue.push(pending);
        }
    }

    pub(crate) fn take_for_publisher(
        &self,
        room: &str,
        publisher_identity: &str,
    ) -> Vec<PendingPublisherRemoteTrack> {
        self.entries
            .lock()
            .ok()
            .and_then(|mut entries| {
                entries.remove(&(room.to_string(), publisher_identity.to_string()))
            })
            .unwrap_or_default()
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.retain(|(candidate_room, candidate_publisher), _| {
                candidate_room != room || candidate_publisher != identity
            });
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SubscriberOfferIdStore {
    ids: Arc<Mutex<HashMap<SubscriberOfferIdKey, u32>>>,
}

impl SubscriberOfferIdStore {
    pub(crate) fn next_offer_id(&self, room: &str, subscriber_identity: &str) -> u32 {
        self.ids
            .lock()
            .map(|mut ids| {
                let entry = ids
                    .entry((room.to_string(), subscriber_identity.to_string()))
                    .or_insert(0);
                *entry = entry.wrapping_add(1);
                if *entry == 0 {
                    *entry = 1;
                }
                tracing::debug!(
                    room,
                    subscriber_identity,
                    offer_id = *entry,
                    "subscriber_offer_id_next"
                );
                *entry
            })
            .unwrap_or(1)
    }

    pub(crate) fn current_offer_id(&self, room: &str, subscriber_identity: &str) -> Option<u32> {
        let current = self.ids.lock().ok().and_then(|ids| {
            ids.get(&(room.to_string(), subscriber_identity.to_string()))
                .copied()
        });
        tracing::debug!(
            room,
            subscriber_identity,
            current_offer_id = ?current,
            "subscriber_offer_id_current"
        );
        current
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut ids) = self.ids.lock() {
            let removed = ids.remove(&(room.to_string(), identity.to_string()));
            tracing::debug!(
                room,
                identity,
                removed_offer_id = ?removed,
                remaining_offer_ids = ids.len(),
                "subscriber_offer_id_remove_participant"
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriberOfferNegotiationRequest {
    Start,
    Coalesced,
}

#[derive(Debug, Clone, Copy)]
enum SubscriberOfferNegotiationState {
    Creating { pending: bool },
    InFlight { offer_id: u32, pending: bool },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SubscriberOfferNegotiationStore {
    entries: Arc<Mutex<HashMap<SubscriberOfferIdKey, SubscriberOfferNegotiationState>>>,
}

impl SubscriberOfferNegotiationStore {
    pub(crate) fn request_offer(
        &self,
        room: &str,
        subscriber_identity: &str,
    ) -> SubscriberOfferNegotiationRequest {
        let Ok(mut entries) = self.entries.lock() else {
            return SubscriberOfferNegotiationRequest::Start;
        };
        let key = (room.to_string(), subscriber_identity.to_string());
        match entries.get_mut(&key) {
            Some(state) => {
                match state {
                    SubscriberOfferNegotiationState::Creating { pending }
                    | SubscriberOfferNegotiationState::InFlight { pending, .. } => *pending = true,
                }
                tracing::debug!(
                    room,
                    subscriber_identity,
                    "subscriber_offer_negotiation_coalesced"
                );
                SubscriberOfferNegotiationRequest::Coalesced
            }
            None => {
                entries.insert(
                    key,
                    SubscriberOfferNegotiationState::Creating { pending: false },
                );
                SubscriberOfferNegotiationRequest::Start
            }
        }
    }

    pub(crate) fn mark_offer_in_flight(
        &self,
        room: &str,
        subscriber_identity: &str,
        offer_id: u32,
    ) {
        if let Ok(mut entries) = self.entries.lock() {
            let key = (room.to_string(), subscriber_identity.to_string());
            let pending = match entries.remove(&key) {
                Some(SubscriberOfferNegotiationState::Creating { pending }) => pending,
                Some(SubscriberOfferNegotiationState::InFlight { pending, .. }) => pending,
                None => false,
            };
            entries.insert(
                key,
                SubscriberOfferNegotiationState::InFlight { offer_id, pending },
            );
        }
    }

    pub(crate) fn abort_offer_creation(&self, room: &str, subscriber_identity: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            let key = (room.to_string(), subscriber_identity.to_string());
            if matches!(
                entries.get(&key),
                Some(SubscriberOfferNegotiationState::Creating { .. })
            ) {
                entries.remove(&key);
            }
        }
    }

    /// Finishes the matching offer and reports whether coalesced work must be retried.
    pub(crate) fn finish_answer(
        &self,
        room: &str,
        subscriber_identity: &str,
        offer_id: u32,
    ) -> bool {
        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };
        let key = (room.to_string(), subscriber_identity.to_string());
        let Some(SubscriberOfferNegotiationState::InFlight {
            offer_id: expected_offer_id,
            pending,
        }) = entries.get(&key).copied()
        else {
            return false;
        };
        if expected_offer_id != offer_id {
            return false;
        }
        entries.remove(&key);
        pending
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(&(room.to_string(), identity.to_string()));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingMediaSectionKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PendingMediaSectionCounts {
    pub(crate) audios: u32,
    pub(crate) videos: u32,
}

impl PendingMediaSectionCounts {
    pub(crate) fn is_empty(self) -> bool {
        self.audios == 0 && self.videos == 0
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PendingMediaSectionRequestStore {
    keys: Arc<Mutex<HashMap<MediaForwardingKey, PendingMediaSectionKind>>>,
    requested_keys: Arc<Mutex<HashSet<MediaForwardingKey>>>,
    negotiating_subscribers: Arc<Mutex<HashSet<PendingMediaSectionSubscriberKey>>>,
}

impl PendingMediaSectionRequestStore {
    pub(crate) fn insert_once(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
        kind: PendingMediaSectionKind,
    ) -> bool {
        self.keys
            .lock()
            .map(|mut keys| {
                let inserted = keys
                    .insert(
                        (
                            room.to_string(),
                            publisher_identity.to_string(),
                            track_sid.to_string(),
                            subscriber_identity.to_string(),
                        ),
                        kind,
                    )
                    .is_none();
                tracing::debug!(
                    room,
                    publisher_identity,
                    track_sid,
                    subscriber_identity,
                    kind = ?kind,
                    inserted,
                    pending_keys = keys.len(),
                    "pending_media_section_request_insert_once"
                );
                inserted
            })
            .unwrap_or(false)
    }

    pub(crate) fn remove(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) {
        let key = (
            room.to_string(),
            publisher_identity.to_string(),
            track_sid.to_string(),
            subscriber_identity.to_string(),
        );

        let removed_kind = self.keys.lock().ok().and_then(|mut keys| {
            let removed = keys.remove(&key);
            tracing::debug!(
                room,
                publisher_identity,
                track_sid,
                subscriber_identity,
                removed_kind = ?removed,
                pending_keys = keys.len(),
                "pending_media_section_request_remove_key"
            );
            removed
        });

        let Ok(mut requested_keys) = self.requested_keys.lock() else {
            tracing::warn!(
                room,
                publisher_identity,
                track_sid,
                subscriber_identity,
                "pending_media_section_request_remove_requested_lock_failed"
            );
            return;
        };

        if requested_keys.remove(&key) {
            tracing::debug!(
                room,
                publisher_identity,
                track_sid,
                subscriber_identity,
                requested_keys = requested_keys.len(),
                "pending_media_section_request_remove_requested_marker"
            );
            return;
        }

        let Some(removed_kind) = removed_kind else {
            return;
        };

        // Media section requirements are kind-level capacity, not permanently
        // bound to one track. If an offered media section is consumed by a
        // different unrequested pending track of the same kind, release one
        // matching requested marker so the still-pending track is requested
        // again after the answer.
        let Some(released_key) = self.keys.lock().ok().and_then(|keys| {
            requested_keys.iter().find_map(|requested_key| {
                let (
                    requested_room,
                    _requested_publisher,
                    _requested_track_sid,
                    requested_subscriber,
                ) = requested_key;
                if requested_room == room
                    && requested_subscriber == subscriber_identity
                    && keys.get(requested_key) == Some(&removed_kind)
                {
                    Some(requested_key.clone())
                } else {
                    None
                }
            })
        }) else {
            return;
        };

        requested_keys.remove(&released_key);
        tracing::debug!(
            room,
            subscriber_identity,
            removed_kind = ?removed_kind,
            released_key = ?released_key,
            requested_keys = requested_keys.len(),
            "pending_media_section_request_released_same_kind_request_marker"
        );
    }

    pub(crate) fn take_unrequested_counts(
        &self,
        room: &str,
        subscriber_identity: &str,
    ) -> PendingMediaSectionCounts {
        let pending_for_subscriber = self.pending_keys_for_subscriber(room, subscriber_identity);

        let Ok(mut requested_keys) = self.requested_keys.lock() else {
            return PendingMediaSectionCounts::default();
        };

        let mut counts = PendingMediaSectionCounts::default();
        let mut marked_requested = Vec::new();
        for (key, kind) in pending_for_subscriber {
            if requested_keys.insert(key.clone()) {
                marked_requested.push((key, kind));
                match kind {
                    PendingMediaSectionKind::Audio => counts.audios += 1,
                    PendingMediaSectionKind::Video => counts.videos += 1,
                }
            }
        }

        tracing::debug!(
            room,
            subscriber_identity,
            counts = ?counts,
            marked_requested = ?marked_requested,
            requested_keys = requested_keys.len(),
            "pending_media_section_request_take_unrequested_counts"
        );

        counts
    }

    pub(crate) fn begin_negotiation_if_idle(&self, room: &str, subscriber_identity: &str) -> bool {
        self.negotiating_subscribers
            .lock()
            .map(|mut subscribers| {
                let inserted =
                    subscribers.insert((room.to_string(), subscriber_identity.to_string()));
                tracing::debug!(
                    room,
                    subscriber_identity,
                    inserted,
                    negotiating_subscribers = subscribers.len(),
                    "pending_media_section_request_begin_negotiation_if_idle"
                );
                inserted
            })
            .unwrap_or(false)
    }

    /// Releases in-flight request markers for media sections that remain unresolved.
    ///
    /// A client may send a publish offer that omits a previously requested receive
    /// section when publishing and subscribing are negotiated concurrently. Those
    /// pending tracks need another `MediaSectionsRequirement` after that answer.
    pub(crate) fn release_requested_for_unresolved(&self, room: &str, subscriber_identity: &str) {
        let unresolved = self.pending_keys_for_subscriber(room, subscriber_identity);
        if unresolved.is_empty() {
            return;
        }

        let unresolved_keys = unresolved
            .into_iter()
            .map(|(key, _kind)| key)
            .collect::<HashSet<_>>();
        let Ok(mut requested_keys) = self.requested_keys.lock() else {
            return;
        };
        let before = requested_keys.len();
        requested_keys.retain(|key| !unresolved_keys.contains(key));
        tracing::debug!(
            room,
            subscriber_identity,
            released_requested_keys = before.saturating_sub(requested_keys.len()),
            requested_keys = requested_keys.len(),
            "pending_media_section_request_release_unresolved_requested_markers"
        );
    }

    pub(crate) fn clear_negotiation(&self, room: &str, subscriber_identity: &str) {
        let subscriber_key = (room.to_string(), subscriber_identity.to_string());
        if let Ok(mut subscribers) = self.negotiating_subscribers.lock() {
            let removed = subscribers.remove(&subscriber_key);
            tracing::debug!(
                room,
                subscriber_identity,
                removed,
                negotiating_subscribers = subscribers.len(),
                "pending_media_section_request_clear_negotiation"
            );
        }
        if let Ok(mut requested_keys) = self.requested_keys.lock() {
            let before = requested_keys.len();
            requested_keys.retain(
                |(candidate_room, _publisher_identity, _track_sid, candidate_subscriber)| {
                    candidate_room != room || candidate_subscriber != subscriber_identity
                },
            );
            tracing::debug!(
                room,
                subscriber_identity,
                removed_requested_keys = before.saturating_sub(requested_keys.len()),
                requested_keys = requested_keys.len(),
                "pending_media_section_request_clear_requested_keys"
            );
        }
    }

    pub(crate) fn clear_negotiation_if_no_pending(&self, room: &str, subscriber_identity: &str) {
        if self.has_for_subscriber(room, subscriber_identity) {
            return;
        }
        self.clear_negotiation(room, subscriber_identity);
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut keys) = self.keys.lock() {
            keys.retain(
                |(candidate_room, publisher_identity, _track_sid, subscriber_identity), _kind| {
                    candidate_room != room
                        || (publisher_identity != identity && subscriber_identity != identity)
                },
            );
        }
        if let Ok(mut requested_keys) = self.requested_keys.lock() {
            requested_keys.retain(
                |(candidate_room, publisher_identity, _track_sid, subscriber_identity)| {
                    candidate_room != room
                        || (publisher_identity != identity && subscriber_identity != identity)
                },
            );
        }
        if let Ok(mut subscribers) = self.negotiating_subscribers.lock() {
            subscribers.retain(|(candidate_room, subscriber_identity)| {
                candidate_room != room || subscriber_identity != identity
            });
        }
    }

    pub(crate) fn contains(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> bool {
        self.keys
            .lock()
            .map(|keys| {
                keys.contains_key(&(
                    room.to_string(),
                    publisher_identity.to_string(),
                    track_sid.to_string(),
                    subscriber_identity.to_string(),
                ))
            })
            .unwrap_or(false)
    }

    pub(crate) fn has_for_subscriber(&self, room: &str, subscriber_identity: &str) -> bool {
        !self
            .pending_counts_for_subscriber(room, subscriber_identity)
            .is_empty()
    }

    fn pending_keys_for_subscriber(
        &self,
        room: &str,
        subscriber_identity: &str,
    ) -> Vec<(MediaForwardingKey, PendingMediaSectionKind)> {
        self.keys
            .lock()
            .map(|keys| {
                keys.iter()
                    .filter_map(
                        |(
                            key @ (
                                candidate_room,
                                _publisher_identity,
                                _track_sid,
                                candidate_subscriber,
                            ),
                            kind,
                        )| {
                            if candidate_room == room && candidate_subscriber == subscriber_identity
                            {
                                Some((key.clone(), *kind))
                            } else {
                                None
                            }
                        },
                    )
                    .collect()
            })
            .unwrap_or_default()
    }

    fn pending_counts_for_subscriber(
        &self,
        room: &str,
        subscriber_identity: &str,
    ) -> PendingMediaSectionCounts {
        self.pending_keys_for_subscriber(room, subscriber_identity)
            .into_iter()
            .fold(
                PendingMediaSectionCounts::default(),
                |mut counts, (_key, kind)| {
                    match kind {
                        PendingMediaSectionKind::Audio => counts.audios += 1,
                        PendingMediaSectionKind::Video => counts.videos += 1,
                    }
                    counts
                },
            )
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MediaForwardingStore {
    keys: Arc<Mutex<HashSet<MediaForwardingKey>>>,
}

#[cfg(test)]
mod tests {
    use super::{
        MediaSubscriptionStore, PendingMediaSectionCounts, PendingMediaSectionKind,
        PendingMediaSectionRequestStore, SubscriberOfferIdStore, SubscriberOfferNegotiationRequest,
        SubscriberOfferNegotiationStore,
    };

    #[test]
    fn subscriber_offer_id_store_increments_and_resets() {
        let store = SubscriberOfferIdStore::default();
        assert_eq!(store.current_offer_id("room", "alice"), None);
        assert_eq!(store.next_offer_id("room", "alice"), 1);
        assert_eq!(store.next_offer_id("room", "alice"), 2);
        assert_eq!(store.current_offer_id("room", "alice"), Some(2));
        store.remove_participant("room", "alice");
        assert_eq!(store.current_offer_id("room", "alice"), None);
    }

    #[test]
    fn subscriber_offer_negotiation_coalesces_until_matching_answer() {
        let store = SubscriberOfferNegotiationStore::default();

        assert_eq!(
            store.request_offer("room", "alice"),
            SubscriberOfferNegotiationRequest::Start
        );
        assert_eq!(
            store.request_offer("room", "alice"),
            SubscriberOfferNegotiationRequest::Coalesced
        );
        store.mark_offer_in_flight("room", "alice", 1);
        assert!(
            !store.finish_answer("room", "alice", 2),
            "a stale answer must not clear the outstanding offer"
        );
        assert!(
            store.finish_answer("room", "alice", 1),
            "the matching answer must request one coalesced follow-up"
        );
        assert_eq!(
            store.request_offer("room", "alice"),
            SubscriberOfferNegotiationRequest::Start
        );
        store.abort_offer_creation("room", "alice");
        assert_eq!(
            store.request_offer("room", "alice"),
            SubscriberOfferNegotiationRequest::Start
        );
        store.remove_participant("room", "alice");
        assert_eq!(
            store.request_offer("room", "alice"),
            SubscriberOfferNegotiationRequest::Start
        );
    }

    #[test]
    fn media_subscription_store_revision_advances_on_mutation() {
        let store = MediaSubscriptionStore::default();
        let initial = store.revision();

        store.set_subscribed("room", "pub", "TR_1", "sub", true);
        let after_set = store.revision();
        assert!(after_set > initial);

        store.remove_participant("room", "sub");
        let after_remove = store.revision();
        assert!(after_remove > after_set);
    }

    #[test]
    fn pending_media_section_request_store_contains_roundtrip() {
        let store = PendingMediaSectionRequestStore::default();
        assert!(!store.contains("room", "pub", "TR_1", "sub"));

        assert!(store.insert_once("room", "pub", "TR_1", "sub", PendingMediaSectionKind::Audio));
        assert!(store.contains("room", "pub", "TR_1", "sub"));

        store.remove("room", "pub", "TR_1", "sub");
        assert!(!store.contains("room", "pub", "TR_1", "sub"));
    }

    #[test]
    fn pending_media_section_request_store_tracks_any_pending_for_subscriber() {
        let store = PendingMediaSectionRequestStore::default();
        assert!(!store.has_for_subscriber("room", "sub"));

        assert!(store.insert_once(
            "room",
            "pub-a",
            "TR_audio",
            "sub",
            PendingMediaSectionKind::Audio
        ));
        assert!(store.has_for_subscriber("room", "sub"));

        assert!(store.insert_once(
            "room",
            "pub-b",
            "TR_video",
            "sub",
            PendingMediaSectionKind::Video
        ));
        assert!(store.has_for_subscriber("room", "sub"));

        store.remove("room", "pub-a", "TR_audio", "sub");
        assert!(store.has_for_subscriber("room", "sub"));

        store.remove("room", "pub-b", "TR_video", "sub");
        assert!(!store.has_for_subscriber("room", "sub"));
    }

    #[test]
    fn pending_media_section_request_store_clears_negotiation_only_after_pending_requests_drain() {
        let store = PendingMediaSectionRequestStore::default();

        assert!(store.insert_once(
            "room",
            "pub",
            "TR_audio",
            "sub",
            PendingMediaSectionKind::Audio
        ));
        assert!(store.begin_negotiation_if_idle("room", "sub"));

        store.clear_negotiation_if_no_pending("room", "sub");
        assert!(!store.begin_negotiation_if_idle("room", "sub"));

        store.remove("room", "pub", "TR_audio", "sub");
        store.clear_negotiation_if_no_pending("room", "sub");
        assert!(store.begin_negotiation_if_idle("room", "sub"));
    }

    #[test]
    fn pending_media_section_request_store_reports_unrequested_deltas_by_kind() {
        let store = PendingMediaSectionRequestStore::default();

        assert!(store.insert_once(
            "room",
            "pub-a",
            "TR_audio_a",
            "sub",
            PendingMediaSectionKind::Audio,
        ));
        assert_eq!(
            store.take_unrequested_counts("room", "sub"),
            PendingMediaSectionCounts {
                audios: 1,
                videos: 0,
            }
        );
        assert_eq!(
            store.take_unrequested_counts("room", "sub"),
            PendingMediaSectionCounts::default()
        );

        assert!(store.insert_once(
            "room",
            "pub-b",
            "TR_video_b",
            "sub",
            PendingMediaSectionKind::Video,
        ));
        assert!(store.insert_once(
            "room",
            "pub-c",
            "TR_audio_c",
            "sub",
            PendingMediaSectionKind::Audio,
        ));
        assert_eq!(
            store.take_unrequested_counts("room", "sub"),
            PendingMediaSectionCounts {
                audios: 1,
                videos: 1,
            }
        );

        store.remove("room", "pub-a", "TR_audio_a", "sub");
        store.remove("room", "pub-b", "TR_video_b", "sub");
        store.remove("room", "pub-c", "TR_audio_c", "sub");
        store.clear_negotiation_if_no_pending("room", "sub");

        assert!(store.insert_once(
            "room",
            "pub-d",
            "TR_video_d",
            "sub",
            PendingMediaSectionKind::Video,
        ));
        assert_eq!(
            store.take_unrequested_counts("room", "sub"),
            PendingMediaSectionCounts {
                audios: 0,
                videos: 1,
            }
        );
    }

    #[test]
    fn pending_media_section_request_store_reissues_requirement_when_pending_track_replaced() {
        let store = PendingMediaSectionRequestStore::default();

        assert!(store.insert_once(
            "room",
            "pub-old",
            "TR_audio_old",
            "sub",
            PendingMediaSectionKind::Audio,
        ));
        assert_eq!(
            store.take_unrequested_counts("room", "sub"),
            PendingMediaSectionCounts {
                audios: 1,
                videos: 0,
            }
        );

        // Simulate churn: old pending request is removed and replaced with a new
        // track of the same media kind while negotiation is still in progress.
        store.remove("room", "pub-old", "TR_audio_old", "sub");
        assert!(store.insert_once(
            "room",
            "pub-new",
            "TR_audio_new",
            "sub",
            PendingMediaSectionKind::Audio,
        ));

        assert_eq!(
            store.take_unrequested_counts("room", "sub"),
            PendingMediaSectionCounts {
                audios: 1,
                videos: 0,
            }
        );
    }

    #[test]
    fn pending_media_section_request_store_reissues_when_offered_section_consumes_other_track() {
        let store = PendingMediaSectionRequestStore::default();

        assert!(store.insert_once(
            "room",
            "pub-requested",
            "TR_video_requested",
            "sub",
            PendingMediaSectionKind::Video,
        ));
        assert_eq!(
            store.take_unrequested_counts("room", "sub"),
            PendingMediaSectionCounts {
                audios: 0,
                videos: 1,
            }
        );

        assert!(store.insert_once(
            "room",
            "pub-other",
            "TR_video_other",
            "sub",
            PendingMediaSectionKind::Video,
        ));

        // The incoming offer only provides generic video capacity. If that
        // section is attached to a different video track than the originally
        // requested key, the still-pending requested video must be eligible for
        // another media-section requirement.
        store.remove("room", "pub-other", "TR_video_other", "sub");

        assert_eq!(
            store.take_unrequested_counts("room", "sub"),
            PendingMediaSectionCounts {
                audios: 0,
                videos: 1,
            }
        );
    }
}

impl MediaForwardingStore {
    pub(crate) fn contains(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> bool {
        self.keys
            .lock()
            .map(|keys| {
                keys.contains(&(
                    room.to_string(),
                    publisher_identity.to_string(),
                    track_sid.to_string(),
                    subscriber_identity.to_string(),
                ))
            })
            .unwrap_or(false)
    }

    pub(crate) fn insert_once(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) -> bool {
        self.keys
            .lock()
            .map(|mut keys| {
                keys.insert((
                    room.to_string(),
                    publisher_identity.to_string(),
                    track_sid.to_string(),
                    subscriber_identity.to_string(),
                ))
            })
            .unwrap_or(false)
    }

    pub(crate) fn remove(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) {
        if let Ok(mut keys) = self.keys.lock() {
            keys.remove(&(
                room.to_string(),
                publisher_identity.to_string(),
                track_sid.to_string(),
                subscriber_identity.to_string(),
            ));
        }
    }

    pub(crate) fn remove_track(&self, room: &str, publisher_identity: &str, track_sid: &str) {
        if let Ok(mut keys) = self.keys.lock() {
            keys.retain(
                |(
                    candidate_room,
                    candidate_publisher,
                    candidate_track_sid,
                    _subscriber_identity,
                )| {
                    candidate_room != room
                        || candidate_publisher != publisher_identity
                        || candidate_track_sid != track_sid
                },
            );
        }
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut keys) = self.keys.lock() {
            keys.retain(
                |(candidate_room, publisher_identity, _track_sid, subscriber_identity)| {
                    candidate_room != room
                        || (publisher_identity != identity && subscriber_identity != identity)
                },
            );
        }
    }
}

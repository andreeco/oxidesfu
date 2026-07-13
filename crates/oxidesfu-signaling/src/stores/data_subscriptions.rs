use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use livekit_protocol as proto;
use oxidesfu_room::RoomStore;

use crate::stores::DataTrackStore;

type DataTrackSubscriptionKey = (String, String, String);

#[derive(Debug, Clone)]
struct DataTrackSubscription {
    publisher_identity: String,
    publisher_sid: String,
    track_sid: String,
    pub_handle: u32,
    sub_handle: u32,
    options: Option<proto::DataTrackSubscriptionOptions>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DataTrackSubscriptionStore {
    subscriptions: Arc<Mutex<HashMap<DataTrackSubscriptionKey, DataTrackSubscription>>>,
    next_handle: Arc<Mutex<u32>>,
}

impl DataTrackSubscriptionStore {
    pub(crate) fn update(
        &self,
        room: &str,
        subscriber_identity: &str,
        update: proto::UpdateDataSubscription,
        data_tracks: &DataTrackStore,
        rooms: &RoomStore,
    ) -> proto::DataTrackSubscriberHandles {
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            for update in update.updates {
                let key = (
                    room.to_string(),
                    subscriber_identity.to_string(),
                    update.track_sid.clone(),
                );
                if !update.subscribe {
                    subscriptions.remove(&key);
                    continue;
                }
                if let Some(subscription) = subscriptions.get_mut(&key) {
                    subscription.options = update.options;
                    continue;
                }
                let Some((publisher_identity, track)) =
                    data_tracks.find_by_sid(room, &update.track_sid)
                else {
                    continue;
                };
                let options = update.options;
                let track_sid = update.track_sid;
                let pub_handle = track.pub_handle;
                let publisher_sid = rooms
                    .get_participant(room, &publisher_identity)
                    .map(|participant| participant.sid)
                    .unwrap_or_default();
                let Some(sub_handle) = self.allocate_handle() else {
                    continue;
                };
                subscriptions.insert(
                    key,
                    DataTrackSubscription {
                        publisher_identity,
                        publisher_sid,
                        track_sid,
                        pub_handle,
                        sub_handle,
                        options,
                    },
                );
            }
        }

        self.handles_for_subscriber(room, subscriber_identity)
    }

    pub(crate) fn handles_for_subscriber(
        &self,
        room: &str,
        subscriber_identity: &str,
    ) -> proto::DataTrackSubscriberHandles {
        let sub_handles = self
            .subscriptions
            .lock()
            .map(|subscriptions| {
                subscriptions
                    .iter()
                    .filter(|((subscription_room, identity, _), _)| {
                        subscription_room == room && identity == subscriber_identity
                    })
                    .map(|(_, subscription)| {
                        (
                            subscription.sub_handle,
                            proto::data_track_subscriber_handles::PublishedDataTrack {
                                publisher_identity: subscription.publisher_identity.clone(),
                                publisher_sid: subscription.publisher_sid.clone(),
                                track_sid: subscription.track_sid.clone(),
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        proto::DataTrackSubscriberHandles { sub_handles }
    }

    pub(crate) fn subscribers_for_packet_with_publisher_sid(
        &self,
        room: &str,
        publisher_identity: &str,
        publisher_sid: &str,
        pub_handle: u32,
    ) -> Vec<(String, u32)> {
        self.subscriptions
            .lock()
            .map(|subscriptions| {
                subscriptions
                    .iter()
                    .filter(|((subscription_room, _, _), subscription)| {
                        subscription_room == room
                            && subscription.publisher_identity == publisher_identity
                            && subscription.publisher_sid == publisher_sid
                            && subscription.pub_handle == pub_handle
                    })
                    .map(|((_, subscriber_identity, _), subscription)| {
                        (subscriber_identity.clone(), subscription.sub_handle)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn subscribers_for_packet(
        &self,
        room: &str,
        publisher_identity: &str,
        pub_handle: u32,
    ) -> Vec<(String, u32)> {
        self.subscriptions
            .lock()
            .map(|subscriptions| {
                subscriptions
                    .iter()
                    .filter(|((subscription_room, _, _), subscription)| {
                        subscription_room == room
                            && subscription.publisher_identity == publisher_identity
                            && subscription.pub_handle == pub_handle
                    })
                    .map(|((_, subscriber_identity, _), subscription)| {
                        (subscriber_identity.clone(), subscription.sub_handle)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn options_for_track(
        &self,
        room: &str,
        subscriber_identity: &str,
        track_sid: &str,
    ) -> Option<proto::DataTrackSubscriptionOptions> {
        self.subscriptions.lock().ok().and_then(|subscriptions| {
            subscriptions
                .get(&(
                    room.to_string(),
                    subscriber_identity.to_string(),
                    track_sid.to_string(),
                ))
                .and_then(|subscription| subscription.options.clone())
        })
    }

    pub(crate) fn revoke_disallowed_subscribers(
        &self,
        room: &str,
        publisher_identity: &str,
        pub_handle: u32,
        allowed_subscriber_identities: &[String],
        rooms: &RoomStore,
    ) -> Vec<String> {
        let allowed = allowed_subscriber_identities
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();

        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            let mut revoked = Vec::new();
            subscriptions.retain(
                |(subscription_room, subscriber_identity, _), subscription| {
                    if subscription_room != room
                        || subscription.publisher_identity != publisher_identity
                        || subscription.pub_handle != pub_handle
                    {
                        return true;
                    }

                    let is_permission_exempt = rooms
                        .get_participant(room, subscriber_identity)
                        .map(|participant| {
                            participant.kind == proto::participant_info::Kind::Egress as i32
                        })
                        .unwrap_or(false);
                    if is_permission_exempt {
                        return true;
                    }

                    if allowed.contains(subscriber_identity.as_str()) {
                        return true;
                    }

                    revoked.push(subscriber_identity.clone());
                    false
                },
            );
            revoked.sort();
            revoked.dedup();
            return revoked;
        }

        Vec::new()
    }

    pub(crate) fn remove_published_track(
        &self,
        room: &str,
        publisher_identity: &str,
        pub_handle: u32,
    ) {
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            subscriptions.retain(|(subscription_room, _, _), subscription| {
                subscription_room != room
                    || subscription.publisher_identity != publisher_identity
                    || subscription.pub_handle != pub_handle
            });
        }
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            subscriptions.retain(
                |(subscription_room, subscriber_identity, _), subscription| {
                    subscription_room != room
                        || (subscriber_identity != identity
                            && subscription.publisher_identity != identity)
                },
            );
        }
    }

    fn allocate_handle(&self) -> Option<u32> {
        let mut next = self.next_handle.lock().ok()?;
        *next = next.saturating_add(1);
        if *next == 0 || *next > u16::MAX as u32 {
            return None;
        }
        Some(*next)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use livekit_protocol as proto;
    use oxidesfu_room::RoomStore;

    use super::{DataTrackStore, DataTrackSubscriptionStore};

    fn subscribe_track(
        subscriptions: &DataTrackSubscriptionStore,
        room: &str,
        subscriber: &str,
        track_sid: &str,
        data_tracks: &DataTrackStore,
        rooms: &RoomStore,
    ) {
        let _ = subscriptions.update(
            room,
            subscriber,
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: track_sid.to_string(),
                    subscribe: true,
                    options: None,
                }],
            },
            data_tracks,
            rooms,
        );
    }

    #[test]
    fn subscribers_for_packet_filters_stale_publisher_sid() {
        let room_store = RoomStore::default();
        let room = "data-track-publisher-sid-filter-room";
        let publisher = "pub";
        let subscriber = "sub";

        room_store
            .create_room(proto::CreateRoomRequest {
                name: room.to_string(),
                ..Default::default()
            })
            .expect("room should be created");
        room_store
            .join_participant(room, publisher, publisher, String::new(), HashMap::new())
            .expect("publisher should join room");
        room_store
            .join_participant(room, subscriber, subscriber, String::new(), HashMap::new())
            .expect("subscriber should join room");

        let current_publisher_sid = room_store
            .get_participant(room, publisher)
            .expect("publisher should be present")
            .sid;

        let data_tracks = DataTrackStore::default();
        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 7,
                    name: "track".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let subscriptions = DataTrackSubscriptionStore::default();
        subscribe_track(
            &subscriptions,
            room,
            subscriber,
            &published.sid,
            &data_tracks,
            &room_store,
        );

        let current = subscriptions.subscribers_for_packet_with_publisher_sid(
            room,
            publisher,
            &current_publisher_sid,
            published.pub_handle,
        );
        assert_eq!(
            current.len(),
            1,
            "current publisher SID should route packet"
        );

        let stale = subscriptions.subscribers_for_packet_with_publisher_sid(
            room,
            publisher,
            "PA_stale_sid",
            published.pub_handle,
        );
        assert!(
            stale.is_empty(),
            "stale publisher SID should not route packet to subscribers"
        );
    }

    #[test]
    fn revoke_disallowed_subscribers_keeps_allowed_and_egress() {
        let room_store = RoomStore::default();
        let room = "data-track-revoke-room";
        let publisher = "pub";
        let allowed = "allowed";
        let disallowed = "disallowed";
        let recorder = "recorder";

        room_store
            .create_room(proto::CreateRoomRequest {
                name: room.to_string(),
                ..Default::default()
            })
            .expect("room should be created");

        for identity in [publisher, allowed, disallowed, recorder] {
            room_store
                .join_participant(room, identity, identity, String::new(), HashMap::new())
                .expect("participant should join room");
        }

        room_store
            .set_participant_kind(
                room,
                recorder,
                proto::participant_info::Kind::Egress as i32,
                Vec::new(),
            )
            .expect("recorder participant kind should be set");

        let data_tracks = DataTrackStore::default();
        let published = data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 1,
                    name: "test".to_string(),
                    ..Default::default()
                },
            )
            .expect("data track should publish");

        let subscriptions = DataTrackSubscriptionStore::default();
        subscribe_track(
            &subscriptions,
            room,
            allowed,
            &published.sid,
            &data_tracks,
            &room_store,
        );
        subscribe_track(
            &subscriptions,
            room,
            disallowed,
            &published.sid,
            &data_tracks,
            &room_store,
        );
        subscribe_track(
            &subscriptions,
            room,
            recorder,
            &published.sid,
            &data_tracks,
            &room_store,
        );

        let revoked = subscriptions.revoke_disallowed_subscribers(
            room,
            publisher,
            published.pub_handle,
            &[allowed.to_string()],
            &room_store,
        );

        assert_eq!(revoked, vec![disallowed.to_string()]);
        assert!(
            subscriptions
                .handles_for_subscriber(room, disallowed)
                .sub_handles
                .is_empty()
        );
        assert!(
            !subscriptions
                .handles_for_subscriber(room, allowed)
                .sub_handles
                .is_empty()
        );
        assert!(
            !subscriptions
                .handles_for_subscriber(room, recorder)
                .sub_handles
                .is_empty()
        );
    }
}

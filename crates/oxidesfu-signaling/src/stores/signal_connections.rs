use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::router::OutboundSignalSender;

type SignalConnectionKey = (String, String);

#[derive(Debug, Clone, Default)]
pub(crate) struct SignalConnectionStore {
    senders: Arc<Mutex<HashMap<SignalConnectionKey, OutboundSignalSender>>>,
}

impl SignalConnectionStore {
    pub(crate) fn insert(&self, room: &str, identity: &str, sender: OutboundSignalSender) {
        if let Ok(mut senders) = self.senders.lock() {
            senders.insert((room.to_string(), identity.to_string()), sender);
        }
    }

    pub(crate) fn get(&self, room: &str, identity: &str) -> Option<OutboundSignalSender> {
        self.senders.lock().ok().and_then(|senders| {
            senders
                .get(&(room.to_string(), identity.to_string()))
                .cloned()
        })
    }

    pub(crate) fn is_same(
        &self,
        room: &str,
        identity: &str,
        sender: &OutboundSignalSender,
    ) -> bool {
        let Ok(senders) = self.senders.lock() else {
            return false;
        };
        senders
            .get(&(room.to_string(), identity.to_string()))
            .is_some_and(|existing| existing.same_channel(sender))
    }

    pub(crate) fn remove_if_same(
        &self,
        room: &str,
        identity: &str,
        sender: &OutboundSignalSender,
    ) -> bool {
        let Ok(mut senders) = self.senders.lock() else {
            return false;
        };
        let key = (room.to_string(), identity.to_string());
        let should_remove = senders
            .get(&key)
            .is_some_and(|existing| existing.same_channel(sender));
        if should_remove {
            senders.remove(&key);
            true
        } else {
            false
        }
    }
}

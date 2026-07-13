use std::sync::{Arc, Mutex};

use livekit_protocol as proto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DataChannelMessage {
    pub(crate) room: String,
    pub(crate) identity: String,
    pub(crate) text: String,
    pub(crate) user_payload: Option<Vec<u8>>,
    pub(crate) topic: Option<String>,
    pub(crate) kind: i32,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DataChannelMessageStore {
    messages: Arc<Mutex<Vec<DataChannelMessage>>>,
}

impl DataChannelMessageStore {
    pub(crate) fn push_text(&self, room: &str, identity: &str, text: String) {
        if let Ok(mut messages) = self.messages.lock() {
            messages.push(DataChannelMessage {
                room: room.to_string(),
                identity: identity.to_string(),
                text,
                user_payload: None,
                topic: None,
                kind: proto::data_packet::Kind::Reliable as i32,
            });
        }
    }

    #[allow(deprecated)]
    pub(crate) fn push_data_packet(&self, room: &str, identity: &str, packet: proto::DataPacket) {
        let Some(proto::data_packet::Value::User(user)) = packet.value else {
            return;
        };
        if let Ok(mut messages) = self.messages.lock() {
            messages.push(DataChannelMessage {
                room: room.to_string(),
                identity: identity.to_string(),
                text: String::from_utf8_lossy(&user.payload).into_owned(),
                user_payload: Some(user.payload),
                topic: user.topic,
                kind: packet.kind,
            });
        }
    }

    #[cfg(test)]
    pub(crate) fn last(&self) -> Option<DataChannelMessage> {
        self.messages
            .lock()
            .ok()
            .and_then(|messages| messages.last().cloned())
    }
}

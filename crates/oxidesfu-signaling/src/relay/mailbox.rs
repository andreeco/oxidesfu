use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oxidesfu_room::{RedisHashStore, RoomNodeRegistryError};
use serde::{Deserialize, Serialize};

use super::intents::{
    NonLocalRelayJoinIntent, NonLocalRelayJoinResponse, NonLocalRelayOutboundSignalQuery,
    NonLocalRelayRoomServiceIntent, NonLocalRelayRoomServiceResponse,
    NonLocalRelaySessionTerminationIntent, NonLocalRelaySignalRequestIntent,
    NonLocalRelaySignalRequestResponse, RelayIntentReceipt,
};

const REDIS_RELAY_INTENTS_KEY: &str = "oxidesfu:relay_join:intents";
const REDIS_RELAY_RESPONSES_KEY: &str = "oxidesfu:relay_join:responses";
const REDIS_RELAY_TERMINATION_INTENTS_KEY: &str = "oxidesfu:relay_termination:intents";
const REDIS_RELAY_SIGNAL_INTENTS_KEY: &str = "oxidesfu:relay_signal:intents";
const REDIS_RELAY_SIGNAL_RESPONSES_KEY: &str = "oxidesfu:relay_signal:responses";
const REDIS_RELAY_ROOM_SERVICE_INTENTS_KEY: &str = "oxidesfu:relay_room_service:intents";
const REDIS_RELAY_ROOM_SERVICE_RESPONSES_KEY: &str = "oxidesfu:relay_room_service:responses";
const REDIS_RELAY_OUTBOUND_SIGNAL_RESPONSES_KEY: &str = "oxidesfu:relay_signal:outbound_responses";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRelayIntent {
    intent_id: String,
    intent: NonLocalRelayJoinIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRelayTerminationIntent {
    intent_id: String,
    intent: NonLocalRelaySessionTerminationIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRelaySignalRequestIntent {
    intent_id: String,
    intent: NonLocalRelaySignalRequestIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRelayRoomServiceIntent {
    intent_id: String,
    intent: NonLocalRelayRoomServiceIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRelayOutboundSignalResponse {
    event_id: String,
    room: String,
    identity: String,
    selected_room_node_id: String,
    signal_response: Vec<u8>,
}

/// Hash-backed relay mailbox adapter that can be backed by Redis hash operations.
#[derive(Debug, Clone)]
pub struct RedisRelayMailbox<S> {
    store: S,
    intents_key: &'static str,
    responses_key: &'static str,
    termination_intents_key: &'static str,
    signal_intents_key: &'static str,
    signal_responses_key: &'static str,
    room_service_intents_key: &'static str,
    room_service_responses_key: &'static str,
    outbound_signal_responses_key: &'static str,
    next_intent_id: Arc<std::sync::atomic::AtomicU64>,
}

impl<S> RedisRelayMailbox<S>
where
    S: RedisHashStore,
{
    fn next_unique_id(&self, prefix: &str) -> String {
        let seq = self
            .next_intent_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let pid = std::process::id();
        format!("{prefix}-{now_nanos:032x}-{pid:08x}-{seq:016x}")
    }

    /// Creates a hash-backed relay mailbox adapter with default key names.
    pub fn with_store(store: S) -> Self {
        Self {
            store,
            intents_key: REDIS_RELAY_INTENTS_KEY,
            responses_key: REDIS_RELAY_RESPONSES_KEY,
            termination_intents_key: REDIS_RELAY_TERMINATION_INTENTS_KEY,
            signal_intents_key: REDIS_RELAY_SIGNAL_INTENTS_KEY,
            signal_responses_key: REDIS_RELAY_SIGNAL_RESPONSES_KEY,
            room_service_intents_key: REDIS_RELAY_ROOM_SERVICE_INTENTS_KEY,
            room_service_responses_key: REDIS_RELAY_ROOM_SERVICE_RESPONSES_KEY,
            outbound_signal_responses_key: REDIS_RELAY_OUTBOUND_SIGNAL_RESPONSES_KEY,
            next_intent_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Dispatches and stores a relay intent and returns its correlation receipt.
    pub fn dispatch_intent(
        &self,
        intent: &NonLocalRelayJoinIntent,
    ) -> Result<RelayIntentReceipt, RoomNodeRegistryError> {
        let intent_id = self.next_unique_id("relay-intent");
        let encoded = serde_json::to_string(&StoredRelayIntent {
            intent_id: intent_id.clone(),
            intent: intent.clone(),
        })
        .map_err(|err| RoomNodeRegistryError::Backend {
            message: format!("failed to encode relay intent: {err}"),
        })?;
        self.store.hset(self.intents_key, &intent_id, &encoded)?;
        Ok(RelayIntentReceipt { intent_id })
    }

    /// Dispatches and stores a relay termination intent.
    pub fn dispatch_termination_intent(
        &self,
        intent: &NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), RoomNodeRegistryError> {
        let intent_id = self.next_unique_id("relay-term-intent");
        let encoded = serde_json::to_string(&StoredRelayTerminationIntent {
            intent_id: intent_id.clone(),
            intent: intent.clone(),
        })
        .map_err(|err| RoomNodeRegistryError::Backend {
            message: format!("failed to encode relay termination intent: {err}"),
        })?;
        self.store
            .hset(self.termination_intents_key, &intent_id, &encoded)
    }

    /// Dispatches and stores a relay signal request intent and returns its correlation receipt.
    pub fn dispatch_signal_request_intent(
        &self,
        intent: &NonLocalRelaySignalRequestIntent,
    ) -> Result<RelayIntentReceipt, RoomNodeRegistryError> {
        let intent_id = self.next_unique_id("relay-signal-intent");
        let encoded = serde_json::to_string(&StoredRelaySignalRequestIntent {
            intent_id: intent_id.clone(),
            intent: intent.clone(),
        })
        .map_err(|err| RoomNodeRegistryError::Backend {
            message: format!("failed to encode relay signal request intent: {err}"),
        })?;
        self.store
            .hset(self.signal_intents_key, &intent_id, &encoded)?;
        Ok(RelayIntentReceipt { intent_id })
    }

    /// Dispatches and stores a relay RoomService intent and returns its correlation receipt.
    pub fn dispatch_room_service_intent(
        &self,
        intent: &NonLocalRelayRoomServiceIntent,
    ) -> Result<RelayIntentReceipt, RoomNodeRegistryError> {
        let intent_id = self.next_unique_id("relay-room-service-intent");
        let encoded = serde_json::to_string(&StoredRelayRoomServiceIntent {
            intent_id: intent_id.clone(),
            intent: intent.clone(),
        })
        .map_err(|err| RoomNodeRegistryError::Backend {
            message: format!("failed to encode relay room service intent: {err}"),
        })?;
        self.store
            .hset(self.room_service_intents_key, &intent_id, &encoded)?;
        Ok(RelayIntentReceipt { intent_id })
    }

    /// Claims the earliest pending relay intent targeted at `selected_room_node_id`.
    pub fn claim_next_intent_for_node(
        &self,
        selected_room_node_id: &str,
    ) -> Result<Option<(RelayIntentReceipt, NonLocalRelayJoinIntent)>, RoomNodeRegistryError> {
        let mut candidates = self
            .store
            .hvals(self.intents_key)?
            .into_iter()
            .map(|encoded| {
                serde_json::from_str::<StoredRelayIntent>(&encoded).map_err(|err| {
                    RoomNodeRegistryError::Backend {
                        message: format!("failed to decode relay intent: {err}"),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        candidates
            .retain(|candidate| candidate.intent.selected_room_node_id == selected_room_node_id);
        candidates.sort_by(|left, right| left.intent_id.cmp(&right.intent_id));

        let Some(claimed) = candidates.into_iter().next() else {
            return Ok(None);
        };

        self.store.hdel(self.intents_key, &claimed.intent_id)?;
        Ok(Some((
            RelayIntentReceipt {
                intent_id: claimed.intent_id,
            },
            claimed.intent,
        )))
    }

    /// Claims the earliest pending relay termination intent targeted at `selected_room_node_id`.
    pub fn claim_next_termination_intent_for_node(
        &self,
        selected_room_node_id: &str,
    ) -> Result<Option<NonLocalRelaySessionTerminationIntent>, RoomNodeRegistryError> {
        let mut candidates = self
            .store
            .hvals(self.termination_intents_key)?
            .into_iter()
            .map(|encoded| {
                serde_json::from_str::<StoredRelayTerminationIntent>(&encoded).map_err(|err| {
                    RoomNodeRegistryError::Backend {
                        message: format!("failed to decode relay termination intent: {err}"),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        candidates
            .retain(|candidate| candidate.intent.selected_room_node_id == selected_room_node_id);
        candidates.sort_by(|left, right| left.intent_id.cmp(&right.intent_id));

        let Some(claimed) = candidates.into_iter().next() else {
            return Ok(None);
        };

        self.store
            .hdel(self.termination_intents_key, &claimed.intent_id)?;
        Ok(Some(claimed.intent))
    }

    /// Claims the earliest pending relay signal request intent targeted at `selected_room_node_id`.
    pub fn claim_next_signal_request_intent_for_node(
        &self,
        selected_room_node_id: &str,
    ) -> Result<Option<(RelayIntentReceipt, NonLocalRelaySignalRequestIntent)>, RoomNodeRegistryError>
    {
        let mut candidates = self
            .store
            .hvals(self.signal_intents_key)?
            .into_iter()
            .map(|encoded| {
                serde_json::from_str::<StoredRelaySignalRequestIntent>(&encoded).map_err(|err| {
                    RoomNodeRegistryError::Backend {
                        message: format!("failed to decode relay signal request intent: {err}"),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        candidates
            .retain(|candidate| candidate.intent.selected_room_node_id == selected_room_node_id);
        candidates.sort_by(|left, right| left.intent_id.cmp(&right.intent_id));

        let Some(claimed) = candidates.into_iter().next() else {
            return Ok(None);
        };

        self.store
            .hdel(self.signal_intents_key, &claimed.intent_id)?;
        Ok(Some((
            RelayIntentReceipt {
                intent_id: claimed.intent_id,
            },
            claimed.intent,
        )))
    }

    /// Claims the earliest pending relay RoomService intent targeted at `selected_room_node_id`.
    pub fn claim_next_room_service_intent_for_node(
        &self,
        selected_room_node_id: &str,
    ) -> Result<Option<(RelayIntentReceipt, NonLocalRelayRoomServiceIntent)>, RoomNodeRegistryError>
    {
        let mut candidates = self
            .store
            .hvals(self.room_service_intents_key)?
            .into_iter()
            .map(|encoded| {
                serde_json::from_str::<StoredRelayRoomServiceIntent>(&encoded).map_err(|err| {
                    RoomNodeRegistryError::Backend {
                        message: format!("failed to decode relay room service intent: {err}"),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        candidates
            .retain(|candidate| candidate.intent.selected_room_node_id == selected_room_node_id);
        candidates.sort_by(|left, right| left.intent_id.cmp(&right.intent_id));

        let Some(claimed) = candidates.into_iter().next() else {
            return Ok(None);
        };

        self.store
            .hdel(self.room_service_intents_key, &claimed.intent_id)?;
        Ok(Some((
            RelayIntentReceipt {
                intent_id: claimed.intent_id,
            },
            claimed.intent,
        )))
    }

    /// Stores relay response payload for a previously dispatched intent.
    pub fn store_response(
        &self,
        receipt: &RelayIntentReceipt,
        response: &NonLocalRelayJoinResponse,
    ) -> Result<(), RoomNodeRegistryError> {
        let encoded =
            serde_json::to_string(response).map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("failed to encode relay response: {err}"),
            })?;
        self.store
            .hset(self.responses_key, &receipt.intent_id, &encoded)
    }

    /// Stores a persistent outbound signal response for a relayed remote-owned session.
    pub fn store_outbound_signal_response(
        &self,
        room: &str,
        identity: &str,
        selected_room_node_id: &str,
        signal_response: Vec<u8>,
    ) -> Result<(), RoomNodeRegistryError> {
        let event_id = self.next_unique_id("relay-outbound-signal");
        let encoded = serde_json::to_string(&StoredRelayOutboundSignalResponse {
            event_id: event_id.clone(),
            room: room.to_string(),
            identity: identity.to_string(),
            selected_room_node_id: selected_room_node_id.to_string(),
            signal_response,
        })
        .map_err(|err| RoomNodeRegistryError::Backend {
            message: format!("failed to encode outbound relay signal response: {err}"),
        })?;
        self.store
            .hset(self.outbound_signal_responses_key, &event_id, &encoded)
    }

    /// Claims persistent outbound signal responses for a relayed remote-owned session.
    pub fn claim_outbound_signal_responses(
        &self,
        query: &NonLocalRelayOutboundSignalQuery,
    ) -> Result<Vec<Vec<u8>>, RoomNodeRegistryError> {
        let mut candidates = self
            .store
            .hvals(self.outbound_signal_responses_key)?
            .into_iter()
            .map(|encoded| {
                serde_json::from_str::<StoredRelayOutboundSignalResponse>(&encoded).map_err(|err| {
                    RoomNodeRegistryError::Backend {
                        message: format!("failed to decode outbound relay signal response: {err}"),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        candidates.retain(|candidate| {
            candidate.room == query.room
                && candidate.identity == query.identity
                && candidate.selected_room_node_id == query.selected_room_node_id
        });
        candidates.sort_by(|left, right| left.event_id.cmp(&right.event_id));

        let mut responses = Vec::new();
        for candidate in candidates.into_iter().take(query.max_events) {
            self.store
                .hdel(self.outbound_signal_responses_key, &candidate.event_id)?;
            responses.push(candidate.signal_response);
        }
        Ok(responses)
    }

    /// Stores relay RoomService response payload for a previously dispatched room service intent.
    pub fn store_room_service_response(
        &self,
        receipt: &RelayIntentReceipt,
        response: &NonLocalRelayRoomServiceResponse,
    ) -> Result<(), RoomNodeRegistryError> {
        let encoded =
            serde_json::to_string(response).map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("failed to encode relay room service response: {err}"),
            })?;
        self.store.hset(
            self.room_service_responses_key,
            &receipt.intent_id,
            &encoded,
        )
    }

    /// Stores relay signal response payload for a previously dispatched signal request intent.
    pub fn store_signal_response(
        &self,
        receipt: &RelayIntentReceipt,
        response: &NonLocalRelaySignalRequestResponse,
    ) -> Result<(), RoomNodeRegistryError> {
        let encoded =
            serde_json::to_string(response).map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("failed to encode relay signal response: {err}"),
            })?;
        self.store
            .hset(self.signal_responses_key, &receipt.intent_id, &encoded)
    }

    /// Returns current number of pending relay intents.
    pub fn pending_intents_len(&self) -> Result<usize, RoomNodeRegistryError> {
        Ok(self.store.hvals(self.intents_key)?.len())
    }

    /// Returns current number of pending relay signal request intents.
    pub fn pending_signal_intents_len(&self) -> Result<usize, RoomNodeRegistryError> {
        Ok(self.store.hvals(self.signal_intents_key)?.len())
    }

    /// Retrieves a relay response for a dispatched intent when present.
    pub fn fetch_response(
        &self,
        receipt: &RelayIntentReceipt,
    ) -> Result<Option<NonLocalRelayJoinResponse>, RoomNodeRegistryError> {
        let Some(encoded) = self.store.hget(self.responses_key, &receipt.intent_id)? else {
            return Ok(None);
        };
        let decoded =
            serde_json::from_str::<NonLocalRelayJoinResponse>(&encoded).map_err(|err| {
                RoomNodeRegistryError::Backend {
                    message: format!("failed to decode relay response: {err}"),
                }
            })?;
        Ok(Some(decoded))
    }

    /// Retrieves and deletes relay response for a dispatched intent when present.
    pub fn take_response(
        &self,
        receipt: &RelayIntentReceipt,
    ) -> Result<Option<NonLocalRelayJoinResponse>, RoomNodeRegistryError> {
        let response = self.fetch_response(receipt)?;
        if response.is_some() {
            self.store.hdel(self.responses_key, &receipt.intent_id)?;
        }
        Ok(response)
    }

    /// Retrieves a relay RoomService response for a dispatched room service intent when present.
    pub fn fetch_room_service_response(
        &self,
        receipt: &RelayIntentReceipt,
    ) -> Result<Option<NonLocalRelayRoomServiceResponse>, RoomNodeRegistryError> {
        let Some(encoded) = self
            .store
            .hget(self.room_service_responses_key, &receipt.intent_id)?
        else {
            return Ok(None);
        };
        let decoded =
            serde_json::from_str::<NonLocalRelayRoomServiceResponse>(&encoded).map_err(|err| {
                RoomNodeRegistryError::Backend {
                    message: format!("failed to decode relay room service response: {err}"),
                }
            })?;
        Ok(Some(decoded))
    }

    /// Retrieves and deletes relay RoomService response for a dispatched room service intent when present.
    pub fn take_room_service_response(
        &self,
        receipt: &RelayIntentReceipt,
    ) -> Result<Option<NonLocalRelayRoomServiceResponse>, RoomNodeRegistryError> {
        let response = self.fetch_room_service_response(receipt)?;
        if response.is_some() {
            self.store
                .hdel(self.room_service_responses_key, &receipt.intent_id)?;
        }
        Ok(response)
    }

    /// Retrieves a relay signal response for a dispatched signal request when present.
    pub fn fetch_signal_response(
        &self,
        receipt: &RelayIntentReceipt,
    ) -> Result<Option<NonLocalRelaySignalRequestResponse>, RoomNodeRegistryError> {
        let Some(encoded) = self
            .store
            .hget(self.signal_responses_key, &receipt.intent_id)?
        else {
            return Ok(None);
        };
        let decoded = serde_json::from_str::<NonLocalRelaySignalRequestResponse>(&encoded)
            .map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("failed to decode relay signal response: {err}"),
            })?;
        Ok(Some(decoded))
    }

    /// Retrieves and deletes a relay signal response for a dispatched signal request when present.
    pub fn take_signal_response(
        &self,
        receipt: &RelayIntentReceipt,
    ) -> Result<Option<NonLocalRelaySignalRequestResponse>, RoomNodeRegistryError> {
        let response = self.fetch_signal_response(receipt)?;
        if response.is_some() {
            self.store
                .hdel(self.signal_responses_key, &receipt.intent_id)?;
        }
        Ok(response)
    }
}

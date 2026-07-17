use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use livekit_protocol as proto;

/// Defaults applied when rooms are created without explicit room-service values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomDefaults {
    /// Maximum participants in auto-created rooms; zero is unlimited.
    pub max_participants: u32,
    /// Empty-room cleanup timeout in seconds.
    pub empty_timeout: u32,
    /// Post-departure cleanup timeout in seconds.
    pub departure_timeout: u32,
}

impl Default for RoomDefaults {
    fn default() -> Self {
        Self {
            max_participants: 0,
            empty_timeout: 300,
            departure_timeout: 20,
        }
    }
}

/// Thread-safe in-memory room store.
#[derive(Debug, Clone)]
pub struct RoomStore {
    pub(crate) inner: Arc<RwLock<RoomStoreInner>>,
    pub(crate) defaults: RoomDefaults,
}

impl Default for RoomStore {
    fn default() -> Self {
        Self::with_defaults(RoomDefaults::default())
    }
}

impl RoomStore {
    /// Creates a store with defaults for auto-created and unspecified rooms.
    pub fn with_defaults(defaults: RoomDefaults) -> Self {
        Self {
            inner: Arc::new(RwLock::new(RoomStoreInner::default())),
            defaults,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RoomStoreInner {
    pub(crate) rooms: HashMap<String, RoomRecord>,
    pub(crate) next_room_id: u64,
    pub(crate) next_participant_id: u64,
    pub(crate) next_agent_dispatch_id: u64,
    pub(crate) media_unsubscribed: HashSet<(String, String, String, String)>,
    pub(crate) media_subscription_revision: u64,
    pub(crate) sip_legacy_trunks: HashMap<String, proto::SipTrunkInfo>,
    pub(crate) sip_inbound_trunks: HashMap<String, proto::SipInboundTrunkInfo>,
    pub(crate) sip_outbound_trunks: HashMap<String, proto::SipOutboundTrunkInfo>,
    pub(crate) sip_dispatch_rules: HashMap<String, proto::SipDispatchRuleInfo>,
    pub(crate) room_locks: HashMap<String, RoomLockState>,
    pub(crate) next_room_lock_token_id: u64,
    pub(crate) stored_agent_dispatches_by_room:
        HashMap<String, HashMap<String, proto::AgentDispatch>>,
    pub(crate) stored_agent_jobs_by_room: HashMap<String, HashMap<String, proto::Job>>,
    pub(crate) egress_infos: HashMap<String, proto::EgressInfo>,
    pub(crate) ingress_infos: HashMap<String, proto::IngressInfo>,
    pub(crate) next_ingress_id: u64,
    pub(crate) next_egress_id: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct RoomLockState {
    pub(crate) token: String,
    pub(crate) expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RoomInternalCompat {
    pub track_egress: Option<proto::AutoTrackEgress>,
    pub participant_egress: Option<proto::AutoParticipantEgress>,
}

#[derive(Debug, Clone)]
pub(crate) struct RoomRecord {
    pub(crate) room: proto::Room,
    pub(crate) room_internal: Option<RoomInternalCompat>,
    pub(crate) participants: HashMap<String, proto::ParticipantInfo>,
    /// Latest emitted participant version by identity, retained across departures
    /// so a subsequent rejoin is newer than its disconnected snapshot.
    pub(crate) participant_versions: HashMap<String, u32>,
    pub(crate) agent_dispatches: Vec<StoredAgentDispatch>,
    pub(crate) empty_since_unix_ms: Option<i64>,
    pub(crate) had_participants: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct StoredAgentDispatch {
    pub(crate) dispatch: proto::AgentDispatch,
}

pub(crate) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;

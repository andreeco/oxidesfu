use livekit_protocol as proto;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Sender used by relay workers to persist remote-owner outbound signal responses.
pub type RelayOutboundSignalSender = mpsc::UnboundedSender<proto::SignalResponse>;

/// Relay intent emitted when placement selects a non-local room node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NonLocalRelayJoinIntent {
    /// Room name being joined.
    pub room: String,
    /// Joining participant identity.
    pub identity: String,
    /// Joining participant display name.
    pub name: String,
    /// Joining participant metadata (token/request effective value).
    pub metadata: String,
    /// Joining participant attributes (token+join-request merged value).
    pub attributes: std::collections::HashMap<String, String>,
    /// Requested participant SID for reconnect attempts, when present.
    pub requested_participant_sid: Option<String>,
    /// Selected non-local room-node ID.
    pub selected_room_node_id: String,
    /// Whether the client uses a server-offered subscriber transport (v0 dual-PC).
    ///
    /// The remote room owner must preserve this topology when it handles the
    /// relayed publisher offer.
    #[serde(default)]
    pub subscriber_primary: bool,
    /// Whether this participant can publish media tracks.
    pub can_publish: bool,
    /// Whether this participant can subscribe to tracks.
    pub can_subscribe: bool,
    /// Whether this participant can publish data tracks.
    pub can_publish_data: bool,
    /// Whether this participant can update their own metadata.
    pub can_update_metadata: bool,
    /// Whether this participant should be hidden from participant lists.
    pub hidden: bool,
    /// API key (`iss`) that signed the original participant JWT.
    pub api_key: String,
    /// Participant kind claim from original token.
    pub kind: String,
    /// Participant kind details claim from original token.
    pub kind_details: Vec<String>,
    /// Destination room from original token grants.
    pub destination_room: String,
    /// Optional room config claim from original token.
    pub room_config: Option<serde_json::Value>,
    /// Encoded protobuf client metadata from the original join request.
    ///
    /// This preserves owner-side browser policy resolution without requiring
    /// generated protocol types to be mailbox-serialization types.
    pub client_info: Option<Vec<u8>>,
}

/// Relay intent emitted when a non-local relayed session is terminated on the origin node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NonLocalRelaySessionTerminationIntent {
    /// Room name of the relayed participant.
    pub room: String,
    /// Relayed participant identity.
    pub identity: String,
    /// Participant SID of the relayed session being terminated.
    ///
    /// The owner uses this to avoid removing a newer rejoin with the same identity.
    pub participant_sid: String,
    /// Selected non-local room-node ID.
    pub selected_room_node_id: String,
}

/// Relay result returned by a remote-node join handling path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NonLocalRelayJoinResponse {
    /// Remote node accepted join and can provide join envelope details.
    Accepted {
        participant_sid: String,
        server_version: String,
        ping_interval: i32,
        ping_timeout: i32,
    },
    /// Remote node accepted join and returned a full protobuf-encoded [`proto::JoinResponse`].
    AcceptedWithJoin { join_response: Vec<u8> },
    /// Remote node accepted join and returned initial owner-originated signal responses.
    ///
    /// Used for v0 relayed joins, whose subscriber offer must arrive before the
    /// client can complete its separate subscriber transport.
    AcceptedWithJoinAndSignals {
        join_response: Vec<u8>,
        initial_signal_responses: Vec<Vec<u8>>,
    },
    /// Remote node rejected join with actionable code/message.
    Rejected { code: String, msg: String },
}

/// Relay intent emitted for a long-lived non-local signal request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NonLocalRelaySignalRequestIntent {
    /// Room name of the relayed participant.
    pub room: String,
    /// Relayed participant identity.
    pub identity: String,
    /// Selected non-local room-node ID.
    pub selected_room_node_id: String,
    /// Protobuf-encoded [`proto::SignalRequest`].
    pub signal_request: Vec<u8>,
}

/// Relay intent emitted for a non-local RoomService operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NonLocalRelayRoomServiceIntent {
    /// Room name targeted by the RoomService method.
    pub room: String,
    /// Selected non-local room-node ID.
    pub selected_room_node_id: String,
    /// RoomService method name (for diagnostics/dispatch), e.g. `GetParticipant`.
    pub method: String,
    /// Protobuf-encoded request payload for the method.
    pub request: Vec<u8>,
}

/// Relay response emitted for a remote-owned RoomService operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NonLocalRelayRoomServiceResponse {
    /// The remote owner produced a protobuf response payload.
    Success { response: Vec<u8> },
    /// The remote owner produced a Twirp error envelope.
    TwirpError {
        status: u16,
        code: String,
        msg: String,
    },
}

/// Query for draining persistent remote-owner outbound signal responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NonLocalRelayOutboundSignalQuery {
    /// Room name of the relayed participant.
    pub room: String,
    /// Relayed participant identity.
    pub identity: String,
    /// Selected non-local room-node ID.
    pub selected_room_node_id: String,
    /// Maximum number of outbound events to drain in one call.
    pub max_events: usize,
}

/// Relay response emitted after a remote owner handles a signal request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NonLocalRelaySignalRequestResponse {
    /// The remote owner produced a protobuf-encoded [`proto::SignalResponse`].
    Response {
        signal_response: Vec<u8>,
        outbound_signal_responses: Vec<Vec<u8>>,
    },
    /// The remote owner produced only outbound asynchronous [`proto::SignalResponse`] payloads.
    Outbound {
        outbound_signal_responses: Vec<Vec<u8>>,
    },
    /// The remote owner accepted the request but produced no immediate response.
    NoResponse,
    /// The remote owner terminated the relayed session, such as after `Leave`.
    Closed,
    /// The remote owner could not process the request.
    Error { message: String },
}

/// Dispatch receipt for stored relay intents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayIntentReceipt {
    /// Deterministic ID used to correlate async relay responses.
    pub intent_id: String,
}

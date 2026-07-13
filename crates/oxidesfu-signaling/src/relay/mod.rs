mod dispatcher;
mod intents;
mod mailbox;
mod metrics;

use livekit_protocol as proto;
use prost::Message;

pub use dispatcher::{
    NonLocalRelayDispatcher, NoopNonLocalRelayDispatcher, NoopRelayIntentExecutionDriver,
    RedisMailboxRelayDispatcher, RelayIntentExecutionDriver,
};
pub use intents::{
    NonLocalRelayJoinIntent, NonLocalRelayJoinResponse, NonLocalRelayOutboundSignalQuery,
    NonLocalRelayRoomServiceIntent, NonLocalRelayRoomServiceResponse,
    NonLocalRelaySessionTerminationIntent, NonLocalRelaySignalRequestIntent,
    NonLocalRelaySignalRequestResponse, RelayIntentReceipt, RelayOutboundSignalSender,
};
pub use mailbox::RedisRelayMailbox;
pub use metrics::{RelayMetricsSnapshot, relay_metrics_snapshot};

pub(crate) use metrics::{
    inc_dispatch_attempts, inc_dispatch_failures, inc_fallback_to_local, inc_responses_accepted,
    inc_responses_rejected,
};

/// Encodes a signal response for relay mailbox persistence.
pub fn encode_relay_signal_response(response: &proto::SignalResponse) -> Vec<u8> {
    response.encode_to_vec()
}

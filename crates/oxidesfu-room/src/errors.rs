use thiserror::Error;

/// Errors returned by the in-memory room store.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RoomStoreError {
    /// The requested room does not exist.
    #[error("room not found")]
    RoomNotFound,
    /// The requested participant does not exist.
    #[error("participant not found")]
    ParticipantNotFound,
    /// The requested agent dispatch does not exist.
    #[error("agent dispatch not found")]
    AgentDispatchNotFound,
    /// The requested SIP trunk does not exist.
    #[error("sip trunk not found")]
    SipTrunkNotFound,
    /// The requested SIP dispatch rule does not exist.
    #[error("sip dispatch rule not found")]
    SipDispatchRuleNotFound,
    /// The requested ingress record does not exist.
    #[error("ingress not found")]
    IngressNotFound,
    /// The requested egress record does not exist.
    #[error("egress not found")]
    EgressNotFound,
    /// The room has reached its configured maximum participant count.
    #[error("room has exceeded its max participants")]
    MaxParticipantsExceeded,
    /// Request payload is invalid.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Internal synchronization failed.
    #[error("room store lock poisoned")]
    LockPoisoned,
}

/// Errors returned by node discovery and room allocation registries.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RoomNodeRegistryError {
    /// Internal synchronization failed.
    #[error("room node registry lock poisoned")]
    LockPoisoned,
    /// A room has no assigned node, or references a missing node.
    #[error("node not found")]
    NodeNotFound,
    /// Backend storage operation failed.
    #[error("room node backend error: {message}")]
    Backend { message: String },
}

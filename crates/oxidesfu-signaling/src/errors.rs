use thiserror::Error;

#[derive(Debug, Error)]
pub enum SignalError {
    #[error("failed to decode signal request: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("failed to decode json signal request: {0}")]
    JsonDecode(#[from] serde_json::Error),
    #[error("websocket closed")]
    WebSocketClosed,
    #[error("participant left")]
    ParticipantLeft,
    #[error("remote relayed session closed")]
    RemoteRelayedSessionClosed,
    #[error("signal request handling failed: {message}")]
    RequestHandling { message: String },
}

impl SignalError {
    pub fn is_terminal_for_socket_loop(&self) -> bool {
        matches!(self, Self::ParticipantLeft | Self::WebSocketClosed)
    }

    pub fn is_participant_left(&self) -> bool {
        matches!(self, Self::ParticipantLeft)
    }
}

pub type SignalResult<T> = Result<T, SignalError>;

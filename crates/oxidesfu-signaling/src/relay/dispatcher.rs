use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use oxidesfu_room::RedisHashStore;

use super::{
    intents::{
        NonLocalRelayJoinIntent, NonLocalRelayJoinResponse, NonLocalRelayOutboundSignalQuery,
        NonLocalRelayRoomServiceIntent, NonLocalRelayRoomServiceResponse,
        NonLocalRelaySessionTerminationIntent, NonLocalRelaySignalRequestIntent,
        NonLocalRelaySignalRequestResponse,
    },
    mailbox::RedisRelayMailbox,
    metrics::{inc_signal_failures, inc_signal_requests, inc_signal_responses},
};

/// Dispatches relay intents for non-local room-node placement decisions.
pub trait NonLocalRelayDispatcher: Send + Sync {
    /// Dispatches a non-local join intent toward the selected remote room node.
    ///
    /// Returns:
    /// - `Ok(Some(response))` when a remote node handled the join and produced a response,
    /// - `Ok(None)` when no remote response is currently available (origin may fallback locally),
    /// - `Err(message)` when dispatch failed.
    fn dispatch_non_local_join(
        &self,
        intent: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String>;

    /// Dispatches a non-local session termination intent toward the selected remote room node.
    fn dispatch_non_local_termination(
        &self,
        intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String>;

    /// Dispatches a long-lived signal request to the selected remote room node.
    fn dispatch_non_local_signal_request(
        &self,
        _intent: NonLocalRelaySignalRequestIntent,
    ) -> Result<Option<NonLocalRelaySignalRequestResponse>, String> {
        Ok(None)
    }

    /// Dispatches a non-local RoomService operation toward the selected remote room node.
    fn dispatch_non_local_room_service(
        &self,
        _intent: NonLocalRelayRoomServiceIntent,
    ) -> Result<Option<NonLocalRelayRoomServiceResponse>, String> {
        Ok(None)
    }

    /// Drains persistent outbound signal responses for a relayed remote-owned session.
    fn drain_non_local_outbound_signal_responses(
        &self,
        _query: NonLocalRelayOutboundSignalQuery,
    ) -> Result<Vec<Vec<u8>>, String> {
        Ok(Vec::new())
    }
}

/// Default non-local relay dispatcher that performs no action.
#[derive(Debug)]
pub struct NoopNonLocalRelayDispatcher;

impl NonLocalRelayDispatcher for NoopNonLocalRelayDispatcher {
    fn dispatch_non_local_join(
        &self,
        _intent: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String> {
        Ok(None)
    }

    fn dispatch_non_local_termination(
        &self,
        _intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String> {
        Ok(())
    }

    fn dispatch_non_local_signal_request(
        &self,
        _intent: NonLocalRelaySignalRequestIntent,
    ) -> Result<Option<NonLocalRelaySignalRequestResponse>, String> {
        Ok(None)
    }

    fn dispatch_non_local_room_service(
        &self,
        _intent: NonLocalRelayRoomServiceIntent,
    ) -> Result<Option<NonLocalRelayRoomServiceResponse>, String> {
        Ok(None)
    }
}

/// Executes pending relay intents for a selected node after dispatch.
pub trait RelayIntentExecutionDriver<S>: Send + Sync
where
    S: RedisHashStore,
{
    /// Attempts to process relay join intents targeted at `selected_room_node_id`.
    fn drive_for_node(
        &self,
        mailbox: &RedisRelayMailbox<S>,
        selected_room_node_id: &str,
    ) -> Result<(), String>;

    /// Attempts to process relay termination intents targeted at `selected_room_node_id`.
    fn drive_termination_for_node(
        &self,
        _mailbox: &RedisRelayMailbox<S>,
        _selected_room_node_id: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Attempts to process relayed signal request intents targeted at `selected_room_node_id`.
    fn drive_signal_requests_for_node(
        &self,
        _mailbox: &RedisRelayMailbox<S>,
        _selected_room_node_id: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Attempts to process relayed RoomService intents targeted at `selected_room_node_id`.
    fn drive_room_service_requests_for_node(
        &self,
        _mailbox: &RedisRelayMailbox<S>,
        _selected_room_node_id: &str,
    ) -> Result<(), String> {
        Ok(())
    }
}

/// No-op relay execution driver used by default for mailbox-backed dispatchers.
#[derive(Debug)]
pub struct NoopRelayIntentExecutionDriver;

impl<S> RelayIntentExecutionDriver<S> for NoopRelayIntentExecutionDriver
where
    S: RedisHashStore,
{
    fn drive_for_node(
        &self,
        _mailbox: &RedisRelayMailbox<S>,
        _selected_room_node_id: &str,
    ) -> Result<(), String> {
        Ok(())
    }
}

/// Relay dispatcher backed by a hash-based relay mailbox.
pub struct RedisMailboxRelayDispatcher<S>
where
    S: RedisHashStore,
{
    mailbox: RedisRelayMailbox<S>,
    execution_driver: Arc<dyn RelayIntentExecutionDriver<S>>,
    response_poll_interval: Duration,
    response_wait_timeout: Duration,
    dispatch_retry_attempts: u8,
    dispatch_retry_backoff: Duration,
    max_pending_intents: Option<usize>,
}

impl<S> RedisMailboxRelayDispatcher<S>
where
    S: RedisHashStore,
{
    /// Creates a mailbox-backed dispatcher with a no-op execution driver.
    pub fn with_mailbox(mailbox: RedisRelayMailbox<S>) -> Self {
        Self::with_mailbox_and_policy(
            mailbox,
            Arc::new(NoopRelayIntentExecutionDriver),
            Duration::ZERO,
            Duration::ZERO,
            0,
            Duration::ZERO,
            None,
        )
    }

    /// Creates a mailbox-backed dispatcher with an explicit execution driver.
    pub fn with_mailbox_and_driver(
        mailbox: RedisRelayMailbox<S>,
        execution_driver: Arc<dyn RelayIntentExecutionDriver<S>>,
    ) -> Self {
        Self::with_mailbox_and_policy(
            mailbox,
            execution_driver,
            Duration::ZERO,
            Duration::ZERO,
            0,
            Duration::ZERO,
            None,
        )
    }

    /// Creates a mailbox-backed dispatcher with explicit response polling timing.
    pub fn with_mailbox_and_timing(
        mailbox: RedisRelayMailbox<S>,
        execution_driver: Arc<dyn RelayIntentExecutionDriver<S>>,
        response_poll_interval: Duration,
        response_wait_timeout: Duration,
    ) -> Self {
        Self::with_mailbox_and_policy(
            mailbox,
            execution_driver,
            response_poll_interval,
            response_wait_timeout,
            0,
            Duration::ZERO,
            None,
        )
    }

    /// Creates a mailbox-backed dispatcher with explicit timeout/retry/backpressure policy.
    pub fn with_mailbox_and_policy(
        mailbox: RedisRelayMailbox<S>,
        execution_driver: Arc<dyn RelayIntentExecutionDriver<S>>,
        response_poll_interval: Duration,
        response_wait_timeout: Duration,
        dispatch_retry_attempts: u8,
        dispatch_retry_backoff: Duration,
        max_pending_intents: Option<usize>,
    ) -> Self {
        Self {
            mailbox,
            execution_driver,
            response_poll_interval,
            response_wait_timeout,
            dispatch_retry_attempts,
            dispatch_retry_backoff,
            max_pending_intents,
        }
    }
}

impl<S> NonLocalRelayDispatcher for RedisMailboxRelayDispatcher<S>
where
    S: RedisHashStore + Send + Sync + 'static,
{
    fn dispatch_non_local_join(
        &self,
        intent: NonLocalRelayJoinIntent,
    ) -> Result<Option<NonLocalRelayJoinResponse>, String> {
        let selected_room_node_id = intent.selected_room_node_id.clone();

        if let Some(max_pending) = self.max_pending_intents {
            let pending = self
                .mailbox
                .pending_intents_len()
                .map_err(|err| err.to_string())?;
            if pending >= max_pending {
                return Err(format!(
                    "relay mailbox backpressure: pending intents {pending} reached max {max_pending}"
                ));
            }
        }

        let mut receipt = None;
        let mut last_error = None;
        for attempt in 0..=self.dispatch_retry_attempts {
            match self.mailbox.dispatch_intent(&intent) {
                Ok(value) => {
                    receipt = Some(value);
                    break;
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                    if attempt < self.dispatch_retry_attempts {
                        std::thread::sleep(self.dispatch_retry_backoff);
                    }
                }
            }
        }

        let receipt = receipt.ok_or_else(|| {
            last_error.unwrap_or_else(|| "relay dispatch failed without error details".to_string())
        })?;

        self.execution_driver
            .drive_for_node(&self.mailbox, &selected_room_node_id)?;

        let started_at = Instant::now();
        loop {
            if let Some(response) = self
                .mailbox
                .take_response(&receipt)
                .map_err(|err| err.to_string())?
            {
                return Ok(Some(response));
            }

            if started_at.elapsed() >= self.response_wait_timeout {
                return Ok(None);
            }

            std::thread::sleep(self.response_poll_interval);
        }
    }

    fn dispatch_non_local_termination(
        &self,
        intent: NonLocalRelaySessionTerminationIntent,
    ) -> Result<(), String> {
        self.mailbox
            .dispatch_termination_intent(&intent)
            .map_err(|err| err.to_string())?;
        self.execution_driver
            .drive_termination_for_node(&self.mailbox, &intent.selected_room_node_id)
    }

    fn dispatch_non_local_signal_request(
        &self,
        intent: NonLocalRelaySignalRequestIntent,
    ) -> Result<Option<NonLocalRelaySignalRequestResponse>, String> {
        inc_signal_requests();
        let selected_room_node_id = intent.selected_room_node_id.clone();
        let receipt = self
            .mailbox
            .dispatch_signal_request_intent(&intent)
            .map_err(|err| {
                inc_signal_failures();
                err.to_string()
            })?;

        if let Err(err) = self
            .execution_driver
            .drive_signal_requests_for_node(&self.mailbox, &selected_room_node_id)
        {
            inc_signal_failures();
            return Err(err);
        }

        // Signal requests (Offer/Answer/Trickle/etc.) are latency-sensitive but can take
        // materially longer than join dispatch when a remote worker must claim intent,
        // execute WebRTC operations, and persist a response. Use a larger minimum wait
        // window here to avoid dropping legitimate delayed responses.
        const MIN_SIGNAL_RESPONSE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
        let signal_response_wait_timeout = self
            .response_wait_timeout
            .max(MIN_SIGNAL_RESPONSE_WAIT_TIMEOUT);
        let deadline = Instant::now() + signal_response_wait_timeout;
        loop {
            if let Some(response) = self
                .mailbox
                .take_signal_response(&receipt)
                .map_err(|err| err.to_string())?
            {
                inc_signal_responses();
                return Ok(Some(response));
            }

            if Instant::now() >= deadline {
                tracing::warn!(
                    room = %intent.room,
                    identity = %intent.identity,
                    selected_room_node_id = %selected_room_node_id,
                    timeout_ms = signal_response_wait_timeout.as_millis(),
                    "relay_signal_request_response_timeout"
                );
                return Ok(None);
            }

            std::thread::sleep(self.response_poll_interval);
        }
    }

    fn dispatch_non_local_room_service(
        &self,
        intent: NonLocalRelayRoomServiceIntent,
    ) -> Result<Option<NonLocalRelayRoomServiceResponse>, String> {
        let selected_room_node_id = intent.selected_room_node_id.clone();
        let receipt = self
            .mailbox
            .dispatch_room_service_intent(&intent)
            .map_err(|err| err.to_string())?;

        self.execution_driver
            .drive_room_service_requests_for_node(&self.mailbox, &selected_room_node_id)?;

        let started_at = Instant::now();
        loop {
            if let Some(response) = self
                .mailbox
                .take_room_service_response(&receipt)
                .map_err(|err| err.to_string())?
            {
                return Ok(Some(response));
            }

            if started_at.elapsed() >= self.response_wait_timeout {
                return Ok(None);
            }

            std::thread::sleep(self.response_poll_interval);
        }
    }

    fn drain_non_local_outbound_signal_responses(
        &self,
        query: NonLocalRelayOutboundSignalQuery,
    ) -> Result<Vec<Vec<u8>>, String> {
        self.mailbox
            .claim_outbound_signal_responses(&query)
            .map_err(|err| err.to_string())
    }
}

use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;

/// Default interval between WHIP connection notifications.
pub(crate) const DEFAULT_WHIP_SESSION_NOTIFY_INTERVAL: Duration = Duration::from_secs(10);

/// Minimal participant handle needed by the WHIP notification loop.
pub(crate) trait WhipParticipant: Send + Sync {
    fn is_closed(&self) -> bool;
    fn participant_id(&self) -> &str;
}

/// Minimal notify payload used by the ingress WHIP notifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WhipRtcConnectionNotifyRequest {
    pub participant_id: String,
    pub closed: bool,
}

/// Ingress notifier interface used by WHIP session notification.
#[async_trait]
pub(crate) trait IngressWhipNotifier: Send + Sync {
    async fn notify_connection(
        &self,
        request: WhipRtcConnectionNotifyRequest,
    ) -> Result<(), WhipNotifyError>;
}

/// WHIP notification loop errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum WhipNotifyError {
    #[error("participant not found")]
    ParticipantNotFound,
    #[error("notify failed: {message}")]
    NotifyFailed { message: String },
}

/// Sends one WHIP connection notification for `participant`.
///
/// Returns [`WhipNotifyError::ParticipantNotFound`] when the participant has
/// already closed, and skips notifier invocation in that case.
pub(crate) async fn send_connection_notify(
    notifier: &dyn IngressWhipNotifier,
    participant: &dyn WhipParticipant,
) -> Result<(), WhipNotifyError> {
    if participant.is_closed() {
        return Err(WhipNotifyError::ParticipantNotFound);
    }

    notifier
        .notify_connection(WhipRtcConnectionNotifyRequest {
            participant_id: participant.participant_id().to_string(),
            closed: false,
        })
        .await
}

/// Repeatedly sends WHIP connection notifications until participant closure,
/// context cancellation, or hard notifier failure.
pub(crate) async fn notify_session(
    ctx: &tokio::sync::watch::Receiver<bool>,
    notifier: &dyn IngressWhipNotifier,
    participant: &dyn WhipParticipant,
) -> Result<(), WhipNotifyError> {
    notify_session_with_interval(
        ctx,
        notifier,
        participant,
        DEFAULT_WHIP_SESSION_NOTIFY_INTERVAL,
    )
    .await
}

async fn notify_session_with_interval(
    ctx: &tokio::sync::watch::Receiver<bool>,
    notifier: &dyn IngressWhipNotifier,
    participant: &dyn WhipParticipant,
    interval: Duration,
) -> Result<(), WhipNotifyError> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ctx = ctx.clone();

    if let Err(err) = send_connection_notify(notifier, participant).await {
        match err {
            WhipNotifyError::ParticipantNotFound => return Ok(()),
            other => return Err(other),
        }
    }

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(err) = send_connection_notify(notifier, participant).await {
                    match err {
                        WhipNotifyError::ParticipantNotFound => return Ok(()),
                        other => return Err(other),
                    }
                }
            }
            changed = ctx.changed() => {
                if changed.is_err() || *ctx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use super::{
        IngressWhipNotifier, WhipNotifyError, WhipParticipant, WhipRtcConnectionNotifyRequest,
        notify_session_with_interval, send_connection_notify,
    };

    #[derive(Debug)]
    struct FakeParticipant {
        id: String,
        closed: Arc<AtomicBool>,
    }

    impl WhipParticipant for FakeParticipant {
        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }

        fn participant_id(&self) -> &str {
            &self.id
        }
    }

    #[derive(Debug, Default)]
    struct FakeNotifier {
        notify_count: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl IngressWhipNotifier for FakeNotifier {
        async fn notify_connection(
            &self,
            _request: WhipRtcConnectionNotifyRequest,
        ) -> Result<(), WhipNotifyError> {
            self.notify_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // Upstream mapping:
    // livekit/pkg/service/roommanager_service_test.go::TestWhipNotifySessionStopsWhenParticipantLeaves
    #[tokio::test]
    async fn notify_session_stops_when_participant_leaves() {
        let closed = Arc::new(AtomicBool::new(false));
        let participant = FakeParticipant {
            id: "PA_test".to_string(),
            closed: Arc::clone(&closed),
        };
        let notifier = Arc::new(FakeNotifier::default());
        let (ctx_tx, ctx_rx) = tokio::sync::watch::channel(false);

        let done = {
            let notifier = Arc::clone(&notifier);
            tokio::spawn(async move {
                notify_session_with_interval(
                    &ctx_rx,
                    notifier.as_ref(),
                    &participant,
                    Duration::from_millis(5),
                )
                .await
            })
        };

        tokio::time::timeout(Duration::from_secs(1), async {
            while notifier.notify_count.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("expected notifications while participant is connected");

        closed.store(true, Ordering::SeqCst);

        let result = tokio::time::timeout(Duration::from_secs(1), done)
            .await
            .expect("notify_session should stop after participant leaves")
            .expect("join handle should not panic");
        assert_eq!(result, Ok(()));

        let count_at_stop = notifier.notify_count.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            notifier.notify_count.load(Ordering::SeqCst),
            count_at_stop,
            "should not notify after participant left"
        );

        drop(ctx_tx);
    }

    // Upstream mapping:
    // livekit/pkg/service/roommanager_service_test.go::TestWhipNotifySessionStopsOnContextCancel
    #[tokio::test]
    async fn notify_session_stops_on_context_cancel() {
        let participant = FakeParticipant {
            id: "PA_test".to_string(),
            closed: Arc::new(AtomicBool::new(false)),
        };
        let notifier = FakeNotifier::default();
        let (ctx_tx, ctx_rx) = tokio::sync::watch::channel(false);

        let done = tokio::spawn(async move {
            notify_session_with_interval(&ctx_rx, &notifier, &participant, Duration::from_millis(5))
                .await
        });

        ctx_tx
            .send(true)
            .expect("context cancellation signal should send");

        let result = tokio::time::timeout(Duration::from_secs(1), done)
            .await
            .expect("notify_session should stop after context cancel")
            .expect("join handle should not panic");
        assert_eq!(result, Ok(()));
    }

    // Upstream mapping:
    // livekit/pkg/service/roommanager_service_test.go::TestWhipSendConnectionNotifySkipsClosedParticipant
    #[tokio::test]
    async fn send_connection_notify_skips_closed_participant() {
        let participant = FakeParticipant {
            id: "PA_test".to_string(),
            closed: Arc::new(AtomicBool::new(true)),
        };
        let notifier = FakeNotifier::default();

        let err = send_connection_notify(&notifier, &participant)
            .await
            .expect_err("closed participant should short-circuit notify");
        assert_eq!(err, WhipNotifyError::ParticipantNotFound);
        assert_eq!(
            notifier.notify_count.load(Ordering::SeqCst),
            0,
            "should not issue notification for closed participant"
        );
    }
}

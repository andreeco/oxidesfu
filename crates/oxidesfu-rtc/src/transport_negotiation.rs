#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NegotiationStateLite {
    #[default]
    Idle,
    Remote,
    Retry,
}

/// Lightweight negotiation controller that mirrors key `PCTransport` retry/failure contracts.
#[derive(Debug, Default)]
pub struct NegotiationControllerLite {
    next_offer_id: u32,
    pending_offer_id: Option<u32>,
    retry_queued: bool,
    state: NegotiationStateLite,
}

impl NegotiationControllerLite {
    pub fn state(&self) -> NegotiationStateLite {
        self.state
    }

    pub fn pending_offer_id(&self) -> Option<u32> {
        self.pending_offer_id
    }

    pub fn negotiate(&mut self, force: bool) -> Option<u32> {
        if !force {
            return None;
        }

        if self.pending_offer_id.is_none() {
            return Some(self.issue_offer());
        }

        if self.state == NegotiationStateLite::Remote {
            self.state = NegotiationStateLite::Retry;
        }
        self.retry_queued = true;
        None
    }

    /// Starts an ICE restart. If an offer/answer cycle is currently unresolved,
    /// emits a recovery offer and queues one additional restart offer.
    pub fn ice_restart(&mut self) -> Option<u32> {
        if self.pending_offer_id.is_some() {
            self.retry_queued = true;
        }
        Some(self.issue_offer())
    }

    /// Applies a remote answer. Returns a follow-up offer ID when retry/restart was queued.
    pub fn handle_remote_answer(&mut self, answer_id: u32) -> Option<u32> {
        if self.pending_offer_id != Some(answer_id) {
            return None;
        }

        self.pending_offer_id = None;
        if self.retry_queued {
            self.retry_queued = false;
            return Some(self.issue_offer());
        }

        self.state = NegotiationStateLite::Idle;
        None
    }

    /// Returns true when negotiation failed due to timeout while waiting on an answer.
    pub fn on_negotiation_timeout(&mut self) -> bool {
        if self.pending_offer_id.is_none() {
            return false;
        }

        self.pending_offer_id = None;
        self.retry_queued = false;
        self.state = NegotiationStateLite::Idle;
        true
    }

    fn issue_offer(&mut self) -> u32 {
        self.next_offer_id = self.next_offer_id.wrapping_add(1);
        if self.next_offer_id == 0 {
            self.next_offer_id = 1;
        }
        self.pending_offer_id = Some(self.next_offer_id);
        self.state = NegotiationStateLite::Remote;
        self.next_offer_id
    }
}

#[cfg(test)]
mod tests {
    use super::{NegotiationControllerLite, NegotiationStateLite};

    // Upstream: livekit/pkg/rtc/transport_test.go::TestNegotiationTiming
    #[test]
    fn negotiation_timing_matches_upstream_retry_contract() {
        let mut controller = NegotiationControllerLite::default();

        let first_offer = controller
            .negotiate(true)
            .expect("initial forced negotiate should emit offer");
        assert_eq!(controller.state(), NegotiationStateLite::Remote);

        assert!(controller.negotiate(true).is_none());
        assert_eq!(controller.state(), NegotiationStateLite::Retry);

        assert!(controller.negotiate(true).is_none());
        assert_eq!(controller.state(), NegotiationStateLite::Retry);

        let second_offer = controller
            .handle_remote_answer(first_offer)
            .expect("queued retry should emit follow-up offer after answer");
        assert!(second_offer > first_offer);
        assert_eq!(controller.state(), NegotiationStateLite::Remote);
    }

    // Upstream: livekit/pkg/rtc/transport_test.go::TestMissingAnswerDuringICERestart
    #[test]
    fn missing_answer_during_ice_restart_recovers_with_followup_offer() {
        let mut controller = NegotiationControllerLite::default();

        let _initial_offer = controller
            .negotiate(true)
            .expect("initial negotiate should emit offer");

        let recovery_offer = controller
            .ice_restart()
            .expect("ice restart should emit recovery offer");
        let restart_offer = controller
            .handle_remote_answer(recovery_offer)
            .expect("recovery answer should trigger queued restart offer");

        assert!(controller.handle_remote_answer(restart_offer).is_none());
        assert_eq!(controller.state(), NegotiationStateLite::Idle);
        assert!(controller.pending_offer_id().is_none());
    }

    // Upstream: livekit/pkg/rtc/transport_test.go::TestFirstOfferMissedDuringICERestart
    #[test]
    fn first_offer_missed_during_ice_restart_emits_recovery_then_restart_offer() {
        let mut controller = NegotiationControllerLite::default();

        let _missed_first_offer = controller
            .negotiate(true)
            .expect("initial offer should emit");
        let recovery_offer = controller
            .ice_restart()
            .expect("ice restart should emit recovery offer");
        let restart_offer = controller
            .handle_remote_answer(recovery_offer)
            .expect("answering recovery offer should emit restart offer");

        assert!(restart_offer > recovery_offer);
    }

    // Upstream: livekit/pkg/rtc/transport_test.go::TestFirstAnswerMissedDuringICERestart
    #[test]
    fn first_answer_missed_during_ice_restart_emits_recovery_then_restart_offer() {
        let mut controller = NegotiationControllerLite::default();

        let first_offer = controller
            .negotiate(true)
            .expect("initial offer should emit");
        // first answer was missed -> keep pending
        assert_eq!(controller.pending_offer_id(), Some(first_offer));

        let recovery_offer = controller
            .ice_restart()
            .expect("ice restart should emit recovery offer");
        let restart_offer = controller
            .handle_remote_answer(recovery_offer)
            .expect("answering recovery offer should emit restart offer");

        assert!(restart_offer > recovery_offer);
    }

    // Upstream: livekit/pkg/rtc/transport_test.go::TestNegotiationFailed
    #[test]
    fn negotiation_failed_on_timeout_matches_upstream_contract() {
        let mut controller = NegotiationControllerLite::default();

        let first_offer = controller
            .negotiate(true)
            .expect("initial offer should emit");
        assert!(controller.on_negotiation_timeout());
        assert_eq!(controller.state(), NegotiationStateLite::Idle);
        assert!(controller.pending_offer_id().is_none());

        assert!(controller.handle_remote_answer(first_offer).is_none());
    }
}

/// Result of evaluating one incoming video packet for a subscriber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VideoIngressDecision {
    Forward { selected_ssrc_changed: bool },
    DropNonSelectedSsrc,
}

/// Chooses an incoming simulcast stream for exactly one subscriber.
///
/// A selector never makes global publisher decisions: subscribers may choose different
/// layers from the same publication according to their own adaptive-stream settings.
#[derive(Debug, Default)]
pub(crate) struct SubscriberVideoLayerSelector {
    selected_ssrc: Option<u32>,
}

impl SubscriberVideoLayerSelector {
    pub(crate) fn selected_ssrc(&self) -> Option<u32> {
        self.selected_ssrc
    }

    /// Clears the selected layer after this subscriber's target quality changes.
    pub(crate) fn reset(&mut self) {
        self.selected_ssrc = None;
    }

    /// Selects only packets that satisfy this subscriber's current quality limit.
    ///
    /// When the current layer becomes ineligible, clearing it lets the next eligible
    /// packet establish the replacement layer. The caller requests a PLI whenever
    /// `selected_ssrc_changed` is true.
    pub(crate) fn observe_packet(
        &mut self,
        incoming_ssrc: u32,
        is_eligible_for_subscriber: bool,
    ) -> VideoIngressDecision {
        if !is_eligible_for_subscriber {
            if self.selected_ssrc == Some(incoming_ssrc) {
                self.selected_ssrc = None;
            }
            return VideoIngressDecision::DropNonSelectedSsrc;
        }

        match self.selected_ssrc {
            None => {
                self.selected_ssrc = Some(incoming_ssrc);
                VideoIngressDecision::Forward {
                    selected_ssrc_changed: true,
                }
            }
            Some(selected_ssrc) if selected_ssrc == incoming_ssrc => {
                VideoIngressDecision::Forward {
                    selected_ssrc_changed: false,
                }
            }
            Some(_) => VideoIngressDecision::DropNonSelectedSsrc,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SubscriberVideoLayerSelector, VideoIngressDecision};

    #[test]
    fn subscriber_selectors_choose_independent_layers_and_downgrade_without_black_hole() {
        let mut high_subscriber = SubscriberVideoLayerSelector::default();
        let mut low_subscriber = SubscriberVideoLayerSelector::default();

        assert_eq!(
            high_subscriber.observe_packet(111, true),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(
            low_subscriber.observe_packet(222, true),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(high_subscriber.selected_ssrc(), Some(111));
        assert_eq!(low_subscriber.selected_ssrc(), Some(222));

        // A lower quality request resets only the affected subscriber. Its next
        // eligible low-layer packet must be forwarded even if the old high layer
        // has already stopped publishing.
        high_subscriber.reset();
        assert_eq!(
            high_subscriber.observe_packet(222, true),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(high_subscriber.selected_ssrc(), Some(222));
        assert_eq!(low_subscriber.selected_ssrc(), Some(222));
    }

    #[test]
    fn selected_layer_stays_forwardable_while_eligible() {
        let mut selector = SubscriberVideoLayerSelector::default();

        assert_eq!(
            selector.observe_packet(111, true),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(
            selector.observe_packet(111, true),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: false
            }
        );
        assert_eq!(
            selector.observe_packet(222, true),
            VideoIngressDecision::DropNonSelectedSsrc
        );
    }
}

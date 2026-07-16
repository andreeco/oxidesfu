/// A spatial simulcast layer, ordered from low to high.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SpatialLayer {
    Low = 0,
    Medium = 1,
    High = 2,
}

impl SpatialLayer {
    pub(crate) const fn from_quality(quality: livekit_protocol::VideoQuality) -> Self {
        match quality {
            livekit_protocol::VideoQuality::Low => Self::Low,
            livekit_protocol::VideoQuality::Medium => Self::Medium,
            livekit_protocol::VideoQuality::High | livekit_protocol::VideoQuality::Off => {
                Self::High
            }
        }
    }
}

/// Subscriber policy derived from settings and allocation. `max` is an admission bound while
/// `desired` is the layer the target should acquire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LayerPolicy {
    pub(crate) max: SpatialLayer,
    pub(crate) desired: SpatialLayer,
}

impl LayerPolicy {
    pub(crate) const fn fixed(layer: SpatialLayer) -> Self {
        Self {
            max: layer,
            desired: layer,
        }
    }
}

/// Identifies whether spatial layers are separate source streams or packets inside one scalable
/// source stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VideoSourceKind {
    /// Independent simulcast SSRC/RID sources; policy selects one spatial source.
    Simulcast,
    /// A single VP9/AV1 scalable SSRC; policy must not be mistaken for source selection.
    SingleScalable,
}

/// Metadata resolved by the forwarding reader before a target-local decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LayerPacketMetadata {
    pub(crate) ssrc: u32,
    pub(crate) spatial: Option<SpatialLayer>,
    pub(crate) source_kind: VideoSourceKind,
    pub(crate) is_decodable_switch_point: bool,
}

/// Result of evaluating one incoming video packet for a subscriber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VideoIngressDecision {
    Forward { selected_ssrc_changed: bool },
    DropWaitingForKeyframe,
    DropNonSelectedSsrc,
    DropAboveMaximum,
    DropUnknownLayer,
}

/// Observable reader-local acquisition state for one subscriber target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LayerAcquisitionState {
    Stable,
    WaitingForDesired,
    WaitingForFallback,
    FallbackLocked,
}

/// Result of a timer-driven target-layer acquisition retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KeyframeRequest {
    pub(crate) media_ssrc: u32,
}

/// Reader-local selector for one subscriber target.
///
/// Packet decisions do not read the clock or perform I/O. The owning reader invokes
/// [`Self::on_timer`] from its existing timer path, which bounds retries and fallback.
#[derive(Debug)]
pub(crate) struct SubscriberVideoLayerSelector {
    policy: LayerPolicy,
    current: Option<(u32, SpatialLayer)>,
    rtp_seen_ssrc: [Option<u32>; 3],
    decoder_usable_ssrc: [Option<u32>; 3],
    seen_age_ticks: [u8; 3],
    waiting_for: SpatialLayer,
    acquisition_ticks: u8,
    fallback_started: bool,
    fallback_locked: bool,
    pli_ticks: u8,
    remaining_pli_requests: u8,
    single_scalable_active: bool,
    single_scalable_current: Option<u32>,
    single_scalable_seen: Option<u32>,
    single_scalable_seen_age_ticks: u8,
    single_scalable_current_age_ticks: u8,
}

impl Default for SubscriberVideoLayerSelector {
    fn default() -> Self {
        Self::new(LayerPolicy::fixed(SpatialLayer::High))
    }
}

impl SubscriberVideoLayerSelector {
    // The reader timer is 250 ms. One second preserves the LiveKit initial acquisition grace.
    const ACQUISITION_GRACE_TICKS: u8 = 4;
    // PLI is sent immediately on the next timer tick, then no more than once per 500 ms.
    const KEYFRAME_RETRY_TICKS: u8 = 2;
    // A target may not continuously request keyframes if its publisher cannot satisfy the
    // request. A policy update starts a fresh, bounded acquisition attempt.
    // At the 500 ms retry cadence this permits six seconds of acquisition, which covers
    // encoders that only produce a target-layer keyframe after several PLI round trips.
    const MAX_KEYFRAME_REQUESTS_PER_ACQUISITION: u8 = 12;
    // Sources that have not produced RTP for two seconds are no longer candidates.
    const SOURCE_STALE_TICKS: u8 = 8;

    pub(crate) const fn new(policy: LayerPolicy) -> Self {
        Self {
            policy,
            current: None,
            rtp_seen_ssrc: [None; 3],
            decoder_usable_ssrc: [None; 3],
            seen_age_ticks: [u8::MAX; 3],
            waiting_for: policy.desired,
            acquisition_ticks: 0,
            fallback_started: false,
            fallback_locked: false,
            pli_ticks: 0,
            remaining_pli_requests: Self::MAX_KEYFRAME_REQUESTS_PER_ACQUISITION,
            single_scalable_active: false,
            single_scalable_current: None,
            single_scalable_seen: None,
            single_scalable_seen_age_ticks: u8::MAX,
            single_scalable_current_age_ticks: u8::MAX,
        }
    }

    pub(crate) fn selected_ssrc(&self) -> Option<u32> {
        self.single_scalable_current
            .or_else(|| self.current.map(|(ssrc, _)| ssrc))
    }

    /// Returns the selected simulcast spatial source, if any.
    ///
    /// A selected single-SSRC scalable source deliberately returns `None`: its packet-level
    /// decode targets are not interchangeable with simulcast source selection.
    pub(crate) fn current_spatial(&self) -> Option<SpatialLayer> {
        self.current.map(|(_, spatial)| spatial)
    }

    pub(crate) const fn source_kind(&self) -> VideoSourceKind {
        if self.single_scalable_active {
            VideoSourceKind::SingleScalable
        } else {
            VideoSourceKind::Simulcast
        }
    }

    pub(crate) const fn policy(&self) -> LayerPolicy {
        self.policy
    }

    pub(crate) const fn waiting_for(&self) -> SpatialLayer {
        self.waiting_for
    }

    pub(crate) const fn acquisition_ticks(&self) -> u8 {
        self.acquisition_ticks
    }

    pub(crate) const fn remaining_pli_requests(&self) -> u8 {
        self.remaining_pli_requests
    }

    pub(crate) fn acquisition_state(&self) -> LayerAcquisitionState {
        if self.single_scalable_active {
            return if self.single_scalable_current.is_some() {
                LayerAcquisitionState::Stable
            } else {
                LayerAcquisitionState::WaitingForDesired
            };
        }
        if self.fallback_locked {
            LayerAcquisitionState::FallbackLocked
        } else if self.fallback_started {
            LayerAcquisitionState::WaitingForFallback
        } else if self
            .current
            .is_some_and(|(_, spatial)| spatial == self.policy.desired)
        {
            LayerAcquisitionState::Stable
        } else {
            LayerAcquisitionState::WaitingForDesired
        }
    }

    /// Applies a policy update without disrupting the current decodable layer. A later switch is
    /// committed only at a decodable boundary.
    pub(crate) fn set_policy(&mut self, policy: LayerPolicy) {
        if self.policy == policy {
            return;
        }

        self.policy = policy;
        self.waiting_for = policy.desired;
        self.acquisition_ticks = 0;
        self.fallback_started = false;
        self.fallback_locked = false;
        self.pli_ticks = 0;
        self.remaining_pli_requests = Self::MAX_KEYFRAME_REQUESTS_PER_ACQUISITION;

        if self
            .current
            .is_some_and(|(_, spatial)| spatial == policy.desired)
        {
            self.waiting_for = policy.desired;
        }
    }

    /// Evaluates a packet without dependency-descriptor availability metadata.
    ///
    /// This preserves the existing keyframe fallback for codecs that do not provide a descriptor.
    pub(crate) fn observe_packet(&mut self, packet: LayerPacketMetadata) -> VideoIngressDecision {
        self.observe_packet_with_dependency_descriptor_metadata(packet, false)
    }

    /// Evaluates a packet with whether dependency-descriptor metadata was available.
    ///
    /// When metadata is available, only a verified descriptor switch boundary makes a layer
    /// fallback-usable. RTP from descriptor-absent codecs retains the legacy keyframe behavior.
    pub(crate) fn observe_packet_with_dependency_descriptor_metadata(
        &mut self,
        packet: LayerPacketMetadata,
        dependency_descriptor_metadata_available: bool,
    ) -> VideoIngressDecision {
        if packet.source_kind == VideoSourceKind::SingleScalable {
            return self.observe_single_scalable_packet(packet);
        }

        let Some(spatial) = packet.spatial else {
            return if self.current.is_some_and(|(ssrc, _)| ssrc == packet.ssrc) {
                VideoIngressDecision::Forward {
                    selected_ssrc_changed: false,
                }
            } else {
                VideoIngressDecision::DropUnknownLayer
            };
        };
        if spatial > self.policy.max {
            return VideoIngressDecision::DropAboveMaximum;
        }

        let spatial_index = spatial as usize;
        if self.rtp_seen_ssrc[spatial_index] != Some(packet.ssrc) {
            self.decoder_usable_ssrc[spatial_index] = None;
        }
        self.rtp_seen_ssrc[spatial_index] = Some(packet.ssrc);
        self.seen_age_ticks[spatial_index] = 0;
        if !dependency_descriptor_metadata_available || packet.is_decodable_switch_point {
            self.decoder_usable_ssrc[spatial_index] = Some(packet.ssrc);
        }

        if let Some((selected_ssrc, selected_spatial)) = self.current
            && selected_ssrc == packet.ssrc
        {
            // RID/SSRC layer metadata may arrive late or be temporarily inconsistent with the
            // source catalog. A selected source that is still producing RTP is live; preserve
            // its known spatial identity until a keyframe-gated switch changes it.
            self.rtp_seen_ssrc[selected_spatial as usize] = Some(packet.ssrc);
            self.seen_age_ticks[selected_spatial as usize] = 0;
            return VideoIngressDecision::Forward {
                selected_ssrc_changed: false,
            };
        }

        // A fallback suppresses timer-driven reacquisition/PLI, but a later decodable desired
        // layer is concrete availability evidence and may promote this target immediately.
        let should_switch_to_desired = packet.is_decodable_switch_point
            && spatial == self.policy.desired
            && self
                .current
                .is_some_and(|(_, current)| current != self.policy.desired);
        let target = if should_switch_to_desired {
            self.policy.desired
        } else {
            self.waiting_for
        };
        if spatial != target || !packet.is_decodable_switch_point {
            return if spatial == target {
                VideoIngressDecision::DropWaitingForKeyframe
            } else {
                VideoIngressDecision::DropNonSelectedSsrc
            };
        }

        let previous = self.current;
        self.current = Some((packet.ssrc, spatial));
        self.acquisition_ticks = 0;
        self.pli_ticks = 0;
        self.fallback_locked = self.fallback_started && spatial != self.policy.desired;
        self.fallback_started = false;
        self.remaining_pli_requests = Self::MAX_KEYFRAME_REQUESTS_PER_ACQUISITION;
        self.waiting_for = self.policy.desired;
        VideoIngressDecision::Forward {
            selected_ssrc_changed: previous != self.current,
        }
    }

    /// Advances bounded acquisition and returns a target-aware PLI request when one is due.
    /// The caller owns RTCP I/O and must keep this separate from downstream PLI/FIR relay.
    pub(crate) fn on_timer(&mut self) -> Option<KeyframeRequest> {
        self.expire_stale_sources();
        if self.single_scalable_active {
            return self.on_single_scalable_timer();
        }
        if self.fallback_locked {
            return None;
        }
        if self
            .current
            .is_some_and(|(_, spatial)| spatial == self.policy.desired)
        {
            return None;
        }

        self.acquisition_ticks = self.acquisition_ticks.saturating_add(1);
        if !self.fallback_started && self.acquisition_ticks >= Self::ACQUISITION_GRACE_TICKS {
            self.fallback_started = true;
            self.acquisition_ticks = 0;
        }
        if self.fallback_started {
            let fallback = self
                .best_decoder_usable_allowed()
                .unwrap_or(self.policy.desired);
            if fallback != self.waiting_for {
                self.waiting_for = fallback;
                self.pli_ticks = 0;
                self.remaining_pli_requests = Self::MAX_KEYFRAME_REQUESTS_PER_ACQUISITION;
            }
        }

        if self.pli_ticks > 0 {
            self.pli_ticks -= 1;
            return None;
        }
        if self.remaining_pli_requests == 0 {
            return None;
        }

        let ssrc = self.rtp_seen_ssrc[self.waiting_for as usize]?;
        self.pli_ticks = Self::KEYFRAME_RETRY_TICKS.saturating_sub(1);
        self.remaining_pli_requests -= 1;
        Some(KeyframeRequest { media_ssrc: ssrc })
    }

    fn observe_single_scalable_packet(
        &mut self,
        packet: LayerPacketMetadata,
    ) -> VideoIngressDecision {
        self.single_scalable_active = true;
        self.single_scalable_seen = Some(packet.ssrc);
        self.single_scalable_seen_age_ticks = 0;

        if self.single_scalable_current == Some(packet.ssrc) {
            self.single_scalable_current_age_ticks = 0;
            return VideoIngressDecision::Forward {
                selected_ssrc_changed: false,
            };
        }
        if self.single_scalable_current.is_some() {
            return VideoIngressDecision::DropNonSelectedSsrc;
        }
        if !packet.is_decodable_switch_point {
            return VideoIngressDecision::DropWaitingForKeyframe;
        }

        self.single_scalable_current = Some(packet.ssrc);
        self.single_scalable_current_age_ticks = 0;
        self.acquisition_ticks = 0;
        self.pli_ticks = 0;
        self.remaining_pli_requests = Self::MAX_KEYFRAME_REQUESTS_PER_ACQUISITION;
        VideoIngressDecision::Forward {
            selected_ssrc_changed: true,
        }
    }

    fn on_single_scalable_timer(&mut self) -> Option<KeyframeRequest> {
        if self.single_scalable_current.is_some() {
            return None;
        }
        self.acquisition_ticks = self.acquisition_ticks.saturating_add(1);
        if self.pli_ticks > 0 {
            self.pli_ticks -= 1;
            return None;
        }
        if self.remaining_pli_requests == 0 {
            return None;
        }

        let media_ssrc = self.single_scalable_seen?;
        self.pli_ticks = Self::KEYFRAME_RETRY_TICKS.saturating_sub(1);
        self.remaining_pli_requests -= 1;
        Some(KeyframeRequest { media_ssrc })
    }

    fn expire_stale_sources(&mut self) {
        if self.single_scalable_active {
            self.single_scalable_seen_age_ticks =
                self.single_scalable_seen_age_ticks.saturating_add(1);
            if self.single_scalable_seen_age_ticks >= Self::SOURCE_STALE_TICKS {
                self.single_scalable_seen = None;
            }
            if self.single_scalable_current.is_some() {
                self.single_scalable_current_age_ticks =
                    self.single_scalable_current_age_ticks.saturating_add(1);
                if self.single_scalable_current_age_ticks >= Self::SOURCE_STALE_TICKS {
                    self.single_scalable_current = None;
                }
            }
            return;
        }

        for index in 0..self.rtp_seen_ssrc.len() {
            self.seen_age_ticks[index] = self.seen_age_ticks[index].saturating_add(1);
            if self.seen_age_ticks[index] >= Self::SOURCE_STALE_TICKS {
                self.rtp_seen_ssrc[index] = None;
                self.decoder_usable_ssrc[index] = None;
            }
        }
        if let Some((ssrc, spatial)) = self.current
            && self.rtp_seen_ssrc[spatial as usize] != Some(ssrc)
        {
            self.current = None;
            self.waiting_for = self.policy.desired;
            self.acquisition_ticks = 0;
            self.fallback_started = false;
            self.fallback_locked = false;
            self.pli_ticks = 0;
            self.remaining_pli_requests = Self::MAX_KEYFRAME_REQUESTS_PER_ACQUISITION;
        }
    }

    fn best_decoder_usable_allowed(&self) -> Option<SpatialLayer> {
        [SpatialLayer::High, SpatialLayer::Medium, SpatialLayer::Low]
            .into_iter()
            .find(|layer| {
                *layer <= self.policy.max && self.decoder_usable_ssrc[*layer as usize].is_some()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        KeyframeRequest, LayerAcquisitionState, LayerPacketMetadata, LayerPolicy, SpatialLayer,
        SubscriberVideoLayerSelector, VideoIngressDecision, VideoSourceKind,
    };

    fn packet(ssrc: u32, spatial: SpatialLayer, keyframe: bool) -> LayerPacketMetadata {
        LayerPacketMetadata {
            ssrc,
            spatial: Some(spatial),
            source_kind: VideoSourceKind::Simulcast,
            is_decodable_switch_point: keyframe,
        }
    }

    #[test]
    fn acquisition_state_exposes_waiting_and_fallback_lock() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.acquisition_state(),
            LayerAcquisitionState::WaitingForDesired
        );
        let _ = selector.observe_packet(packet(10, SpatialLayer::Low, false));
        for _ in 0..SubscriberVideoLayerSelector::ACQUISITION_GRACE_TICKS {
            let _ = selector.on_timer();
        }
        assert_eq!(
            selector.acquisition_state(),
            LayerAcquisitionState::WaitingForFallback
        );
        assert_eq!(selector.waiting_for(), SpatialLayer::Low);
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(
            selector.acquisition_state(),
            LayerAcquisitionState::FallbackLocked
        );
    }

    #[test]
    fn single_scalable_source_acquires_without_conflating_packet_spatial_policy() {
        let mut selector = SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::Low));
        let delta = LayerPacketMetadata {
            ssrc: 10,
            spatial: None,
            source_kind: VideoSourceKind::SingleScalable,
            is_decodable_switch_point: false,
        };
        assert_eq!(
            selector.observe_packet(delta),
            VideoIngressDecision::DropWaitingForKeyframe
        );
        assert_eq!(
            selector.observe_packet(LayerPacketMetadata {
                is_decodable_switch_point: true,
                ..delta
            }),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(selector.selected_ssrc(), Some(10));
        assert_eq!(selector.current_spatial(), None);
        assert_eq!(selector.source_kind(), VideoSourceKind::SingleScalable);
        assert_eq!(
            selector.observe_packet(delta),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: false
            }
        );
    }

    #[test]
    fn stale_single_scalable_source_requires_a_fresh_decodable_boundary_to_replace() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        let first_source = LayerPacketMetadata {
            ssrc: 10,
            spatial: None,
            source_kind: VideoSourceKind::SingleScalable,
            is_decodable_switch_point: true,
        };
        assert_eq!(
            selector.observe_packet(first_source),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );

        let replacement_delta = LayerPacketMetadata {
            ssrc: 20,
            is_decodable_switch_point: false,
            ..first_source
        };
        assert_eq!(
            selector.observe_packet(replacement_delta),
            VideoIngressDecision::DropNonSelectedSsrc
        );
        for _ in 0..SubscriberVideoLayerSelector::SOURCE_STALE_TICKS {
            assert_eq!(
                selector.observe_packet(replacement_delta),
                VideoIngressDecision::DropNonSelectedSsrc
            );
            let _ = selector.on_timer();
        }
        assert_eq!(selector.selected_ssrc(), None);
        assert_eq!(
            selector.observe_packet(replacement_delta),
            VideoIngressDecision::DropWaitingForKeyframe
        );
        assert_eq!(
            selector.observe_packet(LayerPacketMetadata {
                is_decodable_switch_point: true,
                ..replacement_delta
            }),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(selector.selected_ssrc(), Some(20));
    }

    #[test]
    fn high_target_does_not_latch_low_when_high_keyframe_arrives_during_grace() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, true)),
            VideoIngressDecision::DropNonSelectedSsrc
        );
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(selector.selected_ssrc(), Some(30));
    }

    #[test]
    fn target_switch_waits_for_a_decodable_boundary() {
        let mut selector = SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::Low));
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        selector.set_policy(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, false)),
            VideoIngressDecision::DropWaitingForKeyframe
        );
        assert_eq!(selector.selected_ssrc(), Some(10));
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
    }

    #[test]
    fn acquisition_falls_back_once_to_best_seen_allowed_layer_and_retries_are_bounded() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, false)),
            VideoIngressDecision::DropNonSelectedSsrc
        );
        assert_eq!(selector.on_timer(), None);
        assert_eq!(selector.on_timer(), None);
        assert_eq!(selector.on_timer(), None);
        assert_eq!(
            selector.on_timer(),
            Some(KeyframeRequest { media_ssrc: 10 })
        );
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(selector.on_timer(), None);
        assert_eq!(selector.on_timer(), None);
        assert_eq!(selector.current_spatial(), Some(SpatialLayer::Low));
    }

    #[test]
    fn high_low_high_transitions_are_keyframe_gated() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        selector.set_policy(LayerPolicy::fixed(SpatialLayer::Low));
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, false)),
            VideoIngressDecision::DropWaitingForKeyframe
        );
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        selector.set_policy(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
    }

    #[test]
    fn selected_source_stays_live_when_later_metadata_disagrees() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );

        for _ in 0..(SubscriberVideoLayerSelector::SOURCE_STALE_TICKS + 1) {
            assert_eq!(
                selector.observe_packet(packet(30, SpatialLayer::Low, false)),
                VideoIngressDecision::Forward {
                    selected_ssrc_changed: false
                }
            );
            assert_eq!(selector.on_timer(), None);
        }
        assert_eq!(selector.selected_ssrc(), Some(30));
        assert_eq!(selector.current_spatial(), Some(SpatialLayer::High));
    }

    #[test]
    fn stale_current_source_reacquires_a_live_fallback_without_oscillation() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        // Keep the lower source live while the selected high source disappears.
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, false)),
            VideoIngressDecision::DropNonSelectedSsrc
        );

        for _ in 0..(SubscriberVideoLayerSelector::SOURCE_STALE_TICKS - 1) {
            let _ = selector.observe_packet(packet(10, SpatialLayer::Low, false));
            assert_eq!(selector.on_timer(), None);
            assert_eq!(selector.current_spatial(), Some(SpatialLayer::High));
        }

        // Expiry clears the unavailable current source, waits through acquisition grace, then
        // asks for a decodable low fallback once. It does not promote low on a delta packet.
        let _ = selector.observe_packet(packet(10, SpatialLayer::Low, false));
        assert_eq!(selector.on_timer(), None);
        for _ in 0..(SubscriberVideoLayerSelector::ACQUISITION_GRACE_TICKS - 2) {
            let _ = selector.observe_packet(packet(10, SpatialLayer::Low, false));
            assert_eq!(selector.on_timer(), None);
        }
        let _ = selector.observe_packet(packet(10, SpatialLayer::Low, false));
        assert_eq!(
            selector.on_timer(),
            Some(KeyframeRequest { media_ssrc: 10 })
        );
        assert_eq!(
            selector.observe_packet(packet(10, SpatialLayer::Low, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(selector.current_spatial(), Some(SpatialLayer::Low));

        for _ in 0..8 {
            let _ = selector.observe_packet(packet(10, SpatialLayer::Low, false));
            assert_eq!(selector.on_timer(), None);
            assert_eq!(selector.current_spatial(), Some(SpatialLayer::Low));
        }

        // A decodable desired-layer packet is concrete renewed availability. It may upgrade the
        // target even though the timer remains quiet while the fallback is stable.
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, true)),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(selector.current_spatial(), Some(SpatialLayer::High));
    }

    #[test]
    fn descriptor_rtp_seen_layer_is_not_fallback_usable_until_a_switch_boundary() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));

        assert_eq!(
            selector.observe_packet_with_dependency_descriptor_metadata(
                packet(10, SpatialLayer::Low, false),
                true,
            ),
            VideoIngressDecision::DropNonSelectedSsrc
        );
        for _ in 0..SubscriberVideoLayerSelector::ACQUISITION_GRACE_TICKS {
            assert_eq!(selector.on_timer(), None);
        }
        assert_eq!(selector.waiting_for(), SpatialLayer::High);

        // Once the descriptor-backed switch boundary arrives, low becomes a usable fallback
        // candidate. It is still not selected until the selector requests and receives a later
        // decodable boundary, preserving source-switch safety.
        assert_eq!(
            selector.observe_packet_with_dependency_descriptor_metadata(
                packet(10, SpatialLayer::Low, true),
                true,
            ),
            VideoIngressDecision::DropNonSelectedSsrc
        );
        assert_eq!(
            selector.on_timer(),
            Some(KeyframeRequest { media_ssrc: 10 })
        );
        assert_eq!(selector.waiting_for(), SpatialLayer::Low);
    }

    #[test]
    fn unavailable_target_has_a_bounded_keyframe_request_budget() {
        let mut selector =
            SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        assert_eq!(
            selector.observe_packet(packet(30, SpatialLayer::High, false)),
            VideoIngressDecision::DropWaitingForKeyframe
        );

        let mut requests = 0;
        for _ in 0..20 {
            requests += usize::from(selector.on_timer().is_some());
        }
        assert!(
            (1..=12).contains(&requests),
            "a target acquisition must be bounded without suppressing all retries (requests={requests})"
        );
    }

    #[test]
    fn targets_are_isolated_for_identical_interleaved_packets() {
        let mut high = SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::High));
        let mut low = SubscriberVideoLayerSelector::new(LayerPolicy::fixed(SpatialLayer::Low));
        for target in [&mut high, &mut low] {
            let _ = target.observe_packet(packet(10, SpatialLayer::Low, true));
            let _ = target.observe_packet(packet(30, SpatialLayer::High, true));
        }
        assert_eq!(high.current_spatial(), Some(SpatialLayer::High));
        assert_eq!(low.current_spatial(), Some(SpatialLayer::Low));
    }
}

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

pub(crate) const RTP_DUPLICATE_WINDOW: usize = 512;
pub(crate) const RTP_RETRANSMISSION_CACHE_SIZE: usize = 256;
const MEDIA_FEEDBACK_WINDOW_MILLIS: u64 = 5_000;
const MEDIA_FEEDBACK_DEGRADED_FRACTION_LOST_THRESHOLD: u8 = 64;
const MEDIA_FEEDBACK_MODERATE_FRACTION_LOST_THRESHOLD: u8 = 24;
const MEDIA_FEEDBACK_DEGRADED_MIN_REPORTS: u32 = 2;
const MEDIA_FEEDBACK_RECOVERY_MIN_REPORTS: u32 = 2;
const MEDIA_FEEDBACK_QUALITY_RECOMMENDATION_MIN_GAP_MILLIS: u64 = 2_000;

pub(crate) type ForwardTrackKey = (String, String, String, String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyFrameRequestKind {
    Pli,
    Fir,
}

#[derive(Debug, Clone, Default)]
struct KeyFrameRequestGate {
    last_pli_millis: Option<u64>,
    last_fir_millis: Option<u64>,
}

impl KeyFrameRequestGate {
    fn should_forward(
        &mut self,
        request: KeyFrameRequestKind,
        now_millis: u64,
        min_gap_millis: u64,
    ) -> bool {
        let last = match request {
            KeyFrameRequestKind::Pli => &mut self.last_pli_millis,
            KeyFrameRequestKind::Fir => &mut self.last_fir_millis,
        };

        if let Some(previous) = *last
            && now_millis.saturating_sub(previous) < min_gap_millis
        {
            return false;
        }

        *last = Some(now_millis);
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MappedSenderReport {
    pub(crate) ssrc: u32,
    pub(crate) rtp_timestamp: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct SenderReportMapper {
    target_ssrc: Option<u32>,
    base_incoming_rtp_timestamp: Option<u32>,
    base_outgoing_rtp_timestamp: Option<u32>,
}

impl SenderReportMapper {
    fn map(&mut self, target_ssrc: u32, incoming_rtp_timestamp: u32) -> MappedSenderReport {
        let outgoing_timestamp =
            if let (Some(base_in), Some(base_out), Some(existing_target_ssrc)) = (
                self.base_incoming_rtp_timestamp,
                self.base_outgoing_rtp_timestamp,
                self.target_ssrc,
            ) {
                if existing_target_ssrc == target_ssrc {
                    base_out.wrapping_add(incoming_rtp_timestamp.wrapping_sub(base_in))
                } else {
                    self.base_incoming_rtp_timestamp = Some(incoming_rtp_timestamp);
                    self.base_outgoing_rtp_timestamp = Some(incoming_rtp_timestamp);
                    self.target_ssrc = Some(target_ssrc);
                    incoming_rtp_timestamp
                }
            } else {
                self.base_incoming_rtp_timestamp = Some(incoming_rtp_timestamp);
                self.base_outgoing_rtp_timestamp = Some(incoming_rtp_timestamp);
                self.target_ssrc = Some(target_ssrc);
                incoming_rtp_timestamp
            };

        MappedSenderReport {
            ssrc: target_ssrc,
            rtp_timestamp: outgoing_timestamp,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecommendedVideoQuality {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct MediaFeedbackSummary {
    pub(crate) last_rr_ssrc: Option<u32>,
    pub(crate) rr_max_fraction_lost: u8,
    pub(crate) rr_report_count: u32,
    pub(crate) last_twcc_media_ssrc: Option<u32>,
    pub(crate) twcc_packet_status_count: u32,
    pub(crate) is_degraded: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct MediaFeedbackAdaptationState {
    last_recommended_quality: Option<RecommendedVideoQuality>,
    last_recommendation_millis: Option<u64>,
}

impl MediaFeedbackAdaptationState {
    fn maybe_recommend(
        &mut self,
        summary: MediaFeedbackSummary,
        now_millis: u64,
    ) -> Option<RecommendedVideoQuality> {
        let desired = if summary.is_degraded {
            Some(RecommendedVideoQuality::Low)
        } else if summary.rr_report_count >= MEDIA_FEEDBACK_RECOVERY_MIN_REPORTS
            && summary.rr_max_fraction_lost >= MEDIA_FEEDBACK_MODERATE_FRACTION_LOST_THRESHOLD
        {
            Some(RecommendedVideoQuality::Medium)
        } else if summary.rr_report_count >= MEDIA_FEEDBACK_RECOVERY_MIN_REPORTS {
            Some(RecommendedVideoQuality::High)
        } else {
            None
        }?;

        if self.last_recommended_quality == Some(desired) {
            return None;
        }

        if let Some(last_millis) = self.last_recommendation_millis
            && now_millis.saturating_sub(last_millis)
                < MEDIA_FEEDBACK_QUALITY_RECOMMENDATION_MIN_GAP_MILLIS
        {
            return None;
        }

        self.last_recommended_quality = Some(desired);
        self.last_recommendation_millis = Some(now_millis);
        Some(desired)
    }
}

#[derive(Debug, Clone, Default)]
struct MediaFeedbackState {
    rr_window_start_millis: Option<u64>,
    rr_window_last_ssrc: Option<u32>,
    rr_window_max_fraction_lost: u8,
    rr_window_report_count: u32,
    twcc_window_start_millis: Option<u64>,
    twcc_window_last_media_ssrc: Option<u32>,
    twcc_window_packet_status_count: u32,
}

impl MediaFeedbackState {
    fn observe_receiver_report(
        &mut self,
        now_millis: u64,
        ssrc: u32,
        max_fraction_lost: u8,
        report_count: u16,
    ) {
        self.refresh_rr_window(now_millis);
        self.rr_window_last_ssrc = Some(ssrc);
        self.rr_window_max_fraction_lost = self.rr_window_max_fraction_lost.max(max_fraction_lost);
        self.rr_window_report_count = self
            .rr_window_report_count
            .saturating_add(u32::from(report_count));
    }

    fn observe_transport_wide_cc(
        &mut self,
        now_millis: u64,
        media_ssrc: u32,
        packet_status_count: u16,
    ) {
        self.refresh_twcc_window(now_millis);
        self.twcc_window_last_media_ssrc = Some(media_ssrc);
        self.twcc_window_packet_status_count = self
            .twcc_window_packet_status_count
            .saturating_add(u32::from(packet_status_count));
    }

    fn summary(&mut self, now_millis: u64) -> MediaFeedbackSummary {
        self.refresh_rr_window(now_millis);
        self.refresh_twcc_window(now_millis);

        let rr_report_count = self.rr_window_report_count;
        let rr_max_fraction_lost = self.rr_window_max_fraction_lost;
        let is_degraded = rr_report_count >= MEDIA_FEEDBACK_DEGRADED_MIN_REPORTS
            && rr_max_fraction_lost >= MEDIA_FEEDBACK_DEGRADED_FRACTION_LOST_THRESHOLD;

        MediaFeedbackSummary {
            last_rr_ssrc: self.rr_window_last_ssrc,
            rr_max_fraction_lost,
            rr_report_count,
            last_twcc_media_ssrc: self.twcc_window_last_media_ssrc,
            twcc_packet_status_count: self.twcc_window_packet_status_count,
            is_degraded,
        }
    }

    fn refresh_rr_window(&mut self, now_millis: u64) {
        match self.rr_window_start_millis {
            Some(window_start)
                if now_millis.saturating_sub(window_start) <= MEDIA_FEEDBACK_WINDOW_MILLIS => {}
            _ => {
                self.rr_window_start_millis = Some(now_millis);
                self.rr_window_last_ssrc = None;
                self.rr_window_max_fraction_lost = 0;
                self.rr_window_report_count = 0;
            }
        }
    }

    fn refresh_twcc_window(&mut self, now_millis: u64) {
        match self.twcc_window_start_millis {
            Some(window_start)
                if now_millis.saturating_sub(window_start) <= MEDIA_FEEDBACK_WINDOW_MILLIS => {}
            _ => {
                self.twcc_window_start_millis = Some(now_millis);
                self.twcc_window_last_media_ssrc = None;
                self.twcc_window_packet_status_count = 0;
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SubscriberRtpState {
    highest_incoming_ext_seq_by_ssrc: HashMap<u32, u64>,
    seen_incoming_ext_seq: HashSet<(u32, u64)>,
    incoming_ext_seq_order: VecDeque<(u32, u64)>,
    next_outgoing_ext_seq: Option<u64>,
    timestamp_mapper_incoming_ssrc: Option<u32>,
    timestamp_mapper_base_incoming: Option<u32>,
    timestamp_mapper_base_outgoing: Option<u32>,
    timestamp_mapper_last_outgoing: Option<u32>,
    retransmission_cache: HashMap<u16, rtc::rtp::Packet>,
    retransmission_order: VecDeque<u16>,
    keyframe_gate: KeyFrameRequestGate,
    fir_sequence_numbers: HashMap<u32, u8>,
    sender_report_mapper: SenderReportMapper,
    media_feedback: MediaFeedbackState,
    media_feedback_adaptation: MediaFeedbackAdaptationState,
}

impl SubscriberRtpState {
    fn rewrite_packet(
        &mut self,
        packet: &rtc::rtp::Packet,
        target_ssrc: Option<u32>,
        negotiated_payload_type: Option<u8>,
    ) -> Option<rtc::rtp::Packet> {
        let incoming_ssrc = packet.header.ssrc;
        let incoming_seq = packet.header.sequence_number;
        let (incoming_ext_seq, is_new_highest) =
            self.extend_sequence_number(incoming_ssrc, incoming_seq);
        let incoming_key = (incoming_ssrc, incoming_ext_seq);

        if !is_new_highest && !self.seen_incoming_ext_seq.insert(incoming_key) {
            return None;
        }

        let outgoing_ext_seq = if let Some(next) = self.next_outgoing_ext_seq {
            next
        } else {
            incoming_ext_seq
        };
        self.next_outgoing_ext_seq = Some(outgoing_ext_seq.saturating_add(1));

        if is_new_highest {
            self.seen_incoming_ext_seq.insert(incoming_key);
        }
        self.incoming_ext_seq_order.push_back(incoming_key);
        while self.incoming_ext_seq_order.len() > RTP_DUPLICATE_WINDOW {
            if let Some(evicted_incoming_ext_seq) = self.incoming_ext_seq_order.pop_front() {
                self.seen_incoming_ext_seq.remove(&evicted_incoming_ext_seq);
            }
        }

        let mut rewritten_packet = packet.clone();
        if let Some(payload_type) = negotiated_payload_type {
            rewritten_packet.header.payload_type = payload_type;
        }
        rewritten_packet.header.sequence_number = outgoing_ext_seq as u16;
        rewritten_packet.header.timestamp =
            self.map_packet_timestamp(incoming_ssrc, rewritten_packet.header.timestamp);
        if let Some(target_ssrc) = target_ssrc {
            rewritten_packet.header.ssrc = target_ssrc;
        }

        self.cache_retransmission_packet(rewritten_packet.clone());
        Some(rewritten_packet)
    }

    fn retransmission_packet(&self, outgoing_sequence_number: u16) -> Option<rtc::rtp::Packet> {
        self.retransmission_cache
            .get(&outgoing_sequence_number)
            .cloned()
    }

    fn should_forward_keyframe_request(
        &mut self,
        request: KeyFrameRequestKind,
        now_millis: u64,
        min_gap_millis: u64,
    ) -> bool {
        self.keyframe_gate
            .should_forward(request, now_millis, min_gap_millis)
    }

    fn map_sender_report(
        &mut self,
        target_ssrc: u32,
        incoming_rtp_timestamp: u32,
    ) -> MappedSenderReport {
        self.sender_report_mapper
            .map(target_ssrc, incoming_rtp_timestamp)
    }

    fn next_fir_sequence_number(&mut self, media_ssrc: u32) -> u8 {
        let entry = self.fir_sequence_numbers.entry(media_ssrc).or_insert(0);
        let sequence_number = *entry;
        *entry = entry.wrapping_add(1);
        sequence_number
    }

    fn observe_receiver_report(
        &mut self,
        now_millis: u64,
        ssrc: u32,
        max_fraction_lost: u8,
        report_count: u16,
    ) {
        self.media_feedback.observe_receiver_report(
            now_millis,
            ssrc,
            max_fraction_lost,
            report_count,
        );
    }

    fn observe_transport_wide_cc(
        &mut self,
        now_millis: u64,
        media_ssrc: u32,
        packet_status_count: u16,
    ) {
        self.media_feedback
            .observe_transport_wide_cc(now_millis, media_ssrc, packet_status_count);
    }

    fn media_feedback_summary(&mut self, now_millis: u64) -> MediaFeedbackSummary {
        self.media_feedback.summary(now_millis)
    }

    fn recommend_video_quality(&mut self, now_millis: u64) -> Option<RecommendedVideoQuality> {
        let summary = self.media_feedback.summary(now_millis);
        self.media_feedback_adaptation
            .maybe_recommend(summary, now_millis)
    }

    fn map_packet_timestamp(&mut self, incoming_ssrc: u32, incoming_timestamp: u32) -> u32 {
        let outgoing_timestamp =
            if let (Some(mapped_ssrc), Some(base_incoming), Some(base_outgoing)) = (
                self.timestamp_mapper_incoming_ssrc,
                self.timestamp_mapper_base_incoming,
                self.timestamp_mapper_base_outgoing,
            ) {
                if mapped_ssrc == incoming_ssrc {
                    base_outgoing.wrapping_add(incoming_timestamp.wrapping_sub(base_incoming))
                } else {
                    let resumed = self
                        .timestamp_mapper_last_outgoing
                        .map(|last| last.wrapping_add(1))
                        .unwrap_or(incoming_timestamp);
                    tracing::debug!(
                        previous_incoming_ssrc = mapped_ssrc,
                        new_incoming_ssrc = incoming_ssrc,
                        incoming_timestamp,
                        previous_base_incoming_timestamp = base_incoming,
                        previous_base_outgoing_timestamp = base_outgoing,
                        previous_last_outgoing_timestamp = ?self.timestamp_mapper_last_outgoing,
                        resumed_outgoing_timestamp = resumed,
                        "video_source_switch_timestamp_resumed"
                    );
                    self.timestamp_mapper_incoming_ssrc = Some(incoming_ssrc);
                    self.timestamp_mapper_base_incoming = Some(incoming_timestamp);
                    self.timestamp_mapper_base_outgoing = Some(resumed);
                    resumed
                }
            } else {
                self.timestamp_mapper_incoming_ssrc = Some(incoming_ssrc);
                self.timestamp_mapper_base_incoming = Some(incoming_timestamp);
                self.timestamp_mapper_base_outgoing = Some(incoming_timestamp);
                incoming_timestamp
            };

        self.timestamp_mapper_last_outgoing = Some(outgoing_timestamp);
        outgoing_timestamp
    }

    fn cache_retransmission_packet(&mut self, packet: rtc::rtp::Packet) {
        let outgoing_sequence_number = packet.header.sequence_number;
        match self.retransmission_cache.entry(outgoing_sequence_number) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(packet);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.retransmission_order
                    .push_back(outgoing_sequence_number);
                entry.insert(packet);
            }
        }

        while self.retransmission_order.len() > RTP_RETRANSMISSION_CACHE_SIZE {
            if let Some(evicted_sequence_number) = self.retransmission_order.pop_front() {
                self.retransmission_cache.remove(&evicted_sequence_number);
            }
        }
    }

    fn extend_sequence_number(
        &mut self,
        incoming_ssrc: u32,
        incoming_sequence_number: u16,
    ) -> (u64, bool) {
        let incoming_sequence_number = incoming_sequence_number as u64;
        match self.highest_incoming_ext_seq_by_ssrc.entry(incoming_ssrc) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let extended =
                    extend_sequence_number_from_reference(incoming_sequence_number, *entry.get());
                let should_update = extended > *entry.get();
                if should_update {
                    entry.insert(extended);
                }
                (extended, should_update)
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(incoming_sequence_number);
                (incoming_sequence_number, true)
            }
        }
    }
}

pub(crate) fn extend_sequence_number_from_reference(
    incoming_sequence_number: u64,
    reference_ext_seq: u64,
) -> u64 {
    let seq16 = incoming_sequence_number & 0xffff;
    let reference_cycle = (reference_ext_seq >> 16) as i128;
    let reference_value = reference_ext_seq as i128;

    let mut best_candidate = reference_ext_seq;
    let mut best_distance = i128::MAX;

    for cycle in [reference_cycle - 1, reference_cycle, reference_cycle + 1] {
        if cycle < 0 {
            continue;
        }
        let candidate = ((cycle << 16) | seq16 as i128) as u64;
        let distance = (candidate as i128 - reference_value).abs();
        if distance < best_distance {
            best_candidate = candidate;
            best_distance = distance;
        }
    }

    best_candidate
}

type SubscriberRtpStateHandle = Arc<Mutex<SubscriberRtpState>>;

/// Target-local handle for steady-state RTP rewriting.
///
/// The owning forwarding reader resolves this once when its target snapshot changes,
/// while RTCP callbacks continue to reach the same state through [`RtpForwardingStore`].
#[derive(Clone)]
pub(crate) struct SubscriberRtpForwarder {
    state: SubscriberRtpStateHandle,
}

impl SubscriberRtpForwarder {
    pub(crate) fn rewrite_packet_with_target_ssrc_and_payload_type(
        &self,
        packet: &rtc::rtp::Packet,
        target_ssrc: Option<u32>,
        negotiated_payload_type: Option<u8>,
    ) -> Option<rtc::rtp::Packet> {
        self.state.lock().ok().and_then(|mut state| {
            state.rewrite_packet(packet, target_ssrc, negotiated_payload_type)
        })
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RtpForwardingStore {
    states: Arc<Mutex<HashMap<ForwardTrackKey, SubscriberRtpStateHandle>>>,
}

impl RtpForwardingStore {
    fn get_or_insert_state(&self, key: &ForwardTrackKey) -> Option<SubscriberRtpStateHandle> {
        self.states.lock().ok().map(|mut states| {
            states
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(SubscriberRtpState::default())))
                .clone()
        })
    }

    /// Resolves one target's rewrite state for use in a forwarding-reader snapshot.
    pub(crate) fn forwarder_for(&self, key: &ForwardTrackKey) -> Option<SubscriberRtpForwarder> {
        self.get_or_insert_state(key)
            .map(|state| SubscriberRtpForwarder { state })
    }

    fn get_state(&self, key: &ForwardTrackKey) -> Option<SubscriberRtpStateHandle> {
        self.states
            .lock()
            .ok()
            .and_then(|states| states.get(key).cloned())
    }

    #[allow(dead_code)]
    pub(crate) fn rewrite_packet_for_subscriber(
        &self,
        key: &ForwardTrackKey,
        packet: rtc::rtp::Packet,
    ) -> Option<rtc::rtp::Packet> {
        self.rewrite_packet_for_subscriber_with_target_ssrc(key, packet, None)
    }

    pub(crate) fn rewrite_packet_for_subscriber_with_target_ssrc(
        &self,
        key: &ForwardTrackKey,
        packet: rtc::rtp::Packet,
        target_ssrc: Option<u32>,
    ) -> Option<rtc::rtp::Packet> {
        self.rewrite_packet_for_subscriber_with_target_ssrc_and_payload_type(
            key,
            packet,
            target_ssrc,
            None,
        )
    }

    /// Rewrites an RTP packet using the payload type negotiated by this forwarding target.
    pub(crate) fn rewrite_packet_for_subscriber_with_target_ssrc_and_payload_type(
        &self,
        key: &ForwardTrackKey,
        packet: rtc::rtp::Packet,
        target_ssrc: Option<u32>,
        negotiated_payload_type: Option<u8>,
    ) -> Option<rtc::rtp::Packet> {
        self.forwarder_for(key)?
            .rewrite_packet_with_target_ssrc_and_payload_type(
                &packet,
                target_ssrc,
                negotiated_payload_type,
            )
    }

    pub(crate) fn get_retransmission_packet(
        &self,
        key: &ForwardTrackKey,
        outgoing_sequence_number: u16,
    ) -> Option<rtc::rtp::Packet> {
        self.get_state(key).and_then(|state| {
            state
                .lock()
                .ok()
                .and_then(|state| state.retransmission_packet(outgoing_sequence_number))
        })
    }

    pub(crate) fn should_forward_keyframe_request(
        &self,
        key: &ForwardTrackKey,
        request: KeyFrameRequestKind,
        now_millis: u64,
        min_gap_millis: u64,
    ) -> bool {
        self.get_or_insert_state(key)
            .and_then(|state| {
                state.lock().ok().map(|mut state| {
                    state.should_forward_keyframe_request(request, now_millis, min_gap_millis)
                })
            })
            .unwrap_or(true)
    }

    pub(crate) fn next_fir_sequence_number(&self, key: &ForwardTrackKey, media_ssrc: u32) -> u8 {
        self.get_or_insert_state(key)
            .and_then(|state| {
                state
                    .lock()
                    .ok()
                    .map(|mut state| state.next_fir_sequence_number(media_ssrc))
            })
            .unwrap_or(0)
    }

    pub(crate) fn map_sender_report(
        &self,
        key: &ForwardTrackKey,
        target_ssrc: u32,
        incoming_rtp_timestamp: u32,
    ) -> MappedSenderReport {
        self.get_or_insert_state(key)
            .and_then(|state| {
                state
                    .lock()
                    .ok()
                    .map(|mut state| state.map_sender_report(target_ssrc, incoming_rtp_timestamp))
            })
            .unwrap_or(MappedSenderReport {
                ssrc: target_ssrc,
                rtp_timestamp: incoming_rtp_timestamp,
            })
    }

    pub(crate) fn observe_receiver_report(
        &self,
        key: &ForwardTrackKey,
        now_millis: u64,
        ssrc: u32,
        max_fraction_lost: u8,
        report_count: u16,
    ) {
        if let Some(state) = self.get_or_insert_state(key)
            && let Ok(mut state) = state.lock()
        {
            state.observe_receiver_report(now_millis, ssrc, max_fraction_lost, report_count);
        }
    }

    pub(crate) fn observe_transport_wide_cc(
        &self,
        key: &ForwardTrackKey,
        now_millis: u64,
        media_ssrc: u32,
        packet_status_count: u16,
    ) {
        if let Some(state) = self.get_or_insert_state(key)
            && let Ok(mut state) = state.lock()
        {
            state.observe_transport_wide_cc(now_millis, media_ssrc, packet_status_count);
        }
    }

    pub(crate) fn media_feedback_summary(
        &self,
        key: &ForwardTrackKey,
        now_millis: u64,
    ) -> MediaFeedbackSummary {
        self.get_or_insert_state(key)
            .and_then(|state| {
                state
                    .lock()
                    .ok()
                    .map(|mut state| state.media_feedback_summary(now_millis))
            })
            .unwrap_or_default()
    }

    pub(crate) fn recommend_video_quality(
        &self,
        key: &ForwardTrackKey,
        now_millis: u64,
    ) -> Option<RecommendedVideoQuality> {
        self.get_or_insert_state(key).and_then(|state| {
            state
                .lock()
                .ok()
                .and_then(|mut state| state.recommend_video_quality(now_millis))
        })
    }

    pub(crate) fn remove(
        &self,
        room: &str,
        publisher_identity: &str,
        track_sid: &str,
        subscriber_identity: &str,
    ) {
        if let Ok(mut states) = self.states.lock() {
            states.remove(&(
                room.to_string(),
                publisher_identity.to_string(),
                track_sid.to_string(),
                subscriber_identity.to_string(),
            ));
        }
    }

    pub(crate) fn remove_track(&self, room: &str, publisher_identity: &str, track_sid: &str) {
        if let Ok(mut states) = self.states.lock() {
            states.retain(
                |(
                    candidate_room,
                    candidate_publisher,
                    candidate_track_sid,
                    _subscriber_identity,
                ),
                 _| {
                    candidate_room != room
                        || candidate_publisher != publisher_identity
                        || candidate_track_sid != track_sid
                },
            );
        }
    }

    pub(crate) fn remove_participant(&self, room: &str, identity: &str) {
        if let Ok(mut states) = self.states.lock() {
            states.retain(
                |(candidate_room, publisher_identity, _track_sid, subscriber_identity), _| {
                    candidate_room != room
                        || (publisher_identity != identity && subscriber_identity != identity)
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forwarding_key(subscriber: &str) -> ForwardTrackKey {
        (
            "room-a".to_string(),
            "publisher-a".to_string(),
            "track-a".to_string(),
            subscriber.to_string(),
        )
    }

    fn packet_with_seq(sequence_number: u16) -> rtc::rtp::Packet {
        rtc::rtp::Packet {
            header: rtc::rtp::header::Header {
                sequence_number,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn packet_with_seq_ssrc_timestamp(
        sequence_number: u16,
        ssrc: u32,
        timestamp: u32,
    ) -> rtc::rtp::Packet {
        rtc::rtp::Packet {
            header: rtc::rtp::header::Header {
                sequence_number,
                ssrc,
                timestamp,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn extend_sequence_number_from_reference_tracks_small_increments_without_collapse() {
        let reference = 0_u64;
        assert_eq!(extend_sequence_number_from_reference(1, reference), 1);
        assert_eq!(extend_sequence_number_from_reference(2, 1), 2);
        assert_eq!(extend_sequence_number_from_reference(300, 299), 300);
    }

    #[test]
    fn rewrite_packet_tracks_highest_extended_sequence_across_wrap_and_out_of_order_packets() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        let packets = [65533_u16, 65534_u16, 2_u16, 65535_u16];
        for sequence_number in packets {
            let _ = store.rewrite_packet_for_subscriber(
                &key,
                packet_with_seq_ssrc_timestamp(sequence_number, 123, sequence_number as u32),
            );
        }

        let highest = {
            let states = store
                .states
                .lock()
                .expect("rtp forwarding store lock should not be poisoned");
            let state = states
                .get(&key)
                .expect("subscriber forwarding state should exist")
                .lock()
                .expect("subscriber forwarding state lock should not be poisoned");
            state
                .highest_incoming_ext_seq_by_ssrc
                .get(&123)
                .copied()
                .expect("ssrc sequence state should exist")
        };

        assert_eq!(highest as u16, 2);
        assert_eq!(highest, 65_536 + 2);
    }

    #[test]
    fn rewrite_packet_drops_duplicate_incoming_sequence_number() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        let first = store
            .rewrite_packet_for_subscriber(&key, packet_with_seq_ssrc_timestamp(23333, 123, 1_000))
            .expect("first packet should forward");
        assert_eq!(first.header.sequence_number, 23333);

        let duplicate = store
            .rewrite_packet_for_subscriber(&key, packet_with_seq_ssrc_timestamp(23333, 123, 1_000));
        assert!(duplicate.is_none(), "duplicate packet should be dropped");

        let next = store
            .rewrite_packet_for_subscriber(&key, packet_with_seq_ssrc_timestamp(23334, 123, 1_960))
            .expect("next unique packet should forward");
        assert_eq!(next.header.sequence_number, 23334);
    }

    #[test]
    fn target_rewrites_borrow_the_source_packet_and_cache_each_target_representation() {
        let store = RtpForwardingStore::default();
        let source = rtc::rtp::Packet {
            header: rtc::rtp::header::Header {
                sequence_number: 1234,
                timestamp: 56_789,
                ssrc: 0x1111_0001,
                payload_type: 96,
                ..Default::default()
            },
            ..Default::default()
        };
        let source_header = source.header.clone();

        let first_key = forwarding_key("subscriber-a");
        let first = store
            .forwarder_for(&first_key)
            .expect("first target should have forwarding state")
            .rewrite_packet_with_target_ssrc_and_payload_type(&source, Some(0x2222_0002), Some(111))
            .expect("first target should rewrite");
        let second_key = forwarding_key("subscriber-b");
        let second = store
            .forwarder_for(&second_key)
            .expect("second target should have forwarding state")
            .rewrite_packet_with_target_ssrc_and_payload_type(&source, Some(0x3333_0003), Some(112))
            .expect("second target should rewrite");

        assert_eq!(
            source.header, source_header,
            "source packet must remain reusable"
        );
        assert_eq!(first.header.ssrc, 0x2222_0002);
        assert_eq!(first.header.payload_type, 111);
        assert_eq!(second.header.ssrc, 0x3333_0003);
        assert_eq!(second.header.payload_type, 112);
        assert_eq!(
            store
                .get_retransmission_packet(&first_key, first.header.sequence_number)
                .expect("first target rewrite should be cached"),
            first
        );
        assert_eq!(
            store
                .get_retransmission_packet(&second_key, second.header.sequence_number)
                .expect("second target rewrite should be cached"),
            second
        );
    }

    #[test]
    fn rewrite_packet_with_target_ssrc_rewrites_ssrc_before_caching() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        let rewritten = store
            .rewrite_packet_for_subscriber_with_target_ssrc(
                &key,
                packet_with_seq(1234),
                Some(0x1234_5678),
            )
            .expect("packet should rewrite");

        assert_eq!(rewritten.header.ssrc, 0x1234_5678);

        let cached = store
            .get_retransmission_packet(&key, rewritten.header.sequence_number)
            .expect("rewritten packet should be cached");
        assert_eq!(cached.header.ssrc, 0x1234_5678);
    }

    #[test]
    fn retransmission_cache_evicts_oldest_packet_beyond_capacity() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        for sequence_number in 0..(RTP_RETRANSMISSION_CACHE_SIZE as u16 + 10) {
            let _ = store.rewrite_packet_for_subscriber(&key, packet_with_seq(sequence_number));
        }

        let (cache_len, first_retained, last_retained) = {
            let states = store
                .states
                .lock()
                .expect("rtp forwarding store lock should not be poisoned");
            let state = states
                .get(&key)
                .expect("subscriber forwarding state should exist")
                .lock()
                .expect("subscriber forwarding state lock should not be poisoned");
            (
                state.retransmission_cache.len(),
                state
                    .retransmission_order
                    .front()
                    .copied()
                    .unwrap_or_default(),
                state
                    .retransmission_order
                    .back()
                    .copied()
                    .unwrap_or_default(),
            )
        };

        assert_eq!(cache_len, RTP_RETRANSMISSION_CACHE_SIZE);
        assert_eq!(first_retained, 10);
        assert_eq!(last_retained, RTP_RETRANSMISSION_CACHE_SIZE as u16 + 9);

        assert!(
            store.get_retransmission_packet(&key, 0).is_none(),
            "oldest packet should be evicted from retransmission cache"
        );
        let newest = RTP_RETRANSMISSION_CACHE_SIZE as u16 + 9;
        assert!(
            store.get_retransmission_packet(&key, newest).is_some(),
            "newest packet should remain in retransmission cache"
        );
    }

    #[test]
    fn retransmission_cache_is_isolated_per_subscriber() {
        let store = RtpForwardingStore::default();
        let key_a = forwarding_key("subscriber-a");
        let key_b = forwarding_key("subscriber-b");

        let _ = store.rewrite_packet_for_subscriber(&key_a, packet_with_seq(100));

        assert!(store.get_retransmission_packet(&key_a, 100).is_some());
        assert!(
            store.get_retransmission_packet(&key_b, 100).is_none(),
            "retransmission packet should not leak across subscriber states"
        );
    }

    #[test]
    fn rewrite_packet_preserves_outgoing_timestamp_continuity_across_incoming_ssrc_switches() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        let first = store
            .rewrite_packet_for_subscriber_with_target_ssrc(
                &key,
                packet_with_seq_ssrc_timestamp(1, 0x1111_0001, 1_000),
                Some(0x9999_0001),
            )
            .expect("first packet should rewrite");
        let second = store
            .rewrite_packet_for_subscriber_with_target_ssrc(
                &key,
                packet_with_seq_ssrc_timestamp(2, 0x1111_0001, 1_960),
                Some(0x9999_0001),
            )
            .expect("second packet should rewrite");
        let switched = store
            .rewrite_packet_for_subscriber_with_target_ssrc(
                &key,
                packet_with_seq_ssrc_timestamp(3, 0x2222_0002, 120),
                Some(0x9999_0001),
            )
            .expect("switched-ssrc packet should rewrite");

        assert_eq!(first.header.timestamp, 1_000);
        assert_eq!(second.header.timestamp, 1_960);
        assert!(
            switched.header.timestamp > second.header.timestamp,
            "outgoing timestamp should continue increasing across incoming SSRC switch"
        );

        let switched_next = store
            .rewrite_packet_for_subscriber_with_target_ssrc(
                &key,
                packet_with_seq_ssrc_timestamp(4, 0x2222_0002, 1_080),
                Some(0x9999_0001),
            )
            .expect("next switched-ssrc packet should rewrite");
        assert_eq!(
            switched_next
                .header
                .timestamp
                .wrapping_sub(switched.header.timestamp),
            960,
            "timestamp delta should remain stable for packets on the new incoming SSRC"
        );
    }

    #[test]
    fn sender_report_mapping_preserves_monotonic_timestamp_delta_for_same_target_ssrc() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        let first = store.map_sender_report(&key, 1234, 10_000);
        let second = store.map_sender_report(&key, 1234, 10_480);

        assert_eq!(first.ssrc, 1234);
        assert_eq!(second.ssrc, 1234);
        assert_eq!(second.rtp_timestamp.wrapping_sub(first.rtp_timestamp), 480);
    }

    #[test]
    fn media_feedback_summary_marks_degraded_after_sustained_receiver_report_loss() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        store.observe_receiver_report(&key, 1_000, 0x11, 80, 1);
        let first = store.media_feedback_summary(&key, 1_100);
        assert_eq!(first.rr_report_count, 1);
        assert_eq!(first.rr_max_fraction_lost, 80);
        assert!(
            !first.is_degraded,
            "single high-loss RR should not immediately mark sustained degradation"
        );

        store.observe_receiver_report(&key, 1_300, 0x11, 90, 1);
        let second = store.media_feedback_summary(&key, 1_350);
        assert_eq!(second.rr_report_count, 2);
        assert_eq!(second.rr_max_fraction_lost, 90);
        assert!(second.is_degraded);
    }

    #[test]
    fn media_feedback_summary_resets_after_feedback_window_expires() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        store.observe_receiver_report(&key, 2_000, 0x22, 75, 2);
        let before_expiry = store.media_feedback_summary(&key, 2_300);
        assert_eq!(before_expiry.rr_report_count, 2);
        assert!(before_expiry.is_degraded);

        let after_expiry = store.media_feedback_summary(&key, 7_100);
        assert_eq!(after_expiry.rr_report_count, 0);
        assert_eq!(after_expiry.rr_max_fraction_lost, 0);
        assert!(!after_expiry.is_degraded);
    }

    #[test]
    fn media_feedback_summary_tracks_twcc_packet_status_counts_with_windowing() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        store.observe_transport_wide_cc(&key, 10_000, 0x44, 10);
        store.observe_transport_wide_cc(&key, 10_200, 0x44, 7);

        let in_window = store.media_feedback_summary(&key, 10_300);
        assert_eq!(in_window.last_twcc_media_ssrc, Some(0x44));
        assert_eq!(in_window.twcc_packet_status_count, 17);

        let after_expiry = store.media_feedback_summary(&key, 15_500);
        assert_eq!(after_expiry.twcc_packet_status_count, 0);
        assert_eq!(after_expiry.last_twcc_media_ssrc, None);
    }

    #[test]
    fn recommend_video_quality_transitions_low_on_degraded_then_high_on_recovery() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        store.observe_receiver_report(&key, 1_000, 0x55, 80, 1);
        store.observe_receiver_report(&key, 1_200, 0x55, 85, 1);

        let degraded = store.recommend_video_quality(&key, 1_300);
        assert_eq!(degraded, Some(RecommendedVideoQuality::Low));

        let duplicate = store.recommend_video_quality(&key, 1_400);
        assert_eq!(duplicate, None);

        store.observe_receiver_report(&key, 8_000, 0x55, 10, 1);
        store.observe_receiver_report(&key, 8_300, 0x55, 12, 1);

        let recovered = store.recommend_video_quality(&key, 8_500);
        assert_eq!(recovered, Some(RecommendedVideoQuality::High));
    }

    #[test]
    fn recommend_video_quality_requires_recovery_window_reset_after_degraded() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        store.observe_receiver_report(&key, 1_000, 0x66, 90, 1);
        store.observe_receiver_report(&key, 1_200, 0x66, 91, 1);
        assert_eq!(
            store.recommend_video_quality(&key, 1_300),
            Some(RecommendedVideoQuality::Low)
        );

        store.observe_receiver_report(&key, 2_000, 0x66, 10, 1);
        store.observe_receiver_report(&key, 2_200, 0x66, 11, 1);
        assert_eq!(
            store.recommend_video_quality(&key, 2_300),
            None,
            "same feedback window should preserve degraded signal"
        );

        store.observe_receiver_report(&key, 7_000, 0x66, 8, 1);
        store.observe_receiver_report(&key, 7_200, 0x66, 9, 1);
        assert_eq!(
            store.recommend_video_quality(&key, 7_300),
            Some(RecommendedVideoQuality::High)
        );
    }

    #[test]
    fn recommend_video_quality_emits_medium_for_moderate_loss() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        store.observe_receiver_report(&key, 1_000, 0x69, 26, 1);
        store.observe_receiver_report(&key, 1_200, 0x69, 28, 1);

        assert_eq!(
            store.recommend_video_quality(&key, 1_300),
            Some(RecommendedVideoQuality::Medium)
        );
    }

    #[test]
    fn recommend_video_quality_transitions_low_to_medium_to_high() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        // Degraded -> Low
        store.observe_receiver_report(&key, 1_000, 0x70, 80, 1);
        store.observe_receiver_report(&key, 1_100, 0x70, 82, 1);
        assert_eq!(
            store.recommend_video_quality(&key, 1_200),
            Some(RecommendedVideoQuality::Low)
        );

        // Moderate recovery -> Medium (after old degraded window expires + min-gap)
        store.observe_receiver_report(&key, 7_000, 0x70, 30, 1);
        store.observe_receiver_report(&key, 7_200, 0x70, 32, 1);
        assert_eq!(
            store.recommend_video_quality(&key, 7_300),
            Some(RecommendedVideoQuality::Medium)
        );

        // Strong recovery -> High (after moderate-loss window expires + min-gap)
        store.observe_receiver_report(&key, 13_000, 0x70, 5, 1);
        store.observe_receiver_report(&key, 13_200, 0x70, 6, 1);
        assert_eq!(
            store.recommend_video_quality(&key, 13_300),
            Some(RecommendedVideoQuality::High)
        );
    }

    #[test]
    fn recommend_video_quality_isolated_per_subscriber_key() {
        let store = RtpForwardingStore::default();
        let key_a = forwarding_key("subscriber-a");
        let key_b = forwarding_key("subscriber-b");

        store.observe_receiver_report(&key_a, 1_000, 0x77, 80, 1);
        store.observe_receiver_report(&key_a, 1_100, 0x77, 82, 1);
        assert_eq!(
            store.recommend_video_quality(&key_a, 1_200),
            Some(RecommendedVideoQuality::Low)
        );

        store.observe_receiver_report(&key_b, 1_000, 0x88, 5, 1);
        store.observe_receiver_report(&key_b, 1_100, 0x88, 7, 1);
        assert_eq!(
            store.recommend_video_quality(&key_b, 1_200),
            Some(RecommendedVideoQuality::High)
        );
    }

    #[test]
    fn recommend_video_quality_rate_limits_flip_flops_within_min_gap() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        store.observe_receiver_report(&key, 100, 0x90, 2, 1);
        store.observe_receiver_report(&key, 200, 0x90, 3, 1);
        assert_eq!(
            store.recommend_video_quality(&key, 300),
            Some(RecommendedVideoQuality::High)
        );

        store.observe_receiver_report(&key, 350, 0x90, 90, 1);
        store.observe_receiver_report(&key, 450, 0x90, 91, 1);
        assert_eq!(
            store.recommend_video_quality(&key, 500),
            None,
            "degraded recommendation should be rate-limited immediately after high recommendation"
        );

        assert_eq!(
            store.recommend_video_quality(&key, 2_600),
            Some(RecommendedVideoQuality::Low),
            "degraded recommendation should emit again once min-gap window expires"
        );
    }

    #[test]
    fn recommend_video_quality_handles_interleaved_multi_subscriber_feedback_under_pressure() {
        let store = RtpForwardingStore::default();
        let key_a = forwarding_key("subscriber-a");
        let key_b = forwarding_key("subscriber-b");

        for idx in 0..8_u64 {
            let now = 1_000 + idx * 100;
            store.observe_receiver_report(&key_a, now, 0xA1, 80 + (idx as u8 % 5), 1);
            store.observe_transport_wide_cc(&key_a, now + 10, 0xA1, 4);

            store.observe_receiver_report(&key_b, now, 0xB1, 4 + (idx as u8 % 3), 1);
            store.observe_transport_wide_cc(&key_b, now + 10, 0xB1, 6);
        }

        let a_summary = store.media_feedback_summary(&key_a, 1_900);
        let b_summary = store.media_feedback_summary(&key_b, 1_900);
        assert!(a_summary.is_degraded);
        assert!(!b_summary.is_degraded);
        assert!(a_summary.twcc_packet_status_count > 0);
        assert!(b_summary.twcc_packet_status_count > 0);

        assert_eq!(
            store.recommend_video_quality(&key_a, 1_950),
            Some(RecommendedVideoQuality::Low)
        );
        assert_eq!(
            store.recommend_video_quality(&key_b, 1_950),
            Some(RecommendedVideoQuality::High)
        );
    }

    #[test]
    fn recommend_video_quality_ignores_twcc_only_feedback_without_rr_reports() {
        let store = RtpForwardingStore::default();
        let key = forwarding_key("subscriber-a");

        for idx in 0..12_u64 {
            store.observe_transport_wide_cc(&key, 5_000 + idx * 50, 0x44, 10);
        }

        let summary = store.media_feedback_summary(&key, 5_700);
        assert_eq!(summary.rr_report_count, 0);
        assert_eq!(summary.twcc_packet_status_count, 120);
        assert_eq!(store.recommend_video_quality(&key, 5_800), None);
    }
}

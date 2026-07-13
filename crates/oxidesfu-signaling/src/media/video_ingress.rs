use std::collections::HashMap;

const MIN_DOMINANT_PAYLOAD_TYPE_PACKETS: u64 = 50;
const MIN_SSRC_PACKETS_BEFORE_SELECT: u64 = 40;
const SELECTED_SSRC_STALE_SWITCH_MILLIS: u64 = 350;
const DOMINANCE_SWITCH_WINDOW_PACKETS: u64 = 400;
const DOMINANCE_SWITCH_MIN_PACKET_COUNT: u64 = 120;
const DOMINANCE_SWITCH_RATIO_NUMERATOR: u64 = 2;
const PAYLOAD_TYPE_SWITCH_MIN_PACKETS: u64 = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VideoIngressDecision {
    Forward { selected_ssrc_changed: bool },
    DropNonDominantPayloadType,
    DropNonSelectedSsrc,
}

#[derive(Debug, Default)]
pub(crate) struct VideoIngressFilter {
    payload_type_counts: HashMap<u8, u64>,
    non_dominant_payload_type_counts: HashMap<u8, u64>,
    dominant_payload_type: Option<u8>,
    ssrc_counts: HashMap<u32, u64>,
    window_ssrc_counts: HashMap<u32, u64>,
    window_total_packets: u64,
    selected_ssrc: Option<u32>,
    selected_last_seen_millis: Option<u64>,
}

impl VideoIngressFilter {
    pub(crate) fn dominant_payload_type(&self) -> Option<u8> {
        self.dominant_payload_type
    }

    pub(crate) fn selected_ssrc(&self) -> Option<u32> {
        self.selected_ssrc
    }

    pub(crate) fn observe_packet(
        &mut self,
        now_millis: u64,
        incoming_ssrc: u32,
        incoming_payload_type: u8,
    ) -> VideoIngressDecision {
        let count_for_payload_type = {
            let count = self
                .payload_type_counts
                .entry(incoming_payload_type)
                .and_modify(|value| *value += 1)
                .or_insert(1);
            *count
        };

        if self.dominant_payload_type.is_none()
            && count_for_payload_type >= MIN_DOMINANT_PAYLOAD_TYPE_PACKETS
        {
            self.dominant_payload_type = self
                .payload_type_counts
                .iter()
                .max_by_key(|(_, count)| **count)
                .map(|(payload_type, _)| *payload_type);
        }

        if let Some(dominant_payload_type) = self.dominant_payload_type {
            if incoming_payload_type != dominant_payload_type {
                let non_dominant_count = {
                    let count = self
                        .non_dominant_payload_type_counts
                        .entry(incoming_payload_type)
                        .and_modify(|value| *value += 1)
                        .or_insert(1);
                    *count
                };

                if non_dominant_count < PAYLOAD_TYPE_SWITCH_MIN_PACKETS {
                    return VideoIngressDecision::DropNonDominantPayloadType;
                }

                self.dominant_payload_type = Some(incoming_payload_type);
                self.non_dominant_payload_type_counts.clear();
            } else if !self.non_dominant_payload_type_counts.is_empty() {
                self.non_dominant_payload_type_counts.clear();
            }
        }

        let count_for_ssrc = {
            let count = self
                .ssrc_counts
                .entry(incoming_ssrc)
                .and_modify(|value| *value += 1)
                .or_insert(1);
            *count
        };

        let _ = self
            .window_ssrc_counts
            .entry(incoming_ssrc)
            .and_modify(|value| *value += 1)
            .or_insert(1);
        self.window_total_packets = self.window_total_packets.saturating_add(1);

        if let Some(selected_ssrc) = self.selected_ssrc
            && self.window_total_packets >= DOMINANCE_SWITCH_WINDOW_PACKETS
        {
            let dominant = self
                .window_ssrc_counts
                .iter()
                .max_by_key(|(_, count)| **count)
                .map(|(ssrc, count)| (*ssrc, *count));

            if let Some((dominant_ssrc, dominant_count)) = dominant {
                let selected_count = self
                    .window_ssrc_counts
                    .get(&selected_ssrc)
                    .copied()
                    .unwrap_or_default();
                let required_count = selected_count
                    .saturating_mul(DOMINANCE_SWITCH_RATIO_NUMERATOR)
                    .max(DOMINANCE_SWITCH_MIN_PACKET_COUNT);

                if dominant_ssrc != selected_ssrc && dominant_count >= required_count {
                    self.selected_ssrc = Some(dominant_ssrc);
                    if dominant_ssrc == incoming_ssrc {
                        self.selected_last_seen_millis = Some(now_millis);
                    }

                    self.window_ssrc_counts.clear();
                    self.window_total_packets = 0;

                    return if dominant_ssrc == incoming_ssrc {
                        VideoIngressDecision::Forward {
                            selected_ssrc_changed: true,
                        }
                    } else {
                        VideoIngressDecision::DropNonSelectedSsrc
                    };
                }
            }

            self.window_ssrc_counts.clear();
            self.window_total_packets = 0;
        }

        match self.selected_ssrc {
            None => {
                if count_for_ssrc < MIN_SSRC_PACKETS_BEFORE_SELECT {
                    return VideoIngressDecision::DropNonSelectedSsrc;
                }

                let selected_ssrc = self
                    .ssrc_counts
                    .iter()
                    .max_by_key(|(_, count)| **count)
                    .map(|(ssrc, _)| *ssrc)
                    .unwrap_or(incoming_ssrc);

                self.selected_ssrc = Some(selected_ssrc);
                if selected_ssrc == incoming_ssrc {
                    self.selected_last_seen_millis = Some(now_millis);
                    VideoIngressDecision::Forward {
                        selected_ssrc_changed: true,
                    }
                } else {
                    VideoIngressDecision::DropNonSelectedSsrc
                }
            }
            Some(selected_ssrc) if selected_ssrc == incoming_ssrc => {
                self.selected_last_seen_millis = Some(now_millis);
                VideoIngressDecision::Forward {
                    selected_ssrc_changed: false,
                }
            }
            Some(selected_ssrc) => {
                let selected_stale = self.selected_last_seen_millis.is_none_or(|last_seen| {
                    now_millis.saturating_sub(last_seen) >= SELECTED_SSRC_STALE_SWITCH_MILLIS
                });

                if selected_stale {
                    self.selected_ssrc = Some(incoming_ssrc);
                    self.selected_last_seen_millis = Some(now_millis);
                    VideoIngressDecision::Forward {
                        selected_ssrc_changed: true,
                    }
                } else {
                    let _ = selected_ssrc;
                    VideoIngressDecision::DropNonSelectedSsrc
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{VideoIngressDecision, VideoIngressFilter};

    #[test]
    fn select_first_ssrc_and_keep_when_active() {
        let mut filter = VideoIngressFilter::default();

        for tick in 0..39_u64 {
            assert_eq!(
                filter.observe_packet(100 + tick, 111, 96),
                VideoIngressDecision::DropNonSelectedSsrc
            );
        }

        assert_eq!(
            filter.observe_packet(140, 111, 96),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );

        assert_eq!(
            filter.observe_packet(160, 111, 96),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: false
            }
        );

        assert_eq!(
            filter.observe_packet(180, 222, 96),
            VideoIngressDecision::DropNonSelectedSsrc
        );

        assert_eq!(filter.selected_ssrc(), Some(111));
    }

    #[test]
    fn switches_selected_ssrc_when_previous_stream_is_stale() {
        let mut filter = VideoIngressFilter::default();

        for tick in 0..40_u64 {
            let _ = filter.observe_packet(100 + tick, 111, 96);
        }
        assert_eq!(filter.selected_ssrc(), Some(111));

        assert_eq!(
            filter.observe_packet(500, 222, 96),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: true
            }
        );
        assert_eq!(filter.selected_ssrc(), Some(222));
    }

    #[test]
    fn switches_selected_ssrc_when_other_ssrc_is_dominant_in_window() {
        let mut filter = VideoIngressFilter::default();

        for tick in 0..40_u64 {
            let _ = filter.observe_packet(100 + tick, 111, 96);
        }
        assert_eq!(filter.selected_ssrc(), Some(111));

        for tick in 0..399_u64 {
            let now = 200 + tick;
            let ssrc = if tick % 4 == 0 { 111 } else { 222 };
            let _ = filter.observe_packet(now, ssrc, 96);
        }

        assert!(matches!(
            filter.observe_packet(600, 222, 96),
            VideoIngressDecision::Forward { .. }
        ));
        assert_eq!(filter.selected_ssrc(), Some(222));
    }

    #[test]
    fn drops_short_non_dominant_payload_type_burst_after_dominant_is_selected() {
        let mut filter = VideoIngressFilter::default();

        for tick in 0..50_u64 {
            let _ = filter.observe_packet(100 + tick, 111, 96);
        }

        assert_eq!(filter.dominant_payload_type(), Some(96));
        assert_eq!(
            filter.observe_packet(200, 111, 97),
            VideoIngressDecision::DropNonDominantPayloadType
        );
        assert_eq!(filter.dominant_payload_type(), Some(96));
    }

    #[test]
    fn switches_dominant_payload_type_after_sustained_change() {
        let mut filter = VideoIngressFilter::default();

        for tick in 0..50_u64 {
            let _ = filter.observe_packet(100 + tick, 111, 96);
        }
        for tick in 0..40_u64 {
            let _ = filter.observe_packet(200 + tick, 111, 96);
        }

        assert_eq!(filter.dominant_payload_type(), Some(96));
        assert_eq!(filter.selected_ssrc(), Some(111));

        for tick in 0..49_u64 {
            assert_eq!(
                filter.observe_packet(300 + tick, 111, 97),
                VideoIngressDecision::DropNonDominantPayloadType
            );
        }

        assert_eq!(
            filter.observe_packet(400, 111, 97),
            VideoIngressDecision::Forward {
                selected_ssrc_changed: false
            }
        );
        assert_eq!(filter.dominant_payload_type(), Some(97));
    }
}

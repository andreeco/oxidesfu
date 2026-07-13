use std::collections::HashSet;

use super::rtp_stats_receiver_restart::RtpStatsReceiverRestartDetector;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RtpFlowUnhandledReason {
    None,
    PreStartTimestamp,
    OldSequenceNumber,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RtpFlowState {
    pub(crate) unhandled_reason: RtpFlowUnhandledReason,
    pub(crate) loss_start_inclusive: u64,
    pub(crate) loss_end_exclusive: u64,
    pub(crate) is_duplicate: bool,
    pub(crate) is_out_of_order: bool,
    pub(crate) ext_sequence_number: u64,
    pub(crate) ext_timestamp: u64,
}

impl Default for RtpFlowState {
    fn default() -> Self {
        Self {
            unhandled_reason: RtpFlowUnhandledReason::None,
            loss_start_inclusive: 0,
            loss_end_exclusive: 0,
            is_duplicate: false,
            is_out_of_order: false,
            ext_sequence_number: 0,
            ext_timestamp: 0,
        }
    }
}

#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub(crate) struct SequenceHistory {
    seen: HashSet<u64>,
}

#[allow(dead_code)]
impl SequenceHistory {
    fn is_set(&self, sequence_number: u64) -> bool {
        self.seen.contains(&sequence_number)
    }

    fn set(&mut self, sequence_number: u64) {
        self.seen.insert(sequence_number);
    }

    fn clear_range(&mut self, start_inclusive: u64, end_inclusive: u64) {
        if start_inclusive > end_inclusive {
            return;
        }
        for sequence_number in start_inclusive..=end_inclusive {
            self.seen.remove(&sequence_number);
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct RtpStatsReceiverLite {
    initialized: bool,
    start_sequence_number: u16,
    start_timestamp: u32,
    highest_sequence_number: u16,
    highest_timestamp: u32,
    packets_lost: u64,
    packets_out_of_order: u64,
    packets_duplicate: u64,
    history: SequenceHistory,
    restart_detector: RtpStatsReceiverRestartDetector,
}

#[allow(dead_code)]
impl RtpStatsReceiverLite {
    pub(crate) fn new() -> Self {
        Self {
            initialized: false,
            start_sequence_number: 0,
            start_timestamp: 0,
            highest_sequence_number: 0,
            highest_timestamp: 0,
            packets_lost: 0,
            packets_out_of_order: 0,
            packets_duplicate: 0,
            history: SequenceHistory::default(),
            restart_detector: RtpStatsReceiverRestartDetector::new(),
        }
    }

    pub(crate) fn update(
        &mut self,
        sequence_number: u16,
        timestamp: u32,
        payload_size: usize,
    ) -> RtpFlowState {
        let mut flow_state = RtpFlowState::default();

        if !self.initialized {
            if payload_size == 0 {
                return flow_state;
            }

            self.initialized = true;
            self.start_sequence_number = sequence_number;
            self.start_timestamp = timestamp;
            self.highest_sequence_number = sequence_number;
            self.highest_timestamp = timestamp;
            self.history.set(u64::from(sequence_number));
            flow_state.ext_sequence_number = u64::from(sequence_number);
            flow_state.ext_timestamp = u64::from(timestamp);
            return flow_state;
        }

        if is_wrapped_less_than_u32(timestamp, self.start_timestamp) {
            if self
                .restart_detector
                .maybe_restart(sequence_number, timestamp, payload_size)
            {
                self.restart_detector.reset_restart();
            }
            flow_state.unhandled_reason = RtpFlowUnhandledReason::PreStartTimestamp;
            flow_state.ext_sequence_number = u64::from(self.highest_sequence_number);
            flow_state.ext_timestamp = u64::from(self.highest_timestamp);
            return flow_state;
        }

        let gap_sn = wrapping_diff_u16(sequence_number, self.highest_sequence_number);
        let gap_ts = wrapping_diff_u32(timestamp, self.highest_timestamp);

        if gap_sn > 0 && gap_ts < 0 {
            if self
                .restart_detector
                .maybe_restart(sequence_number, timestamp, payload_size)
            {
                self.restart_detector.reset_restart();
            }
            flow_state.unhandled_reason = RtpFlowUnhandledReason::OldSequenceNumber;
            flow_state.ext_sequence_number = u64::from(self.highest_sequence_number);
            flow_state.ext_timestamp = u64::from(self.highest_timestamp);
            return flow_state;
        }

        if gap_sn <= 0 {
            if gap_sn != 0 {
                self.packets_out_of_order += 1;
            }

            let ext_sn = u64::from(sequence_number);
            if self.history.is_set(ext_sn) {
                self.packets_duplicate += 1;
                flow_state.is_duplicate = true;
            } else {
                self.history.set(ext_sn);
                if self.packets_lost > 0 {
                    self.packets_lost -= 1;
                }
            }
            flow_state.is_out_of_order = true;
            flow_state.ext_sequence_number = ext_sn;
            flow_state.ext_timestamp = u64::from(timestamp);
            return flow_state;
        }

        let prev_highest = self.highest_sequence_number;
        self.highest_sequence_number = sequence_number;
        self.highest_timestamp = timestamp;

        let missing_count = (gap_sn - 1) as u64;
        self.packets_lost += missing_count;

        let loss_start = u64::from(prev_highest) + 1;
        let loss_end = u64::from(sequence_number);
        flow_state.loss_start_inclusive = loss_start;
        flow_state.loss_end_exclusive = loss_end;

        if gap_sn > 1 {
            self.history.clear_range(loss_start, loss_end - 1);
        }
        self.history.set(u64::from(sequence_number));

        flow_state.ext_sequence_number = u64::from(sequence_number);
        flow_state.ext_timestamp = u64::from(timestamp);
        self.restart_detector.reset_restart();
        flow_state
    }

    pub(crate) fn initialized(&self) -> bool {
        self.initialized
    }

    pub(crate) fn highest_sequence_number(&self) -> u16 {
        self.highest_sequence_number
    }

    pub(crate) fn highest_extended_sequence_number(&self) -> u64 {
        u64::from(self.highest_sequence_number)
    }

    pub(crate) fn highest_timestamp(&self) -> u32 {
        self.highest_timestamp
    }

    pub(crate) fn highest_extended_timestamp(&self) -> u64 {
        u64::from(self.highest_timestamp)
    }

    pub(crate) fn packets_lost(&self) -> u64 {
        self.packets_lost
    }

    pub(crate) fn packets_out_of_order(&self) -> u64 {
        self.packets_out_of_order
    }

    pub(crate) fn packets_duplicate(&self) -> u64 {
        self.packets_duplicate
    }

    pub(crate) fn history_is_set(&self, sequence_number: u64) -> bool {
        self.history.is_set(sequence_number)
    }
}

fn wrapping_diff_u16(new: u16, old: u16) -> i32 {
    let raw = i32::from(new.wrapping_sub(old));
    if raw > i32::from(u16::MAX) / 2 {
        raw - (i32::from(u16::MAX) + 1)
    } else {
        raw
    }
}

fn wrapping_diff_u32(new: u32, old: u32) -> i64 {
    let raw = i64::from(new.wrapping_sub(old));
    if raw > i64::from(u32::MAX) / 2 {
        raw - (i64::from(u32::MAX) + 1)
    } else {
        raw
    }
}

fn is_wrapped_less_than_u32(value: u32, reference: u32) -> bool {
    wrapping_diff_u32(value, reference) < 0
}

#[cfg(test)]
mod tests {
    use super::{RtpFlowUnhandledReason, RtpStatsReceiverLite};

    #[test]
    fn rtp_stats_receiver_update_matches_upstream_contract() {
        let mut receiver = RtpStatsReceiverLite::new();

        let sequence_number_start = 1000u16;
        let timestamp_start = 500_000u32;

        let mut flow_state = receiver.update(sequence_number_start, timestamp_start, 1000);
        assert!(receiver.initialized());
        assert_eq!(receiver.highest_sequence_number(), sequence_number_start);
        assert_eq!(
            receiver.highest_extended_sequence_number(),
            u64::from(sequence_number_start)
        );
        assert_eq!(receiver.highest_timestamp(), timestamp_start);
        assert_eq!(
            receiver.highest_extended_timestamp(),
            u64::from(timestamp_start)
        );
        assert_eq!(flow_state.unhandled_reason, RtpFlowUnhandledReason::None);

        let mut sequence_number = sequence_number_start + 1;
        let mut timestamp = timestamp_start + 3000;
        flow_state = receiver.update(sequence_number, timestamp, 1000);
        assert_eq!(receiver.highest_sequence_number(), sequence_number);
        assert_eq!(
            receiver.highest_extended_sequence_number(),
            u64::from(sequence_number)
        );
        assert_eq!(receiver.highest_timestamp(), timestamp);
        assert_eq!(receiver.highest_extended_timestamp(), u64::from(timestamp));
        assert_eq!(flow_state.unhandled_reason, RtpFlowUnhandledReason::None);

        flow_state = receiver.update(
            sequence_number.wrapping_sub(10),
            timestamp.wrapping_sub(30_000),
            1000,
        );
        assert_eq!(
            flow_state.unhandled_reason,
            RtpFlowUnhandledReason::PreStartTimestamp
        );
        assert_eq!(receiver.highest_sequence_number(), sequence_number);
        assert_eq!(receiver.highest_timestamp(), timestamp);
        assert_eq!(receiver.packets_out_of_order(), 0);
        assert_eq!(receiver.packets_duplicate(), 0);

        flow_state = receiver.update(
            sequence_number.wrapping_sub(10),
            timestamp.wrapping_sub(30_000),
            1000,
        );
        assert_eq!(
            flow_state.unhandled_reason,
            RtpFlowUnhandledReason::PreStartTimestamp
        );
        assert_eq!(receiver.highest_sequence_number(), sequence_number);
        assert_eq!(receiver.highest_timestamp(), timestamp);
        assert_eq!(receiver.packets_out_of_order(), 0);
        assert_eq!(receiver.packets_duplicate(), 0);

        sequence_number = sequence_number.wrapping_add(10);
        timestamp = timestamp.wrapping_add(30_000);
        flow_state = receiver.update(sequence_number, timestamp, 1000);
        assert_eq!(
            flow_state.loss_start_inclusive,
            u64::from(sequence_number - 9)
        );
        assert_eq!(flow_state.loss_end_exclusive, u64::from(sequence_number));
        assert_eq!(receiver.packets_lost(), 9);

        flow_state = receiver.update(
            sequence_number.wrapping_sub(6),
            timestamp.wrapping_sub(18_000),
            1000,
        );
        assert_eq!(receiver.highest_sequence_number(), sequence_number);
        assert_eq!(receiver.highest_timestamp(), timestamp);
        assert_eq!(receiver.packets_out_of_order(), 1);
        assert_eq!(receiver.packets_duplicate(), 0);
        assert_eq!(receiver.packets_lost(), 8);
        assert!(flow_state.is_out_of_order);

        sequence_number = sequence_number.wrapping_add(2);
        timestamp = timestamp.wrapping_add(6000);
        flow_state = receiver.update(sequence_number, timestamp, 1000);
        assert_eq!(
            flow_state.loss_start_inclusive,
            u64::from(sequence_number - 1)
        );
        assert_eq!(flow_state.loss_end_exclusive, u64::from(sequence_number));
        assert_eq!(receiver.packets_lost(), 9);
        assert!(!receiver.history_is_set(u64::from(sequence_number - 1)));

        sequence_number = sequence_number.wrapping_sub(1);
        timestamp = timestamp.wrapping_sub(3000);
        flow_state = receiver.update(sequence_number, timestamp, 999);
        assert_eq!(receiver.packets_lost(), 8);
        assert_eq!(receiver.packets_out_of_order(), 2);
        assert!(receiver.history_is_set(u64::from(sequence_number)));
        assert!(flow_state.is_out_of_order);

        sequence_number = sequence_number.wrapping_add(2);
        timestamp = timestamp.wrapping_add(3000);
        flow_state = receiver.update(sequence_number, timestamp, 0);
        assert_eq!(receiver.packets_lost(), 8);
        assert_eq!(receiver.packets_out_of_order(), 2);
        assert!(receiver.history_is_set(u64::from(sequence_number)));
        assert!(receiver.history_is_set(u64::from(sequence_number - 1)));
        assert!(receiver.history_is_set(u64::from(sequence_number - 2)));
        assert_eq!(flow_state.unhandled_reason, RtpFlowUnhandledReason::None);

        flow_state = receiver.update(
            sequence_number.wrapping_add(400),
            timestamp.wrapping_sub(6000),
            300,
        );
        assert_eq!(
            flow_state.unhandled_reason,
            RtpFlowUnhandledReason::OldSequenceNumber
        );
        assert_eq!(receiver.highest_sequence_number(), sequence_number);
        assert_eq!(receiver.highest_timestamp(), timestamp);
    }
}

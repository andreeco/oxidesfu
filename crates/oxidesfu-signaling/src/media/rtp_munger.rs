use std::cmp::max;

const MIN_RANGES: usize = 1;
const HALF_RANGE_U64: u64 = 1 << 63;

const RTP_MUNGER_RANGE_MAP_SIZE: usize = 100;
const RTX_GATE_WINDOW: u64 = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SequenceNumberOrdering {
    Contiguous,
    OutOfOrder,
    Gap,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TranslationParamsRtp {
    pub(crate) sn_ordering: SequenceNumberOrdering,
    pub(crate) ext_sequence_number: u64,
    pub(crate) ext_timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnTs {
    pub(crate) ext_sequence_number: u64,
    pub(crate) ext_timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RtpMungerError {
    OutOfOrderSequenceNumberCacheMiss,
    DuplicatePacket,
    PaddingOnlyPacket,
    PaddingNotOnFrameBoundary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RtpMungerInputPacket {
    ext_sequence_number: u64,
    ext_timestamp: u64,
    payload_size: usize,
    is_key_frame: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeMapError {
    ReversedOrder,
    KeyNotFound,
    KeyTooOld,
    KeyExcluded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeVal {
    start: u64,
    end: u64,
    value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RangeMapU64 {
    size: usize,
    ranges: Vec<RangeVal>,
}

impl RangeMapU64 {
    fn new(size: usize) -> Self {
        let mut range_map = Self {
            size: max(size, MIN_RANGES),
            ranges: Vec::new(),
        };
        range_map.init_ranges(0, 0);
        range_map
    }

    fn clear_and_reset_value(&mut self, start: u64, value: u64) {
        self.init_ranges(start, value);
    }

    fn dec_value(&mut self, end: u64, dec: u64) {
        let last_index = self.ranges.len() - 1;
        let last_range = &mut self.ranges[last_index];
        if last_range.start > end {
            last_range.value = last_range.value.saturating_sub(dec);
            return;
        }

        last_range.end = end;
        let next_start = end.wrapping_add(1);
        let next_value = last_range.value.saturating_sub(dec);
        self.ranges.push(RangeVal {
            start: next_start,
            end: 0,
            value: next_value,
        });
        self.prune();
    }

    fn exclude_range(
        &mut self,
        start_inclusive: u64,
        end_exclusive: u64,
    ) -> Result<(), RangeMapError> {
        let width = end_exclusive.wrapping_sub(start_inclusive);
        if end_exclusive == start_inclusive || width > HALF_RANGE_U64 {
            return Err(RangeMapError::ReversedOrder);
        }

        let last_index = self.ranges.len() - 1;
        let last_range = &mut self.ranges[last_index];
        if last_range.start > start_inclusive {
            return Err(RangeMapError::ReversedOrder);
        }

        let new_value = last_range.value.saturating_add(width);

        if last_range.start == start_inclusive {
            last_range.start = end_exclusive;
            last_range.value = new_value;
            return Ok(());
        }

        last_range.end = start_inclusive.wrapping_sub(1);
        self.ranges.push(RangeVal {
            start: end_exclusive,
            end: 0,
            value: new_value,
        });
        self.prune();
        Ok(())
    }

    fn get_value(&self, key: u64) -> Result<u64, RangeMapError> {
        let num_ranges = self.ranges.len();
        if num_ranges != 0 {
            if key >= self.ranges[num_ranges - 1].start {
                return Ok(self.ranges[num_ranges - 1].value);
            }

            if key < self.ranges[0].start {
                return Err(RangeMapError::KeyTooOld);
            }
        }

        for idx in (0..num_ranges).rev() {
            let range = &self.ranges[idx];
            if idx != num_ranges - 1
                && key.wrapping_sub(range.start) < HALF_RANGE_U64
                && range.end.wrapping_sub(key) < HALF_RANGE_U64
            {
                return Ok(range.value);
            }

            if idx > 0 {
                let previous = &self.ranges[idx - 1];
                let before_diff = key.wrapping_sub(previous.end);
                let after_diff = range.start.wrapping_sub(key);
                if before_diff > 0
                    && before_diff < HALF_RANGE_U64
                    && after_diff > 0
                    && after_diff < HALF_RANGE_U64
                {
                    return Err(RangeMapError::KeyExcluded);
                }
            }
        }

        Err(RangeMapError::KeyNotFound)
    }

    fn init_ranges(&mut self, start: u64, value: u64) {
        self.ranges = vec![RangeVal {
            start,
            end: 0,
            value,
        }];
    }

    fn prune(&mut self) {
        if self.ranges.len() > self.size + 1 {
            self.ranges = self.ranges[self.ranges.len() - self.size - 1..].to_vec();
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct RtpMunger {
    ext_highest_incoming_sn: u64,
    sn_range_map: RangeMapU64,

    ext_last_sn: u64,
    ext_second_last_sn: u64,
    sn_offset: u64,

    ext_last_ts: u64,
    ext_second_last_ts: u64,
    ts_offset: u64,

    last_marker: bool,
    second_last_marker: bool,

    ext_rtx_gate_sn: u64,
    is_in_rtx_gate_region: bool,
}

#[allow(dead_code)]
impl RtpMunger {
    pub(crate) fn new() -> Self {
        Self {
            ext_highest_incoming_sn: 0,
            sn_range_map: RangeMapU64::new(RTP_MUNGER_RANGE_MAP_SIZE),
            ext_last_sn: 0,
            ext_second_last_sn: 0,
            sn_offset: 0,
            ext_last_ts: 0,
            ext_second_last_ts: 0,
            ts_offset: 0,
            last_marker: false,
            second_last_marker: false,
            ext_rtx_gate_sn: 0,
            is_in_rtx_gate_region: false,
        }
    }

    pub(crate) fn set_last_sn_ts(&mut self, packet: &RtpMungerInputPacket) {
        self.ext_highest_incoming_sn = packet.ext_sequence_number.wrapping_sub(1);

        self.ext_last_sn = packet.ext_sequence_number;
        self.ext_second_last_sn = self.ext_last_sn.wrapping_sub(1);
        self.sn_range_map
            .clear_and_reset_value(packet.ext_sequence_number, 0);
        self.update_sn_offset();

        self.ext_last_ts = packet.ext_timestamp;
        self.ext_second_last_ts = packet.ext_timestamp;
        self.ts_offset = 0;
    }

    pub(crate) fn update_sn_ts_offsets(
        &mut self,
        packet: &RtpMungerInputPacket,
        sn_adjust: u64,
        ts_adjust: u64,
    ) {
        self.ext_highest_incoming_sn = packet.ext_sequence_number.wrapping_sub(1);

        self.sn_range_map.clear_and_reset_value(
            packet.ext_sequence_number,
            packet
                .ext_sequence_number
                .wrapping_sub(self.ext_last_sn)
                .wrapping_sub(sn_adjust),
        );
        self.update_sn_offset();

        self.ts_offset = packet
            .ext_timestamp
            .wrapping_sub(self.ext_last_ts)
            .wrapping_sub(ts_adjust);
    }

    pub(crate) fn packet_dropped(&mut self, packet: &RtpMungerInputPacket) {
        if self.ext_highest_incoming_sn != packet.ext_sequence_number {
            return;
        }

        let _ = self
            .sn_range_map
            .get_value(packet.ext_sequence_number)
            .map(|sn_offset| packet.ext_sequence_number.wrapping_sub(sn_offset));

        let _ = self.sn_range_map.exclude_range(
            self.ext_highest_incoming_sn,
            self.ext_highest_incoming_sn + 1,
        );

        self.ext_last_sn = self.ext_second_last_sn;
        self.update_sn_offset();

        self.ext_last_ts = self.ext_second_last_ts;
        self.last_marker = self.second_last_marker;
    }

    pub(crate) fn update_and_get_sn_ts(
        &mut self,
        packet: &RtpMungerInputPacket,
        marker: bool,
    ) -> Result<TranslationParamsRtp, RtpMungerError> {
        let diff = packet
            .ext_sequence_number
            .wrapping_sub(self.ext_highest_incoming_sn) as i64;

        if (diff == 1 && packet.payload_size != 0) || diff > 1 {
            self.ext_highest_incoming_sn = packet.ext_sequence_number;

            let ordering = if diff > 1 {
                SequenceNumberOrdering::Gap
            } else {
                SequenceNumberOrdering::Contiguous
            };

            let ext_munged_sn = packet.ext_sequence_number.wrapping_sub(self.sn_offset);
            let ext_munged_ts = packet.ext_timestamp.wrapping_sub(self.ts_offset);

            self.ext_second_last_sn = self.ext_last_sn;
            self.ext_last_sn = ext_munged_sn;
            self.ext_second_last_ts = self.ext_last_ts;
            self.ext_last_ts = ext_munged_ts;
            self.second_last_marker = self.last_marker;
            self.last_marker = marker;

            if packet.is_key_frame {
                self.ext_rtx_gate_sn = ext_munged_sn;
                self.is_in_rtx_gate_region = true;
            }

            if self.is_in_rtx_gate_region
                && ext_munged_sn.wrapping_sub(self.ext_rtx_gate_sn) > RTX_GATE_WINDOW
            {
                self.is_in_rtx_gate_region = false;
            }

            return Ok(TranslationParamsRtp {
                sn_ordering: ordering,
                ext_sequence_number: ext_munged_sn,
                ext_timestamp: ext_munged_ts,
            });
        }

        if diff < 0 {
            let Ok(sn_offset) = self.sn_range_map.get_value(packet.ext_sequence_number) else {
                return Err(RtpMungerError::OutOfOrderSequenceNumberCacheMiss);
            };

            let ext_sequence_number = packet.ext_sequence_number.wrapping_sub(sn_offset);
            if ext_sequence_number >= self.ext_last_sn {
                return Err(RtpMungerError::OutOfOrderSequenceNumberCacheMiss);
            }

            return Ok(TranslationParamsRtp {
                sn_ordering: SequenceNumberOrdering::OutOfOrder,
                ext_sequence_number,
                ext_timestamp: packet.ext_timestamp.wrapping_sub(self.ts_offset),
            });
        }

        if diff == 1 {
            self.ext_highest_incoming_sn = packet.ext_sequence_number;
            let _ = self.sn_range_map.exclude_range(
                self.ext_highest_incoming_sn,
                self.ext_highest_incoming_sn + 1,
            );
            self.update_sn_offset();

            return Err(RtpMungerError::PaddingOnlyPacket);
        }

        Err(RtpMungerError::DuplicatePacket)
    }

    pub(crate) fn filter_rtx(&self, nacks: &[u16]) -> Vec<u16> {
        if !self.is_in_rtx_gate_region {
            return nacks.to_vec();
        }

        nacks
            .iter()
            .copied()
            .filter(|sn| sn.wrapping_sub(self.ext_rtx_gate_sn as u16) < (1 << 15))
            .collect()
    }

    pub(crate) fn update_and_get_padding_sn_ts(
        &mut self,
        num: usize,
        clock_rate: u32,
        frame_rate: u32,
        force_marker: bool,
        ext_rtp_timestamp: u64,
    ) -> Result<Vec<SnTs>, RtpMungerError> {
        if num == 0 {
            return Ok(Vec::new());
        }

        let mut use_last_ts_for_first = false;
        let mut ts_offset = 0_u32;
        if !self.last_marker {
            if !force_marker {
                return Err(RtpMungerError::PaddingNotOnFrameBoundary);
            }
            use_last_ts_for_first = true;
            ts_offset = 1;
        }

        let mut ext_last_sn = self.ext_last_sn;
        let mut ext_last_ts = self.ext_last_ts;
        let mut vals = Vec::with_capacity(num);

        for i in 0..num {
            ext_last_sn = ext_last_sn.wrapping_add(1);

            let ext_timestamp = if let Some(frame_rate) = std::num::NonZeroU32::new(frame_rate) {
                if use_last_ts_for_first && i == 0 {
                    self.ext_last_ts
                } else {
                    let i_term = (i as u32).wrapping_add(1).wrapping_sub(ts_offset);
                    let mut ets = ext_rtp_timestamp
                        .wrapping_add((i_term * clock_rate).div_ceil(frame_rate.get()) as u64);
                    if ets <= ext_last_ts {
                        ets = ext_last_ts.wrapping_add(1);
                    }
                    ext_last_ts = ets;
                    ets
                }
            } else {
                self.ext_last_ts
            };

            vals.push(SnTs {
                ext_sequence_number: ext_last_sn,
                ext_timestamp,
            });
        }

        self.ext_second_last_sn = ext_last_sn.wrapping_sub(1);
        self.ext_last_sn = ext_last_sn;
        self.sn_range_map
            .dec_value(self.ext_highest_incoming_sn, num as u64);
        self.update_sn_offset();

        self.ext_second_last_ts = if vals.len() == 1 {
            self.ext_last_ts
        } else {
            vals[vals.len() - 2].ext_timestamp
        };
        self.ts_offset = self
            .ts_offset
            .wrapping_sub(ext_last_ts.wrapping_sub(self.ext_last_ts));
        self.ext_last_ts = ext_last_ts;

        self.second_last_marker = self.last_marker;
        if force_marker {
            self.last_marker = true;
        }

        Ok(vals)
    }

    pub(crate) fn is_on_frame_boundary(&self) -> bool {
        self.last_marker
    }

    fn update_sn_offset(&mut self) {
        self.sn_offset = self
            .sn_range_map
            .get_value(self.ext_highest_incoming_sn.wrapping_add(1))
            .unwrap_or(0);
    }
}

#[cfg(test)]
#[allow(clippy::manual_div_ceil)]
mod tests {
    use super::{
        RangeMapError, RtpMunger, RtpMungerError, RtpMungerInputPacket, SequenceNumberOrdering,
        SnTs, TranslationParamsRtp,
    };

    fn packet(
        sequence_number: u16,
        sn_cycles: u64,
        timestamp: u64,
        payload_size: usize,
        is_key_frame: bool,
    ) -> RtpMungerInputPacket {
        RtpMungerInputPacket {
            ext_sequence_number: sn_cycles * 65_536 + sequence_number as u64,
            ext_timestamp: timestamp,
            payload_size,
            is_key_frame,
        }
    }

    #[test]
    fn rtp_munger_set_last_sn_ts_matches_upstream_contract() {
        let mut munger = RtpMunger::new();

        let ext_packet = packet(23333, 0, 0xabcdef, 10, false);
        munger.set_last_sn_ts(&ext_packet);

        assert_eq!(munger.ext_highest_incoming_sn, 23332);
        assert_eq!(munger.ext_last_sn, 23333);
        assert_eq!(munger.ext_last_ts, 0xabcdef);
        assert!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn)
                .is_err()
        );
        assert_eq!(munger.sn_range_map.get_value(munger.ext_last_sn), Ok(0));
        assert_eq!(munger.sn_offset, 0);
        assert_eq!(munger.ts_offset, 0);
    }

    #[test]
    fn rtp_munger_update_sn_ts_offsets_matches_upstream_contract() {
        let mut munger = RtpMunger::new();

        let ext_packet = packet(23333, 0, 0xabcdef, 10, false);
        munger.set_last_sn_ts(&ext_packet);

        let switched = packet(33333, 0, 0xabcdef, 10, false);
        munger.update_sn_ts_offsets(&switched, 1, 1);

        assert_eq!(munger.ext_highest_incoming_sn, 33332);
        assert_eq!(munger.ext_last_sn, 23333);
        assert_eq!(munger.ext_last_ts, 0xabcdef);
        assert!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn)
                .is_err()
        );
        assert_eq!(
            munger.sn_range_map.get_value(munger.ext_last_sn),
            Err(RangeMapError::KeyTooOld)
        );
        assert_eq!(munger.sn_offset, 9999);
        assert_eq!(munger.ts_offset, u64::MAX);
    }

    #[test]
    fn rtp_munger_packet_dropped_matches_upstream_contract() {
        let mut munger = RtpMunger::new();

        let first = packet(23333, 0, 0xabcdef, 10, false);
        munger.set_last_sn_ts(&first);
        assert_eq!(
            munger
                .update_and_get_sn_ts(&first, false)
                .unwrap()
                .sn_ordering,
            SequenceNumberOrdering::Contiguous
        );

        let non_head_drop = packet(33333, 0, 0xabcdef, 10, false);
        munger.packet_dropped(&non_head_drop);
        assert_eq!(munger.ext_highest_incoming_sn, 23333);
        assert_eq!(munger.ext_last_sn, 23333);
        assert_eq!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn),
            Ok(0)
        );

        let head = packet(44444, 0, 0xabcdef, 20, false);
        let _ = munger.update_and_get_sn_ts(&head, false).unwrap();
        assert_eq!(munger.ext_last_sn, 44444);

        munger.packet_dropped(&head);
        assert_eq!(munger.ext_last_sn, 23333);
        assert_eq!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn),
            Err(RangeMapError::KeyExcluded)
        );
        assert_eq!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn + 1)
                .unwrap(),
            1
        );

        let after = packet(44445, 0, 0xabcdef, 20, false);
        let _ = munger.update_and_get_sn_ts(&after, false).unwrap();
        assert_eq!(munger.ext_last_sn, 44444);
        assert_eq!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn),
            Ok(1)
        );
    }

    #[test]
    fn rtp_munger_out_of_order_sequence_number_matches_upstream_contract() {
        let mut munger = RtpMunger::new();

        let start = packet(23333, 0, 0xabcdef, 10, false);
        munger.set_last_sn_ts(&start);
        let _ = munger.update_and_get_sn_ts(&start, false).unwrap();

        assert_eq!(
            munger.sn_range_map.exclude_range(23332, 23333),
            Err(RangeMapError::ReversedOrder)
        );

        let before_start = packet(23331, 0, 0xabcdef, 10, false);
        assert_eq!(
            munger.update_and_get_sn_ts(&before_start, false),
            Err(RtpMungerError::OutOfOrderSequenceNumberCacheMiss)
        );

        munger
            .sn_range_map
            .exclude_range(23334, 23335)
            .expect("manual exclusion should succeed");

        let next = packet(23336, 0, 0xabcdef, 10, false);
        let _ = munger.update_and_get_sn_ts(&next, false).unwrap();

        let out_of_order = packet(23335, 0, 0xabcdef, 10, false);
        let expected = TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::OutOfOrder,
            ext_sequence_number: 23334,
            ext_timestamp: 0xabcdef,
        };
        assert_eq!(
            munger.update_and_get_sn_ts(&out_of_order, false),
            Ok(expected)
        );

        let miss = packet(23332, 0, 0xabcdef, 10, false);
        assert_eq!(
            munger.update_and_get_sn_ts(&miss, false),
            Err(RtpMungerError::OutOfOrderSequenceNumberCacheMiss)
        );
    }

    #[test]
    fn rtp_munger_padding_only_packet_matches_upstream_contract() {
        let mut munger = RtpMunger::new();

        let start = packet(23333, 0, 0xabcdef, 0, false);
        munger.set_last_sn_ts(&start);

        assert_eq!(
            munger.update_and_get_sn_ts(&start, false),
            Err(RtpMungerError::PaddingOnlyPacket)
        );
        assert_eq!(munger.ext_highest_incoming_sn, 23333);
        assert_eq!(munger.ext_last_sn, 23333);
        assert!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn)
                .is_err()
        );

        let with_gap = packet(23335, 0, 0xabcdef, 0, false);
        let expected = TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Gap,
            ext_sequence_number: 23334,
            ext_timestamp: 0xabcdef,
        };
        assert_eq!(munger.update_and_get_sn_ts(&with_gap, false), Ok(expected));
        assert_eq!(munger.ext_highest_incoming_sn, 23335);
        assert_eq!(munger.ext_last_sn, 23334);
        assert_eq!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn),
            Ok(1)
        );
    }

    #[test]
    fn rtp_munger_gap_in_sequence_number_matches_upstream_contract() {
        let mut munger = RtpMunger::new();

        let start = packet(65533, 0, 0xabcdef, 33, false);
        munger.set_last_sn_ts(&start);
        assert!(munger.update_and_get_sn_ts(&start, false).is_ok());

        let gap = packet(1, 1, 0xabcdef, 33, false);
        let expected_gap = TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Gap,
            ext_sequence_number: 65_537,
            ext_timestamp: 0xabcdef,
        };
        assert_eq!(munger.update_and_get_sn_ts(&gap, false), Ok(expected_gap));
        assert_eq!(munger.ext_highest_incoming_sn, 65_537);
        assert_eq!(munger.ext_last_sn, 65_537);
        assert_eq!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn),
            Ok(0)
        );
        for i in 65_534..65_537 {
            assert_eq!(munger.sn_range_map.get_value(i), Ok(0));
        }

        let padding2 = packet(2, 1, 0xabcdef, 0, false);
        assert_eq!(
            munger.update_and_get_sn_ts(&padding2, false),
            Err(RtpMungerError::PaddingOnlyPacket)
        );
        assert_eq!(munger.ext_highest_incoming_sn, 65_538);
        assert_eq!(munger.ext_last_sn, 65_537);

        let after_gap = packet(4, 1, 0xabcdef, 22, false);
        let expected_after_gap = TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Gap,
            ext_sequence_number: 65_539,
            ext_timestamp: 0xabcdef,
        };
        assert_eq!(
            munger.update_and_get_sn_ts(&after_gap, false),
            Ok(expected_after_gap)
        );
        assert_eq!(munger.ext_highest_incoming_sn, 65_540);
        assert_eq!(munger.ext_last_sn, 65_539);
        assert_eq!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn),
            Ok(1)
        );
        assert_eq!(munger.sn_range_map.get_value(65_539), Ok(1));

        let padding5 = packet(5, 1, 0xabcdef, 0, false);
        assert_eq!(
            munger.update_and_get_sn_ts(&padding5, false),
            Err(RtpMungerError::PaddingOnlyPacket)
        );
        assert_eq!(munger.ext_highest_incoming_sn, 65_541);
        assert_eq!(munger.ext_last_sn, 65_539);

        let after_gap2 = packet(7, 1, 0xabcdef, 22, false);
        let expected_after_gap2 = TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::Gap,
            ext_sequence_number: 65_541,
            ext_timestamp: 0xabcdef,
        };
        assert_eq!(
            munger.update_and_get_sn_ts(&after_gap2, false),
            Ok(expected_after_gap2)
        );
        assert_eq!(munger.ext_highest_incoming_sn, 65_543);
        assert_eq!(munger.ext_last_sn, 65_541);
        assert_eq!(
            munger
                .sn_range_map
                .get_value(munger.ext_highest_incoming_sn),
            Ok(2)
        );
        assert_eq!(munger.sn_range_map.get_value(65_539), Ok(1));
        assert_eq!(munger.sn_range_map.get_value(65_542), Ok(2));

        let miss6 = packet(6, 1, 0xabcdef, 0, false);
        let expected_oo6 = TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::OutOfOrder,
            ext_sequence_number: 65_540,
            ext_timestamp: 0xabcdef,
        };
        assert_eq!(munger.update_and_get_sn_ts(&miss6, false), Ok(expected_oo6));

        let miss3 = packet(3, 1, 0xabcdef, 0, false);
        let expected_oo3 = TranslationParamsRtp {
            sn_ordering: SequenceNumberOrdering::OutOfOrder,
            ext_sequence_number: 65_538,
            ext_timestamp: 0xabcdef,
        };
        assert_eq!(munger.update_and_get_sn_ts(&miss3, false), Ok(expected_oo3));
    }

    #[test]
    fn rtp_munger_update_and_get_padding_sn_ts_matches_upstream_contract() {
        let mut munger = RtpMunger::new();

        let start = packet(23333, 0, 0xabcdef, 20, false);
        munger.set_last_sn_ts(&start);

        assert_eq!(
            munger.update_and_get_padding_sn_ts(10, 10, 5, false, 0),
            Err(RtpMungerError::PaddingNotOnFrameBoundary)
        );

        let num_padding = 10usize;
        let clock_rate = 10u32;
        let frame_rate = 5u32;

        let mut expected = Vec::with_capacity(num_padding);
        for i in 0..num_padding {
            expected.push(SnTs {
                ext_sequence_number: start.ext_sequence_number + i as u64 + 1,
                ext_timestamp: start.ext_timestamp + (((i as u64) * 10 + 5 - 1) / 5),
            });
        }

        let first = munger
            .update_and_get_padding_sn_ts(
                num_padding,
                clock_rate,
                frame_rate,
                true,
                start.ext_timestamp,
            )
            .expect("forced marker should allow padding synthesis");
        assert_eq!(first, expected);

        let mut expected_second = Vec::with_capacity(num_padding);
        for i in 0..num_padding {
            expected_second.push(SnTs {
                ext_sequence_number: start.ext_sequence_number + first.len() as u64 + i as u64 + 1,
                ext_timestamp: first[first.len() - 1].ext_timestamp
                    + (((i as u64 + 1) * 10 + 5 - 1) / 5),
            });
        }

        let second = munger
            .update_and_get_padding_sn_ts(
                num_padding,
                clock_rate,
                frame_rate,
                false,
                first[first.len() - 1].ext_timestamp,
            )
            .expect("padding on frame boundary should succeed");
        assert_eq!(second, expected_second);
    }

    #[test]
    fn rtp_munger_is_on_frame_boundary_matches_upstream_contract() {
        let mut munger = RtpMunger::new();

        let first = packet(23333, 0, 0xabcdef, 20, false);
        munger.set_last_sn_ts(&first);

        let _ = munger
            .update_and_get_sn_ts(&first, false)
            .expect("first packet should rewrite");
        assert!(!munger.is_on_frame_boundary());

        let marker = packet(23334, 0, 0xabcdef, 20, false);
        let _ = munger
            .update_and_get_sn_ts(&marker, true)
            .expect("marker packet should rewrite");
        assert!(munger.is_on_frame_boundary());
    }
}

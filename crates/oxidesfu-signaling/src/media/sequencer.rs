use std::cmp::{max, min};

const DEFAULT_RTT_MS: u32 = 70;
const IGNORE_RETRANSMISSION_MS: u32 = 100;
const MAX_ACKS: u8 = 3;
const INLINE_BYTES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeMapError {
    ReversedOrder,
    KeyTooOld,
    KeyExcluded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeValU64 {
    start: u64,
    end: u64,
    value: u64,
}

#[derive(Debug, Clone)]
struct RangeMapU64 {
    size: usize,
    ranges: Vec<RangeValU64>,
}

impl RangeMapU64 {
    fn new(size: usize) -> Self {
        Self {
            size: max(size, 1),
            ranges: vec![RangeValU64 {
                start: 0,
                end: 0,
                value: 0,
            }],
        }
    }

    fn exclude_range(
        &mut self,
        start_inclusive: u64,
        end_exclusive: u64,
    ) -> Result<(), RangeMapError> {
        if end_exclusive <= start_inclusive {
            return Err(RangeMapError::ReversedOrder);
        }

        let last = self.ranges.last_mut().expect("range map has open range");
        if last.start > start_inclusive {
            return Err(RangeMapError::ReversedOrder);
        }

        let width = end_exclusive - start_inclusive;
        let new_value = last.value.saturating_add(width);

        if last.start == start_inclusive {
            last.start = end_exclusive;
            last.value = new_value;
            return Ok(());
        }

        last.end = start_inclusive.saturating_sub(1);
        self.ranges.push(RangeValU64 {
            start: end_exclusive,
            end: 0,
            value: new_value,
        });

        if self.ranges.len() > self.size + 1 {
            self.ranges = self.ranges[self.ranges.len() - self.size - 1..].to_vec();
        }

        Ok(())
    }

    fn get_value(&self, key: u64) -> Result<u64, RangeMapError> {
        let len = self.ranges.len();
        if len == 0 {
            return Err(RangeMapError::KeyTooOld);
        }

        if key >= self.ranges[len - 1].start {
            return Ok(self.ranges[len - 1].value);
        }

        if key < self.ranges[0].start {
            return Err(RangeMapError::KeyTooOld);
        }

        for idx in (0..len).rev() {
            let range = self.ranges[idx];
            if idx != len - 1 && key >= range.start && key <= range.end {
                return Ok(range.value);
            }

            if idx > 0 {
                let previous = self.ranges[idx - 1];
                if key > previous.end && key < range.start {
                    return Err(RangeMapError::KeyExcluded);
                }
            }
        }

        Err(RangeMapError::KeyTooOld)
    }
}

#[derive(Debug, Clone)]
struct PacketMeta {
    source_seq_no: u64,
    target_seq_no: u16,
    timestamp: u32,
    marker: bool,
    last_nack_ms: u32,
    nacked: u8,
    layer: i8,
    num_codec_bytes_in: u8,
    num_codec_bytes_out: u8,
    codec_bytes_inline: [u8; INLINE_BYTES],
    codec_bytes_slice: Vec<u8>,
    dd_bytes_size: u8,
    dd_bytes_inline: [u8; INLINE_BYTES],
    dd_bytes_slice: Vec<u8>,
    act_bytes: Vec<u8>,
    valid: bool,
}

impl PacketMeta {
    fn invalid() -> Self {
        Self {
            source_seq_no: 0,
            target_seq_no: 0,
            timestamp: 0,
            marker: false,
            last_nack_ms: 0,
            nacked: 0,
            layer: 0,
            num_codec_bytes_in: 0,
            num_codec_bytes_out: 0,
            codec_bytes_inline: [0; INLINE_BYTES],
            codec_bytes_slice: Vec::new(),
            dd_bytes_size: 0,
            dd_bytes_inline: [0; INLINE_BYTES],
            dd_bytes_slice: Vec::new(),
            act_bytes: Vec::new(),
            valid: false,
        }
    }

    fn codec_bytes(&self) -> Vec<u8> {
        if self.codec_bytes_slice.is_empty() {
            self.codec_bytes_inline[..usize::from(self.num_codec_bytes_out)].to_vec()
        } else {
            self.codec_bytes_slice.clone()
        }
    }

    fn dd_bytes(&self) -> Vec<u8> {
        if self.dd_bytes_slice.is_empty() {
            self.dd_bytes_inline[..usize::from(self.dd_bytes_size)].to_vec()
        } else {
            self.dd_bytes_slice.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExtPacketMeta {
    pub(crate) source_seq_no: u64,
    pub(crate) target_seq_no: u16,
    pub(crate) ext_sequence_number: u64,
    pub(crate) ext_timestamp: u64,
    pub(crate) marker: bool,
    pub(crate) layer: i8,
    pub(crate) num_codec_bytes_in: u8,
    pub(crate) num_codec_bytes_out: u8,
    pub(crate) codec_bytes_inline: [u8; INLINE_BYTES],
    pub(crate) codec_bytes_slice: Vec<u8>,
    pub(crate) dd_bytes_size: u8,
    pub(crate) dd_bytes_inline: [u8; INLINE_BYTES],
    pub(crate) dd_bytes_slice: Vec<u8>,
    pub(crate) act_bytes: Vec<u8>,
}

impl ExtPacketMeta {
    #[cfg(test)]
    fn codec_bytes(&self) -> Vec<u8> {
        if self.codec_bytes_slice.is_empty() {
            self.codec_bytes_inline[..usize::from(self.num_codec_bytes_out)].to_vec()
        } else {
            self.codec_bytes_slice.clone()
        }
    }

    #[cfg(test)]
    fn dd_bytes(&self) -> Vec<u8> {
        if self.dd_bytes_slice.is_empty() {
            self.dd_bytes_inline[..usize::from(self.dd_bytes_size)].to_vec()
        } else {
            self.dd_bytes_slice.clone()
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct Sequencer {
    size: usize,
    start_time_ns: i64,
    initialized: bool,
    ext_start_sn: u64,
    ext_highest_sn: u64,
    sn_offset: u64,
    ext_highest_ts: u64,
    meta: Vec<PacketMeta>,
    sn_range_map: Option<RangeMapU64>,
    rtt_ms: u32,
}

#[allow(dead_code)]
impl Sequencer {
    pub(crate) fn new(size: usize, maybe_sparse: bool) -> Option<Self> {
        if size == 0 {
            return None;
        }

        Some(Self {
            size,
            start_time_ns: 0,
            initialized: false,
            ext_start_sn: 0,
            ext_highest_sn: 0,
            sn_offset: 0,
            ext_highest_ts: 0,
            meta: vec![PacketMeta::invalid(); size],
            sn_range_map: maybe_sparse.then(|| RangeMapU64::new((size + 1) / 2)),
            rtt_ms: DEFAULT_RTT_MS,
        })
    }

    #[cfg(test)]
    fn with_start_time_ns(mut self, start_time_ns: i64) -> Self {
        self.start_time_ns = start_time_ns;
        self
    }

    pub(crate) fn push(
        &mut self,
        packet_time_ns: i64,
        ext_incoming_sn: u64,
        ext_modified_sn: u64,
        ext_modified_ts: u64,
        marker: bool,
        layer: i8,
        codec_bytes: &[u8],
        num_codec_bytes_in: usize,
        dd_bytes: &[u8],
        act_bytes: &[u8],
    ) {
        if !self.initialized {
            self.initialized = true;
            if self.start_time_ns == 0 {
                self.start_time_ns = packet_time_ns;
            }
            self.ext_start_sn = ext_modified_sn;
            self.ext_highest_sn = ext_modified_sn;
            self.ext_highest_ts = ext_modified_ts;
            self.update_sn_offset();
        }

        if ext_modified_sn < self.ext_start_sn {
            return;
        }

        let ext_highest_adjusted = self.ext_highest_sn.saturating_sub(self.sn_offset);
        let mut ext_modified_adjusted = ext_modified_sn.saturating_sub(self.sn_offset);

        if ext_modified_sn < self.ext_highest_sn {
            if let Some(range_map) = &self.sn_range_map {
                let Ok(offset) = range_map.get_value(ext_modified_sn) else {
                    return;
                };
                ext_modified_adjusted = ext_modified_sn.saturating_sub(offset);
            }
        }

        if (ext_highest_adjusted as i128 - ext_modified_adjusted as i128) >= self.size as i128 {
            return;
        }

        if ext_modified_adjusted > ext_highest_adjusted {
            let mut invalidated = 0usize;
            for esn in (ext_highest_adjusted + 1)..ext_modified_adjusted {
                self.invalidate_slot((esn % self.size as u64) as usize);
                invalidated += 1;
                if invalidated >= self.size {
                    break;
                }
            }
        }

        let slot = (ext_modified_adjusted % self.size as u64) as usize;
        let mut packet_meta = PacketMeta {
            source_seq_no: ext_incoming_sn,
            target_seq_no: ext_modified_sn as u16,
            timestamp: ext_modified_ts as u32,
            marker,
            last_nack_ms: self.get_ref_time_ms(packet_time_ns),
            nacked: 0,
            layer,
            num_codec_bytes_in: num_codec_bytes_in as u8,
            num_codec_bytes_out: codec_bytes.len() as u8,
            codec_bytes_inline: [0; INLINE_BYTES],
            codec_bytes_slice: Vec::new(),
            dd_bytes_size: dd_bytes.len() as u8,
            dd_bytes_inline: [0; INLINE_BYTES],
            dd_bytes_slice: Vec::new(),
            act_bytes: act_bytes.to_vec(),
            valid: true,
        };

        if codec_bytes.len() > INLINE_BYTES {
            packet_meta.codec_bytes_slice = codec_bytes.to_vec();
        } else {
            packet_meta.codec_bytes_inline[..codec_bytes.len()].copy_from_slice(codec_bytes);
        }

        if dd_bytes.len() > INLINE_BYTES {
            packet_meta.dd_bytes_slice = dd_bytes.to_vec();
        } else {
            packet_meta.dd_bytes_inline[..dd_bytes.len()].copy_from_slice(dd_bytes);
        }

        self.meta[slot] = packet_meta;

        if ext_modified_sn > self.ext_highest_sn {
            self.ext_highest_sn = ext_modified_sn;
        }
        if ext_modified_ts > self.ext_highest_ts {
            self.ext_highest_ts = ext_modified_ts;
        }
    }

    pub(crate) fn push_padding(&mut self, ext_start_sn_inclusive: u64, ext_end_sn_inclusive: u64) {
        if self.sn_range_map.is_none() || !self.initialized {
            return;
        }

        if ext_start_sn_inclusive <= self.ext_highest_sn {
            for esn in ext_start_sn_inclusive..=ext_end_sn_inclusive {
                let diff = esn as i128 - self.ext_highest_sn as i128;
                if diff >= 0 || diff < -(self.size as i128) {
                    continue;
                }

                let sn_offset = self
                    .sn_range_map
                    .as_ref()
                    .and_then(|m| m.get_value(esn).ok())
                    .unwrap_or(0);
                let slot = ((esn.saturating_sub(sn_offset)) % self.size as u64) as usize;
                self.invalidate_slot(slot);
            }
            return;
        }

        let Some(range_map) = &mut self.sn_range_map else {
            return;
        };
        if range_map
            .exclude_range(ext_start_sn_inclusive, ext_end_sn_inclusive + 1)
            .is_err()
        {
            return;
        }

        self.ext_highest_sn = ext_end_sn_inclusive;
        self.update_sn_offset();
    }

    pub(crate) fn get_ext_packet_metas(
        &mut self,
        seq_nos: &[u16],
        now_ns: i64,
    ) -> Vec<ExtPacketMeta> {
        if !self.initialized {
            return Vec::new();
        }

        let ref_time = self.get_ref_time_ms(now_ns);
        let highest_sn = self.ext_highest_sn as u16;
        let highest_ts = self.ext_highest_ts as u32;
        let mut metas = Vec::with_capacity(seq_nos.len());

        for &sn in seq_nos {
            let diff = highest_sn.wrapping_sub(sn);
            if diff > (1 << 15) {
                continue;
            }

            let mut ext_sn = u64::from(sn) + (self.ext_highest_sn & 0xFFFF_FFFF_FFFF_0000);
            if sn > highest_sn {
                ext_sn = ext_sn.saturating_sub(1 << 16);
            }

            let mut sn_offset = 0u64;
            if let Some(range_map) = &self.sn_range_map {
                let Ok(offset) = range_map.get_value(ext_sn) else {
                    continue;
                };
                sn_offset = offset;
            }

            let ext_sn_adjusted = ext_sn.saturating_sub(sn_offset);
            let ext_highest_adjusted = self.ext_highest_sn.saturating_sub(self.sn_offset);
            if ext_highest_adjusted.saturating_sub(ext_sn_adjusted) >= self.size as u64 {
                continue;
            }

            let slot = (ext_sn_adjusted % self.size as u64) as usize;
            if self.is_invalid_slot(slot) {
                continue;
            }

            let nack_interval_ms = min(IGNORE_RETRANSMISSION_MS, 2 * self.rtt_ms);
            let packet_meta = &mut self.meta[slot];
            if packet_meta.target_seq_no != sn {
                continue;
            }

            if packet_meta.nacked >= MAX_ACKS
                || ref_time.saturating_sub(packet_meta.last_nack_ms) <= nack_interval_ms
            {
                continue;
            }

            packet_meta.nacked = packet_meta.nacked.saturating_add(1);
            packet_meta.last_nack_ms = ref_time;

            let mut ext_ts =
                u64::from(packet_meta.timestamp) + (self.ext_highest_ts & 0xFFFF_FFFF_0000_0000);
            if packet_meta.timestamp > highest_ts {
                ext_ts = ext_ts.saturating_sub(1 << 32);
            }

            metas.push(ExtPacketMeta {
                source_seq_no: packet_meta.source_seq_no,
                target_seq_no: packet_meta.target_seq_no,
                ext_sequence_number: ext_sn,
                ext_timestamp: ext_ts,
                marker: packet_meta.marker,
                layer: packet_meta.layer,
                num_codec_bytes_in: packet_meta.num_codec_bytes_in,
                num_codec_bytes_out: packet_meta.num_codec_bytes_out,
                codec_bytes_inline: packet_meta.codec_bytes_inline,
                codec_bytes_slice: packet_meta.codec_bytes_slice.clone(),
                dd_bytes_size: packet_meta.dd_bytes_size,
                dd_bytes_inline: packet_meta.dd_bytes_inline,
                dd_bytes_slice: packet_meta.dd_bytes_slice.clone(),
                act_bytes: packet_meta.act_bytes.clone(),
            });
        }

        metas
    }

    fn get_ref_time_ms(&self, at_ns: i64) -> u32 {
        if at_ns <= self.start_time_ns {
            return 0;
        }
        ((at_ns - self.start_time_ns) / 1_000_000) as u32
    }

    fn update_sn_offset(&mut self) {
        let Some(range_map) = &self.sn_range_map else {
            return;
        };
        if let Ok(sn_offset) = range_map.get_value(self.ext_highest_sn + 1) {
            self.sn_offset = sn_offset;
        }
    }

    fn invalidate_slot(&mut self, slot: usize) {
        if slot >= self.meta.len() {
            return;
        }
        self.meta[slot] = PacketMeta::invalid();
    }

    fn is_invalid_slot(&self, slot: usize) -> bool {
        self.meta.get(slot).is_none_or(|meta| !meta.valid)
    }
}

#[cfg(test)]
mod tests {
    use super::{IGNORE_RETRANSMISSION_MS, Sequencer};

    fn now_from_ms(ms: u32) -> i64 {
        i64::from(ms) * 1_000_000
    }

    #[test]
    fn sequencer_matches_upstream_contract() {
        let mut seq = Sequencer::new(500, false)
            .expect("sequencer should construct")
            .with_start_time_ns(0);
        let off = 15u64;

        for i in 1u64..518 {
            seq.push(now_from_ms(1), i, i + off, 123, true, 2, &[], 0, &[], &[]);
        }
        seq.push(
            now_from_ms(1),
            519,
            519 + off,
            123,
            false,
            2,
            &[],
            0,
            &[],
            &[],
        );
        seq.push(
            now_from_ms(1),
            518,
            518 + off,
            123,
            true,
            2,
            &[],
            0,
            &[],
            &[],
        );

        let req = vec![57, 58, 62, 63, 513, 514, 515, 516, 517];
        let res_early = seq.get_ext_packet_metas(&req, now_from_ms(10));
        assert!(res_early.is_empty());

        let res = seq.get_ext_packet_metas(&req, now_from_ms(IGNORE_RETRANSMISSION_MS + 20));
        assert_eq!(res.len(), req.len());
        for (i, val) in res.iter().enumerate() {
            assert_eq!(val.target_seq_no, req[i]);
            assert_eq!(val.source_seq_no, u64::from(req[i]) - off);
            assert_eq!(val.layer, 2);
            assert_eq!(val.ext_sequence_number, u64::from(req[i]));
            assert_eq!(val.ext_timestamp, 123);
        }

        let second_without_delay =
            seq.get_ext_packet_metas(&req, now_from_ms(IGNORE_RETRANSMISSION_MS + 25));
        assert!(second_without_delay.is_empty());

        let second = seq.get_ext_packet_metas(&req, now_from_ms(2 * IGNORE_RETRANSMISSION_MS + 40));
        assert_eq!(second.len(), req.len());

        seq.push(
            now_from_ms(2 * IGNORE_RETRANSMISSION_MS + 40),
            521,
            521 + off,
            123,
            true,
            1,
            &[],
            0,
            &[],
            &[],
        );
        assert!(
            seq.get_ext_packet_metas(
                &[(521 + off) as u16],
                now_from_ms(2 * IGNORE_RETRANSMISSION_MS + 45)
            )
            .is_empty()
        );
        assert_eq!(
            seq.get_ext_packet_metas(
                &[(521 + off) as u16],
                now_from_ms(3 * IGNORE_RETRANSMISSION_MS + 60)
            )
            .len(),
            1
        );

        seq.push(
            now_from_ms(3 * IGNORE_RETRANSMISSION_MS + 60),
            505,
            505 + off,
            123,
            false,
            1,
            &[],
            0,
            &[],
            &[],
        );
        assert!(
            seq.get_ext_packet_metas(
                &[(505 + off) as u16],
                now_from_ms(3 * IGNORE_RETRANSMISSION_MS + 65)
            )
            .is_empty()
        );
        assert_eq!(
            seq.get_ext_packet_metas(
                &[(505 + off) as u16],
                now_from_ms(4 * IGNORE_RETRANSMISSION_MS + 80)
            )
            .len(),
            1
        );
    }

    #[test]
    fn sequencer_get_nack_seq_no_no_exclusion_matches_upstream_contract() {
        let mut sequencer = Sequencer::new(5, false)
            .expect("sequencer should construct")
            .with_start_time_ns(0);

        let offset = 5u64;
        let marker_odd = true;
        let marker_even = false;
        let codec_odd = vec![1, 2, 3, 4];
        let codec_even = vec![5, 6, 7];
        let dd_odd = vec![8, 9, 10];
        let dd_even = vec![11, 12];
        let act_odd = vec![8, 9, 10];
        let act_even = vec![11, 12];

        let inputs = vec![2u64, 3, 4, 7, 8, 11, 12, 13];
        for input in inputs {
            if input % 2 == 0 {
                sequencer.push(
                    now_from_ms(1),
                    input,
                    input + offset,
                    123,
                    marker_even,
                    3,
                    &codec_even,
                    4,
                    &dd_even,
                    &act_even,
                );
            } else {
                sequencer.push(
                    now_from_ms(1),
                    input,
                    input + offset,
                    123,
                    marker_odd,
                    3,
                    &codec_odd,
                    3,
                    &dd_odd,
                    &act_odd,
                );
            }
        }

        let requested = vec![9, 10, 13, 14, 15, 16, 17];
        let result =
            sequencer.get_ext_packet_metas(&requested, now_from_ms(IGNORE_RETRANSMISSION_MS + 20));

        let got_sources: Vec<u16> = result.iter().map(|m| m.source_seq_no as u16).collect();
        assert_eq!(got_sources, vec![11, 12]);

        for meta in result {
            if meta.source_seq_no % 2 == 0 {
                assert_eq!(meta.marker, marker_even);
                assert_eq!(meta.codec_bytes(), codec_even);
                assert_eq!(meta.num_codec_bytes_in, 4);
                assert_eq!(meta.dd_bytes(), dd_even);
                assert_eq!(meta.act_bytes, act_even);
            } else {
                assert_eq!(meta.marker, marker_odd);
                assert_eq!(meta.codec_bytes(), codec_odd);
                assert_eq!(meta.num_codec_bytes_in, 3);
                assert_eq!(meta.dd_bytes(), dd_odd);
                assert_eq!(meta.act_bytes, act_odd);
            }
        }
    }

    #[test]
    fn sequencer_get_nack_seq_no_exclusion_matches_upstream_contract() {
        let mut sequencer = Sequencer::new(5, true)
            .expect("sequencer should construct")
            .with_start_time_ns(0);

        let offset = 5u64;
        let marker_odd = true;
        let marker_even = false;
        let codec_odd = vec![1, 2, 3, 4];
        let codec_even = vec![5, 6, 7];
        let codec_oversized = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let dd_odd = vec![8, 9, 10];
        let dd_even = vec![11, 12];
        let dd_oversized = vec![11, 12, 13, 14, 15, 16, 17, 18, 19];
        let act_odd = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let act_even = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        let inputs = vec![
            (65526u64, false),
            (65524, false),
            (65525, false),
            (65529, false),
            (65530, false),
            (65531, true),
            (65533, false),
            (65532, true),
            (65534, false),
        ];

        for (seq_no, is_padding) in inputs {
            if is_padding {
                sequencer.push_padding(seq_no + offset, seq_no + offset);
                continue;
            }

            if seq_no % 5 == 0 {
                sequencer.push(
                    now_from_ms(1),
                    seq_no,
                    seq_no + offset,
                    123,
                    marker_odd,
                    3,
                    &codec_oversized,
                    codec_oversized.len(),
                    &dd_oversized,
                    &act_odd,
                );
            } else if seq_no % 2 == 0 {
                sequencer.push(
                    now_from_ms(1),
                    seq_no,
                    seq_no + offset,
                    123,
                    marker_even,
                    3,
                    &codec_even,
                    4,
                    &dd_even,
                    &act_even,
                );
            } else {
                sequencer.push(
                    now_from_ms(1),
                    seq_no,
                    seq_no + offset,
                    123,
                    marker_odd,
                    3,
                    &codec_odd,
                    3,
                    &dd_odd,
                    &act_odd,
                );
            }
        }

        let requested = vec![65531, 65532, 65535, 0, 1, 2, 3];
        let result =
            sequencer.get_ext_packet_metas(&requested, now_from_ms(IGNORE_RETRANSMISSION_MS + 20));

        let got_sources: Vec<u16> = result.iter().map(|m| m.source_seq_no as u16).collect();
        assert_eq!(got_sources, vec![65530, 65533, 65534]);

        for meta in result {
            if meta.source_seq_no % 5 == 0 {
                assert_eq!(meta.marker, marker_odd);
                assert_eq!(meta.codec_bytes(), codec_oversized);
                assert_eq!(meta.num_codec_bytes_in, codec_oversized.len() as u8);
                assert_eq!(meta.dd_bytes(), dd_oversized);
                assert_eq!(meta.dd_bytes_size, dd_oversized.len() as u8);
                assert_eq!(meta.act_bytes, act_odd);
            } else if meta.source_seq_no % 2 == 0 {
                assert_eq!(meta.marker, marker_even);
                assert_eq!(meta.codec_bytes(), codec_even);
                assert_eq!(meta.num_codec_bytes_in, 4);
                assert_eq!(meta.dd_bytes(), dd_even);
                assert_eq!(meta.dd_bytes_size, dd_even.len() as u8);
                assert_eq!(meta.act_bytes, act_even);
            } else {
                assert_eq!(meta.marker, marker_odd);
                assert_eq!(meta.codec_bytes(), codec_odd);
                assert_eq!(meta.num_codec_bytes_in, 3);
                assert_eq!(meta.dd_bytes(), dd_odd);
                assert_eq!(meta.dd_bytes_size, dd_odd.len() as u8);
                assert_eq!(meta.act_bytes, act_odd);
            }
        }
    }
}

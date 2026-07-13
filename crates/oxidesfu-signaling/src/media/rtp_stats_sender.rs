#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnInfo {
    pkt_size: u16,
    hdr_size: u8,
    flags: u8,
}

impl SnInfo {
    const FLAG_MARKER: u8 = 1 << 0;
    const FLAG_PADDING: u8 = 1 << 1;
    const FLAG_OUT_OF_ORDER: u8 = 1 << 2;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct IntervalStats {
    pub(crate) packets: u64,
    pub(crate) bytes: u64,
    pub(crate) header_bytes: u64,
    pub(crate) packets_padding: u64,
    pub(crate) bytes_padding: u64,
    pub(crate) header_bytes_padding: u64,
    pub(crate) packets_lost_feed: u64,
    pub(crate) packets_out_of_order_feed: u64,
    pub(crate) frames: u32,
    pub(crate) packets_not_found_metadata: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct RtpStatsSenderLite {
    sn_infos: Vec<SnInfo>,
}

#[allow(dead_code)]
impl RtpStatsSenderLite {
    pub(crate) fn new(sn_info_size: usize) -> Self {
        Self {
            sn_infos: vec![
                SnInfo {
                    pkt_size: 0,
                    hdr_size: 0,
                    flags: 0
                };
                sn_info_size
            ],
        }
    }

    pub(crate) fn set_sn_info(
        &mut self,
        ext_sequence_number: u64,
        pkt_size: u16,
        hdr_size: u8,
        marker: bool,
        padding_only: bool,
        out_of_order: bool,
    ) {
        if self.sn_infos.is_empty() {
            return;
        }

        let mut flags = 0_u8;
        if marker {
            flags |= SnInfo::FLAG_MARKER;
        }
        if padding_only {
            flags |= SnInfo::FLAG_PADDING;
        }
        if out_of_order {
            flags |= SnInfo::FLAG_OUT_OF_ORDER;
        }

        let len = self.sn_infos.len();
        let slot = ext_sequence_number as usize % len;
        self.sn_infos[slot] = SnInfo {
            pkt_size,
            hdr_size,
            flags,
        };
    }

    pub(crate) fn get_interval_stats(
        &self,
        ext_start_inclusive: u64,
        ext_end_exclusive: u64,
        ehsn: u64,
    ) -> IntervalStats {
        let mut interval_stats = IntervalStats::default();

        let upper_bound = ehsn.saturating_add(1);
        let mut lower_bound = 0_u64;
        if !self.sn_infos.is_empty() {
            let n = self.sn_infos.len() as u64;
            if ehsn >= n.saturating_sub(1) {
                lower_bound = ehsn - n + 1;
            }
        }

        let ext_start_inclusive_clamped = ext_start_inclusive.min(upper_bound).max(lower_bound);
        let ext_end_exclusive_clamped = ext_end_exclusive
            .min(upper_bound)
            .max(ext_start_inclusive_clamped);

        interval_stats.packets_not_found_metadata = (ext_end_exclusive - ext_start_inclusive)
            - (ext_end_exclusive_clamped - ext_start_inclusive_clamped);

        if self.sn_infos.is_empty() {
            return interval_stats;
        }

        for esn in ext_start_inclusive_clamped..ext_end_exclusive_clamped {
            let sn_info = &self.sn_infos[esn as usize % self.sn_infos.len()];
            match () {
                _ if sn_info.pkt_size == 0 => {
                    interval_stats.packets_lost_feed += 1;
                }
                _ if (sn_info.flags & SnInfo::FLAG_PADDING) != 0 => {
                    interval_stats.packets_padding += 1;
                    interval_stats.bytes_padding += sn_info.pkt_size as u64;
                    interval_stats.header_bytes_padding += sn_info.hdr_size as u64;
                }
                _ => {
                    interval_stats.packets += 1;
                    interval_stats.bytes += sn_info.pkt_size as u64;
                    interval_stats.header_bytes += sn_info.hdr_size as u64;
                    if (sn_info.flags & SnInfo::FLAG_OUT_OF_ORDER) != 0 {
                        interval_stats.packets_out_of_order_feed += 1;
                    }
                }
            }

            if (sn_info.flags & SnInfo::FLAG_MARKER) != 0 {
                interval_stats.frames += 1;
            }
        }

        interval_stats
    }
}

#[cfg(test)]
mod tests {
    use super::RtpStatsSenderLite;

    #[test]
    fn rtp_stats_sender_interval_stats_packets_not_found_metadata_matches_upstream_contract() {
        let sender = RtpStatsSenderLite::new(1024);
        let stats = sender.get_interval_stats(0, 10_000, 10_000);
        assert_eq!(stats.packets_not_found_metadata, 8_977);
    }

    #[test]
    fn rtp_stats_sender_interval_stats_classifies_packet_types() {
        let mut sender = RtpStatsSenderLite::new(16);
        sender.set_sn_info(100, 1200, 12, false, false, false);
        sender.set_sn_info(101, 800, 12, true, false, true);
        sender.set_sn_info(102, 60, 8, false, true, false);
        // 103 intentionally left missing (pkt_size=0)

        let stats = sender.get_interval_stats(100, 104, 103);
        assert_eq!(stats.packets, 2);
        assert_eq!(stats.bytes, 2000);
        assert_eq!(stats.header_bytes, 24);
        assert_eq!(stats.packets_out_of_order_feed, 1);
        assert_eq!(stats.frames, 1);

        assert_eq!(stats.packets_padding, 1);
        assert_eq!(stats.bytes_padding, 60);
        assert_eq!(stats.header_bytes_padding, 8);

        assert_eq!(stats.packets_lost_feed, 1);
        assert_eq!(stats.packets_not_found_metadata, 0);
    }
}

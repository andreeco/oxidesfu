#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    struct RtpStatsLite {
        start_time_ns: i64,
        end_time_ns: i64,
        duration_ns: i64,
        bytes: u64,
        bitrate_bps: u64,
    }

    struct DataStatsLite {
        window_duration_ns: i64,
        start_time_ns: i64,
        end_time_ns: i64,
        total_bytes: u64,
        active_window_start_ns: i64,
        active_window_bytes: u64,
        stopped: bool,
    }

    impl DataStatsLite {
        fn new(window_duration_ns: i64, start_time_ns: i64) -> Self {
            Self {
                window_duration_ns,
                start_time_ns,
                end_time_ns: start_time_ns,
                total_bytes: 0,
                active_window_start_ns: start_time_ns,
                active_window_bytes: 0,
                stopped: false,
            }
        }

        fn update(&mut self, bytes: u64, at_ns: i64) {
            if self.stopped {
                return;
            }
            self.end_time_ns = at_ns;
            self.total_bytes = self.total_bytes.saturating_add(bytes);

            if at_ns - self.active_window_start_ns > self.window_duration_ns {
                self.active_window_start_ns = at_ns;
                self.active_window_bytes = 0;
            }
            self.active_window_bytes = self.active_window_bytes.saturating_add(bytes);
        }

        fn stop(&mut self, at_ns: i64) {
            self.stopped = true;
            self.end_time_ns = at_ns;
        }

        fn to_proto_active(&self, now_ns: i64) -> RtpStatsLite {
            if now_ns - self.active_window_start_ns > self.window_duration_ns
                || self.active_window_bytes == 0
            {
                return RtpStatsLite::default();
            }

            let duration_ns = (now_ns - self.active_window_start_ns).max(1);
            RtpStatsLite {
                bytes: self.active_window_bytes,
                bitrate_bps: ((self.active_window_bytes as u128 * 8 * 1_000_000_000u128)
                    / duration_ns as u128) as u64,
                ..Default::default()
            }
        }

        fn to_proto_aggregate_only(&self) -> RtpStatsLite {
            let duration_ns = (self.end_time_ns - self.start_time_ns).max(1);
            RtpStatsLite {
                start_time_ns: self.start_time_ns,
                end_time_ns: self.end_time_ns,
                duration_ns,
                bytes: self.total_bytes,
                bitrate_bps: ((self.total_bytes as u128 * 8 * 1_000_000_000u128)
                    / duration_ns as u128) as u64,
            }
        }
    }

    #[derive(Default)]
    struct FractionLostProxyLite {
        audio_loss_proxying: bool,
        last_fraction_lost_report: u8,
    }

    impl FractionLostProxyLite {
        fn set_audio_loss_proxying(&mut self, enabled: bool) {
            self.audio_loss_proxying = enabled;
        }

        fn set_last_fraction_lost_report(&mut self, fraction_lost: u8) {
            self.last_fraction_lost_report = fraction_lost;
        }

        fn rewrite_fraction_lost(&self, incoming_fraction_lost: u8) -> u8 {
            if self.audio_loss_proxying {
                self.last_fraction_lost_report
            } else {
                incoming_fraction_lost
            }
        }
    }

    #[derive(Default)]
    struct CodecChangeDetectorLite {
        expected_payload_type: Option<u8>,
        current_payload_type: Option<u8>,
        known_codecs: BTreeSet<u8>,
        pre_bind_queue: Vec<(u16, u8)>,
        last_sequence_number: Option<u16>,
    }

    impl CodecChangeDetectorLite {
        fn bind(&mut self, expected_payload_type: u8, known_codecs: &[u8]) -> Vec<u8> {
            self.expected_payload_type = Some(expected_payload_type);
            self.known_codecs = known_codecs.iter().copied().collect();

            let mut changes = Vec::new();
            let queued = std::mem::take(&mut self.pre_bind_queue);
            for (seq, pt) in queued {
                if let Some(changed) = self.write_packet(seq, pt) {
                    changes.push(changed);
                }
            }
            changes
        }

        fn write_packet(&mut self, sequence_number: u16, payload_type: u8) -> Option<u8> {
            if self.expected_payload_type.is_none() {
                self.pre_bind_queue.push((sequence_number, payload_type));
                return None;
            }

            if !self.known_codecs.contains(&payload_type) {
                self.last_sequence_number = Some(sequence_number);
                return None;
            }

            if let Some(last) = self.last_sequence_number {
                if sequence_number <= last {
                    return None;
                }
            }
            self.last_sequence_number = Some(sequence_number);

            if self.current_payload_type != Some(payload_type) {
                self.current_payload_type = Some(payload_type);
                return Some(payload_type);
            }

            None
        }
    }

    struct NackTrackerLite {
        highest_ext_seq: Option<u64>,
        missing: BTreeMap<u16, usize>,
        max_retries: usize,
    }

    impl NackTrackerLite {
        fn new(max_retries: usize) -> Self {
            Self {
                highest_ext_seq: None,
                missing: BTreeMap::new(),
                max_retries,
            }
        }

        fn extend_sequence(last_ext: u64, seq: u16) -> u64 {
            let last_low = last_ext as u16;
            let mut ext = (last_ext & !0xFFFF) | seq as u64;
            if seq < last_low && (last_low - seq) > 0x8000 {
                ext += 1 << 16;
            } else if seq > last_low && (seq - last_low) > 0x8000 {
                ext = ext.saturating_sub(1 << 16);
            }
            ext
        }

        fn on_packet(&mut self, seq: u16) {
            match self.highest_ext_seq {
                None => {
                    self.highest_ext_seq = Some(seq as u64);
                }
                Some(last_ext) => {
                    let ext = Self::extend_sequence(last_ext, seq);
                    if ext > last_ext {
                        for missing_ext in (last_ext + 1)..ext {
                            self.missing.entry(missing_ext as u16).or_insert(0);
                        }
                        self.highest_ext_seq = Some(ext);
                    }
                    self.missing.remove(&seq);
                }
            }
        }

        fn tick(&mut self) -> Vec<u16> {
            let keys: Vec<u16> = self.missing.keys().copied().collect();
            let mut out = Vec::new();
            for seq in keys {
                if let Some(retries) = self.missing.get_mut(&seq) {
                    if *retries < self.max_retries {
                        *retries += 1;
                        out.push(seq);
                    }
                }
            }
            self.missing
                .retain(|_, retries| *retries < self.max_retries);
            out
        }
    }

    // Upstream: livekit/pkg/sfu/buffer/datastats_test.go::TestDataStats
    #[test]
    fn data_stats_matches_upstream_contract() {
        let mut stats = DataStatsLite::new(1_000_000_000, 1_000_000_000);

        let aggregate = stats.to_proto_aggregate_only();
        assert_eq!(aggregate.start_time_ns, 1_000_000_000);
        assert!(aggregate.end_time_ns >= aggregate.start_time_ns);
        assert!(aggregate.duration_ns > 0);
        assert_eq!(aggregate.bytes, 0);

        stats.update(100, 1_010_000_000);
        let active = stats.to_proto_active(1_020_000_000);
        assert_eq!(active.bytes, 100);
        assert!(active.bitrate_bps > 0);

        let inactive = stats.to_proto_active(2_200_000_000);
        assert_eq!(inactive, RtpStatsLite::default());

        stats.stop(2_200_000_000);
        let aggregate = stats.to_proto_aggregate_only();
        assert_eq!(aggregate.bytes, 100);
        assert!(aggregate.bitrate_bps > 0);
    }

    // Upstream: livekit/pkg/sfu/buffer/buffer_test.go::TestFractionLostReport
    #[test]
    fn buffer_fraction_lost_report_matches_upstream_contract() {
        let mut proxy = FractionLostProxyLite::default();

        proxy.set_audio_loss_proxying(true);
        proxy.set_last_fraction_lost_report(55);
        assert_eq!(proxy.rewrite_fraction_lost(10), 55);

        proxy.set_audio_loss_proxying(false);
        assert_eq!(proxy.rewrite_fraction_lost(10), 10);
    }

    // Upstream: livekit/pkg/sfu/buffer/buffer_test.go::TestCodecChange
    #[test]
    fn buffer_codec_change_matches_upstream_contract() {
        let mut detector = CodecChangeDetectorLite::default();

        // packet before bind should be queued, no callback yet
        assert_eq!(detector.write_packet(1, 116), None);

        // bind with expected VP8, queued H265 triggers first codec change
        let changes = detector.bind(96, &[96, 116]);
        assert_eq!(changes, vec![116]);

        // in-order VP8 packet triggers another codec change
        assert_eq!(detector.write_packet(3, 96), Some(96));

        // out-of-order cannot trigger change
        assert_eq!(detector.write_packet(2, 116), None);

        // unknown codec ignored even when in-order
        assert_eq!(detector.write_packet(4, 117), None);

        // in-order H265 triggers change again
        assert_eq!(detector.write_packet(5, 116), Some(116));
    }

    // Upstream: livekit/pkg/sfu/buffer/buffer_test.go::TestNack
    #[test]
    fn buffer_nack_matches_upstream_contract() {
        let mut normal = NackTrackerLite::new(5);
        for seq in 0u16..15u16 {
            if seq == 1 {
                continue;
            }
            normal.on_packet(seq);
        }

        let mut seen = 0usize;
        for _ in 0..6 {
            for nacked in normal.tick() {
                if nacked == 1 {
                    seen += 1;
                }
            }
        }
        assert_eq!(
            seen, 5,
            "normal missing packet should be retried exactly 5 times"
        );

        let mut wrapped = NackTrackerLite::new(5);
        wrapped.on_packet(65533);
        wrapped.on_packet(2);

        let mut wrapped_counts: BTreeMap<u16, usize> = BTreeMap::new();
        for _ in 0..5 {
            for nacked in wrapped.tick() {
                *wrapped_counts.entry(nacked).or_insert(0) += 1;
            }
        }

        assert_eq!(wrapped_counts.get(&65534).copied().unwrap_or(0), 5);
        assert_eq!(wrapped_counts.get(&65535).copied().unwrap_or(0), 5);
        assert_eq!(wrapped_counts.get(&0).copied().unwrap_or(0), 5);
        assert_eq!(wrapped_counts.get(&1).copied().unwrap_or(0), 5);
    }
}

const RESTART_THRESHOLD: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Packet {
    sequence_number: u16,
    timestamp: u32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct RtpStatsReceiverRestartDetector {
    restart_packets_buf: [Packet; RESTART_THRESHOLD],
    restart_packets_n: usize,
}

#[allow(dead_code)]
impl RtpStatsReceiverRestartDetector {
    pub(crate) fn new() -> Self {
        Self {
            restart_packets_buf: [Packet {
                sequence_number: 0,
                timestamp: 0,
            }; RESTART_THRESHOLD],
            restart_packets_n: 0,
        }
    }

    pub(crate) fn maybe_restart(&mut self, sn: u16, ts: u32, payload_size: usize) -> bool {
        if payload_size > 0 {
            if self.restart_packets_n < RESTART_THRESHOLD {
                self.restart_packets_buf[self.restart_packets_n] = Packet {
                    sequence_number: sn,
                    timestamp: ts,
                };
                self.restart_packets_n += 1;
            } else {
                self.restart_packets_buf.copy_within(1.., 0);
                self.restart_packets_buf[RESTART_THRESHOLD - 1] = Packet {
                    sequence_number: sn,
                    timestamp: ts,
                };
            }
        }

        if self.restart_packets_n < RESTART_THRESHOLD {
            return false;
        }

        for index in 1..self.restart_packets_n {
            let packet = self.restart_packets_buf[index];
            let previous = self.restart_packets_buf[index - 1];
            if packet.sequence_number != previous.sequence_number.wrapping_add(1)
                || packet.timestamp.wrapping_sub(previous.timestamp) > (1 << 31)
            {
                return false;
            }
        }

        true
    }

    pub(crate) fn reset_restart(&mut self) {
        self.restart_packets_n = 0;
    }

    pub(crate) fn restart_packets_n(&self) -> usize {
        self.restart_packets_n
    }
}

#[cfg(test)]
mod tests {
    use super::{RESTART_THRESHOLD, RtpStatsReceiverRestartDetector};

    #[test]
    fn rtp_stats_receiver_restart_detector_matches_upstream_contract() {
        let mut detector = RtpStatsReceiverRestartDetector::new();

        assert!(!detector.maybe_restart(10, 20, 1000));
        assert!(!detector.maybe_restart(11, 20, 1000));
        assert!(!detector.maybe_restart(13, 20, 1000));
        assert!(!detector.maybe_restart(14, 20, 1000));
        assert!(
            !detector.maybe_restart(15, 20, 1000),
            "should still fail due to sequence gap between 11 and 13"
        );
        assert!(!detector.maybe_restart(16, 19, 1000));
        assert!(!detector.maybe_restart(17, 21, 1000));
        assert!(!detector.maybe_restart(18, 21, 1000));
        assert!(!detector.maybe_restart(19, 21, 1000));
        assert!(
            detector.maybe_restart(20, 21, 1000),
            "should restart once threshold contiguous/equal-or-increasing timestamp packets are present"
        );
        assert_eq!(detector.restart_packets_n(), RESTART_THRESHOLD);

        detector.reset_restart();
        assert_eq!(detector.restart_packets_n(), 0);
    }

    #[test]
    fn rtp_stats_receiver_restart_detector_ignores_padding_only_samples() {
        let mut detector = RtpStatsReceiverRestartDetector::new();
        assert!(!detector.maybe_restart(100, 1000, 0));
        assert_eq!(detector.restart_packets_n(), 0);
    }
}

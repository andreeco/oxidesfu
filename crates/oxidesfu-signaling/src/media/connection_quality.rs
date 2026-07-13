use std::time::{Duration, Instant};

const DECREASE_FACTOR: f64 = 0.8;
const INCREASE_FACTOR: f64 = 0.4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionQualityLite {
    Excellent,
    Good,
    Poor,
    Lost,
}

#[derive(Debug, Clone, Copy)]
struct StreamDeltaLite {
    packets: u32,
    packets_lost: u32,
    rtt_max_ms: u32,
    jitter_max: f64,
}

#[derive(Debug, Clone)]
struct ConnectionStatsLite {
    score: f64,
    muted: bool,
    unmuted_at: Instant,
    last_window_duration: Duration,
    packet_loss_weight: f64,
}

impl ConnectionStatsLite {
    fn new(now: Instant) -> Self {
        Self {
            score: 100.0,
            muted: false,
            unmuted_at: now,
            last_window_duration: Duration::from_secs(5),
            packet_loss_weight: 8.0,
        }
    }

    fn update_mute_at(&mut self, is_muted: bool, at: Instant) {
        if is_muted {
            self.muted = true;
            // muting when already LOST should not lift quality
            if self.quality() != ConnectionQualityLite::Lost {
                self.score = 100.0;
            }
        } else {
            self.muted = false;
            self.unmuted_at = at;
        }
    }

    fn update_score_at(
        &mut self,
        at: Instant,
        window_duration: Duration,
        streams: &[StreamDeltaLite],
        last_sender_report_time: Option<Instant>,
    ) {
        self.last_window_duration = window_duration;

        let unmute_threshold = window_duration / 2;
        if self.muted || at.saturating_duration_since(self.unmuted_at) < unmute_threshold {
            return;
        }

        if streams.is_empty() {
            return;
        }

        let total_packets: u32 = streams.iter().map(|s| s.packets).sum();
        let total_lost: u32 = streams.iter().map(|s| s.packets_lost).sum();
        let max_rtt = streams.iter().map(|s| s.rtt_max_ms).max().unwrap_or(0);
        let max_jitter = streams.iter().map(|s| s.jitter_max).fold(0.0f64, f64::max);

        let target = if total_packets == 0 {
            match last_sender_report_time {
                Some(last_sr) if at.saturating_duration_since(last_sr) > window_duration => 0.0,
                _ => 20.0,
            }
        } else {
            let packets_f64 = f64::from(total_packets);
            let lost_f64 = f64::from(total_lost);
            let loss_ratio = if packets_f64 > 0.0 {
                lost_f64 / packets_f64
            } else {
                0.0
            };

            // mimic upstream's packet-volume weighting: low packet counts (DTX-ish windows)
            // should discount loss impact.
            let pps_weight = if total_packets >= 100 {
                1.0
            } else {
                let normalized = packets_f64 / 100.0;
                normalized * normalized * normalized
            };

            let loss_penalty = loss_ratio * 100.0 * self.packet_loss_weight * pps_weight;

            // RTT/jitter knockdown, tuned to keep quality transitions aligned with upstream test intent.
            let delay_penalty = (f64::from(max_rtt) / 80.0) + (max_jitter / 5000.0);

            (100.0 - loss_penalty - delay_penalty).clamp(0.0, 100.0)
        };

        if target < self.score {
            self.score = self.score + (target - self.score) * DECREASE_FACTOR;
        } else {
            self.score = self.score + (target - self.score) * INCREASE_FACTOR;
        }

        self.score = self.score.clamp(0.0, 100.0);
    }

    fn mos(&self) -> f32 {
        (1.0 + (self.score / 100.0) * 3.5) as f32
    }

    fn quality(&self) -> ConnectionQualityLite {
        if self.score <= 20.0 {
            ConnectionQualityLite::Lost
        } else if self.score <= 40.0 {
            ConnectionQualityLite::Poor
        } else if self.score <= 82.0 {
            ConnectionQualityLite::Good
        } else {
            ConnectionQualityLite::Excellent
        }
    }

    fn get_score_and_quality(&self) -> (f32, ConnectionQualityLite) {
        (self.mos(), self.quality())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream: livekit/pkg/sfu/connectionquality/connectionstats_test.go::TestConnectionQuality
    #[test]
    fn connection_quality_matches_upstream_contract() {
        let window = Duration::from_secs(5);
        let mut now = Instant::now();

        let mut cs = ConnectionStatsLite::new(now - window);
        cs.update_mute_at(false, now - Duration::from_secs(1));

        // no data + not enough unmute history should hold EXCELLENT
        cs.update_score_at(now, window, &[], None);
        assert_eq!(
            cs.get_score_and_quality().1,
            ConnectionQualityLite::Excellent
        );

        // best conditions keep EXCELLENT
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(
            cs.get_score_and_quality().1,
            ConnectionQualityLite::Excellent
        );

        // 12% loss for Opus should drop to POOR
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[
                StreamDeltaLite {
                    packets: 120,
                    packets_lost: 30,
                    rtt_max_ms: 0,
                    jitter_max: 0.0,
                },
                StreamDeltaLite {
                    packets: 130,
                    packets_lost: 0,
                    rtt_max_ms: 0,
                    jitter_max: 0.0,
                },
            ],
            None,
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Poor);

        // recovery path: GOOD in one window
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Good);

        // still GOOD
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Good);

        // then EXCELLENT
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(
            cs.get_score_and_quality().1,
            ConnectionQualityLite::Excellent
        );

        // 5% loss -> GOOD
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 13,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Good);

        // one more good window -> still GOOD
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Good);

        // one more -> EXCELLENT
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(
            cs.get_score_and_quality().1,
            ConnectionQualityLite::Excellent
        );

        // drop to POOR, then mute should bump back to EXCELLENT
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 30,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Poor);
        cs.update_mute_at(true, now + Duration::from_secs(1));
        assert_eq!(
            cs.get_score_and_quality().1,
            ConnectionQualityLite::Excellent
        );

        // unmute not old enough + no packets => hold EXCELLENT
        cs.update_mute_at(false, now + Duration::from_secs(3));
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 0,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(
            cs.get_score_and_quality().1,
            ConnectionQualityLite::Excellent
        );

        // dry spell without sender report => POOR
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 0,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Poor);

        // dry spell with fresh sender report => still POOR
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 0,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            Some(now + Duration::from_secs(1)),
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Poor);

        // sender report stale => LOST
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 0,
                packets_lost: 0,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            Some(now - window - Duration::from_millis(1)),
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Lost);

        // mute when LOST should stay LOST
        cs.update_mute_at(true, now + Duration::from_secs(1));
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Lost);

        // unmute + sustained good conditions recovers to EXCELLENT
        cs.update_mute_at(false, now + Duration::from_secs(2));
        for _ in 0..4 {
            now += window;
            cs.update_score_at(
                now + window,
                window,
                &[StreamDeltaLite {
                    packets: 250,
                    packets_lost: 0,
                    rtt_max_ms: 0,
                    jitter_max: 0.0,
                }],
                None,
            );
        }
        assert_eq!(
            cs.get_score_and_quality().1,
            ConnectionQualityLite::Excellent
        );

        // low packet count with 10% loss should not knock down quality (DTX-ish weighting)
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 50,
                packets_lost: 5,
                rtt_max_ms: 0,
                jitter_max: 0.0,
            }],
            None,
        );
        assert_eq!(
            cs.get_score_and_quality().1,
            ConnectionQualityLite::Excellent
        );

        // RTT + jitter can drop EXCELLENT to GOOD even with moderate loss
        cs.update_mute_at(true, now + Duration::from_secs(1));
        cs.update_mute_at(false, now + Duration::from_secs(2));
        now += window;
        cs.update_score_at(
            now + window,
            window,
            &[StreamDeltaLite {
                packets: 250,
                packets_lost: 5,
                rtt_max_ms: 400,
                jitter_max: 30_000.0,
            }],
            None,
        );
        assert_eq!(cs.get_score_and_quality().1, ConnectionQualityLite::Good);
    }
}

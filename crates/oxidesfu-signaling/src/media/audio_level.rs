#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    const SAMPLES_PER_BATCH: usize = 25;
    const DEFAULT_ACTIVE_LEVEL: u8 = 30;
    const DEFAULT_PERCENTILE: u8 = 10;
    const DEFAULT_OBSERVE_DURATION_MS: u32 = 500;

    #[derive(Debug, Clone, Copy)]
    struct AudioSample {
        level: u8,
        duration_ms: u32,
        at_ns: i64,
    }

    #[derive(Debug)]
    struct AudioLevelLite {
        active_level: u8,
        min_percentile: u8,
        observe_duration_ms: u32,
        samples: VecDeque<AudioSample>,
        last_observed_at_ns: Option<i64>,
    }

    impl AudioLevelLite {
        fn new(active_level: u8, min_percentile: u8, observe_duration_ms: u32) -> Self {
            Self {
                active_level,
                min_percentile,
                observe_duration_ms,
                samples: VecDeque::new(),
                last_observed_at_ns: None,
            }
        }

        fn observe(&mut self, level: u8, duration_ms: u32, at_ns: i64) {
            self.last_observed_at_ns = Some(at_ns);
            self.samples.push_back(AudioSample {
                level,
                duration_ms,
                at_ns,
            });
        }

        fn observe_with_rtp_timestamp(&mut self, level: u8, _rtp_ts: u32, at_ns: i64) {
            self.observe(level, 20, at_ns);
        }

        fn get_level(&mut self, now_ns: i64) -> (f64, bool) {
            let stale_cutoff_ms = i64::from(self.observe_duration_ms) * 3;
            if self
                .last_observed_at_ns
                .is_some_and(|last| (now_ns - last) / 1_000_000 > stale_cutoff_ms)
            {
                self.samples.clear();
                return (0.0, false);
            }

            let window_ns = i64::from(self.observe_duration_ms) * 1_000_000;
            let oldest = now_ns - window_ns;
            while self
                .samples
                .front()
                .is_some_and(|sample| sample.at_ns < oldest)
            {
                self.samples.pop_front();
            }

            let mut total_ms: u64 = 0;
            let mut noisy_ms: u64 = 0;
            let mut noisy_weighted_level_sum = 0.0_f64;
            for sample in &self.samples {
                let duration = u64::from(sample.duration_ms);
                total_ms += duration;
                if sample.level <= self.active_level {
                    noisy_ms += duration;
                    noisy_weighted_level_sum += convert_audio_level(f64::from(sample.level))
                        * f64::from(sample.duration_ms);
                }
            }

            if total_ms == 0 || noisy_ms == 0 {
                return (0.0, false);
            }

            if total_ms < u64::from(self.observe_duration_ms) {
                return (0.0, false);
            }

            if noisy_ms * 100 < total_ms * u64::from(self.min_percentile) {
                return (0.0, false);
            }

            (noisy_weighted_level_sum / noisy_ms as f64, true)
        }
    }

    fn convert_audio_level(level: f64) -> f64 {
        if level >= 127.0 {
            0.0
        } else {
            10.0_f64.powf((127.0 - level) / 20.0)
        }
    }

    fn observe_samples(a: &mut AudioLevelLite, level: u8, count: usize, base_time_ns: i64) {
        for i in 0..count {
            let at = base_time_ns + (i as i64) * 20_000_000;
            a.observe(level, 20, at);
        }
    }

    fn observe_samples_with_rtp_timestamp(
        a: &mut AudioLevelLite,
        level: u8,
        count: usize,
        base_time_ns: i64,
    ) {
        let mut sample_ts = 10_000_u32;
        for i in 0..count {
            let at = base_time_ns + (i as i64) * 20_000_000;
            if i % 5 == 0 {
                a.observe_with_rtp_timestamp(level, sample_ts.saturating_sub(1920), at);
            }
            a.observe_with_rtp_timestamp(level, sample_ts, at);
            sample_ts = sample_ts.wrapping_add(960);
        }
    }

    #[test]
    fn audio_level_matches_upstream_contract() {
        let mut clock_ns = 1_000_000_000_i64;
        let mut a = AudioLevelLite::new(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION_MS,
        );
        let (_, noisy) = a.get_level(clock_ns);
        assert!(!noisy);

        observe_samples(&mut a, 28, 5, clock_ns);
        clock_ns += 5 * 20_000_000;
        let (_, noisy) = a.get_level(clock_ns);
        assert!(!noisy);

        let mut a = AudioLevelLite::new(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION_MS,
        );
        clock_ns = 2_000_000_000;
        observe_samples(&mut a, 35, 100, clock_ns);
        clock_ns += 100 * 20_000_000;
        let (_, noisy) = a.get_level(clock_ns);
        assert!(!noisy);

        let mut a = AudioLevelLite::new(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION_MS,
        );
        clock_ns = 3_000_000_000;
        observe_samples(&mut a, 35, SAMPLES_PER_BATCH - 2, clock_ns);
        clock_ns += (SAMPLES_PER_BATCH as i64 - 2) * 20_000_000;
        observe_samples(&mut a, 25, 1, clock_ns);
        clock_ns += 20_000_000;
        observe_samples(&mut a, 35, 1, clock_ns);
        clock_ns += 20_000_000;
        let (_, noisy) = a.get_level(clock_ns);
        assert!(!noisy);

        let mut a = AudioLevelLite::new(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION_MS,
        );
        clock_ns = 4_000_000_000;
        observe_samples(&mut a, 35, SAMPLES_PER_BATCH - 16, clock_ns);
        clock_ns += (SAMPLES_PER_BATCH as i64 - 16) * 20_000_000;
        observe_samples(&mut a, 25, 8, clock_ns);
        clock_ns += 8 * 20_000_000;
        observe_samples(&mut a, 29, 8, clock_ns);
        clock_ns += 8 * 20_000_000;
        let (level, noisy) = a.get_level(clock_ns);
        assert!(noisy);
        assert!(level > convert_audio_level(f64::from(DEFAULT_ACTIVE_LEVEL)));
        assert!(level < convert_audio_level(25.0));

        let mut a = AudioLevelLite::new(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION_MS,
        );
        clock_ns = 5_000_000_000;
        observe_samples(&mut a, 25, 100, clock_ns);
        clock_ns += 100 * 20_000_000;
        let (level, noisy) = a.get_level(clock_ns);
        assert!(noisy);
        assert!(level > convert_audio_level(f64::from(DEFAULT_ACTIVE_LEVEL)));
        assert!(level < convert_audio_level(20.0));
        clock_ns += 1_500_000_000;
        let (level, noisy) = a.get_level(clock_ns);
        assert_eq!(level, 0.0);
        assert!(!noisy);

        let mut a = AudioLevelLite::new(
            DEFAULT_ACTIVE_LEVEL,
            DEFAULT_PERCENTILE,
            DEFAULT_OBSERVE_DURATION_MS,
        );
        clock_ns = 6_000_000_000;
        observe_samples_with_rtp_timestamp(&mut a, 25, 100, clock_ns);
        clock_ns += 100 * 20_000_000;
        let (level, noisy) = a.get_level(clock_ns);
        assert!(noisy);
        assert!(level > convert_audio_level(f64::from(DEFAULT_ACTIVE_LEVEL)));
        assert!(level < convert_audio_level(20.0));
        clock_ns += 1_500_000_000;
        let (level, noisy) = a.get_level(clock_ns);
        assert_eq!(level, 0.0);
        assert!(!noisy);
    }
}

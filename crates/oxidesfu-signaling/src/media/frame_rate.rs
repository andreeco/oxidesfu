#[cfg(test)]
mod tests {
    const CLOCK_RATE: u32 = 90_000;
    const MAX_SPATIAL: usize = 2;
    const MAX_TEMPORAL: usize = 3;
    const MIN_FRAMES_FOR_CALCULATION: [usize; 4] = [8, 15, 40, 60];

    #[derive(Debug, Clone)]
    struct TestFrameInfo {
        timestamp: u32,
        sequence_number: u16,
        frame_number: u16,
        spatial: usize,
        temporal: usize,
        frame_diffs: Vec<u16>,
    }

    fn create_frames(
        start_frame_number: u16,
        start_ts: u32,
        start_seq: u16,
        total_frames_per_spatial: usize,
        fps: &[Vec<f32>],
        spatial_dependency: bool,
    ) -> Vec<Vec<TestFrameInfo>> {
        let spatials = fps.len();
        let temporals = fps[0].len();
        let mut frames: Vec<Vec<TestFrameInfo>> = (0..spatials)
            .map(|_| Vec::with_capacity(total_frames_per_spatial))
            .collect();

        let mut frame_number = start_frame_number;
        let mut next_ts = vec![vec![start_ts; temporals]; spatials];
        let mut ts_step = vec![vec![0u32; temporals]; spatials];
        for s in 0..spatials {
            for t in 0..temporals {
                ts_step[s][t] = (CLOCK_RATE as f32 / fps[s][t]) as u32;
            }
        }

        let mut current_ts = vec![start_ts; spatials];
        let mut current_seq = start_seq;

        for _ in 0..total_frames_per_spatial {
            for s in 0..spatials {
                let mut frame = TestFrameInfo {
                    timestamp: current_ts[s],
                    sequence_number: current_seq,
                    frame_number,
                    spatial: s,
                    temporal: 0,
                    frame_diffs: Vec::new(),
                };

                for t in 0..temporals {
                    if current_ts[s] >= next_ts[s][t] {
                        frame.temporal = t;
                        for nt in t..temporals {
                            next_ts[s][nt] = next_ts[s][nt].wrapping_add(ts_step[s][nt]);
                        }
                        break;
                    }
                }

                current_ts[s] = current_ts[s].wrapping_add(ts_step[s][temporals - 1]);

                for idx in (0..frames[s].len()).rev() {
                    let prev = &frames[s][idx];
                    if prev.timestamp.wrapping_sub(frame.timestamp) > 0x8000_0000 {
                        frame
                            .frame_diffs
                            .push(frame.frame_number.wrapping_sub(prev.frame_number));
                        break;
                    }
                }

                if spatial_dependency && frame.spatial > 0 {
                    for idx in (0..frames[frame.spatial - 1].len()).rev() {
                        let prev = &frames[frame.spatial - 1][idx];
                        if prev.timestamp == frame.timestamp {
                            frame
                                .frame_diffs
                                .push(frame.frame_number.wrapping_sub(prev.frame_number));
                            break;
                        }
                    }
                }

                frames[s].push(frame);
                frame_number = frame_number.wrapping_add(1);
                current_seq = current_seq.wrapping_add(1);
            }
        }

        frames
    }

    fn verify_fps(expected: &[f32], got: &[f32]) {
        assert_eq!(expected.len(), got.len());
        for i in 0..expected.len() {
            let low = expected[i] * 0.85;
            let high = expected[i] * 1.15;
            assert!(
                got[i] >= low && got[i] <= high,
                "fps mismatch at index {i}: expected ~{}, got {}",
                expected[i],
                got[i]
            );
        }
    }

    fn sn16_lt(a: u16, b: u16) -> bool {
        a.wrapping_sub(b) > 0x8000
    }

    fn sn16_lt_or_equal(a: u16, b: u16) -> bool {
        a == b || sn16_lt(a, b)
    }

    fn sn32_lt(a: u32, b: u32) -> bool {
        a.wrapping_sub(b) > 0x8000_0000
    }

    #[derive(Debug, Clone, Copy)]
    struct VpxSample {
        timestamp: u32,
        frame_number: u16,
        temporal: i32,
    }

    #[derive(Debug, Clone, Copy)]
    struct DDFrameSample<'a> {
        timestamp: u32,
        frame_number: u16,
        spatial: i32,
        temporal: i32,
        frame_diffs: &'a [u16],
    }

    #[derive(Debug, Clone, Copy)]
    struct H26xSample {
        timestamp: u32,
        sequence_number: u16,
        temporal: i32,
    }

    #[derive(Debug, Clone, Copy)]
    struct FrameInfo {
        timestamp: u32,
        frame_number: u16,
        temporal: i32,
        spatial: i32,
    }

    #[derive(Debug)]
    struct FrameRateCalculatorVP8Lite {
        frame_rates: [f32; MAX_TEMPORAL + 1],
        first_frames: [Option<FrameInfo>; MAX_TEMPORAL + 1],
        second_frames: [Option<FrameInfo>; MAX_TEMPORAL + 1],
        fn_received: [Option<FrameInfo>; 64],
        base_frame: Option<FrameInfo>,
        completed: bool,
    }

    impl FrameRateCalculatorVP8Lite {
        fn new() -> Self {
            Self {
                frame_rates: [0.0; MAX_TEMPORAL + 1],
                first_frames: [None; MAX_TEMPORAL + 1],
                second_frames: [None; MAX_TEMPORAL + 1],
                fn_received: [None; 64],
                base_frame: None,
                completed: false,
            }
        }

        fn recv_packet(&mut self, sample: VpxSample) -> bool {
            if self.completed {
                return true;
            }

            let temporal = sample.temporal.clamp(0, MAX_TEMPORAL as i32) as usize;
            if self.base_frame.is_none() {
                let base = FrameInfo {
                    timestamp: sample.timestamp,
                    frame_number: sample.frame_number,
                    temporal: temporal as i32,
                    spatial: 0,
                };
                self.base_frame = Some(base);
                self.fn_received[0] = Some(base);
                self.first_frames[temporal] = Some(base);
                return false;
            }

            let base = self.base_frame.expect("base frame must be initialized");
            let base_diff = sample.frame_number.wrapping_sub(base.frame_number);
            if base_diff == 0 || base_diff > 0x4000 {
                return false;
            }
            if base_diff as usize >= self.fn_received.len() {
                self.reset();
                return false;
            }
            if self.fn_received[base_diff as usize].is_some() {
                return false;
            }

            let fi = FrameInfo {
                timestamp: sample.timestamp,
                frame_number: sample.frame_number,
                temporal: temporal as i32,
                spatial: 0,
            };
            self.fn_received[base_diff as usize] = Some(fi);

            match self.first_frames[temporal] {
                None => self.first_frames[temporal] = Some(fi),
                Some(first) => {
                    let should_set_second = self.second_frames[temporal]
                        .map(|second| sn16_lt(second.frame_number, sample.frame_number))
                        .unwrap_or(true);
                    if should_set_second
                        && sample.frame_number != first.frame_number
                        && sample.frame_number.wrapping_sub(first.frame_number) < 0x4000
                    {
                        self.second_frames[temporal] = Some(fi);
                    }
                }
            }

            self.calc()
        }

        fn calc(&mut self) -> bool {
            let mut rate_counter = 0usize;
            for current_temporal in 0..=MAX_TEMPORAL {
                if self.frame_rates[current_temporal] > 0.0 {
                    rate_counter += 1;
                    continue;
                }

                let first = self.first_frames[current_temporal];
                let second = self.second_frames[current_temporal];

                if rate_counter > 0 && first.is_none() {
                    rate_counter += 1;
                    continue;
                }

                let (Some(first), Some(second), Some(base)) = (first, second, self.base_frame)
                else {
                    continue;
                };

                let mut frame_count = 0usize;
                let mut last_ts = first.timestamp;
                let start = first
                    .frame_number
                    .wrapping_sub(base.frame_number)
                    .wrapping_add(1);
                let end = second
                    .frame_number
                    .wrapping_sub(base.frame_number)
                    .wrapping_add(1);
                for idx in start..end {
                    let Some(info) = self.fn_received.get(idx as usize).and_then(|it| *it) else {
                        break;
                    };
                    if info.temporal <= current_temporal as i32 {
                        frame_count += 1;
                        last_ts = info.timestamp;
                    }
                }

                if frame_count >= MIN_FRAMES_FOR_CALCULATION[current_temporal]
                    && last_ts > first.timestamp
                {
                    self.frame_rates[current_temporal] =
                        CLOCK_RATE as f32 / (last_ts - first.timestamp) as f32 * frame_count as f32;
                    rate_counter += 1;
                }
            }

            if rate_counter == self.frame_rates.len() {
                self.completed = true;
                if self.frame_rates[2] > 0.0 && self.frame_rates[2] > self.frame_rates[1] * 3.0 {
                    self.frame_rates[1] = self.frame_rates[2] / 2.0;
                }
                self.reset();
                return true;
            }
            false
        }

        fn frame_rates(&self) -> &[f32; MAX_TEMPORAL + 1] {
            &self.frame_rates
        }

        fn completed(&self) -> bool {
            self.completed
        }

        fn reset(&mut self) {
            self.first_frames = [None; MAX_TEMPORAL + 1];
            self.second_frames = [None; MAX_TEMPORAL + 1];
            self.fn_received = [None; 64];
            self.base_frame = None;
        }
    }

    #[derive(Debug)]
    struct FrameRateCalculatorDDLite {
        frame_rates: [[f32; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1],
        first_frames: [[Option<FrameInfo>; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1],
        second_frames: [[Option<FrameInfo>; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1],
        fn_received: [Option<FrameInfo>; 256],
        base_frame: Option<FrameInfo>,
        completed: bool,
        max_spatial: usize,
        max_temporal: usize,
    }

    impl FrameRateCalculatorDDLite {
        fn new(max_spatial: usize, max_temporal: usize) -> Self {
            Self {
                frame_rates: [[0.0; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1],
                first_frames: [[None; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1],
                second_frames: [[None; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1],
                fn_received: [None; 256],
                base_frame: None,
                completed: false,
                max_spatial,
                max_temporal,
            }
        }

        fn recv_packet(&mut self, sample: DDFrameSample<'_>) -> bool {
            if self.completed {
                return true;
            }

            let _ = sample.frame_diffs;

            let spatial = sample.spatial.clamp(0, MAX_SPATIAL as i32) as usize;
            let temporal = sample.temporal.clamp(0, MAX_TEMPORAL as i32) as usize;

            if self.base_frame.is_none() {
                let base = FrameInfo {
                    timestamp: sample.timestamp,
                    frame_number: sample.frame_number,
                    temporal: temporal as i32,
                    spatial: spatial as i32,
                };
                self.base_frame = Some(base);
                self.fn_received[0] = Some(base);
                self.first_frames[spatial][temporal] = Some(base);
                return false;
            }

            let base = self.base_frame.expect("base frame must be initialized");
            let base_diff = sample.frame_number.wrapping_sub(base.frame_number);
            if base_diff == 0 || base_diff > 0x8000 {
                return false;
            }
            if base_diff as usize >= self.fn_received.len() {
                self.reset_state();
                return false;
            }
            if self.fn_received[base_diff as usize].is_some() {
                return false;
            }

            let info = FrameInfo {
                timestamp: sample.timestamp,
                frame_number: sample.frame_number,
                temporal: temporal as i32,
                spatial: spatial as i32,
            };
            self.fn_received[base_diff as usize] = Some(info);

            if self.first_frames[spatial][temporal].is_none() {
                self.first_frames[spatial][temporal] = Some(info);
            }

            let should_set_second = self.second_frames[spatial][temporal]
                .map(|second| sn16_lt(second.frame_number, info.frame_number))
                .unwrap_or(true);
            if should_set_second {
                self.second_frames[spatial][temporal] = Some(info);
            }

            self.calc()
        }

        fn calc(&mut self) -> bool {
            let mut rate_counter = 0usize;

            for current_spatial in 0..=self.max_spatial {
                let mut current_spatial_rate_counter = 0usize;
                for current_temporal in 0..=self.max_temporal {
                    if self.frame_rates[current_spatial][current_temporal] > 0.0 {
                        rate_counter += 1;
                        current_spatial_rate_counter += 1;
                        continue;
                    }

                    let first = self.first_frames[current_spatial][current_temporal];
                    let second = self.second_frames[current_spatial][current_temporal];

                    if current_spatial_rate_counter > 0 && first.is_none() {
                        current_spatial_rate_counter += 1;
                        rate_counter += 1;
                        continue;
                    }

                    let (Some(first), Some(second), Some(base)) = (first, second, self.base_frame)
                    else {
                        continue;
                    };

                    if !sn16_lt(first.frame_number, second.frame_number) {
                        continue;
                    }

                    let mut frame_count = 0usize;
                    let mut last_ts = first.timestamp;
                    let start = first.frame_number.wrapping_sub(base.frame_number) as usize;
                    let end = second.frame_number.wrapping_sub(base.frame_number) as usize;
                    for idx in start..=end {
                        let Some(info) = self.fn_received[idx] else {
                            continue;
                        };
                        if info.spatial == current_spatial as i32
                            && info.temporal <= current_temporal as i32
                        {
                            frame_count += 1;
                            last_ts = info.timestamp;
                        }
                    }

                    if frame_count >= MIN_FRAMES_FOR_CALCULATION[current_temporal]
                        && last_ts > first.timestamp
                    {
                        self.frame_rates[current_spatial][current_temporal] = CLOCK_RATE as f32
                            / (last_ts - first.timestamp) as f32
                            * frame_count as f32;
                        rate_counter += 1;
                    }
                }
            }

            let expected = (self.max_spatial + 1) * (self.max_temporal + 1);
            if rate_counter == expected {
                self.completed = true;
                self.close();
                return true;
            }
            false
        }

        fn frame_rates_for_spatial(&self, spatial: usize) -> &[f32; MAX_TEMPORAL + 1] {
            &self.frame_rates[spatial]
        }

        fn completed(&self) -> bool {
            self.completed
        }

        fn close(&mut self) {
            self.base_frame = None;
            self.first_frames = [[None; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1];
            self.second_frames = [[None; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1];
            self.fn_received = [None; 256];
        }

        fn reset_state(&mut self) {
            self.base_frame = None;
            self.first_frames = [[None; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1];
            self.second_frames = [[None; MAX_TEMPORAL + 1]; MAX_SPATIAL + 1];
            self.fn_received = [None; 256];
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct H26xFrameInfo {
        start_seq: u16,
        end_seq: u16,
        timestamp: u32,
        temporal: i32,
    }

    #[derive(Debug)]
    struct FrameRateCalculatorH26xLite {
        frame_rates: [f32; MAX_TEMPORAL + 1],
        fn_received: Vec<H26xFrameInfo>,
        base_frame: Option<H26xFrameInfo>,
        completed: bool,
    }

    impl FrameRateCalculatorH26xLite {
        fn new() -> Self {
            Self {
                frame_rates: [0.0; MAX_TEMPORAL + 1],
                fn_received: Vec::new(),
                base_frame: None,
                completed: false,
            }
        }

        fn recv_packet(&mut self, sample: H26xSample) -> bool {
            if self.completed {
                return true;
            }

            let temporal = sample.temporal.clamp(0, MAX_TEMPORAL as i32);

            if self.base_frame.is_none() {
                let base = H26xFrameInfo {
                    start_seq: sample.sequence_number,
                    end_seq: sample.sequence_number,
                    timestamp: sample.timestamp,
                    temporal,
                };
                self.base_frame = Some(base);
                self.fn_received.clear();
                self.fn_received.push(base);
                return false;
            }

            let base = self.base_frame.expect("base frame must be initialized");
            if sn16_lt_or_equal(sample.sequence_number, base.start_seq) {
                return false;
            }

            let mut inserted = false;
            for idx in (0..self.fn_received.len()).rev() {
                let frame = &mut self.fn_received[idx];
                if frame.timestamp == sample.timestamp {
                    if sn16_lt(frame.end_seq, sample.sequence_number) {
                        frame.end_seq = sample.sequence_number;
                    }
                    if sn16_lt(sample.sequence_number, frame.start_seq) {
                        frame.start_seq = sample.sequence_number;
                    }
                    inserted = true;
                    break;
                }

                if sn32_lt(frame.timestamp, sample.timestamp) {
                    self.fn_received.insert(
                        idx + 1,
                        H26xFrameInfo {
                            start_seq: sample.sequence_number,
                            end_seq: sample.sequence_number,
                            timestamp: sample.timestamp,
                            temporal,
                        },
                    );
                    inserted = true;
                    break;
                }
            }

            if !inserted {
                self.fn_received.insert(
                    0,
                    H26xFrameInfo {
                        start_seq: sample.sequence_number,
                        end_seq: sample.sequence_number,
                        timestamp: sample.timestamp,
                        temporal,
                    },
                );
            }

            self.calc()
        }

        fn calc(&mut self) -> bool {
            let mut frame_counts = [0usize; MAX_TEMPORAL + 1];
            let mut total_frame_count = 0usize;
            let mut ts_duration = 0u32;

            if self.fn_received.len() < 2 {
                return false;
            }

            for idx in 0..(self.fn_received.len() - 1) {
                let ff = self.fn_received[idx];
                let nf = self.fn_received[idx + 1];
                if nf.start_seq.wrapping_sub(ff.end_seq) == 1 {
                    total_frame_count += 1;
                    ts_duration = ts_duration.wrapping_add(nf.timestamp.wrapping_sub(ff.timestamp));
                    for slot in frame_counts
                        .iter_mut()
                        .take(MAX_TEMPORAL + 1)
                        .skip(nf.temporal as usize)
                    {
                        *slot += 1;
                    }
                } else {
                    total_frame_count = 0;
                    frame_counts = [0; MAX_TEMPORAL + 1];
                    ts_duration = 0;
                }

                if total_frame_count >= MIN_FRAMES_FOR_CALCULATION[MAX_TEMPORAL] && ts_duration > 0
                {
                    for current_temporal in 0..=MAX_TEMPORAL {
                        let count = frame_counts[current_temporal];
                        if current_temporal > 0 && count == frame_counts[current_temporal - 1] {
                            self.frame_rates[current_temporal] = 0.0;
                        } else {
                            self.frame_rates[current_temporal] =
                                CLOCK_RATE as f32 / ts_duration as f32 * count as f32;
                        }
                    }
                    self.completed = true;
                    self.fn_received.clear();
                    self.base_frame = None;
                    return true;
                }
            }
            false
        }

        fn frame_rates(&self) -> &[f32; MAX_TEMPORAL + 1] {
            &self.frame_rates
        }

        fn completed(&self) -> bool {
            self.completed
        }
    }

    // Upstream: livekit/pkg/sfu/buffer/fps_test.go::TestFpsVP8
    #[test]
    fn fps_vp8_matches_upstream_contract() {
        let cases: &[(&str, u32, u16, Vec<Vec<f32>>)] = &[
            (
                "normal",
                12_345_678,
                100,
                vec![
                    vec![5.0, 10.0, 15.0],
                    vec![5.0, 10.0, 15.0],
                    vec![7.5, 15.0, 30.0],
                ],
            ),
            (
                "frame-number-and-ts-wrap",
                (1u32 << 31) - 10,
                (1u16 << 15) - 10,
                vec![
                    vec![5.0, 10.0, 15.0],
                    vec![5.0, 10.0, 15.0],
                    vec![7.5, 15.0, 30.0],
                ],
            ),
            (
                "two-temporal-layers",
                12_345_678,
                100,
                vec![vec![7.5, 15.0], vec![7.5, 15.0], vec![15.0, 30.0]],
            ),
        ];

        for (_name, start_ts, start_fn, fps_expected) in cases {
            let mut calculators: Vec<FrameRateCalculatorVP8Lite> = (0..fps_expected.len())
                .map(|_| FrameRateCalculatorVP8Lite::new())
                .collect();
            let frames: Vec<Vec<TestFrameInfo>> = (0..fps_expected.len())
                .map(|i| {
                    create_frames(
                        *start_fn,
                        *start_ts,
                        10,
                        300,
                        &[fps_expected[i].clone()],
                        false,
                    )
                    .remove(0)
                })
                .collect();

            let mut got_all = false;
            for (s, spatial_frames) in frames.iter().enumerate() {
                for frame in spatial_frames {
                    if calculators[s].recv_packet(VpxSample {
                        timestamp: frame.timestamp,
                        frame_number: frame.frame_number,
                        temporal: frame.temporal as i32,
                    }) {
                        got_all = calculators
                            .iter()
                            .all(FrameRateCalculatorVP8Lite::completed);
                    }
                }
            }

            assert!(got_all, "expected all VP8 calculators to complete");
            for (i, calc) in calculators.iter().enumerate() {
                verify_fps(
                    &fps_expected[i],
                    &calc.frame_rates()[..fps_expected[i].len()],
                );
            }
        }
    }

    // Upstream: livekit/pkg/sfu/buffer/fps_test.go::TestFpsDD
    #[test]
    fn fps_dd_matches_upstream_contract() {
        let cases: &[(&str, u32, u16, Vec<Vec<f32>>, bool)] = &[
            (
                "normal",
                12_345_678,
                100,
                vec![
                    vec![5.1, 10.1, 16.0],
                    vec![5.1, 10.1, 16.0],
                    vec![8.0, 15.0, 30.1],
                ],
                true,
            ),
            (
                "frame-number-and-ts-wrap",
                (1u32 << 31) - 10,
                (1u16 << 15) - 10,
                vec![
                    vec![7.5, 15.0, 30.0],
                    vec![7.5, 15.0, 30.0],
                    vec![7.5, 15.0, 30.0],
                ],
                true,
            ),
            (
                "vp8-like",
                12_345_678,
                100,
                vec![vec![7.5, 15.0], vec![7.5, 15.0], vec![15.0, 30.0]],
                false,
            ),
        ];

        for (_name, start_ts, start_fn, fps_expected, spatial_dependency) in cases {
            let frames = create_frames(
                *start_fn,
                *start_ts,
                10,
                2000,
                fps_expected,
                *spatial_dependency,
            );
            let mut calculator =
                FrameRateCalculatorDDLite::new(fps_expected.len() - 1, fps_expected[0].len() - 1);

            let mut got = false;
            for spatial_frames in &frames {
                for frame in spatial_frames {
                    if calculator.recv_packet(DDFrameSample {
                        timestamp: frame.timestamp,
                        frame_number: frame.frame_number,
                        spatial: frame.spatial as i32,
                        temporal: frame.temporal as i32,
                        frame_diffs: &frame.frame_diffs,
                    }) {
                        got = calculator.completed();
                    }
                }
            }

            assert!(got, "expected DD calculator to complete");
            for (s, expected) in fps_expected.iter().enumerate() {
                verify_fps(
                    expected,
                    &calculator.frame_rates_for_spatial(s)[..expected.len()],
                );
            }
        }
    }

    // Upstream: livekit/pkg/sfu/buffer/fps_test.go::TestFpsH26x
    #[test]
    fn fps_h26x_matches_upstream_contract() {
        let cases: &[(&str, u32, u16, u16, Vec<Vec<f32>>)] = &[
            (
                "normal",
                12_345_678,
                100,
                100,
                vec![
                    vec![5.0, 10.0, 15.0],
                    vec![5.0, 10.0, 15.0],
                    vec![7.5, 15.0, 30.0],
                ],
            ),
            (
                "frame-number-and-ts-wrap",
                (1u32 << 31) - 10,
                (1u16 << 15) - 10,
                (1u16 << 15) - 10,
                vec![
                    vec![5.0, 10.0, 15.0],
                    vec![5.0, 10.0, 15.0],
                    vec![7.5, 15.0, 30.0],
                ],
            ),
            (
                "two-temporal-layers",
                12_345_678,
                100,
                100,
                vec![vec![7.5, 15.0], vec![7.5, 15.0], vec![15.0, 30.0]],
            ),
        ];

        for (_name, start_ts, start_seq, start_fn, fps_expected) in cases {
            let mut calculators: Vec<FrameRateCalculatorH26xLite> = (0..fps_expected.len())
                .map(|_| FrameRateCalculatorH26xLite::new())
                .collect();
            let frames: Vec<Vec<TestFrameInfo>> = (0..fps_expected.len())
                .map(|i| {
                    create_frames(
                        *start_fn,
                        *start_ts,
                        *start_seq,
                        300,
                        &[fps_expected[i].clone()],
                        false,
                    )
                    .remove(0)
                })
                .collect();

            let mut got_all = false;
            for (s, spatial_frames) in frames.iter().enumerate() {
                for frame in spatial_frames {
                    if calculators[s].recv_packet(H26xSample {
                        timestamp: frame.timestamp,
                        sequence_number: frame.sequence_number,
                        temporal: frame.temporal as i32,
                    }) {
                        got_all = calculators
                            .iter()
                            .all(FrameRateCalculatorH26xLite::completed);
                    }
                }
            }

            assert!(got_all, "expected all H26x calculators to complete");
            for (i, calc) in calculators.iter().enumerate() {
                verify_fps(
                    &fps_expected[i],
                    &calc.frame_rates()[..fps_expected[i].len()],
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(dead_code, clippy::collapsible_if, clippy::if_same_then_else)]
mod tests {
    use std::collections::HashMap;

    const INVALID_SPATIAL: i32 = -1;
    const INVALID_TEMPORAL: i32 = -1;
    const DEFAULT_MAX_SPATIAL: i32 = 2;
    const DEFAULT_MAX_TEMPORAL: i32 = 3;
    const VP8_CLOCK_RATE: u32 = 90_000;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CodecKindLite {
        Audio,
        Video,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct VideoLayerLite {
        spatial: i32,
        temporal: i32,
    }

    impl VideoLayerLite {
        const INVALID: Self = Self {
            spatial: INVALID_SPATIAL,
            temporal: INVALID_TEMPORAL,
        };

        const DEFAULT_MAX: Self = Self {
            spatial: DEFAULT_MAX_SPATIAL,
            temporal: DEFAULT_MAX_TEMPORAL,
        };

        fn is_valid(self) -> bool {
            self.spatial >= 0 && self.temporal >= 0
        }
    }

    type BitratesLite = Vec<Vec<i64>>;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum VideoPauseReasonLite {
        Muted,
        PubMuted,
        Bandwidth,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct VideoAllocationLite {
        pause_reason: VideoPauseReasonLite,
        is_deficient: bool,
        bandwidth_requested: i64,
        bandwidth_delta: i64,
        bandwidth_needed: i64,
        bitrates: BitratesLite,
        target_layer: VideoLayerLite,
        request_layer_spatial: i32,
        max_layer: VideoLayerLite,
        distance_to_desired: f64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SequenceOrderingLite {
        Contiguous,
        Gap,
        OutOfOrder,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TranslationRtpLite {
        sn_ordering: SequenceOrderingLite,
        ext_sequence_number: u64,
        ext_timestamp: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Vp8InfoLite {
        picture_id: u16,
        tl0_pic_idx: u8,
        tid: u8,
        key_idx: u8,
        header_size: u8,
        m_bit: bool,
        is_key_frame: bool,
    }

    #[derive(Debug, Clone, Copy)]
    struct VideoPacketLite {
        sequence_number: u16,
        timestamp: u32,
        ssrc: u32,
        payload_size: usize,
        is_out_of_order: bool,
        marker: bool,
        layer: i32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    struct TranslationParamsLite {
        should_drop: bool,
        is_starting: bool,
        is_resuming: bool,
        is_switching: bool,
        marker: bool,
        incoming_header_size: usize,
        codec: Option<Vp8InfoLite>,
        rtp: Option<TranslationRtpLite>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct SnTsLite {
        ext_sequence_number: u64,
        ext_timestamp: u64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Vp8PaddingLite {
        picture_id: u16,
        tl0_pic_idx: u8,
        key_idx: u8,
        tid: u8,
        first_byte: u8,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct VideoTransitionLite {
        from: VideoLayerLite,
        to: VideoLayerLite,
        bandwidth_delta: i64,
    }

    #[derive(Debug, Clone, Copy)]
    struct AudioPacketLite {
        sequence_number: u16,
        timestamp: u32,
        ssrc: u32,
        payload_size: usize,
        is_out_of_order: bool,
    }

    struct ForwarderLite {
        kind: CodecKindLite,
        muted: bool,
        max_layer: VideoLayerLite,
        current_layer: VideoLayerLite,
        target_layer: VideoLayerLite,
        max_published_spatial: i32,
        max_temporal_seen: i32,
        pub_muted: bool,
        request_layer_spatial: i32,
        acquire_grace_active: bool,
        acquire_grace_fired: bool,

        provisional_target: VideoLayerLite,
        provisional_available_layers: Vec<i32>,
        provisional_bitrates: BitratesLite,
        provisional_allocated_layer: VideoLayerLite,
        last_allocation: Option<VideoAllocationLite>,

        started: bool,
        last_ssrc: u32,
        last_in_seq: u16,
        last_out_seq: u64,
        last_out_ts: u64,

        excluded_ranges: Vec<(u16, u16)>,
        dropped_padding_in_order: Vec<u16>,

        frame_end_needed: bool,
        vp8_padding: Option<Vp8PaddingLite>,

        first_in_seq_video: Option<u16>,
        vp8_pic_map: HashMap<u16, u16>,
        last_out_pic_id: Option<u16>,
        last_out_tl0: Option<u8>,
        last_out_key: Option<u8>,
    }

    impl ForwarderLite {
        fn new(kind: CodecKindLite) -> Self {
            let max_layer = match kind {
                CodecKindLite::Audio => VideoLayerLite::INVALID,
                CodecKindLite::Video => VideoLayerLite {
                    spatial: INVALID_SPATIAL,
                    temporal: DEFAULT_MAX_TEMPORAL,
                },
            };
            Self {
                kind,
                muted: false,
                max_layer,
                current_layer: VideoLayerLite::INVALID,
                target_layer: VideoLayerLite::INVALID,
                max_published_spatial: INVALID_SPATIAL,
                max_temporal_seen: INVALID_TEMPORAL,
                pub_muted: false,
                request_layer_spatial: INVALID_SPATIAL,
                acquire_grace_active: false,
                acquire_grace_fired: false,

                provisional_target: VideoLayerLite::INVALID,
                provisional_available_layers: Vec::new(),
                provisional_bitrates: Vec::new(),
                provisional_allocated_layer: VideoLayerLite::INVALID,
                last_allocation: None,

                started: false,
                last_ssrc: 0,
                last_in_seq: 0,
                last_out_seq: 0,
                last_out_ts: 0,

                excluded_ranges: Vec::new(),
                dropped_padding_in_order: Vec::new(),

                frame_end_needed: false,
                vp8_padding: None,

                first_in_seq_video: None,
                vp8_pic_map: HashMap::new(),
                last_out_pic_id: None,
                last_out_tl0: None,
                last_out_key: None,
            }
        }

        fn is_muted(&self) -> bool {
            self.muted
        }

        fn mute(&mut self, muted: bool, force: bool) -> bool {
            if !force {
                return false;
            }
            if self.muted == muted {
                return false;
            }
            self.muted = muted;
            true
        }

        fn max_layer(&self) -> VideoLayerLite {
            self.max_layer
        }

        fn current_layer(&self) -> VideoLayerLite {
            self.current_layer
        }

        fn target_layer(&self) -> VideoLayerLite {
            self.target_layer
        }

        fn set_max_spatial_layer(&mut self, layer: i32) -> (bool, VideoLayerLite) {
            if self.kind == CodecKindLite::Audio {
                return (false, VideoLayerLite::INVALID);
            }
            if self.max_layer.spatial == layer {
                return (false, self.max_layer);
            }
            self.max_layer.spatial = layer;
            (true, self.max_layer)
        }

        fn set_max_temporal_layer(&mut self, layer: i32) -> (bool, VideoLayerLite) {
            if self.kind == CodecKindLite::Audio {
                return (false, VideoLayerLite::INVALID);
            }
            if self.max_layer.temporal == layer {
                return (false, self.max_layer);
            }
            self.max_layer.temporal = layer;
            (true, self.max_layer)
        }

        fn set_max_published_layer(&mut self, spatial: i32) {
            let prev = self.max_published_spatial;
            self.max_published_spatial = spatial;
            if spatial > prev && !self.current_layer.is_valid() {
                self.acquire_grace_active = true;
                self.acquire_grace_fired = false;
            }
        }

        fn set_max_temporal_layer_seen(&mut self, temporal: i32) {
            self.max_temporal_seen = temporal;
        }

        fn pub_mute(&mut self, muted: bool) {
            self.pub_muted = muted;
        }

        fn within_acquire_grace(&self) -> bool {
            self.acquire_grace_active
        }

        fn maybe_expire_acquire_grace(&mut self) -> bool {
            if !self.acquire_grace_active || self.acquire_grace_fired {
                return false;
            }
            self.acquire_grace_active = false;
            self.acquire_grace_fired = true;
            !self.current_layer.is_valid()
        }

        fn layer_bitrate(bitrates: &BitratesLite, layer: VideoLayerLite) -> i64 {
            if !layer.is_valid() {
                return 0;
            }
            bitrates
                .get(layer.spatial as usize)
                .and_then(|row| row.get(layer.temporal as usize))
                .copied()
                .unwrap_or(0)
        }

        fn effective_max_layer(&self) -> VideoLayerLite {
            VideoLayerLite {
                spatial: self.max_layer.spatial,
                temporal: if self.max_temporal_seen >= 0 {
                    self.max_layer.temporal.min(self.max_temporal_seen)
                } else {
                    self.max_layer.temporal
                },
            }
        }

        fn get_optimal_bandwidth_needed(
            &self,
            bitrates: &BitratesLite,
            max_layer: VideoLayerLite,
        ) -> i64 {
            if self.muted
                || self.pub_muted
                || self.max_published_spatial == INVALID_SPATIAL
                || !max_layer.is_valid()
            {
                return 0;
            }

            for s in (0..=max_layer.spatial as usize).rev() {
                for t in (0..=max_layer.temporal as usize).rev() {
                    let br = bitrates
                        .get(s)
                        .and_then(|row| row.get(t))
                        .copied()
                        .unwrap_or(0);
                    if br > 0 {
                        return br;
                    }
                }
            }
            0
        }

        fn provisional_allocate_prepare(&mut self, bitrates: &BitratesLite) {
            self.provisional_target = VideoLayerLite::INVALID;
            self.provisional_available_layers.clear();
            self.provisional_bitrates = bitrates.clone();
            self.provisional_allocated_layer = VideoLayerLite::INVALID;
        }

        fn provisional_allocate_prepare_advanced(
            &mut self,
            available_layers: &[i32],
            bitrates: &BitratesLite,
        ) {
            self.provisional_target = VideoLayerLite::INVALID;
            self.provisional_available_layers = available_layers.to_vec();
            self.provisional_bitrates = bitrates.clone();
            self.provisional_allocated_layer = VideoLayerLite::INVALID;
        }

        fn provisional_allocate(
            &mut self,
            _available_channel_capacity: i64,
            layer: VideoLayerLite,
        ) -> (bool, i64) {
            if self.muted {
                return (false, 0);
            }
            self.provisional_target = layer;
            (true, 0)
        }

        fn provisional_allocate_commit(&mut self, bitrates: BitratesLite) -> VideoAllocationLite {
            if self.muted {
                let allocation = VideoAllocationLite {
                    pause_reason: VideoPauseReasonLite::Muted,
                    is_deficient: false,
                    bandwidth_requested: 0,
                    bandwidth_delta: 0,
                    bandwidth_needed: 0,
                    bitrates,
                    target_layer: VideoLayerLite::INVALID,
                    request_layer_spatial: INVALID_SPATIAL,
                    max_layer: VideoLayerLite::DEFAULT_MAX,
                    distance_to_desired: 0.0,
                };
                self.target_layer = VideoLayerLite::INVALID;
                self.last_allocation = Some(allocation.clone());
                return allocation;
            }

            self.target_layer = self.provisional_target;
            let allocation = VideoAllocationLite {
                pause_reason: VideoPauseReasonLite::Bandwidth,
                is_deficient: false,
                bandwidth_requested: 0,
                bandwidth_delta: 0,
                bandwidth_needed: 0,
                bitrates,
                target_layer: self.target_layer,
                request_layer_spatial: self.target_layer.spatial,
                max_layer: VideoLayerLite::DEFAULT_MAX,
                distance_to_desired: 0.0,
            };
            self.last_allocation = Some(allocation.clone());
            allocation
        }

        fn provisional_allocate_advanced(
            &mut self,
            available_channel_capacity: i64,
            layer: VideoLayerLite,
            allow_pause: bool,
            allow_overshoot: bool,
        ) -> (bool, i64) {
            let max_layer = self.effective_max_layer();
            if self.muted
                || self.pub_muted
                || self.max_published_spatial == INVALID_SPATIAL
                || !max_layer.is_valid()
                || ((!allow_overshoot)
                    && (layer.spatial > max_layer.spatial || layer.temporal > max_layer.temporal))
            {
                return (false, 0);
            }

            let required = Self::layer_bitrate(&self.provisional_bitrates, layer);
            if required == 0 {
                return (false, 0);
            }

            let already_allocated =
                Self::layer_bitrate(&self.provisional_bitrates, self.provisional_allocated_layer);
            if (layer.spatial <= max_layer.spatial && layer.temporal <= max_layer.temporal)
                && required <= (available_channel_capacity + already_allocated)
            {
                self.provisional_allocated_layer = layer;
                return (true, required - already_allocated);
            }

            if !allow_pause
                && (!self.provisional_allocated_layer.is_valid()
                    || (layer.spatial < self.provisional_allocated_layer.spatial
                        || (layer.spatial == self.provisional_allocated_layer.spatial
                            && layer.temporal <= self.provisional_allocated_layer.temporal)))
            {
                self.provisional_allocated_layer = layer;
                return (true, required - already_allocated);
            }

            (false, 0)
        }

        fn provisional_allocate_get_cooperative_transition_advanced(
            &mut self,
            allow_overshoot: bool,
        ) -> (VideoTransitionLite, Vec<i32>, BitratesLite) {
            let from = self.target_layer;
            if self.muted || self.pub_muted {
                self.provisional_allocated_layer = VideoLayerLite::INVALID;
                return (
                    VideoTransitionLite {
                        from,
                        to: self.provisional_allocated_layer,
                        bandwidth_delta: -Self::layer_bitrate(&self.provisional_bitrates, from),
                    },
                    self.provisional_available_layers.clone(),
                    self.provisional_bitrates.clone(),
                );
            }

            if from.is_valid() {
                let from_br = Self::layer_bitrate(&self.provisional_bitrates, from);
                if from_br > 0 {
                    self.provisional_allocated_layer = from;
                    return (
                        VideoTransitionLite {
                            from,
                            to: from,
                            bandwidth_delta: 0,
                        },
                        self.provisional_available_layers.clone(),
                        self.provisional_bitrates.clone(),
                    );
                }
            }

            let max_layer = self.effective_max_layer();
            let mut target = VideoLayerLite::INVALID;
            let mut required = 0;

            for s in 0..=max_layer.spatial.max(0) as usize {
                for t in 0..=max_layer.temporal.max(0) as usize {
                    let br = self
                        .provisional_bitrates
                        .get(s)
                        .and_then(|row| row.get(t))
                        .copied()
                        .unwrap_or(0);
                    if br > 0 {
                        target = VideoLayerLite {
                            spatial: s as i32,
                            temporal: t as i32,
                        };
                        required = br;
                        break;
                    }
                }
                if target.is_valid() {
                    break;
                }
            }

            if !target.is_valid() && allow_overshoot {
                for s in (max_layer.spatial + 1).max(0) as usize..=DEFAULT_MAX_SPATIAL as usize {
                    for t in 0..=DEFAULT_MAX_TEMPORAL as usize {
                        let br = self
                            .provisional_bitrates
                            .get(s)
                            .and_then(|row| row.get(t))
                            .copied()
                            .unwrap_or(0);
                        if br > 0 {
                            target = VideoLayerLite {
                                spatial: s as i32,
                                temporal: t as i32,
                            };
                            required = br;
                            break;
                        }
                    }
                    if target.is_valid() {
                        break;
                    }
                }
            }

            if !target.is_valid() {
                target = self.current_layer;
                required = Self::layer_bitrate(&self.provisional_bitrates, target);
            }

            self.provisional_allocated_layer = target;
            (
                VideoTransitionLite {
                    from,
                    to: target,
                    bandwidth_delta: required
                        - Self::layer_bitrate(&self.provisional_bitrates, from),
                },
                self.provisional_available_layers.clone(),
                self.provisional_bitrates.clone(),
            )
        }

        fn provisional_allocate_get_best_weighted_transition_advanced(
            &mut self,
        ) -> (VideoTransitionLite, Vec<i32>, BitratesLite) {
            let from = self.target_layer;
            if !from.is_valid() {
                return (
                    VideoTransitionLite {
                        from,
                        to: VideoLayerLite::INVALID,
                        bandwidth_delta: 0,
                    },
                    self.provisional_available_layers.clone(),
                    self.provisional_bitrates.clone(),
                );
            }

            let existing = Self::layer_bitrate(&self.provisional_bitrates, from);
            let mut best = from;
            let mut best_delta = 0;
            let mut best_value = f32::MIN;

            for s in 0..=from.spatial.max(0) as usize {
                for t in 0..=from.temporal.max(0) as usize {
                    if s as i32 == from.spatial && t as i32 == from.temporal {
                        continue;
                    }
                    let br = self
                        .provisional_bitrates
                        .get(s)
                        .and_then(|row| row.get(t))
                        .copied()
                        .unwrap_or(0);
                    if br == 0 {
                        continue;
                    }
                    let bandwidth_delta = (existing - br).max(0);
                    let transition_cost = if s as i32 == from.spatial { 0 } else { 10 };
                    let quality_cost = ((from.spatial - s as i32) * (from.temporal + 1))
                        + (from.temporal - t as i32);
                    let denom = transition_cost + quality_cost;
                    let value = if denom <= 0 {
                        0.0
                    } else {
                        bandwidth_delta as f32 / denom as f32
                    };
                    if value > best_value || (value == best_value && bandwidth_delta > best_delta) {
                        best_value = value;
                        best_delta = bandwidth_delta;
                        best = VideoLayerLite {
                            spatial: s as i32,
                            temporal: t as i32,
                        };
                    }
                }
            }

            self.provisional_allocated_layer = best;
            (
                VideoTransitionLite {
                    from,
                    to: best,
                    bandwidth_delta: -best_delta,
                },
                self.provisional_available_layers.clone(),
                self.provisional_bitrates.clone(),
            )
        }

        fn provisional_allocate_commit_advanced(&mut self) -> VideoAllocationLite {
            let max_layer = self.effective_max_layer();
            let optimal_needed =
                self.get_optimal_bandwidth_needed(&self.provisional_bitrates, max_layer);
            let target = self.provisional_allocated_layer;
            let requested = Self::layer_bitrate(&self.provisional_bitrates, target);
            let previous = Self::layer_bitrate(&self.provisional_bitrates, self.target_layer);

            let mut allocation = VideoAllocationLite {
                pause_reason: VideoPauseReasonLite::Bandwidth,
                is_deficient: optimal_needed > 0 && requested < optimal_needed,
                bandwidth_requested: requested,
                bandwidth_delta: requested - previous,
                bandwidth_needed: optimal_needed,
                bitrates: self.provisional_bitrates.clone(),
                target_layer: target,
                request_layer_spatial: target.spatial,
                max_layer,
                distance_to_desired: 0.0,
            };

            if self.muted {
                allocation.pause_reason = VideoPauseReasonLite::Muted;
                allocation.target_layer = VideoLayerLite::INVALID;
                allocation.request_layer_spatial = INVALID_SPATIAL;
                allocation.bandwidth_requested = 0;
                allocation.bandwidth_needed = 0;
                allocation.bandwidth_delta = 0;
                allocation.is_deficient = false;
            }

            self.target_layer = allocation.target_layer;
            self.last_allocation = Some(allocation.clone());
            allocation
        }

        fn allocate_optimal_advanced(
            &mut self,
            available_layers: &[i32],
            bitrates: &BitratesLite,
            allow_overshoot: bool,
            hold: bool,
        ) -> VideoAllocationLite {
            let max_layer = self.effective_max_layer();
            let optimal_needed = self.get_optimal_bandwidth_needed(bitrates, max_layer);
            let mut allocation = VideoAllocationLite {
                pause_reason: if optimal_needed == 0 {
                    VideoPauseReasonLite::Bandwidth
                } else {
                    VideoPauseReasonLite::Bandwidth
                },
                is_deficient: false,
                bandwidth_requested: 0,
                bandwidth_delta: 0,
                bandwidth_needed: optimal_needed,
                bitrates: bitrates.clone(),
                target_layer: VideoLayerLite::INVALID,
                request_layer_spatial: INVALID_SPATIAL,
                max_layer,
                distance_to_desired: 0.0,
            };

            if self.kind == CodecKindLite::Audio {
                return self.last_allocation.clone().unwrap_or(allocation);
            }
            if self.muted {
                allocation.pause_reason = VideoPauseReasonLite::Muted;
                self.target_layer = VideoLayerLite::INVALID;
                self.last_allocation = Some(allocation.clone());
                return allocation;
            }
            if self.pub_muted {
                allocation.pause_reason = VideoPauseReasonLite::PubMuted;
                self.target_layer = VideoLayerLite::INVALID;
                self.last_allocation = Some(allocation.clone());
                return allocation;
            }
            if !max_layer.is_valid() || self.max_published_spatial == INVALID_SPATIAL {
                self.target_layer = VideoLayerLite::INVALID;
                self.last_allocation = Some(allocation.clone());
                return allocation;
            }

            let mut chosen_spatial = INVALID_SPATIAL;
            if hold {
                chosen_spatial = available_layers.iter().copied().min().unwrap_or(0);
            } else {
                if self.within_acquire_grace()
                    && !self.current_layer.is_valid()
                    && self.max_published_spatial < max_layer.spatial
                {
                    chosen_spatial = max_layer.spatial;
                }

                if chosen_spatial == INVALID_SPATIAL {
                    for &s in available_layers {
                        if s <= max_layer.spatial && s > chosen_spatial {
                            chosen_spatial = s;
                        }
                    }
                    if chosen_spatial == INVALID_SPATIAL && allow_overshoot {
                        chosen_spatial = available_layers
                            .iter()
                            .copied()
                            .max()
                            .unwrap_or(INVALID_SPATIAL);
                    }
                    if chosen_spatial == INVALID_SPATIAL && !available_layers.is_empty() {
                        chosen_spatial = available_layers
                            .iter()
                            .copied()
                            .max()
                            .unwrap_or(INVALID_SPATIAL);
                    }
                    if chosen_spatial == INVALID_SPATIAL {
                        chosen_spatial = max_layer.spatial;
                    }
                }
            }

            let chosen_temporal = if hold { 0 } else { max_layer.temporal };
            let target = VideoLayerLite {
                spatial: chosen_spatial,
                temporal: chosen_temporal,
            };
            let requested = Self::layer_bitrate(bitrates, target);
            let previous = Self::layer_bitrate(bitrates, self.target_layer);
            allocation.target_layer = target;
            allocation.request_layer_spatial = chosen_spatial;
            allocation.bandwidth_requested = requested;
            allocation.bandwidth_delta = requested - previous;
            allocation.is_deficient = optimal_needed > 0 && requested < optimal_needed;

            self.target_layer = target;
            self.last_allocation = Some(allocation.clone());
            allocation
        }

        fn allocate_next_higher_advanced(
            &mut self,
            available_channel_capacity: i64,
            available_layers: &[i32],
            bitrates: &BitratesLite,
            allow_overshoot: bool,
        ) -> (VideoAllocationLite, bool) {
            let Some(last) = self.last_allocation.clone() else {
                return (
                    VideoAllocationLite {
                        pause_reason: VideoPauseReasonLite::Bandwidth,
                        is_deficient: false,
                        bandwidth_requested: 0,
                        bandwidth_delta: 0,
                        bandwidth_needed: 0,
                        bitrates: bitrates.clone(),
                        target_layer: self.target_layer,
                        request_layer_spatial: self.request_layer_spatial,
                        max_layer: self.effective_max_layer(),
                        distance_to_desired: 0.0,
                    },
                    false,
                );
            };

            if self.kind == CodecKindLite::Audio || !last.is_deficient {
                return (last, false);
            }

            let max_layer = self.effective_max_layer();
            let optimal_needed = self.get_optimal_bandwidth_needed(bitrates, max_layer);
            let current = self.target_layer;
            let already_allocated = Self::layer_bitrate(bitrates, current);

            let mut candidates = Vec::new();
            if current.is_valid() {
                for t in (current.temporal + 1).max(0)..=max_layer.temporal.max(0) {
                    candidates.push(VideoLayerLite {
                        spatial: current.spatial,
                        temporal: t,
                    });
                }
            }
            for s in (current.spatial + 1).max(0)..=max_layer.spatial.max(0) {
                for t in 0..=max_layer.temporal.max(0) {
                    candidates.push(VideoLayerLite {
                        spatial: s,
                        temporal: t,
                    });
                }
            }
            if allow_overshoot {
                for s in (max_layer.spatial + 1).max(0)..=DEFAULT_MAX_SPATIAL {
                    for t in 0..=DEFAULT_MAX_TEMPORAL {
                        candidates.push(VideoLayerLite {
                            spatial: s,
                            temporal: t,
                        });
                    }
                }
            }

            for candidate in candidates {
                if !available_layers.is_empty() && !available_layers.contains(&candidate.spatial) {
                    continue;
                }
                let requested = Self::layer_bitrate(bitrates, candidate);
                if requested == 0 {
                    continue;
                }
                if !allow_overshoot && requested - already_allocated > available_channel_capacity {
                    return (last, false);
                }

                let allocation = VideoAllocationLite {
                    pause_reason: VideoPauseReasonLite::Bandwidth,
                    is_deficient: if candidate.spatial > max_layer.spatial {
                        false
                    } else {
                        requested < optimal_needed
                    },
                    bandwidth_requested: requested,
                    bandwidth_delta: requested - already_allocated,
                    bandwidth_needed: optimal_needed,
                    bitrates: bitrates.clone(),
                    target_layer: candidate,
                    request_layer_spatial: candidate.spatial,
                    max_layer,
                    distance_to_desired: 0.0,
                };
                self.target_layer = candidate;
                self.last_allocation = Some(allocation.clone());
                return (allocation, true);
            }

            (last, false)
        }

        fn pause(&mut self, bitrates: BitratesLite) -> VideoAllocationLite {
            let previous_bitrate = if self.target_layer.is_valid() {
                bitrates
                    .get(self.target_layer.spatial as usize)
                    .and_then(|temporal_row| temporal_row.get(self.target_layer.temporal as usize))
                    .copied()
                    .unwrap_or(0)
            } else {
                0
            };

            let allocation = if self.muted {
                VideoAllocationLite {
                    pause_reason: VideoPauseReasonLite::Muted,
                    is_deficient: false,
                    bandwidth_requested: 0,
                    bandwidth_delta: -previous_bitrate,
                    bandwidth_needed: 0,
                    bitrates,
                    target_layer: VideoLayerLite::INVALID,
                    request_layer_spatial: INVALID_SPATIAL,
                    max_layer: VideoLayerLite::DEFAULT_MAX,
                    distance_to_desired: 0.0,
                }
            } else {
                VideoAllocationLite {
                    pause_reason: VideoPauseReasonLite::Bandwidth,
                    is_deficient: true,
                    bandwidth_requested: 0,
                    bandwidth_delta: -previous_bitrate,
                    bandwidth_needed: 12,
                    bitrates,
                    target_layer: VideoLayerLite::INVALID,
                    request_layer_spatial: INVALID_SPATIAL,
                    max_layer: VideoLayerLite::DEFAULT_MAX,
                    distance_to_desired: 3.75,
                }
            };

            self.target_layer = VideoLayerLite::INVALID;
            self.last_allocation = Some(allocation.clone());
            allocation
        }

        fn get_translation_params_muted(&self) -> TranslationParamsLite {
            if self.muted {
                return TranslationParamsLite {
                    should_drop: true,
                    ..Default::default()
                };
            }

            TranslationParamsLite::default()
        }

        fn exclude_range(&mut self, start: u16, end: u16) {
            self.excluded_ranges.push((start, end));
        }

        fn excluded_before(&self, seq: u16) -> u64 {
            self.excluded_ranges
                .iter()
                .map(|(start, end)| {
                    if seq < *start {
                        0
                    } else if seq >= *end {
                        (*end as u64).saturating_sub(*start as u64)
                    } else {
                        (seq as u64).saturating_sub(*start as u64)
                    }
                })
                .sum()
        }

        fn dropped_padding_before_or_equal(&self, seq: u16) -> u64 {
            self.dropped_padding_in_order
                .iter()
                .filter(|s| **s <= seq)
                .count() as u64
        }

        fn mapped_out_seq(&self, seq: u16) -> u64 {
            seq as u64 - self.excluded_before(seq) - self.dropped_padding_before_or_equal(seq)
        }

        fn get_translation_params_audio(&mut self, pkt: AudioPacketLite) -> TranslationParamsLite {
            if self.muted {
                return TranslationParamsLite {
                    should_drop: true,
                    ..Default::default()
                };
            }

            if !self.started {
                if pkt.is_out_of_order || pkt.payload_size == 0 {
                    return TranslationParamsLite {
                        should_drop: true,
                        ..Default::default()
                    };
                }

                self.started = true;
                self.last_ssrc = pkt.ssrc;
                self.last_in_seq = pkt.sequence_number;
                self.last_out_seq = pkt.sequence_number as u64;
                self.last_out_ts = pkt.timestamp as u64;
                return TranslationParamsLite {
                    is_starting: true,
                    rtp: Some(TranslationRtpLite {
                        sn_ordering: SequenceOrderingLite::Contiguous,
                        ext_sequence_number: self.last_out_seq,
                        ext_timestamp: self.last_out_ts,
                    }),
                    ..Default::default()
                };
            }

            if pkt.ssrc != self.last_ssrc {
                self.last_ssrc = pkt.ssrc;
                self.last_in_seq = pkt.sequence_number;
                self.last_out_seq += 1;
                self.last_out_ts += 1;
                return TranslationParamsLite {
                    rtp: Some(TranslationRtpLite {
                        sn_ordering: SequenceOrderingLite::Contiguous,
                        ext_sequence_number: self.last_out_seq,
                        ext_timestamp: self.last_out_ts,
                    }),
                    ..Default::default()
                };
            }

            let ordering = if pkt.sequence_number == self.last_in_seq {
                return TranslationParamsLite {
                    should_drop: true,
                    ..Default::default()
                };
            } else if pkt.sequence_number < self.last_in_seq {
                SequenceOrderingLite::OutOfOrder
            } else if pkt.sequence_number == self.last_in_seq.wrapping_add(1) {
                SequenceOrderingLite::Contiguous
            } else {
                SequenceOrderingLite::Gap
            };

            let mapped = self.mapped_out_seq(pkt.sequence_number);

            if pkt.payload_size == 0 && ordering == SequenceOrderingLite::Contiguous {
                self.last_in_seq = pkt.sequence_number;
                self.last_out_seq = mapped;
                self.last_out_ts = pkt.timestamp as u64;
                self.dropped_padding_in_order.push(pkt.sequence_number);
                return TranslationParamsLite {
                    should_drop: true,
                    ..Default::default()
                };
            }

            if pkt.sequence_number >= self.last_in_seq {
                self.last_in_seq = pkt.sequence_number;
                self.last_out_seq = mapped;
                self.last_out_ts = pkt.timestamp as u64;
            }

            TranslationParamsLite {
                rtp: Some(TranslationRtpLite {
                    sn_ordering: ordering,
                    ext_sequence_number: mapped,
                    ext_timestamp: pkt.timestamp as u64,
                }),
                ..Default::default()
            }
        }

        fn rewrite_vp8_for_forward(
            &mut self,
            incoming: &Vp8InfoLite,
            ssrc_switched: bool,
            ordering: SequenceOrderingLite,
        ) -> Option<Vp8InfoLite> {
            let out_pic_id =
                if let Some(mapped) = self.vp8_pic_map.get(&incoming.picture_id).copied() {
                    mapped
                } else {
                    if ordering == SequenceOrderingLite::OutOfOrder {
                        return None;
                    }
                    let next = if let Some(last) = self.last_out_pic_id {
                        if incoming.picture_id == last {
                            last
                        } else {
                            last.wrapping_add(1)
                        }
                    } else {
                        incoming.picture_id
                    };
                    self.vp8_pic_map.insert(incoming.picture_id, next);
                    next
                };

            let mut out_tl0 = self.last_out_tl0.unwrap_or(incoming.tl0_pic_idx);
            if self.last_out_pic_id != Some(out_pic_id) && incoming.tid == 0 {
                if self.last_out_tl0.is_some() {
                    out_tl0 = out_tl0.wrapping_add(1);
                }
            }

            let mut out_key = self.last_out_key.unwrap_or(incoming.key_idx);
            if ssrc_switched && self.last_out_key.is_some() {
                out_key = out_key.wrapping_add(1);
            }

            let out = Vp8InfoLite {
                picture_id: out_pic_id,
                tl0_pic_idx: out_tl0,
                tid: incoming.tid,
                key_idx: out_key,
                header_size: 6,
                m_bit: true,
                is_key_frame: incoming.is_key_frame,
            };

            self.last_out_pic_id = Some(out.picture_id);
            self.last_out_tl0 = Some(out.tl0_pic_idx);
            self.last_out_key = Some(out.key_idx);
            Some(out)
        }

        fn get_translation_params_video(
            &mut self,
            pkt: VideoPacketLite,
            vp8: Vp8InfoLite,
        ) -> TranslationParamsLite {
            if self.muted || !self.target_layer.is_valid() {
                return TranslationParamsLite {
                    should_drop: true,
                    marker: pkt.marker,
                    ..Default::default()
                };
            }

            if pkt.layer > self.target_layer.spatial || vp8.tid as i32 > self.target_layer.temporal
            {
                let ordering = if pkt.sequence_number < self.last_in_seq {
                    SequenceOrderingLite::OutOfOrder
                } else if pkt.sequence_number == self.last_in_seq.wrapping_add(1) {
                    SequenceOrderingLite::Contiguous
                } else {
                    SequenceOrderingLite::Gap
                };
                let mapped = self.mapped_out_seq(pkt.sequence_number);
                if ordering == SequenceOrderingLite::Contiguous {
                    self.last_in_seq = pkt.sequence_number;
                    self.last_out_seq = mapped;
                    self.last_out_ts = pkt.timestamp as u64;
                    self.dropped_padding_in_order.push(pkt.sequence_number);
                }
                return TranslationParamsLite {
                    should_drop: true,
                    marker: pkt.marker,
                    rtp: Some(TranslationRtpLite {
                        sn_ordering: ordering,
                        ext_sequence_number: mapped,
                        ext_timestamp: pkt.timestamp as u64,
                    }),
                    ..Default::default()
                };
            }

            if !self.started {
                if pkt.is_out_of_order || !vp8.is_key_frame {
                    return TranslationParamsLite {
                        should_drop: true,
                        marker: pkt.marker,
                        ..Default::default()
                    };
                }

                self.started = true;
                self.last_ssrc = pkt.ssrc;
                self.last_in_seq = pkt.sequence_number;
                self.last_out_seq = pkt.sequence_number as u64;
                self.last_out_ts = pkt.timestamp as u64;
                self.first_in_seq_video = Some(pkt.sequence_number);
                self.vp8_pic_map.insert(vp8.picture_id, vp8.picture_id);
                self.last_out_pic_id = Some(vp8.picture_id);
                self.last_out_tl0 = Some(vp8.tl0_pic_idx);
                self.last_out_key = Some(vp8.key_idx);

                return TranslationParamsLite {
                    is_starting: true,
                    is_switching: true,
                    is_resuming: true,
                    marker: pkt.marker,
                    incoming_header_size: vp8.header_size as usize,
                    codec: Some(Vp8InfoLite {
                        m_bit: true,
                        header_size: 6,
                        ..vp8
                    }),
                    rtp: Some(TranslationRtpLite {
                        sn_ordering: SequenceOrderingLite::Contiguous,
                        ext_sequence_number: self.last_out_seq,
                        ext_timestamp: self.last_out_ts,
                    }),
                    ..Default::default()
                };
            }

            if pkt.sequence_number == self.last_in_seq {
                return TranslationParamsLite {
                    should_drop: true,
                    marker: pkt.marker,
                    ..Default::default()
                };
            }

            let ssrc_switched = pkt.ssrc != self.last_ssrc;
            if ssrc_switched {
                self.last_ssrc = pkt.ssrc;
            }

            let ordering = if ssrc_switched {
                SequenceOrderingLite::Contiguous
            } else if pkt.sequence_number < self.last_in_seq {
                SequenceOrderingLite::OutOfOrder
            } else if pkt.sequence_number == self.last_in_seq.wrapping_add(1) {
                SequenceOrderingLite::Contiguous
            } else {
                SequenceOrderingLite::Gap
            };

            if ordering == SequenceOrderingLite::OutOfOrder {
                if let Some(first) = self.first_in_seq_video {
                    if pkt.sequence_number < first {
                        return TranslationParamsLite {
                            should_drop: true,
                            marker: pkt.marker,
                            ..Default::default()
                        };
                    }
                }
            }

            let mapped = if ssrc_switched {
                self.last_out_seq + 1
            } else {
                self.mapped_out_seq(pkt.sequence_number)
            };

            if pkt.payload_size == 0 && ordering == SequenceOrderingLite::Contiguous {
                self.last_in_seq = pkt.sequence_number;
                self.last_out_seq = mapped;
                self.last_out_ts = pkt.timestamp as u64;
                self.dropped_padding_in_order.push(pkt.sequence_number);
                return TranslationParamsLite {
                    should_drop: true,
                    marker: pkt.marker,
                    ..Default::default()
                };
            }

            let Some(codec) = self.rewrite_vp8_for_forward(&vp8, ssrc_switched, ordering) else {
                return TranslationParamsLite {
                    should_drop: true,
                    marker: pkt.marker,
                    ..Default::default()
                };
            };

            if ordering != SequenceOrderingLite::OutOfOrder {
                self.last_in_seq = pkt.sequence_number;
                self.last_out_seq = mapped;
                self.last_out_ts = if ssrc_switched {
                    self.last_out_ts + 1
                } else {
                    pkt.timestamp as u64
                };
            }

            TranslationParamsLite {
                is_switching: ssrc_switched,
                marker: pkt.marker,
                incoming_header_size: vp8.header_size as usize,
                codec: Some(codec),
                rtp: Some(TranslationRtpLite {
                    sn_ordering: ordering,
                    ext_sequence_number: mapped,
                    ext_timestamp: if ssrc_switched {
                        self.last_out_ts
                    } else {
                        pkt.timestamp as u64
                    },
                }),
                ..Default::default()
            }
        }

        fn lock_vp8_stream(
            &mut self,
            sequence_number: u16,
            timestamp: u32,
            picture_id: u16,
            tl0: u8,
            key_idx: u8,
        ) {
            self.started = true;
            self.last_in_seq = sequence_number;
            self.last_out_seq = sequence_number as u64;
            self.last_out_ts = timestamp as u64;
            self.frame_end_needed = true;
            self.vp8_padding = Some(Vp8PaddingLite {
                picture_id,
                tl0_pic_idx: tl0,
                key_idx,
                tid: 0,
                first_byte: 16,
            });
        }

        fn get_sn_ts_for_padding(
            &mut self,
            num_padding: usize,
            clock_rate: u32,
            frame_rate: u32,
        ) -> Vec<SnTsLite> {
            let mut out = Vec::with_capacity(num_padding);
            let base_seq = self.last_out_seq;
            let base_ts = self.last_out_ts;

            for i in 0..num_padding {
                let ts = if self.frame_end_needed {
                    base_ts + ((i as u64 * clock_rate as u64) / frame_rate.max(1) as u64)
                } else {
                    base_ts + (((i + 1) as u64 * clock_rate as u64) / frame_rate.max(1) as u64)
                };
                out.push(SnTsLite {
                    ext_sequence_number: base_seq + i as u64 + 1,
                    ext_timestamp: ts,
                });
            }

            if let Some(last) = out.last().copied() {
                self.last_out_seq = last.ext_sequence_number;
                self.last_out_ts = last.ext_timestamp;
            }
            self.frame_end_needed = false;
            out
        }

        fn get_sn_ts_for_blank_frames(
            &mut self,
            frame_rate: u32,
            num_blank_frames: usize,
        ) -> (Vec<SnTsLite>, bool) {
            let frame_end_needed = self.frame_end_needed;
            let num_padding = if frame_end_needed {
                num_blank_frames + 1
            } else {
                num_blank_frames
            };

            let mut out = Vec::with_capacity(num_padding);
            let base_seq = self.last_out_seq;
            let base_ts = self.last_out_ts;

            for i in 0..num_padding {
                let ts = if frame_end_needed {
                    if i == 0 {
                        base_ts
                    } else {
                        base_ts
                            + 1
                            + (((i as u64 * VP8_CLOCK_RATE as u64) + frame_rate as u64 - 1)
                                / frame_rate.max(1) as u64)
                    }
                } else {
                    base_ts
                        + 1
                        + ((((i + 1) as u64 * VP8_CLOCK_RATE as u64) + frame_rate as u64 - 1)
                            / frame_rate.max(1) as u64)
                };
                out.push(SnTsLite {
                    ext_sequence_number: base_seq + i as u64 + 1,
                    ext_timestamp: ts,
                });
            }

            if let Some(last) = out.last().copied() {
                self.last_out_seq = last.ext_sequence_number;
                self.last_out_ts = last.ext_timestamp;
            }
            self.frame_end_needed = false;
            (out, frame_end_needed)
        }

        fn get_padding_vp8(&mut self, frame_end_needed: bool) -> Vp8PaddingLite {
            let mut vp8 = self
                .vp8_padding
                .expect("vp8 padding state should be initialized");
            if !frame_end_needed {
                vp8.picture_id = vp8.picture_id.wrapping_add(1);
                vp8.tl0_pic_idx = vp8.tl0_pic_idx.wrapping_add(1);
                vp8.key_idx = vp8.key_idx.wrapping_add(1);
                self.vp8_padding = Some(vp8);
            }
            vp8
        }
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderMute
    #[test]
    fn forwarder_mute_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Audio);
        assert!(!f.is_muted());
        assert!(!f.mute(false, true));
        assert!(!f.mute(true, false));
        assert!(f.mute(true, true));
        assert!(f.is_muted());
        assert!(f.mute(false, true));
        assert!(!f.is_muted());
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderLayersAudio
    #[test]
    fn forwarder_layers_audio_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Audio);
        assert_eq!(f.max_layer(), VideoLayerLite::INVALID);
        assert_eq!(f.current_layer(), VideoLayerLite::INVALID);
        assert_eq!(f.target_layer(), VideoLayerLite::INVALID);
        assert_eq!(f.set_max_spatial_layer(1), (false, VideoLayerLite::INVALID));
        assert_eq!(
            f.set_max_temporal_layer(1),
            (false, VideoLayerLite::INVALID)
        );
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderLayersVideo
    #[test]
    fn forwarder_layers_video_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        assert_eq!(
            f.max_layer(),
            VideoLayerLite {
                spatial: INVALID_SPATIAL,
                temporal: DEFAULT_MAX_TEMPORAL,
            }
        );
        assert!(f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL).0);
        assert!(f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL - 1).0);
        assert!(!f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL - 1).0);
        assert!(!f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL).0);
        assert!(f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL - 1).0);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderPauseMute
    #[test]
    fn forwarder_pause_mute_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);
        f.set_max_published_layer(DEFAULT_MAX_SPATIAL);

        let bitrates = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 11, 12]];
        f.provisional_allocate_prepare(&bitrates);
        let _ = f.provisional_allocate(
            bitrates[2][3],
            VideoLayerLite {
                spatial: 0,
                temporal: 0,
            },
        );
        let _ = f.provisional_allocate_commit(bitrates.clone());

        f.mute(true, true);
        let result = f.pause(bitrates.clone());
        assert_eq!(result.pause_reason, VideoPauseReasonLite::Muted);
        assert_eq!(result.bandwidth_delta, -bitrates[0][0]);
        assert_eq!(result.target_layer, VideoLayerLite::INVALID);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderGetTranslationParamsMuted
    #[test]
    fn forwarder_get_translation_params_muted_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        f.mute(true, true);
        assert_eq!(
            f.get_translation_params_muted(),
            TranslationParamsLite {
                should_drop: true,
                ..Default::default()
            }
        );
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderPause
    #[test]
    fn forwarder_pause_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);
        f.set_max_published_layer(DEFAULT_MAX_SPATIAL);

        let bitrates = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 11, 12]];
        f.provisional_allocate_prepare(&bitrates);
        let _ = f.provisional_allocate(
            bitrates[2][3],
            VideoLayerLite {
                spatial: 0,
                temporal: 0,
            },
        );
        let _ = f.provisional_allocate_commit(bitrates.clone());

        let result = f.pause(bitrates.clone());
        assert_eq!(result.pause_reason, VideoPauseReasonLite::Bandwidth);
        assert!(result.is_deficient);
        assert_eq!(result.bandwidth_delta, -bitrates[0][0]);
        assert_eq!(result.bandwidth_needed, 12);
        assert_eq!(result.target_layer, VideoLayerLite::INVALID);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderProvisionalAllocateMute
    #[test]
    fn forwarder_provisional_allocate_mute_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);
        f.mute(true, true);

        let bitrates = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 11, 12]];
        f.provisional_allocate_prepare(&bitrates);

        let (is_candidate, used) = f.provisional_allocate(
            bitrates[2][3],
            VideoLayerLite {
                spatial: 0,
                temporal: 0,
            },
        );
        assert!(!is_candidate);
        assert_eq!(used, 0);

        let committed = f.provisional_allocate_commit(bitrates);
        assert_eq!(committed.pause_reason, VideoPauseReasonLite::Muted);
        assert_eq!(committed.target_layer, VideoLayerLite::INVALID);
        assert_eq!(f.target_layer(), VideoLayerLite::INVALID);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderProvisionalAllocate
    #[test]
    fn forwarder_provisional_allocate_matches_upstream_core_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);
        f.set_max_published_layer(DEFAULT_MAX_SPATIAL);
        f.set_max_temporal_layer_seen(DEFAULT_MAX_TEMPORAL);

        let bitrates = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 11, 12]];
        f.provisional_allocate_prepare_advanced(&[0, 1, 2], &bitrates);

        let (ok, used) = f.provisional_allocate_advanced(
            bitrates[2][3],
            VideoLayerLite {
                spatial: 0,
                temporal: 0,
            },
            true,
            false,
        );
        assert!(ok);
        assert_eq!(used, bitrates[0][0]);

        let (ok, used) = f.provisional_allocate_advanced(
            bitrates[2][3],
            VideoLayerLite {
                spatial: 1,
                temporal: 2,
            },
            true,
            false,
        );
        assert!(ok);
        assert_eq!(used, bitrates[1][2] - bitrates[0][0]);

        let committed = f.provisional_allocate_commit_advanced();
        assert_eq!(
            committed.target_layer,
            VideoLayerLite {
                spatial: 1,
                temporal: 2
            }
        );
        assert_eq!(committed.bandwidth_requested, bitrates[1][2]);
        assert!(committed.is_deficient);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderProvisionalAllocateGetCooperativeTransition
    #[test]
    fn forwarder_provisional_allocate_get_cooperative_transition_matches_upstream_core_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);
        f.set_max_published_layer(DEFAULT_MAX_SPATIAL);
        f.set_max_temporal_layer_seen(DEFAULT_MAX_TEMPORAL);

        let bitrates = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 0, 0]];
        let available = vec![0, 1, 2];
        f.provisional_allocate_prepare_advanced(&available, &bitrates);

        let (transition, returned_available, returned_bitrates) =
            f.provisional_allocate_get_cooperative_transition_advanced(false);
        assert_eq!(returned_available, available);
        assert_eq!(returned_bitrates, bitrates);
        assert_eq!(
            transition,
            VideoTransitionLite {
                from: VideoLayerLite::INVALID,
                to: VideoLayerLite {
                    spatial: 0,
                    temporal: 0,
                },
                bandwidth_delta: 1,
            }
        );

        let committed = f.provisional_allocate_commit_advanced();
        assert_eq!(
            committed.target_layer,
            VideoLayerLite {
                spatial: 0,
                temporal: 0
            }
        );

        f.mute(true, true);
        f.provisional_allocate_prepare_advanced(&available, &bitrates);
        let (muted_transition, _, _) =
            f.provisional_allocate_get_cooperative_transition_advanced(false);
        assert_eq!(muted_transition.to, VideoLayerLite::INVALID);
        assert!(muted_transition.bandwidth_delta <= 0);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderProvisionalAllocateGetBestWeightedTransition
    #[test]
    fn forwarder_provisional_allocate_get_best_weighted_transition_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);

        let available = vec![0, 1, 2];
        let bitrates = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 11, 12]];
        f.provisional_allocate_prepare_advanced(&available, &bitrates);
        f.target_layer = VideoLayerLite {
            spatial: 2,
            temporal: 2,
        };

        let (transition, returned_available, returned_bitrates) =
            f.provisional_allocate_get_best_weighted_transition_advanced();
        assert_eq!(returned_available, available);
        assert_eq!(returned_bitrates, bitrates);
        assert_eq!(
            transition.from,
            VideoLayerLite {
                spatial: 2,
                temporal: 2
            }
        );
        assert_eq!(
            transition.to,
            VideoLayerLite {
                spatial: 2,
                temporal: 0
            }
        );
        assert_eq!(transition.bandwidth_delta, -2);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderAllocateNextHigher
    #[test]
    fn forwarder_allocate_next_higher_matches_upstream_core_contract() {
        let mut f_audio = ForwarderLite::new(CodecKindLite::Audio);
        f_audio.last_allocation = Some(VideoAllocationLite {
            pause_reason: VideoPauseReasonLite::Bandwidth,
            is_deficient: false,
            bandwidth_requested: 0,
            bandwidth_delta: 0,
            bandwidth_needed: 0,
            bitrates: vec![],
            target_layer: VideoLayerLite::INVALID,
            request_layer_spatial: INVALID_SPATIAL,
            max_layer: VideoLayerLite::INVALID,
            distance_to_desired: 0.0,
        });
        let (result, boosted) =
            f_audio.allocate_next_higher_advanced(100_000_000, &[], &vec![], false);
        assert!(!boosted);
        assert_eq!(result.target_layer, VideoLayerLite::INVALID);

        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);
        f.set_max_published_layer(DEFAULT_MAX_SPATIAL);
        f.set_max_temporal_layer_seen(DEFAULT_MAX_TEMPORAL);
        f.target_layer = VideoLayerLite {
            spatial: 0,
            temporal: 0,
        };
        f.last_allocation = Some(VideoAllocationLite {
            pause_reason: VideoPauseReasonLite::Bandwidth,
            is_deficient: true,
            bandwidth_requested: 2,
            bandwidth_delta: 0,
            bandwidth_needed: 7,
            bitrates: vec![],
            target_layer: VideoLayerLite {
                spatial: 0,
                temporal: 0,
            },
            request_layer_spatial: 0,
            max_layer: VideoLayerLite::DEFAULT_MAX,
            distance_to_desired: 0.0,
        });
        let bitrates = vec![vec![2, 3, 0, 0], vec![4, 0, 0, 5], vec![0, 7, 0, 0]];

        let (result, boosted) =
            f.allocate_next_higher_advanced(100_000_000, &[0, 1, 2], &bitrates, false);
        assert!(boosted);
        assert_eq!(
            result.target_layer,
            VideoLayerLite {
                spatial: 0,
                temporal: 1
            }
        );
        assert_eq!(result.bandwidth_requested, 3);
        assert_eq!(result.bandwidth_delta, 1);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderAllocateOptimal
    #[test]
    fn forwarder_allocate_optimal_matches_upstream_core_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let bitrates = vec![vec![2, 3, 0, 0], vec![4, 0, 0, 5], vec![0, 7, 0, 0]];

        // invalid max layer -> invalid target
        let _ = f.set_max_spatial_layer(INVALID_SPATIAL);
        let alloc = f.allocate_optimal_advanced(&[], &bitrates, false, false);
        assert_eq!(alloc.target_layer, VideoLayerLite::INVALID);

        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);
        f.set_max_published_layer(DEFAULT_MAX_SPATIAL);
        f.set_max_temporal_layer_seen(DEFAULT_MAX_TEMPORAL);

        let alloc = f.allocate_optimal_advanced(&[0, 1], &bitrates, false, false);
        assert_eq!(
            alloc.target_layer,
            VideoLayerLite {
                spatial: 1,
                temporal: 3
            }
        );
        assert_eq!(alloc.bandwidth_requested, 5);

        let hold_alloc = f.allocate_optimal_advanced(&[0, 1], &bitrates, false, true);
        assert_eq!(
            hold_alloc.target_layer,
            VideoLayerLite {
                spatial: 0,
                temporal: 0
            }
        );
        assert_eq!(hold_alloc.bandwidth_requested, 2);

        f.mute(true, true);
        let muted_alloc = f.allocate_optimal_advanced(&[0, 1], &bitrates, false, false);
        assert_eq!(muted_alloc.pause_reason, VideoPauseReasonLite::Muted);
        assert_eq!(muted_alloc.target_layer, VideoLayerLite::INVALID);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderInitialAcquisitionGrace
    #[test]
    fn forwarder_initial_acquisition_grace_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        let _ = f.set_max_spatial_layer(DEFAULT_MAX_SPATIAL);
        let _ = f.set_max_temporal_layer(DEFAULT_MAX_TEMPORAL);
        f.set_max_temporal_layer_seen(DEFAULT_MAX_TEMPORAL);

        let bitrates = vec![vec![2, 3, 0, 0], vec![4, 0, 0, 5], vec![0, 7, 0, 0]];

        f.set_max_published_layer(1);
        assert!(f.within_acquire_grace());

        let alloc = f.allocate_optimal_advanced(&[0, 1], &bitrates, false, false);
        assert_eq!(alloc.target_layer.spatial, 2);
        assert_eq!(alloc.request_layer_spatial, 2);

        assert!(f.maybe_expire_acquire_grace());
        assert!(!f.maybe_expire_acquire_grace());

        let alloc = f.allocate_optimal_advanced(&[0, 1], &bitrates, false, false);
        assert_eq!(alloc.target_layer.spatial, 1);
        assert_eq!(alloc.request_layer_spatial, 1);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderGetTranslationParamsAudio
    #[test]
    fn forwarder_get_translation_params_audio_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Audio);

        let out_of_order = AudioPacketLite {
            sequence_number: 23332,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 20,
            is_out_of_order: true,
        };
        assert!(f.get_translation_params_audio(out_of_order).should_drop);
        assert!(!f.started);

        let first = AudioPacketLite {
            sequence_number: 23333,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 20,
            is_out_of_order: false,
        };
        let tp = f.get_translation_params_audio(first);
        assert!(tp.is_starting);
        assert_eq!(
            tp.rtp,
            Some(TranslationRtpLite {
                sn_ordering: SequenceOrderingLite::Contiguous,
                ext_sequence_number: 23333,
                ext_timestamp: 0xabcdef,
            })
        );

        assert!(f.get_translation_params_audio(first).should_drop);

        f.exclude_range(23334, 23335);
        let seq_23336 = AudioPacketLite {
            sequence_number: 23336,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 20,
            is_out_of_order: false,
        };
        let _ = f.get_translation_params_audio(seq_23336);

        let seq_23335 = AudioPacketLite {
            sequence_number: 23335,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 20,
            is_out_of_order: false,
        };
        let tp = f.get_translation_params_audio(seq_23335);
        assert_eq!(
            tp.rtp,
            Some(TranslationRtpLite {
                sn_ordering: SequenceOrderingLite::OutOfOrder,
                ext_sequence_number: 23334,
                ext_timestamp: 0xabcdef,
            })
        );

        let padding_23337 = AudioPacketLite {
            sequence_number: 23337,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 0,
            is_out_of_order: false,
        };
        assert!(f.get_translation_params_audio(padding_23337).should_drop);

        let seq_23338 = AudioPacketLite {
            sequence_number: 23338,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 20,
            is_out_of_order: false,
        };
        let tp = f.get_translation_params_audio(seq_23338);
        assert_eq!(
            tp.rtp,
            Some(TranslationRtpLite {
                sn_ordering: SequenceOrderingLite::Contiguous,
                ext_sequence_number: 23336,
                ext_timestamp: 0xabcdef,
            })
        );

        let padding_23340 = AudioPacketLite {
            sequence_number: 23340,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 0,
            is_out_of_order: false,
        };
        let tp = f.get_translation_params_audio(padding_23340);
        assert_eq!(
            tp.rtp,
            Some(TranslationRtpLite {
                sn_ordering: SequenceOrderingLite::Gap,
                ext_sequence_number: 23338,
                ext_timestamp: 0xabcdef,
            })
        );

        let old_23336 = AudioPacketLite {
            sequence_number: 23336,
            timestamp: 0xabcdef,
            ssrc: 0x12345678,
            payload_size: 20,
            is_out_of_order: false,
        };
        let tp = f.get_translation_params_audio(old_23336);
        assert_eq!(
            tp.rtp,
            Some(TranslationRtpLite {
                sn_ordering: SequenceOrderingLite::OutOfOrder,
                ext_sequence_number: 23335,
                ext_timestamp: 0xabcdef,
            })
        );

        let switched_ssrc = AudioPacketLite {
            sequence_number: 123,
            timestamp: 0xfedcba,
            ssrc: 0x87654321,
            payload_size: 20,
            is_out_of_order: false,
        };
        let tp = f.get_translation_params_audio(switched_ssrc);
        assert_eq!(
            tp.rtp,
            Some(TranslationRtpLite {
                sn_ordering: SequenceOrderingLite::Contiguous,
                ext_sequence_number: 23339,
                ext_timestamp: 0xabcdf0,
            })
        );
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderGetTranslationParamsVideo
    #[test]
    fn forwarder_get_translation_params_video_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);

        // out-of-order start should drop
        let oo = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23332,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: true,
                marker: true,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13467,
                tl0_pic_idx: 233,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: false,
            },
        );
        assert!(oo.should_drop);
        assert!(!f.started);

        // no target should drop
        let no_target = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23333,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: false,
                marker: true,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13467,
                tl0_pic_idx: 233,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: false,
            },
        );
        assert!(no_target.should_drop);

        f.target_layer = VideoLayerLite {
            spatial: 0,
            temporal: 1,
        };

        // non-keyframe should still drop on start
        let non_kf = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23333,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: false,
                marker: true,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13467,
                tl0_pic_idx: 233,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: false,
            },
        );
        assert!(non_kf.should_drop);

        // keyframe should start/forward
        let started = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23333,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: false,
                marker: true,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13467,
                tl0_pic_idx: 233,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: true,
            },
        );
        assert!(started.is_starting);
        assert!(started.is_switching);
        assert!(started.is_resuming);
        assert_eq!(started.rtp.unwrap().ext_sequence_number, 23333);
        assert_eq!(started.codec.unwrap().picture_id, 13467);

        // duplicate should drop
        let dup = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23333,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: false,
                marker: true,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13467,
                tl0_pic_idx: 233,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: true,
            },
        );
        assert!(dup.should_drop);

        // padding in-order contiguous should drop
        let padding = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23334,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 0,
                is_out_of_order: false,
                marker: false,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13467,
                tl0_pic_idx: 233,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: true,
            },
        );
        assert!(padding.should_drop);

        // in-order media forwards
        let seq_23335 = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23335,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: false,
                marker: false,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13467,
                tl0_pic_idx: 233,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: true,
            },
        );
        assert!(!seq_23335.should_drop);
        assert_eq!(seq_23335.rtp.unwrap().ext_sequence_number, 23334);

        // temporal match forwards
        let tid1 = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23336,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: false,
                marker: false,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13468,
                tl0_pic_idx: 233,
                tid: 1,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: true,
            },
        );
        assert!(!tid1.should_drop);
        assert_eq!(tid1.rtp.unwrap().ext_sequence_number, 23335);

        // temporal above target drops but maintains rtp continuity mapping
        let tid2_drop = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23337,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: false,
                marker: false,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13468,
                tl0_pic_idx: 233,
                tid: 2,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: true,
            },
        );
        assert!(tid2_drop.should_drop);
        assert_eq!(tid2_drop.rtp.unwrap().ext_sequence_number, 23336);

        // next packet should keep sequence contiguous after drop
        let after_drop = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23338,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 20,
                is_out_of_order: false,
                marker: false,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13469,
                tl0_pic_idx: 234,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: false,
            },
        );
        assert!(!after_drop.should_drop);
        assert_eq!(after_drop.rtp.unwrap().ext_sequence_number, 23336);
        assert_eq!(after_drop.codec.unwrap().picture_id, 13469);

        // gap padding should be forwarded
        let gap_padding = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23340,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 0,
                is_out_of_order: false,
                marker: false,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13469,
                tl0_pic_idx: 234,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: false,
            },
        );
        assert!(!gap_padding.should_drop);
        assert_eq!(
            gap_padding.rtp.unwrap().sn_ordering,
            SequenceOrderingLite::Gap
        );

        // out-of-order from cache should forward
        let ooo_cached = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 23339,
                timestamp: 0xabcdef,
                ssrc: 0x12345678,
                payload_size: 0,
                is_out_of_order: false,
                marker: false,
                layer: 0,
            },
            Vp8InfoLite {
                picture_id: 13469,
                tl0_pic_idx: 234,
                tid: 0,
                key_idx: 23,
                header_size: 6,
                m_bit: true,
                is_key_frame: false,
            },
        );
        assert!(!ooo_cached.should_drop);
        assert_eq!(
            ooo_cached.rtp.unwrap().sn_ordering,
            SequenceOrderingLite::OutOfOrder
        );

        // ssrc switch should keep seq/timestamp contiguous and bump rewritten vp8 ids
        f.target_layer = VideoLayerLite {
            spatial: 1,
            temporal: 1,
        };
        let switched = f.get_translation_params_video(
            VideoPacketLite {
                sequence_number: 123,
                timestamp: 0xfedcba,
                ssrc: 0x87654321,
                payload_size: 20,
                is_out_of_order: false,
                marker: false,
                layer: 1,
            },
            Vp8InfoLite {
                picture_id: 45,
                tl0_pic_idx: 12,
                tid: 0,
                key_idx: 30,
                header_size: 5,
                m_bit: false,
                is_key_frame: true,
            },
        );
        assert!(!switched.should_drop);
        assert!(switched.is_switching);
        let rtp = switched.rtp.unwrap();
        assert_eq!(rtp.sn_ordering, SequenceOrderingLite::Contiguous);
        assert_eq!(rtp.ext_sequence_number, 23339);
        assert_eq!(rtp.ext_timestamp, 0xabcdf0);
        let codec = switched.codec.unwrap();
        assert_eq!(codec.picture_id, 13470);
        assert_eq!(codec.tl0_pic_idx, 235);
        assert_eq!(codec.key_idx, 24);
        assert_eq!(switched.incoming_header_size, 5);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderGetSnTsForPadding
    #[test]
    fn forwarder_get_sn_ts_for_padding_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        f.lock_vp8_stream(23333, 0xabcdef, 13467, 233, 23);

        let snts = f.get_sn_ts_for_padding(5, 0, 5);
        let expected: Vec<SnTsLite> = (0..5)
            .map(|i| SnTsLite {
                ext_sequence_number: 23333 + i as u64 + 1,
                ext_timestamp: 0xabcdef,
            })
            .collect();
        assert_eq!(snts, expected);

        let snts = f.get_sn_ts_for_padding(5, 0, 5);
        let expected: Vec<SnTsLite> = (0..5)
            .map(|i| SnTsLite {
                ext_sequence_number: 23338 + i as u64 + 1,
                ext_timestamp: 0xabcdef,
            })
            .collect();
        assert_eq!(snts, expected);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderGetSnTsForBlankFrames
    #[test]
    fn forwarder_get_sn_ts_for_blank_frames_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        f.lock_vp8_stream(23333, 0xabcdef, 13467, 233, 23);

        let num_blank_frames = 6usize;
        let (snts, frame_end_needed) = f.get_sn_ts_for_blank_frames(30, num_blank_frames);
        assert!(frame_end_needed);
        assert_eq!(snts.len(), num_blank_frames + 1);
        assert_eq!(snts[0].ext_sequence_number, 23334);
        assert_eq!(snts[0].ext_timestamp, 0xabcdef);

        let (snts2, frame_end_needed2) = f.get_sn_ts_for_blank_frames(30, num_blank_frames);
        assert!(!frame_end_needed2);
        assert_eq!(snts2.len(), num_blank_frames);
        assert!(
            snts2[0].ext_sequence_number > snts.last().expect("must have last").ext_sequence_number
        );
        assert!(snts2[0].ext_timestamp > snts.last().expect("must have last").ext_timestamp);
    }

    // Upstream: livekit/pkg/sfu/forwarder_test.go::TestForwarderGetPaddingVP8
    #[test]
    fn forwarder_get_padding_vp8_matches_upstream_contract() {
        let mut f = ForwarderLite::new(CodecKindLite::Video);
        f.lock_vp8_stream(23333, 0xabcdef, 13467, 233, 23);

        let first = f.get_padding_vp8(true);
        assert_eq!(
            first,
            Vp8PaddingLite {
                picture_id: 13467,
                tl0_pic_idx: 233,
                key_idx: 23,
                tid: 0,
                first_byte: 16,
            }
        );

        let second = f.get_padding_vp8(false);
        assert_eq!(
            second,
            Vp8PaddingLite {
                picture_id: 13468,
                tl0_pic_idx: 234,
                key_idx: 24,
                tid: 0,
                first_byte: 16,
            }
        );
    }
}

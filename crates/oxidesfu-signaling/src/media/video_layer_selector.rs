use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectorDecision {
    Missing = 0,
    Dropped = 1,
    Forwarded = 2,
    Unknown = 3,
}

type ExpectCallback = Box<dyn FnMut(u64, SelectorDecision)>;

#[derive(Default)]
struct SelectorDecisionCache {
    initialized: bool,
    base: u64,
    last: u64,
    masks: Vec<u64>,
    num_entries: u64,
    num_nack_entries: u64,
    on_expect_entity_changed: HashMap<u64, Vec<ExpectCallback>>,
}

impl SelectorDecisionCache {
    fn new(max_num_elements: u64, num_nack_entries: u64) -> Self {
        let num_elements = (max_num_elements * 2).div_ceil(64);
        Self {
            initialized: false,
            base: 0,
            last: 0,
            masks: vec![0u64; num_elements as usize],
            num_entries: num_elements * 32,
            num_nack_entries: num_nack_entries.max(1),
            on_expect_entity_changed: HashMap::new(),
        }
    }

    fn add_forwarded(&mut self, entity: u64) {
        self.add_entity(entity, SelectorDecision::Forwarded);
    }

    fn add_dropped(&mut self, entity: u64) {
        self.add_entity(entity, SelectorDecision::Dropped);
    }

    fn get_decision(&self, entity: u64) -> Result<SelectorDecision, String> {
        if !self.initialized || entity < self.base {
            return Ok(SelectorDecision::Missing);
        }

        if entity > self.last {
            return Ok(SelectorDecision::Unknown);
        }

        let offset = self.last.saturating_sub(entity);
        if offset >= self.num_entries {
            return Err(format!(
                "too old, oldest: {}, asking: {}",
                self.last.saturating_sub(self.num_entries).saturating_add(1),
                entity
            ));
        }

        Ok(self.get_entity(entity))
    }

    fn expect_decision(&mut self, entity: u64, callback: ExpectCallback) -> bool {
        if !self.initialized || entity < self.base {
            return false;
        }

        if entity < self.last {
            let offset = self.last - entity;
            if offset >= self.num_entries {
                return false;
            }
        }

        self.on_expect_entity_changed
            .entry(entity)
            .or_default()
            .push(callback);
        true
    }

    fn add_entity(&mut self, entity: u64, decision: SelectorDecision) {
        if !self.initialized {
            self.initialized = true;
            self.base = entity;
            self.last = entity;
            self.set_entity(entity, decision);
            return;
        }

        if entity <= self.base {
            return;
        }

        if entity <= self.last {
            self.set_entity(entity, decision);
            return;
        }

        for e in (self.last + 1)..entity {
            self.set_entity(e, SelectorDecision::Unknown);
        }

        let missing_start = if self.last > self.num_nack_entries + self.base {
            self.last - self.num_nack_entries
        } else {
            self.base
        };
        let missing_end = if entity > self.num_nack_entries + self.base {
            entity - self.num_nack_entries
        } else {
            self.base
        };

        if missing_end > missing_start {
            for e in missing_start..missing_end {
                self.set_entity_if_unknown(e, SelectorDecision::Missing);
            }
        }

        self.set_entity(entity, decision);
        self.last = entity;

        let expired_entities: Vec<u64> = self
            .on_expect_entity_changed
            .keys()
            .copied()
            .filter(|e| e.saturating_add(self.num_entries) < self.last)
            .collect();

        for entity in expired_entities {
            if let Some(mut callbacks) = self.on_expect_entity_changed.remove(&entity) {
                for callback in &mut callbacks {
                    callback(entity, SelectorDecision::Missing);
                }
            }
        }
    }

    fn set_entity_if_unknown(&mut self, entity: u64, decision: SelectorDecision) {
        if self.get_entity(entity) == SelectorDecision::Unknown {
            self.set_entity(entity, decision);
        }
    }

    fn set_entity(&mut self, entity: u64, decision: SelectorDecision) {
        let (index, bitpos) = self.get_pos(entity);
        self.masks[index] &= !(0x3u64 << bitpos);
        self.masks[index] |= ((decision as u64) & 0x3) << bitpos;

        if decision != SelectorDecision::Unknown {
            if let Some(mut callbacks) = self.on_expect_entity_changed.remove(&entity) {
                for callback in &mut callbacks {
                    callback(entity, decision);
                }
            }
        }
    }

    fn get_entity(&self, entity: u64) -> SelectorDecision {
        let (index, bitpos) = self.get_pos(entity);
        match (self.masks[index] >> bitpos) & 0x3 {
            0 => SelectorDecision::Missing,
            1 => SelectorDecision::Dropped,
            2 => SelectorDecision::Forwarded,
            _ => SelectorDecision::Unknown,
        }
    }

    fn get_pos(&self, entity: u64) -> (usize, usize) {
        let offset = (entity - self.base) % self.num_entries;
        (((offset >> 5) as usize), ((offset & 0x1f) as usize) * 2)
    }
}

#[derive(Debug, Clone)]
struct FrameDependencyTemplateLite {
    decode_target_indications: Vec<DecodeTargetIndicationLite>,
    chain_diffs: Vec<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeTargetIndicationLite {
    NotPresent,
    Required,
    Switch,
}

#[derive(Debug, Default)]
struct FrameChainState {
    broken: bool,
    active: bool,
    updating_active: bool,
    expect_frames: Vec<u64>,
}

#[derive(Clone)]
struct FrameChain {
    decisions: Rc<RefCell<SelectorDecisionCache>>,
    chain_idx: usize,
    state: Rc<RefCell<FrameChainState>>,
}

impl FrameChain {
    fn new(decisions: Rc<RefCell<SelectorDecisionCache>>, chain_idx: usize) -> Self {
        Self {
            decisions,
            chain_idx,
            state: Rc::new(RefCell::new(FrameChainState {
                broken: true,
                ..Default::default()
            })),
        }
    }

    fn on_frame(&self, ext_frame_num: u64, fd: &FrameDependencyTemplateLite) -> bool {
        if !self.state.borrow().active {
            return false;
        }

        if fd.chain_diffs.len() <= self.chain_idx {
            return self.state.borrow().broken;
        }

        if fd.chain_diffs[self.chain_idx] == 0 {
            let mut state = self.state.borrow_mut();
            if state.broken {
                state.broken = false;
            }
            state.expect_frames.clear();
            return true;
        }

        if self.state.borrow().broken {
            return false;
        }

        let prev_frame_in_chain =
            ext_frame_num.saturating_sub(fd.chain_diffs[self.chain_idx] as u64);
        let decision = self
            .decisions
            .borrow()
            .get_decision(prev_frame_in_chain)
            .unwrap_or(SelectorDecision::Missing);

        let mut intact = false;
        match decision {
            SelectorDecision::Forwarded => intact = true,
            SelectorDecision::Unknown => {
                let state_ref = self.state.clone();
                let accepted = self.decisions.borrow_mut().expect_decision(
                    prev_frame_in_chain,
                    Box::new(move |frame_num, decision| {
                        let mut state = state_ref.borrow_mut();
                        if state.broken {
                            return;
                        }

                        if let Some(pos) = state.expect_frames.iter().position(|f| *f == frame_num)
                        {
                            if decision != SelectorDecision::Forwarded {
                                state.broken = true;
                            }
                            state.expect_frames.swap_remove(pos);
                        }
                    }),
                );
                if accepted {
                    intact = true;
                    self.state
                        .borrow_mut()
                        .expect_frames
                        .push(prev_frame_in_chain);
                }
            }
            SelectorDecision::Dropped | SelectorDecision::Missing => {}
        }

        if !intact {
            self.state.borrow_mut().broken = true;
        }

        intact
    }

    fn broken(&self) -> bool {
        self.state.borrow().broken
    }

    fn begin_update_active(&self) {
        self.state.borrow_mut().updating_active = false;
    }

    fn update_active(&self, active: bool) {
        let mut state = self.state.borrow_mut();
        state.updating_active = state.updating_active || active;
    }

    fn end_update_active(&self) {
        let mut state = self.state.borrow_mut();
        let active = state.updating_active;
        state.updating_active = false;

        if active == state.active {
            return;
        }

        if !state.active {
            state.broken = true;
        }

        state.active = active;
    }
}

#[derive(Debug, Clone, Copy)]
struct DependencyDescriptorDecodeTargetLite {
    target: usize,
    spatial: i32,
    temporal: i32,
}

#[derive(Clone)]
struct DecodeTarget {
    target: DependencyDescriptorDecodeTargetLite,
    chain: Option<FrameChain>,
    active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrameDetectionResult {
    target_valid: bool,
    dti: DecodeTargetIndicationLite,
}

impl DecodeTarget {
    fn new(target: DependencyDescriptorDecodeTargetLite, chain: Option<FrameChain>) -> Self {
        Self {
            target,
            chain,
            active: false,
        }
    }

    fn valid(&self) -> bool {
        self.chain.as_ref().map(|c| !c.broken()).unwrap_or(true)
    }

    fn active(&self) -> bool {
        self.active
    }

    fn update_active(&mut self, active_bitmask: u32) {
        self.active = (active_bitmask & (1 << self.target.target)) != 0;
        if let Some(chain) = &self.chain {
            chain.update_active(self.active);
        }
    }

    fn on_frame(
        &self,
        _ext_frame_num: u64,
        fd: &FrameDependencyTemplateLite,
    ) -> Result<FrameDetectionResult, String> {
        if fd.decode_target_indications.len() <= self.target.target {
            return Err(format!(
                "mismatch target {} and len(DecodeTargetIndications) {}",
                self.target.target,
                fd.decode_target_indications.len()
            ));
        }

        Ok(FrameDetectionResult {
            target_valid: self.valid(),
            dti: fd.decode_target_indications[self.target.target],
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VideoLayerLite {
    spatial: i32,
    temporal: i32,
}

impl VideoLayerLite {
    const INVALID: Self = Self {
        spatial: -1,
        temporal: -1,
    };

    fn is_valid(self) -> bool {
        self.spatial >= 0 && self.temporal >= 0
    }
}

#[derive(Debug, Clone, Copy)]
struct SimulcastFrameLite {
    is_key_frame: bool,
    video_layer: VideoLayerLite,
}

#[derive(Debug, Default, Clone, Copy)]
struct VideoLayerSelectorResultLite {
    is_selected: bool,
    is_relevant: bool,
    is_switching: bool,
    is_resuming: bool,
}

#[derive(Debug, Clone)]
struct SimulcastSelectorLite {
    current_layer: VideoLayerLite,
    target_layer: VideoLayerLite,
    max_layer: VideoLayerLite,
    max_seen_layer: VideoLayerLite,
    request_spatial: i32,
    enable_start_at_desired_quality: bool,
}

impl SimulcastSelectorLite {
    fn new() -> Self {
        Self {
            current_layer: VideoLayerLite::INVALID,
            target_layer: VideoLayerLite::INVALID,
            max_layer: VideoLayerLite::INVALID,
            max_seen_layer: VideoLayerLite::INVALID,
            request_spatial: -1,
            enable_start_at_desired_quality: false,
        }
    }

    fn set_enable_start_at_desired_quality(&mut self, enabled: bool) {
        self.enable_start_at_desired_quality = enabled;
    }

    fn set_max(&mut self, layer: VideoLayerLite) {
        self.max_layer = layer;
    }

    fn set_max_seen(&mut self, layer: VideoLayerLite) {
        self.max_seen_layer = layer;
    }

    fn set_target(&mut self, layer: VideoLayerLite) {
        self.target_layer = layer;
    }

    fn set_request_spatial(&mut self, spatial: i32) {
        self.request_spatial = spatial;
    }

    fn set_current(&mut self, layer: VideoLayerLite) {
        self.current_layer = layer;
    }

    fn get_current(&self) -> VideoLayerLite {
        self.current_layer
    }

    fn select(&mut self, frame: SimulcastFrameLite, layer: i32) -> VideoLayerSelectorResultLite {
        let mut result = VideoLayerSelectorResultLite::default();

        if self.current_layer.spatial != self.target_layer.spatial {
            let is_active = self.current_layer.is_valid();
            let mut found = false;

            if frame.is_key_frame {
                if self.enable_start_at_desired_quality && !is_active {
                    if layer == self.target_layer.spatial {
                        found = true;
                    }
                } else {
                    if layer > self.current_layer.spatial && layer <= self.target_layer.spatial {
                        found = true;
                    }
                    if layer < self.current_layer.spatial && layer >= self.target_layer.spatial {
                        found = true;
                    }
                }
            }

            if found {
                self.current_layer = VideoLayerLite {
                    spatial: layer,
                    temporal: frame.video_layer.temporal,
                };
                if self.current_layer.spatial >= self.max_layer.spatial
                    || self.current_layer.spatial == self.max_seen_layer.spatial
                {
                    self.target_layer.spatial = self.current_layer.spatial;
                }
                result.is_switching = true;
                result.is_resuming = !is_active;
            }
        }

        result.is_selected = layer == self.current_layer.spatial;
        result.is_relevant = false;
        result
    }
}

#[derive(Debug, Default)]
struct WrapAroundU16ToU64 {
    initialized: bool,
    last: u16,
    cycles: u64,
}

impl WrapAroundU16ToU64 {
    fn update(&mut self, value: u16) -> u64 {
        if !self.initialized {
            self.initialized = true;
            self.last = value;
            return value as u64;
        }

        let diff = value.wrapping_sub(self.last);
        // forward wrap
        if value < self.last && diff < 0x8000 {
            self.cycles = self.cycles.saturating_add(1 << 16);
        }

        self.last = value;
        self.cycles + value as u64
    }
}

#[derive(Debug, Default)]
struct FrameNumberWrapperLite {
    offset: u64,
    last: u64,
    inited: bool,
}

impl FrameNumberWrapperLite {
    fn update_and_get(&mut self, new: u64, update_offset: bool) -> u64 {
        if !self.inited {
            self.last = new;
            self.inited = true;
            return new;
        }

        if new <= self.last {
            return new + self.offset;
        }

        if update_offset {
            let new16 = (new + self.offset) as u16;
            let last16 = (self.last + self.offset) as u16;
            let diff = new16.wrapping_sub(last16);
            if diff > 0x8000 || (diff == 0x8000 && new16 < last16) {
                self.offset = self.offset.saturating_add((65535 - diff as u64) + 6000);
            }
        }

        self.last = new;
        new + self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_order(a: u16, b: u16) -> bool {
        let diff = a.wrapping_sub(b);
        diff < 0x8000 || (diff == 0x8000 && a > b)
    }

    fn next_u32(seed: &mut u32) -> u32 {
        *seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        *seed
    }

    fn get_frame(base: u16, inorder: bool, seed: &mut u32) -> u16 {
        if inorder {
            return base.wrapping_add((next_u32(seed) & 0x7fff) as u16);
        }

        loop {
            let ret = base
                .wrapping_add((next_u32(seed) & 0x7fff) as u16)
                .wrapping_add(0x8000);
            if !in_order(ret, base) {
                return ret;
            }
        }
    }

    // Upstream: livekit/pkg/sfu/videolayerselector/dependencydescriptor_test.go::TestDecodeTarget
    #[test]
    fn decode_target_matches_upstream_contract() {
        let target = DependencyDescriptorDecodeTargetLite {
            target: 1,
            spatial: 1,
            temporal: 2,
        };

        // no chain
        let dt = DecodeTarget::new(target, None);
        assert!(dt.valid());
        assert!(
            dt.on_frame(
                1,
                &FrameDependencyTemplateLite {
                    decode_target_indications: vec![],
                    chain_diffs: vec![],
                }
            )
            .is_err()
        );

        let ret = dt
            .on_frame(
                1,
                &FrameDependencyTemplateLite {
                    decode_target_indications: vec![
                        DecodeTargetIndicationLite::NotPresent,
                        DecodeTargetIndicationLite::Required,
                    ],
                    chain_diffs: vec![],
                },
            )
            .expect("decode target should parse indications");
        assert!(ret.target_valid);
        assert_eq!(ret.dti, DecodeTargetIndicationLite::Required);

        // with chain
        let decisions = Rc::new(RefCell::new(SelectorDecisionCache::new(256, 80)));
        let chain = FrameChain::new(decisions.clone(), 1);
        let mut dt = DecodeTarget::new(target, Some(chain.clone()));
        chain.begin_update_active();
        dt.update_active(1 << dt.target.target);
        chain.end_update_active();

        assert!(dt.active());
        assert!(!dt.valid());

        let frame = FrameDependencyTemplateLite {
            decode_target_indications: vec![
                DecodeTargetIndicationLite::NotPresent,
                DecodeTargetIndicationLite::Required,
            ],
            chain_diffs: vec![0, 0],
        };

        assert!(chain.on_frame(1, &frame));
        assert!(dt.valid());

        let ret = dt
            .on_frame(1, &frame)
            .expect("decode target with chain should return result");
        assert!(ret.target_valid);
        assert_eq!(ret.dti, DecodeTargetIndicationLite::Required);
    }

    // Upstream: livekit/pkg/sfu/videolayerselector/dependencydescriptor_test.go::TestFrameChain
    #[test]
    fn frame_chain_matches_upstream_contract() {
        let decisions = Rc::new(RefCell::new(SelectorDecisionCache::new(256, 3)));
        let chain = FrameChain::new(decisions.clone(), 0);
        assert!(chain.broken());

        let frame_no_diff = FrameDependencyTemplateLite {
            decode_target_indications: vec![],
            chain_diffs: vec![0],
        };

        assert!(!chain.on_frame(1, &frame_no_diff));

        chain.begin_update_active();
        chain.update_active(true);
        chain.end_update_active();

        assert!(chain.on_frame(1, &frame_no_diff));
        decisions.borrow_mut().add_forwarded(1);

        let frame_diff_1 = FrameDependencyTemplateLite {
            decode_target_indications: vec![],
            chain_diffs: vec![1],
        };

        assert!(chain.on_frame(2, &frame_diff_1));
        decisions.borrow_mut().add_forwarded(2);

        assert!(chain.on_frame(5, &frame_diff_1));
        decisions.borrow_mut().add_forwarded(5);

        assert!(chain.on_frame(4, &frame_diff_1));
        decisions.borrow_mut().add_forwarded(4);

        decisions.borrow_mut().add_forwarded(7);
        assert!(chain.broken());

        assert!(chain.on_frame(1000, &frame_no_diff));
        assert!(!chain.broken());
        decisions.borrow_mut().add_forwarded(1000);

        assert!(chain.on_frame(1002, &frame_diff_1));
        decisions.borrow_mut().add_dropped(1001);
        assert!(chain.broken());

        assert!(chain.on_frame(2000, &frame_no_diff));
        decisions.borrow_mut().add_forwarded(2000);
        decisions.borrow_mut().add_dropped(2001);
        assert!(!chain.on_frame(2002, &frame_diff_1));
        assert!(chain.broken());
    }

    // Upstream: livekit/pkg/sfu/videolayerselector/framenumberwrapper_test.go::TestFrameNumberWrapper
    #[test]
    fn frame_number_wrapper_matches_upstream_contract() {
        let mut wrapper = FrameNumberWrapperLite::default();
        let mut wrap_around = WrapAroundU16ToU64::default();
        let mut seed = 0x1a2b3c4du32;

        let mut first_f: u16 = 1000;

        let mut test_frame_order = |frame: u16,
                                    is_key_frame: bool,
                                    frame2: u16,
                                    is_key_frame2: bool,
                                    expect_in_order: bool| {
            let frame_unwrap = wrap_around.update(frame);
            let wrapped_frame = wrapper.update_and_get(frame_unwrap, is_key_frame) as u16;

            let _ = wrap_around.update(frame.wrapping_add(frame2.wrapping_sub(frame) / 2));

            let frame2_unwrap = wrap_around.update(frame2);
            let wrapped_frame2 = wrapper.update_and_get(frame2_unwrap, is_key_frame2) as u16;

            assert_eq!(
                expect_in_order,
                in_order(wrapped_frame2, wrapped_frame),
                "frame {}, frame2 {}, wrappedFrame {}, wrappedFrame2 {}",
                frame,
                frame2,
                wrapped_frame,
                wrapped_frame2
            );

            if !is_key_frame2 {
                assert_eq!(
                    frame2.wrapping_sub(frame),
                    wrapped_frame2.wrapping_sub(wrapped_frame)
                );
            }
        };

        let mut second_f = get_frame(first_f, true, &mut seed);
        test_frame_order(first_f, true, second_f, false, true);

        for _ in 0..100 {
            first_f = second_f;
            second_f = get_frame(first_f, true, &mut seed);
            test_frame_order(first_f, false, second_f, false, true);

            first_f = second_f;
            second_f = get_frame(first_f, false, &mut seed);
            if second_f.wrapping_sub(first_f) == 0x8000 {
                second_f = second_f.wrapping_add(1);
            }
            test_frame_order(first_f, false, second_f, false, false);

            first_f = second_f;
            second_f = get_frame(first_f, true, &mut seed);
            test_frame_order(first_f, false, second_f, true, true);

            first_f = second_f;
            second_f = get_frame(first_f, true, &mut seed);
            test_frame_order(first_f, false, second_f, false, true);

            first_f = second_f;
            second_f = get_frame(first_f, false, &mut seed);
            test_frame_order(first_f, false, second_f, true, true);
        }
    }

    fn key_frame_on_layer(spatial: i32, temporal: i32) -> SimulcastFrameLite {
        SimulcastFrameLite {
            is_key_frame: true,
            video_layer: VideoLayerLite { spatial, temporal },
        }
    }

    // Upstream: livekit/pkg/sfu/videolayerselector/simulcast_test.go::TestSimulcastSelectAcquiresTargetLayerDirectly
    #[test]
    fn simulcast_select_acquires_target_layer_directly_matches_upstream_contract() {
        let mut selector = SimulcastSelectorLite::new();
        selector.set_enable_start_at_desired_quality(true);
        selector.set_max(VideoLayerLite {
            spatial: 2,
            temporal: 2,
        });
        selector.set_max_seen(VideoLayerLite {
            spatial: 2,
            temporal: 2,
        });
        selector.set_target(VideoLayerLite {
            spatial: 2,
            temporal: 2,
        });
        selector.set_request_spatial(2);
        selector.set_current(VideoLayerLite::INVALID);

        assert!(!selector.select(key_frame_on_layer(0, 2), 0).is_selected);
        assert!(!selector.get_current().is_valid());

        assert!(!selector.select(key_frame_on_layer(1, 2), 1).is_selected);
        assert!(!selector.get_current().is_valid());

        assert!(selector.select(key_frame_on_layer(2, 2), 2).is_selected);
        assert_eq!(selector.get_current().spatial, 2);
    }

    // Upstream: livekit/pkg/sfu/videolayerselector/simulcast_test.go::TestSimulcastSelectAcquiresLoweredTarget
    #[test]
    fn simulcast_select_acquires_lowered_target_matches_upstream_contract() {
        let mut selector = SimulcastSelectorLite::new();
        selector.set_enable_start_at_desired_quality(true);
        selector.set_max(VideoLayerLite {
            spatial: 2,
            temporal: 2,
        });
        selector.set_max_seen(VideoLayerLite {
            spatial: 1,
            temporal: 2,
        });
        selector.set_target(VideoLayerLite {
            spatial: 1,
            temporal: 2,
        });
        selector.set_request_spatial(1);
        selector.set_current(VideoLayerLite::INVALID);

        assert!(!selector.select(key_frame_on_layer(0, 2), 0).is_selected);
        assert!(!selector.get_current().is_valid());

        assert!(selector.select(key_frame_on_layer(1, 2), 1).is_selected);
        assert_eq!(selector.get_current().spatial, 1);
    }

    #[derive(Debug, Clone)]
    struct FrameDependencyTemplateDdLite {
        spatial_id: i32,
        temporal_id: i32,
        decode_target_indications: Vec<DecodeTargetIndicationLite>,
        chain_diffs: Vec<i32>,
        frame_diffs: Vec<u64>,
    }

    #[derive(Debug, Clone)]
    struct DdFrameLite {
        is_key_frame: bool,
        ext_frame_num: u64,
        ext_key_frame_num: u64,
        structure_updated: bool,
        active_decode_targets_updated: bool,
        active_decode_targets_bitmask: u32,
        decode_targets: Vec<DependencyDescriptorDecodeTargetLite>,
        decode_target_protected_by_chain: Vec<usize>,
        num_chains: usize,
        fd: FrameDependencyTemplateDdLite,
    }

    struct DependencyDescriptorSelectorLite {
        decisions: Rc<RefCell<SelectorDecisionCache>>,
        key_frame_valid: bool,
        ext_key_frame_num: u64,
        chains: Vec<FrameChain>,
        decode_targets: Vec<DecodeTarget>,
        target_layer: VideoLayerLite,
        current_layer: VideoLayerLite,
        request_spatial: i32,
    }

    impl DependencyDescriptorSelectorLite {
        fn new() -> Self {
            Self {
                decisions: Rc::new(RefCell::new(SelectorDecisionCache::new(256, 80))),
                key_frame_valid: false,
                ext_key_frame_num: 0,
                chains: Vec::new(),
                decode_targets: Vec::new(),
                target_layer: VideoLayerLite::INVALID,
                current_layer: VideoLayerLite::INVALID,
                request_spatial: -1,
            }
        }

        fn set_target(&mut self, layer: VideoLayerLite) {
            self.target_layer = layer;
        }

        fn get_target(&self) -> VideoLayerLite {
            self.target_layer
        }

        fn set_request_spatial(&mut self, spatial: i32) {
            self.request_spatial = spatial;
        }

        fn get_current(&self) -> VideoLayerLite {
            self.current_layer
        }

        fn invalidate_key_frame(&mut self) {
            self.key_frame_valid = false;
            self.chains.clear();
            self.decode_targets.clear();
        }

        fn check_sync(&self) -> (bool, i32) {
            let layer = self.request_spatial;
            if !self.current_layer.is_valid() || !self.key_frame_valid {
                return (false, layer);
            }

            for dt in &self.decode_targets {
                if dt.active() && dt.target.spatial == layer && dt.valid() {
                    return (true, layer);
                }
            }

            (false, layer)
        }

        fn update_dependency_structure(&mut self, frame: &DdFrameLite) {
            self.ext_key_frame_num = frame.ext_frame_num;
            self.key_frame_valid = true;

            self.chains.clear();
            for chain_idx in 0..frame.num_chains {
                self.chains
                    .push(FrameChain::new(self.decisions.clone(), chain_idx));
            }

            let mut new_targets = Vec::with_capacity(frame.decode_targets.len());
            for dt in &frame.decode_targets {
                let chain = if frame.num_chains == 0 {
                    None
                } else {
                    frame
                        .decode_target_protected_by_chain
                        .get(dt.target)
                        .and_then(|idx| self.chains.get(*idx).cloned())
                };
                new_targets.push(DecodeTarget::new(*dt, chain));
            }
            self.decode_targets = new_targets;
        }

        fn update_active_decode_targets(&mut self, bitmask: u32) {
            for chain in &self.chains {
                chain.begin_update_active();
            }

            for dt in &mut self.decode_targets {
                dt.update_active(bitmask);
            }

            for chain in &self.chains {
                chain.end_update_active();
            }
        }

        fn select(&mut self, frame: &DdFrameLite) -> VideoLayerSelectorResultLite {
            let mut result = VideoLayerSelectorResultLite::default();

            if self.current_layer.is_valid() {
                result.is_relevant = true;
            }

            if !self.key_frame_valid && !frame.structure_updated {
                return result;
            }

            let sd = self
                .decisions
                .borrow()
                .get_decision(frame.ext_frame_num)
                .unwrap_or(SelectorDecision::Missing);
            if sd == SelectorDecision::Dropped {
                return result;
            }
            if sd == SelectorDecision::Forwarded {
                result.is_selected = true;
                result.is_relevant = self.current_layer.is_valid();
                return result;
            }

            if frame.structure_updated {
                self.update_dependency_structure(frame);
            }

            if frame.ext_key_frame_num != self.ext_key_frame_num {
                self.decisions.borrow_mut().add_dropped(frame.ext_frame_num);
                self.invalidate_key_frame();
                return result;
            }

            if frame.active_decode_targets_updated {
                self.update_active_decode_targets(frame.active_decode_targets_bitmask);
            }

            if frame.fd.chain_diffs.len() != self.chains.len() {
                self.decisions.borrow_mut().add_dropped(frame.ext_frame_num);
                return result;
            }

            for chain in &self.chains {
                let _ = chain.on_frame(
                    frame.ext_frame_num,
                    &FrameDependencyTemplateLite {
                        decode_target_indications: frame.fd.decode_target_indications.clone(),
                        chain_diffs: frame.fd.chain_diffs.clone(),
                    },
                );
            }

            let mut highest: Option<(
                DependencyDescriptorDecodeTargetLite,
                DecodeTargetIndicationLite,
            )> = None;
            for dt in &self.decode_targets {
                if !dt.active()
                    || dt.target.spatial > self.target_layer.spatial
                    || dt.target.temporal > self.target_layer.temporal
                {
                    continue;
                }

                let fr = match dt.on_frame(
                    frame.ext_frame_num,
                    &FrameDependencyTemplateLite {
                        decode_target_indications: frame.fd.decode_target_indications.clone(),
                        chain_diffs: frame.fd.chain_diffs.clone(),
                    },
                ) {
                    Ok(v) => v,
                    Err(_) => {
                        self.decisions.borrow_mut().add_dropped(frame.ext_frame_num);
                        return result;
                    }
                };

                if fr.target_valid {
                    highest = Some((dt.target, fr.dti));
                    break;
                }
            }

            let Some((target, dti)) = highest else {
                self.decisions.borrow_mut().add_dropped(frame.ext_frame_num);
                return result;
            };

            if dti == DecodeTargetIndicationLite::NotPresent {
                self.decisions.borrow_mut().add_dropped(frame.ext_frame_num);
                return result;
            }

            for diff in &frame.fd.frame_diffs {
                if *diff == 0 {
                    continue;
                }
                let dep = frame.ext_frame_num.saturating_sub(*diff);
                if self
                    .decisions
                    .borrow()
                    .get_decision(dep)
                    .unwrap_or(SelectorDecision::Missing)
                    == SelectorDecision::Dropped
                {
                    self.decisions.borrow_mut().add_dropped(frame.ext_frame_num);
                    return result;
                }
            }

            let next_layer = VideoLayerLite {
                spatial: target.spatial,
                temporal: target.temporal,
            };
            if self.current_layer != next_layer {
                result.is_switching = true;
                result.is_resuming = !self.current_layer.is_valid();
                self.current_layer = next_layer;
                result.is_relevant = true;
            }

            self.decisions
                .borrow_mut()
                .add_forwarded(frame.ext_frame_num);
            result.is_selected = true;
            result
        }
    }

    fn create_dd_frames(
        max_spatial: i32,
        max_temporal: i32,
        start_frame_number: u16,
    ) -> Vec<DdFrameLite> {
        let mut frames = Vec::new();
        let mut active_bitmask: u32 = 0;
        let mut decode_targets = Vec::new();
        let mut decode_targets_protected_by_chain = Vec::new();

        for spatial in 0..=max_spatial {
            for temporal in 0..=max_temporal {
                let target = decode_targets.len();
                decode_targets.push(DependencyDescriptorDecodeTargetLite {
                    target,
                    spatial,
                    temporal,
                });
                decode_targets_protected_by_chain.push(spatial as usize);
                active_bitmask |= 1 << target;
            }
        }

        decode_targets.sort_by(|a, b| {
            if a.spatial != b.spatial {
                b.spatial.cmp(&a.spatial)
            } else {
                b.temporal.cmp(&a.temporal)
            }
        });

        let chain_diffs = vec![0; (max_spatial + 1) as usize];
        let mut dtis = vec![DecodeTargetIndicationLite::NotPresent; decode_targets.len()];
        for dt in &decode_targets {
            dtis[dt.target] = DecodeTargetIndicationLite::Switch;
        }

        let key_frame = DdFrameLite {
            is_key_frame: true,
            ext_frame_num: start_frame_number as u64,
            ext_key_frame_num: start_frame_number as u64,
            structure_updated: true,
            active_decode_targets_updated: true,
            active_decode_targets_bitmask: active_bitmask,
            decode_targets: decode_targets.clone(),
            decode_target_protected_by_chain: decode_targets_protected_by_chain.clone(),
            num_chains: (max_spatial + 1) as usize,
            fd: FrameDependencyTemplateDdLite {
                spatial_id: 0,
                temporal_id: 0,
                decode_target_indications: dtis.clone(),
                chain_diffs: chain_diffs.clone(),
                frame_diffs: Vec::new(),
            },
        };
        frames.push(key_frame.clone());

        let mut chain_prev: Vec<u64> = vec![start_frame_number as u64; (max_spatial + 1) as usize];
        let mut fnum = start_frame_number as u64 + 1;

        for _ in 0..10 {
            for idx in (0..decode_targets.len()).rev() {
                let dt = decode_targets[idx];
                let frame_chain_diffs: Vec<i32> = chain_prev
                    .iter()
                    .map(|prev| fnum.saturating_sub(*prev) as i32)
                    .collect();

                let mut frame_dtis =
                    vec![DecodeTargetIndicationLite::NotPresent; decode_targets.len()];
                for (k, slot) in frame_dtis.iter_mut().enumerate() {
                    if k >= dt.target {
                        *slot = if dt.temporal == 0 {
                            DecodeTargetIndicationLite::Required
                        } else {
                            DecodeTargetIndicationLite::Switch
                        };
                    }
                }

                let frame = DdFrameLite {
                    is_key_frame: false,
                    ext_frame_num: fnum,
                    ext_key_frame_num: key_frame.ext_frame_num,
                    structure_updated: false,
                    active_decode_targets_updated: false,
                    active_decode_targets_bitmask: active_bitmask,
                    decode_targets: decode_targets.clone(),
                    decode_target_protected_by_chain: decode_targets_protected_by_chain.clone(),
                    num_chains: (max_spatial + 1) as usize,
                    fd: FrameDependencyTemplateDdLite {
                        spatial_id: dt.spatial,
                        temporal_id: dt.temporal,
                        decode_target_indications: frame_dtis,
                        chain_diffs: frame_chain_diffs,
                        frame_diffs: Vec::new(),
                    },
                };

                fnum += 1;
                if dt.temporal == 0 {
                    chain_prev[dt.spatial as usize] = fnum;
                }
                frames.push(frame);
            }
        }

        frames
    }

    // Upstream: livekit/pkg/sfu/videolayerselector/dependencydescriptor_test.go::TestDependencyDescriptor
    #[test]
    fn dependency_descriptor_selector_matches_upstream_contract() {
        let mut selector = DependencyDescriptorSelectorLite::new();
        let target_layer = VideoLayerLite {
            spatial: 1,
            temporal: 2,
        };
        selector.set_target(target_layer);
        selector.set_request_spatial(1);

        // no dd ext equivalent path
        let ret = selector.select(&DdFrameLite {
            is_key_frame: false,
            ext_frame_num: 1,
            ext_key_frame_num: 1,
            structure_updated: false,
            active_decode_targets_updated: false,
            active_decode_targets_bitmask: 0,
            decode_targets: vec![],
            decode_target_protected_by_chain: vec![],
            num_chains: 0,
            fd: FrameDependencyTemplateDdLite {
                spatial_id: 0,
                temporal_id: 0,
                decode_target_indications: vec![],
                chain_diffs: vec![],
                frame_diffs: vec![],
            },
        });
        assert!(!ret.is_selected);
        assert!(!ret.is_relevant);

        // non-key frame before structure, dropped
        let ret = selector.select(&DdFrameLite {
            is_key_frame: false,
            ext_frame_num: 2,
            ext_key_frame_num: 2,
            structure_updated: false,
            active_decode_targets_updated: false,
            active_decode_targets_bitmask: 0,
            decode_targets: vec![],
            decode_target_protected_by_chain: vec![],
            num_chains: 0,
            fd: FrameDependencyTemplateDdLite {
                spatial_id: 1,
                temporal_id: 2,
                decode_target_indications: vec![],
                chain_diffs: vec![],
                frame_diffs: vec![],
            },
        });
        assert!(!ret.is_selected);
        assert!(!ret.is_relevant);

        let frames = create_dd_frames(2, 2, 3);
        let ret = selector.select(&frames[0]);
        assert!(ret.is_selected);
        assert_eq!(selector.get_current(), selector.get_target());
        let (sync, _) = selector.check_sync();
        assert!(sync);

        let mut belong_target_case = false;
        let mut exceed_target_case = false;
        let mut lower_target_case = false;
        let mut forwarded = Vec::<usize>::new();
        let mut dropped = Vec::<usize>::new();

        let mut idx = 1usize;
        while idx < frames.len() {
            let fd = &frames[idx].fd;
            let ret = selector.select(&frames[idx]);
            if fd.spatial_id == target_layer.spatial && fd.temporal_id == target_layer.temporal {
                assert!(ret.is_selected);
                belong_target_case = true;
                forwarded.push(idx);
            } else if fd.spatial_id < target_layer.spatial && fd.temporal_id == 0 {
                assert!(ret.is_selected);
                lower_target_case = true;
                forwarded.push(idx);
            } else if fd.spatial_id > target_layer.spatial || fd.temporal_id > target_layer.temporal
            {
                assert!(!ret.is_selected);
                exceed_target_case = true;
                dropped.push(idx);
            }

            if belong_target_case && exceed_target_case && lower_target_case {
                break;
            }
            idx += 1;
        }
        assert!(belong_target_case && exceed_target_case && lower_target_case);

        let ret = selector.select(&frames[forwarded[0]]);
        assert!(ret.is_selected);

        let ret = selector.select(&frames[dropped[0]]);
        assert!(!ret.is_selected);

        idx += 1;
        idx += 1;
        while idx < frames.len() {
            let fd = &frames[idx].fd;
            let ret = selector.select(&frames[idx]);
            if fd.spatial_id == target_layer.spatial && fd.temporal_id == target_layer.temporal {
                assert!(ret.is_selected);
                break;
            }
            idx += 1;
        }

        let mut not_decodable = frames[idx + 1].clone();
        not_decodable.fd.frame_diffs = vec![
            not_decodable
                .ext_frame_num
                .saturating_sub(frames[dropped[0]].ext_frame_num),
        ];
        let ret = selector.select(&not_decodable);
        assert!(!ret.is_selected);

        idx += 1;
        while idx < frames.len() {
            let fd = &frames[idx].fd;
            let ret = selector.select(&frames[idx]);
            if fd.spatial_id == target_layer.spatial && fd.temporal_id == target_layer.temporal {
                assert!(ret.is_selected);
                break;
            }
            idx += 1;
        }

        let mut broken = frames[idx + 1].clone();
        broken.fd.chain_diffs[target_layer.spatial as usize] = (not_decodable
            .ext_frame_num
            .saturating_sub(frames[dropped[0]].ext_frame_num))
            as i32;
        let ret = selector.select(&broken);
        assert!(!ret.is_selected);

        idx += 1;
        let mut switched_to_lower = false;
        while idx < frames.len() {
            let ret = selector.select(&frames[idx]);
            if ret.is_selected {
                assert!(target_layer.spatial > selector.get_current().spatial);
                switched_to_lower = true;
                break;
            }
            idx += 1;
        }
        assert!(switched_to_lower);

        selector.set_request_spatial(target_layer.spatial);
        let (locked, layer) = selector.check_sync();
        assert!(!locked);
        assert_eq!(layer, target_layer.spatial);

        selector.set_request_spatial(selector.get_current().spatial);
        let (locked, _) = selector.check_sync();
        assert!(locked);

        // frame from previous keyframe generation should be dropped and desync
        let previous_frames = create_dd_frames(2, 2, 1000);
        let ret = selector.select(&previous_frames[1]);
        assert!(!ret.is_selected);
        let (locked, _) = selector.check_sync();
        assert!(!locked);

        // keep compiler using fields that mirror upstream payloads
        assert!(frames.iter().any(|f| f.is_key_frame));
    }
}

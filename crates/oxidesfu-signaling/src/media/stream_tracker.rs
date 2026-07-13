use std::array;

const DEFAULT_MAX_SPATIAL_LAYER: i32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum StreamStatus {
    #[default]
    Stopped,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamStatusChange {
    None,
    Stopped,
    Active,
}

#[derive(Debug, Clone, Copy)]
struct StreamTrackerPacketConfig {
    samples_required: u32,
    cycles_required: u32,
}

#[derive(Debug, Clone)]
struct StreamTrackerPacket {
    config: StreamTrackerPacketConfig,
    count_since_last: u32,
    initialized: bool,
    cycle_count: u32,
}

impl StreamTrackerPacket {
    fn new(config: StreamTrackerPacketConfig) -> Self {
        Self {
            config,
            count_since_last: 0,
            initialized: false,
            cycle_count: 0,
        }
    }

    fn reset(&mut self) {
        self.count_since_last = 0;
        self.initialized = false;
        self.cycle_count = 0;
    }

    fn observe(&mut self) -> StreamStatusChange {
        if !self.initialized {
            self.initialized = true;
            self.count_since_last = 1;
            return StreamStatusChange::Active;
        }

        self.count_since_last = self.count_since_last.saturating_add(1);
        StreamStatusChange::None
    }

    fn check_status(&mut self) -> StreamStatusChange {
        if !self.initialized {
            return StreamStatusChange::None;
        }

        if self.count_since_last >= self.config.samples_required {
            self.cycle_count = self.cycle_count.saturating_add(1);
        } else {
            self.cycle_count = 0;
        }

        let status = if self.cycle_count == 0 {
            StreamStatusChange::Stopped
        } else if self.cycle_count >= self.config.cycles_required {
            StreamStatusChange::Active
        } else {
            StreamStatusChange::None
        };

        self.count_since_last = 0;
        status
    }
}

struct StreamTracker {
    packet: StreamTrackerPacket,
    paused: bool,
    status: StreamStatus,
    last_notified_status: StreamStatus,
    on_status_changed: Option<Box<dyn FnMut(StreamStatus)>>,
}

impl StreamTracker {
    fn new(packet: StreamTrackerPacket) -> Self {
        Self {
            packet,
            paused: false,
            status: StreamStatus::Stopped,
            last_notified_status: StreamStatus::Stopped,
            on_status_changed: None,
        }
    }

    fn on_status_changed(&mut self, callback: impl FnMut(StreamStatus) + 'static) {
        self.on_status_changed = Some(Box::new(callback));
    }

    fn status(&self) -> StreamStatus {
        self.status
    }

    fn reset(&mut self) {
        self.packet.reset();
        self.status = StreamStatus::Stopped;
        self.maybe_notify_status();
    }

    fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
        if paused {
            self.status = StreamStatus::Stopped;
        } else {
            self.packet.reset();
            self.status = StreamStatus::Stopped;
        }
        self.maybe_notify_status();
    }

    fn observe(&mut self, payload_size: usize) {
        if self.paused || payload_size == 0 {
            return;
        }

        let status_change = self.packet.observe();
        if status_change == StreamStatusChange::Active {
            self.status = StreamStatus::Active;
            self.maybe_notify_status();
        }
    }

    fn update_status(&mut self) {
        match self.packet.check_status() {
            StreamStatusChange::Stopped => self.status = StreamStatus::Stopped,
            StreamStatusChange::Active => self.status = StreamStatus::Active,
            StreamStatusChange::None => {}
        }
        self.maybe_notify_status();
    }

    fn maybe_notify_status(&mut self) {
        if self.status == self.last_notified_status {
            return;
        }

        self.last_notified_status = self.status;
        if let Some(callback) = self.on_status_changed.as_mut() {
            callback(self.status);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeTargetIndication {
    NotPresent,
    Required,
}

#[derive(Debug, Clone)]
struct DependencyDescriptorDecodeTarget {
    target: usize,
    spatial: i32,
    temporal: i32,
}

#[derive(Debug, Clone)]
struct ExtDependencyDescriptorLite {
    active_decode_targets_bitmask: Option<u32>,
    decode_targets: Vec<DependencyDescriptorDecodeTarget>,
    decode_target_indications: Vec<DecodeTargetIndication>,
    active_decode_targets_updated: bool,
}

struct StreamTrackerDependencyDescriptor {
    paused: bool,
    max_spatial_layer: i32,
    max_temporal_layer: i32,
    on_status_changed: [Option<Box<dyn FnMut(StreamStatus)>>; 3],
}

impl StreamTrackerDependencyDescriptor {
    fn new() -> Self {
        Self {
            paused: false,
            max_spatial_layer: -1,
            max_temporal_layer: -1,
            on_status_changed: array::from_fn(|_| None),
        }
    }

    fn status(&self, layer: i32) -> StreamStatus {
        if layer > self.max_spatial_layer {
            StreamStatus::Stopped
        } else {
            StreamStatus::Active
        }
    }

    fn on_status_changed(&mut self, layer: i32, callback: impl FnMut(StreamStatus) + 'static) {
        if !(0..=DEFAULT_MAX_SPATIAL_LAYER).contains(&layer) {
            return;
        }
        self.on_status_changed[layer as usize] = Some(Box::new(callback));
    }

    fn set_paused(&mut self, paused: bool) {
        if self.paused == paused {
            return;
        }

        self.paused = paused;
        if !paused {
            self.max_spatial_layer = -1;
            self.max_temporal_layer = -1;
            for callback in self.on_status_changed.iter_mut().flatten() {
                callback(StreamStatus::Stopped);
            }
        }
    }

    fn observe(
        &mut self,
        pkt_size: usize,
        payload_size: usize,
        dd: Option<ExtDependencyDescriptorLite>,
    ) {
        if self.paused || payload_size == 0 || pkt_size == 0 {
            return;
        }

        let Some(dd) = dd else {
            return;
        };

        if !dd.active_decode_targets_updated {
            return;
        }

        let Some(mask) = dd.active_decode_targets_bitmask else {
            return;
        };

        let mut max_spatial = -1;
        let mut max_temporal = -1;
        for target in &dd.decode_targets {
            if target.target >= dd.decode_target_indications.len() {
                continue;
            }
            if dd.decode_target_indications[target.target] == DecodeTargetIndication::NotPresent {
                continue;
            }
            if (mask & (1 << target.target)) == 0 {
                continue;
            }

            max_spatial = max_spatial.max(target.spatial.min(DEFAULT_MAX_SPATIAL_LAYER));
            max_temporal = max_temporal.max(target.temporal);
        }

        let old_max_spatial = self.max_spatial_layer;
        self.max_spatial_layer = max_spatial;
        self.max_temporal_layer = max_temporal;

        if old_max_spatial < self.max_spatial_layer {
            for layer in (old_max_spatial + 1)..=self.max_spatial_layer {
                if let Some(callback) = self
                    .on_status_changed
                    .get_mut(layer as usize)
                    .and_then(Option::as_mut)
                {
                    callback(StreamStatus::Active);
                }
            }
        } else if old_max_spatial > self.max_spatial_layer {
            for layer in (self.max_spatial_layer + 1)..=old_max_spatial {
                if let Some(callback) = self
                    .on_status_changed
                    .get_mut(layer as usize)
                    .and_then(Option::as_mut)
                {
                    callback(StreamStatus::Stopped);
                }
            }
        }
    }

    fn layered_tracker(&mut self, layer: i32) -> StreamTrackerDependencyDescriptorLayered<'_> {
        StreamTrackerDependencyDescriptorLayered {
            parent: self,
            layer,
        }
    }
}

struct StreamTrackerDependencyDescriptorLayered<'a> {
    parent: &'a mut StreamTrackerDependencyDescriptor,
    layer: i32,
}

impl StreamTrackerDependencyDescriptorLayered<'_> {
    fn on_status_changed(&mut self, callback: impl FnMut(StreamStatus) + 'static) {
        self.parent.on_status_changed(self.layer, callback);
    }

    fn status(&self) -> StreamStatus {
        self.parent.status(self.layer)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    fn new_stream_tracker_packet(samples_required: u32, cycles_required: u32) -> StreamTracker {
        StreamTracker::new(StreamTrackerPacket::new(StreamTrackerPacketConfig {
            samples_required,
            cycles_required,
        }))
    }

    // Upstream: livekit/pkg/sfu/streamtracker/streamtracker_packet_test.go::TestStreamTracker
    #[test]
    fn stream_tracker_packet_matches_upstream_contract() {
        // flips to active on first observe
        let callback_count = Rc::new(RefCell::new(0u32));
        let callback_count_ref = callback_count.clone();
        let mut tracker = new_stream_tracker_packet(5, 60);
        tracker.on_status_changed(move |_| {
            *callback_count_ref.borrow_mut() += 1;
        });
        assert_eq!(tracker.status(), StreamStatus::Stopped);

        tracker.observe(10);
        assert_eq!(tracker.status(), StreamStatus::Active);
        assert_eq!(*callback_count.borrow(), 1);

        // flips to inactive immediately when status check runs without enough samples
        tracker.update_status();
        assert_eq!(tracker.status(), StreamStatus::Stopped);
        assert_eq!(*callback_count.borrow(), 2);

        // flips back to active only after required number of active cycles
        let mut tracker = new_stream_tracker_packet(1, 2);
        tracker.observe(10);
        assert_eq!(tracker.status(), StreamStatus::Active);

        tracker.status = StreamStatus::Stopped;
        tracker.last_notified_status = StreamStatus::Stopped;

        tracker.observe(10);
        tracker.update_status();
        assert_eq!(tracker.status(), StreamStatus::Stopped);

        tracker.observe(10);
        tracker.update_status();
        assert_eq!(tracker.status(), StreamStatus::Active);

        // changes to inactive when paused
        tracker.set_paused(true);
        tracker.update_status();
        assert_eq!(tracker.status(), StreamStatus::Stopped);

        // first packet after reset should re-activate
        let callback_count = Rc::new(RefCell::new(0u32));
        let callback_count_ref = callback_count.clone();
        let mut tracker = new_stream_tracker_packet(5, 60);
        tracker.on_status_changed(move |_| {
            *callback_count_ref.borrow_mut() += 1;
        });
        tracker.observe(10);
        assert_eq!(*callback_count.borrow(), 1);

        tracker.observe(10);
        tracker.observe(10);
        tracker.observe(10);
        tracker.observe(10);
        tracker.update_status();
        assert_eq!(*callback_count.borrow(), 1);

        tracker.reset();
        assert_eq!(tracker.status(), StreamStatus::Stopped);
        assert_eq!(*callback_count.borrow(), 2);

        tracker.observe(10);
        assert_eq!(tracker.status(), StreamStatus::Active);
        assert_eq!(*callback_count.borrow(), 3);
    }

    fn create_descriptor_dependency_for_targets(
        max_spatial: usize,
        max_temporal: usize,
    ) -> ExtDependencyDescriptorLite {
        let mut decode_targets = Vec::new();
        let mut mask: u32 = 0;
        for spatial in 0..=max_spatial {
            for temporal in 0..=max_temporal {
                let target = decode_targets.len();
                decode_targets.push(DependencyDescriptorDecodeTarget {
                    target,
                    spatial: spatial as i32,
                    temporal: temporal as i32,
                });
                mask |= 1 << target;
            }
        }

        let mut dtis = vec![DecodeTargetIndication::NotPresent; decode_targets.len()];
        for target in &decode_targets {
            dtis[target.target] = DecodeTargetIndication::Required;
        }

        ExtDependencyDescriptorLite {
            active_decode_targets_bitmask: Some(mask),
            decode_targets,
            decode_target_indications: dtis,
            active_decode_targets_updated: true,
        }
    }

    // Upstream: livekit/pkg/sfu/streamtracker/streamtracker_dd_test.go::TestStreamTrackerDD
    #[test]
    fn stream_tracker_dd_matches_upstream_contract() {
        let mut dd_tracker = StreamTrackerDependencyDescriptor::new();
        let statuses = Rc::new(RefCell::new(vec![StreamStatus::Stopped; 3]));

        for layer in 0..=2 {
            let statuses_ref = statuses.clone();
            let mut layered = dd_tracker.layered_tracker(layer);
            layered.on_status_changed(move |status| {
                statuses_ref.borrow_mut()[layer as usize] = status;
            });
            assert_eq!(layered.status(), StreamStatus::Stopped);
        }

        // no active layers
        dd_tracker.observe(1000, 1000, None);
        assert_eq!(
            statuses.borrow().as_slice(),
            &[
                StreamStatus::Stopped,
                StreamStatus::Stopped,
                StreamStatus::Stopped
            ]
        );

        // layers seen [0, 1]
        dd_tracker.observe(
            1000,
            1000,
            Some(create_descriptor_dependency_for_targets(1, 1)),
        );
        assert_eq!(statuses.borrow()[0], StreamStatus::Active);
        assert_eq!(statuses.borrow()[1], StreamStatus::Active);
        assert_eq!(statuses.borrow()[2], StreamStatus::Stopped);

        // layers seen [0, 1, 2]
        dd_tracker.observe(
            1000,
            1000,
            Some(create_descriptor_dependency_for_targets(2, 1)),
        );
        assert_eq!(statuses.borrow()[0], StreamStatus::Active);
        assert_eq!(statuses.borrow()[1], StreamStatus::Active);
        assert_eq!(statuses.borrow()[2], StreamStatus::Active);

        // layer 2 gone, layers seen [0, 1]
        dd_tracker.observe(
            1000,
            1000,
            Some(create_descriptor_dependency_for_targets(1, 1)),
        );
        assert_eq!(statuses.borrow()[0], StreamStatus::Active);
        assert_eq!(statuses.borrow()[1], StreamStatus::Active);
        assert_eq!(statuses.borrow()[2], StreamStatus::Stopped);
    }
}

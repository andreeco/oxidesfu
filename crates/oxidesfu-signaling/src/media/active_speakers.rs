#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    #[derive(Debug, Clone, PartialEq)]
    struct SpeakerInfoLite {
        sid: String,
        level: f32,
        active: bool,
    }

    #[derive(Debug)]
    struct ActiveSpeakerTrackerLite {
        smooth_intervals: usize,
        history: HashMap<String, VecDeque<f32>>,
        active: HashMap<String, bool>,
    }

    impl ActiveSpeakerTrackerLite {
        fn new(smooth_intervals: usize) -> Self {
            Self {
                smooth_intervals: smooth_intervals.max(1),
                history: HashMap::new(),
                active: HashMap::new(),
            }
        }

        fn observe(&mut self, sid: &str, level: f32, active: bool) {
            let entry = self.history.entry(sid.to_string()).or_default();
            entry.push_back(level);
            while entry.len() > self.smooth_intervals {
                entry.pop_front();
            }
            self.active.insert(sid.to_string(), active);
        }

        fn get_active_speakers(&self) -> Vec<SpeakerInfoLite> {
            let mut speakers = self
                .history
                .iter()
                .map(|(sid, levels)| {
                    let sum: f32 = levels.iter().copied().sum();
                    let smoothed = if levels.is_empty() {
                        0.0
                    } else {
                        sum / levels.len() as f32
                    };
                    SpeakerInfoLite {
                        sid: sid.clone(),
                        level: smoothed,
                        active: *self.active.get(sid).unwrap_or(&false),
                    }
                })
                .collect::<Vec<_>>();
            speakers.sort_by(|a, b| {
                b.level
                    .partial_cmp(&a.level)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            speakers
        }
    }

    // Upstream: livekit/pkg/rtc/room_test.go::TestActiveSpeakers
    #[test]
    fn active_speakers_are_sorted_and_smoothed_and_emit_inactive_transition() {
        let mut tracker = ActiveSpeakerTrackerLite::new(3);
        tracker.observe("PA_loud", 0.80, true);
        tracker.observe("PA_quiet", 0.40, true);

        let speakers = tracker.get_active_speakers();
        assert_eq!(speakers.len(), 2);
        assert_eq!(speakers[0].sid, "PA_loud");
        assert_eq!(speakers[1].sid, "PA_quiet");

        tracker.observe("PA_loud", 0.90, true);
        tracker.observe("PA_loud", 1.00, true);
        let speakers = tracker.get_active_speakers();
        let loud = speakers
            .iter()
            .find(|speaker| speaker.sid == "PA_loud")
            .expect("loud speaker should be present");
        assert!(
            loud.level > 0.80,
            "smoothed level should rise above initial sample"
        );

        tracker.observe("PA_loud", 0.0, false);
        let speakers = tracker.get_active_speakers();
        let loud = speakers
            .iter()
            .find(|speaker| speaker.sid == "PA_loud")
            .expect("loud speaker should still be in update as inactive");
        assert!(
            !loud.active,
            "inactive transition should be reflected in updates"
        );
    }
}

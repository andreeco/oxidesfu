use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
enum MimeTypeLite {
    Vp8,
    Av1,
    Red,
    Opus,
}

impl MimeTypeLite {
    fn as_str(self) -> &'static str {
        match self {
            Self::Vp8 => "video/vp8",
            Self::Av1 => "video/av1",
            Self::Red => "audio/red",
            Self::Opus => "audio/opus",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
enum VideoQualityLite {
    Off,
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscribedQualityLite {
    quality: VideoQualityLite,
    enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscribedCodecLite {
    codec: String,
    qualities: Vec<SubscribedQualityLite>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscribedAudioCodecLite {
    codec: String,
    enabled: bool,
}

struct DynacastManagerVideoLite {
    subscriber_qualities: HashMap<(String, MimeTypeLite), VideoQualityLite>,
    node_qualities: HashMap<(String, MimeTypeLite), VideoQualityLite>,
    known_codecs: HashSet<MimeTypeLite>,
    regressed_codec: HashMap<MimeTypeLite, MimeTypeLite>,
    on_subscribed_max_quality_change: Option<Box<dyn FnMut(Vec<SubscribedCodecLite>)>>,
}

impl DynacastManagerVideoLite {
    fn new() -> Self {
        Self {
            subscriber_qualities: HashMap::new(),
            node_qualities: HashMap::new(),
            known_codecs: HashSet::new(),
            regressed_codec: HashMap::new(),
            on_subscribed_max_quality_change: None,
        }
    }

    fn on_subscribed_max_quality_change(
        &mut self,
        callback: impl FnMut(Vec<SubscribedCodecLite>) + 'static,
    ) {
        self.on_subscribed_max_quality_change = Some(Box::new(callback));
    }

    fn handle_codec_regression(&mut self, from_mime: MimeTypeLite, to_mime: MimeTypeLite) {
        self.known_codecs.insert(from_mime);
        self.known_codecs.insert(to_mime);
        self.regressed_codec.insert(from_mime, to_mime);

        if !self
            .subscriber_qualities
            .keys()
            .any(|(_, mime)| *mime == to_mime)
            && !self.node_qualities.keys().any(|(_, mime)| *mime == to_mime)
        {
            self.subscriber_qualities.insert(
                ("__regression_seed__".to_string(), to_mime),
                VideoQualityLite::High,
            );
        }

        self.emit();
    }

    fn notify_subscriber_max_quality(
        &mut self,
        subscriber_id: &str,
        mime: MimeTypeLite,
        quality: VideoQualityLite,
    ) {
        let effective_mime = self.regressed_codec.get(&mime).copied().unwrap_or(mime);
        self.known_codecs.insert(mime);
        self.known_codecs.insert(effective_mime);
        self.subscriber_qualities
            .remove(&("__regression_seed__".to_string(), effective_mime));
        self.subscriber_qualities
            .insert((subscriber_id.to_string(), effective_mime), quality);
        self.emit();
    }

    fn notify_subscriber_node_max_quality(
        &mut self,
        node_id: &str,
        qualities: &[(MimeTypeLite, VideoQualityLite)],
    ) {
        for (mime, quality) in qualities {
            self.known_codecs.insert(*mime);
            if self.regressed_codec.contains_key(mime) {
                continue;
            }
            self.subscriber_qualities
                .remove(&("__regression_seed__".to_string(), *mime));
            self.node_qualities
                .insert((node_id.to_string(), *mime), *quality);
        }
        self.emit();
    }

    fn emit(&mut self) {
        let mut aggregated: Vec<SubscribedCodecLite> = Vec::new();
        let mut codecs: Vec<_> = self.known_codecs.iter().copied().collect();
        codecs.sort();

        for mime in codecs {
            let quality = if self.regressed_codec.contains_key(&mime) {
                VideoQualityLite::Off
            } else {
                self.aggregate_quality_for_codec(mime)
            };

            let qualities = if quality == VideoQualityLite::Off {
                vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: false,
                    },
                ]
            } else {
                vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: VideoQualityLite::Medium <= quality,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: VideoQualityLite::High <= quality,
                    },
                ]
            };

            aggregated.push(SubscribedCodecLite {
                codec: mime.as_str().to_string(),
                qualities,
            });
        }

        if let Some(callback) = self.on_subscribed_max_quality_change.as_mut() {
            callback(aggregated);
        }
    }

    fn aggregate_quality_for_codec(&self, mime: MimeTypeLite) -> VideoQualityLite {
        self.subscriber_qualities
            .iter()
            .filter_map(|((_, m), q)| (*m == mime).then_some(*q))
            .chain(
                self.node_qualities
                    .iter()
                    .filter_map(|((_, m), q)| (*m == mime).then_some(*q)),
            )
            .max()
            .unwrap_or(VideoQualityLite::Off)
    }
}

struct DynacastManagerAudioLite {
    subscriber_enabled: HashMap<(String, MimeTypeLite), bool>,
    node_enabled: HashMap<(String, MimeTypeLite), bool>,
    known_codecs: HashSet<MimeTypeLite>,
    regressed_codec: HashMap<MimeTypeLite, MimeTypeLite>,
    on_subscribed_audio_codec_change: Option<Box<dyn FnMut(Vec<SubscribedAudioCodecLite>)>>,
}

impl DynacastManagerAudioLite {
    fn new() -> Self {
        Self {
            subscriber_enabled: HashMap::new(),
            node_enabled: HashMap::new(),
            known_codecs: HashSet::new(),
            regressed_codec: HashMap::new(),
            on_subscribed_audio_codec_change: None,
        }
    }

    fn on_subscribed_audio_codec_change(
        &mut self,
        callback: impl FnMut(Vec<SubscribedAudioCodecLite>) + 'static,
    ) {
        self.on_subscribed_audio_codec_change = Some(Box::new(callback));
    }

    fn handle_codec_regression(&mut self, from_mime: MimeTypeLite, to_mime: MimeTypeLite) {
        self.known_codecs.insert(from_mime);
        self.known_codecs.insert(to_mime);
        self.regressed_codec.insert(from_mime, to_mime);

        if !self
            .subscriber_enabled
            .keys()
            .any(|(_, mime)| *mime == to_mime)
            && !self.node_enabled.keys().any(|(_, mime)| *mime == to_mime)
        {
            self.subscriber_enabled
                .insert(("__regression_seed__".to_string(), to_mime), true);
        }

        self.emit();
    }

    fn notify_subscription(&mut self, subscriber_id: &str, mime: MimeTypeLite, enabled: bool) {
        self.known_codecs.insert(mime);
        if self.regressed_codec.contains_key(&mime) {
            self.emit();
            return;
        }

        self.subscriber_enabled
            .remove(&("__regression_seed__".to_string(), mime));
        self.subscriber_enabled
            .insert((subscriber_id.to_string(), mime), enabled);
        self.emit();
    }

    fn notify_subscription_node(&mut self, node_id: &str, codecs: &[(MimeTypeLite, bool)]) {
        for (mime, enabled) in codecs {
            self.known_codecs.insert(*mime);
            if self.regressed_codec.contains_key(mime) {
                continue;
            }
            self.subscriber_enabled
                .remove(&("__regression_seed__".to_string(), *mime));
            self.node_enabled
                .insert((node_id.to_string(), *mime), *enabled);
        }
        self.emit();
    }

    fn emit(&mut self) {
        let mut aggregated = Vec::new();
        let mut codecs: Vec<_> = self.known_codecs.iter().copied().collect();
        codecs.sort();

        for mime in codecs {
            let enabled = if self.regressed_codec.contains_key(&mime) {
                false
            } else {
                self.aggregate_enabled_for_codec(mime)
            };
            aggregated.push(SubscribedAudioCodecLite {
                codec: mime.as_str().to_string(),
                enabled,
            });
        }

        if let Some(callback) = self.on_subscribed_audio_codec_change.as_mut() {
            callback(aggregated);
        }
    }

    fn aggregate_enabled_for_codec(&self, mime: MimeTypeLite) -> bool {
        self.subscriber_enabled
            .iter()
            .filter_map(|((_, m), e)| (*m == mime).then_some(*e))
            .chain(
                self.node_enabled
                    .iter()
                    .filter_map(|((_, m), e)| (*m == mime).then_some(*e)),
            )
            .any(|enabled| enabled)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    fn subscribed_codecs_as_string(mut codecs: Vec<SubscribedCodecLite>) -> String {
        codecs.sort_by(|a, b| a.codec.cmp(&b.codec));
        format!("{codecs:?}")
    }

    fn subscribed_audio_codecs_as_string(mut codecs: Vec<SubscribedAudioCodecLite>) -> String {
        codecs.sort_by(|a, b| a.codec.cmp(&b.codec));
        format!("{codecs:?}")
    }

    // Upstream: livekit/pkg/rtc/dynacast/dynacastmanager_test.go::TestSubscribedMaxQuality
    #[test]
    fn dynacast_subscribed_max_quality_matches_upstream_contract() {
        // scenario 1: subscribers muted
        let updates = Rc::new(RefCell::new(Vec::<SubscribedCodecLite>::new()));
        let updates_ref = updates.clone();
        let mut dm = DynacastManagerVideoLite::new();
        dm.on_subscribed_max_quality_change(move |qualities| {
            *updates_ref.borrow_mut() = qualities;
        });

        dm.notify_subscriber_max_quality("s1", MimeTypeLite::Vp8, VideoQualityLite::High);
        dm.notify_subscriber_max_quality("s2", MimeTypeLite::Av1, VideoQualityLite::High);
        dm.notify_subscriber_max_quality("s1", MimeTypeLite::Vp8, VideoQualityLite::Off);

        let expected = vec![
            SubscribedCodecLite {
                codec: MimeTypeLite::Vp8.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: false,
                    },
                ],
            },
            SubscribedCodecLite {
                codec: MimeTypeLite::Av1.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: true,
                    },
                ],
            },
        ];
        assert_eq!(
            subscribed_codecs_as_string(expected),
            subscribed_codecs_as_string(updates.borrow().clone())
        );

        // scenario 2: subscribers max quality + node bumping
        let updates = Rc::new(RefCell::new(Vec::<SubscribedCodecLite>::new()));
        let updates_ref = updates.clone();
        let mut dm = DynacastManagerVideoLite::new();
        dm.on_subscribed_max_quality_change(move |qualities| {
            *updates_ref.borrow_mut() = qualities;
        });

        dm.notify_subscriber_max_quality("s1", MimeTypeLite::Vp8, VideoQualityLite::High);
        dm.notify_subscriber_max_quality("s2", MimeTypeLite::Vp8, VideoQualityLite::Medium);
        dm.notify_subscriber_max_quality("s3", MimeTypeLite::Av1, VideoQualityLite::Medium);

        dm.notify_subscriber_max_quality("s1", MimeTypeLite::Vp8, VideoQualityLite::Low);
        dm.notify_subscriber_max_quality("s2", MimeTypeLite::Vp8, VideoQualityLite::Low);
        dm.notify_subscriber_max_quality("s3", MimeTypeLite::Av1, VideoQualityLite::Low);
        dm.notify_subscriber_max_quality("s2", MimeTypeLite::Vp8, VideoQualityLite::Off);
        dm.notify_subscriber_max_quality("s1", MimeTypeLite::Vp8, VideoQualityLite::Off);
        dm.notify_subscriber_max_quality("s3", MimeTypeLite::Av1, VideoQualityLite::Off);

        let all_off = vec![
            SubscribedCodecLite {
                codec: MimeTypeLite::Vp8.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: false,
                    },
                ],
            },
            SubscribedCodecLite {
                codec: MimeTypeLite::Av1.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: false,
                    },
                ],
            },
        ];
        assert_eq!(
            subscribed_codecs_as_string(all_off),
            subscribed_codecs_as_string(updates.borrow().clone())
        );

        dm.notify_subscriber_max_quality("s1", MimeTypeLite::Vp8, VideoQualityLite::Low);
        dm.notify_subscriber_node_max_quality(
            "n1",
            &[
                (MimeTypeLite::Vp8, VideoQualityLite::High),
                (MimeTypeLite::Av1, VideoQualityLite::Medium),
            ],
        );

        let node_raise = vec![
            SubscribedCodecLite {
                codec: MimeTypeLite::Vp8.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: true,
                    },
                ],
            },
            SubscribedCodecLite {
                codec: MimeTypeLite::Av1.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: false,
                    },
                ],
            },
        ];
        assert_eq!(
            subscribed_codecs_as_string(node_raise),
            subscribed_codecs_as_string(updates.borrow().clone())
        );
    }

    // Upstream: livekit/pkg/rtc/dynacast/dynacastmanager_test.go::TestCodecRegression
    #[test]
    fn dynacast_codec_regression_matches_upstream_contract() {
        // video regression
        let video_updates = Rc::new(RefCell::new(Vec::<SubscribedCodecLite>::new()));
        let video_updates_ref = video_updates.clone();
        let mut video_dm = DynacastManagerVideoLite::new();
        video_dm.on_subscribed_max_quality_change(move |qualities| {
            *video_updates_ref.borrow_mut() = qualities;
        });

        video_dm.notify_subscriber_max_quality("s1", MimeTypeLite::Av1, VideoQualityLite::High);
        video_dm.handle_codec_regression(MimeTypeLite::Av1, MimeTypeLite::Vp8);

        let expected_after_regression = vec![
            SubscribedCodecLite {
                codec: MimeTypeLite::Av1.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: false,
                    },
                ],
            },
            SubscribedCodecLite {
                codec: MimeTypeLite::Vp8.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: true,
                    },
                ],
            },
        ];
        assert_eq!(
            subscribed_codecs_as_string(expected_after_regression),
            subscribed_codecs_as_string(video_updates.borrow().clone())
        );

        // AV1 updates should forward to VP8, AV1 node updates should be ignored
        video_dm.notify_subscriber_max_quality("s1", MimeTypeLite::Av1, VideoQualityLite::Medium);
        video_dm.notify_subscriber_node_max_quality(
            "n1",
            &[(MimeTypeLite::Av1, VideoQualityLite::High)],
        );

        let expected_forwarded = vec![
            SubscribedCodecLite {
                codec: MimeTypeLite::Av1.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: false,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: false,
                    },
                ],
            },
            SubscribedCodecLite {
                codec: MimeTypeLite::Vp8.as_str().to_string(),
                qualities: vec![
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Low,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::Medium,
                        enabled: true,
                    },
                    SubscribedQualityLite {
                        quality: VideoQualityLite::High,
                        enabled: false,
                    },
                ],
            },
        ];
        assert_eq!(
            subscribed_codecs_as_string(expected_forwarded),
            subscribed_codecs_as_string(video_updates.borrow().clone())
        );

        // audio regression
        let audio_updates = Rc::new(RefCell::new(Vec::<SubscribedAudioCodecLite>::new()));
        let audio_updates_ref = audio_updates.clone();
        let mut audio_dm = DynacastManagerAudioLite::new();
        audio_dm.on_subscribed_audio_codec_change(move |codecs| {
            *audio_updates_ref.borrow_mut() = codecs;
        });

        audio_dm.notify_subscription("s1", MimeTypeLite::Red, true);
        audio_dm.handle_codec_regression(MimeTypeLite::Red, MimeTypeLite::Opus);

        let expected_audio_after_regression = vec![
            SubscribedAudioCodecLite {
                codec: MimeTypeLite::Red.as_str().to_string(),
                enabled: false,
            },
            SubscribedAudioCodecLite {
                codec: MimeTypeLite::Opus.as_str().to_string(),
                enabled: true,
            },
        ];
        assert_eq!(
            subscribed_audio_codecs_as_string(expected_audio_after_regression.clone()),
            subscribed_audio_codecs_as_string(audio_updates.borrow().clone())
        );

        audio_dm.notify_subscription("s1", MimeTypeLite::Red, false);
        audio_dm.notify_subscription_node("n1", &[(MimeTypeLite::Red, false)]);
        assert_eq!(
            subscribed_audio_codecs_as_string(expected_audio_after_regression.clone()),
            subscribed_audio_codecs_as_string(audio_updates.borrow().clone())
        );

        audio_dm.notify_subscription("s1", MimeTypeLite::Opus, false);
        let expected_opus_off = vec![
            SubscribedAudioCodecLite {
                codec: MimeTypeLite::Red.as_str().to_string(),
                enabled: false,
            },
            SubscribedAudioCodecLite {
                codec: MimeTypeLite::Opus.as_str().to_string(),
                enabled: false,
            },
        ];
        assert_eq!(
            subscribed_audio_codecs_as_string(expected_opus_off),
            subscribed_audio_codecs_as_string(audio_updates.borrow().clone())
        );

        audio_dm.notify_subscription_node("n1", &[(MimeTypeLite::Opus, true)]);
        let expected_opus_on = vec![
            SubscribedAudioCodecLite {
                codec: MimeTypeLite::Red.as_str().to_string(),
                enabled: false,
            },
            SubscribedAudioCodecLite {
                codec: MimeTypeLite::Opus.as_str().to_string(),
                enabled: true,
            },
        ];
        assert_eq!(
            subscribed_audio_codecs_as_string(expected_opus_on),
            subscribed_audio_codecs_as_string(audio_updates.borrow().clone())
        );
    }
}

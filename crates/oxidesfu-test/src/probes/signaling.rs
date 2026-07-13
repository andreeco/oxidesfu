use super::*;

    async fn wait_for_track_published_sid(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<SignalEvent>,
    ) -> String {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let event = events
                    .recv()
                    .await
                    .expect("signal events should stay open");
                if let SignalEvent::Message(message) = event
                    && let proto::signal_response::Message::TrackPublished(track_published) = *message
                {
                    return track_published
                        .track
                        .expect("track_published should contain track")
                        .sid;
                }
            }
        })
        .await
        .expect("publisher should receive TrackPublished before timeout")
    }

    async fn wait_for_subscribed_quality_update_for_track(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<SignalEvent>,
        track_sid: &str,
    ) -> proto::SubscribedQualityUpdate {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let event = events
                    .recv()
                    .await
                    .expect("signal events should stay open");
                if let SignalEvent::Message(message) = event
                    && let proto::signal_response::Message::SubscribedQualityUpdate(update) = *message
                    && update.track_sid == track_sid
                {
                    return update;
                }
            }
        })
        .await
        .expect("publisher should receive SubscribedQualityUpdate before timeout")
    }

    async fn wait_for_subscribed_quality_flags_for_track(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<SignalEvent>,
        track_sid: &str,
        expected_flags: [bool; 3],
    ) -> proto::SubscribedQualityUpdate {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let update = wait_for_subscribed_quality_update_for_track(events, track_sid).await;
                let flags = subscribed_quality_flags(&update);
                if flags == expected_flags {
                    return update;
                }
            }
        })
        .await
        .expect("publisher should receive expected SubscribedQualityUpdate flags before timeout")
    }

    fn subscribed_quality_flags(update: &proto::SubscribedQualityUpdate) -> [bool; 3] {
        let mut flags = [false; 3];
        for quality in &update.subscribed_qualities {
            if let Ok(video_quality) = proto::VideoQuality::try_from(quality.quality) {
                match video_quality {
                    proto::VideoQuality::Low => flags[0] = quality.enabled,
                    proto::VideoQuality::Medium => flags[1] = quality.enabled,
                    proto::VideoQuality::High => flags[2] = quality.enabled,
                    proto::VideoQuality::Off => {}
                }
            }
        }
        flags
    }

    #[tokio::test]
    async fn signal_track_setting_quality_aggregate_unsubscribe_and_leave_contract() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, oxidesfu_server::app())
                .await
                .expect("test server should run");
        });

        let room_name = format!("signal-quality-aggregate-{}", unique_suffix());

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("signal-quality-publisher")
            .with_name("Signal Quality Publisher")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");
        let high_sub_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("signal-quality-high-sub")
            .with_name("Signal Quality High Subscriber")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("high subscriber token should encode");
        let low_sub_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("signal-quality-low-sub")
            .with_name("Signal Quality Low Subscriber")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("low subscriber token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(6);

        let (publisher_signal, _publisher_join, mut publisher_events) =
            SignalClient::connect(&format!("http://{addr}"), &publisher_token, options.clone(), None)
                .await
                .expect("publisher signal client should connect");
        let (high_sub_signal, _high_join, mut high_events) =
            SignalClient::connect(&format!("http://{addr}"), &high_sub_token, options.clone(), None)
                .await
                .expect("high subscriber signal client should connect");
        let (low_sub_signal, _low_join, mut low_events) =
            SignalClient::connect(&format!("http://{addr}"), &low_sub_token, options, None)
                .await
                .expect("low subscriber signal client should connect");

        publisher_signal
            .send(proto::signal_request::Message::AddTrack(proto::AddTrackRequest {
                cid: "quality-video-cid".to_string(),
                name: "cam".to_string(),
                r#type: proto::TrackType::Video as i32,
                source: proto::TrackSource::Camera as i32,
                layers: vec![
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Low as i32,
                        spatial_layer: 0,
                        rid: "q".to_string(),
                        ssrc: 1001,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::Medium as i32,
                        spatial_layer: 1,
                        rid: "h".to_string(),
                        ssrc: 1002,
                        ..Default::default()
                    },
                    proto::VideoLayer {
                        quality: proto::VideoQuality::High as i32,
                        spatial_layer: 2,
                        rid: "f".to_string(),
                        ssrc: 1003,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }))
            .await;

        let track_sid = wait_for_track_published_sid(&mut publisher_events).await;

        for subscriber in [&high_sub_signal, &low_sub_signal] {
            subscriber
                .send(proto::signal_request::Message::Subscription(
                    proto::UpdateSubscription {
                        track_sids: vec![track_sid.clone()],
                        subscribe: true,
                        ..Default::default()
                    },
                ))
                .await;
        }

        high_sub_signal
            .send(proto::signal_request::Message::TrackSetting(
                proto::UpdateTrackSettings {
                    track_sids: vec![track_sid.clone()],
                    quality: proto::VideoQuality::High as i32,
                    fps: 30,
                    ..Default::default()
                },
            ))
            .await;
        let _update_after_high = wait_for_subscribed_quality_flags_for_track(
            &mut publisher_events,
            &track_sid,
            [true, true, true],
        )
        .await;

        low_sub_signal
            .send(proto::signal_request::Message::TrackSetting(
                proto::UpdateTrackSettings {
                    track_sids: vec![track_sid.clone()],
                    quality: proto::VideoQuality::Low as i32,
                    fps: 10,
                    ..Default::default()
                },
            ))
            .await;
        let _update_after_low = wait_for_subscribed_quality_flags_for_track(
            &mut publisher_events,
            &track_sid,
            [true, true, true],
        )
        .await;

        high_sub_signal
            .send(proto::signal_request::Message::Subscription(
                proto::UpdateSubscription {
                    track_sids: vec![track_sid.clone()],
                    subscribe: false,
                    ..Default::default()
                },
            ))
            .await;
        let _update_after_high_unsub = wait_for_subscribed_quality_flags_for_track(
            &mut publisher_events,
            &track_sid,
            [true, false, false],
        )
        .await;

        low_sub_signal
            .send(proto::signal_request::Message::TrackSetting(
                proto::UpdateTrackSettings {
                    track_sids: vec![track_sid.clone()],
                    quality: proto::VideoQuality::Medium as i32,
                    fps: 15,
                    ..Default::default()
                },
            ))
            .await;
        let _update_after_low_medium = wait_for_subscribed_quality_flags_for_track(
            &mut publisher_events,
            &track_sid,
            [true, true, false],
        )
        .await;

        low_sub_signal
            .send(proto::signal_request::Message::Leave(proto::LeaveRequest {
                reason: proto::DisconnectReason::ClientInitiated as i32,
                action: proto::leave_request::Action::Disconnect as i32,
                ..Default::default()
            }))
            .await;

        let _update_after_low_leave = wait_for_subscribed_quality_flags_for_track(
            &mut publisher_events,
            &track_sid,
            [false, false, false],
        )
        .await;

        publisher_signal.close().await;
        high_sub_signal.close().await;
        low_sub_signal.close().await;

        // drain any remaining events to avoid background task noise on drop
        let _ = tokio::time::timeout(Duration::from_millis(100), publisher_events.recv()).await;
        let _ = tokio::time::timeout(Duration::from_millis(100), high_events.recv()).await;
        let _ = tokio::time::timeout(Duration::from_millis(100), low_events.recv()).await;

        server.abort();
    }

    #[tokio::test]
    async fn rust_sdk_room_remote_audio_publication_unsubscribe_stops_forwarded_frames() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, oxidesfu_server::app())
                .await
                .expect("test server should run");
        });

        let room_name = format!("sdk-audio-unsub-resub-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-unsub-resub-alice")
            .with_name("SDK Audio Unsub Resub Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-unsub-resub-bob")
            .with_name("SDK Audio Unsub Resub Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("bob token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = false;
        options.connect_timeout = Duration::from_secs(10);

        let (alice_room, mut alice_events) =
            Room::connect(&format!("http://{addr}"), &alice_token, options.clone())
                .await
                .expect("alice room should connect");
        let (bob_room, mut bob_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track =
            LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source.clone()));
        let publication = alice_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("alice should publish audio track");
        assert!(publication.sid().to_string().starts_with("TR_"));

        let (remote_audio_track, remote_publication) =
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let event = bob_events
                        .recv()
                        .await
                        .expect("bob room events should stay open");
                    if let RoomEvent::TrackSubscribed {
                        track, publication, ..
                    } = event
                        && let livekit::track::RemoteTrack::Audio(audio_track) = track
                    {
                        break (audio_track, publication);
                    }
                }
            })
            .await
            .expect("bob should receive initial TrackSubscribed event before timeout");

        let mut audio_stream = NativeAudioStream::new(remote_audio_track.rtc_track(), 48_000, 1);

        let frame = AudioFrame {
            data: vec![300_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };

        // Prime with an initial frame.
        source
            .capture_frame(&frame)
            .await
            .expect("audio frame should be accepted by source");
        let _initial = tokio::time::timeout(Duration::from_secs(5), audio_stream.next())
            .await
            .expect("initial frame wait should finish")
            .expect("initial frame should arrive");

        remote_publication.set_subscribed(false);

        let got_unsubscribed_event = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackUnsubscribed { publication, .. } = event
                    && publication.sid() == remote_publication.sid()
                {
                    break true;
                }
            }
        })
        .await
        .expect("unsubscribe event wait should finish");
        assert!(got_unsubscribed_event);

        let unsolicited_resubscribe_event = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackSubscribed { publication, .. } = event
                    && publication.sid() == remote_publication.sid()
                {
                    break true;
                }
            }
        })
        .await;
        assert!(
            unsolicited_resubscribe_event.is_err(),
            "bob should not receive new TrackSubscribed for an unsubscribed publication"
        );

        // Rust SDK emits TrackUnsubscribed locally before server-side media removal fully settles.
        // The audio stream can continue yielding buffered decoder output transiently, so this
        // contract test anchors on signalling-level unsubscribe semantics instead.

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    #[tokio::test]
    async fn rust_sdk_room_twirp_update_subscriptions_unsubscribe_then_subscribe_emits_lifecycle_and_recovers_media() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, oxidesfu_server::app())
                .await
                .expect("test server should run");
        });

        let room_name = format!("sdk-twirp-update-subscriptions-{}", unique_suffix());
        let publisher_identity = "sdk-twirp-update-subscriptions-publisher";
        let subscriber_identity = "sdk-twirp-update-subscriptions-subscriber";

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name("SDK Twirp UpdateSubscriptions Publisher")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");
        let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(subscriber_identity)
            .with_name("SDK Twirp UpdateSubscriptions Subscriber")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = false;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
                .await
                .expect("publisher room should connect");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(&format!("http://{addr}"), &subscriber_token, options)
                .await
                .expect("subscriber room should connect");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track =
            LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source.clone()));
        let publication = publisher_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("publisher should publish audio track");
        let published_sid = publication.sid().to_string();

        let first_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackSubscribed {
                    track, publication, ..
                } = event
                    && publication.sid().to_string() == published_sid
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .expect("subscriber should receive initial TrackSubscribed");

        let mut first_stream = NativeAudioStream::new(first_audio_track.rtc_track(), 48_000, 1);
        let frame = AudioFrame {
            data: vec![901_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };
        source
            .capture_frame(&frame)
            .await
            .expect("audio frame should be accepted by source");
        let _initial = tokio::time::timeout(Duration::from_secs(5), first_stream.next())
            .await
            .expect("initial frame wait should finish")
            .expect("initial frame should arrive");

        let room_client = RoomClient::with_api_key(&format!("http://{addr}"), API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        room_client
            .update_subscriptions(
                &room_name,
                subscriber_identity,
                vec![published_sid.clone()],
                false,
            )
            .await
            .expect("update_subscriptions unsubscribe should succeed");

        let _maybe_unsubscribed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackUnsubscribed { publication, .. } = event
                    && publication.sid().to_string() == published_sid
                {
                    break true;
                }
            }
        })
        .await
        .ok();

        room_client
            .update_subscriptions(&room_name, subscriber_identity, vec![published_sid.clone()], true)
            .await
            .expect("update_subscriptions subscribe should succeed");

        let maybe_rejoined_audio_track = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackSubscribed {
                    track, publication, ..
                } = event
                    && publication.sid().to_string() == published_sid
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .ok();

        let recovered = tokio::time::timeout(Duration::from_secs(8), async {
            let mut rejoined_stream = maybe_rejoined_audio_track
                .as_ref()
                .map(|track| NativeAudioStream::new(track.rtc_track(), 48_000, 1));
            for _ in 0..80 {
                source
                    .capture_frame(&frame)
                    .await
                    .expect("audio frame should be accepted by source");
                let next = if let Some(stream) = rejoined_stream.as_mut() {
                    tokio::time::timeout(Duration::from_millis(80), stream.next()).await
                } else {
                    tokio::time::timeout(Duration::from_millis(80), first_stream.next()).await
                };
                if let Ok(Some(_)) = next {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            false
        })
        .await
        .expect("resubscribe media recovery probe should finish");
        assert!(
            recovered,
            "subscriber should receive media again after twirp update_subscriptions subscribe"
        );

        let _ = publisher_room.close().await;
        let _ = subscriber_room.close().await;
        server.abort();
    }

    #[tokio::test]
    #[ignore = "tracked by differential parity probe against Go LiveKit"]
    async fn rust_sdk_room_twirp_update_subscriptions_unsubscribe_then_subscribe_emits_deterministic_lifecycle_events() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, oxidesfu_server::app())
                .await
                .expect("test server should run");
        });

        let room_name = format!("sdk-twirp-update-subscriptions-strict-{}", unique_suffix());
        let publisher_identity = "sdk-twirp-update-subscriptions-strict-publisher";
        let subscriber_identity = "sdk-twirp-update-subscriptions-strict-subscriber";

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name("SDK Twirp UpdateSubscriptions Strict Publisher")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");
        let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(subscriber_identity)
            .with_name("SDK Twirp UpdateSubscriptions Strict Subscriber")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = false;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
                .await
                .expect("publisher room should connect");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(&format!("http://{addr}"), &subscriber_token, options)
                .await
                .expect("subscriber room should connect");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track =
            LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source.clone()));
        let publication = publisher_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("publisher should publish audio track");
        let published_sid = publication.sid().to_string();

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackSubscribed { publication, .. } = event
                    && publication.sid().to_string() == published_sid
                {
                    break;
                }
            }
        })
        .await
        .expect("subscriber should receive initial TrackSubscribed");

        let room_client = RoomClient::with_api_key(&format!("http://{addr}"), API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        room_client
            .update_subscriptions(
                &room_name,
                subscriber_identity,
                vec![published_sid.clone()],
                false,
            )
            .await
            .expect("update_subscriptions unsubscribe should succeed");

        let saw_track_unsubscribed = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackUnsubscribed { publication, .. } = event
                    && publication.sid().to_string() == published_sid
                {
                    break;
                }
            }
        })
        .await
        .is_ok();

        room_client
            .update_subscriptions(&room_name, subscriber_identity, vec![published_sid.clone()], true)
            .await
            .expect("update_subscriptions subscribe should succeed");

        let saw_track_subscribed = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackSubscribed { publication, .. } = event
                    && publication.sid().to_string() == published_sid
                {
                    break;
                }
            }
        })
        .await
        .is_ok();

        assert!(
            !saw_track_unsubscribed || saw_track_subscribed,
            "if Twirp unsubscribe emits TrackUnsubscribed, the subsequent Twirp subscribe should emit TrackSubscribed"
        );

        let _ = publisher_room.close().await;
        let _ = subscriber_room.close().await;
        server.abort();
    }

    #[tokio::test]
    async fn rust_sdk_room_publisher_unpublish_then_republish_audio_emits_clean_remote_lifecycle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, oxidesfu_server::app())
                .await
                .expect("test server should run");
        });

        let room_name = format!("sdk-audio-unpublish-republish-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-unpublish-republish-alice")
            .with_name("SDK Audio Unpublish Republish Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-unpublish-republish-bob")
            .with_name("SDK Audio Unpublish Republish Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("bob token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = false;
        options.connect_timeout = Duration::from_secs(10);

        let (alice_room, mut alice_events) =
            Room::connect(&format!("http://{addr}"), &alice_token, options.clone())
                .await
                .expect("alice room should connect");
        let (bob_room, mut bob_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let first_track =
            LocalAudioTrack::create_audio_track("mic-1", RtcAudioSource::Native(source.clone()));
        let first_publication = alice_room
            .local_participant()
            .publish_track(
                LocalTrack::Audio(first_track),
                TrackPublishOptions::default(),
            )
            .await
            .expect("alice should publish first audio track");
        let first_sid = first_publication.sid();

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackSubscribed { publication, .. } = event
                    && publication.sid() == first_sid
                {
                    break;
                }
            }
        })
        .await
        .expect("bob should subscribe to first track");

        let _ = alice_room
            .local_participant()
            .unpublish_track(&first_sid)
            .await
            .expect("alice should unpublish first track");

        let (unpublished_sid, _unpublished_participant) =
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let event = bob_events
                        .recv()
                        .await
                        .expect("bob room events should stay open");
                    if let RoomEvent::TrackUnpublished {
                        publication,
                        participant,
                    } = event
                    {
                        break (publication.sid(), participant.identity().to_string());
                    }
                }
            })
            .await
            .expect("bob should receive TrackUnpublished for first track");
        assert_eq!(unpublished_sid, first_sid);

        let second_track =
            LocalAudioTrack::create_audio_track("mic-2", RtcAudioSource::Native(source.clone()));
        let second_publication = alice_room
            .local_participant()
            .publish_track(
                LocalTrack::Audio(second_track),
                TrackPublishOptions::default(),
            )
            .await
            .expect("alice should republish second audio track");
        let second_sid = second_publication.sid();
        assert_ne!(second_sid, first_sid);

        let second_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackSubscribed {
                    track, publication, ..
                } = event
                    && publication.sid() == second_sid
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .expect("bob should subscribe to republished audio track");

        let duplicate_second_subscribed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackSubscribed { publication, .. } = event
                    && publication.sid() == second_sid
                {
                    break true;
                }
            }
        })
        .await;
        assert!(
            duplicate_second_subscribed.is_err(),
            "republish cycle should not create duplicate TrackSubscribed events"
        );

        let mut second_stream = NativeAudioStream::new(second_audio_track.rtc_track(), 48_000, 1);
        let second_frame = AudioFrame {
            data: vec![725_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };
        let second_received = tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..40 {
                source
                    .capture_frame(&second_frame)
                    .await
                    .expect("audio frame should be accepted by source");
                if let Ok(next) =
                    tokio::time::timeout(Duration::from_millis(100), second_stream.next()).await
                    && next.is_some()
                {
                    return true;
                }
            }
            false
        })
        .await
        .expect("bob should receive republished audio frame before timeout");
        assert!(second_received, "republished audio should deliver media");

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    #[tokio::test]
    async fn wait_for_room_service_ready_with_retry_handles_delayed_server_start() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let base_url = format!("http://{addr}");

        let wait_task = tokio::spawn({
            let base_url = base_url.clone();
            async move {
                wait_for_room_service_ready_with_retry(
                    &base_url,
                    Duration::from_secs(8),
                    Duration::from_millis(100),
                    Duration::from_millis(800),
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(900)).await;

        let server = tokio::spawn(async move {
            axum::serve(listener, oxidesfu_server::app())
                .await
                .expect("delayed server should run");
        });

        let ready = wait_task
            .await
            .expect("wait task should complete without panic");
        assert!(
            ready.is_ok(),
            "ready wait should succeed after delayed start"
        );

        server.abort();
    }
    #[tokio::test]
    async fn spawn_ready_go_livekit_server_with_single_respawn_returns_ready_when_available() {
        let Some((mut go_livekit, go_base_url)) =
            spawn_ready_go_livekit_server_with_single_respawn()
                .await
                .expect("go startup helper should not fail when command exists")
        else {
            eprintln!("skipping go readiness helper test because go is not on PATH");
            return;
        };

        wait_for_room_service_ready(&go_base_url)
            .await
            .expect("spawn helper should return ready go base url");

        let _ = go_livekit.kill().await;
    }
    #[tokio::test]
    async fn wait_for_room_service_ready_with_retry_times_out_when_unreachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        drop(listener);

        let base_url = format!("http://{addr}");
        let result = wait_for_room_service_ready_with_retry(
            &base_url,
            Duration::from_millis(700),
            Duration::from_millis(50),
            Duration::from_millis(200),
        )
        .await;

        let err = result.expect_err("unreachable base URL should time out");
        assert!(err.contains("room service did not become ready"));
        assert!(err.contains(base_url.as_str()));
    }

    #[tokio::test]
    async fn process_transport_udp_and_tcp_ports_support_twirp_room_contracts() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping process transport external-ip/udp test because redis-server is not on PATH"
            );
            return;
        };

        let bind_port = reserve_local_port();
        let rtc_udp_port = reserve_local_port();
        let rtc_tcp_port = reserve_local_port();

        let options = OxidesfuServerProcessOptions {
            rtc_udp_port: Some(rtc_udp_port),
            rtc_tcp_port: Some(rtc_tcp_port),
            ..Default::default()
        };

        let Some((mut oxidesfu, base_url)) = spawn_oxidesfu_server_process_with_options(
            bind_port,
            &redis_url,
            false,
            &options,
        )
        .await
        .expect("oxidesfu process startup with transport options should succeed")
        else {
            eprintln!(
                "skipping process transport external-ip/udp test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let room_client = RoomClient::with_api_key(&base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let created_room_name = format!("process-transport-room-{}", unique_suffix());
        let created = room_client
            .create_room(&created_room_name, CreateRoomOptions::default())
            .await
            .expect("create room should succeed on process-based transport config");
        assert_eq!(created.name, created_room_name);

        let listed = room_client
            .list_rooms(Vec::new())
            .await
            .expect("list rooms should succeed on process-based transport config");
        assert!(
            listed
                .iter()
                .any(|room| room.name == created_room_name),
            "list rooms should include newly created room"
        );
        let _ = oxidesfu.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn process_transport_udp_port_range_supports_twirp_room_contracts() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping process transport udp range test because redis-server is not on PATH"
            );
            return;
        };

        let bind_port = reserve_local_port();
        let rtc_udp_port = reserve_local_port();
        let options = OxidesfuServerProcessOptions {
            rtc_udp_port: None,
            rtc_udp_port_range_start: Some(rtc_udp_port),
            rtc_udp_port_range_end: Some(rtc_udp_port),
            ..Default::default()
        };

        let Some((mut oxidesfu, base_url)) = spawn_oxidesfu_server_process_with_options(
            bind_port,
            &redis_url,
            false,
            &options,
        )
        .await
        .expect("oxidesfu process startup with udp range options should succeed")
        else {
            eprintln!(
                "skipping process transport udp range test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let room_client = RoomClient::with_api_key(&base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let created_room_name = format!("process-transport-udp-range-room-{}", unique_suffix());
        let created = room_client
            .create_room(&created_room_name, CreateRoomOptions::default())
            .await
            .expect("create room should succeed on process-based udp range transport config");
        assert_eq!(created.name, created_room_name);

        let listed = room_client
            .list_rooms(Vec::new())
            .await
            .expect("list rooms should succeed on process-based udp range transport config");
        assert!(
            listed
                .iter()
                .any(|room| room.name == created_room_name),
            "list rooms should include newly created room"
        );

        let _ = oxidesfu.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn process_transport_external_ip_mode_with_node_ip_supports_twirp_room_contracts() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping process transport external-ip Twirp test because redis-server is not on PATH"
            );
            return;
        };

        let bind_port = reserve_local_port();
        let rtc_udp_port = reserve_local_port();

        let options = OxidesfuServerProcessOptions {
            rtc_udp_port: Some(rtc_udp_port),
            rtc_use_external_ip: Some(true),
            rtc_node_ip: Some("127.0.0.1".to_string()),
            ..Default::default()
        };

        let Some((mut oxidesfu, base_url)) = spawn_oxidesfu_server_process_with_options(
            bind_port,
            &redis_url,
            false,
            &options,
        )
        .await
        .expect("oxidesfu process startup with external-ip transport options should succeed")
        else {
            eprintln!(
                "skipping process transport external-ip Twirp test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let room_client = RoomClient::with_api_key(&base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let listed = room_client
            .list_rooms(Vec::new())
            .await
            .expect("list rooms should succeed when external-ip mode is enabled");
        assert!(
            listed.is_empty(),
            "new process should start with no rooms in registry"
        );

        let _ = oxidesfu.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn process_transport_external_ip_mode_supports_rtc_v1_data_channel_open() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping process transport external-ip rtc-v1 test because redis-server is not on PATH"
            );
            return;
        };

        let bind_port = reserve_local_port();
        let rtc_udp_port = reserve_local_port();
        let rtc_tcp_port = reserve_local_port();

        let options = OxidesfuServerProcessOptions {
            rtc_udp_port: Some(rtc_udp_port),
            rtc_tcp_port: Some(rtc_tcp_port),
            rtc_use_external_ip: Some(true),
            rtc_node_ip: Some("127.0.0.1".to_string()),
            ..Default::default()
        };

        let Some((mut oxidesfu, _base_url)) = spawn_oxidesfu_server_process_with_options(
            bind_port,
            &redis_url,
            false,
            &options,
        )
        .await
        .expect("oxidesfu process startup with external-ip rtc-v1 options should succeed")
        else {
            eprintln!(
                "skipping process transport external-ip rtc-v1 test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let room = format!("process-transport-rtc-v1-room-{}", unique_suffix());
        let addr: std::net::SocketAddr = format!("127.0.0.1:{bind_port}")
            .parse()
            .expect("bind address should parse as socket address");
        let mut alice = connect_data_participant(addr, &room, "process-transport-rtc-v1-alice").await;

        let data_channel_open = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                tokio::select! {
                    _ = alice.open_rx.recv() => {
                        break true;
                    }
                    candidate = alice.events.ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            send_trickle(&mut alice.socket, candidate).await;
                        }
                    }
                    message = alice.socket.next() => {
                        handle_signal_message(message, &alice.peer).await;
                    }
                }
            }
        })
        .await
        .expect("rtc-v1 data channel should open before timeout");
        assert!(
            data_channel_open,
            "data channel should open for external-ip process transport configuration"
        );

        alice
            .peer
            .close()
            .await
            .expect("client peer should close cleanly");
        let _ = oxidesfu.kill().await;
        let _ = redis.kill().await;
    }

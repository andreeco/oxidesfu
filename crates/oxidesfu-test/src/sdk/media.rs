use super::*;

    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::add_track_response_returns_track_published_and_updates_participant_tracks
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_publish_audio_track_emits_remote_track_published() {
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

        let room_name = format!("sdk-audio-track-publish-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-track-alice")
            .with_name("SDK Audio Track Alice")
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
            .with_identity("sdk-audio-track-bob")
            .with_name("SDK Audio Track Bob")
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
        options.single_peer_connection = true;
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
        let track = LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source));
        let publication = alice_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("alice should publish audio track");

        assert!(!publication.sid().to_string().is_empty());

        let (remote_publication, remote_participant) =
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let event = bob_events
                        .recv()
                        .await
                        .expect("bob room events should stay open");
                    if let RoomEvent::TrackPublished {
                        publication,
                        participant,
                    } = event
                    {
                        break (publication, participant);
                    }
                }
            })
            .await
            .expect("bob should receive remote TrackPublished event before timeout");

        assert_eq!(
            remote_participant.identity().to_string(),
            "sdk-audio-track-alice"
        );
        assert_eq!(remote_publication.name(), "mic");

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::track_subscribed_signal_emission_requires_distinct_known_subscriber_identity
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_publish_audio_track_emits_remote_track_subscribed() {
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

        let room_name = format!("sdk-audio-track-subscribe-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-subscribe-alice")
            .with_name("SDK Audio Subscribe Alice")
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
            .with_identity("sdk-audio-subscribe-bob")
            .with_name("SDK Audio Subscribe Bob")
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
        let published_sid = publication.sid().to_string();
        assert!(published_sid.starts_with("TR_"));

        let frame = AudioFrame {
            data: vec![0_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };
        for _ in 0..6 {
            source
                .capture_frame(&frame)
                .await
                .expect("audio frame should be accepted by source");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let (remote_publication, remote_participant) =
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let event = bob_events
                        .recv()
                        .await
                        .expect("bob room events should stay open");
                    if let RoomEvent::TrackSubscribed {
                        publication,
                        participant,
                        ..
                    } = event
                    {
                        break (publication, participant);
                    }
                }
            })
            .await
            .expect("bob should receive TrackSubscribed event before timeout");

        assert_eq!(
            remote_participant.identity().to_string(),
            "sdk-audio-subscribe-alice"
        );
        assert_eq!(remote_publication.sid().to_string(), published_sid);
        assert_eq!(remote_publication.name(), "mic");

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: direct minimal WebRTC media-frame black-box parity for single-PC is not yet complete.
    // REMOVAL_PLAN: delete only after direct media-plane frame delivery tests land.
    #[tokio::test]
    async fn rust_sdk_room_publish_audio_track_delivers_remote_audio_frames_single_pc_v1() {
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

        let room_name = format!("sdk-audio-track-frames-single-pc-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-frames-single-pc-alice")
            .with_name("SDK Audio Frames Single PC Alice")
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
            .with_identity("sdk-audio-frames-single-pc-bob")
            .with_name("SDK Audio Frames Single PC Bob")
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
        options.single_peer_connection = true;
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

        let sent_frame = AudioFrame {
            data: vec![100_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };

        let remote_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                source
                    .capture_frame(&sent_frame)
                    .await
                    .expect("audio frame should be accepted while waiting for subscription");

                let Ok(Some(event)) = tokio::time::timeout(
                    Duration::from_millis(120),
                    bob_events.recv(),
                )
                .await
                else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                };

                if let RoomEvent::TrackSubscribed { track, .. } = event
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .expect("bob should receive remote audio TrackSubscribed event before timeout");

        let mut audio_stream = NativeAudioStream::new(remote_audio_track.rtc_track(), 48_000, 1);

        let received_frame = tokio::time::timeout(Duration::from_secs(10), async {
            for _ in 0..100 {
                source
                    .capture_frame(&sent_frame)
                    .await
                    .expect("audio frame should be accepted by source");
                if let Ok(frame) =
                    tokio::time::timeout(Duration::from_millis(80), audio_stream.next()).await
                    && let Some(frame) = frame
                {
                    return Some(frame);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            None
        })
        .await
        .expect("audio stream receive loop should finish")
        .expect("bob should receive at least one remote audio frame from alice in single-PC mode");

        assert_eq!(received_frame.sample_rate, 48_000);
        assert_eq!(received_frame.num_channels, 1);
        assert!(!received_frame.data.is_empty());

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: direct minimal WebRTC media-frame black-box parity is not yet complete.
    // REMOVAL_PLAN: delete only after direct media-plane frame delivery tests land.
    #[tokio::test]
    async fn rust_sdk_room_publish_audio_track_delivers_remote_audio_frames() {
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

        let room_name = format!("sdk-audio-track-frames-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-frames-alice")
            .with_name("SDK Audio Frames Alice")
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
            .with_identity("sdk-audio-frames-bob")
            .with_name("SDK Audio Frames Bob")
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

        let remote_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackSubscribed { track, .. } = event
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .expect("bob should receive remote audio TrackSubscribed event before timeout");

        let mut audio_stream = NativeAudioStream::new(remote_audio_track.rtc_track(), 48_000, 1);

        let sent_frame = AudioFrame {
            data: vec![100_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };

        let received_frame = tokio::time::timeout(Duration::from_secs(10), async {
            for _ in 0..100 {
                source
                    .capture_frame(&sent_frame)
                    .await
                    .expect("audio frame should be accepted by source");
                if let Ok(frame) =
                    tokio::time::timeout(Duration::from_millis(80), audio_stream.next()).await
                    && let Some(frame) = frame
                {
                    return Some(frame);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            None
        })
        .await
        .expect("audio stream receive loop should finish")
        .expect("bob should receive at least one remote audio frame from alice");

        assert_eq!(received_frame.sample_rate, 48_000);
        assert_eq!(received_frame.num_channels, 1);
        assert!(!received_frame.data.is_empty());

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: direct signaling + media recovery parity test for unsubscribe/resubscribe is not yet complete.
    // REMOVAL_PLAN: delete only after direct unsubscribe/resubscribe media recovery tests land.
    #[tokio::test]
    async fn rust_sdk_room_remote_audio_publication_unsubscribe_then_resubscribe_recovers_media_once()
     {
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

        let room_name = format!("sdk-audio-unsub-resub-cycle-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-unsub-resub-cycle-alice")
            .with_name("SDK Audio Unsub Resub Cycle Alice")
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
            .with_identity("sdk-audio-unsub-resub-cycle-bob")
            .with_name("SDK Audio Unsub Resub Cycle Bob")
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
        let published_sid = publication.sid();

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
                        && publication.sid() == published_sid
                        && let livekit::track::RemoteTrack::Audio(audio_track) = track
                    {
                        break (audio_track, publication);
                    }
                }
            })
            .await
            .expect("bob should receive initial TrackSubscribed event before timeout");

        let mut initial_stream = NativeAudioStream::new(remote_audio_track.rtc_track(), 48_000, 1);
        let frame = AudioFrame {
            data: vec![450_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };

        source
            .capture_frame(&frame)
            .await
            .expect("audio frame should be accepted by source");
        let _initial = tokio::time::timeout(Duration::from_secs(5), initial_stream.next())
            .await
            .expect("initial frame wait should finish")
            .expect("initial frame should arrive");

        remote_publication.set_subscribed(false);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackUnsubscribed { publication, .. } = event
                    && publication.sid() == published_sid
                {
                    break;
                }
            }
        })
        .await
        .expect("unsubscribe event wait should finish");

        remote_publication.set_subscribed(true);

        let maybe_resubscribed_audio_track = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackSubscribed {
                    track, publication, ..
                } = event
                    && publication.sid() == published_sid
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .ok();

        if maybe_resubscribed_audio_track.is_some() {
            let duplicate_track_subscribed = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let event = bob_events
                        .recv()
                        .await
                        .expect("bob room events should stay open");
                    if let RoomEvent::TrackSubscribed { publication, .. } = event
                        && publication.sid() == published_sid
                    {
                        break true;
                    }
                }
            })
            .await;
            assert!(
                duplicate_track_subscribed.is_err(),
                "resubscribe cycle should not emit duplicate TrackSubscribed events for this publication"
            );
        }

        let recovered = tokio::time::timeout(Duration::from_secs(5), async {
            let mut resubscribed_stream = maybe_resubscribed_audio_track
                .as_ref()
                .map(|track| NativeAudioStream::new(track.rtc_track(), 48_000, 1));
            for _ in 0..40 {
                source
                    .capture_frame(&frame)
                    .await
                    .expect("audio frame should be accepted by source");
                let next = if let Some(stream) = resubscribed_stream.as_mut() {
                    tokio::time::timeout(Duration::from_millis(100), stream.next()).await
                } else {
                    tokio::time::timeout(Duration::from_millis(100), initial_stream.next()).await
                };
                if let Ok(next) = next
                    && next.is_some()
                {
                    return true;
                }
            }
            false
        })
        .await
        .expect("resubscribe media wait should finish");
        assert!(
            recovered,
            "bob should receive media again after resubscribe"
        );

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: direct minimal WebRTC reconnect+resubscribe media-frame parity is not yet complete.
    // REMOVAL_PLAN: delete only after direct media reconnect tests land.
    #[tokio::test]
    async fn rust_sdk_room_audio_track_reconnect_can_resubscribe_and_receive_frame() {
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

        let room_name = format!("sdk-audio-track-reconnect-{}", unique_suffix());
        let result = run_reconnect_resubscribe_audio_track(
            &format!("http://{addr}"),
            &room_name,
            "sdk-audio-track-reconnect-alice",
            "sdk-audio-track-reconnect-bob",
            false,
        )
        .await;

        assert!(result.received_before_reconnect);
        assert!(result.received_after_reconnect);

        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: direct single-PC media reconnect parity is not yet complete.
    // REMOVAL_PLAN: delete only after direct media reconnect tests land.
    #[tokio::test]
    async fn rust_sdk_room_audio_track_reconnect_can_resubscribe_and_receive_frame_single_pc_v1() {
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

        let room_name = format!("sdk-audio-track-reconnect-single-pc-{}", unique_suffix());
        let result = run_reconnect_resubscribe_audio_track(
            &format!("http://{addr}"),
            &room_name,
            "sdk-audio-track-reconnect-single-pc-alice",
            "sdk-audio-track-reconnect-single-pc-bob",
            true,
        )
        .await;

        assert!(result.received_before_reconnect);
        assert!(result.received_after_reconnect);

        server.abort();
    }

    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: direct minimal WebRTC multi-subscriber media-frame parity is not yet complete.
    // REMOVAL_PLAN: delete only after direct multi-subscriber media-frame delivery tests land.
    #[tokio::test]
    async fn rust_sdk_room_publish_audio_track_delivers_frames_to_multiple_subscribers() {
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

        let room_name = format!("sdk-audio-track-multi-subscriber-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-multi-alice")
            .with_name("SDK Audio Multi Alice")
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
            .with_identity("sdk-audio-multi-bob")
            .with_name("SDK Audio Multi Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("bob token should encode");
        let carol_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-audio-multi-carol")
            .with_name("SDK Audio Multi Carol")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("carol token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = false;
        options.connect_timeout = Duration::from_secs(10);

        let (alice_room, mut alice_events) =
            Room::connect(&format!("http://{addr}"), &alice_token, options.clone())
                .await
                .expect("alice room should connect");
        let (bob_room, mut bob_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                .await
                .expect("bob room should connect");
        let (carol_room, mut carol_events) =
            Room::connect(&format!("http://{addr}"), &carol_token, options)
                .await
                .expect("carol room should connect");
        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;
        wait_for_room_connected(&mut carol_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track =
            LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source.clone()));
        let publication = alice_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("alice should publish audio track");
        assert!(publication.sid().to_string().starts_with("TR_"));

        let bob_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::TrackSubscribed { track, .. } = event
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .expect("bob should receive remote audio TrackSubscribed event before timeout");

        let carol_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = carol_events
                    .recv()
                    .await
                    .expect("carol room events should stay open");
                if let RoomEvent::TrackSubscribed { track, .. } = event
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .expect("carol should receive remote audio TrackSubscribed event before timeout");

        let mut bob_stream = NativeAudioStream::new(bob_audio_track.rtc_track(), 48_000, 1);
        let mut carol_stream = NativeAudioStream::new(carol_audio_track.rtc_track(), 48_000, 1);

        let sent_frame = AudioFrame {
            data: vec![200_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };

        let (bob_got_frame, carol_got_frame) =
            tokio::time::timeout(Duration::from_secs(12), async {
                let mut bob_got_frame = false;
                let mut carol_got_frame = false;

                for _ in 0..150 {
                    source
                        .capture_frame(&sent_frame)
                        .await
                        .expect("audio frame should be accepted by source");

                    if !bob_got_frame
                        && let Ok(frame) =
                            tokio::time::timeout(Duration::from_millis(40), bob_stream.next()).await
                        && frame.is_some()
                    {
                        bob_got_frame = true;
                    }
                    if !carol_got_frame
                        && let Ok(frame) =
                            tokio::time::timeout(Duration::from_millis(40), carol_stream.next())
                                .await
                        && frame.is_some()
                    {
                        carol_got_frame = true;
                    }

                    if bob_got_frame && carol_got_frame {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }

                (bob_got_frame, carol_got_frame)
            })
            .await
            .expect("receive loop should finish before timeout");

        assert!(
            bob_got_frame,
            "bob should receive remote audio frame from alice"
        );
        assert!(
            carol_got_frame,
            "carol should receive remote audio frame from alice"
        );

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        let _ = carol_room.close().await;
        server.abort();
    }

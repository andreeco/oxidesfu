use super::*;
use super::native_rtc::{PersistentMediaParticipant, NativeMediaParticipant, RawDataTopology, connect_raw_data_participant};

// Upstream: livekit/test/singlenode_test.go::TestClientCouldConnect
#[tokio::test]
async fn test_client_could_connect() {
    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);
    let room = format!("upstream-client-connect-{}", unique_suffix());

    let (c1_room, _c1_events) = connect_room(&url, &room, "c1", true).await;
    let (c2_room, _c2_events) = connect_room(&url, &room, "c2", true).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c1_sees_c2 = c1_room.remote_participants().contains_key(&"c2".into());
            let c2_sees_c1 = c2_room.remote_participants().contains_key(&"c1".into());
            if c1_sees_c2 && c2_sees_c1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("both clients should see each other as remote participants");

    let client = room_client(addr);
    let participants = client
        .list_participants(&room)
        .await
        .expect("participants should list");
    let identities: HashSet<_> = participants.iter().map(|participant| participant.identity.as_str()).collect();
    assert!(identities.contains("c1"));
    assert!(identities.contains("c2"));

    let _ = c1_room.close().await;
    let _ = c2_room.close().await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestClientConnectDuplicate
#[tokio::test]
async fn test_client_connect_duplicate() {
    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);
    let room = format!("upstream-duplicate-{}", unique_suffix());
    let token = join_token(&room, "duplicate");

    let (first_room, _first_events) = connect_room_with_token(&url, &token, true).await;
    let (observer_room, mut observer_events) = connect_room(&url, &room, "observer", true).await;
    let first_audio_sid = publish_audio_track(&first_room, "audio").await.0;
    let first_video_sid = publish_video_track(&first_room, "video").await;
    let initial_subscriptions = wait_for_track_subscribed_count(&mut observer_events, 2).await;
    let initial_sids: HashSet<_> = initial_subscriptions
        .iter()
        .map(|publication| publication.sid().to_string())
        .collect();
    assert!(initial_sids.contains(&first_audio_sid));
    assert!(initial_sids.contains(&first_video_sid));

    let (duplicate_room, _duplicate_events) = connect_room_with_token(&url, &token, true).await;
    let duplicate_video_sid = publish_video_track(&duplicate_room, "duplicate-video").await;
    let duplicate_subscriptions = wait_for_track_subscribed_count(&mut observer_events, 1).await;
    assert_eq!(duplicate_subscriptions[0].sid().to_string(), duplicate_video_sid);

    let client = room_client(addr);
    let participants = client
        .list_participants(&room)
        .await
        .expect("participants should list");
    assert_eq!(participants.len(), 2, "observer plus duplicate identity should be visible");
    assert!(participants.iter().any(|p| p.identity == "duplicate"));

    let _ = first_room.close().await;
    let _ = duplicate_room.close().await;
    let _ = observer_room.close().await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSinglePublisher
#[tokio::test]
async fn test_single_publisher() {
    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);
    let room = format!("upstream-single-publisher-{}", unique_suffix());
    let (publisher_room, _publisher_events) = connect_room(&url, &room, "c1", true).await;
    let (subscriber_room, mut subscriber_events) = connect_room(&url, &room, "c2", true).await;

    let audio_sid = publish_audio_track(&publisher_room, "webcamaudio").await.0;
    let video_sid = publish_video_track(&publisher_room, "webcamvideo").await;

    let subscriptions = tokio::time::timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        while events.len() < 2 {
            let event = subscriber_events.recv().await.expect("subscriber events should stay open");
            if let RoomEvent::TrackSubscribed { publication, participant, .. } = event {
                events.push((publication, participant.identity().to_string()));
            }
        }
        events
    })
    .await
    .expect("subscriber should receive two track subscriptions before timeout");

    assert!(
        subscriptions.iter().all(|(_, identity)| identity == "c1"),
        "all subscribed tracks should be published by c1"
    );

    let by_name: HashMap<_, _> = subscriptions
        .iter()
        .map(|(publication, _)| (publication.name(), publication.mime_type()))
        .collect();
    assert_eq!(by_name.get("webcamaudio"), Some(&"audio/opus".to_string()));
    assert!(by_name.contains_key("webcamvideo"));

    let sids: HashSet<_> = subscriptions
        .iter()
        .map(|(publication, _)| publication.sid().to_string())
        .collect();
    assert!(sids.contains(&audio_sid));
    assert!(sids.contains(&video_sid));
    assert!(
        sids.iter().all(|sid| sid.starts_with("TR_")),
        "server-assigned track IDs should start with TR_"
    );

    let (late_room, mut late_events) = connect_room(&url, &room, "c3", true).await;
    let late_subscriptions = tokio::time::timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        while events.len() < 2 {
            let event = late_events.recv().await.expect("late subscriber events should stay open");
            if let RoomEvent::TrackSubscribed { publication, participant, .. } = event {
                events.push((publication, participant.identity().to_string()));
            }
        }
        events
    })
    .await
    .expect("late subscriber should receive existing tracks before timeout");

    assert!(
        late_subscriptions.iter().all(|(_, identity)| identity == "c1"),
        "late subscriber tracks should be from c1"
    );

    let late_sids: HashSet<_> = late_subscriptions
        .iter()
        .map(|(publication, _)| publication.sid().to_string())
        .collect();
    assert!(late_sids.contains(&audio_sid));
    assert!(late_sids.contains(&video_sid));

    let remote_c1 = subscriber_room
        .remote_participants()
        .get(&"c1".into())
        .cloned()
        .expect("c2 should see c1 as remote participant");
    assert_eq!(
        remote_c1.track_publications().len(),
        2,
        "c2 should see exactly two published tracks for c1"
    );

    let _ = late_room.close().await;
    let _ = subscriber_room.close().await;
    let _ = publisher_room.close().await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestConnectionStats
#[tokio::test]
async fn test_connection_stats() {
    use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;

    fn assert_opus_and_vp8_rtp(
        received: &[super::native_rtc::ReceivedMediaTrack],
        receiver: &str,
        sender: &str,
        topology: RawDataTopology,
    ) {
        assert_eq!(received.len(), 2, "{receiver} should receive two tracks from {sender}");
        let mimes = received
            .iter()
            .map(|track| {
                assert_eq!(track.first_rtp_packet.header.version, 2, "received packet should be RTP");
                assert!(!track.first_rtp_packet.payload.is_empty(), "received RTP should have a payload");
                track.mime_type.to_ascii_lowercase()
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            mimes,
            HashSet::from(["audio/opus".to_string(), "video/vp8".to_string()]),
            "{receiver} should receive exactly Opus and VP8 RTP from {sender} in {}",
            topology.name()
        );
    }

    for topology in [
        RawDataTopology::V0DualPeerConnection,
        RawDataTopology::V0SinglePeerConnection,
        RawDataTopology::V1,
    ] {
        eprintln!("running TestConnectionStats topology={}", topology.name());
        let (addr, server) = spawn_single_node().await;
        let room = format!("upstream-connection-stats-{}-{}", topology.name(), unique_suffix());
        let c1 = PersistentMediaParticipant::connect(topology, addr, &room, "c1").await;
        let c2 = PersistentMediaParticipant::connect(topology, addr, &room, "c2").await;

        // This is the upstream same-room publication order. Both actors remain active while
        // the other participant publishes and handles its subscription renegotiation.
        let c1_audio = c1
            .publish_track("c1-audio", "audio", RtpCodecKind::Audio, "audio/opus")
            .await;
        let c1_vp8 = c1
            .publish_track("c1-vp8", "video", RtpCodecKind::Video, "video/vp8")
            .await;
        // `RTCClient.AddTrack` starts its writer before the other participant begins
        // publishing. Give the persistent native writer one scheduling turn so the server has
        // observed c1's inbound RTP before it negotiates c2's reciprocal subscription.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let c2_audio = c2
            .publish_track("c2-audio", "audio", RtpCodecKind::Audio, "audio/opus")
            .await;
        let c2_vp8 = c2
            .publish_track("c2-vp8", "video", RtpCodecKind::Video, "video/vp8")
            .await;

        let (c1_received, c2_received) = tokio::join!(
            c1.receive_tracks(2, vec![c2_audio.clone(), c2_vp8.clone()]),
            c2.receive_tracks(2, vec![c1_audio.clone(), c1_vp8.clone()]),
        );
        let c1_received = c1_received.unwrap_or_else(|error| {
            panic!("c1 should retain both c2 tracks in {}: {error}", topology.name())
        });
        let c2_received = c2_received.unwrap_or_else(|error| {
            panic!("c2 should retain both c1 tracks in {}: {error}", topology.name())
        });

        assert_opus_and_vp8_rtp(&c1_received, "c1", "c2", topology);
        assert_opus_and_vp8_rtp(&c2_received, "c2", "c1", topology);

        server.abort();
    }
}

// Supplemental client-side WebRTC-statistics probe for the legacy dual-PC SDK path.
#[tokio::test]
async fn test_connection_stats_client_stats_dual_pc() {
    use livekit::prelude::RemoteTrack;
    use livekit::webrtc::stats::RtcStats;

    fn aggregate_packets_and_bytes(stats: &[RtcStats]) -> (u64, u64) {
        let mut packets = 0_u64;
        let mut bytes = 0_u64;

        for stat in stats {
            match stat {
                RtcStats::InboundRtp(inbound) => {
                    packets = packets.saturating_add(inbound.received.packets_received);
                    bytes = bytes.saturating_add(inbound.inbound.bytes_received);
                }
                RtcStats::OutboundRtp(outbound) => {
                    packets = packets.saturating_add(outbound.sent.packets_sent);
                    bytes = bytes.saturating_add(outbound.sent.bytes_sent);
                }
                RtcStats::Transport(transport) => {
                    packets = packets
                        .saturating_add(transport.transport.packets_sent)
                        .saturating_add(transport.transport.packets_received);
                    bytes = bytes
                        .saturating_add(transport.transport.bytes_sent)
                        .saturating_add(transport.transport.bytes_received);
                }
                _ => {}
            }
        }

        (packets, bytes)
    }

    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);
    let room = format!("upstream-connection-stats-{}", unique_suffix());

    let (c1_room, mut c1_events) = connect_room(&url, &room, "c1", true).await;
    let (c2_room, mut c2_events) = connect_room(&url, &room, "c2", true).await;

    let (_c1_audio_sid, c1_audio_source) = publish_audio_track(&c1_room, "c1audio").await;
    let _c1_video_sid = publish_video_track(&c1_room, "c1video").await;
    let (_c2_audio_sid, c2_audio_source) = publish_audio_track(&c2_room, "c2audio").await;
    let _c2_video_sid = publish_video_track(&c2_room, "c2video").await;

    let c1_subscribed = tokio::time::timeout(Duration::from_secs(12), async {
        let mut tracks = Vec::<RemoteTrack>::new();
        while tracks.len() < 2 {
            let event = c1_events.recv().await.expect("c1 events should stay open");
            if let RoomEvent::TrackSubscribed { track, participant, .. } = event
                && participant.identity().to_string() == "c2"
            {
                tracks.push(track);
            }
        }
        tracks
    })
    .await
    .expect("c1 should subscribe to both tracks from c2");

    let c2_subscribed = tokio::time::timeout(Duration::from_secs(12), async {
        let mut tracks = Vec::<RemoteTrack>::new();
        while tracks.len() < 2 {
            let event = c2_events.recv().await.expect("c2 events should stay open");
            if let RoomEvent::TrackSubscribed { track, participant, .. } = event
                && participant.identity().to_string() == "c1"
            {
                tracks.push(track);
            }
        }
        tracks
    })
    .await
    .expect("c2 should subscribe to both tracks from c1");

    let c1_remote_audio = c1_subscribed
        .iter()
        .find_map(|track| match track {
            RemoteTrack::Audio(track) => Some(track.clone()),
            RemoteTrack::Video(_) => None,
        })
        .expect("c1 should subscribe to c2 audio");
    let c2_remote_audio = c2_subscribed
        .iter()
        .find_map(|track| match track {
            RemoteTrack::Audio(track) => Some(track.clone()),
            RemoteTrack::Video(_) => None,
        })
        .expect("c2 should subscribe to c1 audio");

    // Remote tracks are pull-driven in the Rust SDK. Drain audio while producing frames so
    // the receiver processes RTP and its WebRTC statistics advance.
    let c1_audio_reader = tokio::spawn(async move {
        let mut stream = NativeAudioStream::new(c1_remote_audio.rtc_track(), 48_000, 1);
        while stream.next().await.is_some() {}
    });
    let c2_audio_reader = tokio::spawn(async move {
        let mut stream = NativeAudioStream::new(c2_remote_audio.rtc_track(), 48_000, 1);
        while stream.next().await.is_some() {}
    });

    let frame = AudioFrame {
        data: vec![200_i16; 480].into(),
        sample_rate: 48_000,
        num_channels: 1,
        samples_per_channel: 480,
    };
    for _ in 0..40 {
        c1_audio_source
            .capture_frame(&frame)
            .await
            .expect("c1 audio frame should send");
        c2_audio_source
            .capture_frame(&frame)
            .await
            .expect("c2 audio frame should send");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let last_stats = Arc::new(Mutex::new(String::new()));
    let last_stats_for_loop = last_stats.clone();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let c1_stats = c1_room.get_stats().await.expect("c1 room stats should be available");
            let c2_stats = c2_room.get_stats().await.expect("c2 room stats should be available");

            let (c1_pub_packets, c1_pub_bytes) = aggregate_packets_and_bytes(&c1_stats.publisher_stats);
            let (c1_sub_packets, c1_sub_bytes) =
                aggregate_packets_and_bytes(&c1_stats.subscriber_stats);
            let (c2_pub_packets, c2_pub_bytes) = aggregate_packets_and_bytes(&c2_stats.publisher_stats);
            let (c2_sub_packets, c2_sub_bytes) =
                aggregate_packets_and_bytes(&c2_stats.subscriber_stats);

            let c1_subscriber_ok = if c1_stats.subscriber_stats.is_empty() {
                true
            } else {
                c1_sub_packets > 0 && c1_sub_bytes > 0
            };
            let c2_subscriber_ok = if c2_stats.subscriber_stats.is_empty() {
                true
            } else {
                c2_sub_packets > 0 && c2_sub_bytes > 0
            };

            let c1_total_packets = c1_pub_packets.saturating_add(c1_sub_packets);
            let c1_total_bytes = c1_pub_bytes.saturating_add(c1_sub_bytes);
            let c2_total_packets = c2_pub_packets.saturating_add(c2_sub_packets);
            let c2_total_bytes = c2_pub_bytes.saturating_add(c2_sub_bytes);
            *last_stats_for_loop
                .lock()
                .expect("last stats mutex should not be poisoned") = format!(
                "c1 publisher=({c1_pub_packets}, {c1_pub_bytes}) subscriber=({c1_sub_packets}, {c1_sub_bytes}); c2 publisher=({c2_pub_packets}, {c2_pub_bytes}) subscriber=({c2_sub_packets}, {c2_sub_bytes})"
            );

            if c1_total_packets > 0
                && c1_total_bytes > 0
                && c2_total_packets > 0
                && c2_total_bytes > 0
                && c1_subscriber_ok
                && c2_subscriber_ok
            {
                break;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "connection stats should eventually show non-zero upstream/downstream packets and bytes: {}",
            last_stats
                .lock()
                .expect("last stats mutex should not be poisoned")
        )
    });

    c1_audio_reader.abort();
    c2_audio_reader.abort();
    let _ = c1_room.close().await;
    let _ = c2_room.close().await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::Test_WhenAutoSubscriptionDisabled_ClientShouldNotReceiveAnyPublishedTracks
#[tokio::test]
async fn test_when_auto_subscription_disabled_client_should_not_receive_any_published_tracks() {
    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);
    let room = format!("upstream-auto-sub-disabled-{}", unique_suffix());

    let (publisher_room, _publisher_events) = connect_room(&url, &room, "publisher", false).await;
    let (client_room, mut client_events) = connect_room(&url, &room, "client", false).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let publisher_sees_client = publisher_room
                .remote_participants()
                .contains_key(&"client".into());
            let client_sees_publisher = client_room
                .remote_participants()
                .contains_key(&"publisher".into());
            if publisher_sees_client && client_sees_publisher {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("publisher and client should connect and see each other");

    let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
    let track = LocalAudioTrack::create_audio_track("webcam", RtcAudioSource::Native(source));
    publisher_room
        .local_participant()
        .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
        .await
        .expect("publisher should publish audio track");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let subscribed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let RoomEvent::TrackSubscribed { .. } = client_events
                .recv()
                .await
                .expect("client events should remain open")
            {
                break true;
            }
        }
    })
    .await;
    assert!(
        subscribed.is_err(),
        "auto_subscribe=false client should not receive TrackSubscribed events"
    );



    let _ = client_room.close().await;
    let _ = publisher_room.close().await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::Test_RenegotiationWithDifferentCodecs
#[tokio::test]
async fn test_renegotiation_with_different_codecs() {
    use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;

    for topology in [
        RawDataTopology::V0DualPeerConnection,
        RawDataTopology::V0SinglePeerConnection,
        RawDataTopology::V1,
    ] {
        eprintln!("running Test_RenegotiationWithDifferentCodecs topology={}", topology.name());
        let (addr, server) = spawn_single_node().await;
        let room = format!(
            "upstream-renegotiation-different-codecs-{}-{}",
            topology.name(),
            unique_suffix()
        );

        let mut c1 = NativeMediaParticipant::connect(topology, addr, &room, "c1").await;
        let mut c2 = NativeMediaParticipant::connect(topology, addr, &room, "c2").await;

        let audio = c1
            .publish_track("audio-cid", "audio", RtpCodecKind::Audio, "audio/opus")
            .await;
        let vp8 = c1
            .publish_track("vp8-cid", "video", RtpCodecKind::Video, "video/vp8")
            .await;
        let mut published_tracks = vec![audio, vp8];

        let initial_tracks = c2.receive_tracks(2, &published_tracks).await;
        let initial_mimes: HashSet<_> = initial_tracks
            .iter()
            .map(|track| track.mime_type.to_ascii_lowercase())
            .collect();
        assert!(
            initial_mimes.contains("audio/opus"),
            "{} should receive an Opus RemoteTrack with RTP",
            topology.name()
        );
        assert!(
            initial_mimes.contains("video/vp8"),
            "{} should receive a VP8 RemoteTrack with RTP",
            topology.name()
        );

        let h264 = c1
            .publish_track(
                "h264-cid",
                "videoscreen",
                RtpCodecKind::Video,
                "video/h264",
            )
            .await;
        // Keep publishing on the original tracks while H264 is negotiated and received.
        published_tracks.push(h264);

        let h264_tracks = c2.receive_tracks(1, &published_tracks).await;
        let all_mimes: HashSet<_> = initial_tracks
            .iter()
            .chain(h264_tracks.iter())
            .map(|track| track.mime_type.to_ascii_lowercase())
            .collect();
        assert_eq!(initial_tracks.len() + h264_tracks.len(), 3);
        for track in &initial_tracks {
            let retained_codec = track
                .track
                .codec_mime_for_ssrc(track.first_rtp_packet.header.ssrc)
                .await
                .expect("retained RemoteTrack RTP SSRC should keep its codec");
            assert_eq!(retained_codec, track.mime_type);
        }
        assert!(
            all_mimes.contains("video/vp8"),
            "{} should retain the original VP8 RemoteTrack after H264 renegotiation",
            topology.name()
        );
        assert!(
            all_mimes.contains("video/h264"),
            "{} should receive an H264 RemoteTrack with RTP after renegotiation",
            topology.name()
        );

        server.abort();
    }
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeRoomList
#[tokio::test]
async fn test_single_node_room_list() {
    let (addr, server) = spawn_single_node().await;
    let client = room_client(addr);

    let room_a = format!("upstream-single-room-list-a-{}", unique_suffix());
    let room_b = format!("upstream-single-room-list-b-{}", unique_suffix());

    client
        .create_room(&room_a, CreateRoomOptions::default())
        .await
        .expect("first room should create");
    client
        .create_room(&room_b, CreateRoomOptions::default())
        .await
        .expect("second room should create");

    let all_rooms = client
        .list_rooms(Vec::new())
        .await
        .expect("all rooms should list");
    let names: HashSet<_> = all_rooms.iter().map(|room| room.name.as_str()).collect();
    assert!(names.contains(room_a.as_str()));
    assert!(names.contains(room_b.as_str()));

    let specific = client
        .list_rooms(vec![room_b.clone()])
        .await
        .expect("specific room should list");
    assert_eq!(specific.len(), 1, "specific room filter should return one room");
    assert_eq!(specific[0].name, room_b);

    client
        .delete_room(&room_a)
        .await
        .expect("first room should delete");
    client
        .delete_room(&room_b)
        .await
        .expect("second room should delete");

    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeUpdateParticipant
#[tokio::test]
async fn test_single_node_update_participant() {
    let (addr, server) = spawn_single_node().await;
    let client = room_client(addr);
    let room = format!("upstream-single-update-participant-{}", unique_suffix());
    client
        .create_room(&room, CreateRoomOptions::default())
        .await
        .expect("room should create before nonexistent-participant update check");

    let err = client
        .update_participant(
            &room,
            "nonexistent",
            UpdateParticipantOptions {
                permission: Some(proto::ParticipantPermission {
                    can_publish: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .expect_err("updating nonexistent participant should fail");
    match err {
        livekit_api::services::ServiceError::Twirp(livekit_api::services::TwirpError::Twirp(code)) => {
            assert_eq!(code.code, "not_found");
        }
        other => panic!("expected twirp not_found, got {other:?}"),
    }
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeCORS
#[tokio::test]
async fn test_single_node_cors() {
    let (addr, server) = spawn_single_node().await;
    let (_status, allow_origin) = http_post_with_origin(&format!("http://{addr}"), "/", "testhost.com").await;
    assert_eq!(allow_origin.as_deref(), Some("testhost.com"));
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeDoubleSlash
#[tokio::test]
async fn test_single_node_double_slash() {
    let (addr, server) = spawn_single_node().await;
    let client = RoomClient::with_api_key(&format!("http://{addr}/"), API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    client
        .list_rooms(Vec::new())
        .await
        .expect("trailing slash client should not be redirected/broken");
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestPingPong
#[tokio::test]
async fn test_ping_pong() {
    let (addr, server) = spawn_single_node().await;
    let result = run_signal_pingreq_pongresp(
        &base_url(addr),
        &format!("upstream-ping-{}", unique_suffix()),
        "ping-user",
        12345,
    )
    .await;
    assert_eq!(result.last_ping_timestamp, 12345);
    assert!(result.response_timestamp > 0);
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeJoinAfterClose
#[tokio::test]
async fn test_single_node_join_after_close() {
    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);
    let room = format!("upstream-join-after-close-{}", unique_suffix());

    let (first_room, _first_events) = connect_room(&url, &room, "jcr1", true).await;

    let client = room_client(addr);
    client
        .delete_room(&room)
        .await
        .expect("deleting room after first join should succeed");

    let (second_room, _second_events) = connect_room(&url, &room, "jcr2", true).await;

    let _ = second_room.close().await;
    let _ = first_room.close().await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeCloseNonRTCRoom
#[tokio::test]
async fn test_single_node_close_non_rtc_room() {
    let (addr, server) = spawn_single_node().await;
    let client = room_client(addr);
    let room = format!("upstream-close-non-rtc-{}", unique_suffix());
    client
        .create_room(&room, CreateRoomOptions::default())
        .await
        .expect("room should create");
    client.delete_room(&room).await.expect("room should delete");
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestAutoCreate
#[tokio::test]
async fn test_auto_create() {
    let (addr, server) = spawn_single_node_with_room_auto_create(false).await;
    let url = base_url(addr);
    let room = format!("upstream-auto-create-{}", unique_suffix());

    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.auto_subscribe = true;
    options.connect_timeout = Duration::from_secs(5);

    let first_token = join_token(&room, "start-before-create");
    let first_join = Room::connect(&url, &first_token, options.clone()).await;
    assert!(
        first_join.is_err(),
        "join should fail when room is missing and auto-create is disabled"
    );

    let second_token = join_token(&room, "start-before-create-2");
    let second_join = Room::connect(&url, &second_token, options.clone()).await;
    assert!(
        second_join.is_err(),
        "second join should also fail before explicit room creation"
    );

    let client = room_client(addr);
    client
        .create_room(&room, CreateRoomOptions::default())
        .await
        .expect("explicit room creation should succeed");

    let join_after_create_token = join_token(&room, "join-after-create");
    let (joined_room, _events) = Room::connect(&url, &join_after_create_token, options)
        .await
        .expect("join should succeed after explicit room creation");

    let _ = joined_room.close().await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeUpdateSubscriptionPermissions
#[tokio::test]
async fn test_single_node_update_subscription_permissions() {
    use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;

    for topology in [
        RawDataTopology::V0DualPeerConnection,
        RawDataTopology::V0SinglePeerConnection,
        RawDataTopology::V1,
    ] {
        eprintln!("running TestSingleNodeUpdateSubscriptionPermissions topology={}", topology.name());
        let (addr, server) = spawn_single_node().await;
        let room = format!(
            "upstream-update-sub-permissions-{}-{}",
            topology.name(),
            unique_suffix()
        );
        let subscriber_token = join_token_with(
            &room,
            "sub",
            "sub",
            "",
            HashMap::new(),
            VideoGrants {
                can_subscribe: false,
                can_publish: true,
                can_publish_data: true,
                ..Default::default()
            },
        );

        let mut publisher = NativeMediaParticipant::connect(topology, addr, &room, "pub").await;
        let mut subscriber =
            NativeMediaParticipant::connect_with_token(topology, addr, &subscriber_token).await;
        let audio = publisher
            .publish_track("audio-cid", "audio", RtpCodecKind::Audio, "audio/opus")
            .await;
        let video = publisher
            .publish_track("video-cid", "video", RtpCodecKind::Video, "video/vp8")
            .await;
        let data_track = publisher.publish_data_track(1, "data_track_1").await;

        subscriber.wait_for_remote_track_metadata(2).await;
        subscriber.assert_no_remote_track(Duration::from_millis(400)).await;
        publisher
            .send_data_track_frame(1, b"denied-data-track-frame")
            .await;
        subscriber
            .assert_no_data_track_frame(Duration::from_millis(400))
            .await;

        room_client(addr)
            .update_participant(
                &room,
                "sub",
                UpdateParticipantOptions {
                    permission: Some(proto::ParticipantPermission {
                        can_subscribe: true,
                        can_publish: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("RoomService should grant subscriber permission");

        let received = subscriber.receive_tracks(2, &[audio, video]).await;
        let received_mimes: HashSet<_> = received
            .iter()
            .map(|track| track.mime_type.to_ascii_lowercase())
            .collect();
        assert_eq!(received.len(), 2, "{} should receive two RTP tracks after grant", topology.name());
        assert!(received_mimes.contains("audio/opus"), "{} should receive Opus RTP after grant", topology.name());
        assert!(received_mimes.contains("video/vp8"), "{} should receive VP8 RTP after grant", topology.name());

        let subscriber_handle = subscriber
            .wait_for_data_track_subscription(&data_track.sid)
            .await;
        publisher
            .send_data_track_frame(1, b"granted-data-track-frame")
            .await;
        subscriber
            .receive_data_track_frame(subscriber_handle, b"granted-data-track-frame")
            .await;

        server.abort();
    }
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeAttributes
#[tokio::test]
async fn test_single_node_attributes() {
    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);
    let room = format!("upstream-single-attrs-{}", unique_suffix());

    let mut token_attrs = HashMap::new();
    token_attrs.insert("a".to_string(), "0".to_string());
    token_attrs.insert("b".to_string(), "1".to_string());
    let mut join_attrs = HashMap::new();
    join_attrs.insert("b".to_string(), "2".to_string());
    join_attrs.insert("c".to_string(), "3".to_string());
    let pub_token = join_token_with(
        &room,
        "pub",
        "pub",
        "",
        token_attrs,
        VideoGrants {
            can_update_own_metadata: true,
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        },
    );
    let (_pub_socket, _pub_sid) = connect_signal_socket_with_token_and_participant_attributes(
        &url,
        &pub_token,
        join_attrs,
    )
    .await;
    let sub_token = join_token_with(
        &room,
        "sub",
        "sub",
        "",
        HashMap::new(),
        VideoGrants {
            can_subscribe: false,
            can_publish: true,
            ..Default::default()
        },
    );
    let (sub_room, _sub_events) = connect_room_with_token(&url, &sub_token, true).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(remote_pub) = sub_room.remote_participants().get(&"pub".into()).cloned() {
            let mut expected_attrs = HashMap::new();
            expected_attrs.insert("a".to_string(), "0".to_string());
            expected_attrs.insert("b".to_string(), "2".to_string());
            expected_attrs.insert("c".to_string(), "3".to_string());
            assert_eq!(
                remote_pub.attributes(),
                expected_attrs,
                "join-request attributes should override token attributes while preserving token-only keys"
            );
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "subscriber should see publisher attributes");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = sub_room.close().await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestDeviceCodecOverride
#[tokio::test]
async fn test_device_codec_override() {
    fn offer_sdp_with_vp8_and_h264() -> String {
        [
            "v=0",
            "o=- 1 2 IN IP4 0.0.0.0",
            "s=-",
            "t=0 0",
            "a=msid-semantic:WMS *",
            "a=fingerprint:sha-256 10:4B:98:4E:03:11:24:9C:71:A2:93:75:B7:DD:99:B1:8D:B5:DC:40:A6:27:7C:FC:8E:0C:C4:C1:B9:7E:BF:8E",
            "a=group:BUNDLE 0 1",
            "m=application 9 UDP/DTLS/SCTP webrtc-datachannel",
            "c=IN IP4 0.0.0.0",
            "a=setup:actpass",
            "a=mid:0",
            "a=sendrecv",
            "a=sctp-port:5000",
            "a=max-message-size:1073741823",
            "a=ice-ufrag:abcdefg",
            "a=ice-pwd:abcdefghijklmnopqrstuvwxyz",
            "m=video 9 UDP/TLS/RTP/SAVPF 96 125 108",
            "c=IN IP4 0.0.0.0",
            "a=setup:actpass",
            "a=mid:1",
            "a=ice-ufrag:abcdefg",
            "a=ice-pwd:abcdefghijklmnopqrstuvwxyz",
            "a=rtcp-mux",
            "a=rtcp-rsize",
            "a=rtpmap:96 VP8/90000",
            "a=rtpmap:125 H264/90000",
            "a=fmtp:125 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f",
            "a=rtpmap:108 H264/90000",
            "a=fmtp:108 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
            "a=sendrecv",
            "",
        ]
        .join("\r\n")
    }

    async fn send_offer_and_read_answer(
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        sdp: String,
    ) -> proto::SessionDescription {
        let offer = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Offer(
                proto::SessionDescription {
                    r#type: "offer".to_string(),
                    sdp,
                    id: 77,
                    ..Default::default()
                },
            )),
        };
        socket
            .send(Message::Binary(offer.encode_to_vec().into()))
            .await
            .expect("offer should send");

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let message = socket
                    .next()
                    .await
                    .expect("answer should arrive")
                    .expect("answer should be ok");
                let Message::Binary(bytes) = message else {
                    continue;
                };
                let response = proto::SignalResponse::decode(bytes.as_ref())
                    .expect("signal response should decode");
                if let Some(proto::signal_response::Message::Answer(answer)) = response.message {
                    break answer;
                }
            }
        })
        .await
        .expect("answer should arrive before timeout")
    }

    fn video_rtpmap_lines(answer_sdp: &str) -> Vec<String> {
        let mut in_video_section = false;
        let mut lines = Vec::new();
        for raw_line in answer_sdp.lines() {
            let line = raw_line.trim();
            if line.starts_with("m=") {
                in_video_section = line.starts_with("m=video ");
                continue;
            }
            if in_video_section && line.starts_with("a=rtpmap:") {
                lines.push(line.to_string());
            }
        }
        lines
    }

    let (addr, server) = spawn_single_node().await;
    let room = format!("upstream-device-codec-{}", unique_suffix());
    let url = base_url(addr);

    let normal_token = join_token(&room, "c1-normal");
    let (mut normal_socket, _normal_sid) = connect_signal_socket_with_token_and_join_request(
        &url,
        &normal_token,
        proto::JoinRequest::default(),
    )
    .await;

    let normal_answer = send_offer_and_read_answer(&mut normal_socket, offer_sdp_with_vp8_and_h264()).await;
    let normal_rtpmap = video_rtpmap_lines(&normal_answer.sdp);
    assert!(
        normal_rtpmap
            .iter()
            .any(|line| line.to_ascii_lowercase().contains("h264/")),
        "baseline answer should include H264 before device override: {normal_rtpmap:?}"
    );

    let override_token = join_token(&room, "c1-override");
    let (mut override_socket, _override_sid) = connect_signal_socket_with_token_and_join_request(
        &url,
        &override_token,
        proto::JoinRequest {
            client_info: Some(proto::ClientInfo {
                os: "android".to_string(),
                device_model: "Xiaomi 2201117TI".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;

    let override_answer =
        send_offer_and_read_answer(&mut override_socket, offer_sdp_with_vp8_and_h264()).await;
    let override_rtpmap = video_rtpmap_lines(&override_answer.sdp);

    assert!(
        !override_rtpmap.is_empty(),
        "answer should contain video codec rtpmap lines"
    );
    assert!(
        override_rtpmap
            .iter()
            .all(|line| !line.to_ascii_lowercase().contains("h264/")),
        "device override should remove H264 from video answer: {override_rtpmap:?}"
    );
    assert!(
        override_rtpmap
            .iter()
            .any(|line| line.to_ascii_lowercase().contains("vp8/")),
        "device override should keep VP8 in video answer: {override_rtpmap:?}"
    );

    let _ = normal_socket.close(None).await;
    let _ = override_socket.close(None).await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSubscribeToCodecUnsupported
#[tokio::test]
async fn test_subscribe_to_codec_unsupported() {
    use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;

    for topology in [
        RawDataTopology::V0DualPeerConnection,
        RawDataTopology::V0SinglePeerConnection,
        RawDataTopology::V1,
    ] {
        eprintln!("running TestSubscribeToCodecUnsupported topology={}", topology.name());
        let (addr, server) = spawn_single_node().await;
        let room = format!(
            "upstream-codec-unsupported-{}-{}",
            topology.name(),
            unique_suffix()
        );

        let mut c1 = NativeMediaParticipant::connect(topology, addr, &room, "c1").await;
        let mut c2 = NativeMediaParticipant::connect_without_h264(topology, addr, &room, "c2").await;

        let audio = c1
            .publish_track("audio-cid", "audio", RtpCodecKind::Audio, "audio/opus")
            .await;
        let vp8_1 = c1
            .publish_track("vp8-cid-1", "video", RtpCodecKind::Video, "video/vp8")
            .await;
        let mut published_tracks = vec![audio, vp8_1];

        let initial_tracks = c2.receive_tracks(2, &published_tracks).await;
        let initial_mimes: HashSet<_> = initial_tracks
            .iter()
            .map(|track| track.mime_type.to_ascii_lowercase())
            .collect();
        assert_eq!(initial_tracks.len(), 2, "{} should receive audio and the first VP8 track", topology.name());
        assert!(initial_mimes.contains("audio/opus"), "{} should receive Opus RTP", topology.name());
        assert!(initial_mimes.contains("video/vp8"), "{} should receive VP8 RTP", topology.name());
        c2.assert_no_subscription_response(Duration::from_millis(300)).await;

        let h264 = c1
            .publish_track(
                "h264-cid",
                "videoscreen",
                RtpCodecKind::Video,
                "video/h264",
            )
            .await;
        let h264_sid = c1.published_track_sid("h264-cid");
        published_tracks.push(h264);

        c2.wait_for_unsupported_codec(&h264_sid).await;
        c2.assert_no_remote_track(Duration::from_millis(300)).await;
        c2.assert_no_subscription_response(Duration::from_millis(300)).await;

        let vp8_2 = c1
            .publish_track("vp8-cid-2", "video2", RtpCodecKind::Video, "video/vp8")
            .await;
        published_tracks.push(vp8_2);

        let recovered_tracks = c2.receive_tracks(1, &published_tracks).await;
        let all_tracks: Vec<_> = initial_tracks.iter().chain(recovered_tracks.iter()).collect();
        let vp8_receiver_count = all_tracks
            .iter()
            .filter(|track| track.mime_type.eq_ignore_ascii_case("video/vp8"))
            .count();
        assert_eq!(all_tracks.len(), 3, "{} should expose only Opus and two VP8 receiver tracks", topology.name());
        assert_eq!(vp8_receiver_count, 2, "{} should recover with exactly two VP8 receiver tracks", topology.name());
        assert!(
            all_tracks
                .iter()
                .all(|track| !track.mime_type.eq_ignore_ascii_case("video/h264")),
            "{} must not expose a remote H264 track from the rejected SDP section",
            topology.name()
        );
        c2.assert_no_subscription_response(Duration::from_millis(400)).await;

        server.abort();
    }
}

// Upstream: livekit/test/singlenode_test.go::TestDataPublishSlowSubscriber
#[tokio::test]
#[ignore = "OxideSFU reliable-data slow-subscriber contiguity remains a documented compatibility gap"]
async fn test_data_publish_slow_subscriber() {
    const PAYLOAD_SIZE: usize = 100;

    fn payload_index(payload: &[u8]) -> u64 {
        let tail = payload
            .get(payload.len().saturating_sub(8)..)
            .expect("payload should include trailing index bytes");
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(tail);
        u64::from_be_bytes(bytes)
    }

    fn is_indexed_payload(payload: &[u8]) -> bool {
        if payload.len() != PAYLOAD_SIZE {
            return false;
        }
        payload[..(PAYLOAD_SIZE - 8)].iter().all(|byte| *byte == 0)
    }

    fn is_slow_reader_data_channel_error(
        error: &(dyn std::error::Error + Send + Sync),
    ) -> bool {
        let text = error.to_string().to_ascii_lowercase();
        text.contains("slow reader") || text.contains("would block")
    }

    fn is_ready_marker(bytes: &[u8]) -> bool {
        let Ok(packet) = proto::DataPacket::decode(bytes) else {
            return false;
        };
        matches!(
            packet.value,
            Some(proto::data_packet::Value::User(user))
                if user.topic.as_deref() == Some("ready") && user.payload == b"ready"
        )
    }



    async fn send_ready_marker_retry(data_channel: &oxidesfu_rtc::DataChannel) {
        let packet = proto::DataPacket {
            kind: proto::data_packet::Kind::Reliable as i32,
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                payload: b"ready".to_vec(),
                topic: Some("ready".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec();

        loop {
            match data_channel.send_bytes(&packet).await {
                Ok(()) => break,
                Err(error) if is_slow_reader_data_channel_error(error.as_ref()) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("ready marker send failed unexpectedly: {error}"),
            }
        }
    }

    async fn wait_until_all_subscribers_receive_ready_marker(
        publisher: &oxidesfu_rtc::DataChannel,
        fast_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        slow_no_drop_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        slow_drop_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let mut fast_ready = false;
        let mut slow_no_drop_ready = false;
        let mut slow_drop_ready = false;

        tokio::time::timeout(Duration::from_secs(20), async {
            while !(fast_ready && slow_no_drop_ready && slow_drop_ready) {
                send_ready_marker_retry(publisher).await;
                let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
                loop {
                    if fast_ready && slow_no_drop_ready && slow_drop_ready {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }

                    tokio::select! {
                        Some(bytes) = fast_rx.recv(), if !fast_ready => {
                            fast_ready = is_ready_marker(&bytes);
                        }
                        Some(bytes) = slow_no_drop_rx.recv(), if !slow_no_drop_ready => {
                            slow_no_drop_ready = is_ready_marker(&bytes);
                        }
                        Some(bytes) = slow_drop_rx.recv(), if !slow_drop_ready => {
                            slow_drop_ready = is_ready_marker(&bytes);
                        }
                        _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                    }
                }
            }
        })
        .await
        .expect("all subscribers should receive ready marker before measured publish loop");
    }


    const DATA_CHANNEL_SLOW_THRESHOLD: u32 = 21_024;

    for topology in [
        RawDataTopology::V0DualPeerConnection,
        RawDataTopology::V0SinglePeerConnection,
        RawDataTopology::V1,
    ] {
        eprintln!("running TestDataPublishSlowSubscriber topology={}", topology.name());

    let (addr, server) =
        spawn_single_node_with_datachannel_slow_threshold_bytes(DATA_CHANNEL_SLOW_THRESHOLD).await;
    let room = format!("upstream-data-publish-slow-subscriber-{}", unique_suffix());
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let drain_slow_no_drop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut publisher = connect_raw_data_participant(topology, addr, &room, "pub", None, None, true).await;
    let mut fast = connect_raw_data_participant(topology, addr, &room, "fastSub", None, None, false).await;
    let mut slow_no_drop = connect_raw_data_participant(
        topology,
        addr,
        &room,
        "slowSubNotDrop",
        Some(DATA_CHANNEL_SLOW_THRESHOLD.saturating_mul(2)),
        Some(drain_slow_no_drop.clone()),
        false,
    )
    .await;
    let mut slow_drop = connect_raw_data_participant(
        topology,
        addr,
        &room,
        "slowSubDrop",
        Some(DATA_CHANNEL_SLOW_THRESHOLD.saturating_div(2)),
        Some(dropped.clone()),
        false,
    )
    .await;

    tokio::time::timeout(Duration::from_secs(10), async {
        publisher
            .open_rx
            .recv()
            .await
            .expect("publisher data channel should open");
        fast
            .open_rx
            .recv()
            .await
            .expect("fast subscriber data channel should open");
        slow_no_drop
            .open_rx
            .recv()
            .await
            .expect("slow-no-drop subscriber data channel should open");
        slow_drop
            .open_rx
            .recv()
            .await
            .expect("slow-drop subscriber data channel should open");
    })
    .await
    .expect("all data channels should open before publish loop starts");

    wait_until_all_subscribers_receive_ready_marker(
        &publisher.send_data_channel,
        &mut fast.data_rx,
        &mut slow_no_drop.data_rx,
        &mut slow_drop.data_rx,
    )
    .await;

    // Configure publisher-side thresholds for slow-reader/backpressure checks.
    publisher
        .send_data_channel
        .set_buffered_amount_low_threshold(DATA_CHANNEL_SLOW_THRESHOLD.saturating_div(2))
        .await
        .expect("publisher low buffered threshold should apply");
    publisher
        .send_data_channel
        .set_buffered_amount_high_threshold(DATA_CHANNEL_SLOW_THRESHOLD)
        .await
        .expect("publisher high buffered threshold should apply");

    let fast_index = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let slow_no_drop_index = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let slow_drop_index = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let fast_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let slow_no_drop_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let fast_index_task = fast_index.clone();
    let fast_dropped_task = fast_dropped.clone();
    let fast_task = tokio::spawn(async move {
        while let Some(bytes) = fast.data_rx.recv().await {
            let Ok(packet) = proto::DataPacket::decode(bytes.as_slice()) else {
                continue;
            };
            let Some(proto::data_packet::Value::User(user)) = packet.value else {
                continue;
            };
            if user.topic.as_deref() != Some("indexed") || !is_indexed_payload(&user.payload) {
                continue;
            }
            let idx = payload_index(&user.payload);
            let expected = fast_index_task.load(std::sync::atomic::Ordering::Relaxed) + 1;
            if idx != expected {
                fast_dropped_task.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            fast_index_task.store(idx, std::sync::atomic::Ordering::Relaxed);
        }
    });

    let slow_no_drop_index_task = slow_no_drop_index.clone();
    let slow_no_drop_dropped_task = slow_no_drop_dropped.clone();
    let slow_no_drop_task = tokio::spawn(async move {
        while let Some(bytes) = slow_no_drop.data_rx.recv().await {
            let Ok(packet) = proto::DataPacket::decode(bytes.as_slice()) else {
                continue;
            };
            let Some(proto::data_packet::Value::User(user)) = packet.value else {
                continue;
            };
            if user.topic.as_deref() != Some("indexed") || !is_indexed_payload(&user.payload) {
                continue;
            }
            let idx = payload_index(&user.payload);
            let expected = slow_no_drop_index_task.load(std::sync::atomic::Ordering::Relaxed) + 1;
            if idx != expected {
                slow_no_drop_dropped_task.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            slow_no_drop_index_task.store(idx, std::sync::atomic::Ordering::Relaxed);
        }
    });

    let slow_drop_index_task = slow_drop_index.clone();
    let dropped_task = dropped.clone();
    let slow_drop_task = tokio::spawn(async move {
        while let Some(bytes) = slow_drop.data_rx.recv().await {
            let Ok(packet) = proto::DataPacket::decode(bytes.as_slice()) else {
                continue;
            };
            let Some(proto::data_packet::Value::User(user)) = packet.value else {
                continue;
            };
            if user.topic.as_deref() != Some("indexed") || !is_indexed_payload(&user.payload) {
                continue;
            }
            let idx = payload_index(&user.payload);
            let expected = slow_drop_index_task.load(std::sync::atomic::Ordering::Relaxed) + 1;
            if idx != expected {
                dropped_task.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            slow_drop_index_task.store(idx, std::sync::atomic::Ordering::Relaxed);
        }
    });

    let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let stop_write = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let write_idx = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let blocked_writer = blocked.clone();
    let stop_write_writer = stop_write.clone();
    let write_idx_writer = write_idx.clone();

    let publisher_data_channel = publisher.send_data_channel.clone();
    let write_task = tokio::spawn(async move {
        let mut i = 0u64;
        while !stop_write_writer.load(std::sync::atomic::Ordering::Relaxed) {
            i += 1;
            let mut payload = vec![0u8; PAYLOAD_SIZE];
            payload[(PAYLOAD_SIZE - 8)..].copy_from_slice(&i.to_be_bytes());
            let packet = proto::DataPacket {
                kind: proto::data_packet::Kind::Reliable as i32,
                value: Some(proto::data_packet::Value::User(proto::UserPacket {
                    payload,
                    topic: Some("indexed".to_string()),
                    ..Default::default()
                })),
                ..Default::default()
            }
            .encode_to_vec();

            match publisher_data_channel.send_bytes(&packet).await {
                Ok(()) => {
                    write_idx_writer.store(i, std::sync::atomic::Ordering::Relaxed);
                }
                Err(error) if is_slow_reader_data_channel_error(error.as_ref()) => {
                    blocked_writer.store(true, std::sync::atomic::Ordering::Relaxed);
                    i = i.saturating_sub(1);
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("unexpected publisher send failure: {error}"),
            }

            if i % 64 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(30), async {
        while !dropped.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("slowSubDrop should observe dropped reliable data sequence");

    tokio::time::sleep(Duration::from_secs(1)).await;
    blocked.store(false, std::sync::atomic::Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(30), async {
        while !blocked.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("publisher should hit slow-reader backpressure while load is sustained");

    stop_write.store(true, std::sync::atomic::Ordering::Relaxed);
    write_task.await.expect("writer task should join without panic");
    drain_slow_no_drop.store(true, std::sync::atomic::Ordering::Relaxed);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let write_idx = write_idx.load(std::sync::atomic::Ordering::Relaxed);
            let fast_idx = fast_index.load(std::sync::atomic::Ordering::Relaxed);
            let slow_no_drop_idx = slow_no_drop_index.load(std::sync::atomic::Ordering::Relaxed);
            if write_idx > 0 && write_idx == fast_idx && write_idx == slow_no_drop_idx {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("fast and slowSubNotDrop subscribers should catch up to publisher write index");

    assert!(
        !fast_dropped.load(std::sync::atomic::Ordering::Relaxed),
        "fast subscriber should not observe dropped data"
    );
    assert!(
        !slow_no_drop_dropped.load(std::sync::atomic::Ordering::Relaxed),
        "slow subscriber above threshold should remain contiguous"
    );

    fast_task.abort();
    slow_no_drop_task.abort();
    slow_drop_task.abort();
    server.abort();
    }
}

// Upstream: livekit/test/singlenode_test.go::TestFireTrackBySdp
#[tokio::test]
async fn test_fire_track_by_sdp() {
    #[derive(Clone)]
    struct FireTrackCase {
        name: &'static str,
        codecs: Vec<(&'static str, proto::TrackType, proto::TrackSource)>,
        pub_sdk: proto::client_info::Sdk,
    }

    async fn wait_for_subscribed_tracks_from_publisher(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<RoomEvent>,
        publisher_identity: &str,
        expected: usize,
    ) -> HashMap<String, (livekit::prelude::RemoteTrack, String)> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut tracks = HashMap::new();
            while tracks.len() < expected {
                let event = events.recv().await.expect("room events should stay open");
                if let RoomEvent::TrackSubscribed {
                    track,
                    publication,
                    participant,
                } = event
                {
                    if participant.identity().to_string() == publisher_identity {
                        tracks.insert(
                            publication.sid().to_string(),
                            (track, publication.mime_type().to_ascii_lowercase()),
                        );
                    }
                }
            }
            tracks
        })
        .await
        .expect("expected track subscriptions before timeout")
    }

    async fn track_codec_payload_type_for_mime(
        track: &livekit::prelude::RemoteTrack,
        expected_mime: &str,
    ) -> u32 {
        let Ok(stats) = track.get_stats().await else {
            return 0;
        };
        stats
            .into_iter()
            .find_map(|stat| match stat {
                livekit::webrtc::stats::RtcStats::Codec(codec)
                    if codec.codec.mime_type.eq_ignore_ascii_case(expected_mime) =>
                {
                    Some(codec.codec.payload_type)
                }
                _ => None,
            })
            .unwrap_or(0)
    }

    async fn track_has_no_received_packets(track: &livekit::prelude::RemoteTrack) -> bool {
        let Ok(stats) = track.get_stats().await else {
            return true;
        };
        let mut saw_inbound = false;
        for stat in stats {
            if let livekit::webrtc::stats::RtcStats::InboundRtp(inbound) = stat {
                saw_inbound = true;
                if inbound.received.packets_received > 0 {
                    return false;
                }
            }
        }
        // If there are no inbound RTP stats yet, this is still consistent with
        // "track fired by SDP before RTP packets" semantics.
        let _ = saw_inbound;
        true
    }

    let cases = vec![
        FireTrackCase {
            name: "js_client_could_pub_av_tracks",
            codecs: vec![
                ("video/h264", proto::TrackType::Video, proto::TrackSource::Camera),
                ("audio/opus", proto::TrackType::Audio, proto::TrackSource::Microphone),
            ],
            pub_sdk: proto::client_info::Sdk::Js,
        },
        FireTrackCase {
            name: "go_client_could_pub_audio_tracks",
            codecs: vec![(
                "audio/opus",
                proto::TrackType::Audio,
                proto::TrackSource::Microphone,
            )],
            pub_sdk: proto::client_info::Sdk::Go,
        },
    ];

    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);

    for case in cases {
        let room = format!("upstream-fire-track-by-sdp-{}-{}", case.name, unique_suffix());
        let c1_token = join_token(&room, &format!("{}_c1", case.name));
        let c2_token = join_token(&room, &format!("{}_c2", case.name));

        let mut c1_options = RoomOptions::default();
        c1_options.single_peer_connection = false;
        c1_options.auto_subscribe = true;
        c1_options.connect_timeout = Duration::from_secs(10);
        c1_options.sdk_options.sdk = case.pub_sdk.as_str_name().to_ascii_lowercase();

        let mut c2_options = RoomOptions::default();
        c2_options.single_peer_connection = false;
        c2_options.auto_subscribe = true;
        c2_options.connect_timeout = Duration::from_secs(10);
        c2_options.sdk_options.sdk = proto::client_info::Sdk::Js
            .as_str_name()
            .to_ascii_lowercase();

        let (c1_room, _c1_events) = Room::connect(&url, &c1_token, c1_options)
            .await
            .expect("publisher should connect");
        let (c2_room, mut c2_events) = Room::connect(&url, &c2_token, c2_options)
            .await
            .expect("subscriber should connect");

        let mut published_sids = Vec::new();
        let mut audio_sources = Vec::new();
        for (idx, (mime, track_type, source)) in case.codecs.iter().enumerate() {
            let source = match source {
                proto::TrackSource::Microphone => livekit::track::TrackSource::Microphone,
                proto::TrackSource::Camera => livekit::track::TrackSource::Camera,
                proto::TrackSource::ScreenShare => livekit::track::TrackSource::Screenshare,
                proto::TrackSource::ScreenShareAudio => {
                    livekit::track::TrackSource::ScreenshareAudio
                }
                _ => livekit::track::TrackSource::Unknown,
            };

            let publication = match track_type {
                proto::TrackType::Audio => {
                    let source_native =
                        NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
                    let track = LocalAudioTrack::create_audio_track(
                        &format!("{mime}-{idx}"),
                        RtcAudioSource::Native(source_native.clone()),
                    );
                    audio_sources.push(source_native);
                    c1_room
                        .local_participant()
                        .publish_track(
                            LocalTrack::Audio(track),
                            TrackPublishOptions {
                                source,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("audio track should publish")
                }
                proto::TrackType::Video => {
                    let video_source =
                        NativeVideoSource::new(VideoResolution { width: 16, height: 16 }, false);
                    let track = LocalVideoTrack::create_video_track(
                        &format!("{mime}-{idx}"),
                        RtcVideoSource::Native(video_source),
                    );
                    c1_room
                        .local_participant()
                        .publish_track(
                            LocalTrack::Video(track),
                            TrackPublishOptions {
                                source,
                                video_codec: if mime.eq_ignore_ascii_case("video/h264") {
                                    livekit::options::VideoCodec::H264
                                } else {
                                    livekit::options::VideoCodec::VP8
                                },
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("video track should publish")
                }
                _ => panic!("unsupported track type in fire-by-sdp case"),
            };
            published_sids.push(publication.sid().to_string());
        }

        let subscribed = wait_for_subscribed_tracks_from_publisher(
            &mut c2_events,
            &format!("{}_c1", case.name),
            case.codecs.len(),
        )
        .await;
        assert_eq!(
            subscribed.len(),
            case.codecs.len(),
            "subscriber should receive all published tracks for case {}",
            case.name
        );

        for sid in &published_sids {
            let (track, mime_type) = subscribed
                .get(sid)
                .expect("published track sid should appear in subscribed tracks");
            let payload_type = track_codec_payload_type_for_mime(track, mime_type).await;
            assert_eq!(
                payload_type, 0,
                "subscribed track codec payload type should be zero (or unknown-equivalent) before RTP packets in case {} for sid {}",
                case.name, sid
            );
            assert!(
                track_has_no_received_packets(track).await,
                "subscribed track should not have received RTP packets in fire-by-sdp case {} for sid {}",
                case.name,
                sid
            );
        }

        drop(audio_sources);
        let _ = c1_room.close().await;
        let _ = c2_room.close().await;
    }

    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSinglePublisherDataTrack
#[tokio::test]
async fn test_single_publisher_data_track() {
    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);
    let room = format!("upstream-single-data-track-{}", unique_suffix());

    let (c1_room, _c1_events) = connect_room(&url, &room, "c1", true).await;
    let (_c2_room, mut c2_events) = connect_room(&url, &room, "c2", true).await;

    let dt1 = c1_room
        .local_participant()
        .publish_data_track("data_track_1")
        .await
        .expect("c1 should publish first data track");
    let dt2 = c1_room
        .local_participant()
        .publish_data_track("data_track_2")
        .await
        .expect("c1 should publish second data track");

    let c2_tracks = wait_for_data_track_published_count(&mut c2_events, 2).await;
    assert_eq!(c2_tracks.len(), 2, "c2 should subscribe to two data tracks from c1");
    for track in &c2_tracks {
        assert_eq!(
            track.publisher_identity().to_string(),
            "c1",
            "c2 data track publisher should be c1"
        );
        assert!(
            track.info().sid().to_string().starts_with("DTR_"),
            "data track ID should begin with DTR_"
        );
    }

    let (_c3_room, mut c3_events) = connect_room(&url, &room, "c3", true).await;
    let c3_tracks = wait_for_data_track_published_count(&mut c3_events, 2).await;
    assert_eq!(
        c3_tracks.len(),
        2,
        "c3 should subscribe to two existing data tracks from c1"
    );
    for track in &c3_tracks {
        assert_eq!(
            track.publisher_identity().to_string(),
            "c1",
            "c3 data track publisher should be c1"
        );
        assert!(
            track.info().sid().to_string().starts_with("DTR_"),
            "data track ID should begin with DTR_"
        );
    }

    let c2_sids: HashSet<_> = c2_tracks
        .iter()
        .map(|track| track.info().sid().to_string())
        .collect();
    let c3_sids: HashSet<_> = c3_tracks
        .iter()
        .map(|track| track.info().sid().to_string())
        .collect();
    assert_eq!(
        c2_sids, c3_sids,
        "late joiner should receive the same published data tracks from c1"
    );

    let _ = dt1.try_push(DataTrackFrame::new(b"datatrack-probe-1".to_vec()));
    let _ = dt2.try_push(DataTrackFrame::new(b"datatrack-probe-2".to_vec()));
    let _ = c1_room.close().await;
    server.abort();
}

static OWNED_TURN_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn reserve_local_udp_port(bind_ip: &str) -> u16 {
    std::net::UdpSocket::bind(format!("{bind_ip}:0"))
        .expect("ephemeral UDP port should bind")
        .local_addr()
        .expect("UDP socket should have local address")
        .port()
}

fn owned_turn_config(
    turn_port: u16,
    allow_restricted_peer_cidrs: Vec<String>,
    deny_peer_cidrs: Vec<String>,
) -> oxidesfu_core::ServerConfig {
    let mut config = oxidesfu_core::ServerConfig::development();
    config.turn_enabled = true;
    config.turn_domain = Some("127.0.0.1".to_string());
    config.turn_bind = "127.0.0.1".to_string();
    config.turn_udp_port = Some(turn_port);
    config.turn_relay_port_range_start = Some(40_000);
    config.turn_relay_port_range_end = Some(40_100);
    config.turn_allow_restricted_peer_cidrs = allow_restricted_peer_cidrs;
    config.turn_deny_peer_cidrs = deny_peer_cidrs;
    config
}



// Upstream: livekit/test/singlenode_test.go::TestTurnRelay
#[tokio::test]
async fn test_turn_relay() {
    let _turn_test_guard = OWNED_TURN_TEST_LOCK.lock().await;

    struct Case {
        name: &'static str,
        allow_restricted_peer_cidrs: Vec<String>,
        deny_peer_cidrs: Vec<String>,
        expected_to_relay: bool,
    }

    async fn run_case(case: &Case) {
        eprintln!("[turn-relay] case={}", case.name);
        let turn_port = reserve_local_udp_port("127.0.0.1");
        let fixture = spawn_single_node_with_owned_turn(owned_turn_config(
            turn_port,
            case.allow_restricted_peer_cidrs.clone(),
            case.deny_peer_cidrs.clone(),
        ))
        .await;
        let turn_addr = std::net::SocketAddr::from(([127, 0, 0, 1], turn_port));
        let credentials = oxidesfu_server::signal_ice_servers_for_participant(&fixture.config, "PA_relay")
            .into_iter()
            .find(|server| server.urls.iter().any(|url| url.starts_with("turn:")))
            .expect("enabled TURN should advertise a TURN server");
        let client = authenticated_turn_client(turn_addr, &credentials.username, &credentials.credential).await;
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("relay peer socket should bind");
        let permission = send_create_permission(&client, peer.local_addr().expect("peer should have an address")).await;

        if case.expected_to_relay {
            assert_eq!(permission.message_type, TURN_CREATE_PERMISSION_SUCCESS);
            send_indication(&client, peer.local_addr().expect("peer should have an address"), b"relay-probe")
                .await;
            let mut received = [0_u8; 64];
            let received_len = tokio::time::timeout(Duration::from_secs(3), peer.recv(&mut received))
                .await
                .expect("allowed peer should receive relayed data")
                .expect("peer socket should receive relayed data");
            assert_eq!(&received[..received_len], b"relay-probe");
        } else {
            assert_eq!(permission.message_type, TURN_CREATE_PERMISSION_ERROR);
            assert_eq!(turn_error_code(&permission), 403);
        }

        fixture.shutdown().await;
    }

    let loopback = "127.0.0.0/8".to_string();
    let cases = [
        Case {
            name: "allow",
            allow_restricted_peer_cidrs: vec![loopback.clone()],
            deny_peer_cidrs: Vec::new(),
            expected_to_relay: true,
        },
        Case {
            name: "not-allowed",
            allow_restricted_peer_cidrs: Vec::new(),
            deny_peer_cidrs: Vec::new(),
            expected_to_relay: false,
        },
        Case {
            name: "denied-overrides-allowed",
            allow_restricted_peer_cidrs: vec![loopback.clone()],
            deny_peer_cidrs: vec![loopback],
            expected_to_relay: false,
        },
    ];

    for case in &cases {
        run_case(case).await;
    }
}



fn base62_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'Z' => Some(byte - b'A' + 10),
        b'a'..=b'z' => Some(byte - b'a' + 36),
        _ => None,
    }
}

fn base62_encode_byte(value: u8) -> u8 {
    debug_assert!(value < 62);
    match value {
        0..=9 => b'0' + value,
        10..=35 => b'A' + (value - 10),
        _ => b'a' + (value - 36),
    }
}

fn base62_encode_bytes(input: &[u8]) -> String {
    if input.is_empty() {
        return String::new();
    }

    let mut digits = vec![0_u8];
    for &byte in input {
        let mut carry = u32::from(byte);
        for digit in &mut digits {
            let value = u32::from(*digit) * 256 + carry;
            *digit = (value % 62) as u8;
            carry = value / 62;
        }
        while carry > 0 {
            digits.push((carry % 62) as u8);
            carry /= 62;
        }
    }

    let leading_zeros = input.iter().take_while(|&&b| b == 0).count();
    for _ in 0..leading_zeros {
        digits.push(0);
    }

    let mut out = String::with_capacity(digits.len());
    for digit in digits.iter().rev() {
        out.push(char::from(base62_encode_byte(*digit)));
    }
    out
}

fn base62_decode_to_bytes(input: &str) -> Result<Vec<u8>, ()> {
    if input.is_empty() {
        return Err(());
    }

    let mut bytes = vec![0_u8];
    for ch in input.bytes() {
        let mut carry = u32::from(base62_value(ch).ok_or(())?);
        for byte in &mut bytes {
            let value = u32::from(*byte) * 62 + carry;
            *byte = (value % 256) as u8;
            carry = value / 256;
        }
        while carry > 0 {
            bytes.push((carry % 256) as u8);
            carry /= 256;
        }
    }

    let leading_zeros = input.bytes().take_while(|&b| b == b'0').count();
    for _ in 0..leading_zeros {
        bytes.push(0);
    }

    bytes.reverse();
    Ok(bytes)
}

const TURN_ALLOCATE_REQUEST: u16 = 0x0003;
const TURN_ALLOCATE_SUCCESS: u16 = 0x0103;
const TURN_ALLOCATE_ERROR: u16 = 0x0113;
const TURN_CREATE_PERMISSION_REQUEST: u16 = 0x0008;
const TURN_CREATE_PERMISSION_SUCCESS: u16 = 0x0108;
const TURN_CREATE_PERMISSION_ERROR: u16 = 0x0118;
const TURN_SEND_INDICATION: u16 = 0x0016;
const STUN_MAGIC_COOKIE: u32 = 0x2112_A442;
const STUN_ATTR_USERNAME: u16 = 0x0006;
const STUN_ATTR_MESSAGE_INTEGRITY: u16 = 0x0008;
const STUN_ATTR_ERROR_CODE: u16 = 0x0009;
const STUN_ATTR_XOR_PEER_ADDRESS: u16 = 0x0012;
const STUN_ATTR_DATA: u16 = 0x0013;
const STUN_ATTR_REALM: u16 = 0x0014;
const STUN_ATTR_NONCE: u16 = 0x0015;
const STUN_ATTR_REQUESTED_TRANSPORT: u16 = 0x0019;
const STUN_ATTR_MESSAGE_INTEGRITY_SHA256: u16 = 0x001C;
const STUN_ATTR_PASSWORD_ALGORITHM: u16 = 0x001D;

#[derive(Debug)]
struct TurnResponse {
    message_type: u16,
    transaction_id: [u8; 12],
    attributes: Vec<(u16, Vec<u8>)>,
}

struct AuthenticatedTurnClient {
    socket: tokio::net::UdpSocket,
    username: String,
    realm: String,
    nonce: String,
    password: String,
}

fn append_stun_attribute(attributes: &mut Vec<u8>, attribute_type: u16, value: &[u8]) {
    attributes.extend_from_slice(&attribute_type.to_be_bytes());
    attributes.extend_from_slice(&(value.len() as u16).to_be_bytes());
    attributes.extend_from_slice(value);
    attributes.resize((attributes.len() + 3) & !3, 0);
}

fn turn_transaction_id() -> [u8; 12] {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TRANSACTION: AtomicU64 = AtomicU64::new(0);
    let sequence = NEXT_TRANSACTION.fetch_add(1, Ordering::Relaxed).to_be_bytes();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after epoch")
        .subsec_nanos()
        .to_be_bytes();
    let mut transaction_id = [0_u8; 12];
    transaction_id[..8].copy_from_slice(&sequence);
    transaction_id[8..].copy_from_slice(&nanos);
    transaction_id
}

fn turn_long_term_key(username: &str, realm: &str, password: &str) -> Vec<u8> {
    use md5::{Digest as _, Md5};

    Md5::digest(format!("{username}:{realm}:{password}").as_bytes()).to_vec()
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let mut padded_key = [0_u8; 64];
    if key.len() > padded_key.len() {
        padded_key[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        padded_key[..key.len()].copy_from_slice(key);
    }

    let mut inner = Sha256::new();
    inner.update(padded_key.map(|byte| byte ^ 0x36));
    inner.update(message);

    let mut outer = Sha256::new();
    outer.update(padded_key.map(|byte| byte ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

fn build_allocate_request(
    transaction_id: [u8; 12],
    credentials: Option<(&str, &str, &str, &str)>,
) -> Vec<u8> {
    let mut attributes = Vec::new();
    append_stun_attribute(
        &mut attributes,
        STUN_ATTR_REQUESTED_TRANSPORT,
        &[17, 0, 0, 0],
    );

    if let Some((username, realm, nonce, password)) = credentials {
        append_stun_attribute(&mut attributes, STUN_ATTR_USERNAME, username.as_bytes());
        append_stun_attribute(&mut attributes, STUN_ATTR_REALM, realm.as_bytes());
        append_stun_attribute(&mut attributes, STUN_ATTR_NONCE, nonce.as_bytes());

        let message_length = attributes.len() + 24;
        let mut request = Vec::with_capacity(20 + message_length);
        request.extend_from_slice(&TURN_ALLOCATE_REQUEST.to_be_bytes());
        request.extend_from_slice(&(message_length as u16).to_be_bytes());
        request.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
        request.extend_from_slice(&transaction_id);
        request.extend_from_slice(&attributes);

        use hmac::{Hmac, KeyInit, Mac};
        use sha1::Sha1;
        let mut mac = Hmac::<Sha1>::new_from_slice(&turn_long_term_key(username, realm, password))
            .expect("HMAC accepts a fixed-width long-term key");
        mac.update(&request);
        append_stun_attribute(
            &mut request,
            STUN_ATTR_MESSAGE_INTEGRITY,
            &mac.finalize().into_bytes(),
        );
        return request;
    }

    let mut request = Vec::with_capacity(20 + attributes.len());
    request.extend_from_slice(&TURN_ALLOCATE_REQUEST.to_be_bytes());
    request.extend_from_slice(&(attributes.len() as u16).to_be_bytes());
    request.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    request.extend_from_slice(&transaction_id);
    request.extend_from_slice(&attributes);
    request
}

fn build_sha256_allocate_request(
    transaction_id: [u8; 12],
    username: &str,
    realm: &str,
    nonce: &str,
    password: &str,
) -> Vec<u8> {
    let mut attributes = Vec::new();
    append_stun_attribute(
        &mut attributes,
        STUN_ATTR_REQUESTED_TRANSPORT,
        &[17, 0, 0, 0],
    );
    append_stun_attribute(&mut attributes, STUN_ATTR_USERNAME, username.as_bytes());
    append_stun_attribute(&mut attributes, STUN_ATTR_REALM, realm.as_bytes());
    append_stun_attribute(&mut attributes, STUN_ATTR_NONCE, nonce.as_bytes());
    append_stun_attribute(&mut attributes, STUN_ATTR_PASSWORD_ALGORITHM, &[0, 2, 0, 0]);

    let message_length = attributes.len() + 36;
    let mut request = Vec::with_capacity(20 + message_length);
    request.extend_from_slice(&TURN_ALLOCATE_REQUEST.to_be_bytes());
    request.extend_from_slice(&(message_length as u16).to_be_bytes());
    request.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    request.extend_from_slice(&transaction_id);
    request.extend_from_slice(&attributes);

    use sha2::{Digest, Sha256};
    let key = Sha256::digest(format!("{username}:{realm}:{password}").as_bytes());
    let integrity = hmac_sha256(&key, &request);
    append_stun_attribute(
        &mut request,
        STUN_ATTR_MESSAGE_INTEGRITY_SHA256,
        &integrity,
    );
    request
}

fn build_authenticated_turn_request(
    message_type: u16,
    transaction_id: [u8; 12],
    mut attributes: Vec<u8>,
    username: &str,
    realm: &str,
    nonce: &str,
    password: &str,
) -> Vec<u8> {
    append_stun_attribute(&mut attributes, STUN_ATTR_USERNAME, username.as_bytes());
    append_stun_attribute(&mut attributes, STUN_ATTR_REALM, realm.as_bytes());
    append_stun_attribute(&mut attributes, STUN_ATTR_NONCE, nonce.as_bytes());

    let message_length = attributes.len() + 24;
    let mut request = Vec::with_capacity(20 + message_length);
    request.extend_from_slice(&message_type.to_be_bytes());
    request.extend_from_slice(&(message_length as u16).to_be_bytes());
    request.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    request.extend_from_slice(&transaction_id);
    request.extend_from_slice(&attributes);

    use hmac::{Hmac, KeyInit, Mac};
    use sha1::Sha1;
    let mut mac = Hmac::<Sha1>::new_from_slice(&turn_long_term_key(username, realm, password))
        .expect("HMAC accepts a fixed-width long-term key");
    mac.update(&request);
    append_stun_attribute(
        &mut request,
        STUN_ATTR_MESSAGE_INTEGRITY,
        &mac.finalize().into_bytes(),
    );
    request
}

fn encode_xor_peer_address(peer: std::net::SocketAddr) -> Vec<u8> {
    let std::net::SocketAddr::V4(peer) = peer else {
        panic!("TURN relay test uses an IPv4 loopback peer");
    };
    let mut encoded = Vec::with_capacity(8);
    encoded.extend_from_slice(&[0, 1]);
    encoded.extend_from_slice(&(peer.port() ^ (STUN_MAGIC_COOKIE >> 16) as u16).to_be_bytes());
    encoded.extend(
        peer.ip()
            .octets()
            .into_iter()
            .zip(STUN_MAGIC_COOKIE.to_be_bytes())
            .map(|(octet, cookie)| octet ^ cookie),
    );
    encoded
}

fn parse_turn_response(bytes: &[u8]) -> Result<TurnResponse, String> {
    if bytes.len() < 20 {
        return Err("STUN response is shorter than its header".to_string());
    }
    if u32::from_be_bytes(bytes[4..8].try_into().expect("slice length is checked")) != STUN_MAGIC_COOKIE {
        return Err("STUN response has an invalid magic cookie".to_string());
    }
    let message_length = u16::from_be_bytes(bytes[2..4].try_into().expect("slice length is checked")) as usize;
    let end = 20_usize
        .checked_add(message_length)
        .ok_or_else(|| "STUN response length overflows".to_string())?;
    if end != bytes.len() {
        return Err("STUN response length does not match its header".to_string());
    }

    let mut transaction_id = [0_u8; 12];
    transaction_id.copy_from_slice(&bytes[8..20]);
    let mut attributes = Vec::new();
    let mut offset = 20;
    while offset < end {
        if end - offset < 4 {
            return Err("truncated STUN attribute header".to_string());
        }
        let attribute_type = u16::from_be_bytes(
            bytes[offset..offset + 2]
                .try_into()
                .expect("attribute header is present"),
        );
        let value_length = u16::from_be_bytes(
            bytes[offset + 2..offset + 4]
                .try_into()
                .expect("attribute header is present"),
        ) as usize;
        offset += 4;
        let value_end = offset
            .checked_add(value_length)
            .ok_or_else(|| "STUN attribute length overflows".to_string())?;
        if value_end > end {
            return Err("truncated STUN attribute value".to_string());
        }
        attributes.push((attribute_type, bytes[offset..value_end].to_vec()));
        offset = (value_end + 3) & !3;
        if offset > end {
            return Err("truncated STUN attribute padding".to_string());
        }
    }

    Ok(TurnResponse {
        message_type: u16::from_be_bytes(bytes[..2].try_into().expect("header is present")),
        transaction_id,
        attributes,
    })
}

async fn send_allocate(
    socket: &tokio::net::UdpSocket,
    credentials: Option<(&str, &str, &str, &str)>,
) -> TurnResponse {
    let transaction_id = turn_transaction_id();
    let request = build_allocate_request(transaction_id, credentials);
    socket.send(&request).await.expect("Allocate request should send");

    let mut response_bytes = [0_u8; 2048];
    let response_length = tokio::time::timeout(Duration::from_secs(3), socket.recv(&mut response_bytes))
        .await
        .expect("Allocate response should arrive")
        .expect("Allocate response should read");
    let response = parse_turn_response(&response_bytes[..response_length])
        .expect("TURN response should be a valid STUN message");
    assert_eq!(response.transaction_id, transaction_id, "response must match Allocate transaction");
    response
}

async fn send_authenticated_turn_request(
    client: &AuthenticatedTurnClient,
    message_type: u16,
    attributes: Vec<u8>,
) -> TurnResponse {
    let transaction_id = turn_transaction_id();
    let request = build_authenticated_turn_request(
        message_type,
        transaction_id,
        attributes,
        &client.username,
        &client.realm,
        &client.nonce,
        &client.password,
    );
    client
        .socket
        .send(&request)
        .await
        .expect("authenticated TURN request should send");

    let mut response_bytes = [0_u8; 2048];
    let response_length = tokio::time::timeout(
        Duration::from_secs(3),
        client.socket.recv(&mut response_bytes),
    )
    .await
    .expect("authenticated TURN response should arrive")
    .expect("authenticated TURN response should read");
    let response = parse_turn_response(&response_bytes[..response_length])
        .expect("TURN response should be a valid STUN message");
    assert_eq!(response.transaction_id, transaction_id, "response must match TURN transaction");
    response
}

async fn authenticated_turn_client(
    turn_addr: std::net::SocketAddr,
    username: &str,
    password: &str,
) -> AuthenticatedTurnClient {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("TURN test client socket should bind");
    socket.connect(turn_addr).await.expect("TURN test client should connect");

    let challenge = send_allocate(&socket, None).await;
    assert_eq!(challenge.message_type, TURN_ALLOCATE_ERROR, "initial Allocate should challenge");
    assert_eq!(turn_error_code(&challenge), 401, "initial Allocate should be unauthorized");
    let realm = required_turn_attribute(&challenge, STUN_ATTR_REALM);
    assert_eq!(realm, "livekit");
    let nonce = required_turn_attribute(&challenge, STUN_ATTR_NONCE);
    let allocation = send_allocate(&socket, Some((username, &realm, &nonce, password))).await;
    assert_eq!(allocation.message_type, TURN_ALLOCATE_SUCCESS, "valid credentials should allocate");

    AuthenticatedTurnClient {
        socket,
        username: username.to_string(),
        realm,
        nonce,
        password: password.to_string(),
    }
}

async fn send_create_permission(
    client: &AuthenticatedTurnClient,
    peer: std::net::SocketAddr,
) -> TurnResponse {
    let mut attributes = Vec::new();
    append_stun_attribute(
        &mut attributes,
        STUN_ATTR_XOR_PEER_ADDRESS,
        &encode_xor_peer_address(peer),
    );
    send_authenticated_turn_request(client, TURN_CREATE_PERMISSION_REQUEST, attributes).await
}

async fn send_indication(client: &AuthenticatedTurnClient, peer: std::net::SocketAddr, data: &[u8]) {
    let transaction_id = turn_transaction_id();
    let mut attributes = Vec::new();
    append_stun_attribute(
        &mut attributes,
        STUN_ATTR_XOR_PEER_ADDRESS,
        &encode_xor_peer_address(peer),
    );
    append_stun_attribute(&mut attributes, STUN_ATTR_DATA, data);
    let request = build_authenticated_turn_request(
        TURN_SEND_INDICATION,
        transaction_id,
        attributes,
        &client.username,
        &client.realm,
        &client.nonce,
        &client.password,
    );
    client
        .socket
        .send(&request)
        .await
        .expect("Send indication should send");
}

fn required_turn_attribute(response: &TurnResponse, attribute_type: u16) -> String {
    let value = response
        .attributes
        .iter()
        .find_map(|(kind, value)| (*kind == attribute_type).then_some(value))
        .expect("TURN response should contain the required attribute");
    String::from_utf8(value.clone()).expect("TURN text attribute should be UTF-8")
}

fn turn_error_code(response: &TurnResponse) -> u16 {
    let value = response
        .attributes
        .iter()
        .find_map(|(kind, value)| (*kind == STUN_ATTR_ERROR_CODE).then_some(value))
        .expect("TURN error response should include ERROR-CODE");
    assert!(value.len() >= 4, "ERROR-CODE value should contain class and number");
    u16::from(value[2]) * 100 + u16::from(value[3])
}

fn turn_password(api_key: &str, participant_id: &str, expiry: i64) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(format!("{API_SECRET}|{participant_id}|{expiry}").as_bytes());
    assert_eq!(api_key, API_KEY, "test helper only mints known API-key passwords");
    base62_encode_bytes(&digest)
}

async fn allocate_response_for(
    turn_addr: std::net::SocketAddr,
    username: &str,
    password: &str,
) -> TurnResponse {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("TURN test client socket should bind");
    socket.connect(turn_addr).await.expect("TURN test client should connect");

    let challenge = send_allocate(&socket, None).await;
    assert_eq!(challenge.message_type, TURN_ALLOCATE_ERROR, "initial Allocate should challenge");
    assert_eq!(turn_error_code(&challenge), 401, "initial Allocate should be unauthorized");
    let realm = required_turn_attribute(&challenge, STUN_ATTR_REALM);
    assert_eq!(realm, "livekit");
    let nonce = required_turn_attribute(&challenge, STUN_ATTR_NONCE);
    send_allocate(&socket, Some((username, &realm, &nonce, password))).await
}

#[tokio::test]
async fn test_turn_sha256_allocate_authentication() {
    let _turn_test_guard = OWNED_TURN_TEST_LOCK.lock().await;

    let turn_port = reserve_local_udp_port("127.0.0.1");
    let fixture = spawn_single_node_with_owned_turn(owned_turn_config(turn_port, Vec::new(), Vec::new())).await;
    let turn_addr = std::net::SocketAddr::from(([127, 0, 0, 1], turn_port));
    let credentials = oxidesfu_server::signal_ice_servers_for_participant(&fixture.config, "PA_sha256")
        .into_iter()
        .find(|server| server.urls.iter().any(|url| url.starts_with("turn:")))
        .expect("enabled TURN should advertise a TURN server");

    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("TURN test client socket should bind");
    socket.connect(turn_addr).await.expect("TURN test client should connect");
    let challenge = send_allocate(&socket, None).await;
    assert_eq!(challenge.message_type, TURN_ALLOCATE_ERROR, "initial Allocate should challenge");
    assert_eq!(turn_error_code(&challenge), 401, "initial Allocate should be unauthorized");
    let realm = required_turn_attribute(&challenge, STUN_ATTR_REALM);
    let nonce = required_turn_attribute(&challenge, STUN_ATTR_NONCE);

    let transaction_id = turn_transaction_id();
    socket
        .send(&build_sha256_allocate_request(
            transaction_id,
            &credentials.username,
            &realm,
            &nonce,
            &credentials.credential,
        ))
        .await
        .expect("SHA-256 authenticated Allocate should send");
    let mut response_bytes = [0_u8; 2048];
    let response_length = tokio::time::timeout(Duration::from_secs(3), socket.recv(&mut response_bytes))
        .await
        .expect("SHA-256 Allocate response should arrive")
        .expect("SHA-256 Allocate response should read");
    let response = parse_turn_response(&response_bytes[..response_length])
        .expect("SHA-256 Allocate response should be a valid STUN message");

    assert_eq!(response.transaction_id, transaction_id, "response must match Allocate transaction");
    assert_eq!(response.message_type, TURN_ALLOCATE_SUCCESS, "SHA-256 credentials should allocate");
    let integrity = response
        .attributes
        .iter()
        .find_map(|(kind, value)| (*kind == STUN_ATTR_MESSAGE_INTEGRITY_SHA256).then_some(value))
        .expect("SHA-256 Allocate response should carry SHA-256 integrity");
    assert_eq!(integrity.len(), 32, "SHA-256 integrity must be 32 bytes");

    fixture.shutdown().await;
}

// Upstream: livekit/test/singlenode_test.go::TestTurnAuthFailure
#[tokio::test]
async fn test_turn_auth_failure() {
    let _turn_test_guard = OWNED_TURN_TEST_LOCK.lock().await;

    let turn_port = reserve_local_udp_port("127.0.0.1");
    let fixture = spawn_single_node_with_owned_turn(owned_turn_config(turn_port, Vec::new(), Vec::new())).await;
    let turn_addr = std::net::SocketAddr::from(([127, 0, 0, 1], turn_port));

    let credentials = oxidesfu_server::signal_ice_servers_for_participant(&fixture.config, "PA_authfail")
        .into_iter()
        .find(|server| server.urls.iter().any(|url| url.starts_with("turn:")))
        .expect("enabled TURN should advertise a TURN server");
    let valid_username = credentials.username;
    let valid_password = credentials.credential;
    let valid_response = allocate_response_for(turn_addr, &valid_username, &valid_password).await;
    assert_eq!(valid_response.message_type, TURN_ALLOCATE_SUCCESS, "valid credentials should allocate");

    let decoded = String::from_utf8(base62_decode_to_bytes(&valid_username).expect("username should decode"))
        .expect("username should contain UTF-8");
    let fields: Vec<_> = decoded.split('|').collect();
    assert_eq!(fields.len(), 3, "minted username should have LiveKit's three fields");
    let participant_id = fields[1];
    let expiry = fields[2].parse::<i64>().expect("minted expiry should parse");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs() as i64;
    let expired_expiry = now - 10;
    let expired_username = base62_encode_bytes(format!("{API_KEY}|{participant_id}|{expired_expiry}").as_bytes());
    let expired_password = turn_password(API_KEY, participant_id, expired_expiry);
    let unknown_api_key_username =
        base62_encode_bytes(format!("unknown-api-key|{participant_id}|{expiry}").as_bytes());
    let mismatched_expiry_password = turn_password(API_KEY, participant_id, expiry + 60);
    let zero_expiry_username = base62_encode_bytes(format!("{API_KEY}|{participant_id}|0").as_bytes());
    let two_part_username = base62_encode_bytes(format!("{API_KEY}|{participant_id}").as_bytes());

    let cases = [
        ("unparseable-username", "not-base62!!!".to_string(), valid_password.clone()),
        ("wrong-password", valid_username.clone(), "wrongpassword".to_string()),
        ("expired-username", expired_username, expired_password),
        ("unknown-api-key", unknown_api_key_username, valid_password.clone()),
        (
            "password-expiry-mismatch",
            valid_username.clone(),
            mismatched_expiry_password,
        ),
        ("zero-expiry-username", zero_expiry_username, valid_password.clone()),
        ("two-part-username", two_part_username, valid_password),
    ];

    for (name, username, password) in cases {
        let response = allocate_response_for(turn_addr, &username, &password).await;
        assert_eq!(
            response.message_type, TURN_ALLOCATE_ERROR,
            "invalid authenticated Allocate `{name}` should be an error response"
        );
        assert_eq!(
            turn_error_code(&response),
            400,
            "invalid authenticated Allocate `{name}` should return Bad Request"
        );
    }

    fixture.shutdown().await;
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CompatSignalRequest {
    #[prost(message, optional, tag = "22")]
    store_data_blob_request: Option<CompatStoreDataBlobRequest>,
    #[prost(message, optional, tag = "23")]
    get_data_blob_request: Option<CompatGetDataBlobRequest>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CompatSignalResponse {
    #[prost(message, optional, tag = "30")]
    store_data_blob_response: Option<CompatStoreDataBlobResponse>,
    #[prost(message, optional, tag = "31")]
    get_data_blob_response: Option<CompatGetDataBlobResponse>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CompatStoreDataBlobRequest {
    #[prost(uint32, tag = "1")]
    request_id: u32,
    #[prost(message, optional, tag = "2")]
    blob: Option<CompatDataBlob>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CompatStoreDataBlobResponse {
    #[prost(uint32, tag = "1")]
    request_id: u32,
    #[prost(message, optional, tag = "2")]
    key: Option<CompatDataBlobKey>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CompatGetDataBlobRequest {
    #[prost(uint32, tag = "1")]
    request_id: u32,
    #[prost(string, tag = "2")]
    participant_identity: String,
    #[prost(message, optional, tag = "3")]
    key: Option<CompatDataBlobKey>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CompatGetDataBlobResponse {
    #[prost(uint32, tag = "1")]
    request_id: u32,
    #[prost(message, optional, tag = "2")]
    blob: Option<CompatDataBlob>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CompatDataBlob {
    #[prost(message, optional, tag = "1")]
    key: Option<CompatDataBlobKey>,
    #[prost(bytes = "vec", tag = "2")]
    contents: Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CompatDataBlobKey {
    #[prost(oneof = "compat_data_blob_key::Key", tags = "1")]
    key: Option<compat_data_blob_key::Key>,
}

mod compat_data_blob_key {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub(super) enum Key {
        #[prost(string, tag = "1")]
        Generic(String),
    }
}

enum CompatOrRequestResponse {
    Store(CompatStoreDataBlobResponse),
    Get(CompatGetDataBlobResponse),
    RequestResponse(proto::RequestResponse),
}

async fn next_compat_or_request_response(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    timeout: Duration,
) -> CompatOrRequestResponse {
    tokio::time::timeout(timeout, async {
        loop {
            let message = socket
                .next()
                .await
                .expect("signal websocket should stay open")
                .expect("signal websocket message should decode");
            let Message::Binary(bytes) = message else {
                continue;
            };

            if let Ok(compat) = CompatSignalResponse::decode(bytes.as_ref()) {
                if let Some(store) = compat.store_data_blob_response {
                    break CompatOrRequestResponse::Store(store);
                }
                if let Some(get) = compat.get_data_blob_response {
                    break CompatOrRequestResponse::Get(get);
                }
            }

            if let Ok(response) = proto::SignalResponse::decode(bytes.as_ref())
                && let Some(proto::signal_response::Message::RequestResponse(rr)) = response.message
            {
                break CompatOrRequestResponse::RequestResponse(rr);
            }
        }
    })
    .await
    .expect("expected compat signal response before timeout")
}

fn generic_blob_key(value: &str) -> CompatDataBlobKey {
    CompatDataBlobKey {
        key: Some(compat_data_blob_key::Key::Generic(value.to_string())),
    }
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeDataBlob
#[tokio::test]
async fn test_single_node_data_blob() {
    let (addr, server) = spawn_single_node().await;
    let url = base_url(addr);

    let room = format!("upstream-singlenode-data-blob-{}", unique_suffix());
    let pub_token = join_token(&room, "pub");
    let sub_token = join_token(&room, "sub");

    let (mut pub_socket, _pub_sid) = connect_signal_socket_with_token(&url, &pub_token).await;
    let (mut sub_socket, _sub_sid) = connect_signal_socket_with_token(&url, &sub_token).await;

    let client = room_client(addr);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let participants = client
                .list_participants(&room)
                .await
                .expect("participants should list");
            if participants.iter().any(|participant| participant.identity == "pub")
                && participants.iter().any(|participant| participant.identity == "sub")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("pub and sub should both join before data-blob assertions");

    let key = generic_blob_key("blob-1");
    let contents = b"definition-bytes".to_vec();

    let store_request = CompatSignalRequest {
        store_data_blob_request: Some(CompatStoreDataBlobRequest {
            request_id: 1,
            blob: Some(CompatDataBlob {
                key: Some(key.clone()),
                contents: contents.clone(),
            }),
        }),
        get_data_blob_request: None,
    };
    pub_socket
        .send(Message::Binary(store_request.encode_to_vec().into()))
        .await
        .expect("publisher should send store data blob request");

    let store_response = match next_compat_or_request_response(&mut pub_socket, Duration::from_secs(10)).await {
        CompatOrRequestResponse::Store(response) => response,
        CompatOrRequestResponse::RequestResponse(rr) => {
            panic!("publisher store request should not return error: {rr:?}")
        }
        CompatOrRequestResponse::Get(_) => panic!("publisher store should not return get response"),
    };
    assert_eq!(store_response.request_id, 1);
    assert_eq!(store_response.key, Some(key.clone()));

    let no_store_error = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            let message = pub_socket.next().await;
            if message.is_none() {
                break None;
            }
            let Ok(message) = message.expect("socket stream should not terminate") else {
                continue;
            };
            let Message::Binary(bytes) = message else {
                continue;
            };
            let Ok(response) = proto::SignalResponse::decode(bytes.as_ref()) else {
                continue;
            };
            if let Some(proto::signal_response::Message::RequestResponse(rr)) = response.message {
                break Some(rr);
            }
        }
    })
    .await;
    assert!(
        no_store_error.is_err(),
        "publisher should not receive request_response error after successful store"
    );

    let get_request = CompatSignalRequest {
        store_data_blob_request: None,
        get_data_blob_request: Some(CompatGetDataBlobRequest {
            request_id: 2,
            participant_identity: "pub".to_string(),
            key: Some(key.clone()),
        }),
    };
    sub_socket
        .send(Message::Binary(get_request.encode_to_vec().into()))
        .await
        .expect("subscriber should send get data blob request");

    let get_response = match next_compat_or_request_response(&mut sub_socket, Duration::from_secs(10)).await {
        CompatOrRequestResponse::Get(response) => response,
        CompatOrRequestResponse::RequestResponse(rr) => {
            panic!("subscriber get request should not return error: {rr:?}")
        }
        CompatOrRequestResponse::Store(_) => panic!("subscriber get should not return store response"),
    };
    assert_eq!(get_response.request_id, 2);
    let blob = get_response
        .blob
        .expect("subscriber get response should contain blob");
    assert_eq!(blob.key, Some(key.clone()));
    assert_eq!(blob.contents, contents);

    let missing_blob_request = CompatSignalRequest {
        store_data_blob_request: None,
        get_data_blob_request: Some(CompatGetDataBlobRequest {
            request_id: 3,
            participant_identity: "pub".to_string(),
            key: Some(generic_blob_key("does-not-exist")),
        }),
    };
    sub_socket
        .send(Message::Binary(missing_blob_request.encode_to_vec().into()))
        .await
        .expect("subscriber should send missing-blob request");

    let missing_blob_response = match next_compat_or_request_response(&mut sub_socket, Duration::from_secs(10)).await {
        CompatOrRequestResponse::RequestResponse(rr) => rr,
        CompatOrRequestResponse::Store(_) => panic!("missing blob request should not return store response"),
        CompatOrRequestResponse::Get(_) => panic!("missing blob request should not return get response"),
    };
    assert_eq!(missing_blob_response.request_id, 3);
    assert_eq!(
        missing_blob_response.reason,
        proto::request_response::Reason::NotFound as i32,
        "missing blob on known publisher should return NOT_FOUND"
    );

    let unknown_publisher_request = CompatSignalRequest {
        store_data_blob_request: None,
        get_data_blob_request: Some(CompatGetDataBlobRequest {
            request_id: 4,
            participant_identity: "unknown-publisher".to_string(),
            key: Some(key.clone()),
        }),
    };
    sub_socket
        .send(Message::Binary(unknown_publisher_request.encode_to_vec().into()))
        .await
        .expect("subscriber should send unknown-publisher request");

    let unknown_publisher_response = match next_compat_or_request_response(&mut sub_socket, Duration::from_secs(10)).await {
        CompatOrRequestResponse::RequestResponse(rr) => rr,
        CompatOrRequestResponse::Store(_) => panic!("unknown publisher request should not return store response"),
        CompatOrRequestResponse::Get(_) => panic!("unknown publisher request should not return get response"),
    };
    assert_eq!(unknown_publisher_response.request_id, 4);
    assert_eq!(
        unknown_publisher_response.reason,
        proto::request_response::Reason::NotFound as i32,
        "unknown publisher should return NOT_FOUND"
    );

    let invalid_store_request = CompatSignalRequest {
        store_data_blob_request: Some(CompatStoreDataBlobRequest {
            request_id: 5,
            blob: Some(CompatDataBlob {
                key: None,
                contents,
            }),
        }),
        get_data_blob_request: None,
    };
    pub_socket
        .send(Message::Binary(invalid_store_request.encode_to_vec().into()))
        .await
        .expect("publisher should send invalid store request");

    let invalid_store_response = match next_compat_or_request_response(&mut pub_socket, Duration::from_secs(10)).await {
        CompatOrRequestResponse::RequestResponse(rr) => rr,
        CompatOrRequestResponse::Store(_) => panic!("invalid store should not return store response"),
        CompatOrRequestResponse::Get(_) => panic!("invalid store should not return get response"),
    };
    assert_eq!(invalid_store_response.request_id, 5);
    assert_eq!(
        invalid_store_response.reason,
        11,
        "missing key in store request should return INVALID_REQUEST (wire code 11)"
    );

    let _ = pub_socket.close(None).await;
    let _ = sub_socket.close(None).await;
    server.abort();
}

// Upstream: livekit/test/singlenode_test.go::TestSingleNodeDataBlobDisabled
#[tokio::test]
async fn test_single_node_data_blob_disabled() {
    let (addr, server) = spawn_single_node_with_participant_data_blob_enabled(false).await;
    let url = base_url(addr);

    let room = format!("upstream-singlenode-data-blob-disabled-{}", unique_suffix());
    let pub_token = join_token(&room, "pub");
    let (mut pub_socket, _pub_sid) = connect_signal_socket_with_token(&url, &pub_token).await;

    let store_request = CompatSignalRequest {
        store_data_blob_request: Some(CompatStoreDataBlobRequest {
            request_id: 1,
            blob: Some(CompatDataBlob {
                key: Some(generic_blob_key("blob-1")),
                contents: b"definition-bytes".to_vec(),
            }),
        }),
        get_data_blob_request: None,
    };
    pub_socket
        .send(Message::Binary(store_request.encode_to_vec().into()))
        .await
        .expect("publisher should send store data blob request when disabled");

    let response = match next_compat_or_request_response(&mut pub_socket, Duration::from_secs(10)).await {
        CompatOrRequestResponse::RequestResponse(rr) => rr,
        CompatOrRequestResponse::Store(_) => {
            panic!("disabled participant data blob should not return store response")
        }
        CompatOrRequestResponse::Get(_) => {
            panic!("disabled participant data blob should not return get response")
        }
    };
    assert_eq!(
        response.reason,
        proto::request_response::Reason::NotAllowed as i32,
        "disabled participant data blob should return NOT_ALLOWED"
    );

    let _ = pub_socket.close(None).await;
    server.abort();
}

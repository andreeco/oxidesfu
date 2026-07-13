use super::*;

// Upstream: livekit/test/multinode_roomservice_test.go::TestMultiNodeRoomList
#[tokio::test]
async fn test_multi_node_room_list() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));

    let room_a = format!("upstream-multinode-room-list-a-{}", unique_suffix());
    let room_b = format!("upstream-multinode-room-list-b-{}", unique_suffix());

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
    assert_eq!(all_rooms.len(), 2, "list-all should include exactly the two created rooms");
    let names: HashSet<_> = all_rooms.iter().map(|room| room.name.as_str()).collect();
    assert!(names.contains(room_a.as_str()));
    assert!(names.contains(room_b.as_str()));

    let specific = client
        .list_rooms(vec![room_b.clone()])
        .await
        .expect("specific room should list");
    assert_eq!(specific.len(), 1, "specific room filter should return one room");
    assert_eq!(specific[0].name, room_b);

    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_roomservice_test.go::TestMultiNodeUpdateRoomMetadata
#[tokio::test]
async fn test_multi_node_update_room_metadata() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    let empty_room = format!("upstream-multinode-empty-metadata-{}", unique_suffix());
    client
        .create_room(&empty_room, CreateRoomOptions::default())
        .await
        .expect("empty room should create");
    let updated = client
        .update_room_metadata(&empty_room, "updated metadata")
        .await
        .expect("empty room metadata should update");
    assert_eq!(updated.metadata, "updated metadata");

    let occupied_room = format!("upstream-multinode-occupied-metadata-{}", unique_suffix());
    let signal = connect_signal(&node_a_url, &occupied_room, "metadata-participant").await;
    let updated = client
        .update_room_metadata(&occupied_room, "updated metadata")
        .await
        .expect("occupied room metadata should update");
    assert_eq!(updated.metadata, "updated metadata");
    signal.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_roomservice_test.go::TestMultiNodeRemoveParticipant
#[tokio::test]
async fn test_multi_node_remove_participant() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let room = format!("upstream-multinode-remove-{}", unique_suffix());
    let signal = connect_signal(&node_a_url, &room, "remove-me").await;
    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    client
        .remove_participant(&room, "remove-me")
        .await
        .expect("participant should remove");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let listed = client
            .list_participants(&room)
            .await
            .expect("participants should list");
        if listed.is_empty() || tokio::time::Instant::now() >= deadline {
            assert!(listed.is_empty());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    signal.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_roomservice_test.go::TestMultiNodeUpdateParticipantMetadata
#[tokio::test]
async fn test_multi_node_update_participant_metadata() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let room = format!("upstream-multinode-participant-metadata-{}", unique_suffix());
    let signal = connect_signal(&node_a_url, &room, "metadata-user").await;
    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    let updated = client
        .update_participant(
            &room,
            "metadata-user",
            UpdateParticipantOptions {
                metadata: "metadata-v2".to_string(),
                name: "Metadata User V2".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("participant should update");
    assert_eq!(updated.metadata, "metadata-v2");
    assert_eq!(updated.name, "Metadata User V2");
    signal.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_roomservice_test.go::TestMultiNodeMutePublishedTrack
#[tokio::test]
async fn test_multi_node_mute_published_track() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("upstream-multinode-mute-{}", unique_suffix());
    let identity = "mute-published-track";

    let (publisher_room, _publisher_events) = connect_room(&node_a_url, &room, identity, true).await;
    let audio_sid = publish_audio_track(&publisher_room, "audio").await.0;
    let video_sid = publish_video_track(&publisher_room, "video").await;

    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let participant = client
                .get_participant(&room, identity)
                .await
                .expect("participant should be queryable");
            if participant.tracks.len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("participant should expose two published tracks before mute");

    let muted = client
        .mute_published_track(&room, identity, &audio_sid, true)
        .await
        .expect("track should mute");
    assert_eq!(muted.sid, audio_sid);
    assert!(muted.muted);

    let participant_after_mute = client
        .get_participant(&room, identity)
        .await
        .expect("participant should still be queryable after mute");
    let track_states: HashMap<_, _> = participant_after_mute
        .tracks
        .iter()
        .map(|track| (track.sid.as_str(), track.muted))
        .collect();
    assert_eq!(track_states.get(audio_sid.as_str()), Some(&true));
    assert_eq!(track_states.get(video_sid.as_str()), Some(&false));

    let _ = publisher_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultiNodeRouting
#[tokio::test]
async fn test_multi_node_routing() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let room = format!("upstream-multinode-routing-{}", unique_suffix());

    let room_client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    room_client
        .create_room(&room, CreateRoomOptions::default())
        .await
        .expect("multinode routing room should create explicitly");

    let publisher_token = join_token_with(
        &room,
        "c1",
        "c1",
        "metadatac1",
        HashMap::new(),
        VideoGrants {
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        },
    );
    let subscriber_token = join_token_with(
        &room,
        "c2",
        "c2",
        "metadatac2",
        HashMap::new(),
        VideoGrants {
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        },
    );

    let (publisher_room, _publisher_events) = connect_room_with_token(&node_a_url, &publisher_token, true).await;
    let (subscriber_room, mut subscriber_events) = connect_room_with_token(&node_b_url, &subscriber_token, true).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let publisher_sees_subscriber = publisher_room
                .remote_participants()
                .contains_key(&"c2".into());
            let subscriber_sees_publisher = subscriber_room
                .remote_participants()
                .contains_key(&"c1".into());
            if publisher_sees_subscriber && subscriber_sees_publisher {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("both cross-node participants should connect before publish");

    let track_sid = publish_audio_track(&publisher_room, "webcam").await.0;
    let subscriptions = wait_for_track_subscribed_count(&mut subscriber_events, 1).await;
    assert_eq!(subscriptions[0].sid().to_string(), track_sid);

    let remote_c1 = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(remote) = subscriber_room.remote_participants().get(&"c1".into()).cloned() {
                if remote.name().to_string() == "c1" && remote.metadata() == "metadatac1" {
                    break remote;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("subscriber should eventually observe publisher identity+metadata");
    assert_eq!(remote_c1.name().to_string(), "c1");
    assert_eq!(remote_c1.metadata(), "metadatac1");

    let _ = publisher_room.close().await;
    let _ = subscriber_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestConnectWithoutCreation
#[tokio::test]
async fn test_connect_without_creation() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let room = format!("upstream-connect-without-create-{}", unique_suffix());
    connect_signal(&node_a_url, &room, "autocreated-user").await.close().await;
    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    let participants = client
        .list_participants(&room)
        .await
        .expect("autocreated room should be visible");
    assert!(participants.is_empty() || participants.iter().any(|p| p.identity == "autocreated-user"));
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultinodePublishingUponJoining
#[tokio::test]
async fn test_multinode_publishing_upon_joining() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("upstream-multinode-publishing-upon-joining-{}", unique_suffix());
    let (c1_room, mut c1_events) = connect_room(&node_a_url, &room, "puj_1", true).await;
    let (c2_room, _c2_events) = connect_room(&node_b_url, &room, "puj_2", true).await;
    let (c3_room, mut c3_events) = connect_room(&node_a_url, &room, "puj_3", true).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c1_ready = c1_room.remote_participants().contains_key(&"puj_2".into())
                && c1_room.remote_participants().contains_key(&"puj_3".into());
            let c2_ready = c2_room.remote_participants().contains_key(&"puj_1".into())
                && c2_room.remote_participants().contains_key(&"puj_3".into());
            let c3_ready = c3_room.remote_participants().contains_key(&"puj_1".into())
                && c3_room.remote_participants().contains_key(&"puj_2".into());
            if c1_ready && c2_ready && c3_ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("all three participants should connect before publish assertions");

    let _c1_audio_sid = publish_audio_track(&c1_room, "webcam-audio").await.0;
    let _c1_video_sid = publish_video_track(&c1_room, "webcam-video").await;
    let _c2_audio_sid = publish_audio_track(&c2_room, "webcam-audio").await.0;
    let _c2_video_sid = publish_video_track(&c2_room, "webcam-video").await;

    tokio::time::timeout(Duration::from_secs(10), async {
        let mut c3_from_c1 = 0usize;
        let mut c3_from_c2 = 0usize;
        loop {
            let event = c3_events.recv().await.expect("c3 room events should stay open");
            if let RoomEvent::TrackSubscribed { participant, .. } = event {
                match participant.identity().to_string().as_str() {
                    "puj_1" => c3_from_c1 += 1,
                    "puj_2" => c3_from_c2 += 1,
                    _ => {}
                }
                if c3_from_c1 >= 2 && c3_from_c2 >= 2 {
                    break;
                }
            }
        }
    })
    .await
    .expect("c3 should subscribe to two tracks from c1 and two tracks from c2");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = c2_room.close().await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c3_c1_tracks = c3_room
                .remote_participants()
                .get(&"puj_1".into())
                .map(|participant| participant.track_publications().len())
                .unwrap_or(0);
            let c3_c2_tracks = c3_room
                .remote_participants()
                .get(&"puj_2".into())
                .map(|participant| participant.track_publications().len())
                .unwrap_or(0);
            let c1_c2_tracks = c1_room
                .remote_participants()
                .get(&"puj_2".into())
                .map(|participant| participant.track_publications().len())
                .unwrap_or(0);

            if c3_c1_tracks == 2 && c3_c2_tracks == 0 && c1_c2_tracks == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("after c2 leaves, c3 should keep only c1 tracks and c1 should have no c2 tracks");

    let (c2_rejoin_room, _c2_rejoin_events) = connect_room(&node_a_url, &room, "puj_2", true).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c2_sees_others = c2_rejoin_room.remote_participants().contains_key(&"puj_1".into())
                && c2_rejoin_room.remote_participants().contains_key(&"puj_3".into());
            if c2_sees_others {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("rejoined c2 should reconnect to existing room participants");

    let _c2_rejoin_audio_sid = publish_audio_track(&c2_rejoin_room, "webcam-audio").await.0;
    let _c2_rejoin_video_sid = publish_video_track(&c2_rejoin_room, "webcam-video").await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c3_c2_tracks = c3_room
                .remote_participants()
                .get(&"puj_2".into())
                .map(|participant| participant.track_publications().len())
                .unwrap_or(0);
            let c1_c2_tracks = c1_room
                .remote_participants()
                .get(&"puj_2".into())
                .map(|participant| participant.track_publications().len())
                .unwrap_or(0);
            if c3_c2_tracks == 2 && c1_c2_tracks == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("after c2 rejoins and republishes, c3 and c1 should each have two c2 tracks");

    // Additive parity check: c1 should have received two TrackSubscribed events from rejoined c2.
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut subscribed_from_c2 = 0usize;
        loop {
            let event = c1_events.recv().await.expect("c1 room events should stay open");
            if let RoomEvent::TrackSubscribed { participant, .. } = event
                && participant.identity().to_string() == "puj_2"
            {
                subscribed_from_c2 += 1;
                if subscribed_from_c2 >= 2 {
                    break;
                }
            }
        }
    })
    .await
    .expect("c1 should subscribe to two tracks from rejoined c2");

    let _ = c2_rejoin_room.close().await;
    let _ = c1_room.close().await;
    let _ = c3_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

/// Regression coverage for the distributed participant lifecycle shared by the
/// upstream publishing and data-publishing scenarios. Keep this focused on
/// presence propagation; track/data propagation has separate upstream contracts below.
#[tokio::test]
async fn test_multinode_remote_participant_rejoin_is_visible_to_existing_participants() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("multinode-remote-rejoin-{}", unique_suffix());
    let (c1_room, _c1_events) = connect_room(&node_a_url, &room, "rejoin_c1", true).await;
    let (c2_room, _c2_events) = connect_room(&node_b_url, &room, "rejoin_c2", true).await;
    let (c3_room, _c3_events) = connect_room(&node_a_url, &room, "rejoin_c3", true).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c1_sees_c2 = c1_room.remote_participants().contains_key(&"rejoin_c2".into());
            let c3_sees_c2 = c3_room.remote_participants().contains_key(&"rejoin_c2".into());
            if c1_sees_c2 && c3_sees_c2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("existing participants should observe the remote participant's initial join");

    let _ = c2_room.close().await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c1_sees_c2 = c1_room.remote_participants().contains_key(&"rejoin_c2".into());
            let c3_sees_c2 = c3_room.remote_participants().contains_key(&"rejoin_c2".into());
            if !c1_sees_c2 && !c3_sees_c2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("existing participants should observe remote participant removal");

    let (c2_rejoin_room, _c2_rejoin_events) =
        connect_room(&node_a_url, &room, "rejoin_c2", true).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c1_sees_c2 = c1_room.remote_participants().contains_key(&"rejoin_c2".into());
            let c3_sees_c2 = c3_room.remote_participants().contains_key(&"rejoin_c2".into());
            if c1_sees_c2 && c3_sees_c2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("existing participants should observe the same remote identity after rejoin");

    let _ = c2_rejoin_room.close().await;
    let _ = c1_room.close().await;
    let _ = c3_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultinodeReceiveBeforePublish
#[tokio::test]
async fn test_multinode_receive_before_publish() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("upstream-multinode-receive-before-publish-{}", unique_suffix());
    let (c1_room, mut c1_events) = connect_room(&node_a_url, &room, "rbp_1", true).await;
    let (c2_room, mut c2_events) = connect_room(&node_a_url, &room, "rbp_2", true).await;

    let (_c1_audio_sid, c1_audio_source) = publish_audio_track(&c1_room, "webcam-audio").await;
    let _c1_video_sid = publish_video_track(&c1_room, "webcam-video").await;

    let c2_remote_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
        let mut subscribed_from_c1 = 0usize;
        let mut audio_track = None;
        loop {
            let event = c2_events.recv().await.expect("c2 room events should stay open");
            if let RoomEvent::TrackSubscribed {
                track,
                publication,
                participant,
            } = event
                && participant.identity().to_string() == "rbp_1"
            {
                subscribed_from_c1 += 1;
                if let livekit::track::RemoteTrack::Audio(track) = track
                    && publication.name() == "webcam-audio"
                {
                    audio_track = Some(track);
                }
                if subscribed_from_c1 >= 2 {
                    break audio_track.expect("c2 should have subscribed to c1 audio track");
                }
            }
        }
    })
    .await
    .expect("c2 should subscribe to c1 media before timeout");

    let mut c2_audio_stream = NativeAudioStream::new(c2_remote_audio_track.rtc_track(), 48_000, 1);

    let frame = AudioFrame {
        data: vec![777_i16; 480].into(),
        sample_rate: 48_000,
        num_channels: 1,
        samples_per_channel: 480,
    };
    c1_audio_source
        .capture_frame(&frame)
        .await
        .expect("c1 audio source should accept frame for downstream flow check");

    let received_frame = tokio::time::timeout(Duration::from_secs(8), c2_audio_stream.next()).await;
    assert!(
        matches!(received_frame, Ok(Some(_))),
        "c2 should receive audio media bytes from c1 before publishing its own tracks"
    );

    let (_c2_audio_sid, _c2_audio_source) = publish_audio_track(&c2_room, "c2-audio").await;
    let _c2_video_sid = publish_video_track(&c2_room, "c2-video").await;

    tokio::time::timeout(Duration::from_secs(10), async {
        let mut subscribed_from_c2 = 0usize;
        loop {
            let event = c1_events.recv().await.expect("c1 room events should stay open");
            if let RoomEvent::TrackSubscribed { participant, .. } = event
                && participant.identity().to_string() == "rbp_2"
            {
                subscribed_from_c2 += 1;
                if subscribed_from_c2 == 2 {
                    break;
                }
            }
        }
    })
    .await
    .expect("c1 should subscribe to both c2 tracks");

    let _ = c2_room.close().await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if c1_room.remote_participants().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("c1 should observe immediate remote-participant removal after c2 leave");

    let _ = c1_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultinodeReconnectAfterNodeShutdown
#[tokio::test]
async fn test_multinode_reconnect_after_node_shutdown() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("upstream-multinode-reconnect-after-shutdown-{}", unique_suffix());
    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));

    let node_b_port = node_b_url
        .strip_prefix("http://127.0.0.1:")
        .expect("node B base URL should use localhost http form")
        .parse::<u16>()
        .expect("node B port should parse");
    client
        .create_room(
            &room,
            CreateRoomOptions {
                node_id: format!("oxidesfu-local-{node_b_port}"),
                ..Default::default()
            },
        )
        .await
        .expect("room creation with explicit node_id should succeed");

    let (c1_room, _c1_events) = connect_room(&node_a_url, &room, "c1", false).await;
    let (c2_room, _c2_events) = connect_room(&node_b_url, &room, "c2", false).await;

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
    .expect("c1 and c2 should both connect before shutdown");

    let _ = c1_room.close().await;
    let _ = c2_room.close().await;

    let _ = node_b.kill().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (c3_room, _c3_events) = connect_room(&node_a_url, &room, "c3", false).await;
    let _ = c3_room.close().await;

    let _ = node_a.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultinodeDataPublishing
#[tokio::test]
async fn test_multinode_data_publishing() {
    async fn wait_connected_pair(a: &Room, b: &Room, a_sees: &str, b_sees: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let a_ready = a.remote_participants().contains_key(&a_sees.into());
                let b_ready = b.remote_participants().contains_key(&b_sees.into());
                if a_ready && b_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("rooms should connect and observe each other");
    }

    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    // scenarioDataPublish
    {
        let room = format!("upstream-multinode-data-publish-{}", unique_suffix());
        let (c1_room, mut c1_events) = connect_room(&node_a_url, &room, "dp1", true).await;
        let (c2_room, mut c2_events) = connect_room(&node_b_url, &room, "dp2", true).await;
        wait_connected_pair(&c1_room, &c2_room, "dp2", "dp1").await;

        c1_room
            .local_participant()
            .publish_data(DataPacket {
                payload: b"test bytes".to_vec(),
                topic: None,
                reliable: true,
                destination_identities: vec![],
            })
            .await
            .expect("c1 should publish reliable data packet");

        let (payload, _topic, kind) = next_data_received(&mut c2_events).await;
        assert_eq!(payload, b"test bytes".to_vec());
        assert_eq!(kind, DataPacketKind::Reliable);

        let _ = c1_room.close().await;
        let _ = c2_room.close().await;
        drop(c1_events);
    }

    // scenarioDataUnlabeledPublish (payload-only assertion matches upstream callback)
    {
        let room = format!("upstream-multinode-data-unlabeled-{}", unique_suffix());
        let (c1_room, _c1_events) = connect_room(&node_a_url, &room, "dup1", true).await;
        let (c2_room, mut c2_events) = connect_room(&node_b_url, &room, "dup2", true).await;
        wait_connected_pair(&c1_room, &c2_room, "dup2", "dup1").await;

        c1_room
            .local_participant()
            .publish_data(DataPacket {
                payload: b"test unlabeled bytes".to_vec(),
                topic: None,
                reliable: true,
                destination_identities: vec![],
            })
            .await
            .expect("c1 should publish unlabeled-style data payload");

        let (payload, _topic, kind) = next_data_received(&mut c2_events).await;
        assert_eq!(payload, b"test unlabeled bytes".to_vec());
        assert_eq!(kind, DataPacketKind::Reliable);

        let _ = c1_room.close().await;
        let _ = c2_room.close().await;
    }

    // scenarioDataTracksPublishingUponJoining
    {
        let room = format!("upstream-multinode-data-tracks-upon-join-{}", unique_suffix());
        let (c1_room, mut c1_events) = connect_room(&node_a_url, &room, "dtpuj_1", true).await;
        let (c2_room, mut c2_events) = connect_room(&node_b_url, &room, "dtpuj_2", true).await;
        let (c3_room, mut c3_events) = connect_room(&node_a_url, &room, "dtpuj_3", true).await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let c1_ready = c1_room.remote_participants().contains_key(&"dtpuj_2".into())
                    && c1_room.remote_participants().contains_key(&"dtpuj_3".into());
                let c2_ready = c2_room.remote_participants().contains_key(&"dtpuj_1".into())
                    && c2_room.remote_participants().contains_key(&"dtpuj_3".into());
                let c3_ready = c3_room.remote_participants().contains_key(&"dtpuj_1".into())
                    && c3_room.remote_participants().contains_key(&"dtpuj_2".into());
                if c1_ready && c2_ready && c3_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("all data-track scenario participants should connect");

        let c1_dt1 = c1_room
            .local_participant()
            .publish_data_track("c1-data-1")
            .await
            .expect("c1 data track 1 publish should succeed");
        let c1_dt2 = c1_room
            .local_participant()
            .publish_data_track("c1-data-2")
            .await
            .expect("c1 data track 2 publish should succeed");
        let c2_dt1 = c2_room
            .local_participant()
            .publish_data_track("c2-data-1")
            .await
            .expect("c2 data track 1 publish should succeed");
        let c2_dt2 = c2_room
            .local_participant()
            .publish_data_track("c2-data-2")
            .await
            .expect("c2 data track 2 publish should succeed");

        let c3_tracks = tokio::time::timeout(Duration::from_secs(10), async {
            let mut tracks = Vec::new();
            while tracks.len() < 4 {
                let event = c3_events.recv().await.expect("c3 events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    tracks.push(track);
                }
            }
            tracks
        })
        .await
        .expect("c3 should receive four data tracks from c1+c2");

        let c1_tracks_from_c2 = tokio::time::timeout(Duration::from_secs(10), async {
            let mut tracks = Vec::new();
            while tracks.len() < 2 {
                let event = c1_events.recv().await.expect("c1 events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event
                    && track.publisher_identity().to_string() == "dtpuj_2"
                {
                    tracks.push(track);
                }
            }
            tracks
        })
        .await
        .expect("c1 should receive c2 data-track publications");

        let mut c3_streams = Vec::new();
        for track in c3_tracks {
            let stream = track
                .subscribe()
                .await
                .expect("c3 should subscribe to published data track");
            c3_streams.push((track.publisher_identity().to_string(), stream));
        }
        let mut c1_streams = Vec::new();
        for track in c1_tracks_from_c2 {
            let stream = track
                .subscribe()
                .await
                .expect("c1 should subscribe to c2 data tracks");
            c1_streams.push(stream);
        }

        let c1_dt1_writer = tokio::spawn(async move {
            for _ in 0..80 {
                c1_dt1
                    .try_push(DataTrackFrame::new(b"c1-track-1".to_vec()))
                    .expect("c1 data track 1 writer should enqueue frame");
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        });
        let c1_dt2_writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            for _ in 0..80 {
                c1_dt2
                    .try_push(DataTrackFrame::new(b"c1-track-2".to_vec()))
                    .expect("c1 data track 2 writer should enqueue frame");
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        });
        let c2_dt1_writer = tokio::spawn(async move {
            for _ in 0..80 {
                c2_dt1
                    .try_push(DataTrackFrame::new(b"c2-track-1".to_vec()))
                    .expect("c2 data track 1 writer should enqueue frame");
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        });
        let c2_dt2_writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            for _ in 0..80 {
                c2_dt2
                    .try_push(DataTrackFrame::new(b"c2-track-2".to_vec()))
                    .expect("c2 data track 2 writer should enqueue frame");
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        });

        for (publisher, stream) in &mut c3_streams {
            let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("c3 data-track receive probe should complete")
                .expect("c3 should receive data-track frame with bounded retries");
            assert!(!frame.payload().is_empty(), "c3 should receive non-empty frame from {publisher}");
        }

        c1_dt1_writer.await.expect("c1 data track 1 writer should finish");
        c1_dt2_writer.await.expect("c1 data track 2 writer should finish");
        c2_dt1_writer.await.expect("c2 data track 1 writer should finish");
        c2_dt2_writer.await.expect("c2 data track 2 writer should finish");

        let _ = c2_room.close().await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let c3_sees_c2 = c3_room.remote_participants().contains_key(&"dtpuj_2".into());
                let c1_sees_c2 = c1_room.remote_participants().contains_key(&"dtpuj_2".into());
                if !c3_sees_c2 && !c1_sees_c2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("c1 and c3 should observe c2 removal and data-track cleanup");

        let (c2_rejoin_room, _c2_rejoin_events) = connect_room(&node_a_url, &room, "dtpuj_2", true).await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let c3_sees_c2 = c3_room.remote_participants().contains_key(&"dtpuj_2".into());
                let c1_sees_c2 = c1_room.remote_participants().contains_key(&"dtpuj_2".into());
                if c3_sees_c2 && c1_sees_c2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("c2 rejoin should be visible to c1 and c3");

        let c2_re_dt1 = c2_rejoin_room
            .local_participant()
            .publish_data_track("c2-data-rejoin-1")
            .await
            .expect("c2 rejoin data track 1 should publish");
        let c2_re_dt2 = c2_rejoin_room
            .local_participant()
            .publish_data_track("c2-data-rejoin-2")
            .await
            .expect("c2 rejoin data track 2 should publish");

        let c3_tracks_after = tokio::time::timeout(Duration::from_secs(10), async {
            let mut tracks = Vec::new();
            while tracks.len() < 2 {
                let event = c3_events.recv().await.expect("c3 events should stay open after rejoin");
                if let RoomEvent::DataTrackPublished(track) = event
                    && track.publisher_identity().to_string() == "dtpuj_2"
                {
                    tracks.push(track);
                }
            }
            tracks
        })
        .await
        .expect("c3 should receive two new c2 data tracks after rejoin");
        let c1_tracks_after = tokio::time::timeout(Duration::from_secs(10), async {
            let mut tracks = Vec::new();
            while tracks.len() < 2 {
                let event = c1_events.recv().await.expect("c1 events should stay open after rejoin");
                if let RoomEvent::DataTrackPublished(track) = event
                    && track.publisher_identity().to_string() == "dtpuj_2"
                {
                    tracks.push(track);
                }
            }
            tracks
        })
        .await
        .expect("c1 should receive two new c2 data tracks after rejoin");

        let mut c3_streams_after = Vec::new();
        for track in c3_tracks_after {
            c3_streams_after.push(
                track
                    .subscribe()
                    .await
                    .expect("c3 should subscribe after c2 rejoin"),
            );
        }
        let mut c1_streams_after = Vec::new();
        for track in c1_tracks_after {
            c1_streams_after.push(
                track
                    .subscribe()
                    .await
                    .expect("c1 should subscribe after c2 rejoin"),
            );
        }

        let c2_re_dt1_writer = tokio::spawn(async move {
            for _ in 0..80 {
                c2_re_dt1
                    .try_push(DataTrackFrame::new(b"c2-rejoin-track-1".to_vec()))
                    .expect("c2 rejoin data track 1 writer should enqueue frame");
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        });
        let c2_re_dt2_writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            for _ in 0..80 {
                c2_re_dt2
                    .try_push(DataTrackFrame::new(b"c2-rejoin-track-2".to_vec()))
                    .expect("c2 rejoin data track 2 writer should enqueue frame");
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        });

        for stream in &mut c3_streams_after {
            let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("c3 post-rejoin stream should receive frame")
                .expect("c3 post-rejoin stream should stay open");
            assert!(!frame.payload().is_empty());
        }
        for stream in &mut c1_streams_after {
            let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("c1 post-rejoin stream should receive frame")
                .expect("c1 post-rejoin stream should stay open");
            assert!(!frame.payload().is_empty());
        }

        c2_re_dt1_writer
            .await
            .expect("c2 rejoin data track 1 writer should finish");
        c2_re_dt2_writer
            .await
            .expect("c2 rejoin data track 2 writer should finish");

        let _ = c2_rejoin_room.close().await;
        let _ = c1_room.close().await;
        let _ = c3_room.close().await;
        drop(c2_events);
    }

    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultiNodeJoinAfterClose
#[tokio::test]
async fn test_multi_node_join_after_close() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let room = format!("upstream-multinode-join-after-close-{}", unique_suffix());

    let (first_room, _first_events) = connect_room(&node_a_url, &room, "jcr1", true).await;

    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    client
        .delete_room(&room)
        .await
        .expect("room should close while first participant is connected");

    let (second_room, _second_events) = connect_room(&node_a_url, &room, "jcr2", true).await;

    let _ = second_room.close().await;
    let _ = first_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultiNodeCloseNonRTCRoom
#[tokio::test]
async fn test_multi_node_close_non_rtc_room() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    let room = format!("upstream-multinode-non-rtc-{}", unique_suffix());
    client
        .create_room(&room, CreateRoomOptions::default())
        .await
        .expect("non-rtc room should create");
    client
        .delete_room(&room)
        .await
        .expect("non-rtc room should delete");
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultiNodeRefreshToken
#[tokio::test]
async fn test_multi_node_refresh_token() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, _node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("upstream-multinode-refresh-token-{}", unique_suffix());
    let c1_token = join_token_with(
        &room,
        "c1",
        "c1",
        "initial-metadata",
        HashMap::new(),
        VideoGrants {
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        },
    );

    let (_c1_room, mut c1_events) = connect_room_with_token(&node_a_url, &c1_token, true).await;

    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    client
        .update_participant(
            &room,
            "c1",
            UpdateParticipantOptions {
                permission: Some(proto::ParticipantPermission {
                    can_publish: false,
                    can_subscribe: true,
                    ..Default::default()
                }),
                metadata: "metadata".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("participant permission + metadata update should succeed");

    let refreshed_token = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = c1_events
                .recv()
                .await
                .expect("room events should remain open while waiting for refresh token");
            if let RoomEvent::TokenRefreshed { token } = event {
                if !token.is_empty() {
                    break token;
                }
            }
        }
    })
    .await
    .expect("token refresh event should arrive before timeout");

    let claims = livekit_api::access_token::TokenVerifier::with_api_key(API_KEY, API_SECRET)
        .verify(&refreshed_token)
        .expect("refreshed token should verify against test API secret");

    assert_eq!(claims.metadata, "metadata", "refreshed token should carry updated metadata");
    assert!(!claims.video.can_publish, "refreshed token can_publish should be false");
    assert!(
        !claims.video.can_publish_data,
        "refreshed token can_publish_data should be false after publish permission revoke"
    );
    assert!(claims.video.can_subscribe, "refreshed token can_subscribe should be true");

    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultiNodeUpdateAttributes
#[tokio::test]
async fn test_multi_node_update_attributes() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("upstream-multinode-update-attrs-{}", unique_suffix());

    let mut c1_attrs = HashMap::new();
    c1_attrs.insert("mykey".to_string(), "au1".to_string());
    let c1_token = join_token_with(
        &room,
        "au1",
        "au1",
        "",
        c1_attrs,
        VideoGrants {
            can_publish: true,
            can_subscribe: true,
            can_update_own_metadata: false,
            ..Default::default()
        },
    );

    let mut c2_attrs = HashMap::new();
    c2_attrs.insert("mykey".to_string(), "au2".to_string());
    let c2_token = join_token_with(
        &room,
        "au2",
        "au2",
        "",
        c2_attrs,
        VideoGrants {
            can_publish: true,
            can_subscribe: true,
            can_update_own_metadata: true,
            ..Default::default()
        },
    );

    let (c1_room, _c1_events) = connect_room_with_token(&node_a_url, &c1_token, true).await;
    let (c2_room, _c2_events) = connect_room_with_token(&node_b_url, &c2_token, true).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c1_sees_c2 = c1_room.remote_participants().contains_key(&"au2".into());
            let c2_sees_c1 = c2_room.remote_participants().contains_key(&"au1".into());
            if c1_sees_c2 && c2_sees_c1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("participants should see each other before attribute assertions");

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c1_remote_c2 = c1_room
                .remote_participants()
                .get(&"au2".into())
                .cloned()
                .expect("c1 should see c2 remote participant");
            let c2_remote_c1 = c2_room
                .remote_participants()
                .get(&"au1".into())
                .cloned()
                .expect("c2 should see c1 remote participant");

            let c1_ok = c2_remote_c1.attributes().get("mykey") == Some(&"au1".to_string());
            let c2_ok = c1_remote_c2.attributes().get("mykey") == Some(&"au2".to_string());
            if c1_ok && c2_ok {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("initial token attributes should propagate to remote participant views");

    let c1_set_result = c1_room
        .local_participant()
        .set_attributes(HashMap::from([(
            "mykey".to_string(),
            "shouldnotchange".to_string(),
        )]))
        .await;
    assert!(
        c1_set_result.is_err(),
        "participant without can_update_own_metadata should not update own attributes"
    );

    c2_room
        .local_participant()
        .set_attributes(HashMap::from([
            ("mykey".to_string(), "au2".to_string()),
            ("secondkey".to_string(), "au2".to_string()),
        ]))
        .await
        .expect("participant with can_update_own_metadata should update own attributes");

    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    client
        .update_participant(
            &room,
            "au1",
            UpdateParticipantOptions {
                attributes: HashMap::from([("secondkey".to_string(), "au1".to_string())]),
                ..Default::default()
            },
        )
        .await
        .expect("room API attribute update for au1 should succeed");

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c2_remote_c1 = c2_room
                .remote_participants()
                .get(&"au1".into())
                .cloned()
                .expect("c2 should continue to see c1");
            let c1_remote_c2 = c1_room
                .remote_participants()
                .get(&"au2".into())
                .cloned()
                .expect("c1 should continue to see c2");

            let c1_ok = c2_remote_c1.attributes().get("mykey") == Some(&"au1".to_string())
                && c2_remote_c1.attributes().get("secondkey") == Some(&"au1".to_string());
            let c2_ok = c1_remote_c2.attributes().get("mykey") == Some(&"au2".to_string())
                && c1_remote_c2.attributes().get("secondkey") == Some(&"au2".to_string());

            if c1_ok && c2_ok {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("attribute updates should propagate to both remote participants");

    let _ = c1_room.close().await;
    let _ = c2_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestMultiNodeRevokePublishPermission
#[tokio::test]
async fn test_multi_node_revoke_publish_permission() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("upstream-multinode-revoke-publish-{}", unique_suffix());
    let (c1_room, _c1_events) = connect_room(&node_a_url, &room, "c1", true).await;
    let (c2_room, mut c2_events) = connect_room(&node_b_url, &room, "c2", true).await;

    // Upstream waits for the RTC clients to connect before c1 publishes. Without
    // this readiness gate, c2 can miss the initial publications during cross-node
    // participant synchronization and the port does not exercise the revoke path.
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
    .expect("c1 and c2 should connect before c1 publishes tracks");

    let (_audio_sid, _audio_source) = publish_audio_track(&c1_room, "webcamaudio").await;
    let _video_sid = publish_video_track(&c1_room, "webcamvideo").await;

    let subscriptions = wait_for_track_subscribed_count(&mut c2_events, 2).await;
    assert_eq!(subscriptions.len(), 2, "c2 should receive c1's two tracks before revoke");

    let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    client
        .update_participant(
            &room,
            "c1",
            UpdateParticipantOptions {
                permission: Some(proto::ParticipantPermission {
                    can_publish: false,
                    can_publish_data: true,
                    can_subscribe: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .expect("revoking c1 publish permission should succeed");

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let service_unpublished = client
                .list_participants(&room)
                .await
                .expect("room API should list c1 after publish permission revoke")
                .iter()
                .find(|participant| participant.identity == "c1")
                .is_some_and(|participant| participant.tracks.is_empty());
            let remote_unpublished = c2_room
                .remote_participants()
                .get(&"c1".into())
                .map(|participant| participant.track_publications().is_empty())
                .unwrap_or(false);
            if service_unpublished && remote_unpublished {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("publish permission revoke should unpublish c1 tracks locally and remotely");

    let _ = c1_room.close().await;
    let _ = c2_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

// Upstream: livekit/test/multinode_test.go::TestCloseDisconnectedParticipantOnSignalClose
#[tokio::test]
async fn test_close_disconnected_participant_on_signal_close() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };
    let room = format!("upstream-close-disconnected-{}", unique_suffix());

    let (observer_room, _observer_events) = connect_room(&node_b_url, &room, "c1", true).await;

    // Upstream creates c2 with signal interceptors that drop Offer/Answer/Leave while allowing join.
    // Using a raw signal socket here mirrors that behavior more closely than a full SDK Room join.
    let c2_token = join_token(&room, "c2");
    let (mut c2_socket, _c2_sid) = connect_signal_socket_with_token(&node_a_url, &c2_token).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if observer_room.remote_participants().contains_key(&"c2".into()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("observer should see c2 join via signal-only connection");

    drop(c2_socket);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !observer_room.remote_participants().contains_key(&"c2".into()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("observer should see c2 removed after signal close");

    let _ = observer_room.close().await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
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

// Upstream: livekit/test/multinode_test.go::TestMultiNodeDataBlob
#[tokio::test]
async fn test_multi_node_data_blob() {
    let Some((mut redis, mut node_a, mut node_b, _redis_url, node_a_url, node_b_url)) =
        spawn_two_process_nodes().await
    else {
        return;
    };

    let room = format!("upstream-multinode-data-blob-{}", unique_suffix());
    let room_client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    room_client
        .create_room(&room, CreateRoomOptions::default())
        .await
        .expect("multinode data-blob room should create on node A before joins");

    let pub_token = join_token(&room, "pub");
    let sub_token = join_token(&room, "sub");

    let (mut pub_socket, _pub_sid) = connect_signal_socket_with_token(&node_a_url, &pub_token).await;
    let (mut sub_socket, _sub_sid) = connect_signal_socket_with_token(&node_b_url, &sub_token).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        let client = RoomClient::with_api_key(&node_a_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
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
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("pub and sub should both join before data-blob get routing checks");

    let key = generic_blob_key("blob-multinode");
    let contents = b"multinode-content".to_vec();

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
            panic!("publisher store request should not return RequestResponse error: {rr:?}")
        }
        CompatOrRequestResponse::Get(_) => {
            panic!("publisher store request should not receive get data blob response")
        }
    };
    assert_eq!(store_response.request_id, 1);
    assert_eq!(store_response.key, Some(key.clone()));

    let no_error_response = tokio::time::timeout(Duration::from_millis(300), async {
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
        no_error_response.is_err(),
        "publisher store success should not emit a request_response error"
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
            panic!("subscriber get request should not return RequestResponse error: {rr:?}")
        }
        CompatOrRequestResponse::Store(_) => {
            panic!("subscriber get request should not receive store data blob response")
        }
    };
    assert_eq!(get_response.request_id, 2);
    let blob = get_response
        .blob
        .expect("get data blob response should include a blob payload");
    assert_eq!(blob.key, Some(key.clone()));
    assert_eq!(blob.contents, contents);

    let missing_request = CompatSignalRequest {
        store_data_blob_request: None,
        get_data_blob_request: Some(CompatGetDataBlobRequest {
            request_id: 3,
            participant_identity: "unknown-publisher".to_string(),
            key: Some(key),
        }),
    };
    sub_socket
        .send(Message::Binary(missing_request.encode_to_vec().into()))
        .await
        .expect("subscriber should send get request for unknown publisher");

    let missing_response = match next_compat_or_request_response(&mut sub_socket, Duration::from_secs(10)).await {
        CompatOrRequestResponse::RequestResponse(rr) => rr,
        CompatOrRequestResponse::Store(_) => {
            panic!("unknown publisher request should not return store response")
        }
        CompatOrRequestResponse::Get(_) => {
            panic!("unknown publisher request should not return get response")
        }
    };
    assert_eq!(missing_response.request_id, 3);
    assert_eq!(
        missing_response.reason,
        proto::request_response::Reason::NotFound as i32,
        "unknown publisher should return NOT_FOUND"
    );

    let _ = pub_socket.close(None).await;
    let _ = sub_socket.close(None).await;
    let _ = node_a.kill().await;
    let _ = node_b.kill().await;
    let _ = redis.kill().await;
}

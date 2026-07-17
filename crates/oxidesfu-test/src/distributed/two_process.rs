use super::*;

    #[tokio::test]
    async fn distributed_two_process_relay_join_is_remote_owned_across_real_node_boundary() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed two-process join test because redis-server is not on PATH"
            );
            return;
        };

        let node_a_port = reserve_local_port();
        let node_b_port = reserve_local_port();

        let Some((mut node_a, node_a_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed two-process join test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed two-process join test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-process-room-{}", unique_suffix());
        let identity = format!("distributed-process-identity-{}", unique_suffix());

        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B");

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Process Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let (mut join_socket, joined_sid) = connect_join_and_hold_socket(
            &node_a_base_url,
            &token,
            &join_request_param(),
        )
        .await;

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let remote = wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
            .await
            .expect("remote-selected node should own participant in process-boundary test");
        assert_eq!(remote.sid, joined_sid);

        let local_lookup = local_client.get_participant(&room_name, &identity).await;
        assert!(
            local_lookup.is_err(),
            "origin node should not own participant for remote-selected process-boundary join"
        );

        let _ = join_socket.send(Message::Close(None)).await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }
    #[tokio::test]
    async fn distributed_two_process_reconnect_via_origin_stays_remote_owned() {

        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed two-process reconnect test because redis-server is not on PATH"
            );
            return;
        };

        let node_a_port = reserve_local_port();
        let node_b_port = reserve_local_port();

        let Some((mut node_a, node_a_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed two-process reconnect test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed two-process reconnect test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-process-reconnect-room-{}", unique_suffix());
        let identity = format!("distributed-process-reconnect-identity-{}", unique_suffix());

        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B");

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Process Reconnect Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let (mut first_socket, first_sid) = connect_join_and_hold_socket(
            &node_a_base_url,
            &token,
            &join_request_param(),
        )
        .await;

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let initial_remote =
            wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
                .await
                .expect("remote-selected node should own participant before reconnect");
        assert_eq!(initial_remote.sid, first_sid);

        let initial_local_lookup = local_client.get_participant(&room_name, &identity).await;
        assert!(
            initial_local_lookup.is_err(),
            "origin node should not own participant before reconnect in remote-owned path"
        );

        let _ = first_socket.send(Message::Close(None)).await;

        let (mut reconnect_socket, reconnect_sid) = reconnect_and_hold_socket(
            &node_a_base_url,
            &token,
            &reconnect_join_request_param(&first_sid, proto::ReconnectReason::RrSignalDisconnected),
            &first_sid,
        )
        .await;

        let remote_lookup =
            wait_for_participant_on_room_client(&remote_client, &room_name, &identity).await;
        let remote = match remote_lookup {
            Ok(remote) => remote,
            Err(err) => {
                let node_b_status = node_b
                    .try_wait()
                    .expect("node B process status should be queryable");
                let node_a_status = node_a
                    .try_wait()
                    .expect("node A process status should be queryable");
                let local_lookup = local_client.get_participant(&room_name, &identity).await;
                let remote_rooms = remote_client.list_rooms(Vec::new()).await;
                let remote_participants = remote_client.list_participants(&room_name).await;
                let local_rooms = local_client.list_rooms(Vec::new()).await;
                panic!(
                    "remote-selected node should own participant after reconnect; remote lookup failed: {err}; node_b_status={node_b_status:?}; node_a_status={node_a_status:?}; local_lookup={local_lookup:?}; remote_rooms={remote_rooms:?}; remote_participants={remote_participants:?}; local_rooms={local_rooms:?}"
                );
            }
        };
        assert_eq!(remote.sid, reconnect_sid);

        let local_lookup = local_client.get_participant(&room_name, &identity).await;
        assert!(
            local_lookup.is_err(),
            "origin node should remain non-owner after reconnect through remote-owned path"
        );

        let _ = reconnect_socket.send(Message::Close(None)).await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }
    #[tokio::test]
    async fn distributed_two_process_relayed_session_supports_pingreq_and_leave_lifecycle() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed relayed lifecycle test because redis-server is not on PATH"
            );
            return;
        };

        let node_a_port = reserve_local_port();
        let node_b_port = reserve_local_port();

        let Some((mut node_a, node_a_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed relayed lifecycle test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed relayed lifecycle test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-process-lifecycle-room-{}", unique_suffix());
        let identity = format!("distributed-process-lifecycle-identity-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B");

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Lifecycle Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let host = node_a_base_url
            .strip_prefix("http://")
            .expect("base_url should start with http://");
        let url = format!("ws://{host}/rtc/v1?join_request={}", join_request_param());
        let mut request = url.into_client_request().expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .expect("authorization header should parse"),
        );

        let (mut socket, _) = connect_async(request)
            .await
            .expect("websocket should connect to origin node");
        let first = socket
            .next()
            .await
            .expect("first websocket message should arrive")
            .expect("first websocket message should be ok");
        let Message::Binary(bytes) = first else {
            panic!("expected binary protobuf signal response");
        };
        let join_response =
            proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
        assert!(
            matches!(
                join_response.message,
                Some(proto::signal_response::Message::Join(_))
            ),
            "first response should be join"
        );

        let _remote_joined =
            wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
                .await
                .expect("remote owner should contain participant while relayed session is active");

        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");
        let ping_request = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp,
                ..Default::default()
            })),
        };
        socket
            .send(Message::Binary(ping_request.encode_to_vec().into()))
            .await
            .expect("ping request should send");

        let second = socket
            .next()
            .await
            .expect("second websocket message should arrive")
            .expect("second websocket message should be ok");
        let Message::Binary(second_bytes) = second else {
            panic!("expected binary pong response");
        };
        let second_response = proto::SignalResponse::decode(second_bytes.as_ref())
            .expect("second response should decode");
        let Some(proto::signal_response::Message::PongResp(pong)) = second_response.message else {
            panic!("expected pong response after ping request");
        };
        assert_eq!(pong.last_ping_timestamp, ping_timestamp);

        let leave = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Leave(
                proto::LeaveRequest::default(),
            )),
        };
        socket
            .send(Message::Binary(leave.encode_to_vec().into()))
            .await
            .expect("leave request should send");

        let closed = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("leave should terminate relayed websocket in bounded time");
        assert!(
            matches!(closed, None | Some(Ok(Message::Close(_))) | Some(Err(_))),
            "relayed websocket should close after leave"
        );

        let removed_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if remote_client
                .get_participant(&room_name, &identity)
                .await
                .is_err()
            {
                break;
            }
            if tokio::time::Instant::now() >= removed_deadline {
                panic!("remote owner did not remove participant after relayed leave");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }
    #[tokio::test]
    async fn distributed_two_process_remote_owned_data_roundtrip_survives_subscriber_reconnect() {

        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed remote-owned data continuity test because redis-server is not on PATH"
            );
            return;
        };

        let node_a_port = reserve_local_port();
        let node_b_port = reserve_local_port();

        let Some((mut node_a, node_a_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed remote-owned data continuity test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed remote-owned data continuity test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-process-data-room-{}", unique_suffix());
        let publisher_identity = format!("distributed-process-data-publisher-{}", unique_suffix());
        let subscriber_identity = format!("distributed-process-data-subscriber-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B");

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&publisher_identity)
            .with_name("Distributed Process Data Publisher")
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
            .with_identity(&subscriber_identity)
            .with_name("Distributed Process Data Subscriber")
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
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(&node_a_base_url, &publisher_token, options.clone())
                .await
                .expect("publisher should connect through origin node");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(&node_a_base_url, &subscriber_token, options.clone())
                .await
                .expect("subscriber should connect through origin node");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        publisher_room
            .local_participant()
            .publish_data(DataPacket {
                payload: b"distributed-first".to_vec(),
                topic: Some("distributed-data".to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .expect("first data publish should succeed");

        let (payload_first, topic_first, kind_first) = next_data_received(&mut subscriber_events).await;
        assert_eq!(payload_first.as_slice(), b"distributed-first");
        assert_eq!(topic_first.as_deref(), Some("distributed-data"));
        assert_eq!(kind_first, DataPacketKind::Reliable);

        let _ = subscriber_room.close().await;

        let (subscriber_room_rejoined, mut subscriber_events_rejoined) =
            Room::connect(&node_a_base_url, &subscriber_token, options)
                .await
                .expect("subscriber should reconnect through origin node");
        wait_for_room_connected(&mut subscriber_events_rejoined).await;

        publisher_room
            .local_participant()
            .publish_data(DataPacket {
                payload: b"distributed-second".to_vec(),
                topic: Some("distributed-data".to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .expect("second data publish should succeed after reconnect");

        let (payload_second, topic_second, kind_second) =
            next_data_received(&mut subscriber_events_rejoined).await;
        assert_eq!(payload_second.as_slice(), b"distributed-second");
        assert_eq!(topic_second.as_deref(), Some("distributed-data"));
        assert_eq!(kind_second, DataPacketKind::Reliable);

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let _remote_publisher =
            wait_for_participant_on_room_client(&remote_client, &room_name, &publisher_identity)
                .await
                .expect("remote owner should keep publisher after data continuity slice");
        let _remote_subscriber =
            wait_for_participant_on_room_client(&remote_client, &room_name, &subscriber_identity)
                .await
                .expect("remote owner should keep rejoined subscriber after data continuity slice");

        assert!(
            local_client
                .get_participant(&room_name, &publisher_identity)
                .await
                .is_err(),
            "origin node should remain non-owner for publisher in remote-owned room"
        );
        assert!(
            local_client
                .get_participant(&room_name, &subscriber_identity)
                .await
                .is_err(),
            "origin node should remain non-owner for subscriber in remote-owned room"
        );

        let _ = subscriber_room_rejoined.close().await;
        let _ = publisher_room.close().await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_remote_owned_audio_roundtrip_survives_subscriber_reconnect() {

        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed remote-owned audio continuity test because redis-server is not on PATH"
            );
            return;
        };

        let node_a_port = reserve_local_port();
        let node_b_port = reserve_local_port();

        let Some((mut node_a, node_a_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed remote-owned audio continuity test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed remote-owned audio continuity test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-process-audio-room-{}", unique_suffix());
        let publisher_identity = format!("distributed-process-audio-publisher-{}", unique_suffix());
        let subscriber_identity = format!("distributed-process-audio-subscriber-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B");

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&publisher_identity)
            .with_name("Distributed Process Audio Publisher")
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
            .with_identity(&subscriber_identity)
            .with_name("Distributed Process Audio Subscriber")
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
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(&node_a_base_url, &publisher_token, options.clone())
                .await
                .expect("publisher should connect through origin node");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(&node_a_base_url, &subscriber_token, options.clone())
                .await
                .expect("subscriber should connect through origin node");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track = LocalAudioTrack::create_audio_track(
            "distributed-mic",
            RtcAudioSource::Native(source.clone()),
        );
        let publication = publisher_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("publisher should publish audio track");
        let published_sid = publication.sid().to_string();

        let frame = AudioFrame {
            data: vec![555_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };

        let first_remote_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                source
                    .capture_frame(&frame)
                    .await
                    .expect("audio frame should be accepted while waiting for initial subscription");

                let Ok(Some(event)) = tokio::time::timeout(
                    Duration::from_millis(120),
                    subscriber_events.recv(),
                )
                .await
                else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                };

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

        let mut first_stream = NativeAudioStream::new(first_remote_audio_track.rtc_track(), 48_000, 1);
        source
            .capture_frame(&frame)
            .await
            .expect("audio frame should be accepted");
        let _first = tokio::time::timeout(Duration::from_secs(5), first_stream.next())
            .await
            .expect("first frame wait should complete")
            .expect("first frame should arrive");

        let _ = subscriber_room.close().await;

        let (subscriber_room_rejoined, mut subscriber_events_rejoined) =
            Room::connect(&node_a_base_url, &subscriber_token, options)
                .await
                .expect("subscriber should reconnect through origin node");
        wait_for_room_connected(&mut subscriber_events_rejoined).await;

        let maybe_rejoined_audio_track = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let event = subscriber_events_rejoined
                    .recv()
                    .await
                    .expect("rejoined subscriber events should stay open");
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

        let recovered = tokio::time::timeout(Duration::from_secs(10), async {
            let mut rejoined_stream = maybe_rejoined_audio_track
                .as_ref()
                .map(|track| NativeAudioStream::new(track.rtc_track(), 48_000, 1));
            for _ in 0..100 {
                source
                    .capture_frame(&frame)
                    .await
                    .expect("audio frame should be accepted");
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
        .expect("rejoined media recovery probe should finish");
        assert!(
            recovered,
            "subscriber should receive audio again after reconnect in remote-owned two-process room"
        );

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let _remote_publisher =
            wait_for_participant_on_room_client(&remote_client, &room_name, &publisher_identity)
                .await
                .expect("remote owner should keep publisher after audio continuity slice");
        let _remote_subscriber =
            wait_for_participant_on_room_client(&remote_client, &room_name, &subscriber_identity)
                .await
                .expect("remote owner should keep rejoined subscriber after audio continuity slice");

        assert!(
            local_client
                .get_participant(&room_name, &publisher_identity)
                .await
                .is_err(),
            "origin node should remain non-owner for publisher in remote-owned room"
        );
        assert!(
            local_client
                .get_participant(&room_name, &subscriber_identity)
                .await
                .is_err(),
            "origin node should remain non-owner for subscriber in remote-owned room"
        );

        let _ = subscriber_room_rejoined.close().await;
        let _ = publisher_room.close().await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_relayed_session_routes_offer_and_trickle_to_remote_owner() {

        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed relayed offer/trickle test because redis-server is not on PATH"
            );
            return;
        };

        let node_a_port = reserve_local_port();
        let node_b_port = reserve_local_port();

        let Some((mut node_a, node_a_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed relayed offer/trickle test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed relayed offer/trickle test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-process-offer-trickle-room-{}", unique_suffix());
        let identity = format!(
            "distributed-process-offer-trickle-identity-{}",
            unique_suffix()
        );
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B");

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Offer Trickle Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let host = node_a_base_url
            .strip_prefix("http://")
            .expect("base_url should start with http://");
        let url = format!("ws://{host}/rtc/v1?join_request={}", join_request_param());
        let mut request = url.into_client_request().expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .expect("authorization header should parse"),
        );

        let (mut socket, _) = connect_async(request)
            .await
            .expect("websocket should connect to origin node");
        let first = socket
            .next()
            .await
            .expect("first websocket message should arrive")
            .expect("first websocket message should be ok");
        let Message::Binary(bytes) = first else {
            panic!("expected binary protobuf signal response");
        };
        let join_response =
            proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
        assert!(
            matches!(
                join_response.message,
                Some(proto::signal_response::Message::Join(_))
            ),
            "first response should be join"
        );

        let remote_joined =
            wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
                .await
                .expect("remote owner should contain participant while relayed session is active");
        assert!(
            local_client
                .get_participant(&room_name, &identity)
                .await
                .is_err(),
            "origin node should not own relayed participant"
        );

        let (client_peer, mut client_events) = oxidesfu_rtc::create_peer_connection_with_events()
            .await
            .expect("client peer connection should create");
        let _data_channel = client_peer
            .create_data_channel("distributed-offer-trickle")
            .await
            .expect("client data channel should create");
        let offer_sdp = client_peer
            .create_offer()
            .await
            .expect("offer should create");
        let offer = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Offer(
                proto::SessionDescription {
                    r#type: "offer".to_string(),
                    sdp: offer_sdp,
                    id: 44,
                    ..Default::default()
                },
            )),
        };
        socket
            .send(Message::Binary(offer.encode_to_vec().into()))
            .await
            .expect("offer should send through relayed origin socket");

        let answer_message = socket
            .next()
            .await
            .expect("answer should arrive through relayed origin socket")
            .expect("answer should be ok");
        let Message::Binary(answer_bytes) = answer_message else {
            panic!("expected binary answer response");
        };
        let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
            .expect("answer response should decode");
        let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
            panic!("expected answer response from remote owner");
        };
        assert_eq!(answer.id, 44);
        client_peer
            .set_remote_answer(answer.sdp)
            .await
            .expect("remote-owner answer should apply to client peer");

        let server_trickle_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match tokio::time::timeout(Duration::from_millis(500), socket.next()).await {
                Ok(Some(Ok(Message::Binary(trickle_bytes)))) => {
                    let response = proto::SignalResponse::decode(trickle_bytes.as_ref())
                        .expect("signal response should decode");
                    if let Some(proto::signal_response::Message::Trickle(trickle)) =
                        response.message
                    {
                        client_peer
                            .add_ice_candidate_json(&trickle.candidate_init)
                            .await
                            .expect("remote-owner server trickle should apply to client peer");
                        break;
                    }
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(err))) => panic!("websocket error while waiting for trickle: {err}"),
                Ok(None) => panic!("websocket closed while waiting for trickle"),
                Err(_) => {}
            }
            if tokio::time::Instant::now() >= server_trickle_deadline {
                panic!("remote-owner server trickle was not backhauled after relayed offer");
            }
        }

        let candidate =
            tokio::time::timeout(Duration::from_secs(5), client_events.ice_candidates.recv())
                .await
                .expect("client candidate wait should be bounded")
                .expect("client should emit an ICE candidate");
        let trickle = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Trickle(
                proto::TrickleRequest {
                    candidate_init: candidate.candidate_init_json,
                    target: proto::SignalTarget::Publisher as i32,
                    r#final: candidate.is_final,
                },
            )),
        };
        socket
            .send(Message::Binary(trickle.encode_to_vec().into()))
            .await
            .expect("trickle should send through relayed origin socket");

        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");
        let ping_request = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp,
                ..Default::default()
            })),
        };
        socket
            .send(Message::Binary(ping_request.encode_to_vec().into()))
            .await
            .expect("ping request should send after relayed trickle");

        let pong_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let next = tokio::time::timeout(Duration::from_millis(500), socket.next())
                .await
                .expect("socket should stay responsive after relayed trickle")
                .expect("websocket should stay open")
                .expect("websocket message should be ok");
            let Message::Binary(pong_bytes) = next else {
                continue;
            };
            let response = proto::SignalResponse::decode(pong_bytes.as_ref())
                .expect("signal response should decode");
            if let Some(proto::signal_response::Message::PongResp(pong)) = response.message {
                assert_eq!(pong.last_ping_timestamp, ping_timestamp);
                break;
            }
            if tokio::time::Instant::now() >= pong_deadline {
                panic!("relayed session did not return pong after offer/trickle continuity path");
            }
        }

        let remote_after = remote_client
            .get_participant(&room_name, &identity)
            .await
            .expect("remote owner should still contain participant after offer/trickle");
        assert_eq!(remote_after.sid, remote_joined.sid);

        let _ = socket.send(Message::Close(None)).await;
        let (mut reconnect_socket, reconnect_sid) = reconnect_and_hold_socket(
            &node_a_base_url,
            &token,
            &reconnect_join_request_param(&remote_after.sid, proto::ReconnectReason::RrSignalDisconnected),
            &remote_after.sid,
        )
        .await;

        let remote_after_reconnect =
            wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
                .await
                .expect("remote owner should still contain participant after reconnect");
        assert_eq!(remote_after_reconnect.sid, reconnect_sid);
        assert!(
            local_client.get_participant(&room_name, &identity).await.is_err(),
            "origin node should remain non-owner after relayed offer/trickle reconnect continuity"
        );

        let _ = reconnect_socket.send(Message::Close(None)).await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }

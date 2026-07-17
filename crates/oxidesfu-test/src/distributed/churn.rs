use super::*;

    #[tokio::test]
    async fn distributed_two_process_remote_owner_outage_falls_back_to_local_owner() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed outage fallback test because redis-server is not on PATH"
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
                "skipping distributed outage fallback test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, _node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed outage fallback test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-outage-fallback-room-{}", unique_suffix());
        let identity = format!("distributed-outage-fallback-identity-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B before outage");

        let _ = node_b.kill().await;
        wait_for_room_node_registration_count(&redis_url, 1)
            .await
            .expect("node B should be removed from redis node directory after outage");

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Outage Fallback Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let (mut join_socket, joined_sid) =
            connect_join_and_hold_socket(&node_a_base_url, &token, &join_request_param()).await;

        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_owned = wait_for_participant_on_room_client(&local_client, &room_name, &identity)
            .await
            .expect("local node should become owner when preselected remote owner is down");
        assert_eq!(local_owned.sid, joined_sid);

        let _ = join_socket.send(Message::Close(None)).await;
        let _ = node_a.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_remote_owner_outage_during_active_session_reconnects_and_stays_local_owned()
    {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed outage active-session test because redis-server is not on PATH"
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
                "skipping distributed outage active-session test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, _node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed outage active-session test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-outage-active-session-room-{}", unique_suffix());
        let identity = format!("distributed-outage-active-session-identity-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B before outage");

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Outage Active Session Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let (mut first_socket, first_sid) =
            connect_join_and_hold_socket(&node_a_base_url, &token, &join_request_param()).await;

        let ping_before = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");
        let ping_request_before = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_before,
                ..Default::default()
            })),
        };
        first_socket
            .send(Message::Binary(ping_request_before.encode_to_vec().into()))
            .await
            .expect("ping before outage should send");
        let pong_message_before = first_socket
            .next()
            .await
            .expect("pong before outage should arrive")
            .expect("pong before outage should be ok");
        let Message::Binary(pong_bytes_before) = pong_message_before else {
            panic!("expected binary pong before outage");
        };
        let pong_response_before =
            proto::SignalResponse::decode(pong_bytes_before.as_ref()).expect("pong should decode");
        let Some(proto::signal_response::Message::PongResp(pong)) = pong_response_before.message else {
            panic!("expected pong response before outage");
        };
        assert_eq!(pong.last_ping_timestamp, ping_before);

        let _ = node_b.kill().await;
        wait_for_room_node_registration_count(&redis_url, 1)
            .await
            .expect("node B should be removed from redis node directory after outage");

        let _ = first_socket.send(Message::Close(None)).await;

        let (mut reconnect_socket, reconnect_sid) = reconnect_and_hold_socket(
            &node_a_base_url,
            &token,
            &reconnect_join_request_param(&first_sid, proto::ReconnectReason::RrSignalDisconnected),
            &first_sid,
        )
        .await;

        let ping_after = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");
        let ping_request_after = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_after,
                ..Default::default()
            })),
        };
        reconnect_socket
            .send(Message::Binary(ping_request_after.encode_to_vec().into()))
            .await
            .expect("ping after outage fallback reconnect should send");
        let pong_message_after = reconnect_socket
            .next()
            .await
            .expect("pong after outage fallback reconnect should arrive")
            .expect("pong after outage fallback reconnect should be ok");
        let Message::Binary(pong_bytes_after) = pong_message_after else {
            panic!("expected binary pong after outage fallback reconnect");
        };
        let pong_response_after =
            proto::SignalResponse::decode(pong_bytes_after.as_ref()).expect("pong should decode");
        let Some(proto::signal_response::Message::PongResp(pong)) = pong_response_after.message else {
            panic!("expected pong response after outage fallback reconnect");
        };
        assert_eq!(pong.last_ping_timestamp, ping_after);

        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_owned = wait_for_participant_on_room_client(&local_client, &room_name, &identity)
            .await
            .expect("local node should own active session after remote outage and reconnect");
        assert_eq!(local_owned.sid, reconnect_sid);

        let _ = reconnect_socket.send(Message::Close(None)).await;
        let _ = node_a.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_remote_owner_outage_during_active_publish_recovers_on_local_owner()
    {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed active-publish outage test because redis-server is not on PATH"
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
                "skipping distributed active-publish outage test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, _node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed active-publish outage test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-active-publish-outage-room-{}", unique_suffix());
        let publisher_identity = format!("distributed-active-publish-publisher-{}", unique_suffix());
        let subscriber_identity = format!("distributed-active-publish-subscriber-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B before outage");

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&publisher_identity)
            .with_name("Distributed Active Publish Publisher")
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
            .with_name("Distributed Active Publish Subscriber")
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
                payload: b"before-owner-outage".to_vec(),
                topic: Some("distributed-active-publish".to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .expect("publish before owner outage should succeed");

        let (payload_before, topic_before, kind_before) = next_data_received(&mut subscriber_events).await;
        assert_eq!(payload_before.as_slice(), b"before-owner-outage");
        assert_eq!(topic_before.as_deref(), Some("distributed-active-publish"));
        assert_eq!(kind_before, DataPacketKind::Reliable);

        let _ = node_b.kill().await;
        wait_for_room_node_registration_count(&redis_url, 1)
            .await
            .expect("node B should be removed from redis node directory after outage");

        let _ = subscriber_room.close().await;
        let _ = publisher_room.close().await;

        let (publisher_room_rejoined, mut publisher_events_rejoined) =
            Room::connect(&node_a_base_url, &publisher_token, options.clone())
                .await
                .expect("publisher should reconnect on local fallback owner");
        let (subscriber_room_rejoined, mut subscriber_events_rejoined) =
            Room::connect(&node_a_base_url, &subscriber_token, options)
                .await
                .expect("subscriber should reconnect on local fallback owner");
        wait_for_room_connected(&mut publisher_events_rejoined).await;
        wait_for_room_connected(&mut subscriber_events_rejoined).await;

        publisher_room_rejoined
            .local_participant()
            .publish_data(DataPacket {
                payload: b"after-owner-outage".to_vec(),
                topic: Some("distributed-active-publish".to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .expect("publish after owner outage fallback should succeed");

        let (payload_after, topic_after, kind_after) =
            next_data_received(&mut subscriber_events_rejoined).await;
        assert_eq!(payload_after.as_slice(), b"after-owner-outage");
        assert_eq!(topic_after.as_deref(), Some("distributed-active-publish"));
        assert_eq!(kind_after, DataPacketKind::Reliable);

        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_publisher =
            wait_for_participant_on_room_client(&local_client, &room_name, &publisher_identity)
                .await
                .expect("local node should own publisher after remote owner outage");
        let local_subscriber =
            wait_for_participant_on_room_client(&local_client, &room_name, &subscriber_identity)
                .await
                .expect("local node should own subscriber after remote owner outage");

        assert!(!local_publisher.sid.is_empty());
        assert!(!local_subscriber.sid.is_empty());

        let _ = subscriber_room_rejoined.close().await;
        let _ = publisher_room_rejoined.close().await;
        let _ = node_a.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_outage_fallback_reconnect_stays_local_owned() {

        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed outage fallback reconnect test because redis-server is not on PATH"
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
                "skipping distributed outage fallback reconnect test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut node_b, _node_b_base_url)) =
            spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
                .await
                .expect("node B process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed outage fallback reconnect test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-outage-fallback-reconnect-room-{}", unique_suffix());
        let identity = format!("distributed-outage-fallback-reconnect-identity-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B before outage");

        let _ = node_b.kill().await;
        wait_for_room_node_registration_count(&redis_url, 1)
            .await
            .expect("node B should be removed from redis node directory after outage");

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Outage Fallback Reconnect Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let (mut first_socket, first_sid) =
            connect_join_and_hold_socket(&node_a_base_url, &token, &join_request_param()).await;
        let _ = first_socket.send(Message::Close(None)).await;

        let (mut reconnect_socket, reconnect_sid) = reconnect_and_hold_socket(
            &node_a_base_url,
            &token,
            &reconnect_join_request_param(&first_sid, proto::ReconnectReason::RrSignalDisconnected),
            &first_sid,
        )
        .await;

        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_owned = wait_for_participant_on_room_client(&local_client, &room_name, &identity)
            .await
            .expect("local node should remain owner after reconnect during remote outage fallback");
        assert_eq!(local_owned.sid, reconnect_sid);

        let _ = reconnect_socket.send(Message::Close(None)).await;
        let _ = node_a.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_reconnect_storm_stays_remote_owned() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed reconnect storm test because redis-server is not on PATH"
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
                "skipping distributed reconnect storm test because oxidesfu-server binary is unavailable"
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
                "skipping distributed reconnect storm test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!(
            "distributed-process-reconnect-storm-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "distributed-process-reconnect-storm-identity-{}",
            unique_suffix()
        );
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B");

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Reconnect Storm Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let (mut current_socket, mut participant_sid) = connect_join_and_hold_socket(
            &node_a_base_url,
            &token,
            &join_request_param(),
        )
        .await;

        for _attempt in 0..5 {
            let _ = current_socket.send(Message::Close(None)).await;
            let (next_socket, next_sid) = reconnect_and_hold_socket(
                &node_a_base_url,
                &token,
                &reconnect_join_request_param(
                    &participant_sid,
                    proto::ReconnectReason::RrSignalDisconnected,
                ),
                &participant_sid,
            )
            .await;
            current_socket = next_socket;
            participant_sid = next_sid;

            let remote = wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
                .await
                .expect("remote-selected node should own participant during reconnect storm");
            assert_eq!(remote.sid, participant_sid);
            assert!(
                local_client
                    .get_participant(&room_name, &identity)
                    .await
                    .is_err(),
                "origin node should remain non-owner during reconnect storm"
            );
        }

        let _ = current_socket.send(Message::Close(None)).await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_reconnect_soak_keeps_remote_ownership_and_ping_liveness() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed reconnect soak test because redis-server is not on PATH"
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
                "skipping distributed reconnect soak test because oxidesfu-server binary is unavailable"
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
                "skipping distributed reconnect soak test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!(
            "distributed-process-reconnect-soak-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "distributed-process-reconnect-soak-identity-{}",
            unique_suffix()
        );
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B");

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Reconnect Soak Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let (mut current_socket, mut participant_sid) = connect_join_and_hold_socket(
            &node_a_base_url,
            &token,
            &join_request_param(),
        )
        .await;

        for attempt in 0..20 {
            let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");
            let ping_request = proto::SignalRequest {
                message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                    timestamp: ping_timestamp,
                    ..Default::default()
                })),
            };
            current_socket
                .send(Message::Binary(ping_request.encode_to_vec().into()))
                .await
                .expect("ping request should send");
            let pong_message = current_socket
                .next()
                .await
                .expect("pong response should arrive")
                .expect("pong response should be ok");
            let Message::Binary(pong_bytes) = pong_message else {
                panic!("expected binary pong response");
            };
            let pong_response = proto::SignalResponse::decode(pong_bytes.as_ref())
                .expect("pong response should decode");
            let Some(proto::signal_response::Message::PongResp(pong)) = pong_response.message else {
                panic!("expected pong response after ping request");
            };
            assert_eq!(pong.last_ping_timestamp, ping_timestamp);

            let _ = current_socket.send(Message::Close(None)).await;
            let (next_socket, next_sid) = reconnect_and_hold_socket(
                &node_a_base_url,
                &token,
                &reconnect_join_request_param(
                    &participant_sid,
                    proto::ReconnectReason::RrSignalDisconnected,
                ),
                &participant_sid,
            )
            .await;
            current_socket = next_socket;
            participant_sid = next_sid;

            let remote = wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
                .await
                .expect("remote-selected node should own participant during reconnect soak");
            assert_eq!(remote.sid, participant_sid);
            assert!(
                local_client
                    .get_participant(&room_name, &identity)
                    .await
                    .is_err(),
                "origin node should remain non-owner during reconnect soak (attempt {attempt})"
            );
        }

        let _ = current_socket.send(Message::Close(None)).await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_multi_room_reconnect_soak_keeps_remote_ownership() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed multi-room reconnect soak test because redis-server is not on PATH"
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
                "skipping distributed multi-room reconnect soak test because oxidesfu-server binary is unavailable"
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
                "skipping distributed multi-room reconnect soak test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let mut active_sessions = Vec::new();
        for room_index in 0..2 {
            let room_name = format!(
                "distributed-process-multi-room-reconnect-soak-room-{room_index}-{}",
                unique_suffix()
            );
            let identity = format!(
                "distributed-process-multi-room-reconnect-soak-identity-{room_index}-{}",
                unique_suffix()
            );
            force_room_assignment_to_node(
                &redis_url,
                &room_name,
                &format!("oxidesfu-local-{node_b_port}"),
            )
            .expect("room assignment should target node B");

            let token = AccessToken::with_api_key(API_KEY, API_SECRET)
                .with_identity(&identity)
                .with_name("Distributed Multi-room Reconnect Soak Identity")
                .with_grants(VideoGrants {
                    room_join: true,
                    room: room_name.clone(),
                    ..Default::default()
                })
                .to_jwt()
                .expect("SDK access token should encode");

            let (socket, participant_sid) =
                connect_join_and_hold_socket(&node_a_base_url, &token, &join_request_param()).await;
            active_sessions.push((room_name, identity, token, socket, participant_sid));
        }

        for attempt in 0..6 {
            for (room_name, identity, token, socket, participant_sid) in &mut active_sessions {
                let ping_timestamp =
                    i64::try_from(unique_suffix()).expect("millis suffix should fit i64");
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
                let pong_message = socket
                    .next()
                    .await
                    .expect("pong response should arrive")
                    .expect("pong response should be ok");
                let Message::Binary(pong_bytes) = pong_message else {
                    panic!("expected binary pong response");
                };
                let pong_response = proto::SignalResponse::decode(pong_bytes.as_ref())
                    .expect("pong response should decode");
                let Some(proto::signal_response::Message::PongResp(pong)) = pong_response.message else {
                    panic!("expected pong response after ping request");
                };
                assert_eq!(pong.last_ping_timestamp, ping_timestamp);

                let _ = socket.send(Message::Close(None)).await;
                let (next_socket, next_sid) = reconnect_and_hold_socket(
                    &node_a_base_url,
                    token,
                    &reconnect_join_request_param(
                        participant_sid,
                        proto::ReconnectReason::RrSignalDisconnected,
                    ),
                    participant_sid,
                )
                .await;
                *socket = next_socket;
                *participant_sid = next_sid;

                let remote = wait_for_participant_on_room_client(&remote_client, room_name, identity)
                    .await
                    .expect("remote-selected node should own participant during multi-room reconnect soak");
                assert_eq!(remote.sid, *participant_sid);
                assert!(
                    local_client
                        .get_participant(room_name, identity)
                        .await
                        .is_err(),
                    "origin node should remain non-owner during multi-room reconnect soak (attempt {attempt})"
                );
            }
        }

        for (_room, _identity, _token, mut socket, _sid) in active_sessions {
            let _ = socket.send(Message::Close(None)).await;
        }
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_redis_outage_new_room_join_falls_back_to_local_owner() {
        let redis_port = reserve_local_port();
        let Some(mut redis) = spawn_redis_server(redis_port).await else {
            eprintln!(
                "skipping distributed redis outage new-room fallback test because redis-server and docker are unavailable"
            );
            return;
        };

        let redis_url = format!("redis://127.0.0.1:{redis_port}/0");
        if let Err(err) = wait_for_redis_ready(&redis_url).await {
            eprintln!("skipping distributed redis outage new-room fallback test because redis failed to start: {err}");
            let _ = redis.kill().await;
            return;
        }

        let node_a_port = reserve_local_port();
        let node_b_port = reserve_local_port();

        let Some((mut node_a, node_a_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed redis outage new-room fallback test because oxidesfu-server binary is unavailable"
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
                "skipping distributed redis outage new-room fallback test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register before redis outage");

        let _ = redis.kill().await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let room_name = format!("distributed-redis-outage-new-room-{}", unique_suffix());
        let identity = format!("distributed-redis-outage-new-identity-{}", unique_suffix());

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Redis Outage New Room")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);

        let (client, join, _events) =
            SignalClient::connect(&node_a_base_url, &token, options, None)
                .await
                .expect("new-room join should fall back to local owner during redis outage");
        assert_eq!(
            join.room.expect("join room should be present").name,
            room_name,
            "join response should report requested room during fallback"
        );

        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let local = wait_for_participant_on_room_client(&local_client, &room_name, &identity)
            .await
            .expect("origin node should own fallback participant during redis outage");
        assert!(!local.sid.is_empty());
        assert!(
            remote_client
                .get_participant(&room_name, &identity)
                .await
                .is_err(),
            "peer node should not own new-room participant while redis is unavailable"
        );

        client.close().await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_redis_outage_and_recovery_preserves_active_data_sessions() {
        let redis_port = reserve_local_port();
        let Some(mut redis) = spawn_redis_server(redis_port).await else {
            eprintln!(
                "skipping distributed redis outage/recovery test because redis-server and docker are unavailable"
            );
            return;
        };

        let redis_url = format!("redis://127.0.0.1:{redis_port}/0");
        if let Err(err) = wait_for_redis_ready(&redis_url).await {
            eprintln!("skipping distributed redis outage/recovery test because redis failed to start: {err}");
            let _ = redis.kill().await;
            return;
        }

        let node_a_port = reserve_local_port();
        let node_b_port = reserve_local_port();

        let Some((mut node_a, node_a_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A process startup should succeed when binary is available")
        else {
            eprintln!(
                "skipping distributed redis outage/recovery test because oxidesfu-server binary is unavailable"
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
                "skipping distributed redis outage/recovery test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register before outage");

        let room_name = format!("distributed-redis-outage-data-room-{}", unique_suffix());
        let publisher_identity = format!("distributed-redis-outage-publisher-{}", unique_suffix());
        let subscriber_identity = format!("distributed-redis-outage-subscriber-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B before redis outage");

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&publisher_identity)
            .with_name("Distributed Redis Outage Publisher")
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
            .with_name("Distributed Redis Outage Subscriber")
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
            Room::connect(&node_a_base_url, &subscriber_token, options)
                .await
                .expect("subscriber should connect through origin node");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        publisher_room
            .local_participant()
            .publish_data(DataPacket {
                payload: b"before-redis-outage".to_vec(),
                topic: Some("distributed-redis".to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .expect("data publish before redis outage should succeed");
        let (payload_before, topic_before, kind_before) = next_data_received(&mut subscriber_events).await;
        assert_eq!(payload_before.as_slice(), b"before-redis-outage");
        assert_eq!(topic_before.as_deref(), Some("distributed-redis"));
        assert_eq!(kind_before, DataPacketKind::Reliable);

        let _ = redis.kill().await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        publisher_room
            .local_participant()
            .publish_data(DataPacket {
                payload: b"during-redis-outage".to_vec(),
                topic: Some("distributed-redis".to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .expect("data publish during redis outage should still succeed for active sessions");
        let (payload_during, topic_during, kind_during) = next_data_received(&mut subscriber_events).await;
        assert_eq!(payload_during.as_slice(), b"during-redis-outage");
        assert_eq!(topic_during.as_deref(), Some("distributed-redis"));
        assert_eq!(kind_during, DataPacketKind::Reliable);

        let Some(mut redis_restarted) = spawn_redis_server(redis_port).await else {
            panic!("redis should restart on the same port for recovery validation");
        };
        wait_for_redis_ready(&redis_url)
            .await
            .expect("redis should become ready after restart");
        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both nodes should re-register after redis recovery");

        publisher_room
            .local_participant()
            .publish_data(DataPacket {
                payload: b"after-redis-recovery".to_vec(),
                topic: Some("distributed-redis".to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .expect("data publish after redis recovery should succeed");
        let (payload_after, topic_after, kind_after) = next_data_received(&mut subscriber_events).await;
        assert_eq!(payload_after.as_slice(), b"after-redis-recovery");
        assert_eq!(topic_after.as_deref(), Some("distributed-redis"));
        assert_eq!(kind_after, DataPacketKind::Reliable);

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let _remote_publisher =
            wait_for_participant_on_room_client(&remote_client, &room_name, &publisher_identity)
                .await
                .expect("remote owner should keep publisher after redis outage/recovery");
        let _remote_subscriber =
            wait_for_participant_on_room_client(&remote_client, &room_name, &subscriber_identity)
                .await
                .expect("remote owner should keep subscriber after redis outage/recovery");

        let _ = subscriber_room.close().await;
        let _ = publisher_room.close().await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis_restarted.kill().await;
    }

    #[tokio::test]
    async fn distributed_two_process_relay_worker_restart_preserves_remote_owned_signal_reconnect_and_ping()
    {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping distributed relay worker restart test because redis-server is not on PATH"
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
                "skipping distributed relay worker restart test because oxidesfu-server binary is unavailable"
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
                "skipping distributed relay worker restart test because oxidesfu-server binary is unavailable"
            );
            let _ = node_a.kill().await;
            let _ = redis.kill().await;
            return;
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both oxidesfu nodes should register in shared redis directory");

        let room_name = format!("distributed-relay-worker-restart-room-{}", unique_suffix());
        let identity = format!("distributed-relay-worker-restart-identity-{}", unique_suffix());
        force_room_assignment_to_node(
            &redis_url,
            &room_name,
            &format!("oxidesfu-local-{node_b_port}"),
        )
        .expect("room assignment should target node B before relay worker restart");

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Relay Worker Restart Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let (mut first_socket, first_sid) =
            connect_join_and_hold_socket(&node_a_base_url, &token, &join_request_param()).await;

        let remote_client = RoomClient::with_api_key(&node_b_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let local_client = RoomClient::with_api_key(&node_a_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let remote_owned = wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
            .await
            .expect("remote owner should own participant before relay worker restart");
        assert_eq!(remote_owned.sid, first_sid);
        assert!(
            local_client.get_participant(&room_name, &identity).await.is_err(),
            "origin node should be non-owner before relay worker restart"
        );

        let ping_timestamp_before = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");
        let ping_before = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp_before,
                ..Default::default()
            })),
        };
        first_socket
            .send(Message::Binary(ping_before.encode_to_vec().into()))
            .await
            .expect("ping before relay worker restart should send");
        let pong_before = first_socket
            .next()
            .await
            .expect("pong before relay worker restart should arrive")
            .expect("pong before relay worker restart should be ok");
        let Message::Binary(pong_before_bytes) = pong_before else {
            panic!("expected binary pong before relay worker restart");
        };
        let pong_before_response = proto::SignalResponse::decode(pong_before_bytes.as_ref())
            .expect("pong before relay worker restart should decode");
        let Some(proto::signal_response::Message::PongResp(pong)) = pong_before_response.message else {
            panic!("expected pong response before relay worker restart");
        };
        assert_eq!(pong.last_ping_timestamp, ping_timestamp_before);

        let _ = node_a.kill().await;
        let Some((mut node_a_restarted, node_a_restart_base_url)) =
            spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
                .await
                .expect("node A restart should succeed when binary is available")
        else {
            panic!("node A should restart for relay worker restart validation");
        };

        wait_for_room_node_registration_count(&redis_url, 2)
            .await
            .expect("both nodes should be registered after origin restart");

        let (mut reconnect_socket, reconnect_sid) = reconnect_and_hold_socket(
            &node_a_restart_base_url,
            &token,
            &reconnect_join_request_param(&first_sid, proto::ReconnectReason::RrSignalDisconnected),
            &first_sid,
        )
        .await;

        let remote_owned_after =
            wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
                .await
                .expect("remote owner should keep participant after relay worker restart");
        assert_eq!(remote_owned_after.sid, reconnect_sid);

        let local_client_after = RoomClient::with_api_key(&node_a_restart_base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        assert!(
            local_client_after
                .get_participant(&room_name, &identity)
                .await
                .is_err(),
            "restarted origin node should remain non-owner for remote-owned participant"
        );

        let ping_timestamp_after = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");
        let ping_after = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp_after,
                ..Default::default()
            })),
        };
        reconnect_socket
            .send(Message::Binary(ping_after.encode_to_vec().into()))
            .await
            .expect("ping after relay worker restart should send");
        let pong_after = reconnect_socket
            .next()
            .await
            .expect("pong after relay worker restart should arrive")
            .expect("pong after relay worker restart should be ok");
        let Message::Binary(pong_after_bytes) = pong_after else {
            panic!("expected binary pong after relay worker restart");
        };
        let pong_after_response = proto::SignalResponse::decode(pong_after_bytes.as_ref())
            .expect("pong after relay worker restart should decode");
        let Some(proto::signal_response::Message::PongResp(pong)) = pong_after_response.message else {
            panic!("expected pong response after relay worker restart");
        };
        assert_eq!(pong.last_ping_timestamp, ping_timestamp_after);

        let _ = reconnect_socket.send(Message::Close(None)).await;
        let _ = node_a_restarted.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }

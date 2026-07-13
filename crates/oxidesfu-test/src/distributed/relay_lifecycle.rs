use super::*;

    #[tokio::test]
    async fn signal_join_falls_back_to_local_when_room_node_selection_errors() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(true));
        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory.clone();
        oxidesfu_server::register_local_room_node(&room_nodes, &config)
            .expect("local room node registration should succeed");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_and_room_nodes(api_state, Some(room_nodes)),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("placement-fallback-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("placement-fallback")
            .with_name("Placement Fallback")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let (client, join, _events) =
            SignalClient::connect(&format!("http://{addr}"), &token, options, None)
                .await
                .expect("signal join should still succeed when room-node placement errors");

        assert_eq!(
            join.room.expect("join room should be present").name,
            room_name
        );
        assert!(
            probe_directory
                .selected_rooms()
                .iter()
                .any(|selected| selected == &room_name),
            "room placement should still be attempted even when fallback is used"
        );

        client.close().await;
        server.abort();
    }
    #[tokio::test]
    async fn signal_join_records_room_node_selection_when_directory_serves() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory.clone();
        let registered = oxidesfu_server::register_local_room_node(&room_nodes, &config)
            .expect("local room node registration should succeed");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_and_room_nodes(api_state, Some(room_nodes)),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("placement-serving-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("placement-serving")
            .with_name("Placement Serving")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let (client, join, _events) =
            SignalClient::connect(&format!("http://{addr}"), &token, options, None)
                .await
                .expect("signal join should succeed when room-node placement succeeds");

        assert_eq!(
            join.room.expect("join room should be present").name,
            room_name
        );
        assert!(
            probe_directory
                .selected_rooms()
                .iter()
                .any(|selected| selected == &room_name),
            "room placement should be attempted for signal join"
        );
        let mapped = probe_directory
            .get_node_for_room(&room_name)
            .expect("serving placement should map room to a node");
        assert_eq!(mapped.id, registered.id);

        client.close().await;
        server.abort();
    }
    #[tokio::test]
    async fn signal_join_succeeds_even_when_room_is_mapped_to_remote_node_today() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        probe_directory
            .register_node(RegisteredNode {
                id: "node-remote".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("remote node should register in probe directory");

        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_and_room_nodes(api_state, Some(room_nodes)),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("placement-remote-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("placement-remote")
            .with_name("Placement Remote")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let (client, join, _events) =
            SignalClient::connect(&format!("http://{addr}"), &token, options, None)
                .await
                .expect("signal join currently falls back to local handling");

        assert_eq!(
            join.room.expect("join room should be present").name,
            room_name
        );

        let mapped = probe_directory
            .get_node_for_room(&room_name)
            .expect("room should be mapped to selected directory node");
        assert_eq!(mapped.id, "node-remote");

        client.close().await;
        server.abort();
    }
    #[tokio::test]
    async fn signal_join_rejects_non_local_placement_when_strict_mode_enabled() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        probe_directory
            .register_node(RegisteredNode {
                id: "node-remote".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("remote node should register in probe directory");

        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_room_nodes_and_placement(
                    api_state,
                    Some(room_nodes),
                    Some("node-local".to_string()),
                    true,
                ),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("placement-remote-strict-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("placement-remote-strict")
            .with_name("Placement Remote Strict")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let result = SignalClient::connect(&format!("http://{addr}"), &token, options, None).await;
        assert!(
            result.is_err(),
            "strict non-local placement should reject signal join"
        );

        server.abort();
    }
    #[tokio::test]
    async fn signal_join_rejects_non_local_placement_when_strict_mode_comes_from_cli_config() {
        let config = oxidesfu_core::ServerConfig::from_env_args_or_development([
            "--reject-non-local-room-placement".to_string(),
            "true".to_string(),
        ])
        .expect("cli config should parse");
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        probe_directory
            .register_node(RegisteredNode {
                id: "node-remote".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("remote node should register in probe directory");

        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_room_nodes_from_config(
                    api_state,
                    Some(room_nodes),
                    Some("node-local".to_string()),
                    &config,
                ),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("placement-remote-strict-config-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("placement-remote-strict-config")
            .with_name("Placement Remote Strict Config")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let result = SignalClient::connect(&format!("http://{addr}"), &token, options, None).await;
        assert!(
            result.is_err(),
            "strict non-local placement from CLI config should reject signal join"
        );

        server.abort();
    }
    #[tokio::test]
    async fn signal_join_non_local_placement_emits_relay_intent_when_dispatcher_wired_from_config()
    {
        let config = oxidesfu_core::ServerConfig::from_env_args_or_development([
            "--reject-non-local-room-placement".to_string(),
            "false".to_string(),
        ])
        .expect("cli config should parse");
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        probe_directory
            .register_node(RegisteredNode {
                id: "node-remote".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("remote node should register in probe directory");
        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory.clone();
        let relay_dispatcher = Arc::new(RecordingRelayDispatcher::default());
        let relay_probe = relay_dispatcher.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_room_nodes_from_config_and_relay_dispatcher(
                    api_state,
                    Some(room_nodes),
                    Some("node-local".to_string()),
                    &config,
                    relay_dispatcher.clone(),
                ),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("placement-relay-intent-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("placement-relay-identity")
            .with_name("Placement Relay Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let (client, join, _events) =
            SignalClient::connect(&format!("http://{addr}"), &token, options, None)
                .await
                .expect("non-strict non-local placement should still fallback-join locally");

        assert_eq!(
            join.room.expect("join room should be present").name,
            room_name
        );

        let intents = relay_probe.take();
        assert_eq!(intents.len(), 1, "one relay intent should be emitted");
        assert_eq!(intents[0].room, room_name);
        assert_eq!(intents[0].identity, "placement-relay-identity");
        assert_eq!(intents[0].name, "Placement Relay Identity");
        assert!(
            intents[0].requested_participant_sid.is_none(),
            "non-reconnect join should not include requested participant sid"
        );
        assert_eq!(intents[0].selected_room_node_id, "node-remote");

        client.close().await;
        server.abort();
    }
    #[tokio::test]
    async fn distributed_join_non_local_dispatches_to_remote_node_over_inmemory_bus() {
        struct InMemoryRelayBusDispatcher {
            tx: tokio::sync::mpsc::UnboundedSender<NonLocalRelayJoinIntent>,
        }

        impl NonLocalRelayDispatcher for InMemoryRelayBusDispatcher {
            fn dispatch_non_local_join(
                &self,
                intent: NonLocalRelayJoinIntent,
            ) -> Result<Option<oxidesfu_signaling::NonLocalRelayJoinResponse>, String> {
                let _ = self.tx.send(intent);
                Ok(None)
            }

            fn dispatch_non_local_termination(
                &self,
                _intent: oxidesfu_signaling::NonLocalRelaySessionTerminationIntent,
            ) -> Result<(), String> {
                Ok(())
            }
        }

        let config = oxidesfu_core::ServerConfig::development();
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        probe_directory
            .register_node(RegisteredNode {
                id: "node-remote".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("remote node should register in probe directory");
        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<NonLocalRelayJoinIntent>();
        let dispatcher: Arc<dyn NonLocalRelayDispatcher> =
            Arc::new(InMemoryRelayBusDispatcher { tx });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_room_nodes_placement_and_relay_dispatcher(
                    api_state,
                    Some(room_nodes),
                    Some("node-local".to_string()),
                    false,
                    dispatcher,
                ),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("distributed-relay-bus-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("distributed-bus-identity")
            .with_name("Distributed Bus Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let (client, join, _events) =
            SignalClient::connect(&format!("http://{addr}"), &token, options, None)
                .await
                .expect("non-strict non-local placement should join via local fallback");
        assert_eq!(
            join.room.expect("join room should be present").name,
            room_name
        );

        let relayed = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("in-memory bus should receive relay intent")
            .expect("relay intent should be present");
        assert_eq!(relayed.room, room_name);
        assert_eq!(relayed.identity, "distributed-bus-identity");
        assert_eq!(relayed.selected_room_node_id, "node-remote");

        client.close().await;
        server.abort();
    }
    #[tokio::test]
    async fn distributed_relay_worker_end_to_end_websocket_join_returns_remote_response() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        probe_directory
            .register_node(RegisteredNode {
                id: "node-remote".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("remote node should register in probe directory");
        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory;

        let redis_store = InMemoryRedisHashStore::default();
        let dispatcher_mailbox = RedisRelayMailbox::with_store(redis_store.clone());
        let worker_mailbox = RedisRelayMailbox::with_store(redis_store);

        let relay_readiness = Arc::new(oxidesfu_server::RelayWorkerReadiness::new(true));
        let (worker_shutdown_tx, worker_shutdown_rx) = tokio::sync::oneshot::channel();
        let worker = oxidesfu_server::spawn_relay_intent_worker(
            worker_mailbox,
            "node-remote".to_string(),
            Arc::new(oxidesfu_server::RoomStoreRelayJoinIntentExecutor::new(
                api_state.rooms.clone(),
            )),
            Duration::from_millis(5),
            worker_shutdown_rx,
            relay_readiness.clone(),
        );

        let relay_dispatcher: Arc<dyn NonLocalRelayDispatcher> =
            Arc::new(RedisMailboxRelayDispatcher::with_mailbox_and_policy(
                dispatcher_mailbox,
                Arc::new(NoopRelayIntentExecutionDriver),
                Duration::from_millis(5),
                Duration::from_secs(2),
                0,
                Duration::ZERO,
                None,
            ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_room_nodes_from_config_relay_dispatcher_and_readiness(
                    api_state,
                    Some(room_nodes),
                    Some("node-local".to_string()),
                    &config,
                    relay_dispatcher,
                    relay_readiness,
                ),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("distributed-relay-worker-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("distributed-relay-worker-identity")
            .with_name("Distributed Relay Worker Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let join_request = proto::JoinRequest::default();
        let join_request_param =
            general_purpose::URL_SAFE_NO_PAD.encode(join_request.encode_to_vec());

        let url = format!("ws://{addr}/rtc/v1?join_request={join_request_param}");
        let mut request = url
            .into_client_request()
            .expect("websocket request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .expect("authorization header should parse"),
        );

        let (mut socket, _) = connect_async(request)
            .await
            .expect("websocket should connect");

        let first = socket
            .next()
            .await
            .expect("first websocket message should arrive")
            .expect("first websocket message should be ok");
        let Message::Binary(bytes) = first else {
            panic!("expected binary protobuf signal response");
        };
        let response =
            proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
        let Some(proto::signal_response::Message::Join(join)) = response.message else {
            panic!("expected join response");
        };

        let participant = join
            .participant
            .expect("relay response should include participant");
        assert!(participant.sid.starts_with("PA_"));

        worker_shutdown_tx
            .send(())
            .expect("relay worker shutdown signal should send once");
        let _ = tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("relay worker should stop cleanly");

        server.abort();
    }
    #[tokio::test]
    async fn distributed_two_node_relay_join_is_remote_owned_and_not_written_to_origin_store() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state_a = oxidesfu_server::api_state_from_config(&config);
        let api_state_b = oxidesfu_server::api_state_from_config(&config);

        let room_nodes = Arc::new(PlacementProbeDirectory::new(false));
        room_nodes
            .register_node(RegisteredNode {
                id: "node-b".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("node-b should register in probe directory");
        let room_nodes_arc: Arc<dyn RoomNodeDirectory> = room_nodes;

        let dispatcher_mailbox = RedisRelayMailbox::with_store(InMemoryRedisHashStore::default());

        let relay_readiness = Arc::new(oxidesfu_server::RelayWorkerReadiness::new(true));
        let relay_dispatcher_a: Arc<dyn NonLocalRelayDispatcher> =
            Arc::new(RedisMailboxRelayDispatcher::with_mailbox_and_policy(
                dispatcher_mailbox,
                Arc::new(RemoteRoomStoreExecutionDriver {
                    remote_rooms: api_state_b.rooms.clone(),
                }),
                Duration::from_millis(1),
                Duration::from_secs(2),
                0,
                Duration::ZERO,
                Some(1024),
            ));

        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener A should bind");
        let addr_a = listener_a
            .local_addr()
            .expect("listener A should have local addr");
        let app_a =
            oxidesfu_server::app_with_api_room_nodes_from_config_relay_dispatcher_and_readiness(
                api_state_a.clone(),
                Some(room_nodes_arc.clone()),
                Some("node-a".to_string()),
                &config,
                relay_dispatcher_a,
                relay_readiness.clone(),
            );
        let server_a = tokio::spawn(async move {
            axum::serve(listener_a, app_a)
                .await
                .expect("node A server should run");
        });

        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener B should bind");
        let app_b = oxidesfu_server::app_with_api_room_nodes_from_config(
            api_state_b.clone(),
            Some(room_nodes_arc),
            Some("node-b".to_string()),
            &config,
        );
        let server_b = tokio::spawn(async move {
            axum::serve(listener_b, app_b)
                .await
                .expect("node B server should run");
        });

        let room_name = format!("distributed-two-node-room-{}", unique_suffix());
        let identity = format!("distributed-two-node-identity-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Two Node Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let url = format!("ws://{addr_a}/rtc/v1?join_request={}", join_request_param());
        let mut request = url.into_client_request().expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .expect("authorization header should parse"),
        );

        let (mut socket, _) = connect_async(request)
            .await
            .expect("websocket should connect to node A");
        let first = socket
            .next()
            .await
            .expect("first websocket message should arrive")
            .expect("first websocket message should be ok");
        let Message::Binary(bytes) = first else {
            panic!("expected binary protobuf signal response");
        };
        let response =
            proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
        let Some(proto::signal_response::Message::Join(join)) = response.message else {
            panic!("expected join response");
        };
        let joined_participant = join
            .participant
            .expect("join response should include participant");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let remote = loop {
            match api_state_b.rooms.get_participant(&room_name, &identity) {
                Ok(value) => break value,
                Err(RoomStoreError::RoomNotFound) | Err(RoomStoreError::ParticipantNotFound)
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(err) => panic!("unexpected remote room store error: {err:?}"),
            }
        };
        assert_eq!(remote.sid, joined_participant.sid);

        let local = api_state_a.rooms.get_participant(&room_name, &identity);
        assert!(
            matches!(
                local,
                Err(RoomStoreError::RoomNotFound) | Err(RoomStoreError::ParticipantNotFound)
            ),
            "origin node room store should not own remote-relayed participant"
        );

        server_a.abort();
        server_b.abort();
    }
    #[tokio::test]
    async fn distributed_draining_owner_keeps_active_room_and_accepts_new_participants() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state_a = oxidesfu_server::api_state_from_config(&config);
        let api_state_b = oxidesfu_server::api_state_from_config(&config);

        let room_nodes = Arc::new(PlacementProbeDirectory::new(false));
        room_nodes
            .register_node(RegisteredNode {
                id: "node-a".to_string(),
                region: "origin-region".to_string(),
            })
            .expect("node-a should register in probe directory");
        room_nodes
            .register_node(RegisteredNode {
                id: "node-b".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("node-b should register in probe directory");
        let room_name = format!("distributed-draining-active-room-{}", unique_suffix());
        room_nodes
            .set_node_for_room(&room_name, "node-b")
            .expect("room should map to node-b before draining");
        room_nodes
            .set_node_draining("node-b", true)
            .expect("node-b should transition to draining");

        let room_nodes_arc: Arc<dyn RoomNodeDirectory> = room_nodes;
        let dispatcher_mailbox = RedisRelayMailbox::with_store(InMemoryRedisHashStore::default());
        let relay_readiness = Arc::new(oxidesfu_server::RelayWorkerReadiness::new(true));
        let relay_dispatcher_a: Arc<dyn NonLocalRelayDispatcher> =
            Arc::new(RedisMailboxRelayDispatcher::with_mailbox_and_policy(
                dispatcher_mailbox,
                Arc::new(RemoteRoomStoreExecutionDriver {
                    remote_rooms: api_state_b.rooms.clone(),
                }),
                Duration::from_millis(1),
                Duration::from_secs(2),
                0,
                Duration::ZERO,
                Some(1024),
            ));

        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener A should bind");
        let addr_a = listener_a
            .local_addr()
            .expect("listener A should have local addr");
        let app_a =
            oxidesfu_server::app_with_api_room_nodes_from_config_relay_dispatcher_and_readiness(
                api_state_a.clone(),
                Some(room_nodes_arc.clone()),
                Some("node-a".to_string()),
                &config,
                relay_dispatcher_a,
                relay_readiness.clone(),
            );
        let server_a = tokio::spawn(async move {
            axum::serve(listener_a, app_a)
                .await
                .expect("node A server should run");
        });

        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener B should bind");
        let app_b = oxidesfu_server::app_with_api_room_nodes_from_config(
            api_state_b.clone(),
            Some(room_nodes_arc),
            Some("node-b".to_string()),
            &config,
        );
        let server_b = tokio::spawn(async move {
            axum::serve(listener_b, app_b)
                .await
                .expect("node B server should run");
        });

        for (identity, name) in [
            (
                format!("distributed-draining-active-identity-a-{}", unique_suffix()),
                "Distributed Draining Active A",
            ),
            (
                format!("distributed-draining-active-identity-b-{}", unique_suffix()),
                "Distributed Draining Active B",
            ),
        ] {
            let token = AccessToken::with_api_key(API_KEY, API_SECRET)
                .with_identity(&identity)
                .with_name(name)
                .with_grants(VideoGrants {
                    room_join: true,
                    room: room_name.clone(),
                    ..Default::default()
                })
                .to_jwt()
                .expect("SDK access token should encode");

            let mut options = SignalOptions::default();
            options.single_peer_connection = true;
            options.connect_timeout = Duration::from_secs(5);
            let (client, _join, _events) =
                SignalClient::connect(&format!("http://{addr_a}"), &token, options, None)
                    .await
                    .expect("join to existing room on draining owner should succeed");
            client.close().await;
        }

        let remote_participants = api_state_b
            .rooms
            .list_participants(&room_name)
            .expect("remote owner should list participants for active draining room");
        assert_eq!(
            remote_participants.len(),
            2,
            "draining owner should continue serving active room joins"
        );
        assert!(
            api_state_a.rooms.list_participants(&room_name).is_err(),
            "origin should not locally own remote draining room participants"
        );

        server_a.abort();
        server_b.abort();
    }

    #[tokio::test]
    async fn distributed_draining_node_is_not_selected_for_new_room() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state_a = oxidesfu_server::api_state_from_config(&config);
        let api_state_b = oxidesfu_server::api_state_from_config(&config);

        let room_nodes = Arc::new(PlacementProbeDirectory::new(false));
        room_nodes
            .register_node(RegisteredNode {
                id: "node-a".to_string(),
                region: "origin-region".to_string(),
            })
            .expect("node-a should register in probe directory");
        room_nodes
            .register_node(RegisteredNode {
                id: "node-b".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("node-b should register in probe directory");
        room_nodes
            .set_node_draining("node-b", true)
            .expect("node-b should transition to draining");

        let room_nodes_arc: Arc<dyn RoomNodeDirectory> = room_nodes;
        let dispatcher_mailbox = RedisRelayMailbox::with_store(InMemoryRedisHashStore::default());
        let relay_readiness = Arc::new(oxidesfu_server::RelayWorkerReadiness::new(true));
        let relay_dispatcher_a: Arc<dyn NonLocalRelayDispatcher> =
            Arc::new(RedisMailboxRelayDispatcher::with_mailbox_and_policy(
                dispatcher_mailbox,
                Arc::new(RemoteRoomStoreExecutionDriver {
                    remote_rooms: api_state_b.rooms.clone(),
                }),
                Duration::from_millis(1),
                Duration::from_secs(2),
                0,
                Duration::ZERO,
                Some(1024),
            ));

        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener A should bind");
        let addr_a = listener_a
            .local_addr()
            .expect("listener A should have local addr");
        let app_a =
            oxidesfu_server::app_with_api_room_nodes_from_config_relay_dispatcher_and_readiness(
                api_state_a.clone(),
                Some(room_nodes_arc.clone()),
                Some("node-a".to_string()),
                &config,
                relay_dispatcher_a,
                relay_readiness.clone(),
            );
        let server_a = tokio::spawn(async move {
            axum::serve(listener_a, app_a)
                .await
                .expect("node A server should run");
        });

        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener B should bind");
        let app_b = oxidesfu_server::app_with_api_room_nodes_from_config(
            api_state_b.clone(),
            Some(room_nodes_arc),
            Some("node-b".to_string()),
            &config,
        );
        let server_b = tokio::spawn(async move {
            axum::serve(listener_b, app_b)
                .await
                .expect("node B server should run");
        });

        let room_name = format!("distributed-draining-new-room-{}", unique_suffix());
        let identity = format!("distributed-draining-new-room-identity-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Draining New Room")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (client, _join, _events) =
            SignalClient::connect(&format!("http://{addr_a}"), &token, options, None)
                .await
                .expect("new-room join should succeed on non-draining node");

        let local = api_state_a
            .rooms
            .get_participant(&room_name, &identity)
            .expect("non-draining local node should own new room participant");
        assert_eq!(local.identity, identity);
        assert!(
            api_state_b.rooms.get_participant(&room_name, &identity).is_err(),
            "draining node should not be selected for new room ownership"
        );

        client.close().await;

        server_a.abort();
        server_b.abort();
    }

    #[tokio::test]
    async fn distributed_room_service_non_owner_methods_forward_to_remote_owned_room() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state_a = oxidesfu_server::api_state_from_config(&config);
        let api_state_b = oxidesfu_server::api_state_from_config(&config);

        let room_nodes = Arc::new(PlacementProbeDirectory::new(false));
        room_nodes
            .register_node(RegisteredNode {
                id: "node-b".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("node-b should register in probe directory");
        let room_nodes_arc: Arc<dyn RoomNodeDirectory> = room_nodes;

        let dispatcher_mailbox = RedisRelayMailbox::with_store(InMemoryRedisHashStore::default());
        let relay_readiness = Arc::new(oxidesfu_server::RelayWorkerReadiness::new(true));
        let relay_dispatcher_a: Arc<dyn NonLocalRelayDispatcher> =
            Arc::new(RedisMailboxRelayDispatcher::with_mailbox_and_policy(
                dispatcher_mailbox,
                Arc::new(RemoteRoomStoreExecutionDriver {
                    remote_rooms: api_state_b.rooms.clone(),
                }),
                Duration::from_millis(1),
                Duration::from_secs(2),
                0,
                Duration::ZERO,
                Some(1024),
            ));

        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener A should bind");
        let addr_a = listener_a
            .local_addr()
            .expect("listener A should have local addr");
        let app_a =
            oxidesfu_server::app_with_api_room_nodes_from_config_relay_dispatcher_and_readiness(
                api_state_a.clone(),
                Some(room_nodes_arc.clone()),
                Some("node-a".to_string()),
                &config,
                relay_dispatcher_a,
                relay_readiness.clone(),
            );
        let server_a = tokio::spawn(async move {
            axum::serve(listener_a, app_a)
                .await
                .expect("node A server should run");
        });

        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener B should bind");
        let addr_b = listener_b
            .local_addr()
            .expect("listener B should have local addr");
        let app_b = oxidesfu_server::app_with_api_room_nodes_from_config(
            api_state_b.clone(),
            Some(room_nodes_arc),
            Some("node-b".to_string()),
            &config,
        );
        let server_b = tokio::spawn(async move {
            axum::serve(listener_b, app_b)
                .await
                .expect("node B server should run");
        });

        let room_name = format!("distributed-non-owner-room-service-room-{}", unique_suffix());
        let identity = format!(
            "distributed-non-owner-room-service-identity-{}",
            unique_suffix()
        );
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Non Owner RoomService")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let url = format!("ws://{addr_a}/rtc/v1?join_request={}", join_request_param());
        let mut request = url.into_client_request().expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .expect("authorization header should parse"),
        );

        let (mut socket, _) = connect_async(request)
            .await
            .expect("websocket should connect to node A");
        let first = socket
            .next()
            .await
            .expect("first websocket message should arrive")
            .expect("first websocket message should be ok");
        let Message::Binary(bytes) = first else {
            panic!("expected binary protobuf signal response");
        };
        let response =
            proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
        let Some(proto::signal_response::Message::Join(join)) = response.message else {
            panic!("expected join response");
        };
        let joined_participant = join
            .participant
            .expect("join response should include participant");

        let origin_client =
            RoomClient::with_api_key(&format!("http://{addr_a}"), API_KEY, API_SECRET)
                .with_failover(false)
                .with_request_timeout(Duration::from_secs(5));
        let remote_client =
            RoomClient::with_api_key(&format!("http://{addr_b}"), API_KEY, API_SECRET)
                .with_failover(false)
                .with_request_timeout(Duration::from_secs(5));

        let remote = wait_for_participant_on_room_client(&remote_client, &room_name, &identity)
            .await
            .expect("remote owner should resolve participant through RoomService");
        assert_eq!(remote.sid, joined_participant.sid);

        let origin_participant = origin_client
            .get_participant(&room_name, &identity)
            .await
            .expect("origin non-owner GetParticipant should forward to remote owner");
        assert_eq!(origin_participant.sid, joined_participant.sid);

        let origin_list = origin_client
            .list_participants(&room_name)
            .await
            .expect("origin non-owner ListParticipants should forward to remote owner");
        assert!(
            origin_list.iter().any(|participant| participant.identity == identity),
            "origin non-owner participant list should include remote-owned participant"
        );

        let rooms_from_origin = origin_client
            .list_rooms(vec![room_name.clone()])
            .await
            .expect("origin non-owner ListRooms should include remote-owned room");
        assert!(
            rooms_from_origin.iter().any(|room| room.name == room_name),
            "origin non-owner list rooms should include remote-owned room"
        );

        origin_client
            .update_subscriptions(&room_name, &identity, vec!["TR_missing".to_string()], false)
            .await
            .expect("origin non-owner UpdateSubscriptions should forward to remote owner");

        origin_client
            .send_data(
                &room_name,
                b"non-owner-send-data".to_vec(),
                SendDataOptions {
                    kind: proto::data_packet::Kind::Reliable,
                    destination_identities: vec![identity.clone()],
                    ..Default::default()
                },
            )
            .await
            .expect("origin non-owner SendData should forward to remote owner");

        origin_client
            .remove_participant(&room_name, &identity)
            .await
            .expect("origin non-owner RemoveParticipant should forward to remote owner");

        assert!(
            remote_client.get_participant(&room_name, &identity).await.is_err(),
            "remove forwarded from origin should remove participant on remote owner"
        );

        let recreate_identity = format!(
            "distributed-non-owner-room-service-identity-recreated-{}",
            unique_suffix()
        );
        let recreate_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&recreate_identity)
            .with_name("Distributed Non Owner RoomService Recreated")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");
        let recreate_url = format!("ws://{addr_a}/rtc/v1?join_request={}", join_request_param());
        let mut recreate_request = recreate_url.into_client_request().expect("request should build");
        recreate_request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {recreate_token}"))
                .expect("authorization header should parse"),
        );
        let (mut recreate_socket, _) = connect_async(recreate_request)
            .await
            .expect("websocket should connect for recreated participant");
        let recreate_first = recreate_socket
            .next()
            .await
            .expect("first websocket message should arrive")
            .expect("first websocket message should be ok");
        let Message::Binary(recreate_bytes) = recreate_first else {
            panic!("expected binary protobuf signal response for recreated participant");
        };
        let recreate_response =
            proto::SignalResponse::decode(recreate_bytes.as_ref()).expect("signal response should decode");
        let Some(proto::signal_response::Message::Join(_)) = recreate_response.message else {
            panic!("expected join response for recreated participant");
        };

        origin_client
            .delete_room(&room_name)
            .await
            .expect("origin non-owner DeleteRoom should forward to remote owner");

        assert!(
            remote_client.list_participants(&room_name).await.is_err(),
            "delete forwarded from origin should delete room on remote owner"
        );

        let _ = recreate_socket.send(Message::Close(None)).await;

        let _ = socket.send(Message::Close(None)).await;
        server_a.abort();
        server_b.abort();
    }

    #[tokio::test]
    async fn distributed_two_node_relay_rejoin_same_identity_stays_remote_owned_under_churn() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state_a = oxidesfu_server::api_state_from_config(&config);
        let api_state_b = oxidesfu_server::api_state_from_config(&config);

        let room_nodes = Arc::new(PlacementProbeDirectory::new(false));
        room_nodes
            .register_node(RegisteredNode {
                id: "node-b".to_string(),
                region: "remote-region".to_string(),
            })
            .expect("node-b should register in probe directory");
        let room_nodes_arc: Arc<dyn RoomNodeDirectory> = room_nodes;

        let dispatcher_mailbox = RedisRelayMailbox::with_store(InMemoryRedisHashStore::default());

        let relay_readiness = Arc::new(oxidesfu_server::RelayWorkerReadiness::new(true));
        let relay_dispatcher_a: Arc<dyn NonLocalRelayDispatcher> =
            Arc::new(RedisMailboxRelayDispatcher::with_mailbox_and_policy(
                dispatcher_mailbox,
                Arc::new(RemoteRoomStoreExecutionDriver {
                    remote_rooms: api_state_b.rooms.clone(),
                }),
                Duration::from_millis(1),
                Duration::from_secs(2),
                0,
                Duration::ZERO,
                Some(1024),
            ));

        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener A should bind");
        let addr_a = listener_a
            .local_addr()
            .expect("listener A should have local addr");
        let app_a =
            oxidesfu_server::app_with_api_room_nodes_from_config_relay_dispatcher_and_readiness(
                api_state_a.clone(),
                Some(room_nodes_arc.clone()),
                Some("node-a".to_string()),
                &config,
                relay_dispatcher_a,
                relay_readiness.clone(),
            );
        let server_a = tokio::spawn(async move {
            axum::serve(listener_a, app_a)
                .await
                .expect("node A server should run");
        });

        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener B should bind");
        let app_b = oxidesfu_server::app_with_api_room_nodes_from_config(
            api_state_b.clone(),
            Some(room_nodes_arc),
            Some("node-b".to_string()),
            &config,
        );
        let server_b = tokio::spawn(async move {
            axum::serve(listener_b, app_b)
                .await
                .expect("node B server should run");
        });

        let room_name = format!("distributed-churn-room-{}", unique_suffix());
        let identity = format!("distributed-churn-identity-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Distributed Churn Identity")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut last_sid = String::new();
        for _ in 0..20 {
            let url = format!("ws://{addr_a}/rtc/v1?join_request={}", join_request_param());
            let mut request = url.into_client_request().expect("request should build");
            request.headers_mut().insert(
                "Authorization",
                HeaderValue::from_str(&format!("Bearer {token}"))
                    .expect("authorization header should parse"),
            );

            let (mut socket, _) = connect_async(request)
                .await
                .expect("websocket should connect to node A");
            let first = socket
                .next()
                .await
                .expect("first websocket message should arrive")
                .expect("first websocket message should be ok");
            let Message::Binary(bytes) = first else {
                panic!("expected binary protobuf signal response");
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("signal response should decode");
            let Some(proto::signal_response::Message::Join(join)) = response.message else {
                panic!("expected join response");
            };
            last_sid = join
                .participant
                .expect("join response should include participant")
                .sid;
        }
        assert!(
            !last_sid.is_empty(),
            "rejoin churn should produce at least one join sid"
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let remote = loop {
            match api_state_b.rooms.get_participant(&room_name, &identity) {
                Ok(value) => break value,
                Err(RoomStoreError::RoomNotFound) | Err(RoomStoreError::ParticipantNotFound)
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(err) => panic!("unexpected remote room store error: {err:?}"),
            }
        };
        assert_eq!(remote.identity, identity);

        let local = api_state_a.rooms.get_participant(&room_name, &identity);
        assert!(
            matches!(
                local,
                Err(RoomStoreError::RoomNotFound) | Err(RoomStoreError::ParticipantNotFound)
            ),
            "origin node should remain non-owner across churn"
        );

        server_a.abort();
        server_b.abort();
    }
    #[tokio::test]
    async fn sdk_reconnect_after_origin_node_loss_succeeds_via_reassigned_node() {
        let config = oxidesfu_core::ServerConfig::development();
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        probe_directory
            .register_node(RegisteredNode {
                id: "node-a".to_string(),
                region: "origin-region".to_string(),
            })
            .expect("node-a should register");
        probe_directory
            .register_node(RegisteredNode {
                id: "node-b".to_string(),
                region: "failover-region".to_string(),
            })
            .expect("node-b should register");
        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                oxidesfu_server::app_with_api_and_room_nodes(api_state, Some(room_nodes)),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("reconnect-reassign-room-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("reconnect-reassign-alice")
            .with_name("Reconnect Reassign Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let (first_client, first_join, _events) =
            SignalClient::connect(&format!("http://{addr}"), &token, options.clone(), None)
                .await
                .expect("first join should succeed");
        assert_eq!(
            first_join.room.expect("join room should exist").name,
            room_name
        );

        let initially_mapped = probe_directory
            .get_node_for_room(&room_name)
            .expect("room should map after first join");
        assert_eq!(initially_mapped.id, "node-a");

        probe_directory
            .unregister_node("node-a")
            .expect("origin node should unregister");

        first_client.close().await;

        let (_second_client, second_join, _events2) =
            SignalClient::connect(&format!("http://{addr}"), &token, options, None)
                .await
                .expect("reconnect via reassigned node should succeed");
        assert_eq!(
            second_join
                .room
                .expect("second join room should exist")
                .name,
            room_name
        );

        let reassigned = probe_directory
            .get_node_for_room(&room_name)
            .expect("room should map to surviving node");
        assert_eq!(reassigned.id, "node-b");

        server.abort();
    }

use super::*;

    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: distributed direct signaling/data test replacement is planned in docs map 12 but not yet implemented.
    // REMOVAL_PLAN: delete after direct distributed data routing parity tests land.
    #[tokio::test]
    async fn livekit_cli_join_publish_data_across_nodes_succeeds() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk cross-node join test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

        let config = oxidesfu_core::ServerConfig::development();
        let api_state = oxidesfu_server::api_state_from_config(&config);

        let probe_directory = Arc::new(PlacementProbeDirectory::new(false));
        probe_directory
            .register_node(RegisteredNode {
                id: "node-a".to_string(),
                region: "local".to_string(),
            })
            .expect("node-a should register");
        probe_directory
            .register_node(RegisteredNode {
                id: "node-b".to_string(),
                region: "remote".to_string(),
            })
            .expect("node-b should register");
        let room_nodes: Arc<dyn RoomNodeDirectory> = probe_directory;

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
                    Some("node-a".to_string()),
                    false,
                ),
            )
            .await
            .expect("test server should run");
        });

        let room_name = format!("cli-cross-node-room-{}", unique_suffix());
        let url = format!("http://{addr}");
        let output = run_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "join",
                "--identity",
                "cli-cross-node-sender",
                "--publish-data",
                "hello distributed",
                "--exit-after-publish",
                room_name.as_str(),
            ],
            None,
        )
        .await
        .expect("lk join command should execute");
        assert_success(
            output,
            "lk join publish-data should succeed in cross-node slice",
        );

        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: CLI command UX belongs to livekit-cli; OxideSFU still needs equivalent direct signaling data publish smoke parity before removal.
    // REMOVAL_PLAN: delete after direct protocol parity and external CLI conformance are stable.
    #[tokio::test]
    async fn livekit_cli_room_join_publish_data_exits_successfully() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk data publish command test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

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

        let room_name = format!("cli-publish-data-command-{}", unique_suffix());
        let url = format!("http://{addr}");
        let output = run_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "join",
                "--identity",
                "cli-data-command-sender",
                "--publish-data",
                "hello from lk",
                "--exit-after-publish",
                room_name.as_str(),
            ],
            None,
        )
        .await
        .expect("lk was available during version check");
        assert_success(
            output,
            "lk room join --publish-data --exit-after-publish should succeed against OxideSFU",
        );

        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: command-level publish-demo behavior is CLI-owned; server-owned direct parity replacement is pending.
    // REMOVAL_PLAN: delete after direct protocol parity and external CLI conformance are stable.
    #[tokio::test]
    async fn livekit_cli_room_join_publish_demo_starts_without_immediate_failure() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk publish-demo test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

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

        let room_name = format!("cli-publish-demo-{}", unique_suffix());
        let url = format!("http://{addr}");
        let mut cli = spawn_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "join",
                "--identity",
                "cli-publish-demo",
                "--publish-demo",
                room_name.as_str(),
            ],
            None,
        )
        .await
        .expect("lk was available during version check");

        tokio::time::sleep(Duration::from_secs(3)).await;

        if let Some(status) = cli
            .try_wait()
            .expect("lk process status should be readable")
        {
            assert!(
                status.success(),
                "lk publish-demo exited early with failure status: {status:?}"
            );
        }

        let _ = cli.kill().await;
        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: deprecated CLI alias behavior is CLI-owned; keep until migration to external CLI suite ownership is complete.
    // REMOVAL_PLAN: delete after direct protocol parity and external CLI conformance are stable.
    #[tokio::test]
    async fn livekit_cli_deprecated_join_room_publish_demo_starts_without_immediate_failure() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping deprecated lk join-room publish-demo test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

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

        let room_name = format!("cli-deprecated-join-room-publish-demo-{}", unique_suffix());
        let url = format!("http://{addr}");
        let mut cli = spawn_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "join-room",
                "--room",
                room_name.as_str(),
                "--identity",
                "cli-deprecated-join-room-publish-demo",
                "--publish-demo",
            ],
            None,
        )
        .await
        .expect("lk was available during version check");

        tokio::time::sleep(Duration::from_secs(3)).await;

        if let Some(status) = cli
            .try_wait()
            .expect("deprecated lk join-room process status should be readable")
        {
            assert!(
                status.success(),
                "deprecated lk join-room --publish-demo exited early with failure status: {status:?}"
            );
        }

        let _ = cli.kill().await;
        server.abort();
    }

    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: CLI-to-SDK mixed harness flow should be replaced by direct protocol/server tests plus external SDK/CLI conformance.
    // REMOVAL_PLAN: delete after direct publication signaling parity is complete.
    #[tokio::test]
    async fn livekit_cli_deprecated_join_room_publish_demo_emits_track_published_to_rust_sdk_room() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping deprecated lk join-room publish-demo test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

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

        let room_name = format!("cli-deprecated-join-room-publish-demo-contract-{}", unique_suffix());
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-cli-deprecated-demo-bob")
            .with_name("SDK CLI Deprecated Demo Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("bob token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (bob_room, mut bob_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut bob_events).await;

        let url = format!("http://{addr}");
        let mut cli = spawn_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "join-room",
                "--room",
                room_name.as_str(),
                "--identity",
                "cli-deprecated-join-room-publish-demo",
                "--publish-demo",
            ],
            None,
        )
        .await
        .expect("lk was available during version check");

        let (publication_name, participant_identity) = tokio::select! {
            event = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let event = bob_events
                        .recv()
                        .await
                        .expect("bob room events should stay open");
                    if let RoomEvent::TrackPublished { publication, participant } = event {
                        break (publication.name().to_string(), participant.identity().to_string());
                    }
                }
            }) => event.expect("bob should receive TrackPublished from deprecated join-room --publish-demo before timeout"),
            status = cli.wait() => {
                panic!("deprecated lk join-room --publish-demo exited before TrackPublished: {status:?}");
            }
        };

        assert_eq!(participant_identity, "cli-deprecated-join-room-publish-demo");
        assert_eq!(publication_name, "demo");

        let _ = cli.kill().await;
        let _ = bob_room.close().await;
        server.abort();
    }

    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: CLI-to-SDK mixed harness flow should be replaced by direct protocol/server tests plus external SDK/CLI conformance.
    // REMOVAL_PLAN: delete after direct publication signaling parity is complete.
    #[tokio::test]
    async fn livekit_cli_room_join_publish_demo_emits_track_published_to_rust_sdk_room() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk publish-demo test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

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

        let room_name = format!("cli-publish-demo-contract-{}", unique_suffix());
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-cli-demo-bob")
            .with_name("SDK CLI Demo Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("bob token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (bob_room, mut bob_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut bob_events).await;

        let url = format!("http://{addr}");
        let mut cli = spawn_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "join",
                "--identity",
                "cli-publish-demo",
                "--publish-demo",
                room_name.as_str(),
            ],
            None,
        )
        .await
        .expect("lk was available during version check");

        let (publication_name, participant_identity) = tokio::select! {
            event = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let event = bob_events
                        .recv()
                        .await
                        .expect("bob room events should stay open");
                    if let RoomEvent::TrackPublished { publication, participant } = event {
                        break (publication.name().to_string(), participant.identity().to_string());
                    }
                }
            }) => event.expect("bob should receive TrackPublished from publish-demo before timeout"),
            status = cli.wait() => {
                panic!("lk room join --publish-demo exited before TrackPublished: {status:?}");
            }
        };

        assert_eq!(participant_identity, "cli-publish-demo");
        assert_eq!(publication_name, "demo");

        let _ = cli.kill().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: DEPRECATION_PLANNED_PENDING_COVERAGE
    // COVERAGE_GAP: CLI-to-SDK mixed data-path harness should be replaced by direct protocol distributed/data tests.
    // REMOVAL_PLAN: delete after direct distributed data parity tests land.
    #[tokio::test]
    async fn livekit_cli_room_join_publish_data_reaches_rust_sdk_room() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk data publish test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

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

        let room_name = format!("cli-publish-data-{}", unique_suffix());
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-cli-data-bob")
            .with_name("SDK CLI Data Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("bob token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (bob_room, mut bob_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut bob_events).await;

        let url = format!("http://{addr}");
        let mut cli = spawn_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "join",
                "--identity",
                "cli-data-sender",
                "--publish-data",
                "hello from lk",
                room_name.as_str(),
            ],
            None,
        )
        .await
        .expect("lk was available during version check");

        let (payload, topic, kind) = tokio::select! {
            data = next_data_received(&mut bob_events) => data,
            status = cli.wait() => {
                panic!("lk room join --publish-data exited before Bob received data: {status:?}");
            }
        };
        assert_eq!(payload.as_slice(), b"hello from lk");
        assert_eq!(topic, None);
        assert_eq!(kind, DataPacketKind::Reliable);

        let _ = cli.kill().await;
        let _ = bob_room.close().await;
        server.abort();
    }

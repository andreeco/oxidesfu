use super::*;

    #[tokio::test]
    async fn livekit_cli_room_create_list_delete_smoke() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk smoke test because lk is not on PATH");
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

        let room = format!("cli-smoke-{}", unique_suffix());
        let url = format!("http://{addr}");
        let common = [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
        ];

        let create = run_lk(
            common.into_iter().chain(["room", "create", room.as_str()]),
            None,
        )
        .await
        .expect("lk was available during version check");
        assert_success(create, "lk room create should succeed against OxideSFU");

        let list = run_lk(
            common
                .into_iter()
                .chain(["room", "list", "--json", room.as_str()]),
            None,
        )
        .await
        .expect("lk was available during version check");
        assert_success_with_stdout_contains(
            list,
            "lk room list should include the created room",
            room.as_str(),
        );

        let delete = run_lk(
            common.into_iter().chain(["room", "delete", room.as_str()]),
            None,
        )
        .await
        .expect("lk was available during version check");
        assert_success(delete, "lk room delete should succeed against OxideSFU");

        server.abort();
    }
    #[tokio::test]
    async fn livekit_cli_deprecated_create_room_and_list_rooms_commands_work() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk deprecated room command test because lk is not on PATH");
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

        let room_name = format!("cli-deprecated-room-{}", unique_suffix());
        let url = format!("http://{addr}");
        let common = [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
        ];

        let create = run_lk(
            common
                .into_iter()
                .chain(["create-room", "--name", room_name.as_str()]),
            None,
        )
        .await
        .expect("deprecated lk create-room command should run");
        assert_success(create, "deprecated lk create-room should succeed against OxideSFU");

        let list = run_lk(common.into_iter().chain(["list-rooms"]), None)
            .await
            .expect("deprecated lk list-rooms command should run");
        assert_success_with_stdout_contains(
            list,
            "deprecated lk list-rooms should include created room",
            room_name.as_str(),
        );

        let delete = run_lk(
            common
                .into_iter()
                .chain(["room", "delete", room_name.as_str()]),
            None,
        )
        .await
        .expect("lk room delete command should run");
        assert_success(delete, "lk room delete should clean up deprecated room command test");

        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_deprecated_room_aliases_for_metadata_and_participants_work() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk deprecated alias test because lk is not on PATH");
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

        let room_name = format!("cli-deprecated-aliases-{}", unique_suffix());
        let identity = "cli-deprecated-aliases-alice";
        let url = format!("http://{addr}");

        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        room_client
            .create_room(&room_name, CreateRoomOptions::default())
            .await
            .expect("room should create");

        let join_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name("CLI Deprecated Aliases Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("join token should encode");
        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (room, mut events) = Room::connect(&url, &join_token, options)
            .await
            .expect("participant should connect");
        wait_for_room_connected(&mut events).await;

        let common = [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
        ];

        let list_room = run_lk(
            common
                .into_iter()
                .chain(["list-room", "--room", room_name.as_str()]),
            None,
        )
        .await
        .expect("deprecated list-room should run");
        assert_success_with_stdout_contains(
            list_room,
            "deprecated list-room should include room name",
            room_name.as_str(),
        );

        let metadata = r#"{"deprecated":"metadata"}"#;
        let update_room_metadata = run_lk(
            common.into_iter().chain([
                "update-room-metadata",
                "--room",
                room_name.as_str(),
                "--metadata",
                metadata,
            ]),
            None,
        )
        .await
        .expect("deprecated update-room-metadata should run");
        assert_success(
            update_room_metadata,
            "deprecated update-room-metadata should succeed",
        );

        let listed_rooms = room_client
            .list_rooms(vec![room_name.clone()])
            .await
            .expect("room list should succeed");
        let updated_room = listed_rooms
            .into_iter()
            .find(|room| room.name == room_name)
            .expect("updated room should exist");
        assert_eq!(updated_room.metadata, metadata);

        let list_participants = run_lk(
            common.into_iter().chain([
                "list-participants",
                "--room",
                room_name.as_str(),
            ]),
            None,
        )
        .await
        .expect("deprecated list-participants should run");
        assert_success_with_stdout_contains(
            list_participants,
            "deprecated list-participants should include identity",
            identity,
        );

        let get_participant = run_lk(
            common.into_iter().chain([
                "get-participant",
                "--room",
                room_name.as_str(),
                "--identity",
                identity,
            ]),
            None,
        )
        .await
        .expect("deprecated get-participant should run");
        assert_success_with_stdout_contains(
            get_participant,
            "deprecated get-participant should include identity",
            identity,
        );

        let participant_metadata = r#"{"deprecated":"participant"}"#;
        let update_participant = run_lk(
            common.into_iter().chain([
                "update-participant",
                "--room",
                room_name.as_str(),
                "--identity",
                identity,
                "--metadata",
                participant_metadata,
            ]),
            None,
        )
        .await
        .expect("deprecated update-participant should run");
        assert_success(
            update_participant,
            "deprecated update-participant should succeed",
        );

        let participant = room_client
            .get_participant(&room_name, identity)
            .await
            .expect("participant should be retrievable");
        assert_eq!(participant.metadata, participant_metadata);

        let remove_participant = run_lk(
            common.into_iter().chain([
                "remove-participant",
                "--room",
                room_name.as_str(),
                "--identity",
                identity,
            ]),
            None,
        )
        .await
        .expect("deprecated remove-participant should run");
        assert_success(
            remove_participant,
            "deprecated remove-participant should succeed",
        );

        let participants_after_remove = room_client
            .list_participants(&room_name)
            .await
            .expect("participants list should succeed");
        assert!(
            participants_after_remove
                .iter()
                .all(|participant| participant.identity != identity),
            "participant should be removed by deprecated remove-participant"
        );

        let _ = room.close().await;
        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_deprecated_top_level_room_control_aliases_work() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk deprecated top-level alias test because lk is not on PATH");
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

        let room_name = format!("cli-deprecated-top-level-{}", unique_suffix());
        let publisher_identity = "cli-deprecated-top-level-publisher";
        let subscriber_identity = "cli-deprecated-top-level-subscriber";
        let url = format!("http://{addr}");

        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name("CLI Deprecated Top-Level Publisher")
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
            .with_name("CLI Deprecated Top-Level Subscriber")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: false,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (publisher_room, mut publisher_events) =
            Room::connect(&url, &publisher_token, options.clone())
                .await
                .expect("publisher should connect");
        let (_subscriber_room, mut subscriber_events) =
            Room::connect(&url, &subscriber_token, options)
                .await
                .expect("subscriber should connect");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track = LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source));
        let publication = publisher_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("publisher should publish audio track");
        let track_sid = publication.sid().to_string();

        let common = [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
        ];

        let mute = run_lk(
            common.into_iter().chain([
                "mute-track",
                "--room",
                room_name.as_str(),
                "--identity",
                publisher_identity,
                "--m",
                track_sid.as_str(),
            ]),
            None,
        )
        .await
        .expect("deprecated top-level mute-track should run");
        assert_success(mute, "deprecated top-level mute-track should succeed");

        let participant = room_client
            .get_participant(&room_name, publisher_identity)
            .await
            .expect("publisher participant should be retrievable");
        assert!(
            participant
                .tracks
                .iter()
                .any(|track| track.sid == track_sid && track.muted),
            "track should be muted after top-level mute-track alias"
        );

        let unsubscribe = run_lk(
            common.into_iter().chain([
                "update-subscriptions",
                "--room",
                room_name.as_str(),
                "--identity",
                subscriber_identity,
                "--subscribe=false",
                track_sid.as_str(),
            ]),
            None,
        )
        .await
        .expect("deprecated top-level update-subscriptions should run");
        assert_success(
            unsubscribe,
            "deprecated top-level update-subscriptions should succeed",
        );

        let send_data = run_lk(
            common.into_iter().chain([
                "send-data",
                "--room",
                room_name.as_str(),
                "--topic",
                "deprecated-top-level",
                "{\"payload\":\"top-level\"}",
            ]),
            None,
        )
        .await
        .expect("deprecated top-level send-data should run");
        assert_success(send_data, "deprecated top-level send-data should succeed");

        let (payload, topic, kind) = next_data_received(&mut subscriber_events).await;
        assert_eq!(payload, b"{\"payload\":\"top-level\"}");
        assert_eq!(topic, Some("deprecated-top-level".to_string()));
        assert_eq!(kind, DataPacketKind::Reliable);

        let _ = publisher_room.close().await;
        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_token_create_token_only_emits_jwt() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk token create test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

        let output = run_lk(
            [
                "--url",
                "http://127.0.0.1:7880",
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "token",
                "create",
                "--room",
                "token-room",
                "--identity",
                "token-alice",
                "--join",
                "--token-only",
            ],
            None,
        )
        .await
        .expect("lk token create command should run");
        let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
        assert_success(output, "lk token create should succeed");

        let segments = token.split('.').count();
        assert_eq!(segments, 3, "token-only output should be a JWT");
    }

    #[tokio::test]
    async fn livekit_cli_token_create_json_output_contains_expected_fields() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk token create json test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

        let output = run_lk(
            [
                "--url",
                "http://127.0.0.1:7880",
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "token",
                "create",
                "--room",
                "token-room-json",
                "--identity",
                "token-json-alice",
                "--join",
                "--json",
            ],
            None,
        )
        .await
        .expect("lk token create --json command should run");
        let parsed: JsonValue =
            serde_json::from_slice(&output.stdout).expect("token create --json output must be JSON");
        assert_success(output, "lk token create --json should succeed");

        assert_eq!(parsed["identity"], "token-json-alice");
        assert_eq!(parsed["room"], "token-room-json");
        let token = parsed["access_token"]
            .as_str()
            .expect("json output should contain access_token string");
        assert_eq!(token.split('.').count(), 3, "access_token should be a JWT");
    }

    #[tokio::test]
    async fn livekit_cli_token_create_without_permissions_fails_in_non_interactive_mode() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk token permission failure test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

        let output = run_lk(
            [
                "--url",
                "http://127.0.0.1:7880",
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "token",
                "create",
                "--room",
                "token-room-no-perms",
                "--identity",
                "token-no-perms-alice",
                "--token-only",
            ],
            None,
        )
        .await
        .expect("lk token create command should run");

        assert!(
            !output.status.success(),
            "token create without permissions should fail in non-interactive mode\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("non-interactive mode"),
            "expected non-interactive permission error, got stderr:\n{stderr}"
        );
    }

    #[tokio::test]
    async fn livekit_cli_deprecated_create_token_alias_json_output_contains_expected_fields() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk create-token json test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

        let output = run_lk(
            [
                "--url",
                "http://127.0.0.1:7880",
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "create-token",
                "--room",
                "token-room-json",
                "--identity",
                "token-json-alice",
                "--join",
                "--json",
            ],
            None,
        )
        .await
        .expect("lk deprecated create-token --json command should run");
        let parsed: JsonValue = serde_json::from_slice(&output.stdout)
            .expect("deprecated create-token --json output must be JSON");
        assert_success(output, "lk deprecated create-token --json should succeed");

        assert_eq!(parsed["identity"], "token-json-alice");
        assert_eq!(parsed["room"], "token-room-json");
        let token = parsed["access_token"]
            .as_str()
            .expect("json output should contain access_token string");
        assert_eq!(token.split('.').count(), 3, "access_token should be a JWT");
    }

    #[tokio::test]
    async fn livekit_cli_deprecated_create_token_alias_emits_jwt() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk create-token test because lk is not on PATH");
            return;
        };
        assert_success(version, "lk --version should run");

        let output = run_lk(
            [
                "--url",
                "http://127.0.0.1:7880",
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "create-token",
                "--room",
                "token-room",
                "--identity",
                "token-alice",
                "--join",
                "--token-only",
            ],
            None,
        )
        .await
        .expect("lk deprecated create-token command should run");
        let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
        assert_success(output, "lk deprecated create-token should succeed");

        let segments = token.split('.').count();
        assert_eq!(segments, 3, "token-only output should be a JWT");
    }

    #[tokio::test]
    async fn livekit_cli_room_join_rejoin_same_identity_exits_successfully_twice() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk reconnect flow test because lk is not on PATH");
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

        let room_name = format!("cli-rejoin-same-identity-{}", unique_suffix());
        let url = format!("http://{addr}");
        let common = [
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
            "cli-rejoin-alice",
            "--publish-data",
        ];

        let first = run_lk(
            common.into_iter().chain([
                "first join payload",
                "--exit-after-publish",
                room_name.as_str(),
            ]),
            None,
        )
        .await
        .expect("lk was available during version check");
        assert_success(first, "first lk room join should succeed");

        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let listed = room_client
                .list_participants(&room_name)
                .await
                .expect("room client should list participants");
            if listed.is_empty() || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let second = run_lk(
            common.into_iter().chain([
                "second join payload",
                "--exit-after-publish",
                room_name.as_str(),
            ]),
            None,
        )
        .await
        .expect("lk was available during version check");
        assert_success(second, "second lk room join should succeed after rejoin");

        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_room_participants_list_get_update_remove_roundtrip() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk participants command test because lk is not on PATH");
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

        let room_name = format!("cli-participants-{}", unique_suffix());
        let identity = "cli-participants-alice";
        let url = format!("http://{addr}");

        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        room_client
            .create_room(&room_name, CreateRoomOptions::default())
            .await
            .expect("room should be created");

        let join_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name("CLI Participants Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("join token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (alice_room, mut alice_events) = Room::connect(&url, &join_token, options)
            .await
            .expect("alice should connect");
        wait_for_room_connected(&mut alice_events).await;

        let common = [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
        ];

        let list = run_lk(
            common.into_iter().chain([
                "room",
                "participants",
                "list",
                room_name.as_str(),
            ]),
            None,
        )
        .await
        .expect("lk participants list command should run");
        assert_success_with_stdout_contains(
            list,
            "lk room participants list should include connected identity",
            identity,
        );

        let get = run_lk(
            common.into_iter().chain([
                "room",
                "participants",
                "get",
                "--room",
                room_name.as_str(),
                identity,
            ]),
            None,
        )
        .await
        .expect("lk participants get command should run");
        assert_success_with_stdout_contains(
            get,
            "lk room participants get should return connected identity",
            identity,
        );

        let updated_metadata = r#"{"role":"speaker"}"#;
        let update = run_lk(
            common.into_iter().chain([
                "room",
                "participants",
                "update",
                "--room",
                room_name.as_str(),
                "--metadata",
                updated_metadata,
                identity,
            ]),
            None,
        )
        .await
        .expect("lk participants update command should run");
        assert_success(
            update,
            "lk room participants update should succeed against OxideSFU",
        );

        let participant = room_client
            .get_participant(&room_name, identity)
            .await
            .expect("updated participant should be retrievable");
        assert_eq!(participant.metadata, updated_metadata);

        let remove = run_lk(
            common.into_iter().chain([
                "room",
                "participants",
                "remove",
                "--room",
                room_name.as_str(),
                identity,
            ]),
            None,
        )
        .await
        .expect("lk participants remove command should run");
        assert_success(
            remove,
            "lk room participants remove should succeed against OxideSFU",
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let listed = room_client
                .list_participants(&room_name)
                .await
                .expect("room client should list participants");
            if listed.is_empty() || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let _ = alice_room.close().await;
        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_room_update_metadata_roundtrip() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk room update test because lk is not on PATH");
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

        let room_name = format!("cli-room-update-{}", unique_suffix());
        let metadata = r#"{"stage":"prod","owner":"cli"}"#;
        let url = format!("http://{addr}");

        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        room_client
            .create_room(&room_name, CreateRoomOptions::default())
            .await
            .expect("room should be created");

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
                "update",
                "--metadata",
                metadata,
                room_name.as_str(),
            ],
            None,
        )
        .await
        .expect("lk room update command should run");
        assert_success(output, "lk room update should succeed against OxideSFU");

        let rooms = room_client
            .list_rooms(vec![room_name.clone()])
            .await
            .expect("room list should succeed");
        let room = rooms
            .into_iter()
            .find(|room| room.name == room_name)
            .expect("updated room should be listed");
        assert_eq!(room.metadata, metadata);

        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_room_mute_track_roundtrip_updates_track_state() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk room mute-track test because lk is not on PATH");
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

        let room_name = format!("cli-mute-track-{}", unique_suffix());
        let publisher_identity = "cli-mute-track-publisher";
        let url = format!("http://{addr}");

        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name("CLI MuteTrack Publisher")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (publisher_room, mut publisher_events) = Room::connect(&url, &publisher_token, options)
            .await
            .expect("publisher should connect");
        wait_for_room_connected(&mut publisher_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track = LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source));
        let publication = publisher_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("publisher should publish audio track");
        let track_sid = publication.sid().to_string();

        let mute = run_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "mute-track",
                "--room",
                room_name.as_str(),
                "--identity",
                publisher_identity,
                "--mute",
                track_sid.as_str(),
            ],
            None,
        )
        .await
        .expect("lk room mute-track command should run");
        assert_success(mute, "lk room mute-track --mute should succeed against OxideSFU");

        let participant = room_client
            .get_participant(&room_name, publisher_identity)
            .await
            .expect("publisher participant should be retrievable");
        assert!(
            participant
                .tracks
                .iter()
                .any(|track| track.sid == track_sid && track.muted),
            "published track should be marked muted after lk room mute-track --mute"
        );

        let unmute = run_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "mute-track",
                "--room",
                room_name.as_str(),
                "--identity",
                publisher_identity,
                "--unmute",
                track_sid.as_str(),
            ],
            None,
        )
        .await
        .expect("lk room mute-track --unmute command should run");
        assert_success(
            unmute,
            "lk room mute-track --unmute should succeed against OxideSFU",
        );

        let participant = room_client
            .get_participant(&room_name, publisher_identity)
            .await
            .expect("publisher participant should be retrievable after unmute");
        assert!(
            participant
                .tracks
                .iter()
                .any(|track| track.sid == track_sid && !track.muted),
            "published track should be unmuted after lk room mute-track --unmute"
        );

        let _ = publisher_room.close().await;
        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_room_update_subscriptions_command_succeeds_with_existing_track() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk room update-subscriptions test because lk is not on PATH");
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

        let room_name = format!("cli-update-subs-{}", unique_suffix());
        let publisher_identity = "cli-update-subs-publisher";
        let subscriber_identity = "cli-update-subs-subscriber";
        let url = format!("http://{addr}");

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name("CLI UpdateSubscriptions Publisher")
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
            .with_name("CLI UpdateSubscriptions Subscriber")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish: false,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (publisher_room, mut publisher_events) = Room::connect(&url, &publisher_token, options.clone())
            .await
            .expect("publisher should connect");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(&url, &subscriber_token, options)
                .await
                .expect("subscriber should connect");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track = LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source));
        let publication = publisher_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("publisher should publish audio track");
        let track_sid = publication.sid().to_string();

        let unsubscribe = run_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "update-subscriptions",
                "--room",
                room_name.as_str(),
                "--identity",
                subscriber_identity,
                "--unsubscribe",
                track_sid.as_str(),
            ],
            None,
        )
        .await
        .expect("lk room update-subscriptions --unsubscribe should run");
        assert_success(
            unsubscribe,
            "lk room update-subscriptions --unsubscribe should succeed against OxideSFU",
        );

        let subscribe = run_lk(
            [
                "--url",
                url.as_str(),
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--yes",
                "room",
                "update-subscriptions",
                "--room",
                room_name.as_str(),
                "--identity",
                subscriber_identity,
                "--subscribe",
                track_sid.as_str(),
            ],
            None,
        )
        .await
        .expect("lk room update-subscriptions --subscribe should run");
        assert_success(
            subscribe,
            "lk room update-subscriptions --subscribe should succeed against OxideSFU",
        );

        let _ = subscriber_room.close().await;
        let _ = publisher_room.close().await;
        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_room_participants_forward_command_succeeds_for_existing_participant() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk participants forward test because lk is not on PATH");
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

        let source_room = format!("cli-forward-src-{}", unique_suffix());
        let destination_room = format!("cli-forward-dst-{}", unique_suffix());
        let identity = "cli-forward-alice";
        let url = format!("http://{addr}");

        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        room_client
            .create_room(&source_room, CreateRoomOptions::default())
            .await
            .expect("source room should create");
        room_client
            .create_room(&destination_room, CreateRoomOptions::default())
            .await
            .expect("destination room should create");

        let join_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name("CLI Forward Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: source_room.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("join token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (room, mut events) = Room::connect(&url, &join_token, options)
            .await
            .expect("participant should connect");
        wait_for_room_connected(&mut events).await;

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
                "participants",
                "forward",
                "--room",
                source_room.as_str(),
                "--identity",
                identity,
                "--destination-room",
                destination_room.as_str(),
            ],
            None,
        )
        .await
        .expect("lk room participants forward command should run");
        assert_success(
            output,
            "lk room participants forward should succeed against OxideSFU",
        );

        let _ = room.close().await;
        server.abort();
    }

    #[tokio::test]
    async fn livekit_cli_room_participants_move_transfers_participant_between_rooms() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk participants move test because lk is not on PATH");
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

        let source_room = format!("cli-move-src-{}", unique_suffix());
        let destination_room = format!("cli-move-dst-{}", unique_suffix());
        let identity = "cli-move-alice";
        let url = format!("http://{addr}");

        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        room_client
            .create_room(&source_room, CreateRoomOptions::default())
            .await
            .expect("source room should create");
        room_client
            .create_room(&destination_room, CreateRoomOptions::default())
            .await
            .expect("destination room should create");

        let join_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name("CLI Move Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: source_room.clone(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("join token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (room, mut events) = Room::connect(&url, &join_token, options)
            .await
            .expect("participant should connect");
        wait_for_room_connected(&mut events).await;

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
                "participants",
                "move",
                "--room",
                source_room.as_str(),
                "--identity",
                identity,
                "--destination-room",
                destination_room.as_str(),
            ],
            None,
        )
        .await
        .expect("lk room participants move command should run");
        assert_success(
            output,
            "lk room participants move should succeed against OxideSFU",
        );

        let source_participants = room_client
            .list_participants(&source_room)
            .await
            .expect("source room participants should list");
        assert!(source_participants.iter().all(|participant| participant.identity != identity));

        let destination_participants = room_client
            .list_participants(&destination_room)
            .await
            .expect("destination room participants should list");
        assert!(destination_participants.iter().any(|participant| participant.identity == identity));

        let _ = room.close().await;
        server.abort();
    }



    #[tokio::test]
    async fn livekit_cli_room_send_data_reaches_sdk_participant() {
        let Some(version) = run_lk(["--version"], None).await else {
            eprintln!("skipping lk room send-data test because lk is not on PATH");
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

        let room_name = format!("cli-send-data-{}", unique_suffix());
        let receiver_identity = "cli-send-data-receiver";
        let payload = r#"{"message":"hello from lk room send-data"}"#;
        let url = format!("http://{addr}");

        let receiver_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(receiver_identity)
            .with_name("CLI SendData Receiver")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("receiver token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (receiver_room, mut receiver_events) = Room::connect(&url, &receiver_token, options)
            .await
            .expect("receiver room should connect");
        wait_for_room_connected(&mut receiver_events).await;
        let room_client = RoomClient::with_api_key(&url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        wait_for_participant_on_room_client(&room_client, &room_name, receiver_identity)
            .await
            .expect("receiver should be visible before lk room send-data runs");

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
                "send-data",
                "--room",
                room_name.as_str(),
                "--identity",
                receiver_identity,
                "--topic",
                "cli-topic",
                payload,
            ],
            None,
        )
        .await
        .expect("lk room send-data command should run");
        assert_success(output, "lk room send-data should succeed against OxideSFU");

        let (received_payload, topic, kind) = next_data_received(&mut receiver_events).await;
        assert_eq!(received_payload, payload.as_bytes());
        assert_eq!(topic, Some("cli-topic".to_string()));
        assert_eq!(kind, DataPacketKind::Reliable);

        let _ = receiver_room.close().await;
        server.abort();
    }

use super::*;

    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::{rtc_v1_publish_data_track_rejects_invalid_handle, rtc_v1_publish_data_track_rejects_duplicate_handle, rtc_v1_publish_data_track_rejects_duplicate_name}
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_publish_data_track_succeeds() {
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

        let room_name = format!("sdk-data-track-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-alice")
            .with_name("SDK Data Track Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (room, mut events) = Room::connect(&format!("http://{addr}"), &token, options)
            .await
            .expect("room should connect");
        wait_for_room_connected(&mut events).await;

        let track = room
            .local_participant()
            .publish_data_track("sensor-readings")
            .await
            .expect("publish_data_track should succeed");

        assert!(track.is_published());
        assert_eq!(track.info().name(), "sensor-readings");
        assert!(track.info().sid().to_string().starts_with("DTR_"));

        let _ = room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::rtc_v1_publish_data_track_rejects_empty_name
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_publish_empty_data_track_name_is_rejected() {
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

        let room_name = format!("sdk-data-track-invalid-name-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-invalid-name-alice")
            .with_name("SDK Data Track Invalid Name Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (room, mut events) = Room::connect(&format!("http://{addr}"), &token, options)
            .await
            .expect("room should connect");
        wait_for_room_connected(&mut events).await;

        let publish = room.local_participant().publish_data_track("").await;
        assert!(matches!(publish, Err(PublishError::InvalidName)));

        let _ = room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::rtc_v1_publish_data_track_requires_can_publish_data_permission
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_publish_data_track_requires_can_publish_data() {
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

        let room_name = format!("sdk-data-track-not-allowed-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-not-allowed-alice")
            .with_name("SDK Data Track Not Allowed Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: false,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (room, mut events) = Room::connect(&format!("http://{addr}"), &token, options)
            .await
            .expect("room should connect");
        wait_for_room_connected(&mut events).await;

        let publish = room
            .local_participant()
            .publish_data_track("sensor-readings")
            .await;
        assert!(matches!(publish, Err(PublishError::NotAllowed)));

        let _ = room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::rtc_v1_publish_data_track_rejects_duplicate_name
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_publish_duplicate_data_track_name_is_rejected() {
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

        let room_name = format!("sdk-data-track-duplicate-{}", unique_suffix());
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-duplicate-alice")
            .with_name("SDK Data Track Duplicate Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);
        let (room, mut events) = Room::connect(&format!("http://{addr}"), &token, options)
            .await
            .expect("room should connect");
        wait_for_room_connected(&mut events).await;

        let first = room
            .local_participant()
            .publish_data_track("duplicate-track")
            .await
            .expect("first publish_data_track should succeed");
        assert!(first.is_published());

        let second = room
            .local_participant()
            .publish_data_track("duplicate-track")
            .await;
        assert!(matches!(second, Err(PublishError::DuplicateName)));

        let _ = room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::{rtc_v1_update_data_subscription_with_can_subscribe_returns_handles, rtc_v1_user_data_packet_reaches_oxidesfu}
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_publish_data_track_reaches_other_room() {
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

        let room_name = format!("sdk-remote-data-track-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-remote-data-track-alice")
            .with_name("SDK Remote Data Track Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-remote-data-track-bob")
            .with_name("SDK Remote Data Track Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
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

        let local_track = alice_room
            .local_participant()
            .publish_data_track("shared-sensor")
            .await
            .expect("alice publish_data_track should succeed");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");

        assert!(local_track.is_published());
        assert!(remote_track.is_published());
        assert_eq!(remote_track.info().name(), "shared-sensor");
        assert_eq!(
            remote_track.publisher_identity(),
            "sdk-remote-data-track-alice"
        );

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/session.rs::tests::relay_data_track_packet_delivers_small_and_large_frames_to_subscriber
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_frame_reaches_subscriber() {
        assert_rust_sdk_data_track_frame_reaches_subscriber(
            "sdk-data-track-frame",
            "telemetry",
            vec![0xFA; 256],
        )
        .await;
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/session.rs::tests::relay_data_track_packet_delivers_small_and_large_frames_to_subscriber
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_large_data_track_frame_reaches_subscriber() {
        assert_rust_sdk_data_track_frame_reaches_subscriber(
            "sdk-large-data-track-frame",
            "bulk-telemetry",
            vec![0xBC; 32_000],
        )
        .await;
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/session.rs::tests::relay_data_track_packet_with_subscription_options_delivers_frame_and_preserves_options
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_subscribe_with_options_receives_frame() {
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

        let room_name = format!("sdk-data-track-subscribe-options-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-options-alice")
            .with_name("SDK Data Track Options Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-options-bob")
            .with_name("SDK Data Track Options Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
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

        let local_track = alice_room
            .local_participant()
            .publish_data_track("optioned-track")
            .await
            .expect("alice publish_data_track should succeed");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");

        let mut stream = remote_track
            .subscribe_with_options(DataTrackSubscribeOptions::default().with_buffer_size(1))
            .await
            .expect("bob should subscribe with options");

        let payload = vec![0x55; 64];
        local_track
            .try_push(DataTrackFrame::new(payload.clone()))
            .expect("alice should push frame");

        let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("bob should receive data-track frame before timeout")
            .expect("data-track stream should stay open");
        assert_eq!(frame.payload().as_ref(), payload.as_slice());

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::{update_data_subscription_without_can_subscribe_does_not_create_mapping, rtc_v1_update_data_subscription_without_can_subscribe_returns_empty_handles}
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_subscribe_with_options_requires_can_subscribe() {
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

        let room_name = format!("sdk-data-track-no-subscribe-options-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-no-subscribe-options-alice")
            .with_name("SDK Data Track No Subscribe Options Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-no-subscribe-options-bob")
            .with_name("SDK Data Track No Subscribe Options Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
                can_subscribe: false,
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

        let _local_track = alice_room
            .local_participant()
            .publish_data_track("restricted-options-track")
            .await
            .expect("alice publish_data_track should succeed");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");

        let subscribe_result = tokio::time::timeout(
            Duration::from_secs(12),
            remote_track
                .subscribe_with_options(DataTrackSubscribeOptions::default().with_buffer_size(1)),
        )
        .await
        .expect("subscribe_with_options should complete before outer timeout");
        assert!(matches!(
            subscribe_result,
            Err(DataTrackSubscribeError::Timeout)
        ));

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::{update_data_subscription_without_can_subscribe_does_not_create_mapping, rtc_v1_update_data_subscription_without_can_subscribe_returns_empty_handles}
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_subscribe_requires_can_subscribe() {
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

        let room_name = format!("sdk-data-track-no-subscribe-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-no-subscribe-alice")
            .with_name("SDK Data Track No Subscribe Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-no-subscribe-bob")
            .with_name("SDK Data Track No Subscribe Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
                can_subscribe: false,
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

        let _local_track = alice_room
            .local_participant()
            .publish_data_track("restricted-track")
            .await
            .expect("alice publish_data_track should succeed");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");

        let subscribe_result =
            tokio::time::timeout(Duration::from_secs(12), remote_track.subscribe())
                .await
                .expect("subscribe should complete before outer timeout");
        assert!(matches!(
            subscribe_result,
            Err(DataTrackSubscribeError::Timeout)
        ));

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::{rtc_v1_disconnect_and_rejoin_can_resubscribe_data_track, rtc_v1_leave_and_rejoin_can_resubscribe_data_track}
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_reconnect_can_resubscribe_and_receive_frame() {
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

        let room_name = format!("sdk-data-track-reconnect-{}", unique_suffix());
        let alice_identity = "sdk-data-track-reconnect-alice";
        let bob_identity = "sdk-data-track-reconnect-bob";

        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(alice_identity)
            .with_name("SDK Data Track Reconnect Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(bob_identity)
            .with_name("SDK Data Track Reconnect Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
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
            Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;

        let local_track = alice_room
            .local_participant()
            .publish_data_track("reconnect-track")
            .await
            .expect("alice publish_data_track should succeed");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive initial data-track published event before timeout");

        let mut first_stream = remote_track
            .subscribe()
            .await
            .expect("bob should subscribe before reconnect");
        local_track
            .try_push(DataTrackFrame::new(vec![0x44; 64]))
            .expect("alice should push first frame before reconnect");
        let _ = tokio::time::timeout(Duration::from_secs(10), first_stream.next())
            .await
            .expect("bob should receive first frame before reconnect");

        let _ = bob_room.close().await;

        let (bob_room_reconnect, mut bob_reconnect_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob should reconnect");
        wait_for_room_connected(&mut bob_reconnect_events).await;

        let remote_track_after_reconnect = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_reconnect_events
                    .recv()
                    .await
                    .expect("bob reconnect room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event after reconnect before timeout");

        let mut stream_after_reconnect = remote_track_after_reconnect
            .subscribe()
            .await
            .expect("bob should resubscribe after reconnect");
        let payload = vec![0x66; 96];
        local_track
            .try_push(DataTrackFrame::new(payload.clone()))
            .expect("alice should push frame after bob reconnect");
        let frame = tokio::time::timeout(Duration::from_secs(10), stream_after_reconnect.next())
            .await
            .expect("bob should receive frame after reconnect before timeout")
            .expect("data-track stream should stay open after reconnect");
        assert_eq!(frame.payload().as_ref(), payload.as_slice());

        let _ = alice_room.close().await;
        let _ = bob_room_reconnect.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::{rtc_v1_disconnect_and_rejoin_can_resubscribe_data_track, rtc_v1_leave_and_rejoin_can_resubscribe_data_track}
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_reconnect_loop_keeps_resubscribe_working() {
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

        let room_name = format!("sdk-data-track-reconnect-loop-{}", unique_suffix());
        let alice_identity = "sdk-data-track-reconnect-loop-alice";
        let bob_identity = "sdk-data-track-reconnect-loop-bob";

        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(alice_identity)
            .with_name("SDK Data Track Reconnect Loop Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(bob_identity)
            .with_name("SDK Data Track Reconnect Loop Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
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
        wait_for_room_connected(&mut alice_events).await;

        let local_track = alice_room
            .local_participant()
            .publish_data_track("reconnect-loop-track")
            .await
            .expect("alice publish_data_track should succeed");

        for round in 0..3u8 {
            let (bob_room, mut bob_events) =
                Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                    .await
                    .expect("bob room should connect");
            wait_for_room_connected(&mut bob_events).await;

            let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let event = bob_events
                        .recv()
                        .await
                        .expect("bob room events should stay open");
                    if let RoomEvent::DataTrackPublished(track) = event {
                        break track;
                    }
                }
            })
            .await
            .expect("bob should receive data-track published event before timeout");

            let mut stream = remote_track
                .subscribe()
                .await
                .expect("bob should subscribe after reconnect");
            let payload = vec![round + 1; 80];
            local_track
                .try_push(DataTrackFrame::new(payload.clone()))
                .expect("alice should push round frame");
            let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("bob should receive round frame before timeout")
                .expect("stream should stay open for round frame");
            assert_eq!(frame.payload().as_ref(), payload.as_slice());

            let _ = bob_room.close().await;
        }

        let _ = alice_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/session.rs::tests::relay_data_track_packet_staggered_multi_subscriber_reconnect_routes_to_active_subscribers_only
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_staggered_reconnect_multi_subscriber_receives_frames() {
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

        let room_name = format!("sdk-data-track-staggered-reconnect-{}", unique_suffix());

        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-staggered-alice")
            .with_name("SDK Data Track Staggered Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-staggered-bob")
            .with_name("SDK Data Track Staggered Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("bob token should encode");
        let charlie_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-staggered-charlie")
            .with_name("SDK Data Track Staggered Charlie")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("charlie token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);

        let (alice_room, mut alice_events) =
            Room::connect(&format!("http://{addr}"), &alice_token, options.clone())
                .await
                .expect("alice room should connect");
        let (mut bob_room, mut bob_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                .await
                .expect("bob room should connect");
        let (mut charlie_room, mut charlie_events) =
            Room::connect(&format!("http://{addr}"), &charlie_token, options.clone())
                .await
                .expect("charlie room should connect");
        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;
        wait_for_room_connected(&mut charlie_events).await;

        let local_track = alice_room
            .local_participant()
            .publish_data_track("staggered-reconnect-track")
            .await
            .expect("alice publish_data_track should succeed");

        let bob_remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");
        let charlie_remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = charlie_events
                    .recv()
                    .await
                    .expect("charlie room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("charlie should receive data-track published event before timeout");

        let mut bob_stream = bob_remote_track
            .subscribe()
            .await
            .expect("bob should subscribe to remote data track");
        let mut charlie_stream = charlie_remote_track
            .subscribe()
            .await
            .expect("charlie should subscribe to remote data track");

        let baseline_payload = vec![0x10; 64];
        local_track
            .try_push(DataTrackFrame::new(baseline_payload.clone()))
            .expect("alice should push baseline frame");
        let bob_baseline = tokio::time::timeout(Duration::from_secs(10), bob_stream.next())
            .await
            .expect("bob should receive baseline frame before timeout")
            .expect("bob stream should stay open for baseline");
        let charlie_baseline = tokio::time::timeout(Duration::from_secs(10), charlie_stream.next())
            .await
            .expect("charlie should receive baseline frame before timeout")
            .expect("charlie stream should stay open for baseline");
        assert_eq!(bob_baseline.payload().as_ref(), baseline_payload.as_slice());
        assert_eq!(
            charlie_baseline.payload().as_ref(),
            baseline_payload.as_slice()
        );

        let _ = bob_room.close().await;

        let charlie_only_payload = vec![0x20; 64];
        local_track
            .try_push(DataTrackFrame::new(charlie_only_payload.clone()))
            .expect("alice should push frame while bob disconnected");
        let charlie_only = tokio::time::timeout(Duration::from_secs(10), charlie_stream.next())
            .await
            .expect("charlie should receive frame while bob disconnected")
            .expect("charlie stream should stay open while bob disconnected");
        assert_eq!(
            charlie_only.payload().as_ref(),
            charlie_only_payload.as_slice()
        );

        let (bob_room_reconnect, mut bob_reconnect_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                .await
                .expect("bob should reconnect");
        wait_for_room_connected(&mut bob_reconnect_events).await;
        let bob_remote_track_reconnect = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_reconnect_events
                    .recv()
                    .await
                    .expect("bob reconnect room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event after reconnect before timeout");
        bob_stream = bob_remote_track_reconnect
            .subscribe()
            .await
            .expect("bob should resubscribe after reconnect");
        bob_room = bob_room_reconnect;

        let both_payload_after_bob = vec![0x30; 64];
        local_track
            .try_push(DataTrackFrame::new(both_payload_after_bob.clone()))
            .expect("alice should push frame after bob reconnect");
        let bob_after_bob_reconnect =
            tokio::time::timeout(Duration::from_secs(10), bob_stream.next())
                .await
                .expect("bob should receive frame after bob reconnect")
                .expect("bob stream should stay open after bob reconnect");
        let charlie_after_bob_reconnect =
            tokio::time::timeout(Duration::from_secs(10), charlie_stream.next())
                .await
                .expect("charlie should receive frame after bob reconnect")
                .expect("charlie stream should stay open after bob reconnect");
        assert_eq!(
            bob_after_bob_reconnect.payload().as_ref(),
            both_payload_after_bob.as_slice()
        );
        assert_eq!(
            charlie_after_bob_reconnect.payload().as_ref(),
            both_payload_after_bob.as_slice()
        );

        let _ = charlie_room.close().await;

        let bob_only_payload = vec![0x40; 64];
        local_track
            .try_push(DataTrackFrame::new(bob_only_payload.clone()))
            .expect("alice should push frame while charlie disconnected");
        let bob_only = tokio::time::timeout(Duration::from_secs(10), bob_stream.next())
            .await
            .expect("bob should receive frame while charlie disconnected")
            .expect("bob stream should stay open while charlie disconnected");
        assert_eq!(bob_only.payload().as_ref(), bob_only_payload.as_slice());

        let (charlie_room_reconnect, mut charlie_reconnect_events) =
            Room::connect(&format!("http://{addr}"), &charlie_token, options)
                .await
                .expect("charlie should reconnect");
        wait_for_room_connected(&mut charlie_reconnect_events).await;
        let charlie_remote_track_reconnect = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = charlie_reconnect_events
                    .recv()
                    .await
                    .expect("charlie reconnect room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("charlie should receive data-track published event after reconnect before timeout");
        charlie_stream = charlie_remote_track_reconnect
            .subscribe()
            .await
            .expect("charlie should resubscribe after reconnect");
        charlie_room = charlie_room_reconnect;

        let both_payload_after_charlie = vec![0x50; 64];
        local_track
            .try_push(DataTrackFrame::new(both_payload_after_charlie.clone()))
            .expect("alice should push frame after charlie reconnect");
        let bob_after_charlie_reconnect =
            tokio::time::timeout(Duration::from_secs(10), bob_stream.next())
                .await
                .expect("bob should receive frame after charlie reconnect")
                .expect("bob stream should stay open after charlie reconnect");
        let charlie_after_charlie_reconnect =
            tokio::time::timeout(Duration::from_secs(10), charlie_stream.next())
                .await
                .expect("charlie should receive frame after charlie reconnect")
                .expect("charlie stream should stay open after charlie reconnect");
        assert_eq!(
            bob_after_charlie_reconnect.payload().as_ref(),
            both_payload_after_charlie.as_slice()
        );
        assert_eq!(
            charlie_after_charlie_reconnect.payload().as_ref(),
            both_payload_after_charlie.as_slice()
        );

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        let _ = charlie_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/session.rs::tests::relay_data_track_packet_burst_during_disconnect_is_not_replayed_after_reconnect
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_reconnect_under_burst_receives_only_new_frames() {
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

        let room_name = format!("sdk-data-track-reconnect-burst-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-reconnect-burst-alice")
            .with_name("SDK Data Track Reconnect Burst Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-reconnect-burst-bob")
            .with_name("SDK Data Track Reconnect Burst Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
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
            Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;

        let local_track = alice_room
            .local_participant()
            .publish_data_track("reconnect-burst-track")
            .await
            .expect("alice publish_data_track should succeed");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");

        let mut stream = remote_track
            .subscribe()
            .await
            .expect("bob should subscribe before reconnect");

        let pre_disconnect_payload = vec![0xAA; 64];
        local_track
            .try_push(DataTrackFrame::new(pre_disconnect_payload.clone()))
            .expect("alice should push pre-disconnect frame");
        let pre_disconnect_frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("bob should receive pre-disconnect frame before timeout")
            .expect("stream should stay open before disconnect");
        assert_eq!(
            pre_disconnect_frame.payload().as_ref(),
            pre_disconnect_payload.as_slice()
        );

        let _ = bob_room.close().await;

        for i in 0..8u8 {
            local_track
                .try_push(DataTrackFrame::new(vec![0xB0 + i; 80]))
                .expect("alice should push burst frame while bob disconnected");
        }

        let (bob_room_reconnect, mut bob_reconnect_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob should reconnect");
        wait_for_room_connected(&mut bob_reconnect_events).await;

        let remote_track_after_reconnect = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_reconnect_events
                    .recv()
                    .await
                    .expect("bob reconnect room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event after reconnect before timeout");

        let mut stream_after_reconnect = remote_track_after_reconnect
            .subscribe()
            .await
            .expect("bob should resubscribe after reconnect");

        let stale_frame =
            tokio::time::timeout(Duration::from_millis(700), stream_after_reconnect.next()).await;
        assert!(
            stale_frame.is_err(),
            "reconnected subscription should not replay burst frames sent while disconnected"
        );

        let post_reconnect_payload = vec![0xCC; 96];
        local_track
            .try_push(DataTrackFrame::new(post_reconnect_payload.clone()))
            .expect("alice should push frame after reconnect");
        let post_reconnect_frame =
            tokio::time::timeout(Duration::from_secs(10), stream_after_reconnect.next())
                .await
                .expect("bob should receive post-reconnect frame before timeout")
                .expect("stream should stay open after reconnect");
        assert_eq!(
            post_reconnect_frame.payload().as_ref(),
            post_reconnect_payload.as_slice()
        );

        let _ = alice_room.close().await;
        let _ = bob_room_reconnect.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/session.rs::tests::relay_data_track_packet_reconnect_burst_reports_delivery_metrics
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_reconnect_burst_reports_delivery_metrics() {
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

        let room_name = format!("sdk-data-track-reconnect-metrics-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-reconnect-metrics-alice")
            .with_name("SDK Data Track Reconnect Metrics Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-reconnect-metrics-bob")
            .with_name("SDK Data Track Reconnect Metrics Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
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
            Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;

        let local_track = alice_room
            .local_participant()
            .publish_data_track("metrics-track")
            .await
            .expect("alice publish_data_track should succeed");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");
        let mut stream = remote_track
            .subscribe()
            .await
            .expect("bob should subscribe before reconnect");

        let mut next_seq: u32 = 1;
        for _ in 0..12 {
            local_track
                .try_push(DataTrackFrame::new(next_seq.to_be_bytes().to_vec()))
                .expect("alice should push warmup frame");
            let _ = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .expect("bob should receive warmup frame before timeout");
            next_seq += 1;
        }

        let _ = bob_room.close().await;

        for _ in 0..16 {
            local_track
                .try_push(DataTrackFrame::new(next_seq.to_be_bytes().to_vec()))
                .expect("alice should push disconnected burst frame");
            next_seq += 1;
        }

        let (bob_room_reconnect, mut bob_reconnect_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob should reconnect");
        wait_for_room_connected(&mut bob_reconnect_events).await;

        let remote_track_after_reconnect = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_reconnect_events
                    .recv()
                    .await
                    .expect("bob reconnect room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event after reconnect before timeout");

        let mut stream_after_reconnect = remote_track_after_reconnect
            .subscribe()
            .await
            .expect("bob should resubscribe after reconnect");

        let stale =
            tokio::time::timeout(Duration::from_millis(700), stream_after_reconnect.next()).await;
        assert!(
            stale.is_err(),
            "reconnected stream should not replay disconnected burst frames"
        );

        let post_reconnect_start_seq = next_seq;
        let post_reconnect_sent: u32 = 24;
        let mut received_sequences = HashSet::new();
        let mut max_lag: u32 = 0;

        for _ in 0..post_reconnect_sent {
            let sent_seq = next_seq;
            local_track
                .try_push(DataTrackFrame::new(sent_seq.to_be_bytes().to_vec()))
                .expect("alice should push post-reconnect metric frame");

            if let Ok(Some(frame)) =
                tokio::time::timeout(Duration::from_millis(800), stream_after_reconnect.next())
                    .await
                && let Some(received_seq) = data_track_frame_seq(&frame)
                && received_seq >= post_reconnect_start_seq
            {
                let lag = sent_seq.saturating_sub(received_seq);
                if lag > max_lag {
                    max_lag = lag;
                }
                received_sequences.insert(received_seq);
            }
            next_seq += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let delivery_ratio = received_sequences.len() as f64 / post_reconnect_sent as f64;
        assert!(
            delivery_ratio >= 0.70,
            "expected delivery ratio >= 0.70 after reconnect, got {delivery_ratio:.2}"
        );
        assert!(
            max_lag <= 6,
            "expected max lag <= 6 frames after reconnect, got {max_lag}"
        );

        let _ = alice_room.close().await;
        let _ = bob_room_reconnect.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/session.rs::tests::relay_data_track_packet_dual_reconnect_burst_reports_delivery_metrics
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_dual_reconnect_under_burst_reports_delivery_metrics() {
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

        let room_name = format!("sdk-data-track-dual-reconnect-metrics-{}", unique_suffix());

        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-dual-reconnect-metrics-alice")
            .with_name("SDK Data Track Dual Reconnect Metrics Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-dual-reconnect-metrics-bob")
            .with_name("SDK Data Track Dual Reconnect Metrics Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("bob token should encode");
        let charlie_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-dual-reconnect-metrics-charlie")
            .with_name("SDK Data Track Dual Reconnect Metrics Charlie")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("charlie token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);

        let (alice_room, mut alice_events) =
            Room::connect(&format!("http://{addr}"), &alice_token, options.clone())
                .await
                .expect("alice room should connect");
        let (bob_room, mut bob_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                .await
                .expect("bob room should connect");
        let (charlie_room, mut charlie_events) =
            Room::connect(&format!("http://{addr}"), &charlie_token, options.clone())
                .await
                .expect("charlie room should connect");

        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;
        wait_for_room_connected(&mut charlie_events).await;

        let local_track = alice_room
            .local_participant()
            .publish_data_track("dual-reconnect-metrics-track")
            .await
            .expect("alice publish_data_track should succeed");

        let bob_remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");

        let charlie_remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = charlie_events
                    .recv()
                    .await
                    .expect("charlie room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("charlie should receive data-track published event before timeout");

        let mut bob_stream = bob_remote_track
            .subscribe()
            .await
            .expect("bob should subscribe before reconnect");
        let mut charlie_stream = charlie_remote_track
            .subscribe()
            .await
            .expect("charlie should subscribe before reconnect");

        let mut next_seq: u32 = 1;
        for _ in 0..8 {
            local_track
                .try_push(DataTrackFrame::new(next_seq.to_be_bytes().to_vec()))
                .expect("alice should push warmup frame");
            let _ = tokio::time::timeout(Duration::from_secs(2), bob_stream.next())
                .await
                .expect("bob should receive warmup frame before timeout");
            let _ = tokio::time::timeout(Duration::from_secs(2), charlie_stream.next())
                .await
                .expect("charlie should receive warmup frame before timeout");
            next_seq += 1;
        }

        let _ = bob_room.close().await;
        let _ = charlie_room.close().await;

        for _ in 0..12 {
            local_track
                .try_push(DataTrackFrame::new(next_seq.to_be_bytes().to_vec()))
                .expect("alice should push disconnected burst frame");
            next_seq += 1;
        }

        let server_url = format!("http://{addr}");
        let (bob_reconnect_result, charlie_reconnect_result) = tokio::join!(
            Room::connect(&server_url, &bob_token, options.clone()),
            Room::connect(&server_url, &charlie_token, options),
        );
        let (bob_room_reconnect, mut bob_reconnect_events) =
            bob_reconnect_result.expect("bob should reconnect");
        let (charlie_room_reconnect, mut charlie_reconnect_events) =
            charlie_reconnect_result.expect("charlie should reconnect");

        wait_for_room_connected(&mut bob_reconnect_events).await;
        wait_for_room_connected(&mut charlie_reconnect_events).await;

        let bob_remote_track_reconnect = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_reconnect_events
                    .recv()
                    .await
                    .expect("bob reconnect room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event after reconnect before timeout");

        let charlie_remote_track_reconnect = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = charlie_reconnect_events
                    .recv()
                    .await
                    .expect("charlie reconnect room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("charlie should receive data-track published event after reconnect before timeout");

        bob_stream = bob_remote_track_reconnect
            .subscribe()
            .await
            .expect("bob should resubscribe after reconnect");
        charlie_stream = charlie_remote_track_reconnect
            .subscribe()
            .await
            .expect("charlie should resubscribe after reconnect");

        let bob_stale = tokio::time::timeout(Duration::from_millis(700), bob_stream.next()).await;
        assert!(
            bob_stale.is_err(),
            "bob should not receive stale replay from disconnected burst"
        );
        let charlie_stale =
            tokio::time::timeout(Duration::from_millis(700), charlie_stream.next()).await;
        assert!(
            charlie_stale.is_err(),
            "charlie should not receive stale replay from disconnected burst"
        );

        let post_reconnect_start_seq = next_seq;
        let post_reconnect_sent: u32 = 28;

        let mut bob_received_sequences = HashSet::new();
        let mut charlie_received_sequences = HashSet::new();
        let mut bob_max_lag: u32 = 0;
        let mut charlie_max_lag: u32 = 0;

        for _ in 0..post_reconnect_sent {
            let sent_seq = next_seq;
            local_track
                .try_push(DataTrackFrame::new(sent_seq.to_be_bytes().to_vec()))
                .expect("alice should push post-reconnect metric frame");

            if let Ok(Some(frame)) =
                tokio::time::timeout(Duration::from_secs(2), bob_stream.next()).await
                && let Some(received_seq) = data_track_frame_seq(&frame)
                && received_seq >= post_reconnect_start_seq
            {
                let lag = sent_seq.saturating_sub(received_seq);
                if lag > bob_max_lag {
                    bob_max_lag = lag;
                }
                bob_received_sequences.insert(received_seq);
            }

            if let Ok(Some(frame)) =
                tokio::time::timeout(Duration::from_secs(2), charlie_stream.next()).await
                && let Some(received_seq) = data_track_frame_seq(&frame)
                && received_seq >= post_reconnect_start_seq
            {
                let lag = sent_seq.saturating_sub(received_seq);
                if lag > charlie_max_lag {
                    charlie_max_lag = lag;
                }
                charlie_received_sequences.insert(received_seq);
            }

            next_seq += 1;
            tokio::time::sleep(Duration::from_millis(4)).await;
        }

        let bob_delivery_ratio = bob_received_sequences.len() as f64 / post_reconnect_sent as f64;
        let charlie_delivery_ratio =
            charlie_received_sequences.len() as f64 / post_reconnect_sent as f64;

        assert!(
            bob_delivery_ratio >= 0.60,
            "expected bob delivery ratio >= 0.60 after reconnect, got {bob_delivery_ratio:.2}"
        );
        assert!(
            charlie_delivery_ratio >= 0.60,
            "expected charlie delivery ratio >= 0.60 after reconnect, got {charlie_delivery_ratio:.2}"
        );
        assert!(
            bob_max_lag <= 8,
            "expected bob max lag <= 8 after reconnect, got {bob_max_lag}"
        );
        assert!(
            charlie_max_lag <= 8,
            "expected charlie max lag <= 8 after reconnect, got {charlie_max_lag}"
        );

        let _ = alice_room.close().await;
        let _ = bob_room_reconnect.close().await;
        let _ = charlie_room_reconnect.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/session.rs::tests::relay_data_track_packet_packets_before_resubscribe_are_not_buffered_and_only_new_packets_flow
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_resubscribe_receives_only_new_frames() {
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

        let room_name = format!("sdk-data-track-resubscribe-{}", unique_suffix());
        let alice_identity = "sdk-data-track-resubscribe-alice";
        let bob_identity = "sdk-data-track-resubscribe-bob";
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(alice_identity)
            .with_name("SDK Data Track Resubscribe Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(bob_identity)
            .with_name("SDK Data Track Resubscribe Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
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

        let local_track = alice_room
            .local_participant()
            .publish_data_track("resubscribe-telemetry")
            .await
            .expect("alice publish_data_track should succeed");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");

        let mut first_subscription = remote_track
            .subscribe()
            .await
            .expect("bob should subscribe to remote data track");

        let first_payload = vec![0x11; 128];
        let dropped_payload = vec![0x22; 128];
        let second_payload = vec![0x33; 128];

        local_track
            .try_push(DataTrackFrame::new(first_payload.clone()))
            .expect("alice should push first data-track frame");
        let first_frame = tokio::time::timeout(Duration::from_secs(10), first_subscription.next())
            .await
            .expect("bob should receive first data-track frame before timeout")
            .expect("data-track stream should stay open");
        assert_eq!(first_frame.payload().as_ref(), first_payload.as_slice());

        drop(first_subscription);
        tokio::time::sleep(Duration::from_millis(300)).await;

        local_track
            .try_push(DataTrackFrame::new(dropped_payload))
            .expect("alice should push frame while bob is unsubscribed");
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut second_subscription = remote_track
            .subscribe()
            .await
            .expect("bob should re-subscribe to remote data track");

        let stale_frame =
            tokio::time::timeout(Duration::from_millis(700), second_subscription.next()).await;
        assert!(
            stale_frame.is_err(),
            "re-subscription should not receive frames sent while unsubscribed"
        );

        local_track
            .try_push(DataTrackFrame::new(second_payload.clone()))
            .expect("alice should push frame after bob re-subscribes");
        let second_frame =
            tokio::time::timeout(Duration::from_secs(10), second_subscription.next())
                .await
                .expect("bob should receive frame after re-subscribe before timeout")
                .expect("re-subscribed data-track stream should stay open");
        assert_eq!(second_frame.payload().as_ref(), second_payload.as_slice());

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::unpublish_data_track_removes_subscriber_mappings_and_allows_clean_republish
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_unpublish_data_track_reaches_other_room() {
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

        let room_name = format!("sdk-unpublish-data-track-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-unpublish-data-track-alice")
            .with_name("SDK Unpublish Data Track Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-unpublish-data-track-bob")
            .with_name("SDK Unpublish Data Track Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
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

        let local_track = alice_room
            .local_participant()
            .publish_data_track("temporary-sensor")
            .await
            .expect("alice publish_data_track should succeed");

        let published_sid = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track.info().sid();
                }
            }
        })
        .await
        .expect("bob should receive data-track published event before timeout");

        local_track.unpublish();
        local_track.wait_for_unpublish().await;

        let unpublished_sid = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackUnpublished(sid) = event {
                    break sid;
                }
            }
        })
        .await
        .expect("bob should receive data-track unpublished event before timeout");

        assert_eq!(unpublished_sid, published_sid);

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::unpublish_data_track_removes_subscriber_mappings_and_allows_clean_republish
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_data_track_unpublish_while_subscriber_reconnects_allows_clean_republish()
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

        let room_name = format!("sdk-data-track-unpublish-reconnect-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-unpublish-reconnect-alice")
            .with_name("SDK Data Track Unpublish Reconnect Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-data-track-unpublish-reconnect-bob")
            .with_name("SDK Data Track Unpublish Reconnect Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name,
                can_publish_data: true,
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
            Room::connect(&format!("http://{addr}"), &bob_token, options.clone())
                .await
                .expect("bob room should connect");
        wait_for_room_connected(&mut alice_events).await;
        wait_for_room_connected(&mut bob_events).await;

        let old_track = alice_room
            .local_participant()
            .publish_data_track("churn-track")
            .await
            .expect("alice should publish initial data track");

        let old_remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_events
                    .recv()
                    .await
                    .expect("bob room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive initial data-track publication");
        let old_sid = old_remote_track.info().sid();
        let mut old_stream = old_remote_track
            .subscribe()
            .await
            .expect("bob should subscribe to initial data track");

        old_track
            .try_push(DataTrackFrame::new(vec![0x11; 48]))
            .expect("alice should push initial frame");
        let _ = tokio::time::timeout(Duration::from_secs(10), old_stream.next())
            .await
            .expect("bob should receive initial frame before timeout")
            .expect("initial stream should stay open");

        let _ = bob_room.close().await;

        old_track.unpublish();
        old_track.wait_for_unpublish().await;

        let new_track = alice_room
            .local_participant()
            .publish_data_track("churn-track-republished")
            .await
            .expect("alice should publish a replacement data track after unpublish");

        let (bob_room_reconnect, mut bob_reconnect_events) =
            Room::connect(&format!("http://{addr}"), &bob_token, options)
                .await
                .expect("bob should reconnect");
        wait_for_room_connected(&mut bob_reconnect_events).await;

        let new_remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = bob_reconnect_events
                    .recv()
                    .await
                    .expect("bob reconnect events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("bob should receive republished data track after reconnect");

        assert_ne!(new_remote_track.info().sid(), old_sid);
        assert_eq!(new_remote_track.info().name(), "churn-track-republished");

        let mut new_stream = new_remote_track
            .subscribe()
            .await
            .expect("bob should subscribe to republished data track");
        let payload = vec![0x22; 64];
        new_track
            .try_push(DataTrackFrame::new(payload.clone()))
            .expect("alice should push frame on republished data track");
        let received = tokio::time::timeout(Duration::from_secs(10), new_stream.next())
            .await
            .expect("bob should receive republished frame before timeout")
            .expect("republished stream should stay open");
        assert_eq!(received.payload().as_ref(), payload.as_slice());

        let _ = alice_room.close().await;
        let _ = bob_room_reconnect.close().await;
        server.abort();
    }
    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::rtc_v1_user_data_packet_reaches_oxidesfu
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_room_publish_data_reaches_other_room() {
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

        let room_name = format!("sdk-room-data-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-room-alice")
            .with_name("SDK Room Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("alice token should encode");
        let bob_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-room-bob")
            .with_name("SDK Room Bob")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.clone(),
                can_publish_data: true,
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

        alice_room
            .local_participant()
            .publish_data(DataPacket {
                payload: b"hello from sdk room".to_vec(),
                topic: Some("sdk-room-topic".to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .expect("alice publish_data should succeed");

        let (payload, topic, kind) = next_data_received(&mut bob_events).await;
        assert_eq!(payload.as_slice(), b"hello from sdk room");
        assert_eq!(topic.as_deref(), Some("sdk-room-topic"));
        assert_eq!(kind, DataPacketKind::Reliable);

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }

use super::*;

    #[tokio::test]
    async fn rust_sdk_room_publish_lossy_data_reaches_other_room() {
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

        let room_name = format!("sdk-room-lossy-data-{}", unique_suffix());
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-room-lossy-alice")
            .with_name("SDK Room Lossy Alice")
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
            .with_identity("sdk-room-lossy-bob")
            .with_name("SDK Room Lossy Bob")
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
                payload: b"lossy payload".to_vec(),
                topic: Some("lossy-topic".to_string()),
                reliable: false,
                ..Default::default()
            })
            .await
            .expect("alice lossy publish_data should succeed");

        let (payload, topic, kind) = next_data_received(&mut bob_events).await;
        assert_eq!(payload.as_slice(), b"lossy payload");
        assert_eq!(topic.as_deref(), Some("lossy-topic"));
        assert_eq!(kind, DataPacketKind::Lossy);

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }

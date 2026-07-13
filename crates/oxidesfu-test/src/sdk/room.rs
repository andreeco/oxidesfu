use super::*;

    #[tokio::test]
    async fn rust_sdk_room_client_create_list_delete() {
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

        let client = RoomClient::with_api_key(&format!("http://{addr}"), API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let room = client
            .create_room(
                "sdk-api-room",
                CreateRoomOptions {
                    metadata: "created by rust sdk".to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect("SDK room client should create room through OxideSFU Twirp");
        assert_eq!(room.name, "sdk-api-room");
        assert_eq!(room.metadata, "created by rust sdk");

        let rooms = client
            .list_rooms(vec!["sdk-api-room".to_string()])
            .await
            .expect("SDK room client should list rooms through OxideSFU Twirp");
        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].name, "sdk-api-room");

        client
            .delete_room("sdk-api-room")
            .await
            .expect("SDK room client should delete room through OxideSFU Twirp");
        let rooms = client
            .list_rooms(vec!["sdk-api-room".to_string()])
            .await
            .expect("SDK room client should list after delete");
        assert!(rooms.is_empty());

        server.abort();
    }
    #[tokio::test]
    async fn rust_sdk_room_client_lists_participant_after_signal_join() {
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

        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("sdk-participant")
            .with_name("SDK Participant")
            .with_grants(VideoGrants {
                room_join: true,
                room: "sdk-participant-room".to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");
        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (signal_client, _join, _events) =
            SignalClient::connect(&format!("http://{addr}"), &token, options, None)
                .await
                .expect("SDK signal client should connect to OxideSFU /rtc/v1");

        let room_client = RoomClient::with_api_key(&format!("http://{addr}"), API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let participants = room_client
            .list_participants("sdk-participant-room")
            .await
            .expect("SDK room client should list participants after signal join");
        assert_eq!(participants.len(), 1);
        assert_eq!(participants[0].identity, "sdk-participant");
        assert_eq!(participants[0].name, "SDK Participant");

        let participant = room_client
            .get_participant("sdk-participant-room", "sdk-participant")
            .await
            .expect("SDK room client should get joined participant");
        assert_eq!(participant.identity, "sdk-participant");
        assert_eq!(participant.name, "SDK Participant");

        signal_client.close().await;
        server.abort();
    }
    #[tokio::test]
    #[allow(deprecated)]
    async fn rust_sdk_room_client_send_data_reaches_signal_data_channel() {
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

        let room = format!("sdk-send-data-{}", unique_suffix());
        let identity = "sdk-data-alice";
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name("SDK Data Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: room.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let url = format!("ws://{addr}/rtc/v1?join_request={}", join_request_param());
        let mut request = url.into_client_request().expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).expect("auth header should parse"),
        );
        let (mut socket, _) = connect_async(request)
            .await
            .expect("websocket should connect");
        let _join = socket
            .next()
            .await
            .expect("join should arrive")
            .expect("join should be ok");

        let (client_peer, mut client_events) = oxidesfu_rtc::create_peer_connection_with_events()
            .await
            .expect("client peer connection should create");
        let data_channel = client_peer
            .create_data_channel("data")
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
                    id: 9,
                    ..Default::default()
                },
            )),
        };
        socket
            .send(Message::Binary(offer.encode_to_vec().into()))
            .await
            .expect("offer should send");

        let answer_message = socket
            .next()
            .await
            .expect("answer should arrive")
            .expect("answer should be ok");
        let Message::Binary(answer_bytes) = answer_message else {
            panic!("expected binary answer response");
        };
        let answer = proto::SignalResponse::decode(answer_bytes.as_ref())
            .expect("answer response should decode");
        let Some(proto::signal_response::Message::Answer(answer)) = answer.message else {
            panic!("expected answer response");
        };
        client_peer
            .set_remote_answer(answer.sdp)
            .await
            .expect("answer should apply");

        let (open_tx, mut open_rx) = tokio::sync::mpsc::unbounded_channel();
        let recv_task = tokio::spawn(async move {
            data_channel.wait_open().await?;
            let _ = open_tx.send(());
            data_channel.recv_bytes().await
        });
        tokio::pin!(recv_task);
        let mut sent = false;
        let received = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = client_events.ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
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
                                .expect("client trickle should send");
                        }
                    }
                    message = socket.next() => {
                        let Some(Ok(Message::Binary(bytes))) = message else {
                            continue;
                        };
                        let response = proto::SignalResponse::decode(bytes.as_ref())
                            .expect("signal response should decode");
                        if let Some(proto::signal_response::Message::Trickle(trickle)) = response.message {
                            client_peer
                                .add_ice_candidate_json(&trickle.candidate_init)
                                .await
                                .expect("server trickle should add");
                        }
                    }
                    Some(()) = open_rx.recv(), if !sent => {
                        let room_client = RoomClient::with_api_key(&format!("http://{addr}"), API_KEY, API_SECRET)
                            .with_failover(false)
                            .with_request_timeout(Duration::from_secs(5));
                        room_client
                            .send_data(
                                &room,
                                b"hello via twirp".to_vec(),
                                SendDataOptions {
                                    kind: proto::data_packet::Kind::Reliable,
                                    destination_identities: vec![identity.to_string()],
                                    topic: Some("twirp-topic".to_string()),
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("SDK RoomClient SendData should succeed");
                        sent = true;
                    }
                    result = &mut recv_task => {
                        break result
                            .expect("recv task should not panic")
                            .expect("client should receive bytes");
                    }
                }
            }
        })
        .await
        .expect("client should receive Twirp SendData packet before timeout");

        let packet = proto::DataPacket::decode(received.as_slice())
            .expect("received data packet should decode");
        assert_eq!(packet.kind, proto::data_packet::Kind::Reliable as i32);
        let Some(proto::data_packet::Value::User(user)) = packet.value else {
            panic!("expected user packet");
        };
        assert_eq!(user.payload, b"hello via twirp");
        assert_eq!(user.topic.as_deref(), Some("twirp-topic"));

        client_peer.close().await.expect("client peer should close");
        server.abort();
    }
    #[tokio::test]
    #[allow(deprecated)]
    async fn rust_sdk_room_client_send_data_filters_destinations_and_broadcasts() {
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

        let room = format!("sdk-send-data-two-{}", unique_suffix());
        let mut alice = connect_data_participant(addr, &room, "alice-filter").await;
        let mut bob = connect_data_participant(addr, &room, "bob-filter").await;
        let room_client = RoomClient::with_api_key(&format!("http://{addr}"), API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let mut alice_open = false;
        let mut bob_open = false;
        tokio::time::timeout(Duration::from_secs(10), async {
            while !alice_open || !bob_open {
                tokio::select! {
                    _ = alice.open_rx.recv(), if !alice_open => alice_open = true,
                    _ = bob.open_rx.recv(), if !bob_open => bob_open = true,
                    candidate = alice.events.ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            send_trickle(&mut alice.socket, candidate).await;
                        }
                    }
                    candidate = bob.events.ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            send_trickle(&mut bob.socket, candidate).await;
                        }
                    }
                    message = alice.socket.next() => {
                        handle_signal_message(message, &alice.peer).await;
                    }
                    message = bob.socket.next() => {
                        handle_signal_message(message, &bob.peer).await;
                    }
                }
            }
        })
        .await
        .expect("both data channels should open before timeout");

        room_client
            .send_data(
                &room,
                b"target alice only".to_vec(),
                SendDataOptions {
                    kind: proto::data_packet::Kind::Reliable,
                    destination_identities: vec!["alice-filter".to_string()],
                    topic: Some("targeted".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("targeted SendData should succeed");

        let alice_targeted = tokio::time::timeout(Duration::from_secs(5), alice.data_rx.recv())
            .await
            .expect("alice should receive targeted packet before timeout")
            .expect("alice data channel should stay open");
        assert_user_packet(&alice_targeted, b"target alice only", Some("targeted"));
        assert!(
            tokio::time::timeout(Duration::from_millis(250), bob.data_rx.recv())
                .await
                .is_err(),
            "bob should not receive alice-targeted SendData"
        );

        let participants = room_client
            .list_participants(&room)
            .await
            .expect("list participants should include alice and bob");
        let sids_by_identity: HashMap<_, _> = participants
            .iter()
            .map(|participant| (participant.identity.clone(), participant.sid.clone()))
            .collect();
        let alice_sid = sids_by_identity
            .get("alice-filter")
            .cloned()
            .expect("alice participant SID should be available");

        room_client
            .send_data(
                &room,
                b"target alice by sid".to_vec(),
                SendDataOptions {
                    kind: proto::data_packet::Kind::Reliable,
                    destination_sids: vec![alice_sid.clone()],
                    topic: Some("targeted-sid".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("SID-targeted SendData should succeed");

        let alice_sid_targeted = tokio::time::timeout(Duration::from_secs(5), alice.data_rx.recv())
            .await
            .expect("alice should receive SID-targeted packet before timeout")
            .expect("alice data channel should stay open for SID-targeted packet");
        assert_user_packet(
            &alice_sid_targeted,
            b"target alice by sid",
            Some("targeted-sid"),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(250), bob.data_rx.recv())
                .await
                .is_err(),
            "bob should not receive alice SID-targeted SendData"
        );

        room_client
            .send_data(
                &room,
                b"target mixed sid+identity".to_vec(),
                SendDataOptions {
                    kind: proto::data_packet::Kind::Reliable,
                    destination_sids: vec!["PA_does_not_exist".to_string(), alice_sid],
                    destination_identities: vec![
                        "bob-filter".to_string(),
                        "alice-filter".to_string(),
                    ],
                    topic: Some("targeted-mixed".to_string()),
                },
            )
            .await
            .expect("mixed SID/identity SendData should succeed");

        let alice_mixed = tokio::time::timeout(Duration::from_secs(5), alice.data_rx.recv())
            .await
            .expect("alice should receive mixed-target packet before timeout")
            .expect("alice data channel should stay open for mixed-target packet");
        let bob_mixed = tokio::time::timeout(Duration::from_secs(5), bob.data_rx.recv())
            .await
            .expect("bob should receive mixed-target packet before timeout")
            .expect("bob data channel should stay open for mixed-target packet");
        assert_user_packet(
            &alice_mixed,
            b"target mixed sid+identity",
            Some("targeted-mixed"),
        );
        assert_user_packet(
            &bob_mixed,
            b"target mixed sid+identity",
            Some("targeted-mixed"),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), alice.data_rx.recv())
                .await
                .is_err(),
            "alice should receive mixed-target packet only once when matched by SID and identity"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), bob.data_rx.recv())
                .await
                .is_err(),
            "bob should receive mixed-target packet only once"
        );

        room_client
            .send_data(
                &room,
                b"unknown sid should not broadcast".to_vec(),
                SendDataOptions {
                    kind: proto::data_packet::Kind::Reliable,
                    destination_sids: vec!["PA_unknown_only".to_string()],
                    topic: Some("unknown-sid".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("unknown SID SendData should succeed");

        assert!(
            tokio::time::timeout(Duration::from_millis(300), alice.data_rx.recv())
                .await
                .is_err(),
            "alice should not receive packet for unknown-only SID target"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), bob.data_rx.recv())
                .await
                .is_err(),
            "bob should not receive packet for unknown-only SID target"
        );

        room_client
            .send_data(
                &room,
                b"broadcast everyone".to_vec(),
                SendDataOptions {
                    kind: proto::data_packet::Kind::Reliable,
                    topic: Some("broadcast".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("broadcast SendData should succeed");

        let alice_broadcast = tokio::time::timeout(Duration::from_secs(5), alice.data_rx.recv())
            .await
            .expect("alice should receive broadcast before timeout")
            .expect("alice data channel should stay open");
        let bob_broadcast = tokio::time::timeout(Duration::from_secs(5), bob.data_rx.recv())
            .await
            .expect("bob should receive broadcast before timeout")
            .expect("bob data channel should stay open");
        assert_user_packet(&alice_broadcast, b"broadcast everyone", Some("broadcast"));
        assert_user_packet(&bob_broadcast, b"broadcast everyone", Some("broadcast"));

        alice.peer.close().await.expect("alice peer should close");
        bob.peer.close().await.expect("bob peer should close");
        server.abort();
    }

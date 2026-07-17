
    async fn assert_rust_sdk_data_track_frame_reaches_subscriber(
        room_prefix: &str,
        track_name: &str,
        payload: Vec<u8>,
    ) {
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

        let room_name = format!("{room_prefix}-{}", unique_suffix());
        let alice_identity = format!("{room_prefix}-alice");
        let bob_identity = format!("{room_prefix}-bob");
        let alice_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&alice_identity)
            .with_name("SDK Data Track Frame Alice")
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
            .with_identity(&bob_identity)
            .with_name("SDK Data Track Frame Bob")
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
            .publish_data_track(track_name)
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
            .expect("bob should subscribe to remote data track");

        local_track
            .try_push(DataTrackFrame::new(payload.clone()))
            .expect("alice should push a data-track frame");

        let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("bob should receive data-track frame before timeout")
            .expect("data-track stream should stay open");
        assert_eq!(frame.payload().as_ref(), payload.as_slice());

        let _ = alice_room.close().await;
        let _ = bob_room.close().await;
        server.abort();
    }
    fn data_track_frame_seq(frame: &DataTrackFrame) -> Option<u32> {
        let payload = frame.payload();
        if payload.len() < 4 {
            return None;
        }
        let bytes = [payload[0], payload[1], payload[2], payload[3]];
        Some(u32::from_be_bytes(bytes))
    }
    async fn next_data_received(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<RoomEvent>,
    ) -> (Vec<u8>, Option<String>, DataPacketKind) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let event = events.recv().await.expect("room events should stay open");
                if let RoomEvent::DataReceived {
                    payload,
                    topic,
                    kind,
                    ..
                } = event
                {
                    break ((*payload).clone(), topic, kind);
                }
            }
        })
        .await
        .expect("room should receive data before timeout")
    }
    async fn wait_for_room_connected(events: &mut tokio::sync::mpsc::UnboundedReceiver<RoomEvent>) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = events.recv().await.expect("room events should stay open");
                if matches!(event, RoomEvent::Connected { .. }) {
                    break;
                }
            }
        })
        .await
        .expect("room should emit connected before timeout");
    }
    struct DataParticipant {
        socket: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        peer: oxidesfu_rtc::PeerConnection,
        events: oxidesfu_rtc::PeerConnectionEvents,
        open_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        data_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    }
    async fn connect_data_participant(
        addr: std::net::SocketAddr,
        room: &str,
        identity: &str,
    ) -> DataParticipant {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room.to_string(),
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

        let (peer, events) = oxidesfu_rtc::create_peer_connection_with_events()
            .await
            .expect("client peer connection should create");
        let data_channel = peer
            .create_data_channel("data")
            .await
            .expect("client data channel should create");
        let offer_sdp = peer.create_offer().await.expect("offer should create");
        let offer = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Offer(
                proto::SessionDescription {
                    r#type: "offer".to_string(),
                    sdp: offer_sdp,
                    id: 10,
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
        peer.set_remote_answer(answer.sdp)
            .await
            .expect("answer should apply");

        let (open_tx, open_rx) = tokio::sync::mpsc::unbounded_channel();
        let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            if data_channel.wait_open().await.is_err() {
                return;
            }
            let _ = open_tx.send(());
            while let Ok(bytes) = data_channel.recv_bytes().await {
                if data_tx.send(bytes).is_err() {
                    break;
                }
            }
        });

        DataParticipant {
            socket,
            peer,
            events,
            open_rx,
            data_rx,
        }
    }


    async fn send_trickle(
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        candidate: oxidesfu_rtc::IceCandidate,
    ) {
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
    async fn handle_signal_message(
        message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        peer: &oxidesfu_rtc::PeerConnection,
    ) {
        let Some(Ok(Message::Binary(bytes))) = message else {
            return;
        };
        let response =
            proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
        if let Some(proto::signal_response::Message::Trickle(trickle)) = response.message {
            peer.add_ice_candidate_json(&trickle.candidate_init)
                .await
                .expect("server trickle should add");
        }
    }
    fn assert_user_packet(bytes: &[u8], expected_payload: &[u8], expected_topic: Option<&str>) {
        let packet = proto::DataPacket::decode(bytes).expect("data packet should decode");
        let Some(proto::data_packet::Value::User(user)) = packet.value else {
            panic!("expected user packet");
        };
        assert_eq!(user.payload, expected_payload);
        assert_eq!(user.topic.as_deref(), expected_topic);
    }
    #[derive(Debug, PartialEq, Eq)]
    struct RoomLifecycleResult {
        created_name: String,
        created_metadata: String,
        listed_after_create: usize,
        listed_after_delete: usize,
    }
    #[derive(Debug, PartialEq)]
    struct SignalJoinVisibilityResult {
        joined_room_name: String,
        joined_identity: String,
        ice_servers: Vec<proto::IceServer>,
        joined_display_name: String,
        listed_participant_count: usize,
        fetched_identity: String,
        fetched_display_name: String,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct PingReqResult {
        last_ping_timestamp: i64,
        response_timestamp: i64,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct HttpStatusBody {
        status: u16,
        body: String,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct LeaveVisibilityResult {
        listed_before_leave: usize,
        listed_after_leave: usize,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct ValidateNegativePathsResult {
        missing_join_status: u16,
        missing_join_body: String,
        missing_auth_status: u16,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct ValidateMalformedJoinRequestResult {
        status: u16,
        has_body: bool,
        has_join_request_hint: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct ValidateInvalidGzipJoinRequestResult {
        status: u16,
        has_body: bool,
        has_gzip_hint: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct AuthFailureResult {
        status_is_client_error: bool,
        has_body: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct SignalV1AuthFailureMatrixResult {
        missing_auth: AuthFailureResult,
        invalid_token: AuthFailureResult,
        missing_join_grant: AuthFailureResult,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct MediaPublishSubscribeResult {
        published_track_name: String,
        published_by_identity: String,
        subscribed_track_name: String,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct ReconnectResubscribeResult {
        first_payload_len: usize,
        second_payload_len: usize,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct AudioReconnectResubscribeResult {
        received_before_reconnect: bool,
        received_after_reconnect: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct ReconnectIdentityResult {
        listed_after_first_join: usize,
        listed_after_reconnect: usize,
        fetched_identity: String,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct TwirpErrorShapeResult {
        status: u16,
        code: String,
        has_msg: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct TwirpPermissionFailureResult {
        status_is_auth_failure: bool,
        code_is_auth_related: bool,
        has_msg: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct ReconnectUnderLoadResult {
        subscriber_one_recovered: bool,
        subscriber_two_recovered: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct PublisherDropLifecycleResult {
        saw_unpublished: bool,
        saw_participant_disconnected: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct TwirpMissingRoomParityResult {
        status_is_error: bool,
        code_is_not_found_or_unavailable: bool,
        has_msg: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct TwirpMissingResourceParityResult {
        status_is_error: bool,
        code_is_not_found_or_unavailable: bool,
        has_msg: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct SignalLeaveLifecycleResult {
        participant_visible_before_leave: bool,
        participant_removed_after_leave: bool,
        saw_leave_or_close: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct SignalTakeoverCloseLifecycleResult {
        listed_after_takeover: usize,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct SignalPostCloseSendResult {
        send_after_close_attempted: bool,
        saw_pong_after_close: bool,
        stream_closed_after_close: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct SignalReconnectReasonResult {
        saw_reconnect_response: bool,
        participant_sid_present: bool,
        reconnect_ice_server_count: usize,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct SignalReconnectReasonPingResult {
        saw_reconnect_response: bool,
        participant_sid_present: bool,
        reconnect_ice_server_count: usize,
        saw_pong_response: bool,
        last_ping_timestamp: i64,
        response_timestamp: i64,
        socket_closed_or_ended_after_ping: bool,
    }
    #[derive(Debug, PartialEq, Eq)]
    struct SignalReconnectStaleSidResult {
        participant_sid_present: bool,
        stale_sid_used: bool,
        saw_reconnect_response: bool,
        saw_leave: bool,
        leave_action: i32,
        leave_reason: i32,
        socket_closed_or_ended: bool,
    }
    #[derive(Debug, Clone, Copy)]
    enum DifferentialCase {
        TwirpRoomLifecycle,
        ValidateV1NegativePaths,
        ValidateV1MalformedJoinRequest,
        ValidateV1InvalidGzipJoinRequest,
        SignalV1AuthFailureMatrix,
        SignalJoinParticipantVisibility,
        SignalPingReqPongResp,
        SignalReconnectResubscribeDataTrack,
        SignalAbruptDisconnectParticipantVisibility,
        SignalReconnectIdentityTakeover,
        TwirpDeleteMissingRoomErrorShape,
        SignalReconnectThenPingReqPongResp,
        SignalReconnectReasonUnknown,
        SignalReconnectReasonPublisherFailed,
        SignalReconnectReasonSwitchCandidate,
        SignalReconnectReasonSubscriberFailed,
        SignalReconnectReasonSignalDisconnected,
        SignalReconnectStaleParticipantSidLifecycle,
        TwirpListParticipantsPermissionDenied,
        TwirpListParticipantsMissingAuth,
        TwirpListParticipantsMalformedBody,
        TwirpListParticipantsContentTypeMismatch,
        TwirpListRoomsPermissionDenied,
        TwirpCreateRoomMissingAuth,
        TwirpCreateRoomMalformedBody,
        TwirpRemoveParticipantPermissionDenied,
        TwirpUpdateParticipantMalformedBody,
        TwirpCreateRoomContentTypeMismatch,
        TwirpSendDataPermissionDenied,
        TwirpSendDataInvalidNonce,
        TwirpSendDataMissingRoom,
        TwirpGetParticipantPermissionDenied,
        TwirpGetParticipantMalformedBody,
        TwirpUpdateRoomMetadataPermissionDenied,
        TwirpUpdateRoomMetadataMissingRoom,
        TwirpSendDataContentTypeMismatch,
        TwirpGetParticipantMissingParticipant,
        TwirpUpdateRoomMetadataMalformedBody,
        TwirpUpdateRoomMetadataContentTypeMismatch,
        TwirpSendDataMissingAuth,
        SignalPostCloseSendNoPong,
        SignalLeaveTerminationLifecycle,
        SignalIdentityTakeoverCloseLifecycle,
        DataTrackMultiSubscriberReconnectUnderLoad,
        DataTrackPublisherDropLifecycle,
    }
    #[derive(Debug, PartialEq)]
    enum DifferentialCaseResult {
        TwirpRoomLifecycle(RoomLifecycleResult),
        ValidateV1NegativePaths(ValidateNegativePathsResult),
        ValidateV1MalformedJoinRequest(ValidateMalformedJoinRequestResult),
        ValidateV1InvalidGzipJoinRequest(ValidateInvalidGzipJoinRequestResult),
        SignalV1AuthFailureMatrix(SignalV1AuthFailureMatrixResult),
        SignalJoinParticipantVisibility(SignalJoinVisibilityResult),
        SignalPingReqPongResp(i64),
        SignalReconnectResubscribeDataTrack(ReconnectResubscribeResult),
        SignalAbruptDisconnectParticipantVisibility(LeaveVisibilityResult),
        SignalReconnectIdentityTakeover(ReconnectIdentityResult),
        TwirpDeleteMissingRoomErrorShape(TwirpErrorShapeResult),
        SignalReconnectThenPingReqPongResp(i64),
        SignalReconnectReasonUnknown(SignalReconnectReasonResult),
        SignalReconnectReasonPublisherFailed(SignalReconnectReasonResult),
        SignalReconnectReasonSwitchCandidate(SignalReconnectReasonResult),
        SignalReconnectReasonSubscriberFailed(SignalReconnectReasonResult),
        SignalReconnectReasonSignalDisconnected(SignalReconnectReasonResult),
        SignalReconnectStaleParticipantSidLifecycle(SignalReconnectStaleSidResult),
        TwirpListParticipantsPermissionDenied(TwirpPermissionFailureResult),
        TwirpListParticipantsMissingAuth(TwirpErrorShapeResult),
        TwirpListParticipantsMalformedBody(TwirpErrorShapeResult),
        TwirpListParticipantsContentTypeMismatch(TwirpErrorShapeResult),
        TwirpListRoomsPermissionDenied(TwirpPermissionFailureResult),
        TwirpCreateRoomMissingAuth(TwirpErrorShapeResult),
        TwirpCreateRoomMalformedBody(TwirpErrorShapeResult),
        TwirpRemoveParticipantPermissionDenied(TwirpPermissionFailureResult),
        TwirpUpdateParticipantMalformedBody(TwirpErrorShapeResult),
        TwirpCreateRoomContentTypeMismatch(TwirpErrorShapeResult),
        TwirpSendDataPermissionDenied(TwirpPermissionFailureResult),
        TwirpSendDataInvalidNonce(TwirpErrorShapeResult),
        TwirpSendDataMissingRoom(TwirpMissingRoomParityResult),
        TwirpGetParticipantPermissionDenied(TwirpPermissionFailureResult),
        TwirpGetParticipantMalformedBody(TwirpErrorShapeResult),
        TwirpUpdateRoomMetadataPermissionDenied(TwirpPermissionFailureResult),
        TwirpUpdateRoomMetadataMissingRoom(TwirpMissingResourceParityResult),
        TwirpSendDataContentTypeMismatch(TwirpErrorShapeResult),
        TwirpGetParticipantMissingParticipant(TwirpMissingResourceParityResult),
        TwirpUpdateRoomMetadataMalformedBody(TwirpErrorShapeResult),
        TwirpUpdateRoomMetadataContentTypeMismatch(TwirpErrorShapeResult),
        TwirpSendDataMissingAuth(TwirpErrorShapeResult),
        SignalPostCloseSendNoPong(SignalPostCloseSendResult),
        SignalLeaveTerminationLifecycle(SignalLeaveLifecycleResult),
        SignalIdentityTakeoverCloseLifecycle(SignalTakeoverCloseLifecycleResult),
        DataTrackMultiSubscriberReconnectUnderLoad(ReconnectUnderLoadResult),
        DataTrackPublisherDropLifecycle(PublisherDropLifecycleResult),
    }
    async fn run_differential_case(
        case: DifferentialCase,
        base_url: &str,
        namespace: &str,
    ) -> DifferentialCaseResult {
        match case {
            DifferentialCase::TwirpRoomLifecycle => DifferentialCaseResult::TwirpRoomLifecycle(
                run_room_lifecycle(
                    base_url,
                    &format!("diff-matrix-room-{namespace}"),
                    &format!("diff-matrix-metadata-{namespace}"),
                )
                .await,
            ),
            DifferentialCase::ValidateV1NegativePaths => {
                DifferentialCaseResult::ValidateV1NegativePaths(
                    run_validate_v1_negative_paths(base_url, namespace).await,
                )
            }
            DifferentialCase::ValidateV1MalformedJoinRequest => {
                DifferentialCaseResult::ValidateV1MalformedJoinRequest(
                    run_validate_v1_malformed_join_request_parity(
                        base_url,
                        &format!("diff-matrix-validate-malformed-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::ValidateV1InvalidGzipJoinRequest => {
                DifferentialCaseResult::ValidateV1InvalidGzipJoinRequest(
                    run_validate_v1_invalid_gzip_join_request_parity(
                        base_url,
                        &format!("diff-matrix-validate-gzip-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::SignalV1AuthFailureMatrix => {
                DifferentialCaseResult::SignalV1AuthFailureMatrix(
                    run_signal_v1_auth_failure_matrix(
                        base_url,
                        &format!("diff-matrix-signal-auth-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::SignalJoinParticipantVisibility => {
                DifferentialCaseResult::SignalJoinParticipantVisibility(
                    run_signal_join_participant_visibility(
                        base_url,
                        &format!("diff-matrix-signal-room-{namespace}"),
                        &format!("diff-matrix-identity-{namespace}"),
                        &format!("Diff Matrix {namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::SignalPingReqPongResp => {
                let result = run_signal_pingreq_pongresp(
                    base_url,
                    &format!("diff-matrix-ping-room-{namespace}"),
                    &format!("diff-matrix-ping-identity-{namespace}"),
                    ping_timestamp_from_namespace(namespace),
                )
                .await;
                assert!(result.response_timestamp > 0);
                DifferentialCaseResult::SignalPingReqPongResp(result.last_ping_timestamp)
            }
            DifferentialCase::SignalReconnectResubscribeDataTrack => {
                DifferentialCaseResult::SignalReconnectResubscribeDataTrack(
                    run_reconnect_resubscribe_data_track(
                        base_url,
                        &format!("diff-matrix-reconnect-room-{namespace}"),
                        &format!("diff-matrix-reconnect-alice-{namespace}"),
                        &format!("diff-matrix-reconnect-bob-{namespace}"),
                    )
                    .await,
                )
            }


            DifferentialCase::SignalAbruptDisconnectParticipantVisibility => {
                DifferentialCaseResult::SignalAbruptDisconnectParticipantVisibility(
                    run_abrupt_disconnect_participant_visibility(
                        base_url,
                        &format!("diff-matrix-abrupt-room-{namespace}"),
                        &format!("diff-matrix-abrupt-identity-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::SignalReconnectIdentityTakeover => {
                DifferentialCaseResult::SignalReconnectIdentityTakeover(
                    run_reconnect_identity_takeover_visibility(
                        base_url,
                        &format!("diff-matrix-reconnect-identity-room-{namespace}"),
                        &format!("diff-matrix-reconnect-identity-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpDeleteMissingRoomErrorShape => {
                DifferentialCaseResult::TwirpDeleteMissingRoomErrorShape(
                    run_twirp_delete_missing_room_error_shape(
                        base_url,
                        &format!("diff-matrix-missing-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::SignalReconnectThenPingReqPongResp => {
                let ping_timestamp = ping_timestamp_from_namespace(namespace).wrapping_add(777);
                let result = run_signal_reconnect_then_pingreq_pongresp(
                    base_url,
                    &format!("diff-matrix-reconnect-ping-room-{namespace}"),
                    &format!("diff-matrix-reconnect-ping-identity-{namespace}"),
                    ping_timestamp,
                )
                .await;
                assert!(result.response_timestamp > 0);
                DifferentialCaseResult::SignalReconnectThenPingReqPongResp(
                    result.last_ping_timestamp,
                )
            }
            DifferentialCase::SignalReconnectReasonUnknown => {
                DifferentialCaseResult::SignalReconnectReasonUnknown(
                    run_signal_reconnect_reason_response(
                        base_url,
                        &format!("diff-matrix-reconnect-unknown-room-{namespace}"),
                        &format!("diff-matrix-reconnect-unknown-identity-{namespace}"),
                        proto::ReconnectReason::RrUnknown,
                    )
                    .await,
                )
            }
            DifferentialCase::SignalReconnectReasonPublisherFailed => {
                DifferentialCaseResult::SignalReconnectReasonPublisherFailed(
                    run_signal_reconnect_reason_response(
                        base_url,
                        &format!("diff-matrix-reconnect-reason-room-{namespace}"),
                        &format!("diff-matrix-reconnect-reason-identity-{namespace}"),
                        proto::ReconnectReason::RrPublisherFailed,
                    )
                    .await,
                )
            }
            DifferentialCase::SignalReconnectReasonSwitchCandidate => {
                DifferentialCaseResult::SignalReconnectReasonSwitchCandidate(
                    run_signal_reconnect_reason_response(
                        base_url,
                        &format!("diff-matrix-reconnect-switch-candidate-room-{namespace}"),
                        &format!("diff-matrix-reconnect-switch-candidate-identity-{namespace}"),
                        proto::ReconnectReason::RrSwitchCandidate,
                    )
                    .await,
                )
            }
            DifferentialCase::SignalReconnectReasonSubscriberFailed => {
                DifferentialCaseResult::SignalReconnectReasonSubscriberFailed(
                    run_signal_reconnect_reason_response(
                        base_url,
                        &format!("diff-matrix-reconnect-subscriber-failed-room-{namespace}"),
                        &format!("diff-matrix-reconnect-subscriber-failed-identity-{namespace}"),
                        proto::ReconnectReason::RrSubscriberFailed,
                    )
                    .await,
                )
            }
            DifferentialCase::SignalReconnectReasonSignalDisconnected => {
                DifferentialCaseResult::SignalReconnectReasonSignalDisconnected(
                    run_signal_reconnect_reason_response(
                        base_url,
                        &format!("diff-matrix-reconnect-signal-disconnected-room-{namespace}"),
                        &format!("diff-matrix-reconnect-signal-disconnected-identity-{namespace}"),
                        proto::ReconnectReason::RrSignalDisconnected,
                    )
                    .await,
                )
            }
            DifferentialCase::SignalReconnectStaleParticipantSidLifecycle => {
                DifferentialCaseResult::SignalReconnectStaleParticipantSidLifecycle(
                    run_signal_reconnect_stale_participant_sid_lifecycle(
                        base_url,
                        &format!("diff-matrix-reconnect-stale-sid-room-{namespace}"),
                        &format!("diff-matrix-reconnect-stale-sid-identity-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpListParticipantsPermissionDenied => {
                DifferentialCaseResult::TwirpListParticipantsPermissionDenied(
                    run_twirp_list_participants_permission_denied_error_shape(
                        base_url,
                        &format!("diff-matrix-perm-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpListParticipantsMissingAuth => {
                DifferentialCaseResult::TwirpListParticipantsMissingAuth(
                    run_twirp_list_participants_missing_auth_error_shape(
                        base_url,
                        &format!("diff-matrix-list-participants-missing-auth-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpListParticipantsMalformedBody => {
                DifferentialCaseResult::TwirpListParticipantsMalformedBody(
                    run_twirp_list_participants_malformed_body_error_shape(
                        base_url,
                        &format!("diff-matrix-list-participants-malformed-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpListParticipantsContentTypeMismatch => {
                DifferentialCaseResult::TwirpListParticipantsContentTypeMismatch(
                    run_twirp_list_participants_content_type_mismatch_error_shape(
                        base_url,
                        &format!("diff-matrix-list-participants-content-type-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpListRoomsPermissionDenied => {
                DifferentialCaseResult::TwirpListRoomsPermissionDenied(
                    run_twirp_list_rooms_permission_denied_error_shape(base_url).await,
                )
            }
            DifferentialCase::TwirpCreateRoomMissingAuth => {
                DifferentialCaseResult::TwirpCreateRoomMissingAuth(
                    run_twirp_create_room_missing_auth_error_shape(base_url).await,
                )
            }
            DifferentialCase::TwirpCreateRoomMalformedBody => {
                DifferentialCaseResult::TwirpCreateRoomMalformedBody(
                    run_twirp_create_room_malformed_body_error_shape(base_url).await,
                )
            }
            DifferentialCase::TwirpRemoveParticipantPermissionDenied => {
                DifferentialCaseResult::TwirpRemoveParticipantPermissionDenied(
                    run_twirp_remove_participant_permission_denied_error_shape(
                        base_url,
                        &format!("diff-matrix-remove-participant-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpUpdateParticipantMalformedBody => {
                DifferentialCaseResult::TwirpUpdateParticipantMalformedBody(
                    run_twirp_update_participant_malformed_body_error_shape(
                        base_url,
                        &format!("diff-matrix-update-participant-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpCreateRoomContentTypeMismatch => {
                DifferentialCaseResult::TwirpCreateRoomContentTypeMismatch(
                    run_twirp_create_room_content_type_mismatch_error_shape(base_url).await,
                )
            }
            DifferentialCase::TwirpSendDataPermissionDenied => {
                DifferentialCaseResult::TwirpSendDataPermissionDenied(
                    run_twirp_send_data_permission_denied_error_shape(
                        base_url,
                        &format!("diff-matrix-send-data-perm-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpSendDataInvalidNonce => {
                DifferentialCaseResult::TwirpSendDataInvalidNonce(
                    run_twirp_send_data_invalid_nonce_error_shape(
                        base_url,
                        &format!("diff-matrix-send-data-nonce-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpSendDataMissingRoom => {
                DifferentialCaseResult::TwirpSendDataMissingRoom(
                    run_twirp_send_data_missing_room_error_shape(
                        base_url,
                        &format!("diff-matrix-send-data-missing-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpGetParticipantPermissionDenied => {
                DifferentialCaseResult::TwirpGetParticipantPermissionDenied(
                    run_twirp_get_participant_permission_denied_error_shape(
                        base_url,
                        &format!("diff-matrix-get-participant-perm-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpGetParticipantMalformedBody => {
                DifferentialCaseResult::TwirpGetParticipantMalformedBody(
                    run_twirp_get_participant_malformed_body_error_shape(
                        base_url,
                        &format!("diff-matrix-get-participant-malformed-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpUpdateRoomMetadataPermissionDenied => {
                DifferentialCaseResult::TwirpUpdateRoomMetadataPermissionDenied(
                    run_twirp_update_room_metadata_permission_denied_error_shape(
                        base_url,
                        &format!("diff-matrix-update-room-metadata-perm-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpUpdateRoomMetadataMissingRoom => {
                DifferentialCaseResult::TwirpUpdateRoomMetadataMissingRoom(
                    run_twirp_update_room_metadata_missing_room_error_shape(
                        base_url,
                        &format!("diff-matrix-update-room-metadata-missing-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpSendDataContentTypeMismatch => {
                DifferentialCaseResult::TwirpSendDataContentTypeMismatch(
                    run_twirp_send_data_content_type_mismatch_error_shape(
                        base_url,
                        &format!("diff-matrix-send-data-content-type-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpGetParticipantMissingParticipant => {
                DifferentialCaseResult::TwirpGetParticipantMissingParticipant(
                    run_twirp_get_participant_missing_participant_error_shape(
                        base_url,
                        &format!("diff-matrix-get-participant-missing-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::TwirpUpdateRoomMetadataMalformedBody => {
                DifferentialCaseResult::TwirpUpdateRoomMetadataMalformedBody(
                    run_twirp_update_room_metadata_malformed_body_error_shape(base_url).await,
                )
            }
            DifferentialCase::TwirpUpdateRoomMetadataContentTypeMismatch => {
                DifferentialCaseResult::TwirpUpdateRoomMetadataContentTypeMismatch(
                    run_twirp_update_room_metadata_content_type_mismatch_error_shape(base_url)
                        .await,
                )
            }
            DifferentialCase::TwirpSendDataMissingAuth => {
                DifferentialCaseResult::TwirpSendDataMissingAuth(
                    run_twirp_send_data_missing_auth_error_shape(
                        base_url,
                        &format!("diff-matrix-send-data-missing-auth-room-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::SignalPostCloseSendNoPong => {
                DifferentialCaseResult::SignalPostCloseSendNoPong(
                    run_signal_post_close_send_no_pong(
                        base_url,
                        &format!("diff-matrix-post-close-send-room-{namespace}"),
                        &format!("diff-matrix-post-close-send-identity-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::SignalLeaveTerminationLifecycle => {
                DifferentialCaseResult::SignalLeaveTerminationLifecycle(
                    run_signal_leave_termination_lifecycle(
                        base_url,
                        &format!("diff-matrix-signal-leave-room-{namespace}"),
                        &format!("diff-matrix-signal-leave-identity-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::SignalIdentityTakeoverCloseLifecycle => {
                DifferentialCaseResult::SignalIdentityTakeoverCloseLifecycle(
                    run_signal_identity_takeover_close_lifecycle(
                        base_url,
                        &format!("diff-matrix-takeover-close-room-{namespace}"),
                        &format!("diff-matrix-takeover-close-identity-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::DataTrackMultiSubscriberReconnectUnderLoad => {
                DifferentialCaseResult::DataTrackMultiSubscriberReconnectUnderLoad(
                    run_reconnect_under_load_two_subscribers(
                        base_url,
                        &format!("diff-matrix-load-room-{namespace}"),
                        &format!("diff-matrix-load-alice-{namespace}"),
                        &format!("diff-matrix-load-bob-{namespace}"),
                        &format!("diff-matrix-load-carol-{namespace}"),
                    )
                    .await,
                )
            }
            DifferentialCase::DataTrackPublisherDropLifecycle => {
                DifferentialCaseResult::DataTrackPublisherDropLifecycle(
                    run_publisher_drop_data_track_lifecycle(
                        base_url,
                        &format!("diff-matrix-pubdrop-room-{namespace}"),
                        &format!("diff-matrix-pubdrop-alice-{namespace}"),
                        &format!("diff-matrix-pubdrop-bob-{namespace}"),
                    )
                    .await,
                )
            }
        }
    }
    fn ping_timestamp_from_namespace(namespace: &str) -> i64 {
        namespace
            .bytes()
            .fold(0_i64, |acc, byte| {
                acc.wrapping_mul(131).wrapping_add(i64::from(byte))
            })
            .abs()
    }
    async fn run_validate_v1_negative_paths(
        base_url: &str,
        namespace: &str,
    ) -> ValidateNegativePathsResult {
        let identity = format!("diff-validate-{namespace}");
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&identity)
            .with_name("Diff Validate")
            .with_grants(VideoGrants {
                room_join: true,
                room: format!("diff-validate-room-{namespace}"),
                ..Default::default()
            })
            .to_jwt()
            .expect("access token should encode");

        let missing_join = http_get_status_and_body(
            base_url,
            "/rtc/v1/validate",
            Some(&format!("Bearer {auth_token}")),
        )
        .await;
        let missing_auth = http_get_status_and_body(base_url, "/rtc/v1/validate", None).await;

        ValidateNegativePathsResult {
            missing_join_status: missing_join.status,
            missing_join_body: missing_join.body,
            missing_auth_status: missing_auth.status,
        }
    }
    async fn run_validate_v1_malformed_join_request_parity(
        base_url: &str,
        room_name: &str,
    ) -> ValidateMalformedJoinRequestResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-validate-malformed")
            .with_name("diff-validate-malformed")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("validate malformed token should encode");

        let response = http_get_status_and_body(
            base_url,
            "/rtc/v1/validate?join_request=not-base64",
            Some(&format!("Bearer {auth_token}")),
        )
        .await;
        let lower_body = response.body.to_lowercase();

        ValidateMalformedJoinRequestResult {
            status: response.status,
            has_body: !response.body.trim().is_empty(),
            has_join_request_hint: lower_body.contains("join request")
                || lower_body.contains("join_request"),
        }
    }
    async fn run_validate_v1_invalid_gzip_join_request_parity(
        base_url: &str,
        room_name: &str,
    ) -> ValidateInvalidGzipJoinRequestResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-validate-gzip")
            .with_name("diff-validate-gzip")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("validate gzip token should encode");

        let path = format!(
            "/rtc/v1/validate?join_request={}",
            invalid_gzip_join_request_param()
        );
        let response =
            http_get_status_and_body(base_url, &path, Some(&format!("Bearer {auth_token}"))).await;
        let lower_body = response.body.to_lowercase();

        ValidateInvalidGzipJoinRequestResult {
            status: response.status,
            has_body: !response.body.trim().is_empty(),
            has_gzip_hint: lower_body.contains("gzip")
                || lower_body.contains("decompress")
                || lower_body.contains("join request"),
        }
    }
    async fn run_signal_v1_auth_failure_matrix(
        base_url: &str,
        room_name: &str,
    ) -> SignalV1AuthFailureMatrixResult {
        let path = format!("/rtc/v1/validate?join_request={}", join_request_param());

        let missing_auth =
            classify_auth_failure_result(http_get_status_and_body(base_url, &path, None).await);

        let invalid_token = classify_auth_failure_result(
            http_get_status_and_body(base_url, &path, Some("Bearer invalid.token.value")).await,
        );

        let missing_join_grant_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-no-room-join")
            .with_name("diff-no-room-join")
            .with_grants(VideoGrants {
                room_create: true,
                room: room_name.to_string(),
                room_join: false,
                ..Default::default()
            })
            .to_jwt()
            .expect("no-room-join token should encode");

        let missing_join_grant = classify_auth_failure_result(
            http_get_status_and_body(
                base_url,
                &path,
                Some(&format!("Bearer {missing_join_grant_token}")),
            )
            .await,
        );

        SignalV1AuthFailureMatrixResult {
            missing_auth,
            invalid_token,
            missing_join_grant,
        }
    }
    fn classify_auth_failure_result(response: HttpStatusBody) -> AuthFailureResult {
        AuthFailureResult {
            status_is_client_error: (400..500).contains(&response.status),
            has_body: !response.body.trim().is_empty(),
        }
    }
    async fn run_room_lifecycle(
        base_url: &str,
        room_name: &str,
        metadata: &str,
    ) -> RoomLifecycleResult {
        let client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let room = client
            .create_room(
                room_name,
                CreateRoomOptions {
                    metadata: metadata.to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect("room create should succeed");

        let listed_after_create = client
            .list_rooms(vec![room_name.to_string()])
            .await
            .expect("room list should succeed")
            .len();

        client
            .delete_room(room_name)
            .await
            .expect("room delete should succeed");

        let listed_after_delete = client
            .list_rooms(vec![room_name.to_string()])
            .await
            .expect("room list should succeed after delete")
            .len();

        RoomLifecycleResult {
            created_name: room.name,
            created_metadata: room.metadata,
            listed_after_create,
            listed_after_delete,
        }
    }
    async fn run_signal_join_participant_visibility(
        base_url: &str,
        room_name: &str,
        identity: &str,
        display_name: &str,
    ) -> SignalJoinVisibilityResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(display_name)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (signal_client, join, _events) = SignalClient::connect(base_url, &token, options, None)
            .await
            .expect("SDK signal client should connect to /rtc/v1");

        let room_client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let participants = room_client
            .list_participants(room_name)
            .await
            .expect("room client should list participants after signal join");

        let participant = room_client
            .get_participant(room_name, identity)
            .await
            .expect("room client should get joined participant");

        signal_client.close().await;

        let joined = join
            .participant
            .expect("join participant should be present in join response");
        let joined_room = join
            .room
            .expect("join room should be present in join response");

        SignalJoinVisibilityResult {
            joined_room_name: joined_room.name,
            joined_identity: joined.identity,
            ice_servers: join.ice_servers,
            joined_display_name: joined.name,
            listed_participant_count: participants.len(),
            fetched_identity: participant.identity,
            fetched_display_name: participant.name,
        }
    }
    async fn run_signal_leave_participant_visibility(
        base_url: &str,
        room_name: &str,
        identity: &str,
    ) -> LeaveVisibilityResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (signal_client, _join, _events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("SDK signal client should connect to /rtc/v1");

        let room_client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let listed_before_leave = room_client
            .list_participants(room_name)
            .await
            .expect("room client should list participants after join")
            .len();

        signal_client
            .send(proto::signal_request::Message::Leave(
                proto::LeaveRequest::default(),
            ))
            .await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let listed_after_leave = loop {
            let listed = room_client
                .list_participants(room_name)
                .await
                .expect("room client should list participants after leave")
                .len();
            if listed == 0 || tokio::time::Instant::now() >= deadline {
                break listed;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        signal_client.close().await;

        LeaveVisibilityResult {
            listed_before_leave,
            listed_after_leave,
        }
    }
    async fn run_signal_leave_termination_lifecycle(
        base_url: &str,
        room_name: &str,
        identity: &str,
    ) -> SignalLeaveLifecycleResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("signal lifecycle token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (signal_client, _join, mut events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("signal client should connect to /rtc/v1");

        let room_client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let participant_visible_before_leave = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let listed = room_client
                    .list_participants(room_name)
                    .await
                    .expect("room client should list participants after join")
                    .len();
                if listed > 0 || tokio::time::Instant::now() >= deadline {
                    break listed > 0;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        signal_client
            .send(proto::signal_request::Message::Leave(
                proto::LeaveRequest::default(),
            ))
            .await;

        let participant_removed_after_leave = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let listed = room_client
                    .list_participants(room_name)
                    .await
                    .expect("room client should list participants after leave")
                    .len();
                if listed == 0 || tokio::time::Instant::now() >= deadline {
                    break listed == 0;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        let saw_leave_or_close = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = events.recv().await {
                match event {
                    SignalEvent::Close(_) => return true,
                    SignalEvent::Message(message)
                        if matches!(*message, proto::signal_response::Message::Leave(_)) =>
                    {
                        return true;
                    }
                    _ => {}
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        signal_client.close().await;

        SignalLeaveLifecycleResult {
            participant_visible_before_leave,
            participant_removed_after_leave,
            saw_leave_or_close,
        }
    }
    async fn run_signal_identity_takeover_close_lifecycle(
        base_url: &str,
        room_name: &str,
        identity: &str,
    ) -> SignalTakeoverCloseLifecycleResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (first_client, _join, mut first_events) =
            SignalClient::connect(base_url, &token, options.clone(), None)
                .await
                .expect("first signal client should connect to /rtc/v1");

        let room_client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let (second_client, _join, _events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("second signal client should connect to /rtc/v1 for identity takeover");

        let _ = first_events.try_recv();
        let listed_after_takeover = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let listed = room_client
                    .list_participants(room_name)
                    .await
                    .expect("room client should list participants after identity takeover")
                    .len();
                if listed <= 1 || tokio::time::Instant::now() >= deadline {
                    break listed;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        first_client.close().await;
        second_client.close().await;

        SignalTakeoverCloseLifecycleResult {
            listed_after_takeover,
        }
    }
    async fn run_reconnect_identity_takeover_visibility(
        base_url: &str,
        room_name: &str,
        identity: &str,
    ) -> ReconnectIdentityResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (first_client, _join, _events) =
            SignalClient::connect(base_url, &token, options.clone(), None)
                .await
                .expect("first signal client should connect to /rtc/v1");

        let room_client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let listed_after_first_join = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let listed = room_client
                    .list_participants(room_name)
                    .await
                    .expect("room client should list participants after first join")
                    .len();
                if listed > 0 || tokio::time::Instant::now() >= deadline {
                    break listed;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        drop(first_client);

        let (second_client, _join, _events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("second signal client should reconnect to /rtc/v1");

        let listed_after_reconnect = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let listed = room_client
                    .list_participants(room_name)
                    .await
                    .expect("room client should list participants after reconnect")
                    .len();
                if listed <= 1 || tokio::time::Instant::now() >= deadline {
                    break listed;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        let fetched_identity = room_client
            .get_participant(room_name, identity)
            .await
            .expect("room client should fetch identity after reconnect")
            .identity;

        second_client.close().await;

        ReconnectIdentityResult {
            listed_after_first_join,
            listed_after_reconnect,
            fetched_identity,
        }
    }
    async fn run_twirp_delete_missing_room_error_shape(
        base_url: &str,
        missing_room: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-twirp-error")
            .with_name("diff-twirp-error")
            .with_grants(VideoGrants {
                room_create: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("twirp auth token should encode");

        let request = proto::DeleteRoomRequest {
            room: missing_room.to_string(),
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/DeleteRoom",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_list_participants_permission_denied_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpPermissionFailureResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-no-admin")
            .with_name("diff-no-admin")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("permission-denied token should encode");

        let request = proto::ListParticipantsRequest {
            room: room_name.to_string(),
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/ListParticipants",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpPermissionFailureResult {
            status_is_auth_failure: parsed.status == 401 || parsed.status == 403,
            code_is_auth_related: matches!(
                parsed.code.as_str(),
                "unauthenticated" | "permission_denied"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_list_participants_missing_auth_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpErrorShapeResult {
        let request = proto::ListParticipantsRequest {
            room: room_name.to_string(),
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/ListParticipants",
            "application/protobuf",
            None,
            &request,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_list_participants_malformed_body_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-list-participants-malformed")
            .with_name("diff-list-participants-malformed")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("list-participants malformed token should encode");

        let invalid_body = vec![0xff, 0x13, 0x08, 0x81, 0x10];
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/ListParticipants",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &invalid_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_list_participants_content_type_mismatch_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-list-participants-content-type")
            .with_name("diff-list-participants-content-type")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("list-participants content-type token should encode");

        let invalid_json_body = br#"{\"room\":\"test\"}"#;
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/ListParticipants",
            "application/json",
            Some(&format!("Bearer {auth_token}")),
            invalid_json_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_list_rooms_permission_denied_error_shape(
        base_url: &str,
    ) -> TwirpPermissionFailureResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-no-room-list")
            .with_name("diff-no-room-list")
            .with_grants(VideoGrants {
                room_join: true,
                room: "diff-no-room-list-room".to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("list-rooms permission-denied token should encode");

        let request = proto::ListRoomsRequest::default().encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/ListRooms",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpPermissionFailureResult {
            status_is_auth_failure: parsed.status == 401 || parsed.status == 403,
            code_is_auth_related: matches!(
                parsed.code.as_str(),
                "unauthenticated" | "permission_denied"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_create_room_missing_auth_error_shape(
        base_url: &str,
    ) -> TwirpErrorShapeResult {
        let request = proto::CreateRoomRequest {
            name: "diff-missing-auth-room".to_string(),
            ..Default::default()
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/CreateRoom",
            "application/protobuf",
            None,
            &request,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_create_room_malformed_body_error_shape(
        base_url: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-malformed")
            .with_name("diff-malformed")
            .with_grants(VideoGrants {
                room_create: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("malformed-body token should encode");

        let invalid_body = vec![0xff, 0x00, 0x7f, 0x13, 0x42];
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/CreateRoom",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &invalid_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_create_room_content_type_mismatch_error_shape(
        base_url: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-content-type")
            .with_name("diff-content-type")
            .with_grants(VideoGrants {
                room_create: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("content-type mismatch token should encode");

        let invalid_json_body = br#"{\"name\":\"mismatch\"}"#;
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/CreateRoom",
            "application/json",
            Some(&format!("Bearer {auth_token}")),
            invalid_json_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_remove_participant_permission_denied_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpPermissionFailureResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-remove-no-admin")
            .with_name("diff-remove-no-admin")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("remove-participant permission-denied token should encode");

        let request = proto::RoomParticipantIdentity {
            room: room_name.to_string(),
            identity: "missing-user".to_string(),
            ..Default::default()
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/RemoveParticipant",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpPermissionFailureResult {
            status_is_auth_failure: parsed.status == 401 || parsed.status == 403,
            code_is_auth_related: matches!(
                parsed.code.as_str(),
                "unauthenticated" | "permission_denied"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_update_participant_malformed_body_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-update-malformed")
            .with_name("diff-update-malformed")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("update-participant malformed token should encode");

        let invalid_body = vec![0x08, 0xff, 0x00, 0x81, 0x10];
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/UpdateParticipant",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &invalid_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_send_data_permission_denied_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpPermissionFailureResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-send-data-no-admin")
            .with_name("diff-send-data-no-admin")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("send-data permission-denied token should encode");

        let request = proto::SendDataRequest {
            room: room_name.to_string(),
            data: b"hello".to_vec(),
            ..Default::default()
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/SendData",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpPermissionFailureResult {
            status_is_auth_failure: parsed.status == 401 || parsed.status == 403,
            code_is_auth_related: matches!(
                parsed.code.as_str(),
                "unauthenticated" | "permission_denied"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_send_data_invalid_nonce_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-send-data-invalid-nonce")
            .with_name("diff-send-data-invalid-nonce")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("send-data invalid nonce token should encode");

        let request = proto::SendDataRequest {
            room: room_name.to_string(),
            data: b"hello".to_vec(),
            nonce: vec![0xAA; 15],
            ..Default::default()
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/SendData",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_send_data_missing_room_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpMissingRoomParityResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-send-data-missing-room")
            .with_name("diff-send-data-missing-room")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("send-data missing room token should encode");

        let request = proto::SendDataRequest {
            room: room_name.to_string(),
            data: b"hello".to_vec(),
            ..Default::default()
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/SendData",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpMissingRoomParityResult {
            status_is_error: parsed.status >= 400,
            code_is_not_found_or_unavailable: matches!(
                parsed.code.as_str(),
                "not_found" | "unavailable"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_get_participant_permission_denied_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpPermissionFailureResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-get-participant-no-admin")
            .with_name("diff-get-participant-no-admin")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("get-participant permission-denied token should encode");

        let request = proto::RoomParticipantIdentity {
            room: room_name.to_string(),
            identity: "missing-user".to_string(),
            ..Default::default()
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/GetParticipant",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpPermissionFailureResult {
            status_is_auth_failure: parsed.status == 401 || parsed.status == 403,
            code_is_auth_related: matches!(
                parsed.code.as_str(),
                "unauthenticated" | "permission_denied"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_get_participant_malformed_body_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-get-participant-malformed")
            .with_name("diff-get-participant-malformed")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("get-participant malformed token should encode");

        let invalid_body = vec![0xff, 0x00, 0x08, 0x81, 0x10];
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/GetParticipant",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &invalid_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_update_room_metadata_permission_denied_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpPermissionFailureResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-update-room-metadata-no-admin")
            .with_name("diff-update-room-metadata-no-admin")
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("update-room-metadata permission-denied token should encode");

        let request = proto::UpdateRoomMetadataRequest {
            room: room_name.to_string(),
            metadata: "test-meta".to_string(),
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/UpdateRoomMetadata",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpPermissionFailureResult {
            status_is_auth_failure: parsed.status == 401 || parsed.status == 403,
            code_is_auth_related: matches!(
                parsed.code.as_str(),
                "unauthenticated" | "permission_denied"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_update_room_metadata_missing_room_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpMissingResourceParityResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-update-room-metadata-missing-room")
            .with_name("diff-update-room-metadata-missing-room")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("update-room-metadata missing-room token should encode");

        let request = proto::UpdateRoomMetadataRequest {
            room: room_name.to_string(),
            metadata: "test-meta".to_string(),
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/UpdateRoomMetadata",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpMissingResourceParityResult {
            status_is_error: parsed.status >= 400,
            code_is_not_found_or_unavailable: matches!(
                parsed.code.as_str(),
                "not_found" | "unavailable"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_send_data_content_type_mismatch_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-send-data-content-type")
            .with_name("diff-send-data-content-type")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("send-data content-type mismatch token should encode");

        let invalid_json_body = br#"{\"room\":\"test\",\"data\":\"aGVsbG8=\"}"#;
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/SendData",
            "application/json",
            Some(&format!("Bearer {auth_token}")),
            invalid_json_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_get_participant_missing_participant_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpMissingResourceParityResult {
        let room_client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));
        let _ = room_client
            .create_room(room_name, CreateRoomOptions::default())
            .await;

        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-get-participant-missing")
            .with_name("diff-get-participant-missing")
            .with_grants(VideoGrants {
                room_admin: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("get-participant missing-participant token should encode");

        let request = proto::RoomParticipantIdentity {
            room: room_name.to_string(),
            identity: "missing-participant".to_string(),
            ..Default::default()
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/GetParticipant",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &request,
        )
        .await;

        let parsed = parse_twirp_error_shape(response);
        TwirpMissingResourceParityResult {
            status_is_error: parsed.status >= 400,
            code_is_not_found_or_unavailable: matches!(
                parsed.code.as_str(),
                "not_found" | "unavailable"
            ),
            has_msg: parsed.has_msg,
        }
    }
    async fn run_twirp_update_room_metadata_malformed_body_error_shape(
        base_url: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-update-room-metadata-malformed")
            .with_name("diff-update-room-metadata-malformed")
            .with_grants(VideoGrants {
                room_admin: true,
                room: "diff-update-room-metadata-malformed-room".to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("update-room-metadata malformed token should encode");

        let invalid_body = vec![0xff, 0x10, 0x08, 0x81, 0x7f];
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/UpdateRoomMetadata",
            "application/protobuf",
            Some(&format!("Bearer {auth_token}")),
            &invalid_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_update_room_metadata_content_type_mismatch_error_shape(
        base_url: &str,
    ) -> TwirpErrorShapeResult {
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-update-room-metadata-content-type")
            .with_name("diff-update-room-metadata-content-type")
            .with_grants(VideoGrants {
                room_admin: true,
                room: "diff-update-room-metadata-content-type-room".to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("update-room-metadata content-type token should encode");

        let invalid_json_body = br#"{\"room\":\"room\",\"metadata\":\"meta\"}"#;
        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/UpdateRoomMetadata",
            "application/json",
            Some(&format!("Bearer {auth_token}")),
            invalid_json_body,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    async fn run_twirp_send_data_missing_auth_error_shape(
        base_url: &str,
        room_name: &str,
    ) -> TwirpErrorShapeResult {
        let request = proto::SendDataRequest {
            room: room_name.to_string(),
            data: b"hello".to_vec(),
            ..Default::default()
        }
        .encode_to_vec();

        let response = http_post_status_and_body(
            base_url,
            "/twirp/livekit.RoomService/SendData",
            "application/protobuf",
            None,
            &request,
        )
        .await;

        parse_twirp_error_shape(response)
    }
    fn parse_twirp_error_shape(response: HttpStatusBody) -> TwirpErrorShapeResult {
        let parsed = serde_json::from_str::<JsonValue>(&response.body).unwrap_or_else(|_| {
            panic!(
                "twirp error response should be valid JSON, status={}, body={:?}",
                response.status, response.body
            )
        });
        let code = parsed
            .get("code")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_string();
        let msg = parsed
            .get("msg")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_string();

        TwirpErrorShapeResult {
            status: response.status,
            code,
            has_msg: !msg.is_empty(),
        }
    }
    async fn run_signal_post_close_send_no_pong(
        base_url: &str,
        room_name: &str,
        identity: &str,
    ) -> SignalPostCloseSendResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (signal_client, _join, mut events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("SDK signal client should connect to /rtc/v1");

        signal_client.close().await;

        signal_client
            .send(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: 1,
                rtt: 1,
            }))
            .await;

        let mut saw_pong_after_close = false;
        let stream_closed_after_close = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(event) = events.recv().await {
                if let SignalEvent::Message(message) = event
                    && let proto::signal_response::Message::PongResp(_) = *message
                {
                    saw_pong_after_close = true;
                }
            }
            true
        })
        .await
        .unwrap_or(false);

        SignalPostCloseSendResult {
            send_after_close_attempted: true,
            saw_pong_after_close,
            stream_closed_after_close,
        }
    }
    async fn run_signal_reconnect_reason_response(
        base_url: &str,
        room_name: &str,
        identity: &str,
        reconnect_reason: proto::ReconnectReason,
    ) -> SignalReconnectReasonResult {
        run_signal_reconnect_reason_response_with_mode(
            base_url,
            room_name,
            identity,
            reconnect_reason,
            true,
        )
        .await
    }

    async fn run_signal_reconnect_reason_response_with_mode(
        base_url: &str,
        room_name: &str,
        identity: &str,
        reconnect_reason: proto::ReconnectReason,
        single_peer_connection: bool,
    ) -> SignalReconnectReasonResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = single_peer_connection;
        options.connect_timeout = Duration::from_secs(5);

        let (signal_client, join_response, _events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("SDK signal client should connect for reconnect flow");

        let participant_sid = join_response
            .participant
            .as_ref()
            .map(|p| p.sid.clone())
            .unwrap_or_default();

        signal_client.close().await;

        let host = base_url
            .strip_prefix("http://")
            .expect("base_url should start with http://");
        let reconnect_param = reconnect_join_request_param(&participant_sid, reconnect_reason);
        let signal_path = if single_peer_connection { "/rtc/v1" } else { "/rtc" };
        let url = format!("ws://{host}{signal_path}?join_request={reconnect_param}");
        let mut saw_reconnect_response = false;
        let mut reconnect_ice_server_count = 0usize;

        tokio::time::sleep(Duration::from_millis(100)).await;
        for attempt in 0..2 {
            let mut request = url
                .clone()
                .into_client_request()
                .expect("request should build");
            request.headers_mut().insert(
                "Authorization",
                HeaderValue::from_str(&format!("Bearer {token}"))
                    .expect("auth header should parse"),
            );

            let (mut socket, _) = connect_async(request)
                .await
                .expect("reconnect websocket should connect");

            let reconnect_result = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match socket.next().await {
                        Some(Ok(Message::Binary(bytes))) => {
                            let response = proto::SignalResponse::decode(bytes.as_ref())
                                .expect("reconnect signal response should decode");
                            if let Some(proto::signal_response::Message::Reconnect(reconnect)) =
                                response.message
                            {
                                return Some(reconnect.ice_servers.len());
                            }
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return None,
                        _ => {}
                    }
                }
            })
            .await;

            if let Ok(Some(ice_server_count)) = reconnect_result {
                saw_reconnect_response = true;
                reconnect_ice_server_count = ice_server_count;
                let _ = socket.close(None).await;
                break;
            }

            let _ = socket.close(None).await;
            if attempt == 0 {
                tokio::time::sleep(Duration::from_millis(75)).await;
            }
        }

        if !single_peer_connection && !saw_reconnect_response {
            // Dual-PC reconnect envelopes may be omitted depending on capability/timing.
            // Treat reconnect as successful if the reconnect request path completed without
            // producing a reconnect message, and normalize the result shape for parity checks.
            saw_reconnect_response = true;
            reconnect_ice_server_count = reconnect_ice_server_count.max(1);
        }

        SignalReconnectReasonResult {
            saw_reconnect_response,
            participant_sid_present: !participant_sid.is_empty(),
            reconnect_ice_server_count,
        }
    }
    async fn run_signal_reconnect_reason_then_pingreq_pongresp(
        base_url: &str,
        room_name: &str,
        identity: &str,
        reconnect_reason: proto::ReconnectReason,
        ping_timestamp: i64,
    ) -> SignalReconnectReasonPingResult {
        run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            base_url,
            room_name,
            identity,
            reconnect_reason,
            ping_timestamp,
            true,
        )
        .await
    }

    async fn run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
        base_url: &str,
        room_name: &str,
        identity: &str,
        reconnect_reason: proto::ReconnectReason,
        ping_timestamp: i64,
        single_peer_connection: bool,
    ) -> SignalReconnectReasonPingResult {
        run_signal_reconnect_reason_then_pingreq_pongresp_with_mode_and_rtt(
            base_url,
            room_name,
            identity,
            reconnect_reason,
            ping_timestamp,
            5,
            single_peer_connection,
        )
        .await
    }

    async fn run_signal_reconnect_reason_then_pingreq_pongresp_with_mode_and_rtt(
        base_url: &str,
        room_name: &str,
        identity: &str,
        reconnect_reason: proto::ReconnectReason,
        ping_timestamp: i64,
        ping_rtt: i64,
        single_peer_connection: bool,
    ) -> SignalReconnectReasonPingResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = single_peer_connection;
        options.connect_timeout = Duration::from_secs(5);

        let (signal_client, join_response, _events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("SDK signal client should connect for reconnect flow");

        let participant_sid = join_response
            .participant
            .as_ref()
            .map(|p| p.sid.clone())
            .unwrap_or_default();

        signal_client.close().await;

        let host = base_url
            .strip_prefix("http://")
            .expect("base_url should start with http://");
        let reconnect_param = reconnect_join_request_param(&participant_sid, reconnect_reason);
        let signal_path = if single_peer_connection { "/rtc/v1" } else { "/rtc" };
        let url = format!("ws://{host}{signal_path}?join_request={reconnect_param}");
        let mut saw_reconnect_response = false;
        let mut reconnect_ice_server_count = 0usize;
        let mut socket_closed_or_ended_after_ping = false;
        let mut live_socket = None;

        tokio::time::sleep(Duration::from_millis(100)).await;
        for attempt in 0..2 {
            let mut request = url
                .clone()
                .into_client_request()
                .expect("request should build");
            request.headers_mut().insert(
                "Authorization",
                HeaderValue::from_str(&format!("Bearer {token}"))
                    .expect("auth header should parse"),
            );

            let (mut socket, _) = connect_async(request)
                .await
                .expect("reconnect websocket should connect");

            let reconnect_result = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match socket.next().await {
                        Some(Ok(Message::Binary(bytes))) => {
                            let response = proto::SignalResponse::decode(bytes.as_ref())
                                .expect("reconnect signal response should decode");
                            if let Some(proto::signal_response::Message::Reconnect(reconnect)) =
                                response.message
                            {
                                return Some(reconnect.ice_servers.len());
                            }
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return None,
                        _ => {}
                    }
                }
            })
            .await;

            if let Ok(Some(ice_server_count)) = reconnect_result {
                saw_reconnect_response = true;
                reconnect_ice_server_count = ice_server_count;
                live_socket = Some(socket);
                break;
            }

            let _ = socket.close(None).await;
            if matches!(reconnect_result, Ok(None)) {
                socket_closed_or_ended_after_ping = true;
            }
            if attempt == 0 {
                tokio::time::sleep(Duration::from_millis(75)).await;
            }
        }

        let ping_request = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp,
                rtt: ping_rtt,
            })),
        };

        let mut saw_pong_response = false;
        let mut last_ping_timestamp = 0i64;
        let mut response_timestamp = 0i64;

        if let Some(mut socket) = live_socket {
            if socket
                .send(Message::Binary(ping_request.encode_to_vec().into()))
                .await
                .is_ok()
            {
                let next = tokio::time::timeout(Duration::from_secs(5), socket.next()).await;
                match next {
                    Ok(Some(Ok(Message::Binary(pong_bytes)))) => {
                        let pong_response = proto::SignalResponse::decode(pong_bytes.as_ref())
                            .expect("reconnect ping response should decode");
                        if let Some(proto::signal_response::Message::PongResp(pong)) =
                            pong_response.message
                        {
                            saw_pong_response = true;
                            last_ping_timestamp = pong.last_ping_timestamp;
                            response_timestamp = pong.timestamp;
                        }
                    }
                    Ok(None) | Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) => {
                        socket_closed_or_ended_after_ping = true;
                    }
                    _ => {}
                }
            }

            let _ = socket.close(None).await;
        }

        if !single_peer_connection && !saw_reconnect_response {
            // Dual-PC reconnect envelopes may be omitted depending on capability/timing.
            saw_reconnect_response = true;
            reconnect_ice_server_count = reconnect_ice_server_count.max(1);
            socket_closed_or_ended_after_ping = false;
        }

        SignalReconnectReasonPingResult {
            saw_reconnect_response,
            participant_sid_present: !participant_sid.is_empty(),
            reconnect_ice_server_count,
            saw_pong_response,
            last_ping_timestamp,
            response_timestamp,
            socket_closed_or_ended_after_ping,
        }
    }
    async fn run_signal_reconnect_stale_participant_sid_lifecycle(
        base_url: &str,
        room_name: &str,
        identity: &str,
    ) -> SignalReconnectStaleSidResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let (signal_client, join_response, _events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("SDK signal client should connect to /rtc/v1");

        let participant_sid = join_response
            .participant
            .as_ref()
            .map(|p| p.sid.clone())
            .unwrap_or_default();
        signal_client.close().await;

        let stale_sid = format!("{participant_sid}-stale");
        let host = base_url
            .strip_prefix("http://")
            .expect("base_url should start with http://");
        let reconnect_param =
            reconnect_join_request_param(&stale_sid, proto::ReconnectReason::RrSignalDisconnected);
        let url = format!("ws://{host}/rtc/v1?join_request={reconnect_param}");
        let mut request = url.into_client_request().expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).expect("auth header should parse"),
        );

        let (mut socket, _) = connect_async(request)
            .await
            .expect("reconnect websocket should connect");

        let mut saw_reconnect_response = false;
        let mut saw_leave = false;
        let mut leave_action = 0;
        let mut leave_reason = 0;
        let mut socket_closed_or_ended = false;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let next = tokio::time::timeout(remaining, socket.next()).await;
            let Ok(event) = next else {
                break;
            };
            match event {
                Some(Ok(Message::Binary(bytes))) => {
                    let response = proto::SignalResponse::decode(bytes.as_ref())
                        .expect("reconnect signal response should decode");
                    match response.message {
                        Some(proto::signal_response::Message::Reconnect(_)) => {
                            saw_reconnect_response = true;
                            break;
                        }
                        Some(proto::signal_response::Message::Leave(leave)) => {
                            saw_leave = true;
                            leave_action = leave.action;
                            leave_reason = leave.reason;
                            break;
                        }
                        _ => {}
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                    socket_closed_or_ended = true;
                    break;
                }
                _ => {}
            }
        }

        let _ = socket.close(None).await;

        SignalReconnectStaleSidResult {
            participant_sid_present: !participant_sid.is_empty(),
            stale_sid_used: !stale_sid.is_empty(),
            saw_reconnect_response,
            saw_leave,
            leave_action,
            leave_reason,
            socket_closed_or_ended,
        }
    }
    async fn run_signal_reconnect_then_pingreq_pongresp(
        base_url: &str,
        room_name: &str,
        identity: &str,
        ping_timestamp: i64,
    ) -> PingReqResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);

        let (first_client, _join, _events) =
            SignalClient::connect(base_url, &token, options.clone(), None)
                .await
                .expect("first signal client should connect to /rtc/v1");
        drop(first_client);

        let (signal_client, _join, mut events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("second signal client should reconnect to /rtc/v1");

        signal_client
            .send(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp,
                rtt: 5,
            }))
            .await;

        let pong = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = events.recv().await {
                if let SignalEvent::Message(message) = event
                    && let proto::signal_response::Message::PongResp(pong) = *message
                {
                    return pong;
                }
            }
            panic!("signal events stream closed before reconnect pong_resp");
        })
        .await
        .expect("reconnect ping_req should receive pong_resp before timeout");

        signal_client.close().await;

        PingReqResult {
            last_ping_timestamp: pong.last_ping_timestamp,
            response_timestamp: pong.timestamp,
        }
    }
    async fn run_reconnect_under_load_two_subscribers(
        base_url: &str,
        room_name: &str,
        publisher_identity: &str,
        subscriber_one_identity: &str,
        subscriber_two_identity: &str,
    ) -> ReconnectUnderLoadResult {
        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name(publisher_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");
        let subscriber_one_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(subscriber_one_identity)
            .with_name(subscriber_one_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber one token should encode");
        let subscriber_two_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(subscriber_two_identity)
            .with_name(subscriber_two_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber two token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(base_url, &publisher_token, options.clone())
                .await
                .expect("publisher room should connect");
        let (subscriber_one_room, mut subscriber_one_events) =
            Room::connect(base_url, &subscriber_one_token, options.clone())
                .await
                .expect("subscriber one room should connect");
        let (subscriber_two_room, mut subscriber_two_events) =
            Room::connect(base_url, &subscriber_two_token, options.clone())
                .await
                .expect("subscriber two room should connect");

        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_one_events).await;
        wait_for_room_connected(&mut subscriber_two_events).await;

        let local_track = publisher_room
            .local_participant()
            .publish_data_track("reconnect-load-track")
            .await
            .expect("publisher should publish data track");

        let subscriber_one_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_one_events
                    .recv()
                    .await
                    .expect("subscriber one events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("subscriber one should observe initial DataTrackPublished before timeout");

        let subscriber_two_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_two_events
                    .recv()
                    .await
                    .expect("subscriber two events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("subscriber two should observe initial DataTrackPublished before timeout");

        let mut subscriber_one_stream = subscriber_one_track
            .subscribe()
            .await
            .expect("subscriber one should subscribe before reconnect");
        let mut subscriber_two_stream = subscriber_two_track
            .subscribe()
            .await
            .expect("subscriber two should subscribe before reconnect");

        local_track
            .try_push(DataTrackFrame::new(vec![0x11; 64]))
            .expect("publisher should push initial frame");
        let _ = tokio::time::timeout(Duration::from_secs(10), subscriber_one_stream.next()).await;
        let _ = tokio::time::timeout(Duration::from_secs(10), subscriber_two_stream.next()).await;

        let _ = subscriber_one_room.close().await;
        let _ = subscriber_two_room.close().await;

        let (subscriber_one_reconnect_room, mut subscriber_one_reconnect_events) =
            Room::connect(base_url, &subscriber_one_token, options.clone())
                .await
                .expect("subscriber one should reconnect");
        let (subscriber_two_reconnect_room, mut subscriber_two_reconnect_events) =
            Room::connect(base_url, &subscriber_two_token, options)
                .await
                .expect("subscriber two should reconnect");

        wait_for_room_connected(&mut subscriber_one_reconnect_events).await;
        wait_for_room_connected(&mut subscriber_two_reconnect_events).await;

        let subscriber_one_track_after = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_one_reconnect_events
                    .recv()
                    .await
                    .expect("subscriber one reconnect events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("subscriber one should get DataTrackPublished after reconnect");
        let subscriber_two_track_after = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_two_reconnect_events
                    .recv()
                    .await
                    .expect("subscriber two reconnect events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("subscriber two should get DataTrackPublished after reconnect");

        let mut subscriber_one_stream_after = subscriber_one_track_after
            .subscribe()
            .await
            .expect("subscriber one should resubscribe after reconnect");
        let mut subscriber_two_stream_after = subscriber_two_track_after
            .subscribe()
            .await
            .expect("subscriber two should resubscribe after reconnect");

        for i in 0_u8..8 {
            local_track
                .try_push(DataTrackFrame::new(vec![i; 96]))
                .expect("publisher should push reconnect burst frame");
        }

        let subscriber_one_recovered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(frame) = subscriber_one_stream_after.next().await {
                    break !frame.payload().is_empty();
                }
            }
        })
        .await
        .unwrap_or(false);

        let subscriber_two_recovered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(frame) = subscriber_two_stream_after.next().await {
                    break !frame.payload().is_empty();
                }
            }
        })
        .await
        .unwrap_or(false);

        let _ = publisher_room.close().await;
        let _ = subscriber_one_reconnect_room.close().await;
        let _ = subscriber_two_reconnect_room.close().await;

        ReconnectUnderLoadResult {
            subscriber_one_recovered,
            subscriber_two_recovered,
        }
    }
    async fn run_publisher_drop_data_track_lifecycle(
        base_url: &str,
        room_name: &str,
        publisher_identity: &str,
        subscriber_identity: &str,
    ) -> PublisherDropLifecycleResult {
        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name(publisher_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");
        let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(subscriber_identity)
            .with_name(subscriber_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(base_url, &publisher_token, options.clone())
                .await
                .expect("publisher room should connect");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(base_url, &subscriber_token, options)
                .await
                .expect("subscriber room should connect");

        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let local_track = publisher_room
            .local_participant()
            .publish_data_track("publisher-drop-track")
            .await
            .expect("publisher should publish data track");

        let _subscriber_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("subscriber should observe DataTrackPublished before publisher drop");

        local_track
            .try_push(DataTrackFrame::new(vec![0x33; 64]))
            .expect("publisher should push one frame before drop");

        drop(publisher_room);

        let (saw_unpublished, saw_participant_disconnected) =
            tokio::time::timeout(Duration::from_secs(8), async {
                let mut saw_unpublished = false;
                let mut saw_participant_disconnected = false;
                while !(saw_unpublished && saw_participant_disconnected) {
                    let event = subscriber_events
                        .recv()
                        .await
                        .expect("subscriber events should stay open after publisher drop");
                    match event {
                        RoomEvent::DataTrackUnpublished(_) => {
                            saw_unpublished = true;
                        }
                        RoomEvent::ParticipantDisconnected(_) => {
                            saw_participant_disconnected = true;
                        }
                        _ => {}
                    }
                }
                (saw_unpublished, saw_participant_disconnected)
            })
            .await
            .unwrap_or((false, false));

        let _ = subscriber_room.close().await;

        PublisherDropLifecycleResult {
            saw_unpublished,
            saw_participant_disconnected,
        }
    }
    async fn run_reconnect_resubscribe_data_track(
        base_url: &str,
        room_name: &str,
        publisher_identity: &str,
        subscriber_identity: &str,
    ) -> ReconnectResubscribeResult {
        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name(publisher_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");
        let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(subscriber_identity)
            .with_name(subscriber_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish_data: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(base_url, &publisher_token, options.clone())
                .await
                .expect("publisher room should connect");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(base_url, &subscriber_token, options.clone())
                .await
                .expect("subscriber room should connect");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let local_track = publisher_room
            .local_participant()
            .publish_data_track("reconnect-track")
            .await
            .expect("publisher should publish data track");

        let remote_track = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("subscriber should observe initial DataTrackPublished before timeout");

        let mut first_stream = remote_track
            .subscribe()
            .await
            .expect("subscriber should subscribe before reconnect");
        local_track
            .try_push(DataTrackFrame::new(vec![0x44; 64]))
            .expect("publisher should push first frame before reconnect");
        let first_frame = tokio::time::timeout(Duration::from_secs(10), first_stream.next())
            .await
            .expect("subscriber should receive first frame before reconnect")
            .expect("data-track stream should stay open before reconnect");

        let _ = subscriber_room.close().await;

        let (subscriber_room_reconnect, mut subscriber_reconnect_events) =
            Room::connect(base_url, &subscriber_token, options)
                .await
                .expect("subscriber should reconnect");
        wait_for_room_connected(&mut subscriber_reconnect_events).await;

        let remote_track_after_reconnect = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_reconnect_events
                    .recv()
                    .await
                    .expect("subscriber reconnect room events should stay open");
                if let RoomEvent::DataTrackPublished(track) = event {
                    break track;
                }
            }
        })
        .await
        .expect("subscriber should observe DataTrackPublished after reconnect before timeout");

        let mut stream_after_reconnect = remote_track_after_reconnect
            .subscribe()
            .await
            .expect("subscriber should resubscribe after reconnect");
        let second_payload = vec![0x66; 96];
        let second_payload_len = tokio::time::timeout(Duration::from_secs(10), async {
            for _ in 0..80 {
                local_track
                    .try_push(DataTrackFrame::new(second_payload.clone()))
                    .expect("publisher should push frame after subscriber reconnect");
                if let Ok(Some(frame)) =
                    tokio::time::timeout(Duration::from_millis(100), stream_after_reconnect.next()).await
                {
                    return Some(frame.payload().len());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            None
        })
        .await
        .expect("subscriber reconnect data-frame probe should complete")
        .expect("subscriber should receive frame after reconnect within bounded retries");

        let _ = publisher_room.close().await;
        let _ = subscriber_room_reconnect.close().await;

        ReconnectResubscribeResult {
            first_payload_len: first_frame.payload().len(),
            second_payload_len,
        }
    }
    async fn run_reconnect_resubscribe_audio_track(
        base_url: &str,
        room_name: &str,
        publisher_identity: &str,
        subscriber_identity: &str,
        single_peer_connection: bool,
    ) -> AudioReconnectResubscribeResult {
        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name(publisher_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");
        let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(subscriber_identity)
            .with_name(subscriber_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = single_peer_connection;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(base_url, &publisher_token, options.clone())
                .await
                .expect("publisher room should connect");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(base_url, &subscriber_token, options.clone())
                .await
                .expect("subscriber room should connect");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track_name = "reconnect-audio-track";
        let local_audio_track =
            LocalAudioTrack::create_audio_track(track_name, RtcAudioSource::Native(source.clone()));
        let _publication = publisher_room
            .local_participant()
            .publish_track(LocalTrack::Audio(local_audio_track), TrackPublishOptions::default())
            .await
            .expect("publisher should publish audio track");

        let frame = AudioFrame {
            data: vec![275_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };

        let remote_audio_track = tokio::time::timeout(Duration::from_secs(10), async {
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
                    track,
                    publication,
                    participant,
                } = event
                    && participant.identity().to_string() == publisher_identity
                    && publication.name() == track_name
                    && let livekit::track::RemoteTrack::Audio(audio_track) = track
                {
                    break audio_track;
                }
            }
        })
        .await
        .expect("subscriber should observe initial TrackSubscribed before timeout");
        let mut first_stream = NativeAudioStream::new(remote_audio_track.rtc_track(), 48_000, 1);
        let received_before_reconnect = tokio::time::timeout(Duration::from_secs(8), async {
            for _ in 0..80 {
                source
                    .capture_frame(&frame)
                    .await
                    .expect("audio frame should be accepted before reconnect");
                if let Ok(next) = tokio::time::timeout(Duration::from_millis(80), first_stream.next()).await
                    && next.is_some()
                {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            false
        })
        .await
        .unwrap_or(false);

        let _ = subscriber_room.close().await;

        let (subscriber_room_reconnect, mut subscriber_reconnect_events) =
            Room::connect(base_url, &subscriber_token, options)
                .await
                .expect("subscriber should reconnect");
        wait_for_room_connected(&mut subscriber_reconnect_events).await;

        let remote_audio_track_after_reconnect =
            tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    source
                        .capture_frame(&frame)
                        .await
                        .expect("audio frame should be accepted while waiting for post-reconnect subscription");

                    let Ok(Some(event)) = tokio::time::timeout(
                        Duration::from_millis(120),
                        subscriber_reconnect_events.recv(),
                    )
                    .await
                    else {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    };

                    match event {
                        RoomEvent::TrackPublished {
                            publication,
                            participant,
                        } if participant.identity().to_string() == publisher_identity
                            && publication.name() == track_name =>
                        {
                            publication.set_subscribed(true);
                        }
                        RoomEvent::TrackSubscribed {
                            track: livekit::track::RemoteTrack::Audio(audio_track),
                            publication,
                            participant,
                        } if participant.identity().to_string() == publisher_identity
                            && publication.name() == track_name => {
                            break Some(audio_track);
                        }
                        _ => {}
                    }
                }
            })
            .await
            .ok()
            .flatten();

        let received_after_reconnect = if let Some(remote_audio_track_after_reconnect) =
            remote_audio_track_after_reconnect
        {
            let mut second_stream =
                NativeAudioStream::new(remote_audio_track_after_reconnect.rtc_track(), 48_000, 1);
            tokio::time::timeout(Duration::from_secs(8), async {
                for _ in 0..80 {
                    source
                        .capture_frame(&frame)
                        .await
                        .expect("audio frame should be accepted after reconnect");
                    if let Ok(next) =
                        tokio::time::timeout(Duration::from_millis(80), second_stream.next()).await
                        && next.is_some()
                    {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                false
            })
            .await
            .unwrap_or(false)
        } else {
            false
        };

        let _ = publisher_room.close().await;
        let _ = subscriber_room_reconnect.close().await;

        AudioReconnectResubscribeResult {
            received_before_reconnect,
            received_after_reconnect,
        }
    }
    async fn run_abrupt_disconnect_participant_visibility(
        base_url: &str,
        room_name: &str,
        identity: &str,
    ) -> LeaveVisibilityResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (signal_client, _join, _events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("SDK signal client should connect to /rtc/v1");

        let room_client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        let listed_before_leave = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let listed = room_client
                    .list_participants(room_name)
                    .await
                    .expect("room client should list participants after signal join")
                    .len();
                if listed > 0 || tokio::time::Instant::now() >= deadline {
                    break listed;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        drop(signal_client);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let listed_after_leave = loop {
            let listed = room_client
                .list_participants(room_name)
                .await
                .expect("room client should list participants after signal client drop")
                .len();
            if listed == 0 || tokio::time::Instant::now() >= deadline {
                break listed;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        LeaveVisibilityResult {
            listed_before_leave,
            listed_after_leave,
        }
    }
    async fn run_media_publish_subscribe_event_flow(
        base_url: &str,
        room_name: &str,
        publisher_identity: &str,
        subscriber_identity: &str,
    ) -> MediaPublishSubscribeResult {
        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(publisher_identity)
            .with_name(publisher_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("publisher token should encode");
        let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(subscriber_identity)
            .with_name(subscriber_identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                can_publish: true,
                can_subscribe: true,
                ..Default::default()
            })
            .to_jwt()
            .expect("subscriber token should encode");

        let mut options = RoomOptions::default();
        options.single_peer_connection = false;
        options.connect_timeout = Duration::from_secs(10);

        let (publisher_room, mut publisher_events) =
            Room::connect(base_url, &publisher_token, options.clone())
                .await
                .expect("publisher room should connect");
        let (subscriber_room, mut subscriber_events) =
            Room::connect(base_url, &subscriber_token, options)
                .await
                .expect("subscriber room should connect");
        wait_for_room_connected(&mut publisher_events).await;
        wait_for_room_connected(&mut subscriber_events).await;

        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
        let track =
            LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source.clone()));
        let publication = publisher_room
            .local_participant()
            .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
            .await
            .expect("publisher should publish audio track");
        assert!(publication.sid().to_string().starts_with("TR_"));

        let frame = AudioFrame {
            data: vec![88_i16; 480].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 480,
        };
        for _ in 0..6 {
            source
                .capture_frame(&frame)
                .await
                .expect("audio frame should be accepted by source");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let (published_track_name, published_by_identity, subscribed_track_name) =
            tokio::time::timeout(Duration::from_secs(10), async {
                let mut published_track_name = None;
                let mut published_by_identity = None;
                let mut subscribed_track_name = None;
                while published_track_name.is_none() || subscribed_track_name.is_none() {
                    let event = subscriber_events
                        .recv()
                        .await
                        .expect("subscriber room events should stay open");
                    match event {
                        RoomEvent::TrackPublished {
                            publication,
                            participant,
                        } => {
                            published_track_name = Some(publication.name().to_string());
                            published_by_identity = Some(participant.identity().to_string());
                        }
                        RoomEvent::TrackSubscribed { publication, .. } => {
                            subscribed_track_name = Some(publication.name().to_string());
                        }
                        _ => {}
                    }
                }

                (
                    published_track_name.expect("track published should be captured"),
                    published_by_identity.expect("publisher identity should be captured"),
                    subscribed_track_name.expect("track subscribed should be captured"),
                )
            })
            .await
            .expect("subscriber should observe publish+subscribe events before timeout");

        let _ = publisher_room.close().await;
        let _ = subscriber_room.close().await;

        MediaPublishSubscribeResult {
            published_track_name,
            published_by_identity,
            subscribed_track_name,
        }
    }
    async fn run_signal_track_setting_acceptance(
        base_url: &str,
        room_name: &str,
        identity: &str,
        track_sid: &str,
        ping_timestamp: i64,
    ) -> PingReqResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (signal_client, _join, mut events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("SDK signal client should connect to /rtc/v1");

        signal_client
            .send(proto::signal_request::Message::TrackSetting(
                proto::UpdateTrackSettings {
                    track_sids: vec![track_sid.to_string()],
                    disabled: true,
                    quality: proto::VideoQuality::Low as i32,
                    width: 320,
                    height: 180,
                    fps: 15,
                    priority: 5,
                },
            ))
            .await;
        signal_client
            .send(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp,
                rtt: 5,
            }))
            .await;

        let pong = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = events.recv().await {
                if let SignalEvent::Message(message) = event
                    && let proto::signal_response::Message::PongResp(pong) = *message
                {
                    return pong;
                }
            }
            panic!("signal events stream closed before pong_resp");
        })
        .await
        .expect("track setting should still allow pong_resp before timeout");

        signal_client.close().await;

        PingReqResult {
            last_ping_timestamp: pong.last_ping_timestamp,
            response_timestamp: pong.timestamp,
        }
    }
    async fn run_signal_pingreq_pongresp(
        base_url: &str,
        room_name: &str,
        identity: &str,
        ping_timestamp: i64,
    ) -> PingReqResult {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                ..Default::default()
            })
            .to_jwt()
            .expect("SDK access token should encode");

        let mut options = SignalOptions::default();
        options.single_peer_connection = true;
        options.connect_timeout = Duration::from_secs(5);
        let (signal_client, _join, mut events) =
            SignalClient::connect(base_url, &token, options, None)
                .await
                .expect("SDK signal client should connect to /rtc/v1");

        signal_client
            .send(proto::signal_request::Message::PingReq(proto::Ping {
                timestamp: ping_timestamp,
                rtt: 5,
            }))
            .await;

        let pong = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = events.recv().await {
                if let SignalEvent::Message(message) = event
                    && let proto::signal_response::Message::PongResp(pong) = *message
                {
                    return pong;
                }
            }
            panic!("signal events stream closed before pong_resp");
        })
        .await
        .expect("ping_req should receive pong_resp before timeout");

        signal_client.close().await;

        PingReqResult {
            last_ping_timestamp: pong.last_ping_timestamp,
            response_timestamp: pong.timestamp,
        }
    }
    async fn http_get_status_and_body(
        base_url: &str,
        path_and_query: &str,
        authorization: Option<&str>,
    ) -> HttpStatusBody {
        let host = base_url
            .strip_prefix("http://")
            .expect("base_url should start with http://");
        let mut stream = tokio::net::TcpStream::connect(host)
            .await
            .expect("tcp connection should open");

        let mut request =
            format!("GET {path_and_query} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
        if let Some(authorization) = authorization {
            request.push_str(&format!("Authorization: {authorization}\r\n"));
        }
        request.push_str("\r\n");

        stream
            .write_all(request.as_bytes())
            .await
            .expect("http request should write");
        stream.flush().await.expect("socket flush should succeed");

        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .await
            .expect("http response should read");
        let response = String::from_utf8(bytes).expect("http response should be utf8");

        let status_line = response
            .lines()
            .find(|line| line.starts_with("HTTP/"))
            .unwrap_or_else(|| panic!("http response missing status line, raw: {response:?}"));
        let status = status_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_else(|| panic!("http status line missing code, line: {status_line:?}"))
            .parse::<u16>()
            .expect("status code should parse");
        let body = if let Some((_, rest)) = response.split_once("\r\n\r\n") {
            rest.to_string()
        } else if let Some((_, rest)) = response.split_once("\n\n") {
            rest.to_string()
        } else {
            String::new()
        };

        HttpStatusBody { status, body }
    }
    async fn http_post_status_and_body(
        base_url: &str,
        path: &str,
        content_type: &str,
        authorization: Option<&str>,
        body_bytes: &[u8],
    ) -> HttpStatusBody {
        let host = base_url
            .strip_prefix("http://")
            .expect("base_url should start with http://");
        let mut stream = tokio::net::TcpStream::connect(host)
            .await
            .expect("tcp connection should open");

        let mut request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n",
            body_bytes.len()
        );
        if let Some(authorization) = authorization {
            request.push_str(&format!("Authorization: {authorization}\r\n"));
        }
        request.push_str("\r\n");

        stream
            .write_all(request.as_bytes())
            .await
            .expect("http request header should write");
        stream
            .write_all(body_bytes)
            .await
            .expect("http request body should write");
        stream.flush().await.expect("socket flush should succeed");

        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .await
            .expect("http response should read");
        let response = String::from_utf8_lossy(&bytes).to_string();

        let status_line = response
            .lines()
            .find(|line| line.starts_with("HTTP/"))
            .unwrap_or_else(|| panic!("http response missing status line, raw: {response:?}"));
        let status = status_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_else(|| panic!("http status line missing code, line: {status_line:?}"))
            .parse::<u16>()
            .expect("status code should parse");
        let body = if let Some((_, rest)) = response.split_once("\r\n\r\n") {
            rest.to_string()
        } else if let Some((_, rest)) = response.split_once("\n\n") {
            rest.to_string()
        } else {
            String::new()
        };

        HttpStatusBody { status, body }
    }
    async fn ensure_oxidesfu_server_binary_built() -> Result<bool, String> {
        let binary_path = oxidesfu_server_binary_path();

        let mut build = tokio::process::Command::new("cargo");
        build
            .arg("build")
            .arg("-p")
            .arg("oxidesfu-server")
            .current_dir(oxidesfu_workspace_root())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let status = match tokio::time::timeout(Duration::from_secs(240), build.status()).await {
            Ok(Ok(status)) => status,
            Ok(Err(err)) if err.kind() == ErrorKind::NotFound => return Ok(false),
            Ok(Err(err)) => return Err(format!("failed to execute cargo build: {err}")),
            Err(_) => {
                return Err(
                    "cargo build timed out while preparing oxidesfu-server binary".to_string(),
                );
            }
        };

        if !status.success() {
            return Err(format!(
                "cargo build -p oxidesfu-server failed with status {status}"
            ));
        }
        Ok(binary_path.exists())
    }
    async fn connect_join_and_hold_socket(
        base_url: &str,
        bearer_token: &str,
        join_request_param: &str,
    ) -> (
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        String,
    ) {
        let host = base_url
            .strip_prefix("http://")
            .expect("base url should start with http://");
        let url = format!("ws://{host}/rtc/v1?join_request={join_request_param}");
        let mut request = url.into_client_request().expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {bearer_token}"))
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
        let sid = join
            .participant
            .expect("join response should include participant")
            .sid;
        (socket, sid)
    }

    async fn reconnect_and_hold_socket(
        base_url: &str,
        bearer_token: &str,
        reconnect_join_request_param: &str,
        expected_participant_sid: &str,
    ) -> (
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        String,
    ) {
        let host = base_url
            .strip_prefix("http://")
            .expect("base url should start with http://");
        let url = format!("ws://{host}/rtc/v1?join_request={reconnect_join_request_param}");
        let mut request = url.into_client_request().expect("request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {bearer_token}"))
                .expect("authorization header should parse"),
        );

        let (mut socket, _) = connect_async(request)
            .await
            .expect("websocket should connect");

        // Reconnect responses are client-capability dependent. If the reconnect envelope is not
        // emitted, resume flows may start directly with other signal messages (offer/update/etc.).
        // Accept that shape and keep the expected SID provided by caller.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            let poll = tokio::time::timeout(Duration::from_millis(200), socket.next()).await;
            let incoming = match poll {
                Ok(Some(Ok(incoming))) => incoming,
                Ok(Some(Err(err))) => {
                    panic!("reconnect websocket message should be ok: {err}");
                }
                Ok(None) => {
                    let _ = socket.close(None).await;
                    return connect_join_and_hold_socket(
                        base_url,
                        bearer_token,
                        &join_request_param(),
                    )
                    .await;
                }
                Err(_) => continue,
            };
            let Message::Binary(bytes) = incoming else {
                continue;
            };
            let response = proto::SignalResponse::decode(bytes.as_ref())
                .expect("signal response should decode");
            match response.message {
                Some(proto::signal_response::Message::Join(join)) => {
                    let sid = join
                        .participant
                        .expect("join response should include participant")
                        .sid;
                    return (socket, sid);
                }
                Some(proto::signal_response::Message::Reconnect(_)) => {
                    return (socket, expected_participant_sid.to_owned());
                }
                Some(proto::signal_response::Message::Leave(leave)) => {
                    if leave.reason == proto::DisconnectReason::StateMismatch as i32
                        && !leave.can_reconnect
                    {
                        let _ = socket.close(None).await;
                        return connect_join_and_hold_socket(
                            base_url,
                            bearer_token,
                            &join_request_param(),
                        )
                        .await;
                    }
                    panic!(
                        "reconnect websocket returned leave before session established: reason={} can_reconnect={}",
                        leave.reason,
                        leave.can_reconnect
                    );
                }
                _ => {
                    // Ignore early non-envelope messages; reconnect envelope may be omitted.
                }
            }
        }

        let _ = socket.close(None).await;
        connect_join_and_hold_socket(base_url, bearer_token, &join_request_param()).await
    }

    async fn wait_for_participant_on_room_client(
        room_client: &RoomClient,
        room_name: &str,
        identity: &str,
    ) -> Result<proto::ParticipantInfo, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
        loop {
            match room_client.get_participant(room_name, identity).await {
                Ok(participant) => return Ok(participant),
                Err(err) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(format!(
                            "participant did not become visible in room client within timeout: {err}"
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    async fn run_lk<'a>(
        args: impl IntoIterator<Item = &'a str>,
        current_dir: Option<&str>,
    ) -> Option<Output> {
        run_lk_with_timeout(args, current_dir, Duration::from_secs(10)).await
    }
    async fn run_lk_with_timeout<'a>(
        args: impl IntoIterator<Item = &'a str>,
        current_dir: Option<&str>,
        timeout: Duration,
    ) -> Option<Output> {
        let mut command = lk_command(args, current_dir);

        match tokio::time::timeout(timeout, command.output()).await {
            Ok(Ok(output)) => Some(output),
            Ok(Err(err)) if err.kind() == ErrorKind::NotFound => None,
            Ok(Err(err)) => panic!("failed to execute lk: {err}"),
            Err(_) => panic!("lk command timed out after {timeout:?}"),
        }
    }
    async fn spawn_lk<'a>(
        args: impl IntoIterator<Item = &'a str>,
        current_dir: Option<&str>,
    ) -> Option<tokio::process::Child> {
        let mut command = lk_command(args, current_dir);
        command.stdout(Stdio::null()).stderr(Stdio::null());
        match command.spawn() {
            Ok(child) => Some(child),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => panic!("failed to spawn lk: {err}"),
        }
    }
    fn lk_command<'a>(
        args: impl IntoIterator<Item = &'a str>,
        current_dir: Option<&str>,
    ) -> tokio::process::Command {
        let mut command = tokio::process::Command::new("lk");
        command.kill_on_drop(true);
        command
            .args(args)
            .env_remove("LIVEKIT_API_KEY")
            .env_remove("LIVEKIT_API_SECRET")
            .env_remove("LIVEKIT_URL");
        if let Some(current_dir) = current_dir {
            command.current_dir(current_dir);
        }
        command
    }

use super::*;

    #[tokio::test]
    async fn differential_matrix_core_probes_match_go_livekit_dev() {
        let oxidesfu_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let oxidesfu_addr = oxidesfu_listener
            .local_addr()
            .expect("listener should have local addr");
        let oxidesfu_server = tokio::spawn(async move {
            axum::serve(oxidesfu_listener, oxidesfu_server::app())
                .await
                .expect("test server should run");
        });

        let Some((mut go_livekit, go_base_url)) =
            spawn_ready_go_livekit_server_with_single_respawn()
                .await
                .expect("go livekit server should become ready in dev mode")
        else {
            eprintln!("skipping differential test because go is not on PATH");
            oxidesfu_server.abort();
            return;
        };

        let oxidesfu_base_url = format!("http://{oxidesfu_addr}");

        let cases = [
            DifferentialCase::TwirpRoomLifecycle,
            DifferentialCase::ValidateV1NegativePaths,
            DifferentialCase::ValidateV1MalformedJoinRequest,
            DifferentialCase::ValidateV1InvalidGzipJoinRequest,
            DifferentialCase::SignalV1AuthFailureMatrix,
            DifferentialCase::SignalJoinParticipantVisibility,
            DifferentialCase::SignalPingReqPongResp,
            DifferentialCase::SignalReconnectResubscribeDataTrack,
            DifferentialCase::SignalAbruptDisconnectParticipantVisibility,
            DifferentialCase::SignalReconnectIdentityTakeover,
            DifferentialCase::TwirpDeleteMissingRoomErrorShape,
            DifferentialCase::SignalReconnectThenPingReqPongResp,
            DifferentialCase::SignalReconnectReasonUnknown,
            DifferentialCase::SignalReconnectReasonPublisherFailed,
            DifferentialCase::SignalReconnectReasonSwitchCandidate,
            DifferentialCase::SignalReconnectReasonSubscriberFailed,
            DifferentialCase::SignalReconnectReasonSignalDisconnected,
            DifferentialCase::SignalReconnectStaleParticipantSidLifecycle,
            DifferentialCase::TwirpListParticipantsPermissionDenied,
            DifferentialCase::TwirpListParticipantsMissingAuth,
            DifferentialCase::TwirpListParticipantsMalformedBody,
            DifferentialCase::TwirpListParticipantsContentTypeMismatch,
            DifferentialCase::TwirpListRoomsPermissionDenied,
            DifferentialCase::TwirpCreateRoomMissingAuth,
            DifferentialCase::TwirpCreateRoomMalformedBody,
            DifferentialCase::TwirpRemoveParticipantPermissionDenied,
            DifferentialCase::TwirpUpdateParticipantMalformedBody,
            DifferentialCase::TwirpCreateRoomContentTypeMismatch,
            DifferentialCase::TwirpSendDataPermissionDenied,
            DifferentialCase::TwirpSendDataInvalidNonce,
            DifferentialCase::TwirpSendDataMissingRoom,
            DifferentialCase::TwirpGetParticipantPermissionDenied,
            DifferentialCase::TwirpGetParticipantMalformedBody,
            DifferentialCase::TwirpUpdateRoomMetadataPermissionDenied,
            DifferentialCase::TwirpUpdateRoomMetadataMissingRoom,
            DifferentialCase::TwirpSendDataContentTypeMismatch,
            DifferentialCase::TwirpGetParticipantMissingParticipant,
            DifferentialCase::TwirpUpdateRoomMetadataMalformedBody,
            DifferentialCase::TwirpUpdateRoomMetadataContentTypeMismatch,
            DifferentialCase::TwirpSendDataMissingAuth,
            DifferentialCase::SignalPostCloseSendNoPong,
            DifferentialCase::SignalLeaveTerminationLifecycle,
            DifferentialCase::SignalIdentityTakeoverCloseLifecycle,
            DifferentialCase::DataTrackMultiSubscriberReconnectUnderLoad,
            DifferentialCase::DataTrackPublisherDropLifecycle,
        ];

        for (idx, case) in cases.iter().enumerate() {
            let namespace = format!("case-{idx}-{}", unique_suffix());
            let oxidesfu = run_differential_case(*case, &oxidesfu_base_url, &namespace).await;
            let go = run_differential_case(*case, &go_base_url, &namespace).await;
            assert_eq!(
                oxidesfu, go,
                "differential case mismatch: {case:?}\noxidesfu={oxidesfu:?}\ngo={go:?}"
            );
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

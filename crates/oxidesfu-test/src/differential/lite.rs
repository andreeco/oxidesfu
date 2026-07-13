use super::*;

#[tokio::test]
    async fn differential_matrix_lite_probes_match_go_livekit_dev() {
        let ferrite_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let oxidesfu_addr = ferrite_listener
            .local_addr()
            .expect("listener should have local addr");
        let oxidesfu_server = tokio::spawn(async move {
            axum::serve(ferrite_listener, oxidesfu_server::app())
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
            DifferentialCase::ValidateV1NegativePaths,
            DifferentialCase::SignalPingReqPongResp,
            DifferentialCase::TwirpListParticipantsMissingAuth,
            DifferentialCase::TwirpListParticipantsContentTypeMismatch,
            DifferentialCase::TwirpListRoomsPermissionDenied,
            DifferentialCase::TwirpCreateRoomMissingAuth,
            DifferentialCase::TwirpSendDataMissingAuth,
            DifferentialCase::TwirpSendDataInvalidNonce,
            DifferentialCase::TwirpSendDataContentTypeMismatch,
            DifferentialCase::TwirpUpdateRoomMetadataMalformedBody,
            DifferentialCase::TwirpUpdateRoomMetadataContentTypeMismatch,
            DifferentialCase::SignalPostCloseSendNoPong,
        ];

        for (idx, case) in cases.iter().enumerate() {
            let namespace = format!("lite-case-{idx}-{}", unique_suffix());
            let ferrite = run_differential_case(*case, &oxidesfu_base_url, &namespace).await;
            let go = run_differential_case(*case, &go_base_url, &namespace).await;
            assert_eq!(
                ferrite, go,
                "differential lite case mismatch: {case:?}\nferrite={ferrite:?}\ngo={go:?}"
            );
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

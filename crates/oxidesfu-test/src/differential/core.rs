use super::*;
    #[tokio::test]
    async fn differential_validate_v1_negative_paths_match_go_livekit_dev() {
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
        let auth_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("diff-validate")
            .with_name("Diff Validate")
            .with_grants(VideoGrants {
                room_join: true,
                room: format!("diff-validate-room-{}", unique_suffix()),
                ..Default::default()
            })
            .to_jwt()
            .expect("access token should encode");

        let ferrite_missing_join = http_get_status_and_body(
            &oxidesfu_base_url,
            "/rtc/v1/validate",
            Some(&format!("Bearer {auth_token}")),
        )
        .await;
        let go_missing_join = http_get_status_and_body(
            &go_base_url,
            "/rtc/v1/validate",
            Some(&format!("Bearer {auth_token}")),
        )
        .await;

        assert_eq!(ferrite_missing_join.status, go_missing_join.status);
        assert_eq!(ferrite_missing_join.body, go_missing_join.body);

        let ferrite_missing_auth =
            http_get_status_and_body(&oxidesfu_base_url, "/rtc/v1/validate", None).await;
        let go_missing_auth =
            http_get_status_and_body(&go_base_url, "/rtc/v1/validate", None).await;

        assert_eq!(ferrite_missing_auth.status, go_missing_auth.status);
        assert!(!ferrite_missing_auth.body.trim().is_empty());
        assert!(!go_missing_auth.body.trim().is_empty());

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_process_transport_rr_switch_candidate_reconnect_matches_go_livekit_dev() {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping process transport differential test because redis-server is not on PATH"
            );
            return;
        };

        let oxidesfu_port = reserve_local_port();
        let rtc_udp_port = reserve_local_port();
        let rtc_tcp_port = reserve_local_port();
        let options = OxidesfuServerProcessOptions {
            rtc_udp_port: Some(rtc_udp_port),
            rtc_tcp_port: Some(rtc_tcp_port),
            rtc_use_external_ip: Some(true),
            rtc_node_ip: Some("127.0.0.1".to_string()),
            ..Default::default()
        };

        let Some((mut oxidesfu, oxidesfu_base_url)) = spawn_oxidesfu_server_process_with_options(
            oxidesfu_port,
            &redis_url,
            false,
            &options,
        )
        .await
        .expect("oxidesfu process should start for differential transport probe")
        else {
            eprintln!(
                "skipping process transport differential test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut go_livekit, go_base_url)) =
            spawn_ready_go_livekit_server_with_single_respawn()
                .await
                .expect("go livekit server should become ready in dev mode")
        else {
            eprintln!("skipping differential test because go is not on PATH");
            let _ = oxidesfu.kill().await;
            let _ = redis.kill().await;
            return;
        };

        let room_name = format!("diff-process-transport-room-{}", unique_suffix());
        let oxidesfu_result = run_signal_reconnect_reason_response_with_mode(
            &oxidesfu_base_url,
            &room_name,
            "diff-process-transport-oxidesfu",
            proto::ReconnectReason::RrSwitchCandidate,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response_with_mode(
            &go_base_url,
            &room_name,
            "diff-process-transport-go",
            proto::ReconnectReason::RrSwitchCandidate,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.saw_reconnect_response);

        let _ = go_livekit.kill().await;
        let _ = oxidesfu.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn differential_process_transport_reconnect_reason_ping_rtt_matrix_matches_go_livekit_dev()
    {
        let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await
        else {
            eprintln!(
                "skipping process transport differential test because redis-server is not on PATH"
            );
            return;
        };

        let oxidesfu_port = reserve_local_port();
        let rtc_udp_port = reserve_local_port();
        let rtc_tcp_port = reserve_local_port();
        let options = OxidesfuServerProcessOptions {
            rtc_udp_port: Some(rtc_udp_port),
            rtc_tcp_port: Some(rtc_tcp_port),
            rtc_use_external_ip: Some(true),
            rtc_node_ip: Some("127.0.0.1".to_string()),
            ..Default::default()
        };

        let Some((mut oxidesfu, oxidesfu_base_url)) = spawn_oxidesfu_server_process_with_options(
            oxidesfu_port,
            &redis_url,
            false,
            &options,
        )
        .await
        .expect("oxidesfu process should start for transport reconnect/ping differential probe")
        else {
            eprintln!(
                "skipping process transport differential test because oxidesfu-server binary is unavailable"
            );
            let _ = redis.kill().await;
            return;
        };

        let Some((mut go_livekit, go_base_url)) =
            spawn_ready_go_livekit_server_with_single_respawn()
                .await
                .expect("go livekit server should become ready in dev mode")
        else {
            eprintln!("skipping differential test because go is not on PATH");
            let _ = oxidesfu.kill().await;
            let _ = redis.kill().await;
            return;
        };

        let reconnect_reasons = [
            proto::ReconnectReason::RrUnknown,
            proto::ReconnectReason::RrSwitchCandidate,
            proto::ReconnectReason::RrPublisherFailed,
            proto::ReconnectReason::RrSubscriberFailed,
            proto::ReconnectReason::RrSignalDisconnected,
        ];
        let ping_rtt_values = [5_i64, 250_i64];

        for reconnect_reason in reconnect_reasons {
            for ping_rtt in ping_rtt_values {
                let room_name = format!(
                    "diff-process-transport-reconnect-ping-room-{}-{}-{}",
                    reconnect_reason as i32,
                    ping_rtt,
                    unique_suffix()
                );
                let ping_timestamp = i64::try_from(unique_suffix())
                    .expect("millis suffix should fit i64")
                    .wrapping_add(1_000 + ping_rtt);

                let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode_and_rtt(
                    &oxidesfu_base_url,
                    &room_name,
                    "diff-process-transport-ping-oxidesfu",
                    reconnect_reason,
                    ping_timestamp,
                    ping_rtt,
                    false,
                )
                .await;
                let go_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode_and_rtt(
                    &go_base_url,
                    &room_name,
                    "diff-process-transport-ping-go",
                    reconnect_reason,
                    ping_timestamp,
                    ping_rtt,
                    false,
                )
                .await;

                assert_eq!(
                    oxidesfu_result, go_result,
                    "process transport reconnect+ping parity mismatch for reconnect_reason={:?}, ping_rtt={}",
                    reconnect_reason, ping_rtt
                );
                assert!(
                    oxidesfu_result.participant_sid_present,
                    "participant SID should be present for reconnect_reason={:?}, ping_rtt={}",
                    reconnect_reason,
                    ping_rtt
                );
                if oxidesfu_result.saw_pong_response {
                    assert_eq!(
                        oxidesfu_result.last_ping_timestamp, ping_timestamp,
                        "pong should echo ping timestamp for reconnect_reason={:?}, ping_rtt={}",
                        reconnect_reason, ping_rtt
                    );
                    assert!(
                        oxidesfu_result.response_timestamp > 0,
                        "pong response timestamp should be set for reconnect_reason={:?}, ping_rtt={}",
                        reconnect_reason,
                        ping_rtt
                    );
                }
            }
        }

        let _ = go_livekit.kill().await;
        let _ = oxidesfu.kill().await;
        let _ = redis.kill().await;
    }

    #[tokio::test]
    async fn differential_validate_v1_malformed_join_request_parity_matches_go_livekit_dev() {
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

        let room_name = format!("diff-validate-malformed-room-{}", unique_suffix());
        let oxidesfu_result = run_validate_v1_malformed_join_request_parity(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_validate_v1_malformed_join_request_parity(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert_eq!(oxidesfu_result.status, 400);
        assert!(oxidesfu_result.has_body);
        assert!(oxidesfu_result.has_join_request_hint);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_validate_v1_invalid_gzip_join_request_parity_matches_go_livekit_dev() {
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

        let room_name = format!("diff-validate-gzip-room-{}", unique_suffix());
        let oxidesfu_result = run_validate_v1_invalid_gzip_join_request_parity(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_validate_v1_invalid_gzip_join_request_parity(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert_eq!(oxidesfu_result.status, 400);
        assert!(oxidesfu_result.has_body);
        assert!(oxidesfu_result.has_gzip_hint);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_v1_auth_failure_matrix_matches_go_livekit_dev() {
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

        let room_name = format!("diff-signal-auth-matrix-room-{}", unique_suffix());
        let oxidesfu_result =
            run_signal_v1_auth_failure_matrix(&format!("http://{oxidesfu_addr}"), &room_name).await;
        let go_result = run_signal_v1_auth_failure_matrix(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.missing_auth.status_is_client_error);
        assert!(oxidesfu_result.invalid_token.status_is_client_error);
        assert!(oxidesfu_result.missing_join_grant.status_is_client_error);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_join_participant_visibility_matches_go_livekit_dev() {
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

        let room_name = format!("diff-signal-room-{}", unique_suffix());
        let identity = format!("diff-signal-identity-{}", unique_suffix());
        let display_name = format!("Diff Signal {}", unique_suffix());

        let oxidesfu_result = run_signal_join_participant_visibility(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            &display_name,
        )
        .await;
        let go_result = run_signal_join_participant_visibility(
            &go_base_url,
            &room_name,
            &identity,
            &display_name,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_leave_removes_participant_visibility_matches_go_livekit_dev() {
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

        let room_name = format!("diff-leave-room-{}", unique_suffix());
        let identity = format!("diff-leave-identity-{}", unique_suffix());

        let oxidesfu_result = run_signal_leave_participant_visibility(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
        )
        .await;
        let go_result =
            run_signal_leave_participant_visibility(&go_base_url, &room_name, &identity).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_resubscribe_data_track_matches_go_livekit_dev() {
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

        let room_name = format!("diff-reconnect-room-{}", unique_suffix());
        let oxidesfu_result = run_reconnect_resubscribe_data_track(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            "diff-reconnect-alice",
            "diff-reconnect-bob",
        )
        .await;
        let go_result = run_reconnect_resubscribe_data_track(
            &go_base_url,
            &room_name,
            "diff-reconnect-alice",
            "diff-reconnect-bob",
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_resubscribe_audio_track_matches_go_livekit_dev() {
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

        let room_name = format!("diff-reconnect-audio-room-{}", unique_suffix());
        let oxidesfu_result = run_reconnect_resubscribe_audio_track(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            "diff-reconnect-audio-alice",
            "diff-reconnect-audio-bob",
            false,
        )
        .await;
        let go_result = run_reconnect_resubscribe_audio_track(
            &go_base_url,
            &room_name,
            "diff-reconnect-audio-alice",
            "diff-reconnect-audio-bob",
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.received_before_reconnect);
        assert!(oxidesfu_result.received_after_reconnect);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_resubscribe_audio_track_single_pc_v1_matches_go_livekit_dev() {
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

        let room_name = format!("diff-reconnect-audio-single-pc-room-{}", unique_suffix());
        let oxidesfu_result = run_reconnect_resubscribe_audio_track(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            "diff-reconnect-audio-single-pc-alice",
            "diff-reconnect-audio-single-pc-bob",
            true,
        )
        .await;
        let go_result = run_reconnect_resubscribe_audio_track(
            &go_base_url,
            &room_name,
            "diff-reconnect-audio-single-pc-alice",
            "diff-reconnect-audio-single-pc-bob",
            true,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.received_before_reconnect);
        assert!(oxidesfu_result.received_after_reconnect);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_abrupt_disconnect_removes_participant_visibility_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-abrupt-room-{}", unique_suffix());
        let identity = format!("diff-abrupt-identity-{}", unique_suffix());

        let oxidesfu_result = run_abrupt_disconnect_participant_visibility(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
        )
        .await;
        let go_result =
            run_abrupt_disconnect_participant_visibility(&go_base_url, &room_name, &identity).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_identity_takeover_matches_go_livekit_dev() {
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

        let room_name = format!("diff-reconnect-identity-room-{}", unique_suffix());
        let identity = format!("diff-reconnect-identity-{}", unique_suffix());

        let oxidesfu_result = run_reconnect_identity_takeover_visibility(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
        )
        .await;
        let go_result =
            run_reconnect_identity_takeover_visibility(&go_base_url, &room_name, &identity).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_delete_missing_room_error_shape_matches_go_livekit_dev() {
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

        let missing_room = format!("diff-missing-room-{}", unique_suffix());

        let oxidesfu_result = run_twirp_delete_missing_room_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &missing_room,
        )
        .await;
        let go_result =
            run_twirp_delete_missing_room_error_shape(&go_base_url, &missing_room).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_then_pingreq_pongresp_matches_go_livekit_dev() {
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

        let room_name = format!("diff-reconnect-ping-room-{}", unique_suffix());
        let identity = format!("diff-reconnect-ping-identity-{}", unique_suffix());
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_then_pingreq_pongresp(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            ping_timestamp,
        )
        .await;
        let go_result = run_signal_reconnect_then_pingreq_pongresp(
            &go_base_url,
            &room_name,
            &identity,
            ping_timestamp,
        )
        .await;

        assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
        assert_eq!(go_result.last_ping_timestamp, ping_timestamp);
        assert!(oxidesfu_result.response_timestamp > 0);
        assert!(go_result.response_timestamp > 0);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_rr_unknown_returns_reconnect_response_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-reconnect-unknown-room-{}", unique_suffix());
        let identity = format!("diff-reconnect-unknown-identity-{}", unique_suffix());

        let oxidesfu_result = run_signal_reconnect_reason_response(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrUnknown,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrUnknown,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_publisher_failed_returns_reconnect_response_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-reconnect-reason-room-{}", unique_suffix());
        let identity = format!("diff-reconnect-reason-identity-{}", unique_suffix());

        let oxidesfu_result = run_signal_reconnect_reason_response(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrPublisherFailed,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrPublisherFailed,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_rr_switch_candidate_returns_reconnect_response_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-reconnect-switch-candidate-room-{}", unique_suffix());
        let identity = format!(
            "diff-reconnect-switch-candidate-identity-{}",
            unique_suffix()
        );

        let oxidesfu_result = run_signal_reconnect_reason_response(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSwitchCandidate,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSwitchCandidate,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_rr_subscriber_failed_returns_reconnect_response_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-reconnect-subscriber-failed-room-{}", unique_suffix());
        let identity = format!(
            "diff-reconnect-subscriber-failed-identity-{}",
            unique_suffix()
        );

        let oxidesfu_result = run_signal_reconnect_reason_response(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSubscriberFailed,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSubscriberFailed,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_reconnect_rr_signal_disconnected_returns_reconnect_response_matches_go_livekit_dev()
     {
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

        let room_name = format!(
            "diff-reconnect-signal-disconnected-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-signal-disconnected-identity-{}",
            unique_suffix()
        );

        let oxidesfu_result = run_signal_reconnect_reason_response(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSignalDisconnected,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSignalDisconnected,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }



    #[tokio::test]
    async fn differential_signal_reconnect_rr_unknown_returns_reconnect_response_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-reconnect-unknown-dual-pc-room-{}", unique_suffix());
        let identity = format!("diff-reconnect-unknown-dual-pc-identity-{}", unique_suffix());

        let oxidesfu_result = run_signal_reconnect_reason_response_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrUnknown,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrUnknown,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_switch_candidate_returns_reconnect_response_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-reconnect-switch-candidate-dual-pc-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-switch-candidate-dual-pc-identity-{}",
            unique_suffix()
        );

        let oxidesfu_result = run_signal_reconnect_reason_response_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSwitchCandidate,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSwitchCandidate,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_publisher_failed_returns_reconnect_response_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-reconnect-publisher-failed-dual-pc-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-publisher-failed-dual-pc-identity-{}",
            unique_suffix()
        );

        let oxidesfu_result = run_signal_reconnect_reason_response_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrPublisherFailed,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrPublisherFailed,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_subscriber_failed_returns_reconnect_response_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-reconnect-subscriber-failed-dual-pc-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-subscriber-failed-dual-pc-identity-{}",
            unique_suffix()
        );

        let oxidesfu_result = run_signal_reconnect_reason_response_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSubscriberFailed,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSubscriberFailed,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_signal_disconnected_returns_reconnect_response_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-reconnect-signal-disconnected-dual-pc-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-signal-disconnected-dual-pc-identity-{}",
            unique_suffix()
        );

        let oxidesfu_result = run_signal_reconnect_reason_response_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSignalDisconnected,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_response_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSignalDisconnected,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_switch_candidate_then_pingreq_pongresp_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-reconnect-switch-candidate-ping-room-{}", unique_suffix());
        let identity = format!(
            "diff-reconnect-switch-candidate-ping-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSwitchCandidate,
            ping_timestamp,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSwitchCandidate,
            ping_timestamp,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_signal_disconnected_then_pingreq_pongresp_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-reconnect-signal-disconnected-ping-dual-pc-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-signal-disconnected-ping-dual-pc-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSignalDisconnected,
            ping_timestamp,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSignalDisconnected,
            ping_timestamp,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_publisher_failed_then_pingreq_pongresp_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-reconnect-publisher-failed-ping-dual-pc-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-publisher-failed-ping-dual-pc-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrPublisherFailed,
            ping_timestamp,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrPublisherFailed,
            ping_timestamp,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_subscriber_failed_then_pingreq_pongresp_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-reconnect-subscriber-failed-ping-dual-pc-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-subscriber-failed-ping-dual-pc-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSubscriberFailed,
            ping_timestamp,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSubscriberFailed,
            ping_timestamp,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_unknown_then_pingreq_pongresp_dual_pc_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-reconnect-unknown-ping-dual-pc-room-{}", unique_suffix());
        let identity = format!(
            "diff-reconnect-unknown-ping-dual-pc-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrUnknown,
            ping_timestamp,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrUnknown,
            ping_timestamp,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_switch_candidate_then_pingreq_pongresp_dual_pc_matches_go_livekit_dev() {
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

        let room_name = format!(
            "diff-reconnect-switch-candidate-ping-dual-pc-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-switch-candidate-ping-dual-pc-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSwitchCandidate,
            ping_timestamp,
            false,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp_with_mode(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSwitchCandidate,
            ping_timestamp,
            false,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }



    #[tokio::test]
    async fn differential_signal_reconnect_rr_publisher_failed_then_pingreq_pongresp_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-reconnect-publisher-failed-ping-room-{}", unique_suffix());
        let identity = format!(
            "diff-reconnect-publisher-failed-ping-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrPublisherFailed,
            ping_timestamp,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrPublisherFailed,
            ping_timestamp,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_signal_disconnected_then_pingreq_pongresp_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-reconnect-signal-disconnected-ping-room-{}",
            unique_suffix()
        );
        let identity = format!(
            "diff-reconnect-signal-disconnected-ping-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSignalDisconnected,
            ping_timestamp,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSignalDisconnected,
            ping_timestamp,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_unknown_then_pingreq_pongresp_matches_go_livekit_dev() {
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

        let room_name = format!("diff-reconnect-unknown-ping-room-{}", unique_suffix());
        let identity = format!("diff-reconnect-unknown-ping-identity-{}", unique_suffix());
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrUnknown,
            ping_timestamp,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrUnknown,
            ping_timestamp,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_rr_subscriber_failed_then_pingreq_pongresp_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-reconnect-subscriber-failed-ping-room-{}", unique_suffix());
        let identity = format!(
            "diff-reconnect-subscriber-failed-ping-identity-{}",
            unique_suffix()
        );
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            proto::ReconnectReason::RrSubscriberFailed,
            ping_timestamp,
        )
        .await;
        let go_result = run_signal_reconnect_reason_then_pingreq_pongresp(
            &go_base_url,
            &room_name,
            &identity,
            proto::ReconnectReason::RrSubscriberFailed,
            ping_timestamp,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        if oxidesfu_result.saw_pong_response {
            assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
            assert!(oxidesfu_result.response_timestamp > 0);
        }

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[tokio::test]
    async fn differential_signal_reconnect_stale_participant_sid_lifecycle_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-reconnect-stale-sid-room-{}", unique_suffix());
        let identity = format!("diff-reconnect-stale-sid-identity-{}", unique_suffix());

        let oxidesfu_result = run_signal_reconnect_stale_participant_sid_lifecycle(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
        )
        .await;
        let go_result = run_signal_reconnect_stale_participant_sid_lifecycle(
            &go_base_url,
            &room_name,
            &identity,
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_sid_present);
        assert!(oxidesfu_result.stale_sid_used);
        assert!(
            oxidesfu_result.saw_leave,
            "stale reconnect should receive leave"
        );
        assert_eq!(
            oxidesfu_result.leave_reason,
            proto::DisconnectReason::StateMismatch as i32
        );
        assert_eq!(
            oxidesfu_result.leave_action,
            proto::leave_request::Action::Disconnect as i32
        );

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_list_participants_permission_denied_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-perm-denied-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_list_participants_permission_denied_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_list_participants_permission_denied_error_shape(&go_base_url, &room_name)
                .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_auth_failure);
        assert!(oxidesfu_result.code_is_auth_related);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_list_participants_missing_auth_error_shape_matches_go_livekit_dev()
    {
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

        let room_name = format!(
            "diff-list-participants-missing-auth-room-{}",
            unique_suffix()
        );
        let oxidesfu_result = run_twirp_list_participants_missing_auth_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_list_participants_missing_auth_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert_eq!(oxidesfu_result.status, 401);
        assert_eq!(oxidesfu_result.code, "unauthenticated");
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_list_participants_malformed_body_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-list-participants-malformed-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_list_participants_malformed_body_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_list_participants_malformed_body_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_list_participants_content_type_mismatch_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!(
            "diff-list-participants-content-type-room-{}",
            unique_suffix()
        );
        let oxidesfu_result = run_twirp_list_participants_content_type_mismatch_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_list_participants_content_type_mismatch_error_shape(&go_base_url, &room_name)
                .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_list_rooms_permission_denied_error_shape_matches_go_livekit_dev() {
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

        let oxidesfu_result =
            run_twirp_list_rooms_permission_denied_error_shape(&format!("http://{oxidesfu_addr}"))
                .await;
        let go_result = run_twirp_list_rooms_permission_denied_error_shape(&go_base_url).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_auth_failure);
        assert!(oxidesfu_result.code_is_auth_related);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_create_room_missing_auth_error_shape_matches_go_livekit_dev() {
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

        let oxidesfu_result =
            run_twirp_create_room_missing_auth_error_shape(&format!("http://{oxidesfu_addr}"))
                .await;
        let go_result = run_twirp_create_room_missing_auth_error_shape(&go_base_url).await;

        assert_eq!(oxidesfu_result, go_result);
        assert_eq!(oxidesfu_result.status, 401);
        assert_eq!(oxidesfu_result.code, "unauthenticated");
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_create_room_malformed_body_error_shape_matches_go_livekit_dev() {
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

        let oxidesfu_result =
            run_twirp_create_room_malformed_body_error_shape(&format!("http://{oxidesfu_addr}"))
                .await;
        let go_result = run_twirp_create_room_malformed_body_error_shape(&go_base_url).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_remove_participant_permission_denied_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-remove-perm-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_remove_participant_permission_denied_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_remove_participant_permission_denied_error_shape(&go_base_url, &room_name)
                .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_auth_failure);
        assert!(oxidesfu_result.code_is_auth_related);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_update_participant_malformed_body_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-update-malformed-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_update_participant_malformed_body_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_update_participant_malformed_body_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_create_room_content_type_mismatch_error_shape_matches_go_livekit_dev()
     {
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

        let oxidesfu_result = run_twirp_create_room_content_type_mismatch_error_shape(&format!(
            "http://{oxidesfu_addr}"
        ))
        .await;
        let go_result = run_twirp_create_room_content_type_mismatch_error_shape(&go_base_url).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_send_data_permission_denied_error_shape_matches_go_livekit_dev() {
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

        let room_name = format!("diff-send-data-perm-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_send_data_permission_denied_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_send_data_permission_denied_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_auth_failure);
        assert!(oxidesfu_result.code_is_auth_related);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_send_data_invalid_nonce_error_shape_matches_go_livekit_dev() {
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

        let room_name = format!("diff-send-data-nonce-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_send_data_invalid_nonce_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_send_data_invalid_nonce_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert_eq!(oxidesfu_result.status, 400);
        assert_eq!(oxidesfu_result.code, "invalid_argument");
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_send_data_missing_room_error_shape_matches_go_livekit_dev() {
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

        let room_name = format!("diff-send-data-missing-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_send_data_missing_room_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_send_data_missing_room_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_error);
        assert!(oxidesfu_result.code_is_not_found_or_unavailable);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_get_participant_permission_denied_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-get-participant-perm-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_get_participant_permission_denied_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_get_participant_permission_denied_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_auth_failure);
        assert!(oxidesfu_result.code_is_auth_related);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_get_participant_malformed_body_error_shape_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-get-participant-malformed-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_get_participant_malformed_body_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_get_participant_malformed_body_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_update_room_metadata_permission_denied_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-update-room-metadata-perm-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_update_room_metadata_permission_denied_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_update_room_metadata_permission_denied_error_shape(&go_base_url, &room_name)
                .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_auth_failure);
        assert!(oxidesfu_result.code_is_auth_related);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_update_room_metadata_missing_room_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-update-room-metadata-missing-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_update_room_metadata_missing_room_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_update_room_metadata_missing_room_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_error);
        assert!(oxidesfu_result.code_is_not_found_or_unavailable);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_send_data_content_type_mismatch_error_shape_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-send-data-content-type-room-{}", unique_suffix());
        let oxidesfu_result = run_twirp_send_data_content_type_mismatch_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_send_data_content_type_mismatch_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_get_participant_missing_participant_error_shape_matches_go_livekit_dev()
     {
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

        let room_name = format!("diff-get-participant-missing-{}", unique_suffix());
        let oxidesfu_result = run_twirp_get_participant_missing_participant_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_get_participant_missing_participant_error_shape(&go_base_url, &room_name)
                .await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.status_is_error);
        assert!(oxidesfu_result.code_is_not_found_or_unavailable);
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_update_room_metadata_malformed_body_error_shape_matches_go_livekit_dev()
     {
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

        let oxidesfu_result = run_twirp_update_room_metadata_malformed_body_error_shape(&format!(
            "http://{oxidesfu_addr}"
        ))
        .await;
        let go_result =
            run_twirp_update_room_metadata_malformed_body_error_shape(&go_base_url).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_update_room_metadata_content_type_mismatch_error_shape_matches_go_livekit_dev()
     {
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

        let oxidesfu_result = run_twirp_update_room_metadata_content_type_mismatch_error_shape(
            &format!("http://{oxidesfu_addr}"),
        )
        .await;
        let go_result =
            run_twirp_update_room_metadata_content_type_mismatch_error_shape(&go_base_url).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_twirp_send_data_missing_auth_error_shape_matches_go_livekit_dev() {
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

        let room_name = format!("diff-send-data-missing-auth-{}", unique_suffix());
        let oxidesfu_result = run_twirp_send_data_missing_auth_error_shape(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_send_data_missing_auth_error_shape(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);
        assert_eq!(oxidesfu_result.status, 401);
        assert_eq!(oxidesfu_result.code, "unauthenticated");
        assert!(oxidesfu_result.has_msg);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_post_close_send_no_pong_matches_go_livekit_dev() {
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

        let room_name = format!("diff-post-close-send-room-{}", unique_suffix());
        let identity = format!("diff-post-close-send-identity-{}", unique_suffix());

        let oxidesfu_result = run_signal_post_close_send_no_pong(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
        )
        .await;
        let go_result =
            run_signal_post_close_send_no_pong(&go_base_url, &room_name, &identity).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.send_after_close_attempted);
        assert!(!oxidesfu_result.saw_pong_after_close);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_leave_termination_lifecycle_matches_go_livekit_dev() {
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

        let room_name = format!("diff-signal-leave-lifecycle-room-{}", unique_suffix());
        let identity = format!("diff-signal-leave-lifecycle-{}", unique_suffix());
        let oxidesfu_result = run_signal_leave_termination_lifecycle(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
        )
        .await;
        let go_result =
            run_signal_leave_termination_lifecycle(&go_base_url, &room_name, &identity).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.participant_visible_before_leave);
        assert!(oxidesfu_result.participant_removed_after_leave);
        assert!(oxidesfu_result.saw_leave_or_close);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_identity_takeover_close_lifecycle_matches_go_livekit_dev() {
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

        let room_name = format!("diff-takeover-close-room-{}", unique_suffix());
        let identity = format!("diff-takeover-close-identity-{}", unique_suffix());

        let oxidesfu_result = run_signal_identity_takeover_close_lifecycle(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
        )
        .await;
        let go_result =
            run_signal_identity_takeover_close_lifecycle(&go_base_url, &room_name, &identity).await;

        assert_eq!(oxidesfu_result, go_result);
        assert!(oxidesfu_result.listed_after_takeover <= 1);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_data_track_multi_subscriber_reconnect_under_load_matches_go_livekit_dev()
    {
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

        let room_name = format!("diff-reconnect-load-room-{}", unique_suffix());
        let oxidesfu_result = run_reconnect_under_load_two_subscribers(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            "diff-load-alice",
            "diff-load-bob",
            "diff-load-carol",
        )
        .await;
        let go_result = run_reconnect_under_load_two_subscribers(
            &go_base_url,
            &room_name,
            "diff-load-alice",
            "diff-load-bob",
            "diff-load-carol",
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_data_track_publisher_drop_lifecycle_matches_go_livekit_dev() {
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

        let room_name = format!("diff-publisher-drop-room-{}", unique_suffix());
        let oxidesfu_result = run_publisher_drop_data_track_lifecycle(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            "diff-pubdrop-alice",
            "diff-pubdrop-bob",
        )
        .await;
        let go_result = run_publisher_drop_data_track_lifecycle(
            &go_base_url,
            &room_name,
            "diff-pubdrop-alice",
            "diff-pubdrop-bob",
        )
        .await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_track_setting_acceptance_matches_go_livekit_dev() {
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

        let room_name = format!("diff-track-setting-room-{}", unique_suffix());
        let identity = format!("diff-track-setting-{}", unique_suffix());
        let track_sid = format!("TR_diff_track_setting_{}", unique_suffix());
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_track_setting_acceptance(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            &identity,
            &track_sid,
            ping_timestamp,
        )
        .await;
        let go_result = run_signal_track_setting_acceptance(
            &go_base_url,
            &room_name,
            &identity,
            &track_sid,
            ping_timestamp,
        )
        .await;

        assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
        assert_eq!(go_result.last_ping_timestamp, ping_timestamp);
        assert!(oxidesfu_result.response_timestamp > 0);
        assert!(go_result.response_timestamp > 0);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }
    #[tokio::test]
    async fn differential_signal_pingreq_pongresp_matches_go_livekit_dev() {
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

        let room_name = format!("diff-ping-room-{}", unique_suffix());
        let ping_timestamp = i64::try_from(unique_suffix()).expect("millis suffix should fit i64");

        let oxidesfu_result = run_signal_pingreq_pongresp(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
            "diff-ping-alice",
            ping_timestamp,
        )
        .await;
        let go_result = run_signal_pingreq_pongresp(
            &go_base_url,
            &room_name,
            "diff-ping-alice",
            ping_timestamp,
        )
        .await;

        assert_eq!(oxidesfu_result.last_ping_timestamp, ping_timestamp);
        assert_eq!(go_result.last_ping_timestamp, ping_timestamp);
        assert!(oxidesfu_result.response_timestamp > 0);
        assert!(go_result.response_timestamp > 0);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct UpdateSubscriptionsLifecycleEventParity {
        saw_track_unsubscribed: bool,
        saw_track_resubscribed: bool,
    }

    #[tokio::test]
    async fn differential_twirp_update_subscriptions_lifecycle_event_parity_matches_go_livekit_dev() {
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

        let room_name = format!("diff-update-subscriptions-events-{}", unique_suffix());
        let oxidesfu_result = run_twirp_update_subscriptions_lifecycle_events(
            &format!("http://{oxidesfu_addr}"),
            &room_name,
        )
        .await;
        let go_result =
            run_twirp_update_subscriptions_lifecycle_events(&go_base_url, &room_name).await;

        assert_eq!(oxidesfu_result, go_result);

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

    async fn run_twirp_update_subscriptions_lifecycle_events(
        base_url: &str,
        room_name: &str,
    ) -> UpdateSubscriptionsLifecycleEventParity {
        let publisher_identity = format!("diff-update-subscriptions-publisher-{}", unique_suffix());
        let subscriber_identity = format!("diff-update-subscriptions-subscriber-{}", unique_suffix());

        let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity(&publisher_identity)
            .with_name("Differential UpdateSubscriptions Publisher")
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
            .with_identity(&subscriber_identity)
            .with_name("Differential UpdateSubscriptions Subscriber")
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
        let (subscriber_room, mut subscriber_events) = Room::connect(base_url, &subscriber_token, options)
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
        let published_sid = publication.sid().to_string();

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackSubscribed { publication, .. } = event
                    && publication.sid().to_string() == published_sid
                {
                    break;
                }
            }
        })
        .await
        .expect("subscriber should receive initial TrackSubscribed");

        let room_client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(5));

        room_client
            .update_subscriptions(room_name, &subscriber_identity, vec![published_sid.clone()], false)
            .await
            .expect("update_subscriptions unsubscribe should succeed");

        let saw_track_unsubscribed = tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackUnsubscribed { publication, .. } = event
                    && publication.sid().to_string() == published_sid
                {
                    break;
                }
            }
        })
        .await
        .is_ok();

        room_client
            .update_subscriptions(room_name, &subscriber_identity, vec![published_sid.clone()], true)
            .await
            .expect("update_subscriptions subscribe should succeed");

        let saw_track_resubscribed = tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let event = subscriber_events
                    .recv()
                    .await
                    .expect("subscriber events should stay open");
                if let RoomEvent::TrackSubscribed { publication, .. } = event
                    && publication.sid().to_string() == published_sid
                {
                    break;
                }
            }
        })
        .await
        .is_ok();

        let _ = publisher_room.close().await;
        let _ = subscriber_room.close().await;

        UpdateSubscriptionsLifecycleEventParity {
            saw_track_unsubscribed,
            saw_track_resubscribed,
        }
    }

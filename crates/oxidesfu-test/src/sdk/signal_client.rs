use super::*;

    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::{rtc_v1_join_defaults_auto_subscribe_true_when_parameter_absent, rtc_v1_websocket_accepts_access_token_query_parameter, rtc_v0_and_rtc_v1_join_responses_are_compatible_for_shared_fields}
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_signal_client_connect_v1_receives_join_response() {
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
            .with_identity("sdk-alice")
            .with_name("SDK Alice")
            .with_grants(VideoGrants {
                room_join: true,
                room: "sdk-room".to_string(),
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
                .expect("SDK signal client should connect to OxideSFU /rtc/v1");

        assert!(client.is_single_pc_mode_active());
        assert_eq!(
            join.room.expect("join room should be present").name,
            "sdk-room"
        );
        let participant = join
            .participant
            .expect("join participant should be present");
        assert_eq!(participant.identity, "sdk-alice");
        assert_eq!(participant.name, "SDK Alice");
        assert!(join.ping_interval > 0);
        assert!(join.ping_timeout > join.ping_interval);

        client.close().await;
        server.abort();
    }

    async fn assert_signal_reconnect_reason_roundtrip(
        base_url: &str,
        reconnect_reason: proto::ReconnectReason,
        room_prefix: &str,
        identity_prefix: &str,
    ) {
        let room_name = format!("{room_prefix}-{}", unique_suffix());
        let identity = format!("{identity_prefix}-{}", unique_suffix());
        let result = run_signal_reconnect_reason_response(
            base_url,
            &room_name,
            &identity,
            reconnect_reason,
        )
        .await;

        assert!(
            result.participant_sid_present,
            "reconnect reason {reconnect_reason:?} should preserve participant SID on reconnect path"
        );
    }

    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::{rtc_v1_reconnect_reason_matrix_returns_reconnect_and_pongresp, reconnect_then_old_socket_late_leave_does_not_remove_new_session}
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_signal_client_reconnect_rr_switch_candidate_preserves_participant_sid() {
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

        let base_url = format!("http://{addr}");
        assert_signal_reconnect_reason_roundtrip(
            &base_url,
            proto::ReconnectReason::RrSwitchCandidate,
            "sdk-signal-rr-switch-candidate",
            "sdk-signal-rr-switch-candidate-identity",
        )
        .await;

        server.abort();
    }

    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::rtc_v1_reconnect_reason_matrix_returns_reconnect_and_pongresp
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_signal_client_reconnect_rr_subscriber_failed_preserves_participant_sid() {
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

        let base_url = format!("http://{addr}");
        assert_signal_reconnect_reason_roundtrip(
            &base_url,
            proto::ReconnectReason::RrSubscriberFailed,
            "sdk-signal-rr-subscriber-failed",
            "sdk-signal-rr-subscriber-failed-identity",
        )
        .await;

        server.abort();
    }

    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::rtc_v1_reconnect_reason_matrix_returns_reconnect_and_pongresp
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_signal_client_reconnect_rr_signal_disconnected_preserves_participant_sid() {
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

        let base_url = format!("http://{addr}");
        assert_signal_reconnect_reason_roundtrip(
            &base_url,
            proto::ReconnectReason::RrSignalDisconnected,
            "sdk-signal-rr-signal-disconnected",
            "sdk-signal-rr-signal-disconnected-identity",
        )
        .await;

        server.abort();
    }

    // TEST_LIFECYCLE: SUPERSEDED
    // REPLACED_BY: oxidesfu-signaling/src/router/tests.rs::rtc_v1_reconnect_reason_matrix_returns_reconnect_and_pongresp
    // REMOVAL_PLAN: delete after docs-map lifecycle sign-off and two green conformance cycles.
    #[tokio::test]
    #[ignore = "TEST_LIFECYCLE SUPERSEDED: replaced by direct crate-owned coverage"]
    async fn rust_sdk_signal_client_reconnect_rr_publisher_failed_preserves_participant_sid() {
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

        let base_url = format!("http://{addr}");
        assert_signal_reconnect_reason_roundtrip(
            &base_url,
            proto::ReconnectReason::RrPublisherFailed,
            "sdk-signal-rr-publisher-failed",
            "sdk-signal-rr-publisher-failed-identity",
        )
        .await;

        server.abort();
    }

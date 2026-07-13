use super::*;

#[tokio::test]
    async fn differential_twirp_room_lifecycle_matches_go_livekit_dev() {
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

        let room_name = format!("diff-room-{}", unique_suffix());
        let metadata = format!("diff-metadata-{}", unique_suffix());

        let ferrite_lifecycle =
            run_room_lifecycle(&format!("http://{oxidesfu_addr}"), &room_name, &metadata).await;
        let go_lifecycle = run_room_lifecycle(&go_base_url, &room_name, &metadata).await;

        assert_eq!(ferrite_lifecycle.created_name, go_lifecycle.created_name);
        assert_eq!(
            ferrite_lifecycle.created_metadata,
            go_lifecycle.created_metadata
        );
        assert_eq!(
            ferrite_lifecycle.listed_after_create,
            go_lifecycle.listed_after_create
        );
        assert_eq!(
            ferrite_lifecycle.listed_after_delete,
            go_lifecycle.listed_after_delete
        );

        let _ = go_livekit.kill().await;
        oxidesfu_server.abort();
    }

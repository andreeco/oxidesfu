use super::*;

#[derive(Clone, Default)]
struct WebhookEventStore {
    inner: Arc<Mutex<HashMap<String, proto::WebhookEvent>>>,
}

impl WebhookEventStore {
    fn record(&self, event: proto::WebhookEvent) {
        self.inner
            .lock()
            .expect("webhook event store lock should not be poisoned")
            .insert(event.event.clone(), event);
    }

    fn get(&self, name: &str) -> Option<proto::WebhookEvent> {
        self.inner
            .lock()
            .expect("webhook event store lock should not be poisoned")
            .get(name)
            .cloned()
    }

    fn clear(&self) {
        self.inner
            .lock()
            .expect("webhook event store lock should not be poisoned")
            .clear();
    }
}

async fn spawn_single_node_with_webhook_store() -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    WebhookEventStore,
    tokio::task::JoinHandle<()>,
) {
    let webhook_store = WebhookEventStore::default();
    let webhook_store_for_handler = webhook_store.clone();

    let webhook_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("webhook listener should bind");
    let webhook_addr = webhook_listener
        .local_addr()
        .expect("webhook listener should have local addr");

    let webhook_app = axum::Router::new()
        .route(
            "/",
            axum::routing::post(move |body: axum::body::Bytes| {
                let store = webhook_store_for_handler.clone();
                async move {
                    if let Ok(event) = serde_json::from_slice::<proto::WebhookEvent>(&body) {
                        store.record(event);
                    }
                    axum::http::StatusCode::OK
                }
            }),
        )
        .route("/", axum::routing::get(|| async { axum::http::StatusCode::OK }));
    let webhook_server = tokio::spawn(async move {
        axum::serve(webhook_listener, webhook_app)
            .await
            .expect("webhook test server should run");
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");

    let mut config = oxidesfu_core::ServerConfig::development();
    config.webhook_api_key = Some(API_KEY.to_string());
    config.webhook_urls = vec![format!("http://127.0.0.1:{}/", webhook_addr.port())];

    let api_state = oxidesfu_server::api_state_from_config(&config);

    let webhook_dispatcher = oxidesfu_server::WebhookDispatcher::from_server_config(&config)
        .map(Arc::new)
        .expect("webhook dispatcher should be configured for test server");
    let signal_state = oxidesfu_signaling::SignalState::with_data_channels(
        api_state.rooms.clone(),
        api_state.auth.clone(),
        api_state.data_channels.clone(),
    )
    .with_webhook_event_handler(Some(webhook_dispatcher.signal_webhook_handler()));

    let app = oxidesfu_server::app_with_api_signal_state_readiness_and_webhooks(
        api_state,
        signal_state,
        None,
        Arc::new(oxidesfu_server::AlwaysReadyRelayBackendReadiness),
        Some(webhook_dispatcher),
    );

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("test server should run");
    });

    (addr, server, webhook_store, webhook_server)
}

async fn wait_for_webhook_event(
    store: &WebhookEventStore,
    event_name: &str,
    context: &str,
) -> proto::WebhookEvent {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(event) = store.get(event_name) {
                break event;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect(context)
}

// Upstream: livekit/test/webhook_test.go::TestWebhooks
#[tokio::test]
async fn test_webhooks() {
    let (addr, server, store, webhook_server) = spawn_single_node_with_webhook_store().await;
    let url = base_url(addr);
    let room = format!("upstream-webhooks-{}", unique_suffix());

    let (c1_room, _c1_events) = connect_room(&url, &room, "c1", true).await;

    let started = wait_for_webhook_event(&store, "room_started", "should receive room_started").await;
    let joined =
        wait_for_webhook_event(&store, "participant_joined", "should receive participant_joined")
            .await;
    assert_eq!(started.room.as_ref().map(|r| r.name.as_str()), Some(room.as_str()));
    assert!(!started.id.is_empty());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after UNIX_EPOCH")
        .as_secs() as i64;
    assert!(started.created_at >= now - 100);
    assert!(started.created_at <= now);
    assert_eq!(
        joined
            .participant
            .as_ref()
            .map(|participant| participant.identity.as_str()),
        Some("c1")
    );
    store.clear();

    let (c2_room, _c2_events) = connect_room(&url, &room, "c2", true).await;
    let joined =
        wait_for_webhook_event(&store, "participant_joined", "should receive c2 participant_joined")
            .await;
    assert_eq!(
        joined
            .participant
            .as_ref()
            .map(|participant| participant.identity.as_str()),
        Some("c2")
    );
    store.clear();

    let _c1_audio_sid = publish_audio_track(&c1_room, "webcam-audio").await.0;
    let _c1_video_sid = publish_video_track(&c1_room, "webcam-video").await;
    let c1_sid = c1_room.local_participant().sid().to_string();
    let track_published =
        wait_for_webhook_event(&store, "track_published", "should receive track_published")
            .await;
    assert!(track_published.track.is_some(), "track_published should include track info");
    assert_eq!(
        track_published
            .participant
            .as_ref()
            .map(|participant| participant.sid.as_str()),
        Some(c1_sid.as_str())
    );
    store.clear();

    let _ = c1_room.close().await;
    let left = wait_for_webhook_event(&store, "participant_left", "should receive participant_left").await;
    assert_eq!(
        left.participant
            .as_ref()
            .map(|participant| participant.identity.as_str()),
        Some("c1")
    );
    store.clear();

    room_client(addr)
        .delete_room(&room)
        .await
        .expect("delete_room should succeed and trigger room_finished webhook");
    let finished = wait_for_webhook_event(&store, "room_finished", "should receive room_finished").await;
    assert_eq!(finished.room.as_ref().map(|r| r.name.as_str()), Some(room.as_str()));

    let _ = c2_room.close().await;
    server.abort();
    webhook_server.abort();
}

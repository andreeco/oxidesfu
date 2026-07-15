use std::{error::Error, sync::Arc, time::Duration};

use oxidesfu_core::{RoomNodeDirectoryBackend, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    oxidesfu_server::init_tracing()?;

    let config = ServerConfig::from_env_args_or_development(std::env::args().skip(1))?;
    oxidesfu_server::validate_turn_runtime_from_config(&config).await?;
    let turn_runtime = oxidesfu_server::start_turn_runtime(&config).await?;
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, "starting OxideSFU server");

    let webhook_dispatcher =
        oxidesfu_server::WebhookDispatcher::from_server_config(&config).map(Arc::new);

    let api_state = oxidesfu_server::api_state_from_config(&config);
    let room_nodes = oxidesfu_server::room_node_directory_from_config(&config)?;
    let registered_node = oxidesfu_server::register_local_room_node(&room_nodes, &config)?;
    tracing::info!(
        room_node_id = %registered_node.id,
        room_node_region = %registered_node.region,
        "registered local room node"
    );

    let (node_registration_shutdown_tx, node_registration_shutdown_rx) =
        tokio::sync::oneshot::channel();
    let node_registration_task = oxidesfu_server::spawn_room_node_registration_task(
        room_nodes.clone(),
        registered_node.clone(),
        Duration::from_millis(500),
        node_registration_shutdown_rx,
    );

    let (cleanup_shutdown_tx, cleanup_shutdown_rx) = tokio::sync::oneshot::channel();
    let room_finished_handler = webhook_dispatcher.clone().map(|dispatcher| {
        Arc::new(move |room: livekit_protocol::Room| {
            dispatcher.emit(oxidesfu_server::room_finished_webhook_event(room));
        }) as Arc<dyn Fn(livekit_protocol::Room) + Send + Sync>
    });
    let cleanup_task = oxidesfu_server::spawn_room_cleanup_task_with_room_finished_handler(
        api_state.rooms.clone(),
        config.room_cleanup_interval,
        config.empty_room_max_age,
        cleanup_shutdown_rx,
        room_finished_handler,
    );

    let local_room_node_id = registered_node.id.clone();
    let room_nodes_for_shutdown = room_nodes.clone();

    let mut relay_worker_shutdown_tx = None;
    let mut relay_worker_task = None;
    let mut worker_mailbox = None;
    let mut worker_readiness = None;

    let (non_local_relay_dispatcher, relay_backend_readiness): (
        Arc<dyn oxidesfu_signaling::NonLocalRelayDispatcher>,
        Arc<dyn oxidesfu_server::RelayBackendReadiness>,
    ) = if config.room_node_directory_backend == RoomNodeDirectoryBackend::Redis {
        if let Some(redis_url) = config.redis_url.as_deref() {
            let relay_store = oxidesfu_room::RedisHashClient::from_url(redis_url)?;
            let dispatcher_mailbox =
                oxidesfu_signaling::RedisRelayMailbox::with_store(relay_store.clone());
            worker_mailbox = Some(oxidesfu_signaling::RedisRelayMailbox::with_store(
                relay_store,
            ));

            let readiness = Arc::new(oxidesfu_server::RelayWorkerReadiness::new(true));
            worker_readiness = Some(readiness.clone());
            (
                Arc::new(
                    oxidesfu_signaling::RedisMailboxRelayDispatcher::with_mailbox_and_timing(
                        dispatcher_mailbox,
                        Arc::new(oxidesfu_signaling::NoopRelayIntentExecutionDriver),
                        oxidesfu_server::DEFAULT_RELAY_RESPONSE_POLL_INTERVAL,
                        oxidesfu_server::DEFAULT_RELAY_RESPONSE_WAIT_TIMEOUT,
                    ),
                ),
                readiness,
            )
        } else {
            (
                Arc::new(oxidesfu_signaling::NoopNonLocalRelayDispatcher),
                Arc::new(oxidesfu_server::AlwaysReadyRelayBackendReadiness),
            )
        }
    } else {
        (
            Arc::new(oxidesfu_signaling::NoopNonLocalRelayDispatcher),
            Arc::new(oxidesfu_server::AlwaysReadyRelayBackendReadiness),
        )
    };

    let webhook_event_handler = webhook_dispatcher
        .clone()
        .map(|dispatcher| dispatcher.signal_webhook_handler());

    let signal_state = oxidesfu_signaling::SignalState::with_data_channels_room_nodes_placement_and_relay_dispatcher(
        api_state.rooms.clone(),
        api_state.auth.clone(),
        api_state.data_channels.clone(),
        Some(room_nodes.clone()),
        Some(registered_node.id.clone()),
        config.reject_non_local_room_placement,
        non_local_relay_dispatcher,
    )
    .with_ice_servers(oxidesfu_server::signal_ice_servers_from_config(&config))
    .with_room_auto_create(config.room_auto_create)
    .with_rtc_transport_config(oxidesfu_server::rtc_transport_config_from_server_config(
            &config,
        ))
        .with_datachannel_slow_threshold_bytes(config.datachannel_slow_threshold)
        .with_participant_data_blob_enabled(config.participant_data_blob_enabled)
        .with_webhook_event_handler(webhook_event_handler);
    let signal_state = if config.turn_enabled {
        let turn_config = config.clone();
        signal_state.with_ice_server_provider(move |participant_sid| {
            oxidesfu_server::signal_ice_servers_for_participant(&turn_config, participant_sid)
        })
    } else {
        signal_state
    };

    if let (Some(mailbox), Some(readiness)) = (worker_mailbox, worker_readiness) {
        let (relay_shutdown_tx, relay_shutdown_rx) = tokio::sync::oneshot::channel();
        let worker = oxidesfu_server::spawn_relay_intent_worker(
            mailbox,
            registered_node.id.clone(),
            Arc::new(
                oxidesfu_server::RoomStoreRelayJoinIntentExecutor::with_signal_state(
                    signal_state.clone(),
                ),
            ),
            oxidesfu_server::DEFAULT_RELAY_WORKER_INTERVAL,
            relay_shutdown_rx,
            readiness,
        );
        relay_worker_shutdown_tx = Some(relay_shutdown_tx);
        relay_worker_task = Some(worker);
    }

    let app = oxidesfu_server::app_with_api_signal_state_readiness_webhooks_and_agent_relay(
        api_state,
        signal_state,
        Some(room_nodes),
        relay_backend_readiness,
        webhook_dispatcher,
        config.redis_url.clone(),
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = node_registration_shutdown_tx.send(());
            oxidesfu_server::begin_graceful_shutdown(
                &room_nodes_for_shutdown,
                &local_room_node_id,
                cleanup_shutdown_tx,
            );
            if let Some(tx) = relay_worker_shutdown_tx {
                let _ = tx.send(());
            }
        })
        .await?;

    if let Some(turn_runtime) = turn_runtime {
        turn_runtime.shutdown().await?;
    }

    if let Err(join_error) = cleanup_task.await {
        tracing::warn!(error = %join_error, "room cleanup task join failed");
    }

    if let Err(join_error) = node_registration_task.await {
        tracing::warn!(error = %join_error, "room node registration task join failed");
    }

    if let Some(worker) = relay_worker_task
        && let Err(join_error) = worker.await
    {
        tracing::warn!(error = %join_error, "relay worker task join failed");
    }

    Ok(())
}

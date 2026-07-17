use std::{error::Error, fs, io, sync::Arc, time::Duration};

use oxidesfu_core::{RoomNodeDirectoryBackend, ServerConfig, translate_livekit_yaml};

enum Invocation {
    Serve(Vec<String>),
    ServeLiveKit(String),
    CheckLiveKit(String),
    TranslateLiveKit(String),
}

fn parse_invocation(args: Vec<String>) -> Result<Invocation, io::Error> {
    let Some(first) = args.first() else {
        return Ok(Invocation::Serve(args));
    };
    if first == "--livekit-config" {
        let [_, path] = args.as_slice() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: oxidesfu-server --livekit-config <livekit.yaml>",
            ));
        };
        return Ok(Invocation::ServeLiveKit(path.clone()));
    }
    if first != "config" {
        return Ok(Invocation::Serve(args));
    }
    let [_, command, path] = args.as_slice() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: oxidesfu-server config <check-livekit|translate-livekit> <livekit.yaml>",
        ));
    };
    match command.as_str() {
        "check-livekit" => Ok(Invocation::CheckLiveKit(path.clone())),
        "translate-livekit" => Ok(Invocation::TranslateLiveKit(path.clone())),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unknown config command; expected check-livekit or translate-livekit",
        )),
    }
}

fn run_config_command(invocation: &Invocation) -> Result<bool, Box<dyn Error>> {
    let (path, translate) = match invocation {
        Invocation::Serve(_) | Invocation::ServeLiveKit(_) => return Ok(false),
        Invocation::CheckLiveKit(path) => (path, false),
        Invocation::TranslateLiveKit(path) => (path, true),
    };
    let yaml = fs::read_to_string(path)?;
    let (config, report) = translate_livekit_yaml(&yaml)?;
    if translate {
        // Deliberately omit API secrets. Generated output is a review aid, not a
        // secret-export mechanism; operators should continue using Docker secrets.
        println!("OXIDESFU_BIND={}", config.bind);
        println!("OXIDESFU_ROOM_AUTO_CREATE={}", config.room_auto_create);
        println!("OXIDESFU_RTC_TCP_PORT={}", config.rtc_tcp_port);
        if let Some(url) = config.redis_url {
            println!("OXIDESFU_REDIS_URL={url}");
        }
        println!(
            "# translated LiveKit fields: {}",
            report.translated.join(", ")
        );
    } else {
        println!("LiveKit configuration is supported by the current strict migration subset.");
        println!("Translated fields: {}", report.translated.join(", "));
    }
    Ok(true)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let invocation = parse_invocation(std::env::args().skip(1).collect())?;
    if run_config_command(&invocation)? {
        return Ok(());
    }
    let config = match invocation {
        Invocation::Serve(args) => ServerConfig::from_env_args_or_development(args)?,
        Invocation::ServeLiveKit(path) => {
            let yaml = fs::read_to_string(path)?;
            let (config, report) = translate_livekit_yaml(&yaml)?;
            eprintln!(
                "starting from strict LiveKit YAML compatibility subset; translated fields: {}",
                report.translated.join(", ")
            );
            config
        }
        Invocation::CheckLiveKit(_) | Invocation::TranslateLiveKit(_) => {
            unreachable!("config commands return before server startup")
        }
    };
    oxidesfu_server::init_tracing()?;
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

#[cfg(test)]
mod tests {
    use super::{Invocation, parse_invocation};

    #[test]
    fn config_check_command_is_dispatched_before_server_startup() {
        let invocation = parse_invocation(vec![
            "config".to_string(),
            "check-livekit".to_string(),
            "livekit.yaml".to_string(),
        ])
        .expect("config command should parse");
        assert!(matches!(invocation, Invocation::CheckLiveKit(path) if path == "livekit.yaml"));
    }

    #[test]
    fn livekit_config_startup_mode_requires_only_the_yaml_path() {
        let invocation = parse_invocation(vec![
            "--livekit-config".to_string(),
            "livekit.yaml".to_string(),
        ])
        .expect("LiveKit startup mode should parse");
        assert!(matches!(invocation, Invocation::ServeLiveKit(path) if path == "livekit.yaml"));
        assert!(parse_invocation(vec!["--livekit-config".to_string()]).is_err());
    }

    #[test]
    fn ordinary_server_arguments_remain_unchanged() {
        let args = vec!["--bind".to_string(), "127.0.0.1:7880".to_string()];
        let invocation = parse_invocation(args.clone()).expect("server arguments should parse");
        assert!(matches!(invocation, Invocation::Serve(actual) if actual == args));
    }
}

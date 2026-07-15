use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::{
    DEFAULT_EMPTY_ROOM_MAX_AGE, DEFAULT_ROOM_CLEANUP_INTERVAL, DEFAULT_TRACING_ENV_FILTER,
    RelayBackendReadiness, RelayJoinIntentExecutor, RelayWorkerReadiness,
    RoomStoreRelayJoinIntentExecutor, X_REQUEST_ID, api_state_from_config, app,
    app_with_api_and_room_nodes, app_with_api_room_nodes_from_config,
    app_with_api_room_nodes_relay_dispatcher_and_readiness, begin_graceful_shutdown,
    log_request_completion, register_local_room_node, request_id_from_headers,
    room_node_directory_from_config, room_node_directory_from_config_with_factory,
    rtc_transport_config_from_server_config, set_local_room_node_draining,
    signal_ice_servers_from_config, spawn_relay_intent_worker, spawn_room_cleanup_task,
    spawn_room_cleanup_task_with_room_finished_handler, validate_turn_runtime_from_config,
};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Request, StatusCode},
};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use oxidesfu_auth::{Claims, VideoGrants};
use oxidesfu_core::{
    NodeSelectorAlgorithm as CoreNodeSelectorAlgorithm, NodeSelectorKind, NodeSelectorRegion,
    NodeSelectorSortBy as CoreNodeSelectorSortBy, RoomNodeDirectoryBackend, ServerConfig,
};
use oxidesfu_room::{
    NodeSelectorAlgorithm as RoomNodeSelectorAlgorithm, NodeSelectorConfig,
    NodeSelectorKind as RoomNodeSelectorKind, NodeSelectorSortBy as RoomNodeSelectorSortBy,
    RedisHashStore, RegisteredNode, RoomNodeDirectory, RoomNodeRegistry, RoomNodeRegistryError,
    RoomStore,
};
use oxidesfu_signaling::{
    NonLocalRelayDispatcher, NonLocalRelayJoinIntent, NonLocalRelayJoinResponse,
    NoopNonLocalRelayDispatcher, RedisMailboxRelayDispatcher, RedisRelayMailbox,
};
use prost::Message;
use rtc_stun::message::{BINDING_REQUEST, BINDING_SUCCESS, Message as StunMessage, TransactionId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use tower::ServiceExt;
use tracing_subscriber::{fmt::MakeWriter, prelude::*, registry::Registry};

#[derive(Debug)]
struct FailingRelayBackendReadiness;

impl RelayBackendReadiness for FailingRelayBackendReadiness {
    fn is_ready(&self) -> bool {
        false
    }
}

#[derive(Clone)]
struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for BufferWriter {
    type Writer = BufferGuard;

    fn make_writer(&'a self) -> Self::Writer {
        BufferGuard(self.0.clone())
    }
}

struct BufferGuard(Arc<Mutex<Vec<u8>>>);

impl io::Write for BufferGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut inner = self.0.lock().expect("buffer lock should not be poisoned");
        inner.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn root_not_found_reflects_origin_header() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/")
                .header(axum::http::header::AUTHORIZATION, "bearer xyz")
                .header(axum::http::header::ORIGIN, "testhost.com")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("testhost.com")
    );
}

#[tokio::test]
async fn twirp_list_rooms_accepts_double_slash_path() {
    let config = ServerConfig::development();
    let token = room_list_token(&config, "double-slash-tester");

    let uri_double_slash_leading = axum::http::Uri::builder()
        .path_and_query("//twirp/livekit.RoomService/ListRooms")
        .build()
        .expect("double-slash leading uri should build");

    for uri in [
        "/twirp//livekit.RoomService/ListRooms"
            .parse::<axum::http::Uri>()
            .expect("double-slash segment uri should parse"),
        uri_double_slash_leading,
    ] {
        let body = serde_json::to_vec(&livekit_protocol::ListRoomsRequest::default())
            .expect("request should serialize to json");
        let response = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn healthz_returns_ok_json() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);

    let body = response
        .into_body()
        .collect()
        .await
        .expect("body should collect")
        .to_bytes();
    let decoded: serde_json::Value =
        serde_json::from_slice(&body).expect("health response should be JSON");

    assert_eq!(
        decoded,
        serde_json::json!({
            "service": "ferrite",
            "status": "ok",
        })
    );
}

#[test]
fn signal_ice_servers_from_config_appends_turn_servers_with_expected_credentials() {
    let mut config = ServerConfig::development();
    config.turn_domain = Some("turn.example.net".to_string());
    config.turn_udp_port = Some(3478);
    config.turn_tls_port = Some(5349);
    config.turn_username = Some("turn-user".to_string());
    config.turn_credential = Some("turn-pass".to_string());

    let servers = signal_ice_servers_from_config(&config);
    let turn = servers
        .iter()
        .find(|server| {
            server
                .urls
                .iter()
                .any(|url| url.starts_with("turn:") || url.starts_with("turns:"))
        })
        .expect("turn server entry should be present");

    assert_eq!(
        turn.urls,
        vec![
            "turn:turn.example.net:3478?transport=udp",
            "turns:turn.example.net:5349?transport=tcp",
        ]
    );
    assert_eq!(turn.username, "turn-user");
    assert_eq!(turn.credential, "turn-pass");
}

#[test]
fn signal_ice_servers_from_config_uses_api_credentials_when_turn_credentials_are_unset() {
    let mut config = ServerConfig::development();
    config.api_key = "fallback-key".to_string();
    config.api_secret = "fallback-secret".to_string();
    config.turn_domain = Some("turn.example.net".to_string());
    config.turn_udp_port = Some(3478);

    let servers = signal_ice_servers_from_config(&config);
    let turn = servers
        .iter()
        .find(|server| server.urls.iter().any(|url| url.starts_with("turn:")))
        .expect("turn server entry should be present");

    assert_eq!(turn.username, "fallback-key");
    assert_eq!(turn.credential, "fallback-secret");
}

#[test]
fn signal_ice_servers_from_config_preserves_custom_ice_order_and_appends_turn_entry_last() {
    let mut config = ServerConfig::development();
    config.ice_servers = vec![
        oxidesfu_core::IceServerConfig {
            urls: vec!["stun:stun-a.example.net:3478".to_string()],
            username: String::new(),
            credential: String::new(),
        },
        oxidesfu_core::IceServerConfig {
            urls: vec!["stun:stun-b.example.net:3478".to_string()],
            username: String::new(),
            credential: String::new(),
        },
    ];
    config.turn_domain = Some("turn.example.net".to_string());
    config.turn_udp_port = Some(3478);

    let servers = signal_ice_servers_from_config(&config);

    assert_eq!(servers.len(), 3);
    assert_eq!(servers[0].urls, vec!["stun:stun-a.example.net:3478"]);
    assert_eq!(servers[1].urls, vec!["stun:stun-b.example.net:3478"]);
    assert_eq!(
        servers[2].urls,
        vec!["turn:turn.example.net:3478?transport=udp"]
    );
}

#[test]
fn signal_ice_servers_from_config_omits_turn_entry_when_turn_domain_or_ports_are_missing() {
    let mut no_domain = ServerConfig::development();
    no_domain.turn_udp_port = Some(3478);
    let no_domain_servers = signal_ice_servers_from_config(&no_domain);
    assert!(
        no_domain_servers
            .iter()
            .all(|server| server.urls.iter().all(|url| !url.starts_with("turn:"))),
        "turn URLs require turn_domain"
    );

    let mut no_ports = ServerConfig::development();
    no_ports.turn_domain = Some("turn.example.net".to_string());
    let no_ports_servers = signal_ice_servers_from_config(&no_ports);
    assert!(
        no_ports_servers
            .iter()
            .all(|server| server.urls.iter().all(|url| !url.starts_with("turn:"))),
        "turn URLs require at least one turn port"
    );
}

#[test]
fn signal_ice_servers_from_config_turn_tls_uses_configured_turn_domain() {
    let mut config = ServerConfig::development();
    config.bind = "192.168.1.10:7880".parse().expect("bind should parse");
    config.turn_domain = Some("turn.public.example.net".to_string());
    config.turn_tls_port = Some(5349);

    let servers = signal_ice_servers_from_config(&config);
    let turn = servers
        .iter()
        .find(|server| server.urls.iter().any(|url| url.starts_with("turns:")))
        .expect("turns URL should be present");
    assert_eq!(
        turn.urls,
        vec!["turns:turn.public.example.net:5349?transport=tcp"]
    );
}

#[tokio::test]
async fn validate_turn_runtime_from_config_skips_when_probe_is_disabled() {
    let mut config = ServerConfig::development();
    config.turn_domain = Some("invalid-turn-domain-for-disabled-probe.invalid".to_string());
    config.turn_udp_port = Some(3478);
    config.turn_require_reachable = false;

    let result = validate_turn_runtime_from_config(&config).await;
    assert!(result.is_ok(), "disabled probe should be a no-op");
}

#[tokio::test]
async fn validate_turn_runtime_from_config_fails_on_unreachable_tcp_turn_endpoint() {
    let mut config = ServerConfig::development();
    let port = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind")
        .local_addr()
        .expect("listener should have local addr")
        .port();
    config.turn_domain = Some("127.0.0.1".to_string());
    config.turn_tls_port = Some(port);
    config.turn_require_reachable = true;
    config.turn_probe_timeout_ms = 250;

    let result = validate_turn_runtime_from_config(&config).await;
    assert!(
        result.is_err(),
        "unreachable TCP TURN endpoint should fail runtime validation"
    );
}

#[tokio::test]
async fn validate_turn_runtime_from_config_accepts_reachable_tcp_turn_endpoint() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let port = listener
        .local_addr()
        .expect("listener should have local addr")
        .port();
    let accept_task = tokio::spawn(async move {
        let _ = listener.accept().await;
    });

    let mut config = ServerConfig::development();
    config.turn_domain = Some("127.0.0.1".to_string());
    config.turn_tls_port = Some(port);
    config.turn_require_reachable = true;
    config.turn_probe_timeout_ms = 500;

    let result = validate_turn_runtime_from_config(&config).await;
    assert!(
        result.is_ok(),
        "reachable TCP TURN endpoint should pass runtime validation"
    );
    let _ = accept_task.await;
}

#[tokio::test]
async fn validate_turn_runtime_from_config_accepts_reachable_udp_stun_endpoint() {
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("udp listener should bind");
    let port = udp
        .local_addr()
        .expect("udp listener should have local addr")
        .port();
    let responder = tokio::spawn(async move {
        let mut buf = vec![0_u8; 2048];
        if let Ok((len, peer)) = udp.recv_from(&mut buf).await {
            let mut request = StunMessage::new();
            if request.unmarshal_binary(&buf[..len]).is_ok() {
                let mut response = StunMessage::new();
                if response
                    .build(&[Box::new(request.transaction_id), Box::new(BINDING_SUCCESS)])
                    .is_ok()
                {
                    let _ = udp.send_to(&response.raw, peer).await;
                }
            }
        }
    });

    let mut config = ServerConfig::development();
    config.turn_domain = Some("127.0.0.1".to_string());
    config.turn_udp_port = Some(port);
    config.turn_require_reachable = true;
    config.turn_probe_timeout_ms = 600;

    let result = validate_turn_runtime_from_config(&config).await;
    assert!(
        result.is_ok(),
        "reachable UDP STUN/TURN endpoint should pass runtime validation"
    );
    let _ = responder.await;
}

#[tokio::test]
async fn validate_turn_runtime_from_config_rejects_non_stun_udp_endpoint() {
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("udp listener should bind");
    let port = udp
        .local_addr()
        .expect("udp listener should have local addr")
        .port();
    let responder = tokio::spawn(async move {
        let mut buf = vec![0_u8; 2048];
        if let Ok((len, peer)) = udp.recv_from(&mut buf).await {
            let mut request = StunMessage::new();
            if request.unmarshal_binary(&buf[..len]).is_ok() {
                let mut non_stun = StunMessage::new();
                let _ =
                    non_stun.build(&[Box::new(TransactionId::new()), Box::new(BINDING_REQUEST)]);
                let _ = udp.send_to(&non_stun.raw, peer).await;
            }
        }
    });

    let mut config = ServerConfig::development();
    config.turn_domain = Some("127.0.0.1".to_string());
    config.turn_udp_port = Some(port);
    config.turn_require_reachable = true;
    config.turn_probe_timeout_ms = 600;

    let result = validate_turn_runtime_from_config(&config).await;
    assert!(
        result.is_err(),
        "non-STUN UDP endpoint should fail runtime validation"
    );
    let _ = responder.await;
}

#[tokio::test]
async fn readyz_without_room_node_directory_reports_ready() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body should collect")
        .to_bytes();
    let decoded: serde_json::Value =
        serde_json::from_slice(&body).expect("ready response should be JSON");
    assert_eq!(decoded["status"], "ready");
}

#[tokio::test]
async fn readyz_not_ready_when_relay_backend_unavailable_but_healthz_ok() {
    let config = ServerConfig::development();
    let api_state = api_state_from_config(&config);
    let app = app_with_api_room_nodes_relay_dispatcher_and_readiness(
        api_state,
        None,
        None,
        false,
        Arc::new(NoopNonLocalRelayDispatcher),
        Arc::new(FailingRelayBackendReadiness),
    );

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond for health");
    assert_eq!(health.status(), StatusCode::OK);

    let ready = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond for ready");
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn readyz_with_room_node_directory_without_nodes_reports_not_ready() {
    let config = ServerConfig::development();
    let api_state = api_state_from_config(&config);
    let room_nodes: Arc<dyn RoomNodeDirectory> = Arc::new(RoomNodeRegistry::default());
    let app = app_with_api_and_room_nodes(api_state, Some(room_nodes));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn readyz_with_registered_room_node_reports_ready() {
    let config = ServerConfig::development();
    let api_state = api_state_from_config(&config);
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "local".to_string(),
        })
        .expect("node should register");
    room_nodes
        .set_node_draining("node-1", false)
        .expect("node should be serving");
    let app = app_with_api_and_room_nodes(api_state, Some(room_nodes));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn readyz_reports_not_ready_when_all_registered_nodes_are_draining() {
    let config = ServerConfig::development();
    let api_state = api_state_from_config(&config);
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "local".to_string(),
        })
        .expect("node should register");
    room_nodes
        .set_node_draining("node-1", true)
        .expect("node should be set to draining");
    let app = app_with_api_and_room_nodes(api_state, Some(room_nodes));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn readyz_transitions_from_ready_to_not_ready_when_node_drains() {
    let config = ServerConfig::development();
    let api_state = api_state_from_config(&config);
    let room_nodes = Arc::new(RoomNodeRegistry::default());
    room_nodes
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "local".to_string(),
        })
        .expect("node should register");
    room_nodes
        .set_node_draining("node-1", false)
        .expect("node should be serving");

    let app = app_with_api_and_room_nodes(api_state, Some(room_nodes.clone()));

    let before = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond before drain");
    assert_eq!(before.status(), StatusCode::OK);

    room_nodes
        .set_node_draining("node-1", true)
        .expect("draining transition should succeed");

    let after = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond after drain");
    assert_eq!(after.status(), StatusCode::SERVICE_UNAVAILABLE);
}

fn token_for_claims(config: &ServerConfig, claims: Claims) -> String {
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(config.api_secret.as_bytes()),
    )
    .expect("jwt should encode")
}

fn join_token(config: &ServerConfig, identity: &str, room: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs() as usize;

    token_for_claims(
        config,
        Claims {
            exp: now + 60,
            iss: config.api_key.clone(),
            sub: identity.to_string(),
            name: identity.to_string(),
            video: VideoGrants {
                room_join: true,
                room: room.to_string(),
                ..Default::default()
            },
            ..Default::default()
        },
    )
}

fn agent_token(config: &ServerConfig, identity: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs() as usize;

    token_for_claims(
        config,
        Claims {
            exp: now + 60,
            iss: config.api_key.clone(),
            sub: identity.to_string(),
            name: identity.to_string(),
            video: VideoGrants {
                agent: true,
                ..Default::default()
            },
            ..Default::default()
        },
    )
}

fn room_list_token(config: &ServerConfig, identity: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs() as usize;

    token_for_claims(
        config,
        Claims {
            exp: now + 60,
            iss: config.api_key.clone(),
            sub: identity.to_string(),
            name: identity.to_string(),
            video: VideoGrants {
                room_list: true,
                ..Default::default()
            },
            ..Default::default()
        },
    )
}

async fn spawn_server_for_config(
    config: &ServerConfig,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .expect("listener should bind configured address");
    let addr = listener
        .local_addr()
        .expect("listener should expose local address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let router =
        app_with_api_room_nodes_from_config(api_state_from_config(config), None, None, config);
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server should shut down cleanly");
    });

    (addr, shutdown_tx, server)
}

#[tokio::test]
async fn server_serves_rtc_validate_on_configured_bind_address() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:0".parse().expect("bind override should parse");

    let (addr, shutdown_tx, server) = spawn_server_for_config(&config).await;
    let token = join_token(&config, "alice", "bind-room");
    let response = reqwest::Client::new()
        .get(format!("http://{addr}/rtc/validate?access_token={token}"))
        .send()
        .await
        .expect("rtc validate request should complete");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body = response
        .text()
        .await
        .expect("rtc validate body should decode as utf8");
    assert_eq!(body, "success");

    shutdown_tx
        .send(())
        .expect("shutdown trigger should send once");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should complete without panic");
}

#[tokio::test]
async fn server_accepts_websocket_on_configured_bind_address() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:0".parse().expect("bind override should parse");

    let (addr, shutdown_tx, server) = spawn_server_for_config(&config).await;
    let token = join_token(&config, "ws-alice", "ws-bind-room");

    let (mut websocket, response) = connect_async(format!("ws://{addr}/rtc?access_token={token}"))
        .await
        .expect("websocket connect should succeed");
    assert_eq!(response.status(), reqwest::StatusCode::SWITCHING_PROTOCOLS);

    let join_message = tokio::time::timeout(Duration::from_secs(3), websocket.next())
        .await
        .expect("join response should arrive before timeout")
        .expect("websocket stream should remain open")
        .expect("join response frame should decode")
        .into_data();
    let signal = livekit_protocol::SignalResponse::decode(join_message.as_ref())
        .expect("join signal response should decode");
    let Some(livekit_protocol::signal_response::Message::Join(join)) = signal.message else {
        panic!("first server websocket frame should be Join response");
    };
    assert_eq!(
        join.room.as_ref().map(|room| room.name.as_str()),
        Some("ws-bind-room")
    );
    assert_eq!(
        join.participant
            .as_ref()
            .map(|participant| participant.identity.as_str()),
        Some("ws-alice")
    );

    websocket
        .close(None)
        .await
        .expect("websocket should close cleanly");

    shutdown_tx
        .send(())
        .expect("shutdown trigger should send once");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should complete without panic");
}

#[tokio::test]
async fn server_rejects_agent_websocket_without_agent_grant() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:0".parse().expect("bind override should parse");

    let (addr, shutdown_tx, server) = spawn_server_for_config(&config).await;
    let token = join_token(&config, "alice", "bind-room");

    let mut request = format!("ws://{addr}/agent")
        .into_client_request()
        .expect("agent websocket request should build");
    request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))
            .expect("authorization header should be valid"),
    );

    let err = connect_async(request)
        .await
        .expect_err("agent websocket without agent grant should fail");
    let tokio_tungstenite::tungstenite::Error::Http(response) = err else {
        panic!("expected http error response when agent grant is missing");
    };
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);

    shutdown_tx
        .send(())
        .expect("shutdown trigger should send once");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should complete without panic");
}

#[tokio::test]
async fn server_accepts_agent_registration_websocket_with_agent_grant() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:0".parse().expect("bind override should parse");

    let (addr, shutdown_tx, server) = spawn_server_for_config(&config).await;
    let token = agent_token(&config, "agent-worker");

    let mut request = format!("ws://{addr}/agent")
        .into_client_request()
        .expect("agent websocket request should build");
    request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))
            .expect("authorization header should be valid"),
    );

    let (mut websocket, response) = connect_async(request)
        .await
        .expect("agent websocket connect should succeed");
    assert_eq!(response.status(), reqwest::StatusCode::SWITCHING_PROTOCOLS);

    let register = livekit_protocol::WorkerMessage {
        message: Some(livekit_protocol::worker_message::Message::Register(
            livekit_protocol::RegisterWorkerRequest {
                r#type: livekit_protocol::JobType::JtRoom as i32,
                version: "version".to_string(),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
        )),
    };

    websocket
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            register.encode_to_vec().into(),
        ))
        .await
        .expect("register frame should send");

    let register_response = tokio::time::timeout(Duration::from_secs(3), websocket.next())
        .await
        .expect("register response should arrive before timeout")
        .expect("agent websocket should remain open")
        .expect("register response frame should decode")
        .into_data();

    let server_message = livekit_protocol::ServerMessage::decode(register_response.as_ref())
        .expect("server message should decode");
    let Some(livekit_protocol::server_message::Message::Register(register)) =
        server_message.message
    else {
        panic!("expected register worker response from agent websocket");
    };
    assert!(register.worker_id.starts_with("AW_"));
    assert!(register.server_info.is_some());

    websocket
        .close(None)
        .await
        .expect("agent websocket should close cleanly");

    shutdown_tx
        .send(())
        .expect("shutdown trigger should send once");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should complete without panic");
}

#[tokio::test]
async fn agent_websocket_closes_on_unknown_worker_message_variant() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:0".parse().expect("bind override should parse");

    let (addr, shutdown_tx, server) = spawn_server_for_config(&config).await;
    let token = agent_token(&config, "agent-worker");

    let mut request = format!("ws://{addr}/agent")
        .into_client_request()
        .expect("agent websocket request should build");
    request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))
            .expect("authorization header should be valid"),
    );

    let (mut websocket, _response) = connect_async(request)
        .await
        .expect("agent websocket connect should succeed");

    let register = livekit_protocol::WorkerMessage {
        message: Some(livekit_protocol::worker_message::Message::Register(
            livekit_protocol::RegisterWorkerRequest {
                r#type: livekit_protocol::JobType::JtRoom as i32,
                version: "version".to_string(),
                ..Default::default()
            },
        )),
    };
    websocket
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            register.encode_to_vec().into(),
        ))
        .await
        .expect("register frame should send");

    let _ = tokio::time::timeout(Duration::from_secs(3), websocket.next())
        .await
        .expect("register response should arrive before timeout");

    let invalid = livekit_protocol::WorkerMessage { message: None };
    websocket
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            invalid.encode_to_vec().into(),
        ))
        .await
        .expect("invalid worker frame should send");

    let closed = tokio::time::timeout(Duration::from_secs(3), websocket.next())
        .await
        .expect("socket should close after invalid worker message");
    assert!(
        matches!(
            closed,
            None | Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | Some(Err(_))
        ),
        "expected websocket close/termination after invalid worker message"
    );

    shutdown_tx
        .send(())
        .expect("shutdown trigger should send once");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should complete without panic");
}

#[tokio::test]
async fn server_serves_http_api_on_configured_port() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:0".parse().expect("bind override should parse");

    let (addr, shutdown_tx, server) = spawn_server_for_config(&config).await;
    let token = room_list_token(&config, "api-admin");

    let body = livekit_protocol::ListRoomsRequest::default().encode_to_vec();
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/twirp/livekit.RoomService/ListRooms"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(reqwest::header::CONTENT_TYPE, "application/protobuf")
        .body(body)
        .send()
        .await
        .expect("room service request should complete");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let bytes = response
        .bytes()
        .await
        .expect("room service response bytes should read");
    let decoded = livekit_protocol::ListRoomsResponse::decode(bytes.as_ref())
        .expect("room service protobuf response should decode");
    assert!(decoded.rooms.is_empty());

    shutdown_tx
        .send(())
        .expect("shutdown trigger should send once");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should complete without panic");
}

#[tokio::test]
async fn healthz_and_rtc_validate_work_behind_configured_bind() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:0".parse().expect("bind override should parse");

    let (addr, shutdown_tx, server) = spawn_server_for_config(&config).await;
    let client = reqwest::Client::new();

    let health = client
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .expect("healthz request should complete");
    assert_eq!(health.status(), reqwest::StatusCode::OK);

    let token = join_token(&config, "bob", "bind-room");
    let validate = client
        .get(format!("http://{addr}/rtc/validate?access_token={token}"))
        .send()
        .await
        .expect("rtc validate request should complete");
    assert_eq!(validate.status(), reqwest::StatusCode::OK);
    assert_eq!(
        validate
            .headers()
            .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("validate response should include CORS header"),
        "*"
    );

    shutdown_tx
        .send(())
        .expect("shutdown trigger should send once");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should complete without panic");
}

#[tokio::test]
async fn server_serves_healthz_over_tcp_and_graceful_shutdown_completes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        axum::serve(listener, app())
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server should shut down cleanly");
    });

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client should connect");
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("request should write");

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("response should read");
    let response_text = String::from_utf8(response).expect("response should be utf8");

    assert!(response_text.contains("200 OK"));
    assert!(response_text.contains("\"service\":\"ferrite\""));
    assert!(response_text.contains("\"status\":\"ok\""));

    shutdown_tx
        .send(())
        .expect("shutdown trigger should send once");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should complete without panic");
}

#[test]
fn rtc_transport_config_external_ip_mode_advertises_only_configured_node_ip() {
    let mut config = ServerConfig::development();
    config.bind = "192.168.1.50:7880".parse().expect("bind should parse");
    config.rtc_use_external_ip = true;
    config.rtc_node_ip = Some("198.51.100.33".to_string());

    let transport = rtc_transport_config_from_server_config(&config);
    assert_eq!(transport.nat_1to1_ips, vec!["198.51.100.33"]);
    assert!(
        !transport.nat_1to1_ips.iter().any(|ip| ip == "192.168.1.50"),
        "external IP mode should not advertise private bind address as nat_1to1"
    );
}

#[test]
fn rtc_transport_config_widens_loopback_signaling_bind_for_browser_ice() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:7880".parse().expect("socket should parse");
    config.rtc_udp_port = Some(50_100);

    let transport = rtc_transport_config_from_server_config(&config);

    assert_eq!(transport.udp_addrs, vec!["0.0.0.0:50100"]);
}

#[test]
fn rtc_transport_config_from_server_config_does_not_bind_fixed_tcp_per_peer_connection() {
    let mut enabled = ServerConfig::development();
    enabled.rtc_allow_tcp_fallback = true;
    enabled.rtc_use_external_ip = true;
    enabled.rtc_node_ip = Some("203.0.113.10".to_string());
    enabled.rtc_udp_port = Some(50100);
    enabled.rtc_tcp_port = 7991;

    let enabled_transport = rtc_transport_config_from_server_config(&enabled);
    assert_eq!(enabled_transport.udp_addrs, vec!["0.0.0.0:50100"]);
    assert!(
        enabled_transport.tcp_addrs.is_empty(),
        "fixed ICE/TCP ports require a shared listener; binding them per peer connection collides"
    );
    assert_eq!(enabled_transport.nat_1to1_ips, vec!["203.0.113.10"]);

    let mut disabled = enabled.clone();
    disabled.rtc_allow_tcp_fallback = false;
    let disabled_transport = rtc_transport_config_from_server_config(&disabled);
    assert_eq!(disabled_transport.udp_addrs, vec!["0.0.0.0:50100"]);
    assert!(disabled_transport.tcp_addrs.is_empty());
    assert_eq!(disabled_transport.nat_1to1_ips, vec!["203.0.113.10"]);

    let mut no_external_ip = enabled.clone();
    no_external_ip.rtc_use_external_ip = false;
    let no_external_ip_transport = rtc_transport_config_from_server_config(&no_external_ip);
    assert!(no_external_ip_transport.nat_1to1_ips.is_empty());

    let mut missing_node_ip = enabled.clone();
    missing_node_ip.rtc_node_ip = None;
    let missing_node_ip_transport = rtc_transport_config_from_server_config(&missing_node_ip);
    assert!(missing_node_ip_transport.nat_1to1_ips.is_empty());
}

#[test]
fn rtc_transport_config_from_server_config_expands_udp_port_range() {
    let mut config = ServerConfig::development();
    config.rtc_udp_port = None;
    config.rtc_udp_port_range_start = Some(50100);
    config.rtc_udp_port_range_end = Some(50102);
    config.rtc_allow_tcp_fallback = false;

    let transport = rtc_transport_config_from_server_config(&config);
    assert_eq!(
        transport.udp_addrs,
        vec![
            "0.0.0.0:50100".to_string(),
            "0.0.0.0:50101".to_string(),
            "0.0.0.0:50102".to_string(),
        ]
    );
    assert!(transport.tcp_addrs.is_empty());
}

#[test]
fn rtc_transport_config_from_server_config_uses_udp_mux_port_when_configured() {
    let mut config = ServerConfig::development();
    config.rtc_udp_port = Some(51820);
    config.rtc_udp_port_range_start = None;
    config.rtc_udp_port_range_end = None;

    let transport = rtc_transport_config_from_server_config(&config);
    assert_eq!(transport.udp_addrs, vec!["0.0.0.0:51820"]);
}

#[test]
fn rtc_transport_config_from_server_config_prefers_udp_mux_port_over_range() {
    let mut config = ServerConfig::development();
    config.rtc_udp_port = Some(51821);
    config.rtc_udp_port_range_start = Some(50100);
    config.rtc_udp_port_range_end = Some(50102);

    let transport = rtc_transport_config_from_server_config(&config);
    assert_eq!(transport.udp_addrs, vec!["0.0.0.0:51821"]);
}

#[test]
fn api_state_from_config_uses_configured_api_credentials() {
    let mut config = ServerConfig::development();
    config.bind = "127.0.0.1:7880".parse().expect("socket should parse");
    config.api_key = "custom-key".to_string();
    config.api_secret = "custom-secret".to_string();
    config.room_cleanup_interval = Duration::from_secs(30);
    config.empty_room_max_age = Duration::from_secs(60);
    config.room_node_directory_backend = RoomNodeDirectoryBackend::Memory;
    config.redis_url = None;
    config.reject_non_local_room_placement = false;
    config.ice_servers = vec![oxidesfu_core::IceServerConfig {
        urls: vec!["stun:stun.l.google.com:19302".to_string()],
        username: String::new(),
        credential: String::new(),
    }];
    config.rtc_udp_port = None;
    config.rtc_udp_port_range_start = None;
    config.rtc_udp_port_range_end = None;
    config.rtc_tcp_port = 7881;
    config.rtc_allow_tcp_fallback = true;
    config.rtc_tcp_fallback_rtt_threshold_ms = 0;
    config.rtc_allow_udp_unstable_fallback = false;
    config.rtc_use_external_ip = false;
    config.rtc_node_ip = None;
    config.turn_domain = None;
    config.turn_udp_port = None;
    config.turn_tls_port = None;
    config.turn_username = None;
    config.turn_credential = None;
    config.turn_require_reachable = false;
    config.turn_probe_timeout_ms = 1_500;
    config.webhook_api_key = None;
    config.webhook_urls = Vec::new();

    let state = api_state_from_config(&config);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs() as usize;
    let claims = Claims {
        exp: now + 60,
        iss: config.api_key.clone(),
        sub: "alice".to_string(),
        name: "Alice".to_string(),
        video: VideoGrants {
            room_join: true,
            room: "dev-room".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let jwt = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(config.api_secret.as_bytes()),
    )
    .expect("jwt should encode");

    let auth = state
        .auth
        .verify_authorization_header(&format!("Bearer {jwt}"))
        .expect("configured credentials should verify token");
    assert_eq!(auth.api_key, "custom-key");
    assert_eq!(auth.claims.sub, "alice");
}

#[tokio::test]
async fn healthz_echoes_request_id_header() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .header(X_REQUEST_ID, "client-req-123")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let response_request_id = response
        .headers()
        .get(X_REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .expect("response should include request id header");
    assert_eq!(response_request_id, "client-req-123");
}

#[tokio::test]
async fn healthz_generates_request_id_when_missing() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let response_request_id = response
        .headers()
        .get(X_REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .expect("response should include generated request id header");
    assert!(response_request_id.starts_with("req-"));
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_text() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body should collect")
        .to_bytes();
    let text = String::from_utf8(body.to_vec()).expect("metrics should be utf8");
    assert!(text.contains("# HELP oxidesfu_up"));
    assert!(text.contains("oxidesfu_up 1"));
    assert!(text.contains("oxidesfu_http_requests_total"));
    assert!(text.contains("oxidesfu_room_cleanup_removed_total"));
    assert!(text.contains("oxidesfu_relay_dispatch_attempts_total"));
    assert!(text.contains("oxidesfu_relay_dispatch_failures_total"));
    assert!(text.contains("oxidesfu_relay_responses_accepted_total"));
    assert!(text.contains("oxidesfu_relay_responses_rejected_total"));
    assert!(text.contains("oxidesfu_relay_fallback_to_local_total"));
    assert!(text.contains("oxidesfu_relay_signal_requests_total"));
    assert!(text.contains("oxidesfu_relay_signal_failures_total"));
    assert!(text.contains("oxidesfu_relay_signal_responses_total"));
}

#[tokio::test]
async fn metrics_http_request_counter_increments_after_request() {
    let before = read_metric_from_endpoint("oxidesfu_http_requests_total").await;

    let _ = app()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    let after = read_metric_from_endpoint("oxidesfu_http_requests_total").await;
    assert!(after >= before.saturating_add(1));
}

#[tokio::test]
async fn room_cleanup_task_stops_after_shutdown_signal() {
    let rooms = RoomStore::default();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = spawn_room_cleanup_task(
        rooms,
        Duration::from_millis(10),
        Duration::from_millis(10),
        shutdown_rx,
    );

    tokio::time::sleep(Duration::from_millis(25)).await;
    shutdown_tx
        .send(())
        .expect("cleanup shutdown signal should send once");

    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cleanup task should stop")
        .expect("cleanup task should finish without panic");
}

#[tokio::test]
async fn room_cleanup_task_emits_room_finished_handler_for_removed_rooms() {
    let rooms = RoomStore::default();
    rooms
        .create_room(livekit_protocol::CreateRoomRequest {
            name: "room-finished-a".to_string(),
            departure_timeout: 1,
            ..Default::default()
        })
        .expect("room should create");
    rooms
        .join_participant(
            "room-finished-a",
            "alice",
            "Alice",
            String::new(),
            std::collections::HashMap::new(),
        )
        .expect("participant should join");
    rooms
        .remove_participant("room-finished-a", "alice")
        .expect("participant should leave making room empty");

    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let emitted = Arc::new(Mutex::new(Vec::<String>::new()));
    let emitted_clone = emitted.clone();
    let room_finished_handler = Arc::new(move |room: livekit_protocol::Room| {
        emitted_clone
            .lock()
            .expect("emitted room list lock should not be poisoned")
            .push(room.name);
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = spawn_room_cleanup_task_with_room_finished_handler(
        rooms,
        Duration::from_millis(10),
        Duration::from_millis(1),
        shutdown_rx,
        Some(room_finished_handler),
    );

    tokio::time::sleep(Duration::from_millis(80)).await;

    shutdown_tx
        .send(())
        .expect("cleanup shutdown signal should send once");
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cleanup task should stop")
        .expect("cleanup task should finish without panic");

    let names = emitted
        .lock()
        .expect("emitted room list lock should not be poisoned")
        .clone();
    assert!(
        names.contains(&"room-finished-a".to_string()),
        "cleanup task should emit room-finished callback for removed rooms"
    );
}

#[tokio::test]
async fn room_cleanup_task_removes_stale_empty_rooms() {
    let rooms = RoomStore::default();
    rooms
        .create_room(livekit_protocol::CreateRoomRequest {
            name: "stale-room".to_string(),
            departure_timeout: 1,
            empty_timeout: 1,
            ..Default::default()
        })
        .expect("room should be created with short cleanup timeouts");
    rooms
        .join_participant(
            "stale-room",
            "alice",
            "Alice",
            String::new(),
            std::collections::HashMap::new(),
        )
        .expect("room should create via join");
    rooms
        .remove_participant("stale-room", "alice")
        .expect("participant should leave making room empty");

    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = spawn_room_cleanup_task(
        rooms.clone(),
        Duration::from_millis(10),
        Duration::from_millis(1),
        shutdown_rx,
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let stale_room_exists = rooms
            .room_exists("stale-room")
            .expect("room existence should query");
        if !stale_room_exists || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    shutdown_tx
        .send(())
        .expect("cleanup shutdown signal should send once");
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cleanup task should stop")
        .expect("cleanup task should finish without panic");

    assert!(
        !rooms
            .room_exists("stale-room")
            .expect("room existence should query"),
        "stale empty room should be removed by periodic cleanup task"
    );

    let removed_total = read_metric_from_endpoint("oxidesfu_room_cleanup_removed_total").await;
    assert!(removed_total >= 1);
}

#[test]
fn log_request_completion_emits_expected_structured_fields() {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let collector = Registry::default().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .without_time()
            .with_writer(BufferWriter(buffer.clone())),
    );
    let guard = tracing::subscriber::set_default(collector);

    log_request_completion("req-test", "GET", "/healthz", 200, 12);
    drop(guard);

    let logs = String::from_utf8(
        buffer
            .lock()
            .expect("buffer lock should not be poisoned")
            .clone(),
    )
    .expect("logs should be utf8");

    assert!(logs.contains("http_request_completed"));
    assert!(logs.contains("request_id") && logs.contains("req-test"));
    assert!(logs.contains("method") && logs.contains("GET"));
    assert!(logs.contains("path") && logs.contains("/healthz"));
    assert!(logs.contains("status") && logs.contains("200"));
}

#[tokio::test(flavor = "current_thread")]
async fn request_logging_middleware_emits_completion_fields_for_http_request() {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let collector = Registry::default().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .without_time()
            .with_writer(BufferWriter(buffer.clone())),
    );
    let guard = tracing::subscriber::set_default(collector);

    let response = app()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .header(X_REQUEST_ID, "integration-log-req")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::OK);

    drop(guard);
    let logs = String::from_utf8(
        buffer
            .lock()
            .expect("buffer lock should not be poisoned")
            .clone(),
    )
    .expect("logs should be utf8");

    assert!(logs.contains("http_request_completed"));
    assert!(logs.contains("integration-log-req"));
    assert!(logs.contains("/healthz"));
    assert!(logs.contains("200"));
}

#[tokio::test]
async fn room_cleanup_task_shutdown_is_bounded_during_heavy_cleanup_load() {
    let rooms = RoomStore::default();
    for i in 0..500 {
        let room_name = format!("heavy-cleanup-room-{i}");
        rooms
            .join_participant(
                &room_name,
                "alice",
                "Alice",
                String::new(),
                std::collections::HashMap::new(),
            )
            .expect("room should create via join");
        rooms
            .remove_participant(&room_name, "alice")
            .expect("participant should leave making room empty");
    }

    tokio::time::sleep(Duration::from_millis(20)).await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = spawn_room_cleanup_task(
        rooms,
        Duration::from_millis(1),
        Duration::from_millis(1),
        shutdown_rx,
    );

    tokio::time::sleep(Duration::from_millis(5)).await;
    shutdown_tx
        .send(())
        .expect("cleanup shutdown signal should send once");

    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cleanup task should stop under heavy load")
        .expect("cleanup task should finish without panic");
}

#[test]
fn request_id_from_headers_prefers_non_empty_header_value() {
    let mut headers = HeaderMap::new();
    headers.insert(
        X_REQUEST_ID,
        HeaderValue::from_str("explicit-request-id").expect("header should parse"),
    );

    assert_eq!(request_id_from_headers(&headers), "explicit-request-id");
}

async fn read_metric_from_endpoint(metric_name: &str) -> u64 {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("metrics request should build"),
        )
        .await
        .expect("router should respond to metrics request");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("metrics body should collect")
        .to_bytes();
    let text = String::from_utf8(body.to_vec()).expect("metrics should be utf8");
    parse_metric_value(&text, metric_name)
}

fn parse_metric_value(text: &str, metric_name: &str) -> u64 {
    text.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(' ')?;
            (name == metric_name)
                .then(|| value.parse::<u64>().ok())
                .flatten()
        })
        .unwrap_or_default()
}

#[test]
fn room_node_directory_from_config_uses_memory_backend_by_default() {
    let config = ServerConfig::development();
    let directory = room_node_directory_from_config(&config)
        .expect("default config should build memory room-node directory");
    let node = register_local_room_node(&directory, &config)
        .expect("local node registration should succeed");
    assert!(node.id.starts_with("oxidesfu-local-"));
    assert_eq!(node.region, "local");
    assert!(
        !directory
            .is_node_draining(&node.id)
            .expect("node should exist after registration")
    );

    set_local_room_node_draining(&directory, &node.id, true)
        .expect("draining transition should succeed");
    assert!(
        directory
            .is_node_draining(&node.id)
            .expect("node should exist after draining transition")
    );
}

#[test]
fn room_node_directory_from_config_errors_when_redis_selected_without_url() {
    let mut config = ServerConfig::development();
    config.room_node_directory_backend = RoomNodeDirectoryBackend::Redis;
    config.redis_url = None;

    let result = room_node_directory_from_config(&config);
    assert!(matches!(
        result,
        Err(oxidesfu_room::RoomNodeRegistryError::Backend { .. })
    ));
}

#[test]
fn room_node_directory_from_config_uses_redis_factory_when_redis_backend_configured() {
    let mut config = ServerConfig::development();
    config.room_node_directory_backend = RoomNodeDirectoryBackend::Redis;
    config.redis_url = Some("redis://127.0.0.1:6379/0".to_string());

    let seen_url = Arc::new(Mutex::new(None::<String>));
    let seen_url_for_factory = Arc::clone(&seen_url);

    let directory = room_node_directory_from_config_with_factory(
        &config,
        move |redis_url, _selector_config| {
            let mut guard = seen_url_for_factory
                .lock()
                .expect("url capture lock should not be poisoned");
            *guard = Some(redis_url.to_string());
            Ok(Arc::new(RoomNodeRegistry::default()))
        },
    )
    .expect("redis backend config should call injected redis factory");

    assert_eq!(
        seen_url
            .lock()
            .expect("url capture lock should not be poisoned")
            .as_deref(),
        Some("redis://127.0.0.1:6379/0")
    );

    let node = register_local_room_node(&directory, &config)
        .expect("mocked redis directory should support local node registration");
    assert!(node.id.starts_with("oxidesfu-local-"));
}

#[test]
fn room_node_directory_from_config_maps_selector_tuning_fields() {
    let mut config = ServerConfig::development();
    config.room_node_directory_backend = RoomNodeDirectoryBackend::Redis;
    config.redis_url = Some("redis://127.0.0.1:6379/0".to_string());
    config.node_selector_kind = NodeSelectorKind::SystemLoad;
    config.node_selector_sort_by = CoreNodeSelectorSortBy::Clients;
    config.node_selector_algorithm = CoreNodeSelectorAlgorithm::Lowest;
    config.node_selector_cpu_load_limit = 0.61;
    config.node_selector_system_load_limit = 0.72;
    config.node_selector_available_seconds = 13;

    let captured_selector = Arc::new(Mutex::new(None::<NodeSelectorConfig>));
    let captured_selector_for_factory = Arc::clone(&captured_selector);

    let _ = room_node_directory_from_config_with_factory(
        &config,
        move |_redis_url, selector_config| {
            let mut guard = captured_selector_for_factory
                .lock()
                .expect("selector capture lock should not be poisoned");
            *guard = Some(selector_config);
            Ok(Arc::new(RoomNodeRegistry::default()))
        },
    )
    .expect("selector config should map into room directory factory");

    let selector = captured_selector
        .lock()
        .expect("selector capture lock should not be poisoned")
        .clone()
        .expect("selector config should be captured");

    assert_eq!(selector.kind, RoomNodeSelectorKind::SystemLoad);
    assert_eq!(selector.sort_by, RoomNodeSelectorSortBy::Clients);
    assert_eq!(selector.algorithm, RoomNodeSelectorAlgorithm::Lowest);
    assert_eq!(selector.cpu_load_limit, 0.61);
    assert_eq!(selector.system_load_limit, 0.72);
    assert_eq!(selector.available_seconds, 13);
}

#[test]
fn room_node_directory_from_config_uses_regionaware_selector_for_memory_backend() {
    let mut config = ServerConfig::development();
    config.region = "eu-central".to_string();
    config.node_selector_kind = NodeSelectorKind::RegionAware;
    config.node_selector_regions = vec![
        NodeSelectorRegion {
            name: "eu-central".to_string(),
            lat: 50.1109,
            lon: 8.6821,
        },
        NodeSelectorRegion {
            name: "us-east".to_string(),
            lat: 40.7128,
            lon: -74.0060,
        },
    ];

    let directory = room_node_directory_from_config(&config)
        .expect("regionaware memory directory should build from config");
    directory
        .register_node(RegisteredNode {
            id: "node-us".to_string(),
            region: "us-east".to_string(),
        })
        .expect("us node should register");
    directory
        .register_node(RegisteredNode {
            id: "node-eu".to_string(),
            region: "eu-central".to_string(),
        })
        .expect("eu node should register");

    let selected = directory
        .select_or_assign_node_for_room("server-regional-room")
        .expect("regionaware directory should select a node");
    assert_eq!(selected.id, "node-eu");
}

#[tokio::test]
async fn begin_graceful_shutdown_marks_node_draining_and_signals_cleanup() {
    let directory: Arc<dyn RoomNodeDirectory> = Arc::new(RoomNodeRegistry::default());
    directory
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "local".to_string(),
        })
        .expect("node should register");

    let (tx, rx) = tokio::sync::oneshot::channel();
    begin_graceful_shutdown(&directory, "node-1", tx);

    assert!(
        directory
            .is_node_draining("node-1")
            .expect("node should be present after shutdown initiation")
    );
    rx.await
        .expect("cleanup receiver should observe shutdown signal");
}

#[derive(Debug, Clone, Default)]
struct ToggleHashStore {
    values: Arc<Mutex<HashMap<(String, String), String>>>,
    fail: Arc<AtomicBool>,
}

impl ToggleHashStore {
    fn set_fail(&self, fail: bool) {
        self.fail.store(fail, AtomicOrdering::Relaxed);
    }

    fn should_fail(&self) -> bool {
        self.fail.load(AtomicOrdering::Relaxed)
    }
}

impl RedisHashStore for ToggleHashStore {
    fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), RoomNodeRegistryError> {
        if self.should_fail() {
            return Err(RoomNodeRegistryError::Backend {
                message: "simulated relay redis outage on HSET".to_string(),
            });
        }
        self.values
            .lock()
            .expect("toggle hash store lock should not be poisoned")
            .insert((key.to_string(), field.to_string()), value.to_string());
        Ok(())
    }

    fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RoomNodeRegistryError> {
        if self.should_fail() {
            return Err(RoomNodeRegistryError::Backend {
                message: "simulated relay redis outage on HGET".to_string(),
            });
        }
        Ok(self
            .values
            .lock()
            .expect("toggle hash store lock should not be poisoned")
            .get(&(key.to_string(), field.to_string()))
            .cloned())
    }

    fn hdel(&self, key: &str, field: &str) -> Result<(), RoomNodeRegistryError> {
        if self.should_fail() {
            return Err(RoomNodeRegistryError::Backend {
                message: "simulated relay redis outage on HDEL".to_string(),
            });
        }
        self.values
            .lock()
            .expect("toggle hash store lock should not be poisoned")
            .remove(&(key.to_string(), field.to_string()));
        Ok(())
    }

    fn hvals(&self, key: &str) -> Result<Vec<String>, RoomNodeRegistryError> {
        if self.should_fail() {
            return Err(RoomNodeRegistryError::Backend {
                message: "simulated relay redis outage on HVALS".to_string(),
            });
        }
        let values = self
            .values
            .lock()
            .expect("toggle hash store lock should not be poisoned")
            .iter()
            .filter_map(|((k, _), value)| (k == key).then_some(value.clone()))
            .collect();
        Ok(values)
    }
}

#[derive(Debug)]
struct StaticAcceptRelayExecutor;

#[derive(Debug, Default)]
struct RecordingTerminationExecutor {
    terminations: std::sync::Arc<
        std::sync::Mutex<Vec<oxidesfu_signaling::NonLocalRelaySessionTerminationIntent>>,
    >,
}

#[derive(Debug, Default)]
struct PersistentOutboundSignalExecutor {
    sender: std::sync::Mutex<Option<oxidesfu_signaling::RelayOutboundSignalSender>>,
}

impl RelayJoinIntentExecutor for StaticAcceptRelayExecutor {
    fn execute_join(&self, intent: &NonLocalRelayJoinIntent) -> NonLocalRelayJoinResponse {
        NonLocalRelayJoinResponse::Accepted {
            participant_sid: format!("PA_relay_{}", intent.identity),
            server_version: "relay-worker".to_string(),
            ping_interval: 5,
            ping_timeout: 15,
        }
    }

    fn execute_termination(
        &self,
        _intent: &oxidesfu_signaling::NonLocalRelaySessionTerminationIntent,
    ) {
    }
}

impl RelayJoinIntentExecutor for PersistentOutboundSignalExecutor {
    fn execute_join(&self, _intent: &NonLocalRelayJoinIntent) -> NonLocalRelayJoinResponse {
        NonLocalRelayJoinResponse::Rejected {
            code: "unexpected_join".to_string(),
            msg: "join execution not expected in persistent outbound test".to_string(),
        }
    }

    fn execute_termination(
        &self,
        _intent: &oxidesfu_signaling::NonLocalRelaySessionTerminationIntent,
    ) {
    }

    fn execute_signal_request_with_outbound<'a>(
        &'a self,
        _intent: &'a oxidesfu_signaling::NonLocalRelaySignalRequestIntent,
        outbound_tx: oxidesfu_signaling::RelayOutboundSignalSender,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = oxidesfu_signaling::NonLocalRelaySignalRequestResponse>
                + Send
                + 'a,
        >,
    > {
        *self
            .sender
            .lock()
            .expect("persistent outbound sender lock should not be poisoned") = Some(outbound_tx);
        Box::pin(async { oxidesfu_signaling::NonLocalRelaySignalRequestResponse::NoResponse })
    }
}

impl RelayJoinIntentExecutor for RecordingTerminationExecutor {
    fn execute_join(&self, _intent: &NonLocalRelayJoinIntent) -> NonLocalRelayJoinResponse {
        NonLocalRelayJoinResponse::Rejected {
            code: "unexpected_join".to_string(),
            msg: "join execution not expected in termination test".to_string(),
        }
    }

    fn execute_termination(
        &self,
        intent: &oxidesfu_signaling::NonLocalRelaySessionTerminationIntent,
    ) {
        self.terminations
            .lock()
            .expect("termination recording lock should not be poisoned")
            .push(intent.clone());
    }
}

#[tokio::test]
async fn relay_worker_claims_intent_and_dispatcher_receives_response() {
    let store = ToggleHashStore::default();
    let dispatcher_mailbox = RedisRelayMailbox::with_store(store.clone());
    let worker_mailbox = RedisRelayMailbox::with_store(store);

    let dispatcher = RedisMailboxRelayDispatcher::with_mailbox_and_timing(
        dispatcher_mailbox,
        Arc::new(oxidesfu_signaling::NoopRelayIntentExecutionDriver),
        Duration::from_millis(5),
        Duration::from_millis(250),
    );

    let readiness = Arc::new(RelayWorkerReadiness::new(true));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker = spawn_relay_intent_worker(
        worker_mailbox,
        "node-remote".to_string(),
        Arc::new(StaticAcceptRelayExecutor),
        Duration::from_millis(5),
        shutdown_rx,
        readiness,
    );

    let response = tokio::task::spawn_blocking(move || {
        dispatcher.dispatch_non_local_join(NonLocalRelayJoinIntent {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            name: "Alice".to_string(),
            requested_participant_sid: None,
            selected_room_node_id: "node-remote".to_string(),
            subscriber_primary: false,
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_update_metadata: false,
            hidden: false,
            metadata: String::new(),
            attributes: HashMap::new(),
            api_key: String::new(),
            kind: String::new(),
            kind_details: Vec::new(),
            destination_room: String::new(),
            room_config: None,
        })
    })
    .await
    .expect("blocking relay dispatch task should complete")
    .expect("relay dispatcher should succeed");

    assert_eq!(
        response,
        Some(NonLocalRelayJoinResponse::Accepted {
            participant_sid: "PA_relay_alice".to_string(),
            server_version: "relay-worker".to_string(),
            ping_interval: 5,
            ping_timeout: 15,
        })
    );

    shutdown_tx
        .send(())
        .expect("relay worker shutdown signal should send once");
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("relay worker should stop")
        .expect("relay worker should finish without panic");
}

#[tokio::test]
async fn relay_executor_reconnect_with_matching_sid_resumes_existing_participant() {
    let rooms = RoomStore::default();
    let (_, participant, _) = rooms
        .join_participant(
            "relay-room",
            "alice",
            "Alice",
            String::new(),
            HashMap::new(),
        )
        .expect("initial remote participant should join");

    let executor = RoomStoreRelayJoinIntentExecutor::new(rooms.clone());
    let response = executor.execute_join(&NonLocalRelayJoinIntent {
        room: "relay-room".to_string(),
        identity: "alice".to_string(),
        name: "Alice".to_string(),
        requested_participant_sid: Some(participant.sid.clone()),
        selected_room_node_id: "node-remote".to_string(),
        subscriber_primary: false,
        can_publish: true,
        can_subscribe: true,
        can_publish_data: true,
        can_update_metadata: false,
        hidden: false,
        metadata: String::new(),
        attributes: HashMap::new(),
        api_key: String::new(),
        kind: String::new(),
        kind_details: Vec::new(),
        destination_room: String::new(),
        room_config: None,
    });

    let join = match response {
        NonLocalRelayJoinResponse::AcceptedWithJoin { join_response } => {
            livekit_protocol::JoinResponse::decode(join_response.as_slice())
                .expect("relay reconnect response should decode as JoinResponse")
        }
        other => panic!("expected AcceptedWithJoin relay response, got {other:?}"),
    };
    let resumed_participant = join
        .participant
        .expect("relay reconnect JoinResponse should include participant");
    assert_eq!(resumed_participant.sid, participant.sid);
    assert_eq!(resumed_participant.identity, "alice");
    assert_eq!(join.ping_interval, 5);
    assert_eq!(join.ping_timeout, 15);

    let resumed = rooms
        .get_participant("relay-room", "alice")
        .expect("reconnect resume should keep participant in room");
    assert_eq!(resumed.sid, participant.sid);
    assert_eq!(
        rooms
            .list_participants("relay-room")
            .expect("room should list participants")
            .len(),
        1
    );
}

#[tokio::test]
async fn relay_worker_claims_termination_intent_and_executes_remote_cleanup() {
    let store = ToggleHashStore::default();
    let mailbox = RedisRelayMailbox::with_store(store);

    let executor = Arc::new(RecordingTerminationExecutor::default());
    let recorded = executor.terminations.clone();

    let readiness = Arc::new(RelayWorkerReadiness::new(true));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker = spawn_relay_intent_worker(
        mailbox.clone(),
        "node-remote".to_string(),
        executor,
        Duration::from_millis(5),
        shutdown_rx,
        readiness,
    );

    mailbox
        .dispatch_termination_intent(&oxidesfu_signaling::NonLocalRelaySessionTerminationIntent {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            participant_sid: "PA_alice".to_string(),
            selected_room_node_id: "node-remote".to_string(),
        })
        .expect("termination dispatch should succeed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if recorded
            .lock()
            .expect("termination recording lock should not be poisoned")
            .iter()
            .any(|intent| intent.room == "relay-room" && intent.identity == "alice")
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("relay worker did not execute claimed termination intent");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown_tx
        .send(())
        .expect("relay worker shutdown signal should send once");
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("relay worker should stop")
        .expect("relay worker should finish without panic");
}

#[tokio::test]
async fn relay_worker_claims_signal_request_intent_and_stores_response() {
    let store = ToggleHashStore::default();
    let mailbox = RedisRelayMailbox::with_store(store);
    let config = ServerConfig::development();
    let api_state = api_state_from_config(&config);
    let signal_state =
        oxidesfu_signaling::SignalState::new(api_state.rooms.clone(), api_state.auth.clone());
    let executor = Arc::new(RoomStoreRelayJoinIntentExecutor::with_signal_state(
        signal_state,
    ));

    let readiness = Arc::new(RelayWorkerReadiness::new(true));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker = spawn_relay_intent_worker(
        mailbox.clone(),
        "node-remote".to_string(),
        executor,
        Duration::from_millis(5),
        shutdown_rx,
        readiness,
    );

    let receipt = mailbox
        .dispatch_signal_request_intent(&oxidesfu_signaling::NonLocalRelaySignalRequestIntent {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            selected_room_node_id: "node-remote".to_string(),
            signal_request: vec![0xff, 0x00, 0x7f],
        })
        .expect("signal request dispatch should succeed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if let Some(response) = mailbox
            .fetch_signal_response(&receipt)
            .expect("signal response fetch should succeed")
        {
            assert!(matches!(
                response,
                oxidesfu_signaling::NonLocalRelaySignalRequestResponse::Error { .. }
            ));
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("relay worker did not store claimed signal request response");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown_tx
        .send(())
        .expect("relay worker shutdown signal should send once");
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("relay worker should stop")
        .expect("relay worker should finish without panic");
}

#[tokio::test]
async fn relay_worker_persists_outbound_signal_responses_for_relayed_sessions() {
    let store = ToggleHashStore::default();
    let mailbox = RedisRelayMailbox::with_store(store);

    let executor = Arc::new(PersistentOutboundSignalExecutor::default());
    let readiness = Arc::new(RelayWorkerReadiness::new(true));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker = spawn_relay_intent_worker(
        mailbox.clone(),
        "node-remote".to_string(),
        executor.clone(),
        Duration::from_millis(5),
        shutdown_rx,
        readiness,
    );

    let receipt = mailbox
        .dispatch_signal_request_intent(&oxidesfu_signaling::NonLocalRelaySignalRequestIntent {
            room: "relay-room".to_string(),
            identity: "alice".to_string(),
            selected_room_node_id: "node-remote".to_string(),
            signal_request: vec![0x01],
        })
        .expect("signal request dispatch should succeed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if let Some(response) = mailbox
            .fetch_signal_response(&receipt)
            .expect("signal response fetch should succeed")
        {
            assert!(matches!(
                response,
                oxidesfu_signaling::NonLocalRelaySignalRequestResponse::NoResponse
            ));
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("relay worker did not store signal request response for persistent outbound");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The owner can emit unsolicited signals only after the relayed request has
    // completed (for example, a later MediaSectionsRequirement). Keep the sender
    // alive across that boundary and prove the worker persists the delayed event.
    let sender = executor
        .sender
        .lock()
        .expect("persistent outbound sender lock should not be poisoned")
        .clone()
        .expect("relay executor should retain the outbound sender");
    sender
        .send(Default::default())
        .expect("delayed outbound signal should enqueue");

    let outbound_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let drained = mailbox
            .claim_outbound_signal_responses(
                &oxidesfu_signaling::NonLocalRelayOutboundSignalQuery {
                    room: "relay-room".to_string(),
                    identity: "alice".to_string(),
                    selected_room_node_id: "node-remote".to_string(),
                    max_events: 8,
                },
            )
            .expect("outbound signal responses should claim");
        if !drained.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= outbound_deadline {
            panic!("relay worker did not persist outbound signal responses");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown_tx
        .send(())
        .expect("relay worker shutdown signal should send once");
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("relay worker should stop")
        .expect("relay worker should finish without panic");
}

#[tokio::test]
async fn relay_worker_readiness_transitions_not_ready_on_outage_and_recovers_after_backend_returns()
{
    let store = ToggleHashStore::default();
    store.set_fail(true);

    let mailbox = RedisRelayMailbox::with_store(store.clone());
    let readiness = Arc::new(RelayWorkerReadiness::new(true));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker = spawn_relay_intent_worker(
        mailbox.clone(),
        "node-remote".to_string(),
        Arc::new(StaticAcceptRelayExecutor),
        Duration::from_millis(5),
        shutdown_rx,
        readiness.clone(),
    );

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !readiness.is_ready(),
        "readiness should go not-ready while relay backend is unavailable"
    );

    store.set_fail(false);
    mailbox
        .dispatch_intent(&NonLocalRelayJoinIntent {
            room: "relay-room".to_string(),
            identity: "bob".to_string(),
            name: "Bob".to_string(),
            requested_participant_sid: None,
            selected_room_node_id: "node-remote".to_string(),
            subscriber_primary: false,
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_update_metadata: false,
            hidden: false,
            metadata: String::new(),
            attributes: HashMap::new(),
            api_key: String::new(),
            kind: String::new(),
            kind_details: Vec::new(),
            destination_room: String::new(),
            room_config: None,
        })
        .expect("dispatch should succeed after backend recovers");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if readiness.is_ready() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("relay readiness did not recover after backend returned");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown_tx
        .send(())
        .expect("relay worker shutdown signal should send once");
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("relay worker should stop")
        .expect("relay worker should finish without panic");
}

#[test]
fn default_tracing_env_filter_includes_server_and_http_targets() {
    assert!(DEFAULT_TRACING_ENV_FILTER.contains("oxidesfu_server=info"));
    assert!(DEFAULT_TRACING_ENV_FILTER.contains("tower_http=info"));
    assert!(DEFAULT_ROOM_CLEANUP_INTERVAL >= Duration::from_secs(1));
    assert!(DEFAULT_EMPTY_ROOM_MAX_AGE >= Duration::from_secs(1));
}

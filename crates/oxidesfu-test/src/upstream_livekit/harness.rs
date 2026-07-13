use super::*;

pub(super) async fn spawn_single_node() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    spawn_single_node_with_room_auto_create_datachannel_slow_threshold_and_participant_data_blob(
        true, None, true,
    )
    .await
}

pub(super) async fn spawn_single_node_with_room_auto_create(
    room_auto_create_on_join: bool,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    spawn_single_node_with_room_auto_create_datachannel_slow_threshold_and_participant_data_blob(
        room_auto_create_on_join,
        None,
        true,
    )
    .await
}

pub(super) async fn spawn_single_node_with_datachannel_slow_threshold_bytes(
    threshold: u32,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    spawn_single_node_with_room_auto_create_datachannel_slow_threshold_and_participant_data_blob(
        true,
        Some(threshold),
        true,
    )
    .await
}

pub(super) async fn spawn_single_node_with_participant_data_blob_enabled(
    enabled: bool,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    spawn_single_node_with_room_auto_create_datachannel_slow_threshold_and_participant_data_blob(
        true,
        None,
        enabled,
    )
    .await
}

pub(super) struct OwnedTurnSingleNode {
    pub(super) config: oxidesfu_core::ServerConfig,
    server: tokio::task::JoinHandle<()>,
    turn_runtime: oxidesfu_server::TurnRuntime,
}

impl OwnedTurnSingleNode {
    pub(super) async fn shutdown(self) {
        self.server.abort();
        self.turn_runtime
            .shutdown()
            .await
            .expect("owned TURN runtime should stop");
    }
}

pub(super) async fn spawn_single_node_with_owned_turn(
    config: oxidesfu_core::ServerConfig,
) -> OwnedTurnSingleNode {
    assert!(config.turn_enabled, "owned TURN fixture requires enabled TURN");

    let turn_runtime = oxidesfu_server::start_turn_runtime(&config)
        .await
        .expect("owned TURN runtime should start")
        .expect("enabled TURN should return a runtime");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let api_state = oxidesfu_server::api_state_from_config(&config);
    let turn_config = config.clone();
    let signal_state = oxidesfu_signaling::SignalState::with_data_channels(
        api_state.rooms.clone(),
        api_state.auth.clone(),
        api_state.data_channels.clone(),
    )
    .with_ice_servers(oxidesfu_server::signal_ice_servers_from_config(&config))
    .with_ice_server_provider(move |participant_sid| {
        oxidesfu_server::signal_ice_servers_for_participant(&turn_config, participant_sid)
    });
    let app = oxidesfu_server::app_with_api_signal_state_and_readiness(
        api_state,
        signal_state,
        None,
        Arc::new(oxidesfu_server::AlwaysReadyRelayBackendReadiness),
    );

    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });
    OwnedTurnSingleNode {
        config,
        server,
        turn_runtime,
    }
}

async fn spawn_single_node_with_room_auto_create_datachannel_slow_threshold_and_participant_data_blob(
    room_auto_create_on_join: bool,
    datachannel_slow_threshold_bytes: Option<u32>,
    participant_data_blob_enabled: bool,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");

    let config = oxidesfu_core::ServerConfig::development();
    let api_state = oxidesfu_server::api_state_from_config(&config);
    let signal_state = oxidesfu_signaling::SignalState::with_data_channels(
        api_state.rooms.clone(),
        api_state.auth.clone(),
        api_state.data_channels.clone(),
    )
    .with_room_auto_create(room_auto_create_on_join)
    .with_datachannel_slow_threshold_bytes(datachannel_slow_threshold_bytes)
    .with_participant_data_blob_enabled(participant_data_blob_enabled);
    let app = oxidesfu_server::app_with_api_signal_state_and_readiness(
        api_state,
        signal_state,
        None,
        Arc::new(oxidesfu_server::AlwaysReadyRelayBackendReadiness),
    );

    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });
    (addr, server)
}

pub(super) fn base_url(addr: std::net::SocketAddr) -> String {
    format!("http://{addr}")
}

pub(super) fn room_client(addr: std::net::SocketAddr) -> RoomClient {
    RoomClient::with_api_key(&base_url(addr), API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5))
}

pub(super) fn join_token(room: &str, identity: &str) -> String {
    join_token_with(room, identity, identity, "", HashMap::new(), VideoGrants {
        room_join: true,
        room: room.to_string(),
        can_publish: true,
        can_subscribe: true,
        can_publish_data: true,
        ..Default::default()
    })
}

pub(super) fn join_token_with(
    room: &str,
    identity: &str,
    name: &str,
    metadata: &str,
    attributes: HashMap<String, String>,
    mut grants: VideoGrants,
) -> String {
    grants.room_join = true;
    grants.room = room.to_string();
    AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity(identity)
        .with_name(name)
        .with_metadata(metadata)
        .with_attributes(attributes)
        .with_grants(grants)
        .to_jwt()
        .expect("access token should encode")
}

pub(super) async fn connect_signal(base_url: &str, room: &str, identity: &str) -> SignalClient {
    let token = join_token(room, identity);
    connect_signal_with_token(base_url, room, identity, &token).await
}

pub(super) async fn connect_signal_with_token(
    base_url: &str,
    room: &str,
    identity: &str,
    token: &str,
) -> SignalClient {
    let mut options = SignalOptions::default();
    options.single_peer_connection = true;
    options.connect_timeout = Duration::from_secs(5);
    let (client, join, _events) = SignalClient::connect(base_url, token, options, None)
        .await
        .expect("signal client should connect");
    assert_eq!(join.room.as_ref().map(|room| room.name.as_str()), Some(room));
    assert_eq!(
        join.participant
            .as_ref()
            .map(|participant| participant.identity.as_str()),
        Some(identity)
    );
    client
}

pub(super) async fn connect_room(
    base_url: &str,
    room: &str,
    identity: &str,
    auto_subscribe: bool,
) -> (Room, tokio::sync::mpsc::UnboundedReceiver<RoomEvent>) {
    let token = join_token(room, identity);
    connect_room_with_token(base_url, &token, auto_subscribe).await
}

pub(super) async fn connect_room_with_token(
    base_url: &str,
    token: &str,
    auto_subscribe: bool,
) -> (Room, tokio::sync::mpsc::UnboundedReceiver<RoomEvent>) {
    let mut options = RoomOptions::default();
    // Use dual-PC by default for upstream-native ports because current single-PC
    // SDK E2E setup is flaky in this environment (`wait_pc_connection timed out`).
    options.single_peer_connection = false;
    options.auto_subscribe = auto_subscribe;
    options.connect_timeout = Duration::from_secs(10);
    let (room, events) = Room::connect(base_url, token, options)
        .await
        .expect("room should connect");
    (room, events)
}

pub(super) async fn assert_two_clients_see_each_other(base_url: &str, prefix: &str) {
    let room = format!("{prefix}-{}", unique_suffix());
    let alice = connect_signal(base_url, &room, "alice").await;
    let bob = connect_signal(base_url, &room, "bob").await;
    let client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
        .with_failover(false)
        .with_request_timeout(Duration::from_secs(5));
    let participants = client
        .list_participants(&room)
        .await
        .expect("participants should list");
    let identities: HashSet<_> = participants
        .iter()
        .map(|participant| participant.identity.as_str())
        .collect();
    assert!(identities.contains("alice"));
    assert!(identities.contains("bob"));
    alice.close().await;
    bob.close().await;
}

pub(super) async fn publish_audio_and_wait_for_subscriber(base_url: &str, room_name: &str) -> String {
    let (alice_room, _alice_events) = connect_room(base_url, room_name, "alice", true).await;
    let (bob_room, mut bob_events) = connect_room(base_url, room_name, "bob", true).await;
    let (sid, _) = publish_audio_track(&alice_room, "mic").await;
    let publications = wait_for_track_subscribed_count(&mut bob_events, 1).await;
    assert_eq!(publications[0].sid().to_string(), sid);
    let _ = alice_room.close().await;
    let _ = bob_room.close().await;
    sid
}

pub(super) async fn publish_audio_track(room: &Room, name: &str) -> (String, NativeAudioSource) {
    let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
    let track = LocalAudioTrack::create_audio_track(name, RtcAudioSource::Native(source.clone()));
    let publication = room
        .local_participant()
        .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
        .await
        .expect("audio track should publish");
    let sid = publication.sid().to_string();
    assert!(sid.starts_with("TR_"));
    assert_eq!(publication.name(), name);
    assert_eq!(publication.mime_type(), "audio/opus");
    let frame = AudioFrame {
        data: vec![100_i16; 480].into(),
        sample_rate: 48_000,
        num_channels: 1,
        samples_per_channel: 480,
    };
    source.capture_frame(&frame).await.expect("audio frame should be accepted");
    (sid, source)
}

pub(super) async fn publish_video_track(room: &Room, name: &str) -> String {
    let source = NativeVideoSource::new(VideoResolution { width: 16, height: 16 }, false);
    let track = LocalVideoTrack::create_video_track(name, RtcVideoSource::Native(source));
    let publication = room
        .local_participant()
        .publish_track(LocalTrack::Video(track), TrackPublishOptions::default())
        .await
        .expect("video track should publish");
    let sid = publication.sid().to_string();
    assert!(sid.starts_with("TR_"));
    assert_eq!(publication.name(), name);
    sid
}

pub(super) async fn wait_for_track_subscribed_count(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<RoomEvent>,
    expected: usize,
) -> Vec<livekit::prelude::RemoteTrackPublication> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut publications = Vec::new();
        while publications.len() < expected {
            let event = events.recv().await.expect("room events should stay open");
            if let RoomEvent::TrackSubscribed { publication, .. } = event {
                publications.push(publication);
            }
        }
        publications
    })
    .await
    .expect("expected track subscriptions before timeout")
}

pub(super) async fn wait_for_data_track_published_count(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<RoomEvent>,
    expected: usize,
) -> Vec<livekit::prelude::RemoteDataTrack> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut tracks = Vec::new();
        while tracks.len() < expected {
            let event = events.recv().await.expect("room events should stay open");
            if let RoomEvent::DataTrackPublished(track) = event {
                tracks.push(track);
            }
        }
        tracks
    })
    .await
    .expect("expected data-track publications before timeout")
}

fn join_request_param_for(join_request: proto::JoinRequest) -> String {
    let wrapped = proto::WrappedJoinRequest {
        compression: proto::wrapped_join_request::Compression::None as i32,
        join_request: join_request.encode_to_vec(),
    };
    general_purpose::URL_SAFE.encode(wrapped.encode_to_vec())
}

fn join_request_param_with_participant_attributes(attributes: HashMap<String, String>) -> String {
    join_request_param_for(proto::JoinRequest {
        participant_attributes: attributes,
        ..Default::default()
    })
}

pub(super) async fn connect_signal_socket_with_token(
    base_url: &str,
    token: &str,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
) {
    connect_join_and_hold_socket(base_url, token, &join_request_param()).await
}

pub(super) async fn connect_signal_socket_at_path(
    base_url: &str,
    token: &str,
    path_and_query: &str,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
) {
    let host = base_url
        .strip_prefix("http://")
        .expect("base url should start with http://");
    let url = format!("ws://{host}{path_and_query}");
    let mut request = url.into_client_request().expect("request should build");
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization header should parse"),
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
    let response = proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
    let Some(proto::signal_response::Message::Join(join)) = response.message else {
        panic!("expected join response");
    };
    let sid = join
        .participant
        .expect("join response should include participant")
        .sid;
    (socket, sid)
}

pub(super) fn encoded_default_join_request() -> String {
    join_request_param()
}

pub(super) async fn connect_signal_socket_with_token_and_join_request(
    base_url: &str,
    token: &str,
    join_request: proto::JoinRequest,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
) {
    let join_request = join_request_param_for(join_request);
    connect_join_and_hold_socket(base_url, token, &join_request).await
}

pub(super) async fn connect_signal_socket_with_token_and_participant_attributes(
    base_url: &str,
    token: &str,
    attributes: HashMap<String, String>,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
) {
    let join_request = join_request_param_with_participant_attributes(attributes);
    connect_join_and_hold_socket(base_url, token, &join_request).await
}

pub(super) async fn update_data_subscription_and_wait_handles(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    track_sid: &str,
    subscribe: bool,
) -> proto::DataTrackSubscriberHandles {
    let request = proto::SignalRequest {
        message: Some(proto::signal_request::Message::UpdateDataSubscription(
            proto::UpdateDataSubscription {
                updates: vec![proto::update_data_subscription::Update {
                    track_sid: track_sid.to_string(),
                    subscribe,
                    options: None,
                }],
            },
        )),
    };
    socket
        .send(Message::Binary(request.encode_to_vec().into()))
        .await
        .expect("update data subscription request should send");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = socket
                .next()
                .await
                .expect("data-track handles should arrive")
                .expect("data-track handles should be ok");
            let Message::Binary(bytes) = message else {
                continue;
            };
            let response =
                proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
            if let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
                response.message
            {
                break handles;
            }
        }
    })
    .await
    .expect("data-track handles should arrive before timeout")
}

pub(super) fn data_track_handle_count_for_publisher(
    handles: &proto::DataTrackSubscriberHandles,
    publisher_identity: &str,
) -> usize {
    handles
        .sub_handles
        .values()
        .filter(|mapping| mapping.publisher_identity == publisher_identity)
        .count()
}

pub(super) fn data_track_handles_include_track_for_publisher(
    handles: &proto::DataTrackSubscriberHandles,
    publisher_identity: &str,
    track_sid: &str,
) -> bool {
    handles.sub_handles.values().any(|mapping| {
        mapping.publisher_identity == publisher_identity && mapping.track_sid == track_sid
    })
}

pub(super) async fn publish_data_track_and_wait_for_subscriber(base_url: &str, room_name: &str) {
    let (alice_room, _alice_events) = connect_room(base_url, room_name, "alice", true).await;
    let (bob_room, mut bob_events) = connect_room(base_url, room_name, "bob", true).await;

    let local_track = alice_room
        .local_participant()
        .publish_data_track("shared-sensor")
        .await
        .expect("alice publish_data_track should succeed");

    let remote_tracks = wait_for_data_track_published_count(&mut bob_events, 1).await;
    let remote_track = &remote_tracks[0];

    assert!(local_track.is_published());
    assert!(remote_track.is_published());
    assert_eq!(remote_track.info().name(), "shared-sensor");

    let _ = alice_room.close().await;
    let _ = bob_room.close().await;
}

pub(super) async fn http_post_with_origin(base_url: &str, path: &str, origin: &str) -> (u16, Option<String>) {
    let host = base_url.strip_prefix("http://").expect("base url should be http");
    let mut stream = tokio::net::TcpStream::connect(host)
        .await
        .expect("tcp connection should open");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nAuthorization: bearer xyz\r\nOrigin: {origin}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.expect("request should write");
    stream.flush().await.expect("request should flush");
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.expect("response should read");
    let response = String::from_utf8(bytes).expect("response should be utf8");
    let status = response
        .lines()
        .find(|line| line.starts_with("HTTP/"))
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("status line should include code")
        .parse::<u16>()
        .expect("status code should parse");
    let cors = response.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("Access-Control-Allow-Origin") {
            Some(value.trim().to_string())
        } else {
            None
        }
    });
    (status, cors)
}

pub(super) async fn spawn_two_process_nodes() -> Option<(
    tokio::process::Child,
    tokio::process::Child,
    tokio::process::Child,
    String,
    String,
    String,
)> {
    let Some((redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await else {
        eprintln!("skipping upstream multi-node native test because redis-server is unavailable");
        return None;
    };

    let mut node_a_port = reserve_local_port();
    let mut node_b_port = reserve_local_port();
    if node_a_port > node_b_port {
        std::mem::swap(&mut node_a_port, &mut node_b_port);
    }
    let Some((node_a, node_a_base_url)) = spawn_oxidesfu_server_process(node_a_port, &redis_url, false)
        .await
        .expect("node A should start when oxidesfu-server binary is available")
    else {
        eprintln!("skipping upstream multi-node native test because oxidesfu-server binary is unavailable");
        let mut redis = redis;
        let _ = redis.kill().await;
        return None;
    };
    let Some((node_b, node_b_base_url)) = spawn_oxidesfu_server_process(node_b_port, &redis_url, false)
        .await
        .expect("node B should start when oxidesfu-server binary is available")
    else {
        eprintln!("skipping upstream multi-node native test because oxidesfu-server binary is unavailable");
        let mut redis = redis;
        let mut node_a = node_a;
        let _ = node_a.kill().await;
        let _ = redis.kill().await;
        return None;
    };
    wait_for_room_node_registration_count(&redis_url, 2)
        .await
        .expect("both nodes should register");
    Some((redis, node_a, node_b, redis_url, node_a_base_url, node_b_base_url))
}

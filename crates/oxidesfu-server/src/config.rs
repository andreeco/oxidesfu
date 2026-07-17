use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use livekit_protocol as proto;
use oxidesfu_api::ApiState;
use oxidesfu_core::{RoomNodeDirectoryBackend, ServerConfig};
use oxidesfu_room::{
    NodeSelectorAlgorithm, NodeSelectorConfig, NodeSelectorKind, NodeSelectorSortBy,
    RedisRoomNodeDirectory, RegisteredNode, RoomDefaults, RoomNodeDirectory, RoomNodeRegistry,
    RoomNodeRegistryError, SelectorRegion,
};
use rtc_stun::{
    message::{BINDING_REQUEST, BINDING_SUCCESS, Getter, Message, TransactionId},
    xoraddr::XorMappedAddress,
};

/// Builds an API state from server configuration.
pub fn api_state_from_config(config: &ServerConfig) -> ApiState {
    let mut keys = oxidesfu_auth::ApiKeyStore::new();
    for (api_key, api_secret) in &config.api_keys {
        keys.insert(api_key.clone(), api_secret.clone());
    }
    keys.insert(config.api_key.clone(), config.api_secret.clone());
    ApiState {
        rooms: oxidesfu_room::RoomStore::with_defaults(RoomDefaults {
            max_participants: config.room_max_participants,
            empty_timeout: config.room_empty_timeout_seconds,
            departure_timeout: config.room_departure_timeout_seconds,
        }),
        auth: oxidesfu_auth::TokenVerifier::new(keys),
        data_channels: oxidesfu_rtc::DataChannelStore::default(),
        media_subscription_runtime: None,
        room_service_forwarder: None,
        enable_remote_unmute: false,
    }
}

/// Builds ICE server protobuf config advertised in signaling responses.
pub fn signal_ice_servers_from_config(config: &ServerConfig) -> Vec<proto::IceServer> {
    let mut ice_servers: Vec<proto::IceServer> = config
        .ice_servers
        .iter()
        .map(|ice_server| proto::IceServer {
            urls: ice_server.urls.clone(),
            username: ice_server.username.clone(),
            credential: ice_server.credential.clone(),
        })
        .collect();

    if let Some(domain) = config.turn_domain.as_deref() {
        let mut turn_urls = Vec::new();
        if let Some(udp_port) = config.turn_udp_port {
            turn_urls.push(format!("turn:{domain}:{udp_port}?transport=udp"));
        }
        if let Some(tls_port) = config.turn_tls_port {
            turn_urls.push(format!("turns:{domain}:{tls_port}?transport=tcp"));
        }

        if !turn_urls.is_empty() {
            ice_servers.push(proto::IceServer {
                urls: turn_urls,
                username: config
                    .turn_username
                    .clone()
                    .unwrap_or_else(|| config.api_key.clone()),
                credential: config
                    .turn_credential
                    .clone()
                    .unwrap_or_else(|| config.api_secret.clone()),
            });
        }
    }

    ice_servers
}

/// Builds participant-specific ICE server configuration for signaling responses.
///
/// Enabled owned TURN uses a newly minted LiveKit long-term credential tied to the
/// participant SID; disabled/external TURN keeps the existing static configuration.
pub fn signal_ice_servers_for_participant(
    config: &ServerConfig,
    participant_sid: &str,
) -> Vec<proto::IceServer> {
    if !config.turn_enabled {
        return signal_ice_servers_from_config(config);
    }

    let mut ice_servers: Vec<proto::IceServer> = config
        .ice_servers
        .iter()
        .map(|ice_server| proto::IceServer {
            urls: ice_server.urls.clone(),
            username: ice_server.username.clone(),
            credential: ice_server.credential.clone(),
        })
        .collect();
    let Some(domain) = config.turn_domain.as_deref() else {
        return ice_servers;
    };
    let mut turn_urls = Vec::new();
    if let Some(udp_port) = config.turn_udp_port {
        turn_urls.push(format!("turn:{domain}:{udp_port}?transport=udp"));
    }
    if let Some(tls_port) = config.turn_tls_port {
        turn_urls.push(format!("turns:{domain}:{tls_port}?transport=tcp"));
    }
    if turn_urls.is_empty() {
        return ice_servers;
    }

    let mut secrets = std::collections::HashMap::from_iter(
        config
            .api_keys
            .iter()
            .map(|(key, secret)| (key.clone(), secret.clone())),
    );
    secrets.insert(config.api_key.clone(), config.api_secret.clone());
    let auth = crate::turn_auth::TurnAuthHandler::new(secrets);
    let now = crate::turn_auth::TurnAuthHandler::now_unix_seconds();
    let (username, expiry) = auth.create_username_at(
        &config.api_key,
        participant_sid,
        config.turn_credential_ttl_seconds.min(i64::MAX as u64) as i64,
        now,
    );
    let Ok(credential) = auth.create_password_at(&config.api_key, participant_sid, expiry, now)
    else {
        return ice_servers;
    };

    ice_servers.push(proto::IceServer {
        urls: turn_urls,
        username,
        credential,
    });
    ice_servers
}

/// Builds RTC transport listener addresses used by webrtc-rs peer connections.
pub fn rtc_transport_config_from_server_config(
    config: &ServerConfig,
) -> oxidesfu_rtc::RtcTransportConfig {
    // A loopback HTTP bind is convenient for local signaling, but browser test
    // sandboxes can use a separate network namespace. Bind RTC UDP to all local
    // interfaces in that case so ICE can advertise a routable host candidate.
    // Owned loopback TURN needs a concrete server host candidate for the relay
    // candidate to pair with, so preserve the loopback bind in that topology.
    let rtc_bind_ip = rtc_bind_ip(config);
    let udp_addrs = if let Some(udp_port) = config.rtc_udp_port {
        vec![SocketAddr::new(rtc_bind_ip, udp_port).to_string()]
    } else if let (Some(start), Some(end)) = (
        config.rtc_udp_port_range_start,
        config.rtc_udp_port_range_end,
    ) {
        (start..=end)
            .map(|port| SocketAddr::new(rtc_bind_ip, port).to_string())
            .collect()
    } else {
        vec![SocketAddr::new(rtc_bind_ip, 0).to_string()]
    };
    // Per-peer TCP addresses are retained for explicit wrapper/test use. Production
    // fixed-port ICE/TCP uses the shared mux added by
    // `rtc_transport_config_with_tcp_mux_from_server_config` below.
    let tcp_addrs = Vec::new();
    let nat_1to1_ips = if config.rtc_use_external_ip {
        config
            .rtc_node_ip
            .as_ref()
            .map(|ip| vec![ip.clone()])
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    oxidesfu_rtc::RtcTransportConfig {
        udp_addrs,
        tcp_addrs,
        tcp_mux: None,
        nat_1to1_ips,
    }
}

/// Builds RTC transport configuration and binds the one shared configured ICE/TCP listener.
pub fn rtc_transport_config_with_tcp_mux_from_server_config(
    config: &ServerConfig,
) -> io::Result<oxidesfu_rtc::RtcTransportConfig> {
    let mut transport = rtc_transport_config_from_server_config(config);
    if config.rtc_allow_tcp_fallback {
        let addr = SocketAddr::new(rtc_bind_ip(config), config.rtc_tcp_port);
        transport.tcp_mux = Some(oxidesfu_rtc::bind_tcp_mux(addr)?);
    }
    Ok(transport)
}

fn rtc_bind_ip(config: &ServerConfig) -> IpAddr {
    let configured_bind_ip = config.bind.ip();
    if configured_bind_ip.is_loopback() && !config.turn_enabled {
        return match configured_bind_ip {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        };
    }
    configured_bind_ip
}

/// Resolves the public RTC address through STUN when external-IP mode has no explicit node IP.
///
/// An explicit node IP remains the deterministic deployment override. Discovery is
/// synchronous and startup-fatal after three attempts, matching LiveKit's initial
/// external-IP configuration behavior.
pub async fn resolve_rtc_external_ip_from_config(
    config: &ServerConfig,
) -> Result<Option<String>, io::Error> {
    if !config.rtc_use_external_ip || config.rtc_node_ip.is_some() {
        return Ok(config.rtc_node_ip.clone());
    }
    let endpoint = config
        .ice_servers
        .iter()
        .flat_map(|server| server.urls.iter())
        .find_map(|url| stun_endpoint(url))
        .unwrap_or_else(|| ("stun.l.google.com".to_string(), 19_302));

    let mut last_error = None;
    for attempt in 1..=3 {
        match discover_stun_mapped_ip_from(
            rtc_bind_ip(config),
            &endpoint.0,
            endpoint.1,
            Duration::from_secs(5),
        )
        .await
        {
            Ok(ip) => return Ok(Some(ip.to_string())),
            Err(error) => last_error = Some(error),
        }
        if attempt < 3 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("STUN external-IP discovery failed")))
}

fn stun_endpoint(url: &str) -> Option<(String, u16)> {
    let value = url.strip_prefix("stun:")?.split('?').next()?;
    let (host, port) = value.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

async fn discover_stun_mapped_ip_from(
    source_ip: IpAddr,
    domain: &str,
    port: u16,
    timeout: Duration,
) -> Result<IpAddr, io::Error> {
    let addresses = tokio::time::timeout(timeout, tokio::net::lookup_host((domain, port)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "STUN DNS lookup timed out"))??;
    let mut request = Message::new();
    request
        .build(&[Box::<TransactionId>::default(), Box::new(BINDING_REQUEST)])
        .map_err(|error| io::Error::other(format!("failed building STUN request: {error}")))?;
    let request_id = request.transaction_id;
    let request_bytes = request.raw.clone();
    let mut last_error = None;
    for address in addresses {
        if address.is_ipv4() != source_ip.is_ipv4() {
            continue;
        }
        let bind = SocketAddr::new(source_ip, 0);
        let result = async {
            let socket = tokio::net::UdpSocket::bind(bind).await?;
            socket.connect(address).await?;
            socket.send(&request_bytes).await?;
            let mut bytes = [0_u8; 2048];
            let length = tokio::time::timeout(timeout, socket.recv(&mut bytes))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "STUN response timed out")
                })??;
            let mut response = Message::new();
            response
                .unmarshal_binary(&bytes[..length])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if response.typ != BINDING_SUCCESS || response.transaction_id != request_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected STUN response",
                ));
            }
            let mut mapped = XorMappedAddress::default();
            mapped
                .get_from(&response)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            Ok::<IpAddr, io::Error>(mapped.ip)
        }
        .await;
        match result {
            Ok(ip) => return Ok(ip),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::other(format!(
            "STUN lookup returned no {family} addresses for RTC bind IP {source_ip}",
            family = if source_ip.is_ipv4() { "IPv4" } else { "IPv6" },
        ))
    }))
}

/// Validates configured TURN endpoints are reachable when probing is enabled.
pub async fn validate_turn_runtime_from_config(config: &ServerConfig) -> Result<(), io::Error> {
    // The in-process runtime is bind-checked during startup; probing it before
    // its listener exists would incorrectly reject a valid configuration.
    if config.turn_enabled || !config.turn_require_reachable {
        return Ok(());
    }

    let Some(domain) = config.turn_domain.as_deref() else {
        return Err(io::Error::other(
            "TURN reachability probe requires turn_domain",
        ));
    };

    let timeout = Duration::from_millis(config.turn_probe_timeout_ms);
    if timeout.is_zero() {
        return Err(io::Error::other(
            "TURN reachability probe timeout must be > 0",
        ));
    }

    if config.turn_udp_port.is_none() && config.turn_tls_port.is_none() {
        return Err(io::Error::other(
            "TURN reachability probe requires turn_udp_port and/or turn_tls_port",
        ));
    }

    if let Some(udp_port) = config.turn_udp_port {
        probe_turn_udp_stun_binding(domain, udp_port, timeout).await?;
    }

    if let Some(tls_port) = config.turn_tls_port {
        probe_turn_tcp_reachability(domain, tls_port, timeout).await?;
    }

    Ok(())
}

async fn probe_turn_udp_stun_binding(
    domain: &str,
    port: u16,
    timeout: Duration,
) -> Result<(), io::Error> {
    let resolved = tokio::time::timeout(timeout, tokio::net::lookup_host((domain, port)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TURN UDP DNS lookup timed out"))?
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("TURN UDP DNS lookup failed for {domain}:{port}: {err}"),
            )
        })?
        .collect::<Vec<_>>();

    if resolved.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("TURN UDP DNS lookup returned no addresses for {domain}:{port}"),
        ));
    }

    let mut request = Message::new();
    request
        .build(&[Box::<TransactionId>::default(), Box::new(BINDING_REQUEST)])
        .map_err(|err| io::Error::other(format!("failed building STUN request: {err}")))?;
    let request_bytes = request.raw.clone();
    let request_tid = request.transaction_id;

    let mut last_error: Option<io::Error> = None;
    for addr in resolved {
        let bind_addr = match addr {
            SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
        };

        let socket = match tokio::net::UdpSocket::bind(bind_addr).await {
            Ok(socket) => socket,
            Err(err) => {
                last_error = Some(io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    format!("failed binding UDP probe socket for {addr}: {err}"),
                ));
                continue;
            }
        };

        if let Err(err) = socket.connect(addr).await {
            last_error = Some(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("failed connecting UDP probe socket to {addr}: {err}"),
            ));
            continue;
        }

        if let Err(err) = socket.send(&request_bytes).await {
            last_error = Some(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("failed sending STUN probe to {addr}: {err}"),
            ));
            continue;
        }

        let mut response_buf = vec![0_u8; 2048];
        let response_len = match tokio::time::timeout(timeout, socket.recv(&mut response_buf)).await
        {
            Ok(Ok(len)) => len,
            Ok(Err(err)) => {
                last_error = Some(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("failed receiving STUN response from {addr}: {err}"),
                ));
                continue;
            }
            Err(_) => {
                last_error = Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("timed out waiting for STUN response from {addr}"),
                ));
                continue;
            }
        };

        let mut response = Message::new();
        if let Err(err) = response.unmarshal_binary(&response_buf[..response_len]) {
            last_error = Some(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid STUN response from {addr}: {err}"),
            ));
            continue;
        }

        if response.typ == BINDING_SUCCESS && response.transaction_id == request_tid {
            return Ok(());
        }

        last_error = Some(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unexpected STUN response type from {addr}: {}",
                response.typ
            ),
        ));
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::other(format!(
            "TURN UDP probe failed for {domain}:{port} with unknown error"
        ))
    }))
}

async fn probe_turn_tcp_reachability(
    domain: &str,
    port: u16,
    timeout: Duration,
) -> Result<(), io::Error> {
    let resolved = tokio::time::timeout(timeout, tokio::net::lookup_host((domain, port)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TURN TCP DNS lookup timed out"))?
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("TURN TCP DNS lookup failed for {domain}:{port}: {err}"),
            )
        })?
        .collect::<Vec<_>>();

    if resolved.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("TURN TCP DNS lookup returned no addresses for {domain}:{port}"),
        ));
    }

    let mut last_error: Option<io::Error> = None;
    for addr in resolved {
        match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr)).await {
            Ok(Ok(_stream)) => return Ok(()),
            Ok(Err(err)) => {
                last_error = Some(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("TURN TCP connect failed to {addr}: {err}"),
                ));
            }
            Err(_) => {
                last_error = Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("TURN TCP connect timed out to {addr}"),
                ));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::other(format!(
            "TURN TCP reachability probe failed for {domain}:{port} with unknown error"
        ))
    }))
}

/// Builds a room-node directory implementation from runtime configuration.
pub fn room_node_directory_from_config(
    config: &ServerConfig,
) -> Result<Arc<dyn RoomNodeDirectory>, RoomNodeRegistryError> {
    room_node_directory_from_config_with_factory(config, |redis_url, selector_config| {
        let directory =
            RedisRoomNodeDirectory::from_redis_url_with_selector(redis_url, selector_config)?;
        Ok(Arc::new(directory))
    })
}

pub(crate) fn room_node_directory_from_config_with_factory<F>(
    config: &ServerConfig,
    redis_factory: F,
) -> Result<Arc<dyn RoomNodeDirectory>, RoomNodeRegistryError>
where
    F: FnOnce(
        &str,
        NodeSelectorConfig,
    ) -> Result<Arc<dyn RoomNodeDirectory>, RoomNodeRegistryError>,
{
    let selector_config = node_selector_config_from_server_config(config);
    match config.room_node_directory_backend {
        RoomNodeDirectoryBackend::Memory => {
            Ok(Arc::new(RoomNodeRegistry::with_selector(selector_config)))
        }
        RoomNodeDirectoryBackend::Redis => {
            let Some(redis_url) = config.redis_url.as_deref() else {
                return Err(RoomNodeRegistryError::Backend {
                    message: "redis backend selected but redis_url is not configured".to_string(),
                });
            };
            redis_factory(redis_url, selector_config)
        }
    }
}

fn node_selector_config_from_server_config(config: &ServerConfig) -> NodeSelectorConfig {
    NodeSelectorConfig {
        kind: match config.node_selector_kind {
            oxidesfu_core::NodeSelectorKind::First => NodeSelectorKind::First,
            oxidesfu_core::NodeSelectorKind::RegionAware => NodeSelectorKind::RegionAware,
            oxidesfu_core::NodeSelectorKind::Any => NodeSelectorKind::Any,
            oxidesfu_core::NodeSelectorKind::CpuLoad => NodeSelectorKind::CpuLoad,
            oxidesfu_core::NodeSelectorKind::SystemLoad => NodeSelectorKind::SystemLoad,
        },
        current_region: Some(config.region.clone()),
        regions: config
            .node_selector_regions
            .iter()
            .map(|region| SelectorRegion {
                name: region.name.clone(),
                lat: region.lat,
                lon: region.lon,
            })
            .collect(),
        sort_by: match config.node_selector_sort_by {
            oxidesfu_core::NodeSelectorSortBy::Random => NodeSelectorSortBy::Random,
            oxidesfu_core::NodeSelectorSortBy::SystemLoad => NodeSelectorSortBy::SystemLoad,
            oxidesfu_core::NodeSelectorSortBy::CpuLoad => NodeSelectorSortBy::CpuLoad,
            oxidesfu_core::NodeSelectorSortBy::Rooms => NodeSelectorSortBy::Rooms,
            oxidesfu_core::NodeSelectorSortBy::Clients => NodeSelectorSortBy::Clients,
            oxidesfu_core::NodeSelectorSortBy::Tracks => NodeSelectorSortBy::Tracks,
            oxidesfu_core::NodeSelectorSortBy::BytesPerSec => NodeSelectorSortBy::BytesPerSec,
        },
        algorithm: match config.node_selector_algorithm {
            oxidesfu_core::NodeSelectorAlgorithm::Lowest => NodeSelectorAlgorithm::Lowest,
            oxidesfu_core::NodeSelectorAlgorithm::TwoChoice => NodeSelectorAlgorithm::TwoChoice,
        },
        cpu_load_limit: config.node_selector_cpu_load_limit,
        system_load_limit: config.node_selector_system_load_limit,
        available_seconds: config.node_selector_available_seconds,
        ..NodeSelectorConfig::default()
    }
}

/// Registers the current local node in the selected room-node directory.
pub fn register_local_room_node(
    directory: &Arc<dyn RoomNodeDirectory>,
    config: &ServerConfig,
) -> Result<RegisteredNode, RoomNodeRegistryError> {
    let node = RegisteredNode {
        id: format!("oxidesfu-local-{}", config.bind.port()),
        region: config.region.clone(),
    };
    register_room_node(directory, &node)?;
    Ok(node)
}

/// Registers or refreshes a room node as serving.
pub fn register_room_node(
    directory: &Arc<dyn RoomNodeDirectory>,
    node: &RegisteredNode,
) -> Result<(), RoomNodeRegistryError> {
    directory.register_node(node.clone())?;
    directory.set_node_draining(&node.id, false)
}

/// Periodically refreshes the local room-node registration.
pub fn spawn_room_node_registration_task(
    directory: Arc<dyn RoomNodeDirectory>,
    node: RegisteredNode,
    interval: Duration,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(err) = register_room_node(&directory, &node) {
                        tracing::warn!(
                            error = %err,
                            room_node_id = %node.id,
                            "failed_to_refresh_room_node_registration"
                        );
                    }
                }
                _ = &mut shutdown => break,
            }
        }
    })
}

/// Marks a registered local room node as draining/non-draining.
pub fn set_local_room_node_draining(
    directory: &Arc<dyn RoomNodeDirectory>,
    node_id: &str,
    draining: bool,
) -> Result<(), RoomNodeRegistryError> {
    directory.set_node_draining(node_id, draining)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    use rtc_stun::{
        message::{BINDING_REQUEST, BINDING_SUCCESS, Message},
        xoraddr::XorMappedAddress,
    };

    use super::discover_stun_mapped_ip_from;

    #[tokio::test]
    async fn stun_discovery_binds_to_the_configured_rtc_interface() {
        let server = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("test STUN server should bind");
        let server_addr = server
            .local_addr()
            .expect("test STUN server should expose its address");
        let responder = tokio::spawn(async move {
            let mut bytes = [0_u8; 2048];
            let (length, peer) = server
                .recv_from(&mut bytes)
                .await
                .expect("test STUN server should receive a request");
            assert_eq!(peer.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));

            let mut request = Message::new();
            request
                .unmarshal_binary(&bytes[..length])
                .expect("STUN request should decode");
            assert_eq!(request.typ, BINDING_REQUEST);

            let mut response = Message::new();
            response
                .build(&[
                    Box::new(request.transaction_id),
                    Box::new(BINDING_SUCCESS),
                    Box::new(XorMappedAddress {
                        ip: "203.0.113.10".parse().expect("test public IP should parse"),
                        port: peer.port(),
                    }),
                ])
                .expect("STUN response should encode");
            server
                .send_to(&response.raw, peer)
                .await
                .expect("test STUN server should respond");
        });

        let mapped = discover_stun_mapped_ip_from(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            "127.0.0.1",
            server_addr.port(),
            Duration::from_secs(1),
        )
        .await
        .expect("source-bound STUN discovery should succeed");

        assert_eq!(
            mapped,
            "203.0.113.10"
                .parse::<IpAddr>()
                .expect("test public IP should parse")
        );
        responder.await.expect("test STUN responder should finish");
    }
}

use std::collections::BTreeMap;

use serde::Deserialize;
use thiserror::Error;

use super::{
    ConfigError, IceServerConfig, NodeSelectorAlgorithm, NodeSelectorKind, NodeSelectorSortBy,
    RoomNodeDirectoryBackend, ServerConfig, normalize_ice_servers,
};

/// A deterministic summary of a LiveKit YAML translation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveKitConfigReport {
    /// LiveKit paths that were translated into native OxideSFU settings.
    pub translated: Vec<&'static str>,
}

/// Failure while checking or translating a LiveKit YAML configuration.
#[derive(Debug, Error)]
pub enum LiveKitConfigError {
    #[error("invalid LiveKit YAML: {0}")]
    Parse(#[from] serde_yaml_ng::Error),
    #[error("LiveKit configuration field `{path}` is unsupported: {reason}")]
    Unsupported {
        path: &'static str,
        reason: &'static str,
    },
    #[error("LiveKit configuration field `{path}` has an unsupported value: {reason}")]
    InvalidValue {
        path: &'static str,
        reason: &'static str,
    },
    #[error("translated configuration is invalid: {0}")]
    Native(#[from] ConfigError),
}

/// Strictly translates the supported single-node subset of LiveKit YAML.
///
/// The result is independent of `OXIDESFU_*` environment variables. Unknown
/// YAML fields are rejected by the serde models below so a migration never
/// silently loses a deployment setting.
pub fn translate_livekit_yaml(
    yaml: &str,
) -> Result<(ServerConfig, LiveKitConfigReport), LiveKitConfigError> {
    let source: LiveKitYaml = serde_yaml_ng::from_str(yaml)?;
    let mut config = ServerConfig::development();
    let mut translated = Vec::new();

    reject_present(
        source.prometheus_port,
        "prometheus_port",
        "OxideSFU serves /metrics on its main HTTP listener",
    )?;
    reject_present(
        source.debug_handler_port,
        "debug_handler_port",
        "a dedicated Go debug/pprof listener is not implemented",
    )?;
    reject_present(
        source.logging,
        "logging",
        "LiveKit logging configuration is not compatible; use RUST_LOG",
    )?;
    reject_present(
        source.audio,
        "audio",
        "audio policy configuration is not implemented",
    )?;
    reject_present(
        source.limit,
        "limit",
        "server-wide limit configuration is not implemented",
    )?;
    reject_present(
        source.ingress,
        "ingress",
        "RTMP/WHIP ingress runtime is not implemented",
    )?;
    reject_present(
        source.signal_relay,
        "signal_relay",
        "LiveKit signal-relay tuning is not implemented",
    )?;
    reject_present(
        source.psrpc,
        "psrpc",
        "LiveKit PSRPC tuning is not implemented",
    )?;

    if let Some(port) = source.port {
        config.bind.set_port(port);
        translated.push("port");
    }

    if let Some(keys) = source.keys {
        if keys.is_empty() {
            return Err(LiveKitConfigError::InvalidValue {
                path: "keys",
                reason: "must not be empty",
            });
        }
        let (primary_key, primary_secret) = keys
            .first_key_value()
            .expect("non-empty map has a first entry");
        config.api_key = primary_key.clone();
        config.api_secret = primary_secret.clone();
        config.api_keys = keys.into_iter().collect();
        translated.push("keys");
    }

    if let Some(redis) = source.redis {
        reject_present(
            redis.sentinel_master_name,
            "redis.sentinel_master_name",
            "Redis Sentinel is not implemented",
        )?;
        reject_present(
            redis.sentinel_addresses,
            "redis.sentinel_addresses",
            "Redis Sentinel is not implemented",
        )?;
        reject_present(
            redis.cluster_addresses,
            "redis.cluster_addresses",
            "Redis Cluster is not implemented",
        )?;
        reject_present(
            redis.tls,
            "redis.tls",
            "Redis TLS configuration is not implemented",
        )?;
        if let Some(address) = redis.address {
            if address.trim().is_empty() {
                return Err(LiveKitConfigError::InvalidValue {
                    path: "redis.address",
                    reason: "must not be empty",
                });
            }
            config.room_node_directory_backend = RoomNodeDirectoryBackend::Redis;
            config.redis_url = Some(format!("redis://{address}"));
            translated.push("redis.address");
        }
    }

    if let Some(rtc) = source.rtc {
        reject_unknown_fields(
            &rtc.unsupported,
            "rtc",
            "contains unsupported candidate, congestion, or media policy settings",
        )?;
        if let Some(port) = rtc.udp_port {
            config.rtc_udp_port = Some(port);
            translated.push("rtc.udp_port");
        }
        if let Some(port) = rtc.port_range_start {
            config.rtc_udp_port_range_start = Some(port);
            translated.push("rtc.port_range_start");
        }
        if let Some(port) = rtc.port_range_end {
            config.rtc_udp_port_range_end = Some(port);
            translated.push("rtc.port_range_end");
        }
        if let Some(port) = rtc.tcp_port {
            config.rtc_tcp_port = port;
            translated.push("rtc.tcp_port");
        }
        if let Some(value) = rtc.use_external_ip {
            config.rtc_use_external_ip = value;
            translated.push("rtc.use_external_ip");
        }
        if let Some(value) = rtc.node_ip {
            config.rtc_node_ip = Some(value);
            translated.push("rtc.node_ip");
        }
        if let Some(value) = rtc.allow_tcp_fallback {
            config.rtc_allow_tcp_fallback = value;
            translated.push("rtc.allow_tcp_fallback");
        }
        if let Some(value) = rtc.tcp_fallback_rtt_threshold {
            config.rtc_tcp_fallback_rtt_threshold_ms = value;
            translated.push("rtc.tcp_fallback_rtt_threshold");
        }
        if let Some(value) = rtc.allow_udp_unstable_fallback {
            config.rtc_allow_udp_unstable_fallback = value;
            translated.push("rtc.allow_udp_unstable_fallback");
        }
        if let Some(servers) = rtc.stun_servers {
            config.ice_servers = normalize_ice_servers(
                "livekit.rtc.stun_servers",
                servers
                    .into_iter()
                    .map(|host| IceServerConfig {
                        urls: vec![format!("stun:{host}")],
                        username: String::new(),
                        credential: String::new(),
                    })
                    .collect(),
            )
            .map_err(LiveKitConfigError::Native)?;
            translated.push("rtc.stun_servers");
        }
        if let Some(servers) = rtc.turn_servers {
            config.ice_servers = normalize_ice_servers(
                "livekit.rtc.turn_servers",
                translate_external_turn_servers(servers)?,
            )
            .map_err(LiveKitConfigError::Native)?;
            translated.push("rtc.turn_servers");
        }
    }

    if let Some(turn) = source.turn {
        reject_present(
            turn.tls_port,
            "turn.tls_port",
            "owned TURN/TLS is not implemented",
        )?;
        reject_present(
            turn.external_tls,
            "turn.external_tls",
            "owned TURN/TLS is not implemented",
        )?;
        reject_present(
            turn.cert_file,
            "turn.cert_file",
            "owned TURN/TLS is not implemented",
        )?;
        reject_present(
            turn.key_file,
            "turn.key_file",
            "owned TURN/TLS is not implemented",
        )?;
        if let Some(value) = turn.enabled {
            config.turn_enabled = value;
            translated.push("turn.enabled");
        }
        if let Some(value) = turn.udp_port {
            config.turn_udp_port = Some(value);
            translated.push("turn.udp_port");
        }
        if let Some(value) = turn.relay_range_start {
            config.turn_relay_port_range_start = Some(value);
            translated.push("turn.relay_range_start");
        }
        if let Some(value) = turn.relay_range_end {
            config.turn_relay_port_range_end = Some(value);
            translated.push("turn.relay_range_end");
        }
        if let Some(value) = turn.domain {
            config.turn_domain = Some(value);
            translated.push("turn.domain");
        }
        if let Some(value) = turn.ttl_seconds {
            config.turn_credential_ttl_seconds = value;
            translated.push("turn.ttl_seconds");
        }
        if let Some(value) = turn.allow_restricted_peer_cidrs {
            config.turn_allow_restricted_peer_cidrs = value;
            translated.push("turn.allow_restricted_peer_cidrs");
        }
        if let Some(value) = turn.deny_peer_cidrs {
            config.turn_deny_peer_cidrs = value;
            translated.push("turn.deny_peer_cidrs");
        }
    }

    if let Some(room) = source.room {
        reject_unknown_fields(
            &room.unsupported,
            "room",
            "contains unsupported room lifecycle or media policy settings",
        )?;
        if let Some(value) = room.auto_create {
            config.room_auto_create = value;
            translated.push("room.auto_create");
        }
    }
    if let Some(webhook) = source.webhook {
        if let Some(value) = webhook.api_key {
            config.webhook_api_key = Some(value);
            translated.push("webhook.api_key");
        }
        if let Some(value) = webhook.urls {
            config.webhook_urls = value;
            translated.push("webhook.urls");
        }
    }
    if let Some(region) = source.region {
        config.region = region;
        translated.push("region");
    }
    if let Some(selector) = source.node_selector {
        if let Some(kind) = selector.kind {
            config.node_selector_kind = match kind.as_str() {
                "any" => NodeSelectorKind::Any,
                "sysload" => NodeSelectorKind::SystemLoad,
                "cpuload" => NodeSelectorKind::CpuLoad,
                "regionaware" => NodeSelectorKind::RegionAware,
                _ => {
                    return Err(LiveKitConfigError::InvalidValue {
                        path: "node_selector.kind",
                        reason: "unsupported selector kind",
                    });
                }
            };
            translated.push("node_selector.kind");
        }
        if let Some(sort_by) = selector.sort_by {
            config.node_selector_sort_by = match sort_by.as_str() {
                "random" => NodeSelectorSortBy::Random,
                "sysload" => NodeSelectorSortBy::SystemLoad,
                "cpuload" => NodeSelectorSortBy::CpuLoad,
                "rooms" => NodeSelectorSortBy::Rooms,
                "clients" => NodeSelectorSortBy::Clients,
                "tracks" => NodeSelectorSortBy::Tracks,
                "bytespersec" => NodeSelectorSortBy::BytesPerSec,
                _ => {
                    return Err(LiveKitConfigError::InvalidValue {
                        path: "node_selector.sort_by",
                        reason: "unsupported sort order",
                    });
                }
            };
            translated.push("node_selector.sort_by");
        }
        if let Some(algorithm) = selector.algorithm {
            config.node_selector_algorithm = match algorithm.as_str() {
                "lowest" => NodeSelectorAlgorithm::Lowest,
                "twochoice" => NodeSelectorAlgorithm::TwoChoice,
                _ => {
                    return Err(LiveKitConfigError::InvalidValue {
                        path: "node_selector.algorithm",
                        reason: "unsupported selector algorithm",
                    });
                }
            };
            translated.push("node_selector.algorithm");
        }
        if let Some(value) = selector.sysload_limit {
            config.node_selector_system_load_limit = value;
            translated.push("node_selector.sysload_limit");
        }
    }

    config.validate_transport_constraints()?;
    Ok((config, LiveKitConfigReport { translated }))
}

fn translate_external_turn_servers(
    servers: Vec<ExternalTurnServer>,
) -> Result<Vec<IceServerConfig>, LiveKitConfigError> {
    servers
        .into_iter()
        .map(|server| {
            reject_present(
                server.secret,
                "rtc.turn_servers.secret",
                "dynamic TURN credentials are not implemented",
            )?;
            reject_present(
                server.secret_file,
                "rtc.turn_servers.secret_file",
                "dynamic TURN credentials are not implemented",
            )?;
            reject_present(
                server.ttl,
                "rtc.turn_servers.ttl",
                "dynamic TURN credentials are not implemented",
            )?;

            let host = server
                .host
                .map(|host| host.trim().to_string())
                .filter(|host| !host.is_empty())
                .ok_or(LiveKitConfigError::InvalidValue {
                    path: "rtc.turn_servers.host",
                    reason: "must not be empty",
                })?;
            let port = server.port.ok_or(LiveKitConfigError::InvalidValue {
                path: "rtc.turn_servers.port",
                reason: "must be set",
            })?;
            if port == 0 {
                return Err(LiveKitConfigError::InvalidValue {
                    path: "rtc.turn_servers.port",
                    reason: "must be greater than zero",
                });
            }

            let (scheme, transport) = match server.protocol.as_deref().unwrap_or("tcp") {
                "udp" => ("turn", "udp"),
                "tcp" => ("turn", "tcp"),
                "tls" => ("turns", "tcp"),
                _ => {
                    return Err(LiveKitConfigError::InvalidValue {
                        path: "rtc.turn_servers.protocol",
                        reason: "must be udp, tcp, or tls",
                    });
                }
            };

            Ok(IceServerConfig {
                urls: vec![format!("{scheme}:{host}:{port}?transport={transport}")],
                username: server.username.unwrap_or_default().trim().to_string(),
                credential: server.credential.unwrap_or_default().trim().to_string(),
            })
        })
        .collect()
}

fn reject_present<T>(
    value: Option<T>,
    path: &'static str,
    reason: &'static str,
) -> Result<(), LiveKitConfigError> {
    if value.is_some() {
        Err(LiveKitConfigError::Unsupported { path, reason })
    } else {
        Ok(())
    }
}

fn reject_unknown_fields(
    fields: &BTreeMap<String, serde_yaml_ng::Value>,
    path: &'static str,
    reason: &'static str,
) -> Result<(), LiveKitConfigError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(LiveKitConfigError::Unsupported { path, reason })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveKitYaml {
    port: Option<u16>,
    keys: Option<BTreeMap<String, String>>,
    redis: Option<Redis>,
    rtc: Option<Rtc>,
    prometheus_port: Option<serde_yaml_ng::Value>,
    debug_handler_port: Option<serde_yaml_ng::Value>,
    logging: Option<serde_yaml_ng::Value>,
    room: Option<Room>,
    webhook: Option<Webhook>,
    signal_relay: Option<serde_yaml_ng::Value>,
    psrpc: Option<serde_yaml_ng::Value>,
    audio: Option<serde_yaml_ng::Value>,
    turn: Option<Turn>,
    ingress: Option<serde_yaml_ng::Value>,
    region: Option<String>,
    node_selector: Option<NodeSelector>,
    limit: Option<serde_yaml_ng::Value>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Redis {
    address: Option<String>,
    sentinel_master_name: Option<serde_yaml_ng::Value>,
    sentinel_addresses: Option<serde_yaml_ng::Value>,
    cluster_addresses: Option<serde_yaml_ng::Value>,
    tls: Option<serde_yaml_ng::Value>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Rtc {
    port_range_start: Option<u16>,
    port_range_end: Option<u16>,
    udp_port: Option<u16>,
    tcp_port: Option<u16>,
    use_external_ip: Option<bool>,
    node_ip: Option<String>,
    stun_servers: Option<Vec<String>>,
    turn_servers: Option<Vec<ExternalTurnServer>>,
    allow_tcp_fallback: Option<bool>,
    tcp_fallback_rtt_threshold: Option<u32>,
    allow_udp_unstable_fallback: Option<bool>,
    #[serde(flatten)]
    unsupported: BTreeMap<String, serde_yaml_ng::Value>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalTurnServer {
    host: Option<String>,
    port: Option<u16>,
    protocol: Option<String>,
    username: Option<String>,
    credential: Option<String>,
    secret: Option<serde_yaml_ng::Value>,
    secret_file: Option<serde_yaml_ng::Value>,
    ttl: Option<serde_yaml_ng::Value>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Turn {
    enabled: Option<bool>,
    udp_port: Option<u16>,
    tls_port: Option<serde_yaml_ng::Value>,
    relay_range_start: Option<u16>,
    relay_range_end: Option<u16>,
    external_tls: Option<serde_yaml_ng::Value>,
    domain: Option<String>,
    cert_file: Option<serde_yaml_ng::Value>,
    key_file: Option<serde_yaml_ng::Value>,
    ttl_seconds: Option<u64>,
    allow_restricted_peer_cidrs: Option<Vec<String>>,
    deny_peer_cidrs: Option<Vec<String>>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Room {
    auto_create: Option<bool>,
    #[serde(flatten)]
    unsupported: BTreeMap<String, serde_yaml_ng::Value>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Webhook {
    api_key: Option<String>,
    urls: Option<Vec<String>>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSelector {
    kind: Option<String>,
    sort_by: Option<String>,
    algorithm: Option<String>,
    sysload_limit: Option<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_supported_single_node_yaml() {
        let (config, report) = translate_livekit_yaml(r#"
port: 7885
keys: { key1: secret1, key2: secret2 }
redis: { address: redis:6379 }
rtc: { port_range_start: 50000, port_range_end: 50100, tcp_port: 7881, stun_servers: [stun.example.net:3478] }
turn: { enabled: true, udp_port: 3479, relay_range_start: 55000, relay_range_end: 55100, domain: turn.example.net }
room: { auto_create: false }
webhook: { api_key: key1, urls: [https://events.example.net/livekit] }
region: eu-west
node_selector: { kind: any, sort_by: clients, algorithm: lowest, sysload_limit: 0.7 }
"#).expect("supported YAML should translate");
        assert_eq!(config.bind.port(), 7885);
        assert_eq!(config.api_keys.len(), 2);
        assert_eq!(config.redis_url.as_deref(), Some("redis://redis:6379"));
        assert_eq!(config.rtc_udp_port_range_start, Some(50000));
        assert_eq!(
            config.ice_servers[0].urls,
            vec!["stun:stun.example.net:3478"]
        );
        assert!(report.translated.contains(&"turn.domain"));
    }

    #[test]
    fn translates_static_external_turn_servers() {
        let (config, report) = translate_livekit_yaml(
            r#"
rtc:
  turn_servers:
    - host: turn-udp.example.net
      port: 3478
      protocol: udp
      username: udp-user
      credential: udp-pass
    - host: turn-tcp.example.net
      port: 443
      protocol: tcp
      username: tcp-user
      credential: tcp-pass
    - host: turn-tls.example.net
      port: 5349
      protocol: tls
      username: tls-user
      credential: tls-pass
"#,
        )
        .expect("static external TURN YAML should translate");

        assert_eq!(
            config.ice_servers,
            vec![
                IceServerConfig {
                    urls: vec!["turn:turn-udp.example.net:3478?transport=udp".to_string()],
                    username: "udp-user".to_string(),
                    credential: "udp-pass".to_string(),
                },
                IceServerConfig {
                    urls: vec!["turn:turn-tcp.example.net:443?transport=tcp".to_string()],
                    username: "tcp-user".to_string(),
                    credential: "tcp-pass".to_string(),
                },
                IceServerConfig {
                    urls: vec!["turns:turn-tls.example.net:5349?transport=tcp".to_string()],
                    username: "tls-user".to_string(),
                    credential: "tls-pass".to_string(),
                },
            ]
        );
        assert!(report.translated.contains(&"rtc.turn_servers"));
    }

    #[test]
    fn rejects_dynamic_external_turn_credentials() {
        let error = translate_livekit_yaml(
            "rtc: { turn_servers: [{ host: turn.example.net, port: 3478, secret: shared-secret }] }",
        )
        .expect_err("dynamic external TURN credentials must block migration");
        assert!(matches!(
            error,
            LiveKitConfigError::Unsupported {
                path: "rtc.turn_servers.secret",
                ..
            }
        ));
    }

    #[test]
    fn rejects_turn_tls_instead_of_ignoring_it() {
        let error = translate_livekit_yaml("turn: { tls_port: 5349 }")
            .expect_err("TURN TLS must block migration");
        assert!(matches!(
            error,
            LiveKitConfigError::Unsupported {
                path: "turn.tls_port",
                ..
            }
        ));
    }
}

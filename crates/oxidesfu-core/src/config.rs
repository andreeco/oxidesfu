// Core OxideSFU domain types and configuration.

use std::{collections::HashMap, fs, net::SocketAddr, num::ParseIntError, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ApiKey, ApiSecret};

mod livekit_yaml;
pub use livekit_yaml::{LiveKitConfigError, LiveKitConfigReport, translate_livekit_yaml};

const ENV_BIND: &str = "OXIDESFU_BIND";
const ENV_API_KEY: &str = "OXIDESFU_API_KEY";
const ENV_API_KEY_FILE: &str = "OXIDESFU_API_KEY_FILE";
const ENV_API_SECRET: &str = "OXIDESFU_API_SECRET";
const ENV_API_SECRET_FILE: &str = "OXIDESFU_API_SECRET_FILE";
const ENV_ROOM_CLEANUP_INTERVAL_MS: &str = "OXIDESFU_ROOM_CLEANUP_INTERVAL_MS";
const ENV_EMPTY_ROOM_MAX_AGE_MS: &str = "OXIDESFU_EMPTY_ROOM_MAX_AGE_MS";
const ENV_ROOM_AUTO_CREATE: &str = "OXIDESFU_ROOM_AUTO_CREATE";
const ENV_ROOM_NODE_DIRECTORY_BACKEND: &str = "OXIDESFU_ROOM_NODE_DIRECTORY_BACKEND";
const ENV_REDIS_URL: &str = "OXIDESFU_REDIS_URL";
const ENV_REJECT_NON_LOCAL_ROOM_PLACEMENT: &str = "OXIDESFU_REJECT_NON_LOCAL_ROOM_PLACEMENT";
const ENV_ICE_SERVERS_JSON: &str = "OXIDESFU_ICE_SERVERS_JSON";
const ENV_RTC_UDP_PORT: &str = "OXIDESFU_RTC_UDP_PORT";
const ENV_RTC_UDP_PORT_RANGE_START: &str = "OXIDESFU_RTC_UDP_PORT_RANGE_START";
const ENV_RTC_UDP_PORT_RANGE_END: &str = "OXIDESFU_RTC_UDP_PORT_RANGE_END";
const ENV_RTC_TCP_PORT: &str = "OXIDESFU_RTC_TCP_PORT";
const ENV_RTC_ALLOW_TCP_FALLBACK: &str = "OXIDESFU_RTC_ALLOW_TCP_FALLBACK";
const ENV_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS: &str = "OXIDESFU_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS";
const ENV_RTC_ALLOW_UDP_UNSTABLE_FALLBACK: &str = "OXIDESFU_RTC_ALLOW_UDP_UNSTABLE_FALLBACK";
const ENV_RTC_USE_EXTERNAL_IP: &str = "OXIDESFU_RTC_USE_EXTERNAL_IP";
const ENV_RTC_NODE_IP: &str = "OXIDESFU_RTC_NODE_IP";
const ENV_DATACHANNEL_SLOW_THRESHOLD: &str = "OXIDESFU_DATACHANNEL_SLOW_THRESHOLD";
const ENV_PARTICIPANT_DATA_BLOB_ENABLED: &str = "OXIDESFU_PARTICIPANT_DATA_BLOB_ENABLED";
const ENV_TURN_ENABLED: &str = "OXIDESFU_TURN_ENABLED";
const ENV_TURN_DOMAIN: &str = "OXIDESFU_TURN_DOMAIN";
const ENV_TURN_BIND: &str = "OXIDESFU_TURN_BIND";
const ENV_TURN_EXTERNAL_IP: &str = "OXIDESFU_TURN_EXTERNAL_IP";
const ENV_TURN_UDP_PORT: &str = "OXIDESFU_TURN_UDP_PORT";
const ENV_TURN_RELAY_PORT_RANGE_START: &str = "OXIDESFU_TURN_RELAY_PORT_RANGE_START";
const ENV_TURN_RELAY_PORT_RANGE_END: &str = "OXIDESFU_TURN_RELAY_PORT_RANGE_END";
const ENV_TURN_CREDENTIAL_TTL_SECONDS: &str = "OXIDESFU_TURN_CREDENTIAL_TTL_SECONDS";
const ENV_TURN_ALLOW_RESTRICTED_PEER_CIDRS: &str = "OXIDESFU_TURN_ALLOW_RESTRICTED_PEER_CIDRS";
const ENV_TURN_DENY_PEER_CIDRS: &str = "OXIDESFU_TURN_DENY_PEER_CIDRS";
const ENV_TURN_TLS_PORT: &str = "OXIDESFU_TURN_TLS_PORT";
const ENV_TURN_TLS_BIND: &str = "OXIDESFU_TURN_TLS_BIND";
const ENV_TURN_TLS_CERT_FILE: &str = "OXIDESFU_TURN_TLS_CERT_FILE";
const ENV_TURN_TLS_KEY_FILE: &str = "OXIDESFU_TURN_TLS_KEY_FILE";
const ENV_TURN_USERNAME: &str = "OXIDESFU_TURN_USERNAME";
const ENV_TURN_CREDENTIAL: &str = "OXIDESFU_TURN_CREDENTIAL";
const ENV_TURN_REQUIRE_REACHABLE: &str = "OXIDESFU_TURN_REQUIRE_REACHABLE";
const ENV_TURN_PROBE_TIMEOUT_MS: &str = "OXIDESFU_TURN_PROBE_TIMEOUT_MS";
const ENV_WEBHOOK_API_KEY: &str = "OXIDESFU_WEBHOOK_API_KEY";
const ENV_WEBHOOK_URLS: &str = "OXIDESFU_WEBHOOK_URLS";
const ENV_REGION: &str = "OXIDESFU_REGION";
const ENV_NODE_SELECTOR_KIND: &str = "OXIDESFU_NODE_SELECTOR_KIND";
const ENV_NODE_SELECTOR_REGIONS_JSON: &str = "OXIDESFU_NODE_SELECTOR_REGIONS_JSON";
const ENV_NODE_SELECTOR_SORT_BY: &str = "OXIDESFU_NODE_SELECTOR_SORT_BY";
const ENV_NODE_SELECTOR_ALGORITHM: &str = "OXIDESFU_NODE_SELECTOR_ALGORITHM";
const ENV_NODE_SELECTOR_CPU_LOAD_LIMIT: &str = "OXIDESFU_NODE_SELECTOR_CPU_LOAD_LIMIT";
const ENV_NODE_SELECTOR_SYSTEM_LOAD_LIMIT: &str = "OXIDESFU_NODE_SELECTOR_SYSTEM_LOAD_LIMIT";
const ENV_NODE_SELECTOR_AVAILABLE_SECONDS: &str = "OXIDESFU_NODE_SELECTOR_AVAILABLE_SECONDS";

const ARG_CONFIG: &str = "--config";
const ARG_BIND: &str = "--bind";
const ARG_API_KEY: &str = "--api-key";
const ARG_API_SECRET: &str = "--api-secret";
const ARG_ROOM_CLEANUP_INTERVAL_MS: &str = "--room-cleanup-interval-ms";
const ARG_EMPTY_ROOM_MAX_AGE_MS: &str = "--empty-room-max-age-ms";
const ARG_ROOM_NODE_DIRECTORY_BACKEND: &str = "--room-node-directory-backend";
const ARG_REDIS_URL: &str = "--redis-url";
const ARG_REJECT_NON_LOCAL_ROOM_PLACEMENT: &str = "--reject-non-local-room-placement";
const ARG_ICE_SERVERS_JSON: &str = "--ice-servers-json";
const ARG_RTC_UDP_PORT: &str = "--rtc-udp-port";
const ARG_RTC_UDP_PORT_RANGE_START: &str = "--rtc-udp-port-range-start";
const ARG_RTC_UDP_PORT_RANGE_END: &str = "--rtc-udp-port-range-end";
const ARG_RTC_TCP_PORT: &str = "--rtc-tcp-port";
const ARG_RTC_ALLOW_TCP_FALLBACK: &str = "--rtc-allow-tcp-fallback";
const ARG_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS: &str = "--rtc-tcp-fallback-rtt-threshold-ms";
const ARG_RTC_ALLOW_UDP_UNSTABLE_FALLBACK: &str = "--rtc-allow-udp-unstable-fallback";
const ARG_RTC_USE_EXTERNAL_IP: &str = "--rtc-use-external-ip";
const ARG_RTC_NODE_IP: &str = "--rtc-node-ip";
const ARG_TURN_ENABLED: &str = "--turn-enabled";
const ARG_TURN_DOMAIN: &str = "--turn-domain";
const ARG_TURN_BIND: &str = "--turn-bind";
const ARG_TURN_UDP_PORT: &str = "--turn-udp-port";
const ARG_TURN_RELAY_PORT_RANGE_START: &str = "--turn-relay-port-range-start";
const ARG_TURN_RELAY_PORT_RANGE_END: &str = "--turn-relay-port-range-end";
const ARG_TURN_CREDENTIAL_TTL_SECONDS: &str = "--turn-credential-ttl-seconds";
const ARG_TURN_ALLOW_RESTRICTED_PEER_CIDRS: &str = "--turn-allow-restricted-peer-cidrs";
const ARG_TURN_DENY_PEER_CIDRS: &str = "--turn-deny-peer-cidrs";
const ARG_TURN_TLS_PORT: &str = "--turn-tls-port";
const ARG_TURN_USERNAME: &str = "--turn-username";
const ARG_TURN_CREDENTIAL: &str = "--turn-credential";
const ARG_TURN_REQUIRE_REACHABLE: &str = "--turn-require-reachable";
const ARG_TURN_PROBE_TIMEOUT_MS: &str = "--turn-probe-timeout-ms";
const ARG_REGION: &str = "--region";
const ARG_NODE_SELECTOR_KIND: &str = "--node-selector-kind";
const ARG_NODE_SELECTOR_REGIONS_JSON: &str = "--node-selector-regions-json";
const ARG_NODE_SELECTOR_SORT_BY: &str = "--node-selector-sort-by";
const ARG_NODE_SELECTOR_ALGORITHM: &str = "--node-selector-algorithm";
const ARG_NODE_SELECTOR_CPU_LOAD_LIMIT: &str = "--node-selector-cpu-load-limit";
const ARG_NODE_SELECTOR_SYSTEM_LOAD_LIMIT: &str = "--node-selector-system-load-limit";
const ARG_NODE_SELECTOR_AVAILABLE_SECONDS: &str = "--node-selector-available-seconds";

/// Room-node directory backend choice for distributed room allocation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomNodeDirectoryBackend {
    /// In-process in-memory state.
    Memory,
    /// Redis-backed shared state.
    Redis,
}

impl RoomNodeDirectoryBackend {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "memory" => Some(Self::Memory),
            "redis" => Some(Self::Redis),
            _ => None,
        }
    }
}

/// Node selector choice for room placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeSelectorKind {
    /// Choose first candidate node by id.
    First,
    /// Prefer nearest region relative to local node region.
    RegionAware,
    /// LiveKit-style any selector with sort + algorithm.
    Any,
    /// LiveKit-style CPU-load selector.
    CpuLoad,
    /// LiveKit-style system-load selector.
    SystemLoad,
}

impl NodeSelectorKind {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "first" => Some(Self::First),
            "regionaware" => Some(Self::RegionAware),
            "any" => Some(Self::Any),
            "cpuload" => Some(Self::CpuLoad),
            "systemload" => Some(Self::SystemLoad),
            _ => None,
        }
    }
}

/// Node selector metric sort order for LiveKit-style selectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeSelectorSortBy {
    Random,
    SystemLoad,
    CpuLoad,
    Rooms,
    Clients,
    Tracks,
    BytesPerSec,
}

impl NodeSelectorSortBy {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "random" => Some(Self::Random),
            "sysload" => Some(Self::SystemLoad),
            "cpuload" => Some(Self::CpuLoad),
            "rooms" => Some(Self::Rooms),
            "clients" => Some(Self::Clients),
            "tracks" => Some(Self::Tracks),
            "bytespersec" => Some(Self::BytesPerSec),
            _ => None,
        }
    }
}

/// Node selector algorithm for LiveKit-style selectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeSelectorAlgorithm {
    Lowest,
    TwoChoice,
}

impl NodeSelectorAlgorithm {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "lowest" => Some(Self::Lowest),
            "twochoice" => Some(Self::TwoChoice),
            _ => None,
        }
    }
}

/// Region coordinates used by region-aware node selection.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeSelectorRegion {
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

/// ICE server configuration advertised in join/reconnect responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IceServerConfig {
    pub urls: Vec<String>,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub credential: String,
}

fn default_ice_servers() -> Vec<IceServerConfig> {
    vec![IceServerConfig {
        urls: vec!["stun:stun.l.google.com:19302".to_string()],
        username: String::new(),
        credential: String::new(),
    }]
}

fn normalize_ice_servers(
    key: &'static str,
    value: Vec<IceServerConfig>,
) -> Result<Vec<IceServerConfig>, ConfigError> {
    value
        .into_iter()
        .enumerate()
        .map(|(index, mut server)| {
            server.urls = server
                .urls
                .into_iter()
                .map(|url| url.trim().to_string())
                .filter(|url| !url.is_empty())
                .collect();
            server.username = server.username.trim().to_string();
            server.credential = server.credential.trim().to_string();

            if server.urls.is_empty() {
                return Err(ConfigError::InvalidIceServer {
                    key,
                    message: format!("ice server at index {index} must include at least one URL"),
                });
            }
            Ok(server)
        })
        .collect()
}

/// Runtime configuration shared by OxideSFU server components.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    /// Address that the HTTP/WebSocket server binds to.
    pub bind: SocketAddr,
    /// Development API key used by early compatibility tests.
    pub api_key: ApiKey,
    /// Development API secret used by early compatibility tests.
    pub api_secret: ApiSecret,
    /// API key -> secret map used by token verification and compatibility paths.
    pub api_keys: HashMap<ApiKey, ApiSecret>,
    /// Interval between periodic empty-room cleanup sweeps.
    pub room_cleanup_interval: Duration,
    /// Maximum age for an empty room before cleanup removes it.
    pub empty_room_max_age: Duration,
    /// Whether a signal join implicitly creates a missing room.
    pub room_auto_create: bool,
    /// Default maximum participants for rooms without an explicit service value; zero is unlimited.
    pub room_max_participants: u32,
    /// Default empty-room timeout in seconds for rooms without an explicit service value.
    pub room_empty_timeout_seconds: u32,
    /// Default post-departure timeout in seconds for rooms without an explicit service value.
    pub room_departure_timeout_seconds: u32,
    /// Backend used for room-node directory state.
    pub room_node_directory_backend: RoomNodeDirectoryBackend,
    /// Redis URL used when the room-node directory backend is `redis`.
    pub redis_url: Option<String>,
    /// When true, signalling join rejects non-local room placement instead of falling back to local handling.
    pub reject_non_local_room_placement: bool,
    /// ICE servers advertised to clients in join/reconnect responses.
    pub ice_servers: Vec<IceServerConfig>,
    /// Optional RTC UDP port used for host-candidate bind (LiveKit-like `rtc.udp_port` shape).
    pub rtc_udp_port: Option<u16>,
    /// Optional inclusive RTC UDP port range start (LiveKit-like `rtc.port_range_start`).
    pub rtc_udp_port_range_start: Option<u16>,
    /// Optional inclusive RTC UDP port range end (LiveKit-like `rtc.port_range_end`).
    pub rtc_udp_port_range_end: Option<u16>,
    /// RTC TCP port advertised/used for ICE/TCP connectivity.
    pub rtc_tcp_port: u16,
    /// Enables ICE/TCP fallback behavior for unstable UDP paths.
    pub rtc_allow_tcp_fallback: bool,
    /// Signaling RTT threshold in milliseconds for ICE/TCP fallback.
    pub rtc_tcp_fallback_rtt_threshold_ms: u32,
    /// Enables migration from unstable UDP to TCP/TLS fallback.
    pub rtc_allow_udp_unstable_fallback: bool,
    /// Enables 1:1 NAT external IP advertisement behavior.
    pub rtc_use_external_ip: bool,
    /// External/public node IP used when `rtc_use_external_ip` is enabled.
    pub rtc_node_ip: Option<String>,
    /// Minimum reliable data-channel bitrate in bits per second before a blocked writer drops packets.
    pub datachannel_slow_threshold: Option<u32>,
    /// Enables participant data-blob request/response compatibility paths.
    pub participant_data_blob_enabled: bool,
    /// Whether OxideSFU owns an in-process UDP TURN runtime.
    pub turn_enabled: bool,
    /// TURN domain used to synthesize TURN ice-server advertisements.
    pub turn_domain: Option<String>,
    /// IP address on which the owned TURN UDP runtime listens.
    pub turn_bind: String,
    /// Public IP advertised by the owned TURN runtime when it runs behind NAT.
    pub turn_external_ip: Option<String>,
    /// Optional TURN UDP port used to synthesize `turn:` URLs.
    pub turn_udp_port: Option<u16>,
    /// Public TURN TLS port used to synthesize `turns:` URLs.
    pub turn_tls_port: Option<u16>,
    /// Internal TCP listener for the owned TLS TURN runtime.
    pub turn_tls_bind: Option<SocketAddr>,
    /// PEM certificate chain used by the owned TLS TURN runtime.
    pub turn_tls_cert_file: Option<String>,
    /// PEM private key used by the owned TLS TURN runtime.
    pub turn_tls_key_file: Option<String>,
    /// Optional inclusive relay port range start for the owned TURN runtime.
    pub turn_relay_port_range_start: Option<u16>,
    /// Optional inclusive relay port range end for the owned TURN runtime.
    pub turn_relay_port_range_end: Option<u16>,
    /// TTL used when minting per-participant LiveKit TURN credentials.
    pub turn_credential_ttl_seconds: u64,
    /// CIDRs permitted to contact restricted peers.
    pub turn_allow_restricted_peer_cidrs: Vec<String>,
    /// CIDRs denied even when a peer is otherwise allowed.
    pub turn_deny_peer_cidrs: Vec<String>,
    /// Optional static TURN username override for synthesized TURN entries.
    pub turn_username: Option<String>,
    /// Optional static TURN credential override for synthesized TURN entries.
    pub turn_credential: Option<String>,
    /// When true, server startup validates configured TURN endpoints are reachable.
    pub turn_require_reachable: bool,
    /// Timeout in milliseconds for startup TURN reachability probes.
    pub turn_probe_timeout_ms: u64,
    /// Optional API key used for webhook signing.
    pub webhook_api_key: Option<ApiKey>,
    /// Webhook destination URLs.
    pub webhook_urls: Vec<String>,
    /// Local node region identifier for distributed selection.
    pub region: String,
    /// Selector policy for assigning new room ownership.
    pub node_selector_kind: NodeSelectorKind,
    /// Region coordinates used when `node_selector_kind` is `regionaware`.
    pub node_selector_regions: Vec<NodeSelectorRegion>,
    /// Metric used by LiveKit-style selector kinds (`any`, `cpuload`, `systemload`).
    pub node_selector_sort_by: NodeSelectorSortBy,
    /// Algorithm used by LiveKit-style selector kinds (`any`, `cpuload`, `systemload`).
    pub node_selector_algorithm: NodeSelectorAlgorithm,
    /// CPU load threshold used by `cpuload` selector before fallback to all available nodes.
    pub node_selector_cpu_load_limit: f32,
    /// System load threshold used by `systemload` selector before fallback to all available nodes.
    pub node_selector_system_load_limit: f32,
    /// Freshness window in seconds for node availability.
    pub node_selector_available_seconds: i64,
}

/// Configuration loading errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid bind address in {ENV_BIND}: {value}")]
    InvalidBind { value: String },
    #[error("invalid socket address in {key}: {value}")]
    InvalidSocketAddress {
        key: &'static str,
        value: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("invalid integer millis in {key}: {value}")]
    InvalidMillis {
        key: &'static str,
        value: String,
        #[source]
        source: ParseIntError,
    },
    #[error("invalid room node directory backend in {key}: {value}")]
    InvalidRoomNodeDirectoryBackend { key: &'static str, value: String },
    #[error("invalid boolean in {key}: {value}")]
    InvalidBoolean { key: &'static str, value: String },
    #[error("invalid integer in {key}: {value}")]
    InvalidInteger {
        key: &'static str,
        value: String,
        #[source]
        source: ParseIntError,
    },
    #[error("missing value for argument {arg}")]
    MissingArgumentValue { arg: &'static str },
    #[error("unknown argument {arg}")]
    UnknownArgument { arg: String },
    #[error("failed to read config file at {path}: {source}")]
    ReadConfigFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read secret file from {key} at {path}: {source}")]
    ReadSecretFile {
        key: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid config file line: {line}")]
    InvalidConfigLine { line: String },
    #[error("invalid transport config: {message}")]
    InvalidTransportConfig { message: String },
    #[error("invalid ICE servers JSON in {key}: {source}")]
    InvalidIceServersJson {
        key: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid ICE server in {key}: {message}")]
    InvalidIceServer { key: &'static str, message: String },
    #[error("invalid webhook URL in {key}: {value}")]
    InvalidWebhookUrl { key: &'static str, value: String },
    #[error("invalid node selector kind in {key}: {value}")]
    InvalidNodeSelectorKind { key: &'static str, value: String },
    #[error("invalid node selector regions JSON in {key}: {source}")]
    InvalidNodeSelectorRegionsJson {
        key: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid node selector sort-by in {key}: {value}")]
    InvalidNodeSelectorSortBy { key: &'static str, value: String },
    #[error("invalid node selector algorithm in {key}: {value}")]
    InvalidNodeSelectorAlgorithm { key: &'static str, value: String },
    #[error("invalid float in {key}: {value}")]
    InvalidFloat {
        key: &'static str,
        value: String,
        #[source]
        source: std::num::ParseFloatError,
    },
}

fn parse_bool_like(value: &str, key: &'static str) -> Result<bool, ConfigError> {
    if value.eq_ignore_ascii_case("1")
        || value.eq_ignore_ascii_case("true")
        || value.eq_ignore_ascii_case("yes")
        || value.eq_ignore_ascii_case("on")
    {
        return Ok(true);
    }
    if value.eq_ignore_ascii_case("0")
        || value.eq_ignore_ascii_case("false")
        || value.eq_ignore_ascii_case("no")
        || value.eq_ignore_ascii_case("off")
    {
        return Ok(false);
    }

    Err(ConfigError::InvalidBoolean {
        key,
        value: value.to_string(),
    })
}

fn read_secret_file(path: &str, key: &'static str) -> Result<String, ConfigError> {
    let secret = fs::read_to_string(path).map_err(|source| ConfigError::ReadSecretFile {
        key,
        path: path.to_string(),
        source,
    })?;
    Ok(secret.trim().to_string())
}

#[derive(Debug, Deserialize)]
struct NodeSelectorRegionWire {
    name: String,
    lat: f64,
    lon: f64,
}

fn normalize_node_selector_regions(
    _key: &'static str,
    value: Vec<NodeSelectorRegionWire>,
) -> Result<Vec<NodeSelectorRegion>, ConfigError> {
    Ok(value
        .into_iter()
        .map(|region| NodeSelectorRegion {
            name: region.name.trim().to_string(),
            lat: region.lat,
            lon: region.lon,
        })
        .collect::<Vec<_>>())
}

fn is_valid_webhook_url(url: &str) -> bool {
    let trimmed = url.trim();
    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return false;
    };
    if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")) {
        return false;
    }
    if rest.is_empty() {
        return false;
    }
    let host_port = rest.split('/').next().unwrap_or_default();
    !host_port.is_empty()
}

fn is_valid_domain(domain: &str) -> bool {
    let trimmed = domain.trim();
    if trimmed.is_empty() {
        return false;
    }

    // LiveKit-compatible domain validation contract:
    // ^(?i)[a-z0-9-]+(\.[a-z0-9-]+)+\.?$
    let without_trailing_dot = trimmed.strip_suffix('.').unwrap_or(trimmed);
    let mut labels = without_trailing_dot.split('.');
    let Some(first) = labels.next() else {
        return false;
    };

    let is_valid_label = |label: &str| {
        !label.is_empty()
            && label
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
    };

    if !is_valid_label(first) {
        return false;
    }

    let mut label_count = 1usize;
    for label in labels {
        if !is_valid_label(label) {
            return false;
        }
        label_count = label_count.saturating_add(1);
    }

    // Require at least one dot (two labels), matching upstream test contract.
    label_count >= 2
}

fn resolve_config_text(config_file: &str, config_body: &str) -> Result<String, ConfigError> {
    if !config_body.is_empty() {
        return Ok(config_body.to_string());
    }

    if config_file.is_empty() {
        return Ok(String::new());
    }

    fs::read_to_string(config_file).map_err(|source| ConfigError::ReadConfigFile {
        path: config_file.to_string(),
        source,
    })
}

impl ServerConfig {
    /// Creates a development configuration suitable for local compatibility tests.
    pub fn development() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 7880)),
            api_key: "devkey".to_string(),
            api_secret: "secret".to_string(),
            api_keys: HashMap::from([("devkey".to_string(), "secret".to_string())]),
            room_cleanup_interval: Duration::from_secs(30),
            empty_room_max_age: Duration::from_secs(60),
            room_auto_create: true,
            room_max_participants: 0,
            room_empty_timeout_seconds: 300,
            room_departure_timeout_seconds: 20,
            room_node_directory_backend: RoomNodeDirectoryBackend::Memory,
            redis_url: None,
            reject_non_local_room_placement: false,
            ice_servers: default_ice_servers(),
            rtc_udp_port: None,
            rtc_udp_port_range_start: None,
            rtc_udp_port_range_end: None,
            rtc_tcp_port: 7881,
            rtc_allow_tcp_fallback: true,
            rtc_tcp_fallback_rtt_threshold_ms: 0,
            rtc_allow_udp_unstable_fallback: false,
            rtc_use_external_ip: false,
            rtc_node_ip: None,
            datachannel_slow_threshold: None,
            participant_data_blob_enabled: true,
            turn_enabled: false,
            turn_domain: None,
            turn_bind: "0.0.0.0".to_string(),
            turn_external_ip: None,
            turn_udp_port: None,
            turn_tls_port: None,
            turn_tls_bind: None,
            turn_tls_cert_file: None,
            turn_tls_key_file: None,
            turn_relay_port_range_start: None,
            turn_relay_port_range_end: None,
            turn_credential_ttl_seconds: 86_400,
            turn_allow_restricted_peer_cidrs: Vec::new(),
            turn_deny_peer_cidrs: Vec::new(),
            turn_username: None,
            turn_credential: None,
            turn_require_reachable: false,
            turn_probe_timeout_ms: 1_500,
            webhook_api_key: None,
            webhook_urls: Vec::new(),
            region: "local".to_string(),
            node_selector_kind: NodeSelectorKind::First,
            node_selector_regions: Vec::new(),
            node_selector_sort_by: NodeSelectorSortBy::CpuLoad,
            node_selector_algorithm: NodeSelectorAlgorithm::TwoChoice,
            node_selector_cpu_load_limit: 0.8,
            node_selector_system_load_limit: 1.0,
            node_selector_available_seconds: 5,
        }
    }

    /// Loads config overrides from environment variables, falling back to development defaults.
    pub fn from_env_or_development() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Loads config using env overrides first, then CLI argument overrides.
    ///
    /// Supported args:
    /// - `--bind <host:port>`
    /// - `--api-key <key>`
    /// - `--api-secret <secret>`
    /// - `--room-cleanup-interval-ms <millis>`
    /// - `--empty-room-max-age-ms <millis>`
    /// - `--room-node-directory-backend <memory|redis>`
    /// - `--redis-url <redis-url>`
    /// - `--reject-non-local-room-placement <true|false|1|0|yes|no|on|off>`
    /// - `--ice-servers-json <json-array>`
    /// - `--rtc-udp-port <port>`
    /// - `--rtc-udp-port-range-start <port>`
    /// - `--rtc-udp-port-range-end <port>`
    /// - `--rtc-tcp-port <port>`
    /// - `--rtc-allow-tcp-fallback <true|false|1|0|yes|no|on|off>`
    /// - `--rtc-tcp-fallback-rtt-threshold-ms <millis>`
    /// - `--rtc-allow-udp-unstable-fallback <true|false|1|0|yes|no|on|off>`
    /// - `--rtc-use-external-ip <true|false|1|0|yes|no|on|off>`
    /// - `--rtc-node-ip <ip>`
    /// - `--turn-domain <domain>`
    /// - `--turn-udp-port <port>`
    /// - `--turn-tls-port <port>`
    /// - `--turn-username <username>`
    /// - `--turn-credential <credential>`
    /// - `--turn-require-reachable <true|false|1|0|yes|no|on|off>`
    /// - `--turn-probe-timeout-ms <millis>`
    /// - `--region <name>`
    /// - `--node-selector-kind <first|regionaware|any|cpuload|systemload>`
    /// - `--node-selector-regions-json <json-array>`
    /// - `--node-selector-sort-by <random|sysload|cpuload|rooms|clients|tracks|bytespersec>`
    /// - `--node-selector-algorithm <lowest|twochoice>`
    /// - `--node-selector-cpu-load-limit <float>`
    /// - `--node-selector-system-load-limit <float>`
    /// - `--node-selector-available-seconds <integer>`
    pub fn from_env_args_or_development<I>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = String>,
    {
        let config = Self::from_lookup(|key| std::env::var(key).ok())?;
        Self::apply_args(config, args)
    }

    fn from_lookup<F>(lookup: F) -> Result<Self, ConfigError>
    where
        F: Fn(&'static str) -> Option<String>,
    {
        let mut config = Self::development();

        if let Some(bind) = lookup(ENV_BIND) {
            config.apply_kv(ENV_BIND, bind)?;
        }
        if let Some(api_key) = lookup(ENV_API_KEY) {
            config.apply_kv(ENV_API_KEY, api_key)?;
        } else if let Some(api_key_file) = lookup(ENV_API_KEY_FILE) {
            let api_key = read_secret_file(&api_key_file, ENV_API_KEY_FILE)?;
            config.apply_kv(ENV_API_KEY, api_key)?;
        }
        if let Some(api_secret) = lookup(ENV_API_SECRET) {
            config.apply_kv(ENV_API_SECRET, api_secret)?;
        } else if let Some(api_secret_file) = lookup(ENV_API_SECRET_FILE) {
            let api_secret = read_secret_file(&api_secret_file, ENV_API_SECRET_FILE)?;
            config.apply_kv(ENV_API_SECRET, api_secret)?;
        }
        if let Some(cleanup_interval_ms) = lookup(ENV_ROOM_CLEANUP_INTERVAL_MS) {
            config.apply_kv(ENV_ROOM_CLEANUP_INTERVAL_MS, cleanup_interval_ms)?;
        }
        if let Some(empty_room_max_age_ms) = lookup(ENV_EMPTY_ROOM_MAX_AGE_MS) {
            config.apply_kv(ENV_EMPTY_ROOM_MAX_AGE_MS, empty_room_max_age_ms)?;
        }
        if let Some(room_auto_create) = lookup(ENV_ROOM_AUTO_CREATE) {
            config.apply_kv(ENV_ROOM_AUTO_CREATE, room_auto_create)?;
        }
        if let Some(room_node_directory_backend) = lookup(ENV_ROOM_NODE_DIRECTORY_BACKEND) {
            config.apply_kv(ENV_ROOM_NODE_DIRECTORY_BACKEND, room_node_directory_backend)?;
        }
        if let Some(redis_url) = lookup(ENV_REDIS_URL) {
            config.apply_kv(ENV_REDIS_URL, redis_url)?;
        }
        if let Some(reject_non_local_room_placement) = lookup(ENV_REJECT_NON_LOCAL_ROOM_PLACEMENT) {
            config.apply_kv(
                ENV_REJECT_NON_LOCAL_ROOM_PLACEMENT,
                reject_non_local_room_placement,
            )?;
        }
        if let Some(ice_servers_json) = lookup(ENV_ICE_SERVERS_JSON) {
            config.apply_kv(ENV_ICE_SERVERS_JSON, ice_servers_json)?;
        }
        if let Some(rtc_udp_port) = lookup(ENV_RTC_UDP_PORT) {
            config.apply_kv(ENV_RTC_UDP_PORT, rtc_udp_port)?;
        }
        if let Some(rtc_udp_port_range_start) = lookup(ENV_RTC_UDP_PORT_RANGE_START) {
            config.apply_kv(ENV_RTC_UDP_PORT_RANGE_START, rtc_udp_port_range_start)?;
        }
        if let Some(rtc_udp_port_range_end) = lookup(ENV_RTC_UDP_PORT_RANGE_END) {
            config.apply_kv(ENV_RTC_UDP_PORT_RANGE_END, rtc_udp_port_range_end)?;
        }
        if let Some(rtc_tcp_port) = lookup(ENV_RTC_TCP_PORT) {
            config.apply_kv(ENV_RTC_TCP_PORT, rtc_tcp_port)?;
        }
        if let Some(rtc_allow_tcp_fallback) = lookup(ENV_RTC_ALLOW_TCP_FALLBACK) {
            config.apply_kv(ENV_RTC_ALLOW_TCP_FALLBACK, rtc_allow_tcp_fallback)?;
        }
        if let Some(rtc_tcp_fallback_rtt_threshold_ms) =
            lookup(ENV_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS)
        {
            config.apply_kv(
                ENV_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS,
                rtc_tcp_fallback_rtt_threshold_ms,
            )?;
        }
        if let Some(rtc_allow_udp_unstable_fallback) = lookup(ENV_RTC_ALLOW_UDP_UNSTABLE_FALLBACK) {
            config.apply_kv(
                ENV_RTC_ALLOW_UDP_UNSTABLE_FALLBACK,
                rtc_allow_udp_unstable_fallback,
            )?;
        }
        if let Some(rtc_use_external_ip) = lookup(ENV_RTC_USE_EXTERNAL_IP) {
            config.apply_kv(ENV_RTC_USE_EXTERNAL_IP, rtc_use_external_ip)?;
        }
        if let Some(rtc_node_ip) = lookup(ENV_RTC_NODE_IP) {
            config.apply_kv(ENV_RTC_NODE_IP, rtc_node_ip)?;
        }
        if let Some(datachannel_slow_threshold) = lookup(ENV_DATACHANNEL_SLOW_THRESHOLD) {
            config.apply_kv(ENV_DATACHANNEL_SLOW_THRESHOLD, datachannel_slow_threshold)?;
        }
        if let Some(participant_data_blob_enabled) = lookup(ENV_PARTICIPANT_DATA_BLOB_ENABLED) {
            config.apply_kv(
                ENV_PARTICIPANT_DATA_BLOB_ENABLED,
                participant_data_blob_enabled,
            )?;
        }
        if let Some(turn_enabled) = lookup(ENV_TURN_ENABLED) {
            config.apply_kv(ENV_TURN_ENABLED, turn_enabled)?;
        }
        if let Some(turn_domain) = lookup(ENV_TURN_DOMAIN) {
            config.apply_kv(ENV_TURN_DOMAIN, turn_domain)?;
        }
        if let Some(turn_bind) = lookup(ENV_TURN_BIND) {
            config.apply_kv(ENV_TURN_BIND, turn_bind)?;
        }
        if let Some(turn_external_ip) = lookup(ENV_TURN_EXTERNAL_IP) {
            config.apply_kv(ENV_TURN_EXTERNAL_IP, turn_external_ip)?;
        }
        if let Some(turn_udp_port) = lookup(ENV_TURN_UDP_PORT) {
            config.apply_kv(ENV_TURN_UDP_PORT, turn_udp_port)?;
        }
        if let Some(turn_tls_port) = lookup(ENV_TURN_TLS_PORT) {
            config.apply_kv(ENV_TURN_TLS_PORT, turn_tls_port)?;
        }
        if let Some(value) = lookup(ENV_TURN_TLS_BIND) {
            config.apply_kv(ENV_TURN_TLS_BIND, value)?;
        }
        if let Some(value) = lookup(ENV_TURN_TLS_CERT_FILE) {
            config.apply_kv(ENV_TURN_TLS_CERT_FILE, value)?;
        }
        if let Some(value) = lookup(ENV_TURN_TLS_KEY_FILE) {
            config.apply_kv(ENV_TURN_TLS_KEY_FILE, value)?;
        }
        if let Some(value) = lookup(ENV_TURN_RELAY_PORT_RANGE_START) {
            config.apply_kv(ENV_TURN_RELAY_PORT_RANGE_START, value)?;
        }
        if let Some(value) = lookup(ENV_TURN_RELAY_PORT_RANGE_END) {
            config.apply_kv(ENV_TURN_RELAY_PORT_RANGE_END, value)?;
        }
        if let Some(value) = lookup(ENV_TURN_CREDENTIAL_TTL_SECONDS) {
            config.apply_kv(ENV_TURN_CREDENTIAL_TTL_SECONDS, value)?;
        }
        if let Some(value) = lookup(ENV_TURN_ALLOW_RESTRICTED_PEER_CIDRS) {
            config.apply_kv(ENV_TURN_ALLOW_RESTRICTED_PEER_CIDRS, value)?;
        }
        if let Some(value) = lookup(ENV_TURN_DENY_PEER_CIDRS) {
            config.apply_kv(ENV_TURN_DENY_PEER_CIDRS, value)?;
        }
        if let Some(turn_username) = lookup(ENV_TURN_USERNAME) {
            config.apply_kv(ENV_TURN_USERNAME, turn_username)?;
        }
        if let Some(turn_credential) = lookup(ENV_TURN_CREDENTIAL) {
            config.apply_kv(ENV_TURN_CREDENTIAL, turn_credential)?;
        }
        if let Some(turn_require_reachable) = lookup(ENV_TURN_REQUIRE_REACHABLE) {
            config.apply_kv(ENV_TURN_REQUIRE_REACHABLE, turn_require_reachable)?;
        }
        if let Some(turn_probe_timeout_ms) = lookup(ENV_TURN_PROBE_TIMEOUT_MS) {
            config.apply_kv(ENV_TURN_PROBE_TIMEOUT_MS, turn_probe_timeout_ms)?;
        }
        if let Some(webhook_api_key) = lookup(ENV_WEBHOOK_API_KEY) {
            config.apply_kv(ENV_WEBHOOK_API_KEY, webhook_api_key)?;
        }
        if let Some(webhook_urls) = lookup(ENV_WEBHOOK_URLS) {
            config.apply_kv(ENV_WEBHOOK_URLS, webhook_urls)?;
        }
        if let Some(region) = lookup(ENV_REGION) {
            config.apply_kv(ENV_REGION, region)?;
        }
        if let Some(node_selector_kind) = lookup(ENV_NODE_SELECTOR_KIND) {
            config.apply_kv(ENV_NODE_SELECTOR_KIND, node_selector_kind)?;
        }
        if let Some(node_selector_regions_json) = lookup(ENV_NODE_SELECTOR_REGIONS_JSON) {
            config.apply_kv(ENV_NODE_SELECTOR_REGIONS_JSON, node_selector_regions_json)?;
        }
        if let Some(node_selector_sort_by) = lookup(ENV_NODE_SELECTOR_SORT_BY) {
            config.apply_kv(ENV_NODE_SELECTOR_SORT_BY, node_selector_sort_by)?;
        }
        if let Some(node_selector_algorithm) = lookup(ENV_NODE_SELECTOR_ALGORITHM) {
            config.apply_kv(ENV_NODE_SELECTOR_ALGORITHM, node_selector_algorithm)?;
        }
        if let Some(node_selector_cpu_load_limit) = lookup(ENV_NODE_SELECTOR_CPU_LOAD_LIMIT) {
            config.apply_kv(
                ENV_NODE_SELECTOR_CPU_LOAD_LIMIT,
                node_selector_cpu_load_limit,
            )?;
        }
        if let Some(node_selector_system_load_limit) = lookup(ENV_NODE_SELECTOR_SYSTEM_LOAD_LIMIT) {
            config.apply_kv(
                ENV_NODE_SELECTOR_SYSTEM_LOAD_LIMIT,
                node_selector_system_load_limit,
            )?;
        }
        if let Some(node_selector_available_seconds) = lookup(ENV_NODE_SELECTOR_AVAILABLE_SECONDS) {
            config.apply_kv(
                ENV_NODE_SELECTOR_AVAILABLE_SECONDS,
                node_selector_available_seconds,
            )?;
        }

        config.validate_transport_constraints()?;

        Ok(config)
    }

    fn apply_args<I>(mut config: Self, args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = String>,
    {
        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                ARG_CONFIG => {
                    let path = iter
                        .next()
                        .ok_or(ConfigError::MissingArgumentValue { arg: ARG_CONFIG })?;
                    config.apply_config_file(&path)?;
                }
                ARG_BIND => {
                    let value = iter
                        .next()
                        .ok_or(ConfigError::MissingArgumentValue { arg: ARG_BIND })?;
                    config.apply_kv(ARG_BIND, value)?;
                }
                ARG_API_KEY => {
                    let value = iter
                        .next()
                        .ok_or(ConfigError::MissingArgumentValue { arg: ARG_API_KEY })?;
                    config.apply_kv(ARG_API_KEY, value)?;
                }
                ARG_API_SECRET => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_API_SECRET,
                    })?;
                    config.apply_kv(ARG_API_SECRET, value)?;
                }
                ARG_ROOM_CLEANUP_INTERVAL_MS => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_ROOM_CLEANUP_INTERVAL_MS,
                    })?;
                    config.apply_kv(ARG_ROOM_CLEANUP_INTERVAL_MS, value)?;
                }
                ARG_EMPTY_ROOM_MAX_AGE_MS => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_EMPTY_ROOM_MAX_AGE_MS,
                    })?;
                    config.apply_kv(ARG_EMPTY_ROOM_MAX_AGE_MS, value)?;
                }
                ARG_ROOM_NODE_DIRECTORY_BACKEND => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_ROOM_NODE_DIRECTORY_BACKEND,
                    })?;
                    config.apply_kv(ARG_ROOM_NODE_DIRECTORY_BACKEND, value)?;
                }
                ARG_REDIS_URL => {
                    let value = iter
                        .next()
                        .ok_or(ConfigError::MissingArgumentValue { arg: ARG_REDIS_URL })?;
                    config.apply_kv(ARG_REDIS_URL, value)?;
                }
                ARG_REJECT_NON_LOCAL_ROOM_PLACEMENT => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_REJECT_NON_LOCAL_ROOM_PLACEMENT,
                    })?;
                    config.apply_kv(ARG_REJECT_NON_LOCAL_ROOM_PLACEMENT, value)?;
                }
                ARG_ICE_SERVERS_JSON => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_ICE_SERVERS_JSON,
                    })?;
                    config.apply_kv(ARG_ICE_SERVERS_JSON, value)?;
                }
                ARG_RTC_UDP_PORT => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_UDP_PORT,
                    })?;
                    config.apply_kv(ARG_RTC_UDP_PORT, value)?;
                }
                ARG_RTC_UDP_PORT_RANGE_START => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_UDP_PORT_RANGE_START,
                    })?;
                    config.apply_kv(ARG_RTC_UDP_PORT_RANGE_START, value)?;
                }
                ARG_RTC_UDP_PORT_RANGE_END => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_UDP_PORT_RANGE_END,
                    })?;
                    config.apply_kv(ARG_RTC_UDP_PORT_RANGE_END, value)?;
                }
                ARG_RTC_TCP_PORT => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_TCP_PORT,
                    })?;
                    config.apply_kv(ARG_RTC_TCP_PORT, value)?;
                }
                ARG_RTC_ALLOW_TCP_FALLBACK => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_ALLOW_TCP_FALLBACK,
                    })?;
                    config.apply_kv(ARG_RTC_ALLOW_TCP_FALLBACK, value)?;
                }
                ARG_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS,
                    })?;
                    config.apply_kv(ARG_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS, value)?;
                }
                ARG_RTC_ALLOW_UDP_UNSTABLE_FALLBACK => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_ALLOW_UDP_UNSTABLE_FALLBACK,
                    })?;
                    config.apply_kv(ARG_RTC_ALLOW_UDP_UNSTABLE_FALLBACK, value)?;
                }
                ARG_RTC_USE_EXTERNAL_IP => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_USE_EXTERNAL_IP,
                    })?;
                    config.apply_kv(ARG_RTC_USE_EXTERNAL_IP, value)?;
                }
                ARG_RTC_NODE_IP => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_RTC_NODE_IP,
                    })?;
                    config.apply_kv(ARG_RTC_NODE_IP, value)?;
                }
                ARG_TURN_ENABLED => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_TURN_ENABLED,
                    })?;
                    config.apply_kv(ARG_TURN_ENABLED, value)?;
                }
                ARG_TURN_DOMAIN => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_TURN_DOMAIN,
                    })?;
                    config.apply_kv(ARG_TURN_DOMAIN, value)?;
                }
                ARG_TURN_BIND => {
                    let value = iter
                        .next()
                        .ok_or(ConfigError::MissingArgumentValue { arg: ARG_TURN_BIND })?;
                    config.apply_kv(ARG_TURN_BIND, value)?;
                }
                ARG_TURN_UDP_PORT => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_TURN_UDP_PORT,
                    })?;
                    config.apply_kv(ARG_TURN_UDP_PORT, value)?;
                }
                ARG_TURN_TLS_PORT => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_TURN_TLS_PORT,
                    })?;
                    config.apply_kv(ARG_TURN_TLS_PORT, value)?;
                }
                ARG_TURN_RELAY_PORT_RANGE_START
                | ARG_TURN_RELAY_PORT_RANGE_END
                | ARG_TURN_CREDENTIAL_TTL_SECONDS
                | ARG_TURN_ALLOW_RESTRICTED_PEER_CIDRS
                | ARG_TURN_DENY_PEER_CIDRS => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: match arg.as_str() {
                            ARG_TURN_RELAY_PORT_RANGE_START => ARG_TURN_RELAY_PORT_RANGE_START,
                            ARG_TURN_RELAY_PORT_RANGE_END => ARG_TURN_RELAY_PORT_RANGE_END,
                            ARG_TURN_CREDENTIAL_TTL_SECONDS => ARG_TURN_CREDENTIAL_TTL_SECONDS,
                            ARG_TURN_ALLOW_RESTRICTED_PEER_CIDRS => {
                                ARG_TURN_ALLOW_RESTRICTED_PEER_CIDRS
                            }
                            _ => ARG_TURN_DENY_PEER_CIDRS,
                        },
                    })?;
                    config.apply_kv(&arg, value)?;
                }
                ARG_TURN_USERNAME => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_TURN_USERNAME,
                    })?;
                    config.apply_kv(ARG_TURN_USERNAME, value)?;
                }
                ARG_TURN_CREDENTIAL => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_TURN_CREDENTIAL,
                    })?;
                    config.apply_kv(ARG_TURN_CREDENTIAL, value)?;
                }
                ARG_TURN_REQUIRE_REACHABLE => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_TURN_REQUIRE_REACHABLE,
                    })?;
                    config.apply_kv(ARG_TURN_REQUIRE_REACHABLE, value)?;
                }
                ARG_TURN_PROBE_TIMEOUT_MS => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_TURN_PROBE_TIMEOUT_MS,
                    })?;
                    config.apply_kv(ARG_TURN_PROBE_TIMEOUT_MS, value)?;
                }
                ARG_REGION => {
                    let value = iter
                        .next()
                        .ok_or(ConfigError::MissingArgumentValue { arg: ARG_REGION })?;
                    config.apply_kv(ARG_REGION, value)?;
                }
                ARG_NODE_SELECTOR_KIND => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_NODE_SELECTOR_KIND,
                    })?;
                    config.apply_kv(ARG_NODE_SELECTOR_KIND, value)?;
                }
                ARG_NODE_SELECTOR_REGIONS_JSON => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_NODE_SELECTOR_REGIONS_JSON,
                    })?;
                    config.apply_kv(ARG_NODE_SELECTOR_REGIONS_JSON, value)?;
                }
                ARG_NODE_SELECTOR_SORT_BY => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_NODE_SELECTOR_SORT_BY,
                    })?;
                    config.apply_kv(ARG_NODE_SELECTOR_SORT_BY, value)?;
                }
                ARG_NODE_SELECTOR_ALGORITHM => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_NODE_SELECTOR_ALGORITHM,
                    })?;
                    config.apply_kv(ARG_NODE_SELECTOR_ALGORITHM, value)?;
                }
                ARG_NODE_SELECTOR_CPU_LOAD_LIMIT => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_NODE_SELECTOR_CPU_LOAD_LIMIT,
                    })?;
                    config.apply_kv(ARG_NODE_SELECTOR_CPU_LOAD_LIMIT, value)?;
                }
                ARG_NODE_SELECTOR_SYSTEM_LOAD_LIMIT => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_NODE_SELECTOR_SYSTEM_LOAD_LIMIT,
                    })?;
                    config.apply_kv(ARG_NODE_SELECTOR_SYSTEM_LOAD_LIMIT, value)?;
                }
                ARG_NODE_SELECTOR_AVAILABLE_SECONDS => {
                    let value = iter.next().ok_or(ConfigError::MissingArgumentValue {
                        arg: ARG_NODE_SELECTOR_AVAILABLE_SECONDS,
                    })?;
                    config.apply_kv(ARG_NODE_SELECTOR_AVAILABLE_SECONDS, value)?;
                }
                other => {
                    return Err(ConfigError::UnknownArgument {
                        arg: other.to_string(),
                    });
                }
            }
        }

        config.validate_transport_constraints()?;

        Ok(config)
    }

    fn validate_transport_constraints(&self) -> Result<(), ConfigError> {
        let range_start = self.rtc_udp_port_range_start;
        let range_end = self.rtc_udp_port_range_end;
        if range_start.is_some() ^ range_end.is_some() {
            return Err(ConfigError::InvalidTransportConfig {
                message: "OXIDESFU_RTC_UDP_PORT_RANGE_START and OXIDESFU_RTC_UDP_PORT_RANGE_END must be set together".to_string(),
            });
        }
        if let (Some(start), Some(end)) = (range_start, range_end)
            && start > end
        {
            return Err(ConfigError::InvalidTransportConfig {
                message:
                    "OXIDESFU_RTC_UDP_PORT_RANGE_START must be <= OXIDESFU_RTC_UDP_PORT_RANGE_END"
                        .to_string(),
            });
        }
        if self.rtc_udp_port.is_some() && range_start.is_some() {
            return Err(ConfigError::InvalidTransportConfig {
                message: "OXIDESFU_RTC_UDP_PORT cannot be combined with OXIDESFU_RTC_UDP_PORT_RANGE_START/END".to_string(),
            });
        }

        if let Some(turn_domain) = self.turn_domain.as_deref()
            && !is_valid_domain(turn_domain)
            && !(self.turn_enabled && turn_domain.parse::<std::net::IpAddr>().is_ok())
        {
            return Err(ConfigError::InvalidTransportConfig {
                message:
                    "OXIDESFU_TURN_DOMAIN must be a bare domain name (no scheme, path, or query)"
                        .to_string(),
            });
        }

        let turn_domain_missing = self.turn_domain.as_deref().unwrap_or("").is_empty();
        if turn_domain_missing && (self.turn_udp_port.is_some() || self.turn_tls_port.is_some()) {
            return Err(ConfigError::InvalidTransportConfig {
                message: "OXIDESFU_TURN_UDP_PORT / OXIDESFU_TURN_TLS_PORT require OXIDESFU_TURN_DOMAIN to be set"
                    .to_string(),
            });
        }

        if self.turn_enabled {
            if turn_domain_missing || self.turn_udp_port.is_none() {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_ENABLED requires OXIDESFU_TURN_DOMAIN and OXIDESFU_TURN_UDP_PORT".to_string(),
                });
            }
            if self.turn_bind.parse::<std::net::IpAddr>().is_err() {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_BIND must be an IP address".to_string(),
                });
            }
            if self
                .turn_external_ip
                .as_deref()
                .is_some_and(|value| value.parse::<std::net::IpAddr>().is_err())
            {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_EXTERNAL_IP must be an IP address".to_string(),
                });
            }
            let tls_runtime_configured = self.turn_tls_bind.is_some()
                || self.turn_tls_cert_file.is_some()
                || self.turn_tls_key_file.is_some();
            if (self.turn_tls_port.is_some() || tls_runtime_configured)
                && (self.turn_tls_port.is_none()
                    || self.turn_tls_bind.is_none()
                    || self.turn_tls_cert_file.is_none()
                    || self.turn_tls_key_file.is_none())
            {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "owned TLS TURN requires OXIDESFU_TURN_TLS_PORT, OXIDESFU_TURN_TLS_BIND, OXIDESFU_TURN_TLS_CERT_FILE, and OXIDESFU_TURN_TLS_KEY_FILE".to_string(),
                });
            }
            if self.turn_credential_ttl_seconds == 0 {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_CREDENTIAL_TTL_SECONDS must be > 0".to_string(),
                });
            }
            if self.turn_relay_port_range_start.is_some() ^ self.turn_relay_port_range_end.is_some()
            {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_RELAY_PORT_RANGE_START and OXIDESFU_TURN_RELAY_PORT_RANGE_END must be set together".to_string(),
                });
            }
            if let (Some(start), Some(end)) = (
                self.turn_relay_port_range_start,
                self.turn_relay_port_range_end,
            ) && start > end
            {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_RELAY_PORT_RANGE_START must be <= OXIDESFU_TURN_RELAY_PORT_RANGE_END".to_string(),
                });
            }
        }

        if self.turn_require_reachable {
            if turn_domain_missing {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_REQUIRE_REACHABLE requires OXIDESFU_TURN_DOMAIN"
                        .to_string(),
                });
            }
            if self.turn_udp_port.is_none() && self.turn_tls_port.is_none() {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_REQUIRE_REACHABLE requires OXIDESFU_TURN_UDP_PORT and/or OXIDESFU_TURN_TLS_PORT".to_string(),
                });
            }
            if self.turn_probe_timeout_ms == 0 {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "OXIDESFU_TURN_PROBE_TIMEOUT_MS must be > 0 when TURN reachability probing is enabled".to_string(),
                });
            }
        }

        if !self.webhook_urls.is_empty() && self.webhook_api_key.is_none() {
            return Err(ConfigError::InvalidTransportConfig {
                message: "OXIDESFU_WEBHOOK_URLS requires OXIDESFU_WEBHOOK_API_KEY".to_string(),
            });
        }

        if let Some(webhook_api_key) = self.webhook_api_key.as_ref()
            && !self.api_keys.contains_key(webhook_api_key)
        {
            return Err(ConfigError::InvalidTransportConfig {
                message: "OXIDESFU_WEBHOOK_API_KEY must match a configured API key in this node"
                    .to_string(),
            });
        }

        if self.region.trim().is_empty() {
            return Err(ConfigError::InvalidTransportConfig {
                message: "OXIDESFU_REGION must not be empty".to_string(),
            });
        }
        if self.node_selector_kind == NodeSelectorKind::RegionAware {
            if self.node_selector_regions.is_empty() {
                return Err(ConfigError::InvalidTransportConfig {
                    message:
                        "regionaware node selector requires OXIDESFU_NODE_SELECTOR_REGIONS_JSON"
                            .to_string(),
                });
            }
            if !self
                .node_selector_regions
                .iter()
                .any(|region| region.name == self.region)
            {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "regionaware node selector regions must include OXIDESFU_REGION"
                        .to_string(),
                });
            }
        }
        for region in &self.node_selector_regions {
            if region.name.is_empty()
                || !(-90.0..=90.0).contains(&region.lat)
                || !(-180.0..=180.0).contains(&region.lon)
            {
                return Err(ConfigError::InvalidTransportConfig {
                    message: "node selector regions require non-empty name and valid lat/lon"
                        .to_string(),
                });
            }
        }

        if !self.node_selector_cpu_load_limit.is_finite() || self.node_selector_cpu_load_limit < 0.0
        {
            return Err(ConfigError::InvalidTransportConfig {
                message: "node selector cpu load limit must be a finite value >= 0".to_string(),
            });
        }
        if !self.node_selector_system_load_limit.is_finite()
            || self.node_selector_system_load_limit < 0.0
        {
            return Err(ConfigError::InvalidTransportConfig {
                message: "node selector system load limit must be a finite value >= 0".to_string(),
            });
        }
        if self.node_selector_available_seconds <= 0 {
            return Err(ConfigError::InvalidTransportConfig {
                message: "node selector available seconds must be > 0".to_string(),
            });
        }

        Ok(())
    }

    fn apply_config_file(&mut self, path: &str) -> Result<(), ConfigError> {
        let content = resolve_config_text(path, "")?;
        self.apply_config_file_content(&content)
    }

    fn apply_config_file_content(&mut self, content: &str) -> Result<(), ConfigError> {
        let lines = content.lines().collect::<Vec<_>>();
        let mut index = 0usize;

        while index < lines.len() {
            let raw_line = lines[index];
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                index += 1;
                continue;
            }

            if line == "keys:" {
                let mut key_lines = Vec::new();
                index += 1;
                while index < lines.len() {
                    let child_raw = lines[index];
                    let child_trimmed = child_raw.trim();
                    if child_trimmed.is_empty() || child_trimmed.starts_with('#') {
                        index += 1;
                        continue;
                    }
                    let is_indented = child_raw.starts_with(' ') || child_raw.starts_with('\t');
                    if !is_indented {
                        break;
                    }
                    key_lines.push(child_trimmed.to_string());
                    index += 1;
                }
                self.unmarshal_api_keys(&key_lines.join("\n"))?;
                continue;
            }

            let Some((key, value)) = line.split_once('=') else {
                return Err(ConfigError::InvalidConfigLine {
                    line: line.to_string(),
                });
            };
            self.apply_kv(key.trim(), value.trim().to_string())?;
            index += 1;
        }

        Ok(())
    }

    fn unmarshal_api_keys(&mut self, content: &str) -> Result<(), ConfigError> {
        let mut parsed = HashMap::<String, String>::new();
        let mut first_key = None::<String>;

        for raw_line in content.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let Some((key, secret)) = line.split_once(':') else {
                return Err(ConfigError::InvalidConfigLine {
                    line: line.to_string(),
                });
            };

            let key = key.trim();
            let secret = secret.trim();
            if key.is_empty() || secret.is_empty() {
                return Err(ConfigError::InvalidConfigLine {
                    line: line.to_string(),
                });
            }
            if first_key.is_none() {
                first_key = Some(key.to_string());
            }
            parsed.insert(key.to_string(), secret.to_string());
        }

        let Some(primary_key) = first_key else {
            return Err(ConfigError::InvalidConfigLine {
                line: "keys map cannot be empty".to_string(),
            });
        };

        let primary_secret =
            parsed
                .get(&primary_key)
                .cloned()
                .ok_or(ConfigError::InvalidConfigLine {
                    line: "primary key missing from keys map".to_string(),
                })?;

        self.api_key = primary_key;
        self.api_secret = primary_secret;
        self.api_keys = parsed;

        Ok(())
    }

    fn apply_kv(&mut self, key: &str, value: String) -> Result<(), ConfigError> {
        match key {
            ENV_BIND | ARG_BIND => {
                self.bind = value
                    .parse()
                    .map_err(|_| ConfigError::InvalidBind { value })?;
            }
            ENV_API_KEY | ARG_API_KEY => {
                self.api_key = value;
                self.api_keys
                    .insert(self.api_key.clone(), self.api_secret.clone());
            }
            ENV_API_SECRET | ARG_API_SECRET => {
                self.api_secret = value;
                self.api_keys
                    .insert(self.api_key.clone(), self.api_secret.clone());
            }
            ENV_ROOM_CLEANUP_INTERVAL_MS | ARG_ROOM_CLEANUP_INTERVAL_MS => {
                let millis = value
                    .parse::<u64>()
                    .map_err(|source| ConfigError::InvalidMillis {
                        key: ENV_ROOM_CLEANUP_INTERVAL_MS,
                        value,
                        source,
                    })?;
                self.room_cleanup_interval = Duration::from_millis(millis);
            }
            ENV_EMPTY_ROOM_MAX_AGE_MS | ARG_EMPTY_ROOM_MAX_AGE_MS => {
                let millis = value
                    .parse::<u64>()
                    .map_err(|source| ConfigError::InvalidMillis {
                        key: ENV_EMPTY_ROOM_MAX_AGE_MS,
                        value,
                        source,
                    })?;
                self.empty_room_max_age = Duration::from_millis(millis);
            }
            ENV_ROOM_AUTO_CREATE => {
                self.room_auto_create = parse_bool_like(&value, ENV_ROOM_AUTO_CREATE)?;
            }
            ENV_ROOM_NODE_DIRECTORY_BACKEND | ARG_ROOM_NODE_DIRECTORY_BACKEND => {
                self.room_node_directory_backend = RoomNodeDirectoryBackend::parse(&value).ok_or(
                    ConfigError::InvalidRoomNodeDirectoryBackend {
                        key: ENV_ROOM_NODE_DIRECTORY_BACKEND,
                        value,
                    },
                )?;
            }
            ENV_REDIS_URL | ARG_REDIS_URL => {
                self.redis_url = if value.is_empty() { None } else { Some(value) };
            }
            ENV_REJECT_NON_LOCAL_ROOM_PLACEMENT | ARG_REJECT_NON_LOCAL_ROOM_PLACEMENT => {
                self.reject_non_local_room_placement =
                    parse_bool_like(&value, ENV_REJECT_NON_LOCAL_ROOM_PLACEMENT)?;
            }
            ENV_ICE_SERVERS_JSON | ARG_ICE_SERVERS_JSON => {
                let parsed =
                    serde_json::from_str::<Vec<IceServerConfig>>(&value).map_err(|source| {
                        ConfigError::InvalidIceServersJson {
                            key: ENV_ICE_SERVERS_JSON,
                            source,
                        }
                    })?;
                self.ice_servers = normalize_ice_servers(ENV_ICE_SERVERS_JSON, parsed)?;
            }
            ENV_RTC_UDP_PORT | ARG_RTC_UDP_PORT => {
                self.rtc_udp_port =
                    Some(
                        value
                            .parse::<u16>()
                            .map_err(|source| ConfigError::InvalidInteger {
                                key: ENV_RTC_UDP_PORT,
                                value,
                                source,
                            })?,
                    );
            }
            ENV_RTC_UDP_PORT_RANGE_START | ARG_RTC_UDP_PORT_RANGE_START => {
                self.rtc_udp_port_range_start =
                    Some(
                        value
                            .parse::<u16>()
                            .map_err(|source| ConfigError::InvalidInteger {
                                key: ENV_RTC_UDP_PORT_RANGE_START,
                                value,
                                source,
                            })?,
                    );
            }
            ENV_RTC_UDP_PORT_RANGE_END | ARG_RTC_UDP_PORT_RANGE_END => {
                self.rtc_udp_port_range_end =
                    Some(
                        value
                            .parse::<u16>()
                            .map_err(|source| ConfigError::InvalidInteger {
                                key: ENV_RTC_UDP_PORT_RANGE_END,
                                value,
                                source,
                            })?,
                    );
            }
            ENV_RTC_TCP_PORT | ARG_RTC_TCP_PORT => {
                self.rtc_tcp_port =
                    value
                        .parse::<u16>()
                        .map_err(|source| ConfigError::InvalidInteger {
                            key: ENV_RTC_TCP_PORT,
                            value,
                            source,
                        })?;
            }
            ENV_RTC_ALLOW_TCP_FALLBACK | ARG_RTC_ALLOW_TCP_FALLBACK => {
                self.rtc_allow_tcp_fallback = parse_bool_like(&value, ENV_RTC_ALLOW_TCP_FALLBACK)?;
            }
            ENV_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS | ARG_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS => {
                self.rtc_tcp_fallback_rtt_threshold_ms =
                    value
                        .parse::<u32>()
                        .map_err(|source| ConfigError::InvalidInteger {
                            key: ENV_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS,
                            value,
                            source,
                        })?;
            }
            ENV_RTC_ALLOW_UDP_UNSTABLE_FALLBACK | ARG_RTC_ALLOW_UDP_UNSTABLE_FALLBACK => {
                self.rtc_allow_udp_unstable_fallback =
                    parse_bool_like(&value, ENV_RTC_ALLOW_UDP_UNSTABLE_FALLBACK)?;
            }
            ENV_RTC_USE_EXTERNAL_IP | ARG_RTC_USE_EXTERNAL_IP => {
                self.rtc_use_external_ip = parse_bool_like(&value, ENV_RTC_USE_EXTERNAL_IP)?;
            }
            ENV_RTC_NODE_IP | ARG_RTC_NODE_IP => {
                self.rtc_node_ip = if value.trim().is_empty() {
                    None
                } else {
                    Some(value.trim().to_string())
                };
            }
            ENV_DATACHANNEL_SLOW_THRESHOLD => {
                let threshold =
                    value
                        .parse::<u32>()
                        .map_err(|source| ConfigError::InvalidInteger {
                            key: ENV_DATACHANNEL_SLOW_THRESHOLD,
                            value,
                            source,
                        })?;
                self.datachannel_slow_threshold = (threshold != 0).then_some(threshold);
            }
            ENV_PARTICIPANT_DATA_BLOB_ENABLED => {
                self.participant_data_blob_enabled =
                    parse_bool_like(&value, ENV_PARTICIPANT_DATA_BLOB_ENABLED)?;
            }
            ENV_TURN_ENABLED | ARG_TURN_ENABLED => {
                self.turn_enabled = parse_bool_like(&value, ENV_TURN_ENABLED)?;
            }
            ENV_TURN_DOMAIN | ARG_TURN_DOMAIN => {
                self.turn_domain = if value.trim().is_empty() {
                    None
                } else {
                    Some(value.trim().to_string())
                };
            }
            ENV_TURN_BIND | ARG_TURN_BIND => {
                self.turn_bind = value.trim().to_string();
            }
            ENV_TURN_EXTERNAL_IP => {
                self.turn_external_ip =
                    (!value.trim().is_empty()).then(|| value.trim().to_string());
            }
            ENV_TURN_UDP_PORT | ARG_TURN_UDP_PORT => {
                self.turn_udp_port =
                    Some(
                        value
                            .parse::<u16>()
                            .map_err(|source| ConfigError::InvalidInteger {
                                key: ENV_TURN_UDP_PORT,
                                value,
                                source,
                            })?,
                    );
            }
            ENV_TURN_TLS_PORT | ARG_TURN_TLS_PORT => {
                self.turn_tls_port =
                    Some(
                        value
                            .parse::<u16>()
                            .map_err(|source| ConfigError::InvalidInteger {
                                key: ENV_TURN_TLS_PORT,
                                value,
                                source,
                            })?,
                    );
            }
            ENV_TURN_TLS_BIND => {
                self.turn_tls_bind = Some(value.parse::<SocketAddr>().map_err(|source| {
                    ConfigError::InvalidSocketAddress {
                        key: ENV_TURN_TLS_BIND,
                        value,
                        source,
                    }
                })?);
            }
            ENV_TURN_TLS_CERT_FILE => {
                self.turn_tls_cert_file =
                    (!value.trim().is_empty()).then(|| value.trim().to_string());
            }
            ENV_TURN_TLS_KEY_FILE => {
                self.turn_tls_key_file =
                    (!value.trim().is_empty()).then(|| value.trim().to_string());
            }
            ENV_TURN_RELAY_PORT_RANGE_START | ARG_TURN_RELAY_PORT_RANGE_START => {
                self.turn_relay_port_range_start = Some(value.parse::<u16>().map_err(
                    |source| ConfigError::InvalidInteger {
                        key: ENV_TURN_RELAY_PORT_RANGE_START,
                        value,
                        source,
                    },
                )?);
            }
            ENV_TURN_RELAY_PORT_RANGE_END | ARG_TURN_RELAY_PORT_RANGE_END => {
                self.turn_relay_port_range_end =
                    Some(
                        value
                            .parse::<u16>()
                            .map_err(|source| ConfigError::InvalidInteger {
                                key: ENV_TURN_RELAY_PORT_RANGE_END,
                                value,
                                source,
                            })?,
                    );
            }
            ENV_TURN_CREDENTIAL_TTL_SECONDS | ARG_TURN_CREDENTIAL_TTL_SECONDS => {
                self.turn_credential_ttl_seconds =
                    value
                        .parse::<u64>()
                        .map_err(|source| ConfigError::InvalidMillis {
                            key: ENV_TURN_CREDENTIAL_TTL_SECONDS,
                            value,
                            source,
                        })?;
            }
            ENV_TURN_ALLOW_RESTRICTED_PEER_CIDRS | ARG_TURN_ALLOW_RESTRICTED_PEER_CIDRS => {
                self.turn_allow_restricted_peer_cidrs = value
                    .split(',')
                    .map(str::trim)
                    .filter(|cidr| !cidr.is_empty())
                    .map(ToString::to_string)
                    .collect();
            }
            ENV_TURN_DENY_PEER_CIDRS | ARG_TURN_DENY_PEER_CIDRS => {
                self.turn_deny_peer_cidrs = value
                    .split(',')
                    .map(str::trim)
                    .filter(|cidr| !cidr.is_empty())
                    .map(ToString::to_string)
                    .collect();
            }
            ENV_TURN_USERNAME | ARG_TURN_USERNAME => {
                self.turn_username = if value.trim().is_empty() {
                    None
                } else {
                    Some(value.trim().to_string())
                };
            }
            ENV_TURN_CREDENTIAL | ARG_TURN_CREDENTIAL => {
                self.turn_credential = if value.trim().is_empty() {
                    None
                } else {
                    Some(value.trim().to_string())
                };
            }
            ENV_TURN_REQUIRE_REACHABLE | ARG_TURN_REQUIRE_REACHABLE => {
                self.turn_require_reachable = parse_bool_like(&value, ENV_TURN_REQUIRE_REACHABLE)?;
            }
            ENV_TURN_PROBE_TIMEOUT_MS | ARG_TURN_PROBE_TIMEOUT_MS => {
                self.turn_probe_timeout_ms =
                    value
                        .parse::<u64>()
                        .map_err(|source| ConfigError::InvalidMillis {
                            key: ENV_TURN_PROBE_TIMEOUT_MS,
                            value,
                            source,
                        })?;
            }
            ENV_WEBHOOK_API_KEY => {
                self.webhook_api_key = if value.trim().is_empty() {
                    None
                } else {
                    Some(value.trim().to_string())
                };
            }
            ENV_WEBHOOK_URLS => {
                self.webhook_urls = value
                    .split(',')
                    .map(str::trim)
                    .filter(|entry| !entry.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                for webhook_url in &self.webhook_urls {
                    if !is_valid_webhook_url(webhook_url) {
                        return Err(ConfigError::InvalidWebhookUrl {
                            key: ENV_WEBHOOK_URLS,
                            value: webhook_url.clone(),
                        });
                    }
                }
            }
            ENV_REGION | ARG_REGION => {
                self.region = value.trim().to_string();
            }
            ENV_NODE_SELECTOR_KIND | ARG_NODE_SELECTOR_KIND => {
                self.node_selector_kind = NodeSelectorKind::parse(&value).ok_or(
                    ConfigError::InvalidNodeSelectorKind {
                        key: ENV_NODE_SELECTOR_KIND,
                        value,
                    },
                )?;
            }
            ENV_NODE_SELECTOR_REGIONS_JSON | ARG_NODE_SELECTOR_REGIONS_JSON => {
                let parsed = serde_json::from_str::<Vec<NodeSelectorRegionWire>>(&value).map_err(
                    |source| ConfigError::InvalidNodeSelectorRegionsJson {
                        key: ENV_NODE_SELECTOR_REGIONS_JSON,
                        source,
                    },
                )?;
                self.node_selector_regions =
                    normalize_node_selector_regions(ENV_NODE_SELECTOR_REGIONS_JSON, parsed)?;
            }
            ENV_NODE_SELECTOR_SORT_BY | ARG_NODE_SELECTOR_SORT_BY => {
                self.node_selector_sort_by = NodeSelectorSortBy::parse(&value).ok_or(
                    ConfigError::InvalidNodeSelectorSortBy {
                        key: ENV_NODE_SELECTOR_SORT_BY,
                        value,
                    },
                )?;
            }
            ENV_NODE_SELECTOR_ALGORITHM | ARG_NODE_SELECTOR_ALGORITHM => {
                self.node_selector_algorithm = NodeSelectorAlgorithm::parse(&value).ok_or(
                    ConfigError::InvalidNodeSelectorAlgorithm {
                        key: ENV_NODE_SELECTOR_ALGORITHM,
                        value,
                    },
                )?;
            }
            ENV_NODE_SELECTOR_CPU_LOAD_LIMIT | ARG_NODE_SELECTOR_CPU_LOAD_LIMIT => {
                self.node_selector_cpu_load_limit =
                    value
                        .parse::<f32>()
                        .map_err(|source| ConfigError::InvalidFloat {
                            key: ENV_NODE_SELECTOR_CPU_LOAD_LIMIT,
                            value,
                            source,
                        })?;
            }
            ENV_NODE_SELECTOR_SYSTEM_LOAD_LIMIT | ARG_NODE_SELECTOR_SYSTEM_LOAD_LIMIT => {
                self.node_selector_system_load_limit =
                    value
                        .parse::<f32>()
                        .map_err(|source| ConfigError::InvalidFloat {
                            key: ENV_NODE_SELECTOR_SYSTEM_LOAD_LIMIT,
                            value,
                            source,
                        })?;
            }
            ENV_NODE_SELECTOR_AVAILABLE_SECONDS | ARG_NODE_SELECTOR_AVAILABLE_SECONDS => {
                self.node_selector_available_seconds =
                    value
                        .parse::<i64>()
                        .map_err(|source| ConfigError::InvalidInteger {
                            key: ENV_NODE_SELECTOR_AVAILABLE_SECONDS,
                            value,
                            source,
                        })?;
            }
            other => {
                return Err(ConfigError::UnknownArgument {
                    arg: other.to_string(),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;

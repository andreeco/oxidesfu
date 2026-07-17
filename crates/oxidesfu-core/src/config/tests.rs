use super::{
    ARG_API_KEY, ARG_BIND, ARG_CONFIG, ARG_EMPTY_ROOM_MAX_AGE_MS, ARG_ICE_SERVERS_JSON,
    ARG_NODE_SELECTOR_ALGORITHM, ARG_NODE_SELECTOR_AVAILABLE_SECONDS,
    ARG_NODE_SELECTOR_CPU_LOAD_LIMIT, ARG_NODE_SELECTOR_SORT_BY,
    ARG_NODE_SELECTOR_SYSTEM_LOAD_LIMIT, ARG_REDIS_URL, ARG_REJECT_NON_LOCAL_ROOM_PLACEMENT,
    ARG_ROOM_CLEANUP_INTERVAL_MS, ARG_ROOM_NODE_DIRECTORY_BACKEND, ARG_RTC_ALLOW_TCP_FALLBACK,
    ARG_RTC_ALLOW_UDP_UNSTABLE_FALLBACK, ARG_RTC_NODE_IP, ARG_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS,
    ARG_RTC_TCP_PORT, ARG_RTC_UDP_PORT, ARG_RTC_UDP_PORT_RANGE_END, ARG_RTC_UDP_PORT_RANGE_START,
    ARG_RTC_USE_EXTERNAL_IP, ARG_TURN_CREDENTIAL, ARG_TURN_DOMAIN, ARG_TURN_PROBE_TIMEOUT_MS,
    ARG_TURN_REQUIRE_REACHABLE, ARG_TURN_TLS_PORT, ARG_TURN_UDP_PORT, ARG_TURN_USERNAME,
    ConfigError, NodeSelectorAlgorithm, NodeSelectorKind, NodeSelectorSortBy,
    RoomNodeDirectoryBackend, ServerConfig, resolve_config_text,
};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[test]
fn unmarshal_keys_yaml_like_content_loads_api_keys_and_primary_pair() {
    let mut config = ServerConfig::development();

    config
        .unmarshal_api_keys("key1: secret1\nkey2: secret2")
        .expect("keys map should parse");

    assert_eq!(
        config.api_keys.get("key1").map(String::as_str),
        Some("secret1")
    );
    assert_eq!(
        config.api_keys.get("key2").map(String::as_str),
        Some("secret2")
    );
    assert_eq!(config.api_key, "key1");
    assert_eq!(config.api_secret, "secret1");
}

#[test]
fn apply_config_file_content_accepts_keys_block_and_updates_api_keys() {
    let mut config = ServerConfig::development();

    config
        .apply_config_file_content(
            "keys:\n  key1: secret1\n  key2: secret2\nOXIDESFU_BIND=127.0.0.1:7999",
        )
        .expect("keys block + regular kv lines should parse");

    assert_eq!(
        config.bind,
        "127.0.0.1:7999".parse().expect("socket should parse")
    );
    assert_eq!(
        config.api_keys.get("key1").map(String::as_str),
        Some("secret1")
    );
    assert_eq!(
        config.api_keys.get("key2").map(String::as_str),
        Some("secret2")
    );
    assert_eq!(config.api_key, "key1");
    assert_eq!(config.api_secret, "secret1");
}

#[test]
fn unmarshal_keys_yaml_like_content_rejects_invalid_lines() {
    let mut config = ServerConfig::development();

    let err = config
        .unmarshal_api_keys("key1 secret1")
        .expect_err("missing ':' should fail");

    assert!(matches!(err, ConfigError::InvalidConfigLine { .. }));
}

#[test]
fn development_config_uses_livekit_cli_compatible_defaults() {
    let config = ServerConfig::development();

    assert_eq!(
        config.bind,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7880)
    );
    assert_eq!(config.api_key, "devkey");
    assert_eq!(config.api_secret, "secret");
    assert_eq!(config.api_keys.len(), 1);
    assert_eq!(
        config.api_keys.get("devkey").map(String::as_str),
        Some("secret")
    );
    assert_eq!(config.room_cleanup_interval, Duration::from_secs(30));
    assert_eq!(config.empty_room_max_age, Duration::from_secs(60));
    assert!(config.room_auto_create);
    assert_eq!(
        config.room_node_directory_backend,
        RoomNodeDirectoryBackend::Memory
    );
    assert_eq!(config.redis_url, None);
    assert!(!config.reject_non_local_room_placement);
    assert_eq!(config.ice_servers.len(), 1);
    assert_eq!(
        config.ice_servers[0].urls,
        vec!["stun:stun.l.google.com:19302"]
    );
    assert_eq!(config.rtc_udp_port, None);
    assert_eq!(config.rtc_udp_port_range_start, None);
    assert_eq!(config.rtc_udp_port_range_end, None);
    assert_eq!(config.rtc_tcp_port, 7881);
    assert!(config.rtc_allow_tcp_fallback);
    assert_eq!(config.rtc_tcp_fallback_rtt_threshold_ms, 0);
    assert!(!config.rtc_allow_udp_unstable_fallback);
    assert!(!config.rtc_use_external_ip);
    assert_eq!(config.rtc_node_ip, None);
    assert_eq!(config.turn_domain, None);
    assert_eq!(config.turn_udp_port, None);
    assert_eq!(config.turn_tls_port, None);
    assert_eq!(config.turn_username, None);
    assert_eq!(config.turn_credential, None);
    assert!(!config.turn_require_reachable);
    assert_eq!(config.datachannel_slow_threshold, None);
    assert!(config.participant_data_blob_enabled);
    assert_eq!(config.turn_probe_timeout_ms, 1_500);
    assert_eq!(config.webhook_api_key, None);
    assert!(config.webhook_urls.is_empty());
    assert_eq!(config.region, "local");
    assert_eq!(config.node_selector_kind, NodeSelectorKind::First);
    assert!(config.node_selector_regions.is_empty());
    assert_eq!(config.node_selector_sort_by, NodeSelectorSortBy::CpuLoad);
    assert_eq!(
        config.node_selector_algorithm,
        NodeSelectorAlgorithm::TwoChoice
    );
    assert_eq!(config.node_selector_cpu_load_limit, 0.8);
    assert_eq!(config.node_selector_system_load_limit, 1.0);
    assert_eq!(config.node_selector_available_seconds, 5);
}

#[test]
fn from_lookup_applies_environment_overrides() {
    let env = HashMap::from([
        ("OXIDESFU_BIND", "127.0.0.1:9999".to_string()),
        ("OXIDESFU_API_KEY", "env-key".to_string()),
        ("OXIDESFU_API_SECRET", "env-secret".to_string()),
        ("OXIDESFU_ROOM_CLEANUP_INTERVAL_MS", "2500".to_string()),
        ("OXIDESFU_EMPTY_ROOM_MAX_AGE_MS", "15000".to_string()),
        ("OXIDESFU_ROOM_AUTO_CREATE", "false".to_string()),
        ("OXIDESFU_ROOM_NODE_DIRECTORY_BACKEND", "redis".to_string()),
        ("OXIDESFU_REDIS_URL", "redis://127.0.0.1:6379/0".to_string()),
        (
            "OXIDESFU_REJECT_NON_LOCAL_ROOM_PLACEMENT",
            "true".to_string(),
        ),
        (
            "OXIDESFU_ICE_SERVERS_JSON",
            r#"[{"urls":["stun:stun.example.net:3478"]},{"urls":["turn:turn.example.net:3478?transport=udp"],"username":"u","credential":"p"}]"#.to_string(),
        ),
        ("OXIDESFU_RTC_UDP_PORT", "50000".to_string()),
        ("OXIDESFU_RTC_TCP_PORT", "7999".to_string()),
        ("OXIDESFU_RTC_ALLOW_TCP_FALLBACK", "false".to_string()),
        (
            "OXIDESFU_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS",
            "250".to_string(),
        ),
        (
            "OXIDESFU_RTC_ALLOW_UDP_UNSTABLE_FALLBACK",
            "true".to_string(),
        ),
        ("OXIDESFU_RTC_USE_EXTERNAL_IP", "true".to_string()),
        ("OXIDESFU_RTC_NODE_IP", "203.0.113.10".to_string()),
        ("OXIDESFU_DATACHANNEL_SLOW_THRESHOLD", "21024".to_string()),
        ("OXIDESFU_PARTICIPANT_DATA_BLOB_ENABLED", "false".to_string()),
        ("OXIDESFU_TURN_DOMAIN", "turn.example.net".to_string()),
        ("OXIDESFU_TURN_UDP_PORT", "3478".to_string()),
        ("OXIDESFU_TURN_TLS_PORT", "5349".to_string()),
        ("OXIDESFU_TURN_TLS_BIND", "0.0.0.0:6349".to_string()),
        (
            "OXIDESFU_TURN_TLS_CERT_FILE",
            "/run/secrets/turn-cert.pem".to_string(),
        ),
        (
            "OXIDESFU_TURN_TLS_KEY_FILE",
            "/run/secrets/turn-key.pem".to_string(),
        ),
        ("OXIDESFU_TURN_USERNAME", "turn-user".to_string()),
        ("OXIDESFU_TURN_CREDENTIAL", "turn-pass".to_string()),
        ("OXIDESFU_TURN_REQUIRE_REACHABLE", "true".to_string()),
        ("OXIDESFU_TURN_PROBE_TIMEOUT_MS", "2500".to_string()),
        ("OXIDESFU_WEBHOOK_API_KEY", "env-key".to_string()),
        (
            "OXIDESFU_WEBHOOK_URLS",
            "https://hooks-a.example.test/events,https://hooks-b.example.test/events".to_string(),
        ),
        ("OXIDESFU_REGION", "eu-central".to_string()),
        ("OXIDESFU_NODE_SELECTOR_KIND", "systemload".to_string()),
        (
            "OXIDESFU_NODE_SELECTOR_REGIONS_JSON",
            r#"[{"name":"eu-central","lat":50.1,"lon":8.6},{"name":"us-east","lat":40.7,"lon":-74.0}]"#.to_string(),
        ),
        ("OXIDESFU_NODE_SELECTOR_SORT_BY", "clients".to_string()),
        ("OXIDESFU_NODE_SELECTOR_ALGORITHM", "lowest".to_string()),
        ("OXIDESFU_NODE_SELECTOR_CPU_LOAD_LIMIT", "0.65".to_string()),
        ("OXIDESFU_NODE_SELECTOR_SYSTEM_LOAD_LIMIT", "0.9".to_string()),
        ("OXIDESFU_NODE_SELECTOR_AVAILABLE_SECONDS", "8".to_string()),
    ]);

    let config = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("config should parse from env override map");

    assert_eq!(
        config.bind,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999)
    );
    assert_eq!(config.api_key, "env-key");
    assert_eq!(config.api_secret, "env-secret");
    assert_eq!(
        config.api_keys.get("env-key").map(String::as_str),
        Some("env-secret")
    );
    assert_eq!(config.room_cleanup_interval, Duration::from_millis(2500));
    assert_eq!(config.empty_room_max_age, Duration::from_millis(15000));
    assert!(!config.room_auto_create);
    assert_eq!(
        config.room_node_directory_backend,
        RoomNodeDirectoryBackend::Redis
    );
    assert_eq!(
        config.redis_url.as_deref(),
        Some("redis://127.0.0.1:6379/0")
    );
    assert!(config.reject_non_local_room_placement);
    assert_eq!(config.ice_servers.len(), 2);
    assert_eq!(
        config.ice_servers[0].urls,
        vec!["stun:stun.example.net:3478"]
    );
    assert_eq!(
        config.ice_servers[1].urls,
        vec!["turn:turn.example.net:3478?transport=udp"]
    );
    assert_eq!(config.ice_servers[1].username, "u");
    assert_eq!(config.ice_servers[1].credential, "p");
    assert_eq!(config.rtc_udp_port, Some(50000));
    assert_eq!(config.rtc_udp_port_range_start, None);
    assert_eq!(config.rtc_udp_port_range_end, None);
    assert_eq!(config.rtc_tcp_port, 7999);
    assert!(!config.rtc_allow_tcp_fallback);
    assert_eq!(config.rtc_tcp_fallback_rtt_threshold_ms, 250);
    assert!(config.rtc_allow_udp_unstable_fallback);
    assert!(config.rtc_use_external_ip);
    assert_eq!(config.rtc_node_ip.as_deref(), Some("203.0.113.10"));
    assert_eq!(config.datachannel_slow_threshold, Some(21_024));
    assert!(!config.participant_data_blob_enabled);
    assert_eq!(config.turn_domain.as_deref(), Some("turn.example.net"));
    assert_eq!(config.turn_udp_port, Some(3478));
    assert_eq!(config.turn_tls_port, Some(5349));
    assert_eq!(
        config.turn_tls_bind,
        Some("0.0.0.0:6349".parse().expect("test address should parse"))
    );
    assert_eq!(
        config.turn_tls_cert_file.as_deref(),
        Some("/run/secrets/turn-cert.pem")
    );
    assert_eq!(
        config.turn_tls_key_file.as_deref(),
        Some("/run/secrets/turn-key.pem")
    );
    assert_eq!(config.turn_username.as_deref(), Some("turn-user"));
    assert_eq!(config.turn_credential.as_deref(), Some("turn-pass"));
    assert!(config.turn_require_reachable);
    assert_eq!(config.turn_probe_timeout_ms, 2500);
    assert_eq!(config.webhook_api_key.as_deref(), Some("env-key"));
    assert_eq!(
        config.webhook_urls,
        vec![
            "https://hooks-a.example.test/events".to_string(),
            "https://hooks-b.example.test/events".to_string()
        ]
    );
    assert_eq!(config.region, "eu-central");
    assert_eq!(config.node_selector_kind, NodeSelectorKind::SystemLoad);
    assert_eq!(config.node_selector_regions.len(), 2);
    assert_eq!(config.node_selector_regions[0].name, "eu-central");
    assert_eq!(config.node_selector_sort_by, NodeSelectorSortBy::Clients);
    assert_eq!(
        config.node_selector_algorithm,
        NodeSelectorAlgorithm::Lowest
    );
    assert_eq!(config.node_selector_cpu_load_limit, 0.65);
    assert_eq!(config.node_selector_system_load_limit, 0.9);
    assert_eq!(config.node_selector_available_seconds, 8);
}

#[test]
fn from_lookup_rejects_regionaware_without_matching_region_config() {
    let env = HashMap::from([
        ("OXIDESFU_REGION", "eu-central".to_string()),
        ("OXIDESFU_NODE_SELECTOR_KIND", "regionaware".to_string()),
        (
            "OXIDESFU_NODE_SELECTOR_REGIONS_JSON",
            r#"[{"name":"us-east","lat":40.7,"lon":-74.0}]"#.to_string(),
        ),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("regionaware selector should require local region coordinates");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_rejects_invalid_region_coordinates() {
    let env = HashMap::from([
        ("OXIDESFU_REGION", "eu-central".to_string()),
        ("OXIDESFU_NODE_SELECTOR_KIND", "regionaware".to_string()),
        (
            "OXIDESFU_NODE_SELECTOR_REGIONS_JSON",
            r#"[{"name":"eu-central","lat":120.0,"lon":8.6}]"#.to_string(),
        ),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("invalid coordinates should fail config validation");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_rejects_empty_node_selector_sort_by() {
    let env = HashMap::from([("OXIDESFU_NODE_SELECTOR_SORT_BY", "".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("empty selector sort-by should fail config validation");
    assert!(matches!(
        error,
        ConfigError::InvalidNodeSelectorSortBy {
            key: "OXIDESFU_NODE_SELECTOR_SORT_BY",
            ..
        }
    ));
}

#[test]
fn from_lookup_rejects_unknown_node_selector_sort_by() {
    let env = HashMap::from([("OXIDESFU_NODE_SELECTOR_SORT_BY", "mystery".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("unknown selector sort-by should fail config validation");
    assert!(matches!(
        error,
        ConfigError::InvalidNodeSelectorSortBy {
            key: "OXIDESFU_NODE_SELECTOR_SORT_BY",
            ..
        }
    ));
}

#[test]
fn from_lookup_rejects_empty_node_selector_algorithm() {
    let env = HashMap::from([("OXIDESFU_NODE_SELECTOR_ALGORITHM", "".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("empty selector algorithm should fail config validation");
    assert!(matches!(
        error,
        ConfigError::InvalidNodeSelectorAlgorithm {
            key: "OXIDESFU_NODE_SELECTOR_ALGORITHM",
            ..
        }
    ));
}

#[test]
fn from_lookup_rejects_unknown_node_selector_algorithm() {
    let env = HashMap::from([("OXIDESFU_NODE_SELECTOR_ALGORITHM", "mystery".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("unknown selector algorithm should fail config validation");
    assert!(matches!(
        error,
        ConfigError::InvalidNodeSelectorAlgorithm {
            key: "OXIDESFU_NODE_SELECTOR_ALGORITHM",
            ..
        }
    ));
}

#[test]
fn from_lookup_rejects_non_positive_selector_available_seconds() {
    let env = HashMap::from([("OXIDESFU_NODE_SELECTOR_AVAILABLE_SECONDS", "0".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("non-positive selector available seconds should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn apply_args_accepts_selector_tuning_options() {
    let config = ServerConfig::apply_args(
        ServerConfig::development(),
        vec![
            ARG_NODE_SELECTOR_SORT_BY.to_string(),
            "bytespersec".to_string(),
            ARG_NODE_SELECTOR_ALGORITHM.to_string(),
            "lowest".to_string(),
            ARG_NODE_SELECTOR_CPU_LOAD_LIMIT.to_string(),
            "0.55".to_string(),
            ARG_NODE_SELECTOR_SYSTEM_LOAD_LIMIT.to_string(),
            "0.75".to_string(),
            ARG_NODE_SELECTOR_AVAILABLE_SECONDS.to_string(),
            "9".to_string(),
        ],
    )
    .expect("selector tuning args should parse");

    assert_eq!(
        config.node_selector_sort_by,
        NodeSelectorSortBy::BytesPerSec
    );
    assert_eq!(
        config.node_selector_algorithm,
        NodeSelectorAlgorithm::Lowest
    );
    assert_eq!(config.node_selector_cpu_load_limit, 0.55);
    assert_eq!(config.node_selector_system_load_limit, 0.75);
    assert_eq!(config.node_selector_available_seconds, 9);
}

#[test]
fn from_lookup_reads_api_credentials_from_secret_files_when_env_values_absent() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    let key_path = std::env::temp_dir().join(format!("oxidesfu-api-key-{now}.txt"));
    let secret_path = std::env::temp_dir().join(format!("oxidesfu-api-secret-{now}.txt"));

    std::fs::write(&key_path, "file-key\n").expect("key file should write");
    std::fs::write(&secret_path, "file-secret\r\n").expect("secret file should write");

    let env = HashMap::from([
        (
            "OXIDESFU_API_KEY_FILE",
            key_path.to_string_lossy().to_string(),
        ),
        (
            "OXIDESFU_API_SECRET_FILE",
            secret_path.to_string_lossy().to_string(),
        ),
    ]);

    let config = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("config should read secrets from files");

    assert_eq!(config.api_key, "file-key");
    assert_eq!(config.api_secret, "file-secret");

    let _ = std::fs::remove_file(key_path);
    let _ = std::fs::remove_file(secret_path);
}

#[test]
fn from_lookup_reports_read_secret_file_error_for_missing_secret_path() {
    let env = HashMap::from([(
        "OXIDESFU_API_KEY_FILE",
        "/tmp/definitely-missing-oxidesfu-api-key.txt".to_string(),
    )]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("missing api key file should fail");

    assert!(matches!(
        error,
        ConfigError::ReadSecretFile {
            key: "OXIDESFU_API_KEY_FILE",
            ..
        }
    ));
}

#[test]
fn from_lookup_reports_webhook_urls_without_webhook_api_key() {
    let env = HashMap::from([(
        "OXIDESFU_WEBHOOK_URLS",
        "https://hooks.example.test/events".to_string(),
    )]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("webhook urls without webhook api key should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_webhook_api_key_not_in_local_key_store() {
    let env = HashMap::from([
        ("OXIDESFU_API_KEY", "node-key".to_string()),
        ("OXIDESFU_API_SECRET", "node-secret".to_string()),
        ("OXIDESFU_WEBHOOK_API_KEY", "other-key".to_string()),
        (
            "OXIDESFU_WEBHOOK_URLS",
            "https://hooks.example.test/events".to_string(),
        ),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("webhook api key outside local key store should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_invalid_webhook_url() {
    let env = HashMap::from([
        ("OXIDESFU_WEBHOOK_API_KEY", "devkey".to_string()),
        ("OXIDESFU_WEBHOOK_URLS", "not-a-url".to_string()),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("invalid webhook url should fail");
    assert!(matches!(
        error,
        ConfigError::InvalidWebhookUrl {
            key: "OXIDESFU_WEBHOOK_URLS",
            ..
        }
    ));
}

#[test]
fn from_lookup_reports_invalid_millis_override() {
    let env = HashMap::from([("OXIDESFU_ROOM_CLEANUP_INTERVAL_MS", "oops".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("invalid millis should fail");
    assert!(matches!(
        error,
        ConfigError::InvalidMillis {
            key: "OXIDESFU_ROOM_CLEANUP_INTERVAL_MS",
            ..
        }
    ));
}

#[test]
fn from_lookup_accepts_external_ip_discovery_without_node_ip() {
    let env = HashMap::from([("OXIDESFU_RTC_USE_EXTERNAL_IP", "true".to_string())]);

    let config = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("runtime STUN discovery should resolve a missing node IP");
    assert!(config.rtc_use_external_ip);
    assert_eq!(config.rtc_node_ip, None);
}

#[test]
fn from_lookup_accepts_node_ip_when_use_external_ip_enabled() {
    let env = HashMap::from([
        ("OXIDESFU_RTC_USE_EXTERNAL_IP", "true".to_string()),
        ("OXIDESFU_RTC_NODE_IP", "198.51.100.22".to_string()),
    ]);

    let config = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("external ip mode with node ip should parse");
    assert!(config.rtc_use_external_ip);
    assert_eq!(config.rtc_node_ip.as_deref(), Some("198.51.100.22"));
}

#[test]
fn apply_args_accepts_external_ip_discovery_without_node_ip() {
    let config = ServerConfig::apply_args(
        ServerConfig::development(),
        vec![ARG_RTC_USE_EXTERNAL_IP.to_string(), "true".to_string()],
    )
    .expect("runtime STUN discovery should resolve a missing node IP");
    assert!(config.rtc_use_external_ip);
    assert_eq!(config.rtc_node_ip, None);
}

#[test]
fn from_lookup_reports_turn_ports_without_turn_domain() {
    let env = HashMap::from([("OXIDESFU_TURN_UDP_PORT", "3478".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("turn udp port without turn domain should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_turn_domain_with_http_scheme() {
    let env = HashMap::from([("OXIDESFU_TURN_DOMAIN", "https://host.com".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("turn domain with http scheme should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_turn_domain_with_turn_scheme() {
    let env = HashMap::from([("OXIDESFU_TURN_DOMAIN", "turn://host.com".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("turn domain with turn scheme should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_accepts_bare_turn_domain() {
    let env = HashMap::from([("OXIDESFU_TURN_DOMAIN", "turn.google.com".to_string())]);

    let config = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("bare turn domain should be accepted");
    assert_eq!(config.turn_domain.as_deref(), Some("turn.google.com"));
}

#[test]
fn from_lookup_reports_turn_reachability_probe_without_domain() {
    let env = HashMap::from([("OXIDESFU_TURN_REQUIRE_REACHABLE", "true".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("turn reachability probing without domain should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_turn_reachability_probe_without_ports() {
    let env = HashMap::from([
        ("OXIDESFU_TURN_REQUIRE_REACHABLE", "true".to_string()),
        ("OXIDESFU_TURN_DOMAIN", "turn.example.net".to_string()),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("turn reachability probing without ports should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_zero_turn_probe_timeout_when_probe_enabled() {
    let env = HashMap::from([
        ("OXIDESFU_TURN_REQUIRE_REACHABLE", "true".to_string()),
        ("OXIDESFU_TURN_DOMAIN", "turn.example.net".to_string()),
        ("OXIDESFU_TURN_UDP_PORT", "3478".to_string()),
        ("OXIDESFU_TURN_PROBE_TIMEOUT_MS", "0".to_string()),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("zero turn probe timeout should fail when probing is enabled");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_invalid_rtc_udp_port() {
    let env = HashMap::from([("OXIDESFU_RTC_UDP_PORT", "oops".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("invalid rtc udp port should fail");
    assert!(matches!(
        error,
        ConfigError::InvalidInteger {
            key: "OXIDESFU_RTC_UDP_PORT",
            ..
        }
    ));
}

#[test]
fn from_lookup_applies_rtc_udp_port_range_overrides() {
    let env = HashMap::from([
        ("OXIDESFU_RTC_UDP_PORT_RANGE_START", "50000".to_string()),
        ("OXIDESFU_RTC_UDP_PORT_RANGE_END", "50002".to_string()),
    ]);

    let config = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("rtc udp range env overrides should parse");

    assert_eq!(config.rtc_udp_port, None);
    assert_eq!(config.rtc_udp_port_range_start, Some(50000));
    assert_eq!(config.rtc_udp_port_range_end, Some(50002));
}

#[test]
fn from_lookup_reports_missing_rtc_udp_port_range_end() {
    let env = HashMap::from([("OXIDESFU_RTC_UDP_PORT_RANGE_START", "50000".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("missing range end should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_invalid_rtc_udp_port_range_order() {
    let env = HashMap::from([
        ("OXIDESFU_RTC_UDP_PORT_RANGE_START", "50003".to_string()),
        ("OXIDESFU_RTC_UDP_PORT_RANGE_END", "50000".to_string()),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("start greater than end should fail");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_rtc_udp_port_conflict_with_range() {
    let env = HashMap::from([
        ("OXIDESFU_RTC_UDP_PORT", "50000".to_string()),
        ("OXIDESFU_RTC_UDP_PORT_RANGE_START", "50010".to_string()),
        ("OXIDESFU_RTC_UDP_PORT_RANGE_END", "50020".to_string()),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("rtc udp port + range should conflict");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_reports_invalid_rtc_tcp_port() {
    let env = HashMap::from([("OXIDESFU_RTC_TCP_PORT", "oops".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("invalid rtc tcp port should fail");
    assert!(matches!(
        error,
        ConfigError::InvalidInteger {
            key: "OXIDESFU_RTC_TCP_PORT",
            ..
        }
    ));
}

#[test]
fn from_lookup_reports_invalid_ice_servers_json() {
    let env = HashMap::from([("OXIDESFU_ICE_SERVERS_JSON", "not-json".to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("invalid ice servers json should fail");
    assert!(matches!(
        error,
        ConfigError::InvalidIceServersJson {
            key: "OXIDESFU_ICE_SERVERS_JSON",
            ..
        }
    ));
}

#[test]
fn from_lookup_reports_ice_server_without_urls() {
    let env = HashMap::from([("OXIDESFU_ICE_SERVERS_JSON", r#"[{"urls":[]}]"#.to_string())]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("ice server without urls should fail");
    assert!(matches!(
        error,
        ConfigError::InvalidIceServer {
            key: "OXIDESFU_ICE_SERVERS_JSON",
            ..
        }
    ));
}

#[test]
fn apply_args_bind_zero_zero_zero_zero_enables_lan_access() {
    let config = ServerConfig::apply_args(
        ServerConfig::development(),
        vec![ARG_BIND.to_string(), "0.0.0.0:7880".to_string()],
    )
    .expect("bind override should parse");

    assert_eq!(
        config.bind,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7880)
    );
}

#[test]
fn apply_args_overrides_loaded_env_values() {
    let env = HashMap::from([
        ("OXIDESFU_BIND", "127.0.0.1:9999".to_string()),
        ("OXIDESFU_API_KEY", "env-key".to_string()),
        ("OXIDESFU_API_SECRET", "env-secret".to_string()),
    ]);
    let base = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("base env config should parse");

    let config = ServerConfig::apply_args(
        base,
        vec![
            ARG_BIND.to_string(),
            "127.0.0.1:7777".to_string(),
            ARG_API_KEY.to_string(),
            "cli-key".to_string(),
            ARG_ROOM_CLEANUP_INTERVAL_MS.to_string(),
            "1200".to_string(),
            ARG_EMPTY_ROOM_MAX_AGE_MS.to_string(),
            "4200".to_string(),
            ARG_ROOM_NODE_DIRECTORY_BACKEND.to_string(),
            "redis".to_string(),
            ARG_REDIS_URL.to_string(),
            "redis://127.0.0.1:6380/1".to_string(),
            ARG_REJECT_NON_LOCAL_ROOM_PLACEMENT.to_string(),
            "yes".to_string(),
            ARG_ICE_SERVERS_JSON.to_string(),
            r#"[{"urls":["stun:stun.example.org:3478"]}]"#.to_string(),
            ARG_RTC_UDP_PORT.to_string(),
            "50100".to_string(),
            ARG_RTC_TCP_PORT.to_string(),
            "7998".to_string(),
            ARG_RTC_ALLOW_TCP_FALLBACK.to_string(),
            "no".to_string(),
            ARG_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS.to_string(),
            "333".to_string(),
            ARG_RTC_ALLOW_UDP_UNSTABLE_FALLBACK.to_string(),
            "on".to_string(),
            ARG_RTC_USE_EXTERNAL_IP.to_string(),
            "true".to_string(),
            ARG_RTC_NODE_IP.to_string(),
            "198.51.100.44".to_string(),
            ARG_TURN_DOMAIN.to_string(),
            "turn.cli.example".to_string(),
            ARG_TURN_UDP_PORT.to_string(),
            "3478".to_string(),
            ARG_TURN_TLS_PORT.to_string(),
            "5349".to_string(),
            ARG_TURN_USERNAME.to_string(),
            "turn-cli-user".to_string(),
            ARG_TURN_CREDENTIAL.to_string(),
            "turn-cli-pass".to_string(),
            ARG_TURN_REQUIRE_REACHABLE.to_string(),
            "true".to_string(),
            ARG_TURN_PROBE_TIMEOUT_MS.to_string(),
            "3200".to_string(),
        ],
    )
    .expect("cli overrides should parse");

    assert_eq!(
        config.bind,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7777)
    );
    assert_eq!(config.api_key, "cli-key");
    assert_eq!(config.api_secret, "env-secret");
    assert_eq!(config.room_cleanup_interval, Duration::from_millis(1200));
    assert_eq!(config.empty_room_max_age, Duration::from_millis(4200));
    assert_eq!(
        config.room_node_directory_backend,
        RoomNodeDirectoryBackend::Redis
    );
    assert_eq!(
        config.redis_url.as_deref(),
        Some("redis://127.0.0.1:6380/1")
    );
    assert!(config.reject_non_local_room_placement);
    assert_eq!(config.ice_servers.len(), 1);
    assert_eq!(
        config.ice_servers[0].urls,
        vec!["stun:stun.example.org:3478"]
    );
    assert_eq!(config.rtc_udp_port, Some(50100));
    assert_eq!(config.rtc_udp_port_range_start, None);
    assert_eq!(config.rtc_udp_port_range_end, None);
    assert_eq!(config.rtc_tcp_port, 7998);
    assert!(!config.rtc_allow_tcp_fallback);
    assert_eq!(config.rtc_tcp_fallback_rtt_threshold_ms, 333);
    assert!(config.rtc_allow_udp_unstable_fallback);
    assert!(config.rtc_use_external_ip);
    assert_eq!(config.rtc_node_ip.as_deref(), Some("198.51.100.44"));
    assert_eq!(config.turn_domain.as_deref(), Some("turn.cli.example"));
    assert_eq!(config.turn_udp_port, Some(3478));
    assert_eq!(config.turn_tls_port, Some(5349));
    assert_eq!(config.turn_username.as_deref(), Some("turn-cli-user"));
    assert_eq!(config.turn_credential.as_deref(), Some("turn-cli-pass"));
    assert!(config.turn_require_reachable);
    assert_eq!(config.turn_probe_timeout_ms, 3200);
}

#[test]
fn apply_args_accepts_rtc_udp_port_range_overrides() {
    let config = ServerConfig::apply_args(
        ServerConfig::development(),
        vec![
            ARG_RTC_UDP_PORT_RANGE_START.to_string(),
            "51000".to_string(),
            ARG_RTC_UDP_PORT_RANGE_END.to_string(),
            "51002".to_string(),
        ],
    )
    .expect("rtc udp range args should parse");

    assert_eq!(config.rtc_udp_port, None);
    assert_eq!(config.rtc_udp_port_range_start, Some(51000));
    assert_eq!(config.rtc_udp_port_range_end, Some(51002));
}

#[test]
fn apply_args_rejects_missing_value_unknown_flag_and_invalid_bool() {
    let base = ServerConfig::development();

    let missing = ServerConfig::apply_args(base.clone(), vec![ARG_BIND.to_string()])
        .expect_err("missing argument value should error");
    assert!(matches!(
        missing,
        ConfigError::MissingArgumentValue { arg: ARG_BIND }
    ));

    let unknown = ServerConfig::apply_args(base.clone(), vec!["--bogus".to_string()])
        .expect_err("unknown argument should error");
    assert!(matches!(unknown, ConfigError::UnknownArgument { .. }));

    let invalid_bool = ServerConfig::apply_args(
        base,
        vec![
            ARG_REJECT_NON_LOCAL_ROOM_PLACEMENT.to_string(),
            "maybe".to_string(),
        ],
    )
    .expect_err("invalid bool-like value should error");
    assert!(matches!(
        invalid_bool,
        ConfigError::InvalidBoolean {
            key: "OXIDESFU_REJECT_NON_LOCAL_ROOM_PLACEMENT",
            ..
        }
    ));
}

#[test]
fn resolve_config_text_matches_upstream_get_config_string_precedence() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("oxidesfu-get-config-{now}.env"));
    std::fs::write(&path, "fileContent").expect("temp config file should write");

    let cases = vec![
        (String::new(), String::new(), "".to_string()),
        (
            String::new(),
            "configBody".to_string(),
            "configBody".to_string(),
        ),
        (
            path.to_string_lossy().to_string(),
            "configBody".to_string(),
            "configBody".to_string(),
        ),
        (
            path.to_string_lossy().to_string(),
            String::new(),
            "fileContent".to_string(),
        ),
    ];

    for (config_file, config_body, expected) in cases {
        let resolved = resolve_config_text(&config_file, &config_body)
            .expect("config text should resolve for precedence case");
        assert_eq!(resolved, expected);
    }

    let _ = std::fs::remove_file(path);
}

#[test]
fn resolve_config_text_reports_error_when_file_missing_and_inline_empty() {
    let missing_path = std::env::temp_dir()
        .join("oxidesfu-missing-get-config-string.env")
        .to_string_lossy()
        .to_string();

    let error = resolve_config_text(&missing_path, "")
        .expect_err("missing file without inline config body should fail");
    assert!(matches!(error, ConfigError::ReadConfigFile { .. }));
}

#[test]
fn apply_config_file_content_applies_supported_keys() {
    let mut config = ServerConfig::development();
    config
            .apply_config_file_content(
                "\n# comment\nOXIDESFU_BIND=127.0.0.1:9090\nOXIDESFU_API_KEY=file-key\nOXIDESFU_API_SECRET=file-secret\nOXIDESFU_ROOM_CLEANUP_INTERVAL_MS=3333\nOXIDESFU_EMPTY_ROOM_MAX_AGE_MS=7777\nOXIDESFU_REJECT_NON_LOCAL_ROOM_PLACEMENT=1\n",
            )
            .expect("config content should parse");

    assert_eq!(
        config.bind,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9090)
    );
    assert_eq!(config.api_key, "file-key");
    assert_eq!(config.api_secret, "file-secret");
    assert_eq!(config.room_cleanup_interval, Duration::from_millis(3333));
    assert_eq!(config.empty_room_max_age, Duration::from_millis(7777));
    assert!(config.reject_non_local_room_placement);
}

#[test]
fn apply_config_file_content_keeps_unspecified_defaults() {
    let mut config = ServerConfig::development();
    config
        .apply_config_file_content("OXIDESFU_EMPTY_ROOM_MAX_AGE_MS=10000\n")
        .expect("partial config content should parse");

    assert_eq!(config.empty_room_max_age, Duration::from_millis(10000));
    assert_eq!(config.room_cleanup_interval, Duration::from_secs(30));
    assert_eq!(config.api_key, "devkey");
    assert_eq!(config.api_secret, "secret");
    assert_eq!(
        config.room_node_directory_backend,
        RoomNodeDirectoryBackend::Memory
    );
}

#[test]
fn apply_config_file_content_reports_unknown_key() {
    let error = ServerConfig::development()
        .apply_config_file_content("OXIDESFU_UNKNOWN=10\n")
        .expect_err("unknown config key should fail");

    assert!(matches!(error, ConfigError::UnknownArgument { .. }));
}

#[test]
fn apply_config_file_via_arg_then_cli_values_override_file() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("oxidesfu-config-{now}.env"));
    std::fs::write(
            &path,
            "OXIDESFU_BIND=127.0.0.1:9091\nOXIDESFU_API_KEY=file-key\nOXIDESFU_ROOM_NODE_DIRECTORY_BACKEND=redis\n",
        )
        .expect("temp config file should write");

    let config = ServerConfig::apply_args(
        ServerConfig::development(),
        vec![
            ARG_CONFIG.to_string(),
            path.to_string_lossy().to_string(),
            ARG_BIND.to_string(),
            "127.0.0.1:9092".to_string(),
        ],
    )
    .expect("config should parse with file + cli overrides");

    assert_eq!(
        config.bind,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092)
    );
    assert_eq!(config.api_key, "file-key");
    assert_eq!(
        config.room_node_directory_backend,
        RoomNodeDirectoryBackend::Redis
    );

    let _ = std::fs::remove_file(path);
}

#[test]
fn from_lookup_parses_owned_turn_runtime_configuration() {
    let env = HashMap::from([
        ("OXIDESFU_TURN_ENABLED", "true".to_string()),
        ("OXIDESFU_TURN_DOMAIN", "turn.example.net".to_string()),
        ("OXIDESFU_TURN_BIND", "127.0.0.1".to_string()),
        ("OXIDESFU_TURN_EXTERNAL_IP", "203.0.113.10".to_string()),
        ("OXIDESFU_TURN_UDP_PORT", "3478".to_string()),
        ("OXIDESFU_TURN_TLS_PORT", "443".to_string()),
        ("OXIDESFU_TURN_TLS_BIND", "0.0.0.0:5349".to_string()),
        (
            "OXIDESFU_TURN_TLS_CERT_FILE",
            "/run/oxidesfu-secrets/turn-cert.pem".to_string(),
        ),
        (
            "OXIDESFU_TURN_TLS_KEY_FILE",
            "/run/oxidesfu-secrets/turn-key.pem".to_string(),
        ),
        ("OXIDESFU_TURN_RELAY_PORT_RANGE_START", "40000".to_string()),
        ("OXIDESFU_TURN_RELAY_PORT_RANGE_END", "40010".to_string()),
        ("OXIDESFU_TURN_CREDENTIAL_TTL_SECONDS", "7200".to_string()),
        (
            "OXIDESFU_TURN_ALLOW_RESTRICTED_PEER_CIDRS",
            "127.0.0.0/8,::1/128".to_string(),
        ),
        ("OXIDESFU_TURN_DENY_PEER_CIDRS", "10.0.0.0/8".to_string()),
    ]);

    let config = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("owned TURN runtime configuration should parse");

    assert!(config.turn_enabled);
    assert_eq!(config.turn_bind, "127.0.0.1");
    assert_eq!(config.turn_external_ip.as_deref(), Some("203.0.113.10"));
    assert_eq!(config.turn_udp_port, Some(3478));
    assert_eq!(config.turn_tls_port, Some(443));
    assert_eq!(
        config.turn_tls_bind,
        Some("0.0.0.0:5349".parse().expect("test address should parse"))
    );
    assert_eq!(
        config.turn_tls_cert_file.as_deref(),
        Some("/run/oxidesfu-secrets/turn-cert.pem")
    );
    assert_eq!(
        config.turn_tls_key_file.as_deref(),
        Some("/run/oxidesfu-secrets/turn-key.pem")
    );
    assert_eq!(config.turn_relay_port_range_start, Some(40000));
    assert_eq!(config.turn_relay_port_range_end, Some(40010));
    assert_eq!(config.turn_credential_ttl_seconds, 7200);
    assert_eq!(
        config.turn_allow_restricted_peer_cidrs,
        ["127.0.0.0/8", "::1/128"]
    );
    assert_eq!(config.turn_deny_peer_cidrs, ["10.0.0.0/8"]);
}

#[test]
fn from_lookup_accepts_an_ip_turn_domain_for_owned_local_runtime() {
    let env = HashMap::from([
        ("OXIDESFU_TURN_ENABLED", "true".to_string()),
        ("OXIDESFU_TURN_DOMAIN", "127.0.0.1".to_string()),
        ("OXIDESFU_TURN_UDP_PORT", "3478".to_string()),
    ]);

    let config = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect("an owned local TURN endpoint should accept an IP URL host");
    assert_eq!(config.turn_domain.as_deref(), Some("127.0.0.1"));
}

#[test]
fn from_lookup_rejects_incomplete_owned_tls_turn_configuration() {
    let env = HashMap::from([
        ("OXIDESFU_TURN_ENABLED", "true".to_string()),
        ("OXIDESFU_TURN_DOMAIN", "turn.example.net".to_string()),
        ("OXIDESFU_TURN_UDP_PORT", "3479".to_string()),
        ("OXIDESFU_TURN_TLS_PORT", "443".to_string()),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("owned TLS TURN needs listener and certificate material");

    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn from_lookup_rejects_enabled_turn_without_udp_port() {
    let env = HashMap::from([
        ("OXIDESFU_TURN_ENABLED", "true".to_string()),
        ("OXIDESFU_TURN_DOMAIN", "turn.example.net".to_string()),
    ]);

    let error = ServerConfig::from_lookup(|key| env.get(key).cloned())
        .expect_err("owned TURN needs a UDP listener port");
    assert!(matches!(error, ConfigError::InvalidTransportConfig { .. }));
}

#[test]
fn apply_args_reports_error_when_config_file_does_not_exist() {
    let missing_path = std::env::temp_dir()
        .join("oxidesfu-missing-config-file.env")
        .to_string_lossy()
        .to_string();

    let error = ServerConfig::apply_args(
        ServerConfig::development(),
        vec![ARG_CONFIG.to_string(), missing_path],
    )
    .expect_err("missing config file should fail to load");

    assert!(matches!(error, ConfigError::ReadConfigFile { .. }));
}

#[test]
fn apply_args_reports_invalid_room_node_directory_backend() {
    let error = ServerConfig::apply_args(
        ServerConfig::development(),
        vec![
            ARG_ROOM_NODE_DIRECTORY_BACKEND.to_string(),
            "bogus".to_string(),
        ],
    )
    .expect_err("invalid backend should fail");

    assert!(matches!(
        error,
        ConfigError::InvalidRoomNodeDirectoryBackend { .. }
    ));
}

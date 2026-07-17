use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

use ipnet::IpNet;
use oxidesfu_core::ServerConfig;
use turn_server::{
    ServerHandle,
    codec::{
        crypto::{Password, generate_password},
        message::{attributes::PasswordAlgorithm, methods::Method},
    },
    config::{Config, Interface, Server},
    service::{
        ServiceHandler,
        session::{Identifier, ports::PortRange},
    },
};

use crate::turn_auth::{LIVEKIT_REALM, TurnAuthHandler, TurnMethod, TurnRequestAttributes};

#[derive(Clone)]
struct PeerPolicy {
    allow_restricted: Vec<IpNet>,
    deny: Vec<IpNet>,
}

impl PeerPolicy {
    fn new(allow_restricted: &[String], deny: &[String]) -> Result<Self, io::Error> {
        let parse = |cidrs: &[String], label: &str| -> Result<Vec<IpNet>, io::Error> {
            cidrs
                .iter()
                .map(|cidr| {
                    IpNet::from_str(cidr).map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid {label} CIDR {cidr}: {error}"),
                        )
                    })
                })
                .collect()
        };

        Ok(Self {
            allow_restricted: parse(allow_restricted, "TURN allow-restricted-peer")?,
            deny: parse(deny, "TURN deny-peer")?,
        })
    }

    fn allows(&self, peer: IpAddr) -> bool {
        if is_restricted(peer)
            && !self
                .allow_restricted
                .iter()
                .any(|cidr| cidr.contains(&peer))
        {
            return false;
        }

        !self.deny.iter().any(|cidr| cidr.contains(&peer))
    }
}

fn is_restricted(peer: IpAddr) -> bool {
    match peer {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_private()
                || ip.is_unspecified()
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unicast_link_local()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unspecified()
        }
    }
}

#[derive(Clone)]
struct OxideTurnHandler {
    auth: TurnAuthHandler,
    peer_policy: PeerPolicy,
}

impl OxideTurnHandler {
    fn auth_method(method: Method) -> TurnMethod {
        match method {
            Method::Allocate(_) => TurnMethod::Allocate,
            Method::Refresh(_) => TurnMethod::Refresh,
            Method::CreatePermission(_) => TurnMethod::CreatePermission,
            Method::ChannelBind(_) => TurnMethod::ChannelBind,
            Method::SendIndication => TurnMethod::Send,
            Method::Binding(_) | Method::DataIndication => TurnMethod::Send,
        }
    }

    fn password(&self, username: &str, algorithm: PasswordAlgorithm) -> Option<Password> {
        let parsed = self.auth.parse_username(username).ok()?;
        let password = self
            .auth
            .password_for(
                &parsed.api_key,
                &parsed.participant_id,
                parsed.expiry_unix_seconds,
            )
            .ok()?;
        Some(generate_password(
            username,
            &password,
            LIVEKIT_REALM,
            algorithm,
        ))
    }
}

impl ServiceHandler for OxideTurnHandler {
    async fn get_password(
        &self,
        _id: &Identifier,
        username: &str,
        algorithm: PasswordAlgorithm,
    ) -> Option<Password> {
        self.password(username, algorithm)
    }

    fn allows_auth(&self, _id: &Identifier, username: &str, method: Method) -> bool {
        self.auth
            .handle_auth_at(
                TurnRequestAttributes {
                    username,
                    method: Self::auth_method(method),
                },
                TurnAuthHandler::now_unix_seconds(),
            )
            .ok
    }

    fn allocate_auth_failure_is_bad_request(&self) -> bool {
        true
    }

    fn allows_peer(&self, _client: &Identifier, peer: SocketAddr) -> bool {
        self.peer_policy.allows(peer.ip())
    }
}

/// Owns OxideSFU's in-process TURN transport runtime.
pub struct TurnRuntime {
    handle: ServerHandle,
}

impl TurnRuntime {
    /// Stops TURN listeners and active allocation tasks.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.handle.shutdown().await
    }
}

fn turn_external_address(config: &ServerConfig, port: u16) -> anyhow::Result<SocketAddr> {
    Ok(SocketAddr::new(
        config
            .turn_external_ip
            .as_deref()
            .unwrap_or(config.turn_bind.as_str())
            .parse::<IpAddr>()?,
        port,
    ))
}

/// Starts the configured UDP TURN runtime, or returns `None` when TURN is disabled.
pub async fn start_turn_runtime(config: &ServerConfig) -> anyhow::Result<Option<TurnRuntime>> {
    if !config.turn_enabled {
        return Ok(None);
    }

    let bind_ip = config.turn_bind.parse::<IpAddr>()?;
    let port = config
        .turn_udp_port
        .ok_or_else(|| anyhow::anyhow!("enabled TURN requires a UDP port"))?;
    let listen = SocketAddr::new(bind_ip, port);
    let external = turn_external_address(config, port)?;
    let relay_port_range = match (
        config.turn_relay_port_range_start,
        config.turn_relay_port_range_end,
    ) {
        (Some(start), Some(end)) => PortRange::from_str(&format!("{start}..{end}"))?,
        (None, None) => PortRange::default(),
        _ => {
            return Err(anyhow::anyhow!(
                "TURN relay port range must have both bounds"
            ));
        }
    };
    let mut secrets = HashMap::from_iter(
        config
            .api_keys
            .iter()
            .map(|(key, secret)| (key.clone(), secret.clone())),
    );
    secrets.insert(config.api_key.clone(), config.api_secret.clone());

    // `turn-rs` starts its listener in a background task. Bind once here so a
    // conflicting UDP port fails OxideSFU startup instead of silently leaving
    // an advertised TURN endpoint unreachable.
    std::net::UdpSocket::bind(listen)?;

    let handler = OxideTurnHandler {
        auth: TurnAuthHandler::new(secrets),
        peer_policy: PeerPolicy::new(
            &config.turn_allow_restricted_peer_cidrs,
            &config.turn_deny_peer_cidrs,
        )?,
    };
    let engine_config = Config {
        server: Server {
            realm: LIVEKIT_REALM.to_string(),
            port_range: relay_port_range,
            interfaces: vec![Interface::Udp {
                listen,
                external,
                idle_timeout: 20,
                mtu: 1500,
            }],
            ..Server::default()
        },
        ..Config::default()
    };

    let handle = turn_server::spawn_server_with_handler(engine_config, handler).await?;
    Ok(Some(TurnRuntime { handle }))
}

#[cfg(test)]
mod tests {
    use super::{PeerPolicy, start_turn_runtime, turn_external_address};
    use oxidesfu_core::ServerConfig;

    #[test]
    fn peer_policy_requires_allow_listing_for_restricted_peers_and_denial_overrides_it() {
        let allowed =
            PeerPolicy::new(&["127.0.0.0/8".to_string()], &[]).expect("CIDRs should parse");
        assert!(allowed.allows("127.0.0.1".parse().expect("IP should parse")));

        let denied = PeerPolicy::new(&["127.0.0.0/8".to_string()], &["127.0.0.0/8".to_string()])
            .expect("CIDRs should parse");
        assert!(!denied.allows("127.0.0.1".parse().expect("IP should parse")));

        let default_policy = PeerPolicy::new(&[], &[]).expect("empty lists should parse");
        assert!(!default_policy.allows("127.0.0.1".parse().expect("IP should parse")));
        assert!(default_policy.allows("8.8.8.8".parse().expect("IP should parse")));
    }

    #[test]
    fn owned_turn_uses_configured_public_external_ip() {
        let mut config = ServerConfig::development();
        config.turn_bind = "0.0.0.0".to_string();
        config.turn_external_ip = Some("203.0.113.10".to_string());

        let external = turn_external_address(&config, 3479).expect("external address should parse");

        assert_eq!(
            external,
            "203.0.113.10:3479"
                .parse::<std::net::SocketAddr>()
                .expect("test address should parse")
        );
    }

    #[tokio::test]
    async fn runtime_shutdown_releases_udp_listener() {
        let reserved = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("test socket should bind");
        let port = reserved
            .local_addr()
            .expect("test socket should have an address")
            .port();
        drop(reserved);

        let mut config = ServerConfig::development();
        config.turn_enabled = true;
        config.turn_domain = Some("turn.example.net".to_string());
        config.turn_bind = "127.0.0.1".to_string();
        config.turn_udp_port = Some(port);

        let runtime = start_turn_runtime(&config)
            .await
            .expect("TURN runtime should start")
            .expect("enabled TURN should return a runtime");
        runtime.shutdown().await.expect("TURN runtime should stop");

        tokio::net::UdpSocket::bind(("127.0.0.1", port))
            .await
            .expect("TURN shutdown should release its UDP listener");
    }
}

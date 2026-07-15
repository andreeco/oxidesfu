use tracing_subscriber::prelude::*;

/// Loopback endpoint exposed to the local Tokio Console client by default.
#[cfg(any(feature = "tokio-console", test))]
pub const DEFAULT_TOKIO_CONSOLE_ADDR: &str = "127.0.0.1:6669";
#[cfg(feature = "tokio-console")]
const TOKIO_CONSOLE_ADDR_ENV: &str = "OXIDESFU_TOKIO_CONSOLE_ADDR";

#[cfg(any(feature = "tokio-console", test))]
pub(crate) fn tokio_console_addr_from_str(
    value: &str,
) -> Result<std::net::SocketAddr, std::net::AddrParseError> {
    value.parse()
}

#[cfg(feature = "tokio-console")]
fn tokio_console_addr_from_env() -> Result<std::net::SocketAddr, std::net::AddrParseError> {
    let value = std::env::var(TOKIO_CONSOLE_ADDR_ENV)
        .unwrap_or_else(|_| DEFAULT_TOKIO_CONSOLE_ADDR.to_owned());
    tokio_console_addr_from_str(&value)
}

/// Initializes tracing and, when enabled, the local Tokio Console task inspector.
pub fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| crate::DEFAULT_TRACING_ENV_FILTER.into());

    #[cfg(feature = "tokio-console")]
    {
        let console_addr = tokio_console_addr_from_env()?;
        let console_layer = console_subscriber::ConsoleLayer::builder()
            .server_addr(console_addr)
            .spawn();

        tracing_subscriber::registry()
            .with(env_filter)
            .with(console_layer)
            .with(tracing_subscriber::fmt::layer())
            .try_init()?;
        tracing::info!(%console_addr, "Tokio Console task inspector listening on loopback");
    }

    #[cfg(not(feature = "tokio-console"))]
    {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .try_init()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_TOKIO_CONSOLE_ADDR, tokio_console_addr_from_str};
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn default_tokio_console_address_is_loopback() {
        assert_eq!(DEFAULT_TOKIO_CONSOLE_ADDR, "127.0.0.1:6669");
    }

    #[test]
    fn tokio_console_address_accepts_a_socket_address() {
        assert_eq!(
            tokio_console_addr_from_str("127.0.0.1:7777").expect("address should parse"),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 7777))
        );
    }

    #[test]
    fn tokio_console_address_rejects_invalid_values() {
        assert!(tokio_console_addr_from_str("not-an-address").is_err());
    }
}

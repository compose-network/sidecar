//! Sidecar configuration via CLI arguments and environment variables.
//!
//! Uses `clap` with `#[arg(env = "...")]` for env-var mapping.

use clap::Parser;
use compose_primitives::ChainId;

/// Compose sidecar — cross-chain coordination layer.
#[derive(Debug, Clone, Parser)]
#[command(name = "sidecar")]
pub struct SidecarArgs {
    #[command(flatten)]
    pub server: ServerArgs,

    #[command(flatten)]
    pub publisher: PublisherArgs,

    #[command(flatten)]
    pub chain: ChainArgs,

    #[command(flatten)]
    pub peers: PeerArgs,

    #[command(flatten)]
    pub log: LogArgs,
}

/// HTTP server settings.
#[derive(Debug, Clone, clap::Args)]
pub struct ServerArgs {
    /// HTTP listen address.
    #[arg(
        long = "server.listen-addr",
        env = "SIDECAR_LISTEN_ADDR",
        default_value = "0.0.0.0:8080"
    )]
    pub listen_addr: String,

    /// HTTP read timeout in seconds.
    #[arg(
        long = "server.read-timeout-secs",
        env = "SIDECAR_READ_TIMEOUT_SECS",
        default_value = "30"
    )]
    pub read_timeout_secs: u64,

    /// HTTP write timeout in seconds.
    #[arg(
        long = "server.write-timeout-secs",
        env = "SIDECAR_WRITE_TIMEOUT_SECS",
        default_value = "30"
    )]
    pub write_timeout_secs: u64,
}

/// Publisher (SP) connection settings.
#[derive(Debug, Clone, clap::Args)]
pub struct PublisherArgs {
    /// Enable publisher connection.
    #[arg(
        long = "publisher.enabled",
        env = "SIDECAR_PUBLISHER_ENABLED",
        default_value = "false",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    pub enabled: bool,

    /// Publisher QUIC address.
    #[arg(
        long = "publisher.addr",
        env = "SIDECAR_PUBLISHER_ADDR",
        default_value = ""
    )]
    pub addr: String,

    /// Reconnect delay in seconds.
    #[arg(
        long = "publisher.reconnect-delay-secs",
        env = "SIDECAR_PUBLISHER_RECONNECT_DELAY_SECS",
        default_value = "5"
    )]
    pub reconnect_delay_secs: u64,

    /// Maximum reconnection attempts.
    #[arg(
        long = "publisher.max-retries",
        env = "SIDECAR_PUBLISHER_MAX_RETRIES",
        default_value = "10"
    )]
    pub max_retries: u32,
}

/// Single-chain configuration (one chain per sidecar container).
#[derive(Debug, Clone, clap::Args)]
pub struct ChainArgs {
    /// Chain ID this sidecar manages.
    #[arg(long = "chain.id", env = "SIDECAR_CHAIN_ID", default_value = "0")]
    pub id: u64,

    /// Chain name.
    #[arg(long = "chain.name", env = "SIDECAR_CHAIN_NAME", default_value = "")]
    pub name: String,

    /// Chain RPC endpoint.
    #[arg(long = "chain.rpc", env = "SIDECAR_CHAIN_RPC", default_value = "")]
    pub rpc: String,

    /// Mailbox contract address.
    #[arg(
        long = "chain.mailbox-address",
        env = "SIDECAR_MAILBOX_ADDRESS",
        default_value = ""
    )]
    pub mailbox_address: String,

    /// Private key for signing putInbox transactions.
    #[arg(
        long = "chain.coordinator-key",
        env = "SIDECAR_COORDINATOR_KEY",
        default_value = ""
    )]
    pub coordinator_key: String,
}

impl ChainArgs {
    /// Return the chain ID as a [`ChainId`].
    pub fn chain_id(&self) -> ChainId {
        ChainId(self.id)
    }
}

/// Peer sidecar addresses for CIRC message delivery.
/// Supports up to 4 named peer slots (A–D).
#[derive(Debug, Clone, clap::Args)]
pub struct PeerArgs {
    /// Peer A sidecar address.
    #[arg(long = "peer.a.addr", env = "SIDECAR_PEER_A_ADDR")]
    pub peer_a_addr: Option<String>,

    /// Peer A chain ID.
    #[arg(long = "peer.a.chain-id", env = "SIDECAR_PEER_A_CHAIN_ID")]
    pub peer_a_chain_id: Option<u64>,

    /// Peer B sidecar address.
    #[arg(long = "peer.b.addr", env = "SIDECAR_PEER_B_ADDR")]
    pub peer_b_addr: Option<String>,

    /// Peer B chain ID.
    #[arg(long = "peer.b.chain-id", env = "SIDECAR_PEER_B_CHAIN_ID")]
    pub peer_b_chain_id: Option<u64>,

    /// Peer C sidecar address.
    #[arg(long = "peer.c.addr", env = "SIDECAR_PEER_C_ADDR")]
    pub peer_c_addr: Option<String>,

    /// Peer C chain ID.
    #[arg(long = "peer.c.chain-id", env = "SIDECAR_PEER_C_CHAIN_ID")]
    pub peer_c_chain_id: Option<u64>,

    /// Peer D sidecar address.
    #[arg(long = "peer.d.addr", env = "SIDECAR_PEER_D_ADDR")]
    pub peer_d_addr: Option<String>,

    /// Peer D chain ID.
    #[arg(long = "peer.d.chain-id", env = "SIDECAR_PEER_D_CHAIN_ID")]
    pub peer_d_chain_id: Option<u64>,
}

/// A resolved peer entry.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub chain_id: ChainId,
    pub addr: String,
}

impl PeerArgs {
    /// Collect configured peers into a list, skipping slots without an address.
    /// Uses default chain IDs (901–904) when not explicitly set.
    pub fn to_entries(&self) -> Vec<PeerEntry> {
        let slots: [(Option<&str>, Option<u64>, u64); 4] = [
            (self.peer_a_addr.as_deref(), self.peer_a_chain_id, 901),
            (self.peer_b_addr.as_deref(), self.peer_b_chain_id, 902),
            (self.peer_c_addr.as_deref(), self.peer_c_chain_id, 903),
            (self.peer_d_addr.as_deref(), self.peer_d_chain_id, 904),
        ];

        slots
            .into_iter()
            .filter_map(|(addr, chain_id, default_id)| {
                let addr = addr?;
                if addr.is_empty() {
                    return None;
                }
                Some(PeerEntry {
                    chain_id: ChainId(chain_id.unwrap_or(default_id)),
                    addr: addr.to_string(),
                })
            })
            .collect()
    }
}

/// Logging settings.
#[derive(Debug, Clone, clap::Args)]
pub struct LogArgs {
    /// Log level: debug, info, warn, error.
    #[arg(
        long = "log.level",
        env = "SIDECAR_LOG_LEVEL",
        default_value = "info"
    )]
    pub level: String,

    /// Log format: json or pretty.
    #[arg(
        long = "log.format",
        env = "SIDECAR_LOG_FORMAT",
        default_value = "json"
    )]
    pub format: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_args_are_valid() {
        let args = SidecarArgs::parse_from(["sidecar"]);
        assert_eq!(args.server.listen_addr, "0.0.0.0:8080");
        assert!(!args.publisher.enabled);
        assert_eq!(args.chain.id, 0);
        assert_eq!(args.log.level, "info");
        assert_eq!(args.log.format, "json");
    }

    #[test]
    fn cli_args_override_defaults() {
        let args = SidecarArgs::parse_from([
            "sidecar",
            "--server.listen-addr",
            "0.0.0.0:9090",
            "--chain.id",
            "77777",
            "--chain.rpc",
            "http://localhost:8545",
            "--publisher.enabled",
            "true",
            "--publisher.addr",
            "publisher:8080",
            "--log.level",
            "debug",
        ]);
        assert_eq!(args.server.listen_addr, "0.0.0.0:9090");
        assert_eq!(args.chain.id, 77777);
        assert_eq!(args.chain.rpc, "http://localhost:8545");
        assert!(args.publisher.enabled);
        assert_eq!(args.publisher.addr, "publisher:8080");
        assert_eq!(args.log.level, "debug");
    }

    #[test]
    fn peer_entries_from_partial_config() {
        let args = SidecarArgs::parse_from([
            "sidecar",
            "--peer.a.addr",
            "http://sidecar-a:8090",
            "--peer.a.chain-id",
            "77777",
            "--peer.b.addr",
            "http://sidecar-b:8090",
        ]);
        let entries = args.peers.to_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].chain_id, ChainId(77777));
        assert_eq!(entries[0].addr, "http://sidecar-a:8090");
        assert_eq!(entries[1].chain_id, ChainId(902)); // default
        assert_eq!(entries[1].addr, "http://sidecar-b:8090");
    }

    #[test]
    fn no_peers_when_none_configured() {
        let args = SidecarArgs::parse_from(["sidecar"]);
        assert!(args.peers.to_entries().is_empty());
    }
}

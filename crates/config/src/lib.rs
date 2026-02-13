//! Sidecar configuration types and loading entrypoints.
//!
//! Use [`load`] to build [`SidecarConfig`] from YAML plus `SIDECAR_*`
//! environment-variable overrides.

mod loader;

pub use loader::load;

use compose_primitives::ChainId;
use serde::{Deserialize, Serialize};

/// Top-level sidecar configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SidecarConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub publisher: PublisherConfig,
    #[serde(default)]
    pub chains: ChainsConfig,
    #[serde(default)]
    pub peers: PeersConfig,
    #[serde(default)]
    pub log: LogConfig,
}

/// HTTP server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_timeout")]
    pub read_timeout_secs: u64,
    #[serde(default = "default_timeout")]
    pub write_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            read_timeout_secs: default_timeout(),
            write_timeout_secs: default_timeout(),
        }
    }
}

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_timeout() -> u64 {
    30
}

/// Publisher (SP) connection configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherConfig {
    #[serde(default)]
    pub addr: String,
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub enabled: bool,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            reconnect_delay_secs: default_reconnect_delay(),
            max_retries: default_max_retries(),
            enabled: false,
        }
    }
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_max_retries() -> u32 {
    10
}

/// Configuration for the chains this sidecar manages.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChainsConfig {
    #[serde(default)]
    pub list: Vec<ChainConfig>,
}

/// Individual chain configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub id: u64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub rpc: String,
    #[serde(default)]
    pub mailbox_address: String,
    #[serde(default)]
    pub coordinator_key: String,
}

impl ChainConfig {
    /// Return the chain ID as a [`ChainId`].
    pub fn chain_id(&self) -> ChainId {
        ChainId(self.id)
    }
}

/// Peer sidecar configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeersConfig {
    #[serde(default)]
    pub sidecars: Vec<PeerConfig>,
}

/// Individual peer sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub chain_id: u64,
    pub addr: String,
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "json".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = SidecarConfig::default();
        assert_eq!(cfg.server.listen_addr, "0.0.0.0:8080");
        assert!(!cfg.publisher.enabled);
        assert_eq!(cfg.log.level, "info");
    }

    #[test]
    fn deserialize_yaml() {
        let yaml = r#"
server:
  listen_addr: "0.0.0.0:9090"
publisher:
  addr: "publisher:8080"
  enabled: true
chains:
  list:
    - id: 901
      name: "chain-a"
      rpc: "http://localhost:8545"
log:
  level: debug
  format: pretty
"#;
        let cfg: SidecarConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.server.listen_addr, "0.0.0.0:9090");
        assert!(cfg.publisher.enabled);
        assert_eq!(cfg.chains.list.len(), 1);
        assert_eq!(cfg.chains.list[0].id, 901);
        assert_eq!(cfg.log.level, "debug");
    }
}

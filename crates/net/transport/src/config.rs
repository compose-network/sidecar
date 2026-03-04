//! QUIC transport client and server configuration.

use std::time::Duration;

/// Configuration for a QUIC transport client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Remote server address (host:port).
    pub addr: String,
    /// Client identifier sent during the QUIC identification handshake.
    /// The Go publisher uses this to register the sidecar by chain ID.
    pub client_id: String,
    /// Duration between reconnection attempts.
    pub reconnect_delay: Duration,
    /// Maximum number of reconnection attempts. 0 = unlimited.
    pub max_retries: u32,
    /// Maximum size of a single protobuf message in bytes.
    pub max_message_size: usize,
    /// Keep-alive ping interval.
    pub ping_interval: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            client_id: String::new(),
            reconnect_delay: Duration::from_secs(5),
            max_retries: 10,
            max_message_size: 4 * 1024 * 1024, // 4 MiB
            ping_interval: Duration::from_secs(15),
        }
    }
}

/// Configuration for a QUIC transport server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind (host:port).
    pub listen_addr: String,
    /// Maximum size of a single protobuf message in bytes.
    pub max_message_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8080".to_string(),
            max_message_size: 4 * 1024 * 1024,
        }
    }
}

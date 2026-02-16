//! QUIC client transport implementation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use quinn::Endpoint;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::ClientConfig;
use crate::error::TransportError;
use crate::framing::LengthPrefixCodec;
use crate::tls;
use crate::traits::Transport;

/// QUIC transport client that connects to the shared publisher or peer sidecars.
#[derive(Debug)]
pub struct QuicClient {
    config: ClientConfig,
    codec: LengthPrefixCodec,
    endpoint: Endpoint,
    connection: Mutex<Option<quinn::Connection>>,
    send_stream: Mutex<Option<quinn::SendStream>>,
    recv_stream: Mutex<Option<quinn::RecvStream>>,
    connected: AtomicBool,
}

impl QuicClient {
    /// Create a new QUIC client. Does not connect until [`connect`] is called.
    pub fn new(config: ClientConfig) -> Result<Arc<Self>, TransportError> {
        let tls_config = tls::insecure_client_config()?;
        let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let quinn_config = quinn::ClientConfig::new(std::sync::Arc::new(quic_config));

        let mut endpoint =
            Endpoint::client("0.0.0.0:0".parse().unwrap()).map_err(TransportError::Io)?;
        endpoint.set_default_client_config(quinn_config);

        let codec = LengthPrefixCodec::new(config.max_message_size);

        Ok(Arc::new(Self {
            config,
            codec,
            endpoint,
            connection: Mutex::new(None),
            send_stream: Mutex::new(None),
            recv_stream: Mutex::new(None),
            connected: AtomicBool::new(false),
        }))
    }

    /// Connect to the remote server.
    pub async fn connect(&self) -> Result<(), TransportError> {
        let mut resolved = tokio::net::lookup_host(&self.config.addr)
            .await
            .map_err(|e| TransportError::ConnectionRefused(e.to_string()))?;
        let addr = resolved.next().ok_or_else(|| {
            TransportError::ConnectionRefused(format!(
                "no resolved address for {}",
                self.config.addr
            ))
        })?;

        info!(addr = %self.config.addr, "Connecting to remote");

        let conn = self
            .endpoint
            .connect(addr, "localhost")
            .map_err(|e| TransportError::Quic(e.to_string()))?
            .await?;

        let (send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| TransportError::Quic(e.to_string()))?;

        *self.connection.lock().await = Some(conn);
        *self.send_stream.lock().await = Some(send);
        *self.recv_stream.lock().await = Some(recv);
        self.connected.store(true, Ordering::SeqCst);

        info!(addr = %self.config.addr, "Connected");
        Ok(())
    }

    /// Connect with automatic retries using the configured backoff.
    pub async fn connect_with_retry(&self) -> Result<(), TransportError> {
        let max = if self.config.max_retries == 0 {
            u32::MAX
        } else {
            self.config.max_retries
        };

        for attempt in 1..=max {
            match self.connect().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(
                        attempt,
                        max_retries = self.config.max_retries,
                        error = %e,
                        "Connection failed, retrying"
                    );
                    tokio::time::sleep(self.config.reconnect_delay).await;
                }
            }
        }

        Err(TransportError::ConnectionRefused(format!(
            "failed after {max} attempts",
        )))
    }
}

#[async_trait]
impl Transport for QuicClient {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        let frame = self.codec.encode(&data)?;
        let mut guard = self.send_stream.lock().await;
        let stream = guard.as_mut().ok_or(TransportError::ConnectionClosed)?;
        stream
            .write_all(&frame)
            .await
            .map_err(|e| TransportError::Quic(e.to_string()))?;
        Ok(())
    }

    async fn recv(&self) -> Result<Bytes, TransportError> {
        let mut guard = self.recv_stream.lock().await;
        let stream = guard.as_mut().ok_or(TransportError::ConnectionClosed)?;

        // Read 4-byte length header.
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await?;

        let len = self.codec.decode_length(&header)?;

        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        Ok(Bytes::from(payload))
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.connected.store(false, Ordering::SeqCst);
        if let Some(conn) = self.connection.lock().await.take() {
            conn.close(0u32.into(), b"client closing");
            debug!("Connection closed");
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

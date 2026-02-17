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

/// QUIC transport client.
///
/// Each send opens a new bidirectional stream and writes a single
/// length-prefixed frame. Incoming messages arrive on server-initiated
/// streams, one frame per stream.
#[derive(Debug)]
pub struct QuicClient {
    config: ClientConfig,
    codec: LengthPrefixCodec,
    endpoint: Endpoint,
    connection: Mutex<Option<quinn::Connection>>,
    connected: AtomicBool,
}

impl QuicClient {
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
            connected: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl Transport for QuicClient {
    async fn connect(&self) -> Result<(), TransportError> {
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

        let mut id_stream = conn
            .open_bi()
            .await
            .map_err(|e| TransportError::Quic(e.to_string()))?
            .0;

        let id_frame = self.codec.encode(self.config.client_id.as_bytes())?;
        id_stream
            .write_all(&id_frame)
            .await
            .map_err(|e| TransportError::Quic(e.to_string()))?;
        id_stream
            .finish()
            .map_err(|e| TransportError::Quic(e.to_string()))?;

        *self.connection.lock().await = Some(conn);
        self.connected.store(true, Ordering::SeqCst);

        info!(addr = %self.config.addr, client_id = %self.config.client_id, "Connected");
        Ok(())
    }

    async fn connect_with_retry(&self) -> Result<(), TransportError> {
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

    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        let guard = self.connection.lock().await;
        let conn = guard.as_ref().ok_or(TransportError::ConnectionClosed)?;

        let mut stream = conn
            .open_bi()
            .await
            .map_err(|e| TransportError::Quic(e.to_string()))?
            .0;

        let frame = self.codec.encode(&data)?;
        stream
            .write_all(&frame)
            .await
            .map_err(|e| TransportError::Quic(e.to_string()))?;
        stream
            .finish()
            .map_err(|e| TransportError::Quic(e.to_string()))?;

        Ok(())
    }

    async fn recv(&self) -> Result<Bytes, TransportError> {
        let conn = {
            let guard = self.connection.lock().await;
            guard.as_ref().ok_or(TransportError::ConnectionClosed)?.clone()
        };

        let (_, mut recv_stream) = conn
            .accept_bi()
            .await
            .map_err(|e| TransportError::Quic(e.to_string()))?;

        let mut header = [0u8; 4];
        recv_stream.read_exact(&mut header).await?;
        let len = self.codec.decode_length(&header)?;

        let mut payload = vec![0u8; len];
        recv_stream.read_exact(&mut payload).await?;

        debug!(len, "Received message");
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

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
    connect_lock: Mutex<()>,
    connected: AtomicBool,
}

impl QuicClient {
    pub fn new(config: ClientConfig) -> Result<Arc<Self>, TransportError> {
        let tls_config = tls::insecure_client_config()?;
        let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let mut quinn_config = quinn::ClientConfig::new(std::sync::Arc::new(quic_config));
        let mut transport_config = quinn::TransportConfig::default();
        let keep_alive = if config.ping_interval.is_zero() {
            None
        } else {
            Some(config.ping_interval)
        };
        transport_config.keep_alive_interval(keep_alive);
        quinn_config.transport_config(Arc::new(transport_config));

        let mut endpoint =
            Endpoint::client("0.0.0.0:0".parse().unwrap()).map_err(TransportError::Io)?;
        endpoint.set_default_client_config(quinn_config);

        let codec = LengthPrefixCodec::new(config.max_message_size);

        Ok(Arc::new(Self {
            config,
            codec,
            endpoint,
            connection: Mutex::new(None),
            connect_lock: Mutex::new(()),
            connected: AtomicBool::new(false),
        }))
    }

    async fn mark_disconnected(&self, expected_conn_id: Option<usize>) {
        let mut guard = self.connection.lock().await;
        let should_clear = match (expected_conn_id, guard.as_ref()) {
            (Some(id), Some(conn)) => conn.stable_id() == id,
            (Some(_), None) => false,
            (None, _) => true,
        };

        if !should_clear {
            return;
        }

        self.connected.store(false, Ordering::SeqCst);
        if let Some(conn) = guard.take() {
            conn.close(0u32.into(), b"connection lost");
        }
    }

    async fn current_connection(&self) -> Option<quinn::Connection> {
        self.connection.lock().await.as_ref().cloned()
    }

    async fn ensure_connected(&self) -> Result<quinn::Connection, TransportError> {
        if let Some(conn) = self.current_connection().await {
            return Ok(conn);
        }

        self.connect_with_retry().await?;

        self.current_connection()
            .await
            .ok_or(TransportError::ConnectionClosed)
    }

    async fn connect_once_locked(&self) -> Result<(), TransportError> {
        if self.connection.lock().await.is_some() {
            return Ok(());
        }

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

        let mut guard = self.connection.lock().await;
        if let Some(old) = guard.replace(conn) {
            old.close(0u32.into(), b"replaced connection");
        }
        self.connected.store(true, Ordering::SeqCst);

        info!(addr = %self.config.addr, client_id = %self.config.client_id, "Connected");
        Ok(())
    }
}

#[async_trait]
impl Transport for QuicClient {
    async fn connect(&self) -> Result<(), TransportError> {
        let _guard = self.connect_lock.lock().await;
        self.connect_once_locked().await
    }

    async fn connect_with_retry(&self) -> Result<(), TransportError> {
        if self.current_connection().await.is_some() {
            return Ok(());
        }

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
        for attempt in 1..=2 {
            let conn = self.ensure_connected().await?;
            let conn_id = conn.stable_id();

            let mut stream = match conn.open_bi().await {
                Ok((send_stream, _)) => send_stream,
                Err(e) => {
                    self.mark_disconnected(Some(conn_id)).await;
                    if attempt == 1 {
                        warn!(attempt, error = %e, "open_bi failed, retrying once");
                        continue;
                    }
                    return Err(TransportError::Quic(e.to_string()));
                }
            };

            let frame = self.codec.encode(&data)?;
            match stream.write_all(&frame).await {
                Ok(()) => {}
                Err(quinn::WriteError::ConnectionLost(e)) => {
                    self.mark_disconnected(Some(conn_id)).await;
                    if attempt == 1 {
                        warn!(attempt, error = %e, "write failed due to lost connection, retrying once");
                        continue;
                    }
                    return Err(TransportError::Quic(e.to_string()));
                }
                Err(e) => return Err(TransportError::Quic(e.to_string())),
            }

            match stream.finish() {
                Ok(()) => return Ok(()),
                Err(e) => {
                    self.mark_disconnected(Some(conn_id)).await;
                    if attempt == 1 {
                        warn!(attempt, error = %e, "finish failed, retrying once");
                        continue;
                    }
                    return Err(TransportError::Quic(e.to_string()));
                }
            }
        }

        Err(TransportError::ConnectionClosed)
    }

    async fn recv(&self) -> Result<Bytes, TransportError> {
        loop {
            let conn = self.ensure_connected().await?;
            let conn_id = conn.stable_id();

            let (_, mut recv_stream) = match conn.accept_bi().await {
                Ok(streams) => streams,
                Err(quinn::ConnectionError::ApplicationClosed { .. })
                | Err(quinn::ConnectionError::ConnectionClosed(_))
                | Err(quinn::ConnectionError::LocallyClosed)
                | Err(quinn::ConnectionError::TimedOut) => {
                    self.mark_disconnected(Some(conn_id)).await;
                    continue;
                }
                Err(e) => {
                    self.mark_disconnected(Some(conn_id)).await;
                    warn!(error = %e, "accept_bi failed, reconnecting");
                    continue;
                }
            };

            let mut header = [0u8; 4];
            match recv_stream.read_exact(&mut header).await {
                Ok(()) => {}
                Err(quinn::ReadExactError::ReadError(quinn::ReadError::ConnectionLost(e))) => {
                    self.mark_disconnected(Some(conn_id)).await;
                    warn!(error = %e, "read header failed due to lost connection, reconnecting");
                    continue;
                }
                Err(quinn::ReadExactError::FinishedEarly(read)) => {
                    warn!(
                        bytes_read = read,
                        "read header finished early, dropping stream"
                    );
                    continue;
                }
                Err(e) => {
                    warn!(error = %e, "read header failed, dropping stream");
                    continue;
                }
            }

            let len = self.codec.decode_length(&header)?;
            let mut payload = vec![0u8; len];
            match recv_stream.read_exact(&mut payload).await {
                Ok(()) => {
                    debug!(len, "Received message");
                    return Ok(Bytes::from(payload));
                }
                Err(quinn::ReadExactError::ReadError(quinn::ReadError::ConnectionLost(e))) => {
                    self.mark_disconnected(Some(conn_id)).await;
                    warn!(error = %e, "read payload failed due to lost connection, reconnecting");
                    continue;
                }
                Err(quinn::ReadExactError::FinishedEarly(read)) => {
                    warn!(
                        bytes_read = read,
                        "read payload finished early, dropping stream"
                    );
                    continue;
                }
                Err(e) => {
                    warn!(error = %e, "read payload failed, dropping stream");
                    continue;
                }
            }
        }
    }

    async fn close(&self) -> Result<(), TransportError> {
        let _guard = self.connect_lock.lock().await;
        self.mark_disconnected(None).await;
        debug!("Connection closed");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

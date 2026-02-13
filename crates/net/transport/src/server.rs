//! QUIC server transport implementation.

use std::sync::Arc;

use bytes::Bytes;
use quinn::Endpoint;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::ServerConfig;
use crate::error::TransportError;
use crate::framing::LengthPrefixCodec;
use crate::tls;
use crate::traits::MessageHandler;

/// QUIC transport server that accepts incoming connections and dispatches
/// inbound messages to a [`MessageHandler`].
#[derive(Debug)]
pub struct QuicServer {
    config: ServerConfig,
    endpoint: Option<Endpoint>,
}

impl QuicServer {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            endpoint: None,
        }
    }

    /// Bind and start accepting connections. Returns a handle to the accept
    /// loop task.
    pub async fn start<H: MessageHandler>(
        &mut self,
        handler: Arc<H>,
    ) -> Result<JoinHandle<()>, TransportError> {
        let tls_config = tls::self_signed_server_config()?;
        let quic_config = quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let server_config = quinn::ServerConfig::with_crypto(std::sync::Arc::new(quic_config));

        let addr = self
            .config
            .listen_addr
            .parse()
            .map_err(|e: std::net::AddrParseError| TransportError::Other(e.to_string()))?;

        let endpoint = Endpoint::server(server_config, addr)?;
        info!(addr = %self.config.listen_addr, "QUIC server listening");

        self.endpoint = Some(endpoint.clone());

        let max_msg = self.config.max_message_size;
        let handle = tokio::spawn(async move {
            accept_loop(endpoint, handler, max_msg).await;
        });

        Ok(handle)
    }

    /// Gracefully stop the server.
    pub fn close(&self) {
        if let Some(ep) = &self.endpoint {
            ep.close(0u32.into(), b"server shutting down");
        }
    }
}

async fn accept_loop<H: MessageHandler>(
    endpoint: Endpoint,
    handler: Arc<H>,
    max_message_size: usize,
) {
    while let Some(incoming) = endpoint.accept().await {
        let handler = Arc::clone(&handler);
        let codec = LengthPrefixCodec::new(max_message_size);
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    if let Err(e) = handle_connection(conn, handler, codec).await {
                        warn!(error = %e, "Connection handler error");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to accept connection");
                }
            }
        });
    }
}

async fn handle_connection<H: MessageHandler>(
    conn: quinn::Connection,
    handler: Arc<H>,
    codec: LengthPrefixCodec,
) -> Result<(), TransportError> {
    let (mut _send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| TransportError::Quic(e.to_string()))?;

    loop {
        let mut header = [0u8; 4];
        match recv.read_exact(&mut header).await {
            Ok(()) => {}
            Err(quinn::ReadExactError::FinishedEarly(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        }

        let len = codec.decode_length(&header)?;
        let mut payload = vec![0u8; len];
        recv.read_exact(&mut payload).await?;

        if let Err(e) = handler.handle(Bytes::from(payload)).await {
            error!(error = %e, "Handler error");
        }
    }
}

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::TransportError;

/// Bidirectional byte-stream transport.
///
/// Implementations handle connection lifecycle and framed message exchange.
/// The protocol (QUIC, TCP, etc.) is an implementation detail.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn connect(&self) -> Result<(), TransportError>;
    async fn connect_with_retry(&self) -> Result<(), TransportError>;
    async fn send(&self, data: Bytes) -> Result<(), TransportError>;
    async fn recv(&self) -> Result<Bytes, TransportError>;
    async fn close(&self) -> Result<(), TransportError>;
    fn is_connected(&self) -> bool;
}

/// Handler for inbound transport messages.
#[async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    async fn handle(&self, data: Bytes) -> Result<(), TransportError>;
}

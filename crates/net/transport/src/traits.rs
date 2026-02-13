//! Transport and message handler trait definitions.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::TransportError;

/// Bidirectional byte-stream transport used by both the QUIC client and server.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send a length-prefixed protobuf message.
    async fn send(&self, data: Bytes) -> Result<(), TransportError>;

    /// Receive the next length-prefixed message.
    async fn recv(&self) -> Result<Bytes, TransportError>;

    /// Gracefully close the transport.
    async fn close(&self) -> Result<(), TransportError>;

    /// Whether the underlying connection is still open.
    fn is_connected(&self) -> bool;
}

/// Handler for inbound messages on a transport connection.
#[async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    /// Process an inbound message. The raw bytes contain a length-prefixed
    /// protobuf `Message`.
    async fn handle(&self, data: Bytes) -> Result<(), TransportError>;
}

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use compose_primitives::ChainId;
use compose_primitives_traits::{CoordinatorError, PublisherClient};
use compose_proto::conversions::chain_id_to_bytes;
use compose_proto::rollup_v2::{wire_message, Vote, WireMessage, XtId};
use compose_transport::traits::Transport;
use prost::Message;

/// Publisher connection implementing the `PublisherClient` trait.
///
/// Transport-agnostic: works with any `Transport` implementation (QUIC, TCP, etc.).
pub struct PublisherConnection {
    transport: Arc<dyn Transport>,
    connected: AtomicBool,
    chain_id: ChainId,
}

impl PublisherConnection {
    pub fn new(transport: Arc<dyn Transport>, chain_id: ChainId) -> Self {
        Self {
            transport,
            connected: AtomicBool::new(false),
            chain_id,
        }
    }

    pub fn transport(&self) -> &Arc<dyn Transport> {
        &self.transport
    }
}

impl std::fmt::Debug for PublisherConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublisherConnection")
            .field("connected", &self.connected.load(Ordering::SeqCst))
            .field("chain_id", &self.chain_id)
            .finish()
    }
}

#[async_trait]
impl PublisherClient for PublisherConnection {
    async fn connect(&self) -> Result<(), CoordinatorError> {
        self.transport
            .connect()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))?;
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn connect_with_retry(&self) -> Result<(), CoordinatorError> {
        self.transport
            .connect_with_retry()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))?;
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), CoordinatorError> {
        self.connected.store(false, Ordering::SeqCst);
        self.transport
            .close()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))?;
        Ok(())
    }

    async fn send_vote(&self, instance_id: &[u8], vote: bool) -> Result<(), CoordinatorError> {
        let msg = WireMessage {
            sender_id: String::new(),
            payload: Some(wire_message::Payload::Vote(Vote {
                sender_chain_id: chain_id_to_bytes(self.chain_id),
                xt_id: Some(XtId {
                    hash: instance_id.to_vec(),
                }),
                vote,
            })),
        };

        self.transport
            .send(Bytes::from(msg.encode_to_vec()))
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn send_raw(&self, data: &[u8]) -> Result<(), CoordinatorError> {
        self.transport
            .send(Bytes::copy_from_slice(data))
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst) && self.transport.is_connected()
    }
}

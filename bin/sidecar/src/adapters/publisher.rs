//! Publisher transport adapter implementing coordinator publisher traits.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use compose_coordinator::error::CoordinatorError;
use compose_coordinator::traits::publisher::PublisherClient;
use compose_proto::rollup_v2::{wire_message, Vote, WireMessage, XtId};
use compose_transport::client::QuicClient;
use compose_transport::traits::Transport;
use prost::Message;

/// Publisher adapter bridging the QUIC transport to the coordinator's
/// `PublisherClient` trait.
pub(crate) struct QuicPublisherAdapter {
    client: Arc<QuicClient>,
    connected: AtomicBool,
}

impl QuicPublisherAdapter {
    pub(crate) fn new(client: Arc<QuicClient>) -> Self {
        Self {
            client,
            connected: AtomicBool::new(false),
        }
    }
}

impl std::fmt::Debug for QuicPublisherAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicPublisherAdapter")
            .field("connected", &self.connected.load(Ordering::SeqCst))
            .finish()
    }
}

#[async_trait]
impl PublisherClient for QuicPublisherAdapter {
    async fn connect(&self) -> Result<(), CoordinatorError> {
        self.client
            .connect()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))?;
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn connect_with_retry(&self) -> Result<(), CoordinatorError> {
        self.client
            .connect_with_retry()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))?;
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), CoordinatorError> {
        self.connected.store(false, Ordering::SeqCst);
        self.client
            .close()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))?;
        Ok(())
    }

    async fn send_vote(&self, instance_id: &[u8], vote: bool) -> Result<(), CoordinatorError> {
        let msg = WireMessage {
            sender_id: String::new(),
            payload: Some(wire_message::Payload::Vote(Vote {
                sender_chain_id: Vec::new(),
                xt_id: Some(XtId {
                    hash: instance_id.to_vec(),
                }),
                vote,
            })),
        };

        let data = msg.encode_to_vec();
        self.client
            .send(Bytes::from(data))
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))?;

        Ok(())
    }

    async fn send_raw(&self, data: &[u8]) -> Result<(), CoordinatorError> {
        self.client
            .send(Bytes::copy_from_slice(data))
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))?;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst) && self.client.is_connected()
    }
}

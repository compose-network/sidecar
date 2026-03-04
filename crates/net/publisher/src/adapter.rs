use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use compose_coordinator::error::CoordinatorError;
use compose_coordinator::traits::publisher::PublisherClient;
use compose_primitives::ChainId;
use compose_proto::conversions::chain_id_to_proto;
use compose_proto::rollup_v2::{wire_message, Vote, WireMessage};
use compose_transport::client::QuicClient;
use compose_transport::traits::Transport;
use prost::Message;

pub struct QuicPublisherAdapter {
    client: Arc<QuicClient>,
    chain_id: ChainId,
}

impl QuicPublisherAdapter {
    pub fn new(client: Arc<QuicClient>, chain_id: ChainId) -> Self {
        Self {
            client,
            chain_id,
        }
    }
}

impl std::fmt::Debug for QuicPublisherAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicPublisherAdapter")
            .field("connected", &self.client.is_connected())
            .field("chain_id", &self.chain_id)
            .finish()
    }
}

#[async_trait]
impl PublisherClient for QuicPublisherAdapter {
    async fn connect(&self) -> Result<(), CoordinatorError> {
        self.client
            .connect()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn connect_with_retry(&self) -> Result<(), CoordinatorError> {
        self.client
            .connect_with_retry()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn disconnect(&self) -> Result<(), CoordinatorError> {
        self.client
            .close()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn send_vote(&self, instance_id: &[u8], vote: bool) -> Result<(), CoordinatorError> {
        let msg = WireMessage {
            sender_id: String::new(),
            payload: Some(wire_message::Payload::Vote(Vote {
                instance_id: instance_id.to_vec(),
                chain_id: chain_id_to_proto(self.chain_id),
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
        self.client.is_connected()
    }
}

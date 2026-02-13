//! Peer coordinator trait and error types.

use async_trait::async_trait;
use compose_primitives::{ChainId, SequenceNumber};
use std::collections::HashMap;

/// Peer coordinator for forwarding XTs and votes between sidecars.
#[async_trait]
pub trait PeerCoordinator: Send + Sync + 'static {
    /// Forward a cross-chain transaction to peer sidecars.
    async fn forward_xt(
        &self,
        instance_id: &str,
        txs: &HashMap<ChainId, Vec<Vec<u8>>>,
        origin_chain: ChainId,
        origin_seq: SequenceNumber,
    ) -> Result<(), PeerError>;

    /// Send a vote to all peer sidecars.
    async fn send_vote_to_peers(
        &self,
        instance_id: &str,
        chain_id: ChainId,
        vote: bool,
    ) -> Result<(), PeerError>;

    /// Return the chain IDs of connected peers.
    fn peer_chain_ids(&self) -> Vec<ChainId>;

    /// Close all peer connections.
    async fn close(&self);
}

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("peer not found for chain {0}")]
    PeerNotFound(ChainId),
    #[error("request failed: {0}")]
    Request(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

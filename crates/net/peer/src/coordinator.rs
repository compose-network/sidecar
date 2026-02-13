//! HTTP peer coordinator implementation for sidecar-to-sidecar calls.

use std::collections::HashMap;

use async_trait::async_trait;
use compose_primitives::{ChainId, SequenceNumber};
use reqwest::Client;
use tracing::{error, info, warn};

use crate::traits::{PeerCoordinator, PeerError};
use crate::types::{VoteRequest, XtForwardRequest};

/// Peer configuration entry.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub chain_id: ChainId,
    pub addr: String,
}

/// HTTP-based peer coordinator that forwards XTs and votes to peer sidecars
/// using JSON-over-HTTP.
#[derive(Debug)]
pub struct HttpPeerCoordinator {
    client: Client,
    peers: Vec<PeerEntry>,
}

impl HttpPeerCoordinator {
    pub fn new(peers: Vec<PeerEntry>) -> Self {
        Self {
            client: Client::new(),
            peers,
        }
    }

    fn all_peer_urls(&self, path: &str) -> Vec<(ChainId, String)> {
        self.peers
            .iter()
            .map(|p| (p.chain_id, format!("http://{}{}", p.addr, path)))
            .collect()
    }
}

#[async_trait]
impl PeerCoordinator for HttpPeerCoordinator {
    async fn forward_xt(
        &self,
        instance_id: &str,
        txs: &HashMap<ChainId, Vec<Vec<u8>>>,
        origin_seq: SequenceNumber,
    ) -> Result<(), PeerError> {
        let origin_chain = self.peers.first().map(|p| p.chain_id).unwrap_or(ChainId(0));

        let req = XtForwardRequest::new(instance_id.to_string(), txs, origin_chain, origin_seq);

        let urls = self.all_peer_urls("/xt/forward");
        for (chain_id, url) in urls {
            let body =
                serde_json::to_vec(&req).map_err(|e| PeerError::Serialization(e.to_string()))?;

            match self
                .client
                .post(&url)
                .body(body)
                .header("content-type", "application/json")
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!(chain_id = %chain_id, instance_id, "Forwarded XT to peer");
                }
                Ok(resp) => {
                    warn!(
                        chain_id = %chain_id,
                        status = %resp.status(),
                        "Peer rejected forwarded XT"
                    );
                }
                Err(e) => {
                    error!(chain_id = %chain_id, error = %e, "Failed to forward XT to peer");
                }
            }
        }

        Ok(())
    }

    async fn send_vote_to_peers(
        &self,
        instance_id: &str,
        chain_id: ChainId,
        vote: bool,
    ) -> Result<(), PeerError> {
        let req = VoteRequest {
            instance_id: instance_id.to_string(),
            chain_id: chain_id.0,
            vote,
        };

        let urls = self.all_peer_urls("/xt/vote");
        for (peer_chain_id, url) in urls {
            let body =
                serde_json::to_vec(&req).map_err(|e| PeerError::Serialization(e.to_string()))?;

            match self
                .client
                .post(&url)
                .body(body)
                .header("content-type", "application/json")
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!(
                        peer_chain_id = %peer_chain_id,
                        instance_id,
                        vote,
                        "Sent vote to peer"
                    );
                }
                Ok(resp) => {
                    warn!(
                        peer_chain_id = %peer_chain_id,
                        status = %resp.status(),
                        "Peer rejected vote"
                    );
                }
                Err(e) => {
                    error!(
                        peer_chain_id = %peer_chain_id,
                        error = %e,
                        "Failed to send vote to peer"
                    );
                }
            }
        }

        Ok(())
    }

    fn peer_chain_ids(&self) -> Vec<ChainId> {
        self.peers.iter().map(|p| p.chain_id).collect()
    }

    async fn close(&self) {
        // HTTP client does not need explicit closing.
    }
}

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
            .map(|p| {
                let base = p.addr.trim_end_matches('/');
                (p.chain_id, format!("{base}{path}"))
            })
            .collect()
    }
}

#[async_trait]
impl PeerCoordinator for HttpPeerCoordinator {
    async fn forward_xt(
        &self,
        instance_id: &str,
        txs: &HashMap<ChainId, Vec<Vec<u8>>>,
        origin_chain: ChainId,
        origin_seq: SequenceNumber,
    ) -> Result<(), PeerError> {
        let req = XtForwardRequest::new(instance_id.to_string(), txs, origin_chain, origin_seq);
        let body = serde_json::to_vec(&req).map_err(|e| PeerError::Serialization(e.to_string()))?;

        let urls = self.all_peer_urls("/xt/forward");
        let futs = urls.into_iter().map(|(chain_id, url)| {
            let client = &self.client;
            let body = body.clone();
            let instance_id = instance_id.to_string();
            async move {
                match client
                    .post(&url)
                    .body(body)
                    .header("content-type", "application/json")
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        info!(chain_id = %chain_id, instance_id = %instance_id, "Forwarded XT to peer");
                        None
                    }
                    Ok(resp) => {
                        warn!(chain_id = %chain_id, status = %resp.status(), "Peer rejected forwarded XT");
                        Some(format!("chain {chain_id} rejected forward: {}", resp.status()))
                    }
                    Err(e) => {
                        error!(chain_id = %chain_id, error = %e, "Failed to forward XT to peer");
                        Some(format!("chain {chain_id} forward failed: {e}"))
                    }
                }
            }
        });

        let errors: Vec<String> = futures::future::join_all(futs)
            .await
            .into_iter()
            .flatten()
            .collect();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(PeerError::Request(errors.join("; ")))
        }
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
        let body = serde_json::to_vec(&req).map_err(|e| PeerError::Serialization(e.to_string()))?;

        let urls = self.all_peer_urls("/xt/vote");
        let futs = urls.into_iter().map(|(peer_chain_id, url)| {
            let client = &self.client;
            let body = body.clone();
            let instance_id = instance_id.to_string();
            async move {
                match client
                    .post(&url)
                    .body(body)
                    .header("content-type", "application/json")
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        info!(
                            peer_chain_id = %peer_chain_id,
                            instance_id = %instance_id,
                            vote,
                            "Sent vote to peer"
                        );
                        None
                    }
                    Ok(resp) => {
                        warn!(peer_chain_id = %peer_chain_id, status = %resp.status(), "Peer rejected vote");
                        Some(format!(
                            "chain {peer_chain_id} rejected vote: {}",
                            resp.status()
                        ))
                    }
                    Err(e) => {
                        error!(peer_chain_id = %peer_chain_id, error = %e, "Failed to send vote to peer");
                        Some(format!("chain {peer_chain_id} vote failed: {e}"))
                    }
                }
            }
        });

        let errors: Vec<String> = futures::future::join_all(futs)
            .await
            .into_iter()
            .flatten()
            .collect();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(PeerError::Request(errors.join("; ")))
        }
    }

    fn peer_chain_ids(&self) -> Vec<ChainId> {
        self.peers.iter().map(|p| p.chain_id).collect()
    }

    async fn close(&self) {
        // HTTP client does not need explicit closing.
    }
}

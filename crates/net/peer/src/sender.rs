//! HTTP-based mailbox sender for delivering CIRC messages to peer sidecars.

use std::collections::HashMap;

use async_trait::async_trait;
use compose_primitives::ChainId;
use compose_primitives_traits::{CoordinatorError, MailboxSender};
use compose_proto::rollup_v2::MailboxMessage;
use prost::Message;
use reqwest::Client;
use tracing::{error, info};

use crate::coordinator::PeerEntry;

/// Forwards CIRC mailbox messages to peer sidecars via HTTP.
pub struct PeerMailboxSender {
    client: Client,
    peer_addrs: HashMap<ChainId, String>,
}

impl PeerMailboxSender {
    pub fn with_peer_entries(entries: &[PeerEntry]) -> Self {
        let peer_addrs = entries
            .iter()
            .map(|e| (e.chain_id, e.addr.clone()))
            .collect();

        Self {
            client: Client::new(),
            peer_addrs,
        }
    }
}

impl std::fmt::Debug for PeerMailboxSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerMailboxSender").finish()
    }
}

#[async_trait]
impl MailboxSender for PeerMailboxSender {
    async fn send(
        &self,
        dest_chain_id: ChainId,
        msg: &MailboxMessage,
    ) -> Result<(), CoordinatorError> {
        let addr = self.peer_addrs.get(&dest_chain_id).ok_or_else(|| {
            CoordinatorError::Other(format!("no peer address for chain {dest_chain_id}"))
        })?;

        if addr.is_empty() {
            return Err(CoordinatorError::Other(format!(
                "empty peer address for chain {dest_chain_id}"
            )));
        }

        let base = addr.trim_end_matches('/');
        let url = format!("{base}/mailbox");
        let body = msg.encode_to_vec();

        match self
            .client
            .post(&url)
            .header("content-type", "application/octet-stream")
            .body(body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    dest_chain = %dest_chain_id,
                    label = %msg.label,
                    "Sent mailbox message to peer"
                );
                Ok(())
            }
            Ok(resp) => Err(CoordinatorError::Mailbox(format!(
                "peer returned status {} for mailbox message",
                resp.status()
            ))),
            Err(e) => {
                error!(dest_chain = %dest_chain_id, error = %e, "Failed to send mailbox message");
                Err(CoordinatorError::Mailbox(e.to_string()))
            }
        }
    }
}

//! Mailbox sender adapter used by the sidecar binary.

use std::sync::Arc;

use async_trait::async_trait;
use compose_coordinator::error::CoordinatorError;
use compose_coordinator::traits::mailbox::MailboxSender;
use compose_peer::traits::PeerCoordinator;
use compose_primitives::ChainId;
use compose_proto::rollup_v2::MailboxMessage;

/// Mailbox sender adapter that forwards CIRC messages to peer sidecars via HTTP.
pub(crate) struct PeerMailboxSender {
    #[allow(dead_code)]
    peers: Arc<dyn PeerCoordinator>,
}

impl PeerMailboxSender {
    pub(crate) fn new(peers: Arc<dyn PeerCoordinator>) -> Self {
        Self { peers }
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
        _dest_chain_id: ChainId,
        _msg: &MailboxMessage,
    ) -> Result<(), CoordinatorError> {
        // TODO: Send the protobuf-encoded message to the peer's /mailbox endpoint.
        Ok(())
    }
}

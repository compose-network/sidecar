//! Mailbox queue and sender trait definitions.

use async_trait::async_trait;
use compose_primitives::ChainId;
use compose_proto::rollup_v2::MailboxMessage;

use crate::error::MailboxError;

/// Queue for recording and querying inbound CIRC messages.
#[async_trait]
pub trait MailboxQueue: Send + Sync + 'static {
    /// Record an inbound mailbox message.
    async fn record(&self, msg: &MailboxMessage) -> Result<(), MailboxError>;

    /// Query pending messages for a given instance.
    async fn pending(&self, instance_id: &[u8]) -> Result<Vec<MailboxMessage>, MailboxError>;
}

/// Sender for outbound CIRC messages to peer sidecars.
#[async_trait]
pub trait MailboxSender: Send + Sync + 'static {
    /// Send a mailbox message to the destination chain's sidecar.
    async fn send(&self, dest_chain_id: ChainId, msg: &MailboxMessage) -> Result<(), MailboxError>;
}

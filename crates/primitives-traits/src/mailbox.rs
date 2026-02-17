//! Mailbox sender trait for CIRC message delivery.

use async_trait::async_trait;
use compose_primitives::ChainId;
use compose_proto::rollup_v2::MailboxMessage;

use crate::error::CoordinatorError;

/// Sender for CIRC mailbox messages to peer sidecars.
#[async_trait]
pub trait MailboxSender: Send + Sync + 'static {
    async fn send(
        &self,
        dest_chain_id: ChainId,
        msg: &MailboxMessage,
    ) -> Result<(), CoordinatorError>;
}

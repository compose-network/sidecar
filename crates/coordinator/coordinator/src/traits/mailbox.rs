//! Mailbox sender trait used by the coordinator.

use async_trait::async_trait;
use compose_primitives::ChainId;
use compose_proto::rollup_v2::MailboxMessage;

use crate::error::CoordinatorError;

/// Sender for CIRC mailbox messages to peer sidecars.
///
/// This is separate from the mailbox crate's `MailboxSender` to allow
/// the coordinator to use its own error type.
#[async_trait]
pub trait MailboxSender: Send + Sync + 'static {
    async fn send(
        &self,
        dest_chain_id: ChainId,
        msg: &MailboxMessage,
    ) -> Result<(), CoordinatorError>;
}

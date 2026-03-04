//! Builders for putInbox transaction calldata and signing hooks.

use alloy::primitives::U256;
use async_trait::async_trait;
use compose_primitives::CrossRollupDependency;

use crate::abi;
use crate::error::MailboxError;

/// Builds `putInbox` calldata for fulfilled cross-rollup dependencies.
#[async_trait]
pub trait PutInboxBuilder: Send + Sync + 'static {
    /// Get the pending nonce for the coordinator address.
    async fn pending_nonce_at(&self) -> Result<u64, MailboxError>;

    /// Build a signed `putInbox` transaction with the given nonce.
    async fn build_put_inbox_tx(
        &self,
        dep: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, MailboxError>;
}

/// Encode the `putInbox` calldata (without signing).
pub fn encode_put_inbox_calldata(dep: &CrossRollupDependency) -> Result<Vec<u8>, MailboxError> {
    let session_id = dep.session_id.unwrap_or(U256::ZERO);
    let data = dep.data.as_deref().unwrap_or_default();

    abi::encode_put_inbox(
        dep.source_chain_id.0,
        dep.sender,
        dep.receiver,
        session_id,
        &dep.label,
        data,
    )
}

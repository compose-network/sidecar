//! Decision handler trait definitions.

use async_trait::async_trait;

use compose_primitives_traits::CoordinatorError;

/// Decision handler for determining commit/abort outcomes.
#[async_trait]
pub trait DecisionHandler: Send + Sync + 'static {
    /// Record a vote and potentially trigger a decision.
    async fn record_vote(
        &self,
        instance_id: &str,
        chain_id: u64,
        vote: bool,
    ) -> Result<Option<bool>, CoordinatorError>;
}

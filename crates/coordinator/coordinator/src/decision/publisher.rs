//! Decision handler that defers finalization to the publisher.

use async_trait::async_trait;

use compose_primitives_traits::CoordinatorError;
use crate::traits::decision::DecisionHandler;

/// Decision handler that defers to the publisher's 2PC mechanism.
///
/// The publisher collects all votes and broadcasts a `Decided` message.
/// This handler does not make local decisions — it just records votes
/// and waits for the publisher's authoritative decision.
#[derive(Debug)]
pub struct PublisherDecisionHandler;

#[async_trait]
impl DecisionHandler for PublisherDecisionHandler {
    async fn record_vote(
        &self,
        _instance_id: &str,
        _chain_id: u64,
        _vote: bool,
    ) -> Result<Option<bool>, CoordinatorError> {
        // In publisher mode, the publisher makes the decision.
        // Votes are forwarded to the publisher, not collected locally.
        Ok(None)
    }
}

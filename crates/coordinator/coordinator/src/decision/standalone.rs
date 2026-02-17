//! Standalone decision handler based on locally collected votes.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::debug;

use compose_primitives_traits::CoordinatorError;
use crate::traits::decision::DecisionHandler;

/// Tracks vote state per instance for standalone decision-making.
#[derive(Debug, Default)]
struct VoteState {
    expected: usize,
    votes: HashMap<u64, bool>,
}

/// Decision handler for standalone mode (no publisher).
///
/// Collects votes from the local chain and peer sidecars, then makes a
/// commit/abort decision when all expected votes are in.
#[derive(Debug)]
pub struct StandaloneDecisionHandler {
    states: Arc<RwLock<HashMap<String, VoteState>>>,
}

impl Default for StandaloneDecisionHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl StandaloneDecisionHandler {
    pub fn new() -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register an instance with the expected number of participating chains.
    pub async fn register(&self, instance_id: &str, expected_votes: usize) {
        let mut states = self.states.write().await;
        states.insert(
            instance_id.to_string(),
            VoteState {
                expected: expected_votes,
                votes: HashMap::new(),
            },
        );
    }
}

#[async_trait]
impl DecisionHandler for StandaloneDecisionHandler {
    async fn record_vote(
        &self,
        instance_id: &str,
        chain_id: u64,
        vote: bool,
    ) -> Result<Option<bool>, CoordinatorError> {
        let mut states = self.states.write().await;
        let state = states
            .get_mut(instance_id)
            .ok_or_else(|| CoordinatorError::InstanceNotFound(instance_id.to_string()))?;

        state.votes.insert(chain_id, vote);

        debug!(
            instance_id,
            chain_id,
            vote,
            collected = state.votes.len(),
            expected = state.expected,
            "Recorded vote"
        );

        if state.votes.len() >= state.expected {
            let all_yes = state.votes.values().all(|&v| v);
            Ok(Some(all_yes))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unanimous_commit() {
        let handler = StandaloneDecisionHandler::new();
        handler.register("xt-1", 2).await;

        let r1 = handler.record_vote("xt-1", 901, true).await.unwrap();
        assert_eq!(r1, None);

        let r2 = handler.record_vote("xt-1", 902, true).await.unwrap();
        assert_eq!(r2, Some(true));
    }

    #[tokio::test]
    async fn one_abort_means_abort() {
        let handler = StandaloneDecisionHandler::new();
        handler.register("xt-2", 2).await;

        handler.record_vote("xt-2", 901, true).await.unwrap();
        let r2 = handler.record_vote("xt-2", 902, false).await.unwrap();
        assert_eq!(r2, Some(false));
    }
}

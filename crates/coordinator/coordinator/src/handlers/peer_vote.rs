//! Peer vote intake and standalone decision aggregation.

use compose_primitives::ChainId;
use tracing::info;

use crate::coordinator::DefaultCoordinator;
use crate::error::CoordinatorError;

impl DefaultCoordinator {
    /// Process a vote received from a peer sidecar.
    pub async fn handle_peer_vote(
        &self,
        instance_id: &str,
        chain_id: ChainId,
        vote: bool,
    ) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().await;

        let xt = state
            .pending
            .get_mut(instance_id)
            .ok_or_else(|| CoordinatorError::InstanceNotFound(instance_id.to_string()))?;

        xt.peer_votes.insert(chain_id, vote);

        info!(
            instance_id,
            peer_chain = %chain_id,
            vote,
            "Received peer vote"
        );

        // Attempt to make a local decision if all votes are in.
        let expected = xt.raw_txs.len();
        let mut collected = 0;
        let mut all_yes = true;

        if let Some(local) = xt.local_vote {
            collected += 1;
            if !local {
                all_yes = false;
            }
        }

        for (cid, &v) in &xt.peer_votes {
            if *cid == self.chain_id {
                continue;
            }
            collected += 1;
            if !v {
                all_yes = false;
            }
        }

        if collected >= expected && xt.decision.is_none() {
            let decision = all_yes;
            xt.decision = Some(decision);
            xt.decided_at = Some(std::time::Instant::now());

            info!(
                instance_id,
                decision,
                votes = collected,
                "Made local decision (standalone mode)"
            );
        }

        Ok(())
    }
}

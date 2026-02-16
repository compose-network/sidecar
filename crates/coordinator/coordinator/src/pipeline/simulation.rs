//! Simulation pipeline and vote emission flow.

use tracing::{debug, error, info, warn};

use crate::coordinator::DefaultCoordinator;
use crate::model::pending_xt::PendingXt;

/// Maximum number of resimulation attempts per transaction.
const MAX_RESIMULATIONS: usize = 3;

impl DefaultCoordinator {
    /// Run the simulation pipeline for the local chain's portion of an XT.
    ///
    /// Simulates transactions sequentially, discovers mailbox dependencies,
    /// waits for CIRC messages, and sends a vote.
    pub(crate) async fn process_xt(&self, instance_id: &str, _xt: &PendingXt) {
        info!(instance_id, chain_id = %self.chain_id, "Processing XT");

        let (tx_bytes_list, base_overrides) = {
            let state = self.state.read().await;
            match state.pending.get(instance_id) {
                Some(xt) => match xt.raw_txs.get(&self.chain_id) {
                    Some(txs) if !txs.is_empty() => {
                        let has_chain_state = xt.chain_states.contains_key(&self.chain_id);
                        let has_overrides = xt
                            .chain_states
                            .get(&self.chain_id)
                            .and_then(|cs| cs.state_overrides.as_ref())
                            .is_some();
                        debug!(
                            instance_id,
                            chain_id = %self.chain_id,
                            has_chain_state,
                            has_overrides,
                            num_chain_states = xt.chain_states.len(),
                            "Simulation state check"
                        );
                        let overrides = xt
                            .chain_states
                            .get(&self.chain_id)
                            .and_then(|cs| cs.state_overrides.clone())
                            .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
                        (txs.clone(), overrides)
                    }
                    _ => {
                        warn!(instance_id, "No local transactions, rejecting");
                        drop(state);
                        let _ = self.send_vote(instance_id, &[], false).await;
                        return;
                    }
                },
                None => return,
            }
        };

        // Lock the local chain.
        {
            let mut state = self.state.write().await;
            if let Some(xt) = state.pending.get_mut(instance_id) {
                xt.locked_chains.insert(self.chain_id, true);
            }
        }

        let simulator = match &self.simulator {
            Some(s) => s.clone(),
            None => {
                warn!("No simulator configured, voting yes without simulation");
                let _ = self.send_vote(instance_id, &[], true).await;
                return;
            }
        };

        // Simulate each transaction sequentially.
        for (tx_index, tx_bytes) in tx_bytes_list.iter().enumerate() {
            let mut success = false;

            for attempt in 0..MAX_RESIMULATIONS {
                match simulator
                    .simulate(self.chain_id, tx_bytes, &base_overrides)
                    .await
                {
                    Ok(result) => {
                        if !result.success && result.dependencies.is_empty() {
                            warn!(
                                instance_id,
                                tx_index,
                                error = ?result.error,
                                "Simulation returned failure with no dependencies"
                            );
                            let _ = self.send_vote(instance_id, &[], false).await;
                            return;
                        }

                        if result.success {
                            success = true;
                            break;
                        }

                        // Has dependencies — would wait for CIRC messages here.
                        // For now, retry up to MAX_RESIMULATIONS.
                        if attempt + 1 >= MAX_RESIMULATIONS {
                            warn!(
                                instance_id,
                                tx_index, "Simulation still failing after max attempts"
                            );
                            let _ = self.send_vote(instance_id, &[], false).await;
                            return;
                        }
                    }
                    Err(e) => {
                        error!(instance_id, error = %e, "Simulation failed");
                        let _ = self.send_vote(instance_id, &[], false).await;
                        return;
                    }
                }
            }

            if !success {
                warn!(
                    instance_id,
                    tx_index, "Simulation failed after max attempts"
                );
                let _ = self.send_vote(instance_id, &[], false).await;
                return;
            }
        }

        let _ = self.send_vote(instance_id, &[], true).await;
    }

    /// Send a vote for the given instance.
    pub(crate) async fn send_vote(
        &self,
        instance_id: &str,
        _instance_id_bytes: &[u8],
        vote: bool,
    ) -> Result<(), crate::error::CoordinatorError> {
        {
            let mut state = self.state.write().await;
            if let Some(xt) = state.pending.get_mut(instance_id) {
                xt.simulated_at = Some(std::time::Instant::now());
                xt.vote_sent = true;
                xt.local_vote = Some(vote);
            }
        }

        if self.is_publisher_connected().await {
            if let Some(publisher) = &self.publisher {
                let instance_bytes = {
                    let state = self.state.read().await;
                    state
                        .pending
                        .get(instance_id)
                        .map(|xt| xt.instance_id.clone())
                        .unwrap_or_default()
                };
                if let Err(e) = publisher.send_vote(&instance_bytes, vote).await {
                    error!(instance_id, error = %e, "Failed to send vote to publisher");
                }
                info!(instance_id, vote, "Vote sent to publisher");
            }
        } else {
            info!(
                instance_id,
                vote,
                chain_id = %self.chain_id,
                "Local vote recorded (standalone mode)"
            );

            // Forward vote to peers.
            if let Some(peer_coord) = &self.peer_coordinator {
                let chain_id = self.chain_id;
                let id = instance_id.to_string();
                let pc = peer_coord.clone();
                tokio::spawn(async move {
                    if let Err(e) = pc.send_vote_to_peers(&id, chain_id, vote).await {
                        error!(instance_id = %id, error = %e, "Failed to send vote to peers");
                    }
                });
            }
        }

        Ok(())
    }
}

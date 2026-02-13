//! Builder poll handling and hold/ready response logic.

use compose_primitives::{BuilderPollRequest, BuilderPollResponse, ChainState};

use crate::coordinator::DefaultCoordinator;
use crate::error::CoordinatorError;
use crate::model::ordering::xt_less;

impl DefaultCoordinator {
    /// Process a builder poll from op-rbuilder. Returns committed transactions
    /// or a hold signal if an undecided XT is blocking.
    pub async fn handle_builder_poll(
        &self,
        req: &BuilderPollRequest,
    ) -> Result<BuilderPollResponse, CoordinatorError> {
        if req.flashblock_index == 0 {
            return Ok(BuilderPollResponse {
                hold: false,
                txs: Vec::new(),
                poll_after_ms: None,
                max_hold_ms: None,
            });
        }

        self.nonce_manager.reset_for_block(req.block_number).await;

        let state_snapshot = ChainState {
            chain_id: req.chain_id,
            block_number: req.block_number,
            flashblock_index: req.flashblock_index,
            state_root: req.state_root,
            timestamp: req.timestamp,
            gas_limit: req.gas_limit,
            state_overrides: req.state_overrides.clone(),
        };

        let mut state = self.state.write().await;
        state
            .chain_states
            .insert(req.chain_id, state_snapshot.clone());

        // Collect entries that involve this chain.
        let mut entries: Vec<(String, bool)> = Vec::new();
        for (id, xt) in &mut state.pending {
            if !xt.raw_txs.contains_key(&req.chain_id) {
                continue;
            }
            // Update chain state if not locked.
            if xt.decision.is_none()
                && !xt
                    .locked_chains
                    .get(&req.chain_id)
                    .copied()
                    .unwrap_or(false)
            {
                xt.chain_states.insert(req.chain_id, state_snapshot.clone());
            }
            entries.push((id.clone(), xt.decision.is_none()));
        }

        if entries.is_empty() {
            return Ok(BuilderPollResponse {
                hold: false,
                txs: Vec::new(),
                poll_after_ms: None,
                max_hold_ms: None,
            });
        }

        // Check if the first undecided XT is ready for processing.
        let mut first_undecided: Option<String> = None;
        for (id, is_undecided) in &entries {
            if !is_undecided {
                continue;
            }
            match &first_undecided {
                None => first_undecided = Some(id.clone()),
                Some(current) => {
                    let a = &state.pending[id];
                    let b = &state.pending[current];
                    if xt_less(id, a, current, b) {
                        first_undecided = Some(id.clone());
                    }
                }
            }
        }

        // If we have an undecided XT that's ready, trigger processing.
        if let Some(ref undecided_id) = first_undecided {
            let xt = &state.pending[undecided_id];
            let ready = if self.is_publisher_connected().await {
                self.all_chains_ready(xt)
            } else {
                xt.chain_states.contains_key(&self.chain_id)
            };

            if ready && !xt.vote_sent {
                let id = undecided_id.clone();
                let xt_clone = xt.clone();
                let coordinator = self.clone_ref();
                // Process asynchronously to avoid holding the lock.
                drop(state);
                tokio::spawn(async move {
                    coordinator.process_xt(&id, &xt_clone).await;
                });
                // Re-acquire to keep the lock alive until after the response.
                let _state = self.state.read().await;
                return Ok(BuilderPollResponse {
                    hold: true,
                    txs: Vec::new(),
                    poll_after_ms: Some(50),
                    max_hold_ms: Some(self.circ_timeout_ms),
                });
            }
        }

        // Check for deliverable committed XTs.
        let has_undecided = first_undecided.is_some();
        drop(state);

        if has_undecided {
            Ok(BuilderPollResponse {
                hold: true,
                txs: Vec::new(),
                poll_after_ms: Some(50),
                max_hold_ms: Some(self.circ_timeout_ms),
            })
        } else {
            Ok(BuilderPollResponse {
                hold: false,
                txs: Vec::new(),
                poll_after_ms: None,
                max_hold_ms: None,
            })
        }
    }

    fn all_chains_ready(&self, xt: &crate::model::pending_xt::PendingXt) -> bool {
        xt.raw_txs
            .keys()
            .all(|chain_id| xt.chain_states.contains_key(chain_id))
    }
}

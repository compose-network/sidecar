//! Builder poll handling and hold/ready response logic.

use compose_primitives::{BuilderPollRequest, BuilderPollResponse, ChainState};
use tracing::{debug, error, info};

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;
use crate::model::ordering::xt_less;
use crate::pipeline::delivery::{build_transaction_payloads, deps_for_chain, DeliverableXt};

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
                transactions: Vec::new(),
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
        let mut entries = Vec::<String>::new();
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
            entries.push(id.clone());
        }

        if entries.is_empty() {
            return Ok(BuilderPollResponse {
                hold: false,
                transactions: Vec::new(),
                poll_after_ms: None,
                max_hold_ms: None,
            });
        }

        debug!(
            chain_id = %req.chain_id,
            block_number = req.block_number,
            flashblock_index = req.flashblock_index,
            entries = entries.len(),
            "Builder poll found pending entries"
        );

        entries.sort_by(|a_id, b_id| {
            let a = &state.pending[a_id];
            let b = &state.pending[b_id];
            if xt_less(a_id, a, b_id, b) {
                std::cmp::Ordering::Less
            } else if xt_less(b_id, b, a_id, a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });

        // Check if the first undecided XT is ready for processing.
        let first_undecided = entries
            .iter()
            .find(|id| state.pending[*id].decision.is_none())
            .cloned();

        // If we have an undecided XT that's ready, trigger processing.
        if let Some(ref undecided_id) = first_undecided {
            let xt = &state.pending[undecided_id];
            let ready = if self.is_publisher_connected().await {
                self.all_chains_ready(xt)
            } else {
                xt.chain_states.contains_key(&self.chain_id)
            };
            let is_locked = xt
                .locked_chains
                .get(&self.chain_id)
                .copied()
                .unwrap_or(false);

            if ready && !xt.vote_sent && !is_locked {
                let id = undecided_id.clone();
                let xt_clone = xt.clone();
                if let Some(xt_mut) = state.pending.get_mut(&id) {
                    xt_mut.locked_chains.insert(self.chain_id, true);
                }

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
                    transactions: Vec::new(),
                    poll_after_ms: Some(50),
                    max_hold_ms: Some(self.circ_timeout_ms),
                });
            }
        }

        let mut deliverables = Vec::<DeliverableXt>::new();
        let mut has_blocking_undecided = false;
        for id in &entries {
            let xt = state
                .pending
                .get_mut(id)
                .expect("entry id collected from pending state");

            if xt
                .delivered_chains
                .get(&req.chain_id)
                .copied()
                .unwrap_or(false)
            {
                continue;
            }

            match xt.decision {
                None => {
                    has_blocking_undecided = true;
                    break;
                }
                Some(false) => {
                    xt.delivered_chains.insert(req.chain_id, true);
                    continue;
                }
                Some(true) => {}
            }

            let raw_txs = xt.raw_txs.get(&req.chain_id).cloned().unwrap_or_default();
            if raw_txs.is_empty() {
                xt.delivered_chains.insert(req.chain_id, true);
                continue;
            }

            let deps = deps_for_chain(&xt.fulfilled_deps, req.chain_id);

            deliverables.push(DeliverableXt {
                id: id.clone(),
                put_inbox_txs: Vec::new(),
                raw_txs,
                deps,
            });
        }

        drop(state);

        debug!(
            chain_id = %req.chain_id,
            deliverables = deliverables.len(),
            has_blocking_undecided,
            "Builder poll deliverables collected"
        );

        if !deliverables.is_empty() {
            if let Err(e) = self.build_put_inbox_transactions(&mut deliverables).await {
                error!(chain_id = %req.chain_id, error = %e, "Failed to build putInbox transactions");
                return Ok(BuilderPollResponse {
                    hold: true,
                    transactions: Vec::new(),
                    poll_after_ms: Some(50),
                    max_hold_ms: Some(self.circ_timeout_ms),
                });
            }

            let transactions = build_transaction_payloads(&deliverables);
            info!(
                chain_id = %req.chain_id,
                tx_count = transactions.len(),
                deliverable_ids = ?deliverables.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
                "Delivering committed transactions to builder"
            );

            let mut state = self.state.write().await;
            for entry in &deliverables {
                if let Some(xt) = state.pending.get_mut(&entry.id) {
                    xt.delivered_chains.insert(req.chain_id, true);
                }
            }

            return Ok(BuilderPollResponse {
                hold: false,
                transactions,
                poll_after_ms: None,
                max_hold_ms: None,
            });
        }

        if has_blocking_undecided {
            Ok(BuilderPollResponse {
                hold: true,
                transactions: Vec::new(),
                poll_after_ms: Some(50),
                max_hold_ms: Some(self.circ_timeout_ms),
            })
        } else {
            Ok(BuilderPollResponse {
                hold: false,
                transactions: Vec::new(),
                poll_after_ms: None,
                max_hold_ms: None,
            })
        }
    }

    async fn build_put_inbox_transactions(
        &self,
        deliverables: &mut [DeliverableXt],
    ) -> Result<(), CoordinatorError> {
        let total_deps = deliverables.iter().map(|entry| entry.deps.len()).sum::<usize>();
        if total_deps == 0 {
            return Ok(());
        }

        let builder = self
            .put_inbox_builder
            .as_ref()
            .cloned()
            .ok_or(CoordinatorError::PutInboxNotConfigured)?;

        let nonce_builder = builder.clone();
        let mut next_nonce = self
            .nonce_manager
            .reserve(total_deps, move || {
                let builder = nonce_builder.clone();
                async move { builder.pending_nonce_at().await }
            })
            .await?;

        for entry in deliverables.iter_mut() {
            for dep in &entry.deps {
                let put_inbox_tx = builder
                    .build_put_inbox_tx_with_nonce(dep, next_nonce)
                    .await?;
                entry.put_inbox_txs.push(put_inbox_tx);
                next_nonce += 1;
            }
        }

        Ok(())
    }

    fn all_chains_ready(&self, xt: &crate::model::pending_xt::PendingXt) -> bool {
        xt.raw_txs
            .keys()
            .all(|chain_id| xt.chain_states.contains_key(chain_id))
    }
}

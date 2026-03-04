//! Builder poll handling and hold/ready response logic.

use alloy::primitives::Address;
use compose_primitives::{
    BuilderPollRequest, BuilderPollResponse, ChainId, ChainState, InstanceId, StateOverride,
};
use tracing::{debug, error, info};

use crate::coordinator::DefaultCoordinator;
use crate::model::ordering::xt_less;
use crate::pipeline::delivery::{build_transaction_payloads, deps_for_chain, DeliverableXt};
use compose_primitives_traits::CoordinatorError;

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

        let state_snapshot = ChainState {
            chain_id: req.chain_id,
            block_number: req.block_number,
            flashblock_index: req.flashblock_index,
            state_root: req.state_root,
            timestamp: req.timestamp,
            gas_limit: req.gas_limit,
            state_overrides: req.state_overrides.clone(),
        };

        // Gather everything needed under the write lock, then release before
        // making any async calls to avoid holding the lock across await points.
        // Chain state is tracked globally in `state.chain_states` rather than
        // per-XT, so no per-XT chain_states field is needed.
        struct UndecidedInfo {
            id: InstanceId,
            is_locked: bool,
            vote_sent: bool,
            raw_tx_chain_ids: Vec<ChainId>,
            known_chain_ids: Vec<ChainId>,
            has_local_chain_state: bool,
        }

        let (entries, undecided_info) = {
            let mut state = self.state.write().await;
            state
                .chain_states
                .insert(req.chain_id, state_snapshot.clone());

            let mut entries = Vec::<InstanceId>::new();
            for (id, xt) in &state.pending {
                if !xt.raw_txs.contains_key(&req.chain_id) {
                    continue;
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

            // Snapshot data needed to determine readiness — avoids holding the
            // write lock across the async is_publisher_connected call below.
            // Chain readiness is now checked against the global chain_states map
            // (one entry per chain that has ever polled), not per-XT copies.
            let known_chain_ids: Vec<ChainId> = state.chain_states.keys().copied().collect();
            let has_local_chain_state = state.chain_states.contains_key(&req.chain_id);
            let undecided_info = entries
                .iter()
                .find(|id| state.pending[*id].decision.is_none())
                .map(|undecided_id| {
                    let xt = &state.pending[undecided_id];
                    UndecidedInfo {
                        id: undecided_id.clone(),
                        is_locked: xt.locked_chains.contains(&req.chain_id),
                        vote_sent: xt.vote_sent,
                        raw_tx_chain_ids: xt.raw_txs.keys().copied().collect(),
                        known_chain_ids: known_chain_ids.clone(),
                        has_local_chain_state,
                    }
                });

            (entries, undecided_info)
        }; // write lock released here

        // Check publisher connection with no lock held.
        if let Some(info) = undecided_info {
            let is_connected = self.is_publisher_connected().await;
            let ready = if is_connected {
                info.raw_tx_chain_ids
                    .iter()
                    .all(|c| info.known_chain_ids.contains(c))
            } else {
                info.has_local_chain_state
            };

            if ready && !info.vote_sent && !info.is_locked {
                // Re-acquire write lock to guard the locked_chains insert against
                // concurrent polls, then spawn simulation outside the lock.
                let should_spawn = {
                    let mut state = self.state.write().await;
                    state.pending.get_mut(&info.id).is_some_and(|xt| {
                        if xt.vote_sent || xt.locked_chains.contains(&req.chain_id) {
                            false
                        } else {
                            xt.locked_chains.insert(req.chain_id);
                            true
                        }
                    })
                };

                if should_spawn {
                    let coordinator = self.clone();
                    let id = info.id;
                    self.task_tracker.spawn(async move {
                        coordinator.process_xt(&id).await;
                    });
                    if let Some(m) = &self.metrics {
                        m.builder_poll_hold_total.inc();
                    }
                    return Ok(BuilderPollResponse {
                        hold: true,
                        transactions: Vec::new(),
                        poll_after_ms: Some(50),
                        max_hold_ms: Some(self.circ_timeout_ms),
                    });
                }
            }
        }

        let mut deliverables = Vec::<DeliverableXt>::new();
        let mut has_blocking_undecided = false;

        {
            let mut state = self.state.write().await;
            for id in &entries {
                let Some(xt) = state.pending.get_mut(id) else {
                    continue;
                };

                if xt.delivered_chains.contains(&req.chain_id) {
                    continue;
                }

                match xt.decision {
                    None => {
                        has_blocking_undecided = true;
                        break;
                    }
                    Some(false) => {
                        xt.delivered_chains.insert(req.chain_id);
                        continue;
                    }
                    Some(true) => {}
                }

                let raw_txs = xt.raw_txs.get(&req.chain_id).cloned().unwrap_or_default();
                if raw_txs.is_empty() {
                    xt.delivered_chains.insert(req.chain_id);
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
        }

        debug!(
            chain_id = %req.chain_id,
            deliverables = deliverables.len(),
            has_blocking_undecided,
            "Builder poll deliverables collected"
        );

        if !deliverables.is_empty() {
            if let Err(e) = self
                .build_put_inbox_transactions(&mut deliverables, req.state_overrides.as_ref())
                .await
            {
                error!(chain_id = %req.chain_id, error = %e, "Failed to build putInbox transactions");
                if let Some(m) = &self.metrics {
                    m.builder_poll_hold_total.inc();
                }
                return Ok(BuilderPollResponse {
                    hold: true,
                    transactions: Vec::new(),
                    poll_after_ms: Some(50),
                    max_hold_ms: Some(self.circ_timeout_ms),
                });
            }

            let transactions = build_transaction_payloads(&deliverables);

            // Mark as delivered only after the response is fully built so
            // concurrent polls can still pick up the XT if this one fails.
            {
                let mut state = self.state.write().await;
                for entry in &deliverables {
                    if let Some(xt) = state.pending.get_mut(&entry.id) {
                        xt.delivered_chains.insert(req.chain_id);
                    }
                }
            }

            info!(
                chain_id = %req.chain_id,
                tx_count = transactions.len(),
                deliverable_ids = ?deliverables.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
                "Delivering committed transactions to builder"
            );

            if let Some(m) = &self.metrics {
                m.builder_poll_deliver_total.inc();
            }
            return Ok(BuilderPollResponse {
                hold: false,
                transactions,
                poll_after_ms: None,
                max_hold_ms: None,
            });
        }

        if has_blocking_undecided {
            if let Some(m) = &self.metrics {
                m.builder_poll_hold_total.inc();
            }
            Ok(BuilderPollResponse {
                hold: true,
                transactions: Vec::new(),
                poll_after_ms: Some(50),
                max_hold_ms: Some(self.circ_timeout_ms),
            })
        } else {
            if let Some(m) = &self.metrics {
                m.builder_poll_empty_total.inc();
            }
            Ok(BuilderPollResponse {
                hold: false,
                transactions: Vec::new(),
                poll_after_ms: None,
                max_hold_ms: None,
            })
        }
    }

    /// Build signed putInbox transactions for each dependency in the batch.
    ///
    /// When `state_overrides` is provided and contains the coordinator's nonce,
    /// that value is used instead of the chain RPC. This ensures nonces match
    /// the builder's in-progress state when multiple flashblocks deliver
    /// putInbox txs in the same block.
    async fn build_put_inbox_transactions(
        &self,
        deliverables: &mut [DeliverableXt],
        state_overrides: Option<&StateOverride>,
    ) -> Result<(), CoordinatorError> {
        let total_deps = deliverables
            .iter()
            .map(|entry| entry.deps.len())
            .sum::<usize>();
        if total_deps == 0 {
            return Ok(());
        }

        let builder = self
            .put_inbox_builder
            .as_ref()
            .cloned()
            .ok_or(CoordinatorError::PutInboxNotConfigured)?;

        let base_nonce =
            state_overrides.and_then(|o| coordinator_nonce_from_overrides(o, builder.signer_address()));

        let mut next_nonce = if let Some(base) = base_nonce {
            self.nonce_manager.resync(|| async { Ok(base) }).await?;
            self.nonce_manager
                .reserve(total_deps, || async { panic!("resync already set base") })
                .await?
        } else {
            let nonce_builder = builder.clone();
            self.nonce_manager
                .reserve(total_deps, move || {
                    let builder = nonce_builder.clone();
                    async move { builder.pending_nonce_at().await }
                })
                .await?
        };

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
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;
    use compose_primitives::{BuilderPollRequest, ChainId};

    use crate::coordinator::DefaultCoordinator;
    use crate::model::pending_xt::PendingXt;

    fn make_poll_req(chain_id: ChainId, flashblock_index: u64) -> BuilderPollRequest {
        BuilderPollRequest {
            chain_id,
            block_number: 100,
            flashblock_index,
            state_root: B256::ZERO,
            timestamp: 0,
            gas_limit: 30_000_000,
            state_overrides: None,
        }
    }

    #[tokio::test]
    async fn builder_poll_returns_empty_when_no_pending() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, None, 1000);

        let resp = coordinator
            .handle_builder_poll(&make_poll_req(ChainId(77777), 1))
            .await
            .unwrap();
        assert!(!resp.hold);
        assert!(resp.transactions.is_empty());
    }

    #[tokio::test]
    async fn builder_poll_returns_hold_when_undecided_xt_exists() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, None, 10_000);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xde, 0xad]]);
            xt.vote_sent = true; // already voted; lock not re-triggered
            state.pending.insert(xt.id.clone(), xt);
        }

        let resp = coordinator
            .handle_builder_poll(&make_poll_req(ChainId(77777), 1))
            .await
            .unwrap();
        assert!(resp.hold);
        assert!(resp.transactions.is_empty());
    }

    #[tokio::test]
    async fn builder_poll_skips_aborted_xts() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, None, 1000);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.decision = Some(false);
            xt.decided_at = Some(std::time::Instant::now());
            state.pending.insert(xt.id.clone(), xt);
        }

        let resp = coordinator
            .handle_builder_poll(&make_poll_req(ChainId(77777), 1))
            .await
            .unwrap();
        assert!(!resp.hold);
        assert!(resp.transactions.is_empty());
    }

    #[tokio::test]
    async fn builder_poll_returns_committed_txs() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, None, 1000);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            xt.decision = Some(true);
            xt.decided_at = Some(std::time::Instant::now());
            state.pending.insert(xt.id.clone(), xt);
        }

        let resp = coordinator
            .handle_builder_poll(&make_poll_req(ChainId(77777), 1))
            .await
            .unwrap();
        assert!(!resp.hold);
        assert_eq!(resp.transactions.len(), 1);
        assert_eq!(resp.transactions[0].instance_id, "xt-77777-1");
    }
}

fn coordinator_nonce_from_overrides(overrides: &StateOverride, signer: Address) -> Option<u64> {
    overrides.get(&signer).and_then(|acct| acct.nonce)
}
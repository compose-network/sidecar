//! Builder poll handling and hold/ready response logic.

use alloy_rpc_types_eth::state::StateOverride;
use compose_primitives::{BuilderPollRequest, BuilderPollResponse, ChainId, ChainState};
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

        // Parse the builder-provided JSON state overrides into the typed form at
        // the HTTP boundary. Parsing failures are treated as empty overrides.
        let typed_overrides: Option<StateOverride> = req
            .state_overrides
            .as_ref()
            .and_then(|v| serde_json::from_value::<StateOverride>(v.clone()).ok());

        let state_snapshot = ChainState {
            chain_id: req.chain_id,
            block_number: req.block_number,
            flashblock_index: req.flashblock_index,
            state_root: req.state_root,
            timestamp: req.timestamp,
            gas_limit: req.gas_limit,
            state_overrides: typed_overrides,
        };

        // Gather everything needed under the write lock, then release before
        // making any async calls to avoid holding the lock across await points.
        struct UndecidedInfo {
            id: String,
            is_locked: bool,
            vote_sent: bool,
            raw_tx_chain_ids: Vec<ChainId>,
            cs_chain_ids: Vec<ChainId>,
            has_local_chain_state: bool,
        }

        let (entries, undecided_info) = {
            let mut state = self.state.write().await;
            state.chain_states.insert(req.chain_id, state_snapshot.clone());

            let mut entries = Vec::<String>::new();
            for (id, xt) in &mut state.pending {
                if !xt.raw_txs.contains_key(&req.chain_id) {
                    continue;
                }
                // Update chain state snapshot if not yet locked for simulation.
                if xt.decision.is_none() && !xt.locked_chains.contains(&req.chain_id) {
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

            // Snapshot data needed to determine readiness — avoids holding the
            // write lock across the async is_publisher_connected call below.
            let undecided_info = entries
                .iter()
                .find(|id| state.pending[*id].decision.is_none())
                .map(|undecided_id| {
                    let xt = &state.pending[undecided_id];
                    UndecidedInfo {
                        id: undecided_id.clone(),
                        is_locked: xt.locked_chains.contains(&req.chain_id),
                        vote_sent: xt.vote_sent,
                        raw_tx_chain_ids: xt.raw_txs.keys().cloned().collect(),
                        cs_chain_ids: xt.chain_states.keys().cloned().collect(),
                        has_local_chain_state: xt.chain_states.contains_key(&req.chain_id),
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
                    .all(|c| info.cs_chain_ids.contains(c))
            } else {
                info.has_local_chain_state
            };

            if ready && !info.vote_sent && !info.is_locked {
                // Re-acquire write lock to guard the locked_chains insert against
                // concurrent polls, then spawn simulation outside the lock.
                let xt_opt = {
                    let mut state = self.state.write().await;
                    state.pending.get_mut(&info.id).and_then(|xt| {
                        if xt.vote_sent || xt.locked_chains.contains(&req.chain_id) {
                            None
                        } else {
                            xt.locked_chains.insert(req.chain_id);
                            Some(xt.clone())
                        }
                    })
                };

                if let Some(xt_clone) = xt_opt {
                    let coordinator = self.clone();
                    let id = info.id;
                    self.task_tracker.spawn(async move {
                        coordinator.process_xt(&id, &xt_clone).await;
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
                let xt = state
                    .pending
                    .get_mut(id)
                    .expect("entry id collected from pending state");

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
            if let Err(e) = self.build_put_inbox_transactions(&mut deliverables).await {
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
            info!(
                chain_id = %req.chain_id,
                tx_count = transactions.len(),
                deliverable_ids = ?deliverables.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
                "Delivering committed transactions to builder"
            );

            let mut state = self.state.write().await;
            for entry in &deliverables {
                if let Some(xt) = state.pending.get_mut(&entry.id) {
                    xt.delivered_chains.insert(req.chain_id);
                }
            }

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

}

#[cfg(test)]
mod tests {
    use compose_primitives::{BuilderPollRequest, ChainId};
    use alloy::primitives::B256;

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
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        let resp = coordinator
            .handle_builder_poll(&make_poll_req(ChainId(77777), 1))
            .await
            .unwrap();
        assert!(!resp.hold);
        assert!(resp.transactions.is_empty());
    }

    #[tokio::test]
    async fn builder_poll_returns_hold_when_undecided_xt_exists() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            None,
            10_000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xde, 0xad]]);
            xt.vote_sent = true; // already voted; lock not re-triggered
            state.pending.insert("xt-77777-1".to_string(), xt);
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
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.decision = Some(false);
            xt.decided_at = Some(std::time::Instant::now());
            state.pending.insert("xt-77777-1".to_string(), xt);
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
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            xt.decision = Some(true);
            xt.decided_at = Some(std::time::Instant::now());
            state.pending.insert("xt-77777-1".to_string(), xt);
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

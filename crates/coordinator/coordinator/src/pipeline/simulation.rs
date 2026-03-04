//! Simulation pipeline and vote emission flow.

use std::time::{Duration, Instant as StdInstant};

use compose_mailbox::matching::{contains_message, dep_key, matches_dependency};
use compose_mailbox::overrides::merge_overrides;
use compose_primitives::{ChainId, CrossRollupDependency, CrossRollupMessage, StateOverride};
use compose_proto::rollup_v2::MailboxMessage;
use tokio::time::{sleep_until, Instant};
use tracing::{debug, error, info, warn};

use crate::coordinator::DefaultCoordinator;
use crate::model::chain_overlay::ChainOverlay;

/// Canonical dedup key for a sent mailbox message.
fn mailbox_message_key(msg: &MailboxMessage) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        hex::encode(&msg.instance_id),
        msg.source_chain,
        msg.destination_chain,
        hex::encode(&msg.source),
        hex::encode(&msg.receiver),
        msg.label,
    )
}

impl DefaultCoordinator {
    /// Run the simulation pipeline for the local chain's portion of an XT.
    ///
    /// Simulates transactions sequentially, discovers mailbox dependencies,
    /// waits for CIRC messages, and sends a vote.
    pub(crate) async fn process_xt(&self, instance_id: &str) {
        info!(instance_id, chain_id = %self.chain_id, "Processing XT");

        // Capture everything we need from state in a single read lock. Chain
        // state is read from the global `state.chain_states` (updated on every
        // builder poll) rather than per-XT copies, which eliminates the
        // per-XT `chain_states` HashMap allocation.
        let (tx_bytes_list, mut current_overrides, sim_block_number, sim_flashblock_index) = {
            let state = self.state.read().await;
            match state.pending.get(instance_id) {
                Some(xt) => match xt.raw_txs.get(&self.chain_id) {
                    Some(txs) if !txs.is_empty() => {
                        let chain_state = state.chain_states.get(&self.chain_id);
                        let has_chain_state = chain_state.is_some();
                        let has_overrides = chain_state
                            .and_then(|cs| cs.state_overrides.as_ref())
                            .is_some();
                        debug!(
                            instance_id,
                            chain_id = %self.chain_id,
                            has_chain_state,
                            has_overrides,
                            "Simulation state check"
                        );

                        let block_number = chain_state.map(|cs| cs.block_number).unwrap_or(0);
                        let flashblock_index =
                            chain_state.map(|cs| cs.flashblock_index).unwrap_or(0);

                        // Start from the builder-provided overrides for this chain,
                        // then layer in any accumulated overlay from prior committed XTs
                        // in the same block/flashblock window.
                        let mut overrides = chain_state
                            .and_then(|cs| cs.state_overrides.clone())
                            .unwrap_or_default();

                        // Apply chain overlay from prior committed XTs.
                        if let Some(chain_overlay) = state.chain_overlay.get(&self.chain_id) {
                            if chain_overlay.matches(block_number, flashblock_index) {
                                merge_overrides(&mut overrides, &chain_overlay.overlay);
                            }
                        }

                        (txs.clone(), overrides, block_number, flashblock_index)
                    }
                    _ => {
                        warn!(instance_id, "No local transactions, rejecting");
                        drop(state);
                        let _ = self.send_vote(instance_id, false).await;
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
                xt.locked_chains.insert(self.chain_id);
            }
        }

        let simulator = match &self.simulator {
            Some(s) => s.clone(),
            None => {
                warn!("No simulator configured, voting yes without simulation");
                let _ = self.send_vote(instance_id, true).await;
                return;
            }
        };

        // Initialize local tracking once. These are refreshed from state only
        // after a dep-wait cycle, not on every simulation attempt.
        let (mut already_sent_msgs, mut fulfilled_deps) = {
            let state = self.state.read().await;
            match state.pending.get(instance_id) {
                Some(xt) => (xt.outbound_messages.clone(), xt.fulfilled_deps.clone()),
                None => return,
            }
        };

        // Simulate each transaction sequentially.
        for (tx_index, tx_bytes) in tx_bytes_list.iter().enumerate() {
            // Retry simulation until success or CIRC timeout. The
            // wait_for_dependencies deadline is the only bound on the loop.
            loop {
                let sim_start = StdInstant::now();
                let sim_result = simulator
                    .simulate_with_mailbox(
                        self.chain_id,
                        tx_bytes,
                        &current_overrides,
                        &already_sent_msgs,
                        &fulfilled_deps,
                    )
                    .await;
                if let Some(m) = &self.metrics {
                    m.simulation_duration_seconds
                        .observe(sim_start.elapsed().as_secs_f64());
                }
                match sim_result {
                    Ok(result) => {
                        current_overrides = self
                            .record_simulation_state(
                                instance_id,
                                &result,
                                &current_overrides,
                                sim_block_number,
                                sim_flashblock_index,
                            )
                            .await;

                        if !result.success && result.dependencies.is_empty() {
                            warn!(
                                instance_id,
                                tx_index,
                                error = ?result.error,
                                "Simulation returned failure with no dependencies"
                            );
                            let _ = self.send_vote(instance_id, false).await;
                            return;
                        }

                        if result.success {
                            // Extend local already_sent_msgs so the next TX in the
                            // sequence doesn't re-send the same outbound messages.
                            for msg in &result.outbound_messages {
                                if !contains_message(&already_sent_msgs, msg) {
                                    already_sent_msgs.push(msg.clone());
                                }
                            }
                            if let Err(e) = self
                                .dispatch_outbound_mailbox(instance_id, &result.outbound_messages)
                                .await
                            {
                                error!(instance_id, error = %e, "Failed to dispatch mailbox messages");
                                let _ = self.send_vote(instance_id, false).await;
                                return;
                            }
                            break;
                        }

                        info!(
                            instance_id,
                            tx_index,
                            dep_count = result.dependencies.len(),
                            "Simulation waiting for mailbox dependencies"
                        );

                        if !self
                            .wait_for_dependencies(instance_id, &result.dependencies)
                            .await
                        {
                            warn!(
                                instance_id,
                                tx_index, "Timed out waiting for mailbox dependencies"
                            );
                            let _ = self.send_vote(instance_id, false).await;
                            return;
                        }

                        // Deps were fulfilled: refresh locals from state once per
                        // dep-wait cycle instead of once per simulation attempt.
                        {
                            let state = self.state.read().await;
                            match state.pending.get(instance_id) {
                                Some(xt) => {
                                    already_sent_msgs = xt.outbound_messages.clone();
                                    fulfilled_deps = xt.fulfilled_deps.clone();
                                }
                                None => return,
                            }
                        }
                    }
                    Err(e) => {
                        error!(instance_id, error = %e, "Simulation failed");
                        let _ = self.send_vote(instance_id, false).await;
                        return;
                    }
                }
            }
        }

        let _ = self.send_vote(instance_id, true).await;
    }

    /// Record simulation results into XT state, update the chain overlay with
    /// the post-simulation overrides so subsequent XTs see the committed state,
    /// and return the merged overrides for the next simulation step.
    ///
    /// `block_number` and `flashblock_index` are the values captured at the
    /// start of `process_xt` so the overlay is tagged to the correct window
    /// regardless of any builder polls that arrive during simulation.
    async fn record_simulation_state(
        &self,
        instance_id: &str,
        result: &compose_primitives::SimulationResult,
        base_overrides: &StateOverride,
        block_number: u64,
        flashblock_index: u64,
    ) -> StateOverride {
        let mut state = self.state.write().await;
        let Some(xt) = state.pending.get_mut(instance_id) else {
            return base_overrides.clone();
        };

        let mut merged_overrides = base_overrides.clone();
        if let Some(ref result_overrides) = result.state_overrides {
            merge_overrides(&mut merged_overrides, result_overrides);
        }
        xt.state_overrides
            .insert(self.chain_id, merged_overrides.clone());

        for dep in &result.dependencies {
            let key = dep_key(dep);
            if xt.dep_keys.insert(key) {
                xt.dependencies.push(dep.clone());
            }
        }

        for msg in &result.outbound_messages {
            if !contains_message(&xt.outbound_messages, msg) {
                xt.outbound_messages.push(msg.clone());
            }
        }

        // Update the chain overlay for this block/flashblock window so that the
        // next XT simulated on this chain sees the accumulated post-simulation state.
        if result.success {
            let overlay = state
                .chain_overlay
                .entry(self.chain_id)
                .or_insert_with(|| ChainOverlay::new(block_number, flashblock_index));
            // Reset overlay when moving to a new block/flashblock window, or
            // when a reorg causes the block number to regress.
            if !overlay.matches(block_number, flashblock_index)
                || block_number < overlay.block_number
            {
                *overlay = ChainOverlay::new(block_number, flashblock_index);
            }
            merge_overrides(&mut overlay.overlay, &merged_overrides);
        }

        merged_overrides
    }

    /// Wait until at least one dependency is fulfilled or the CIRC timeout
    /// expires. Uses `Notify` to wake immediately when a mailbox message
    /// arrives, replacing the previous 50 ms busy-poll loop.
    async fn wait_for_dependencies(
        &self,
        instance_id: &str,
        deps: &[CrossRollupDependency],
    ) -> bool {
        let deadline = Instant::now() + Duration::from_millis(self.circ_timeout_ms);
        loop {
            let fulfilled = self
                .fulfill_dependencies_from_mailbox(instance_id, deps)
                .await;
            if fulfilled > 0 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            // Clone the Arc before dropping the lock so we can call .notified()
            // outside the critical section. This avoids missing a notification
            // that arrives between the dependency check above and the select below.
            let notify = {
                let state = self.state.read().await;
                state.mailbox_notify.clone()
            };
            tokio::select! {
                _ = notify.notified() => {}
                _ = sleep_until(deadline) => { return false; }
            }
        }
    }

    async fn fulfill_dependencies_from_mailbox(
        &self,
        instance_id: &str,
        deps: &[CrossRollupDependency],
    ) -> usize {
        let mut added = 0usize;

        let mut state = self.state.write().await;
        let Some(xt) = state.pending.get_mut(instance_id) else {
            return 0;
        };

        for dep in deps {
            let key = dep_key(dep);
            if xt.fulfilled_dep_keys.contains(&key) {
                continue;
            }

            if let Some(idx) = xt
                .pending_mailbox
                .iter()
                .position(|msg| matches_dependency(msg, dep))
            {
                let mailbox_msg = xt.pending_mailbox.remove(idx);
                let mut fulfilled = dep.clone();
                fulfilled.data = mailbox_msg.data.first().cloned();
                xt.fulfilled_dep_keys.insert(key);
                xt.fulfilled_deps.push(fulfilled);
                added += 1;
            }
        }

        if added > 0 {
            info!(
                instance_id,
                fulfilled = added,
                total_fulfilled = xt.fulfilled_deps.len(),
                "Fulfilled dependencies from mailbox messages"
            );
        }

        added
    }

    async fn dispatch_outbound_mailbox(
        &self,
        instance_id: &str,
        outbound_messages: &[CrossRollupMessage],
    ) -> Result<(), compose_primitives_traits::CoordinatorError> {
        if outbound_messages.is_empty() {
            return Ok(());
        }

        let Some(sender) = self.mailbox_sender.as_ref().cloned() else {
            warn!(
                instance_id,
                "Mailbox sender not configured, skipping outbound mailbox delivery"
            );
            return Ok(());
        };

        let mut to_send = Vec::<MailboxMessage>::new();
        {
            let mut state = self.state.write().await;
            let Some(xt) = state.pending.get_mut(instance_id) else {
                return Ok(());
            };

            for msg in outbound_messages {
                let session_id = msg
                    .session_id
                    .and_then(|id| u64::try_from(id).ok())
                    .unwrap_or_default();

                let mailbox_msg = MailboxMessage {
                    instance_id: xt.instance_id.clone(),
                    source_chain: msg.source_chain_id.0,
                    destination_chain: msg.dest_chain_id.0,
                    source: msg.sender.as_slice().to_vec(),
                    receiver: msg.receiver.as_slice().to_vec(),
                    label: msg.label.clone(),
                    data: vec![msg.data.clone()],
                    session_id,
                };

                let key = mailbox_message_key(&mailbox_msg);
                if xt.sent_mailbox_keys.insert(key) {
                    xt.sent_mailbox.push(mailbox_msg.clone());
                    to_send.push(mailbox_msg);
                }
            }
        }

        let sent_count = to_send.len();
        for msg in to_send {
            sender.send(ChainId(msg.destination_chain), &msg).await?;
        }
        if sent_count > 0 {
            if let Some(m) = &self.metrics {
                m.circ_messages_sent_total.inc_by(sent_count as u64);
            }
        }

        Ok(())
    }

    /// Send a vote for the given instance.
    pub(crate) async fn send_vote(
        &self,
        instance_id: &str,
        vote: bool,
    ) -> Result<(), compose_primitives_traits::CoordinatorError> {
        let standalone_mode = !self.is_publisher_connected().await;
        let mut decision_made: Option<(bool, usize, usize)> = None;

        let instance_bytes = {
            let mut state = self.state.write().await;
            let Some(xt) = state.pending.get_mut(instance_id) else {
                return Ok(());
            };

            // First local vote wins for the instance.
            if xt.local_vote.is_some() {
                debug!(
                    instance_id,
                    existing_vote = ?xt.local_vote,
                    duplicate_vote = vote,
                    "Local vote already recorded, ignoring duplicate"
                );
                return Ok(());
            }

            xt.simulated_at = Some(std::time::Instant::now());
            xt.vote_sent = true;
            xt.local_vote = Some(vote);
            xt.locked_chains.insert(self.chain_id);
            if standalone_mode {
                decision_made = self.maybe_make_standalone_decision(xt);
            }
            xt.instance_id.clone()
        };

        if let Some((decision, collected, expected)) = decision_made {
            info!(
                instance_id,
                decision,
                votes = collected,
                expected_votes = expected,
                "Made local decision (standalone mode)"
            );
            if let Some(m) = &self.metrics {
                m.xt_decision_latency_seconds.observe(
                    self.state
                        .read()
                        .await
                        .pending
                        .get(instance_id)
                        .map(|xt| xt.created_at.elapsed().as_secs_f64())
                        .unwrap_or(0.0),
                );
                m.xt_pending_count.dec();
            }
        }

        if !standalone_mode {
            if let Some(publisher) = &self.publisher {
                if let Err(e) = publisher.send_vote(&instance_bytes, vote).await {
                    error!(instance_id, error = %e, "Failed to send vote to publisher");
                    if let Some(m) = &self.metrics {
                        m.vote_send_failed_total.inc();
                    }
                } else {
                    info!(instance_id, vote, "Vote sent to publisher");
                }
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
                let metrics = self.metrics.clone();
                self.task_tracker.spawn(async move {
                    if let Err(e) = pc.send_vote_to_peers(&id, chain_id, vote).await {
                        error!(instance_id = %id, error = %e, "Failed to send vote to peers");
                        if let Some(m) = metrics {
                            m.vote_send_failed_total.inc();
                        }
                    }
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use compose_primitives::ChainId;

    use crate::coordinator::DefaultCoordinator;
    use crate::model::pending_xt::PendingXt;

    #[tokio::test]
    async fn send_vote_does_not_overwrite_existing_local_vote() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, None, 1000);

        {
            let mut state = coordinator.state.write().await;
            let xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-1", true).await.unwrap();
        coordinator.send_vote("xt-77777-1", false).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-1").unwrap();
        assert_eq!(xt.local_vote, Some(true));
    }

    #[tokio::test]
    async fn send_vote_decides_when_peer_vote_already_present() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, None, 1000);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-2".to_string(), b"xt-77777-2".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.peer_votes.insert(ChainId(88888), true);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-2", true).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-2").unwrap();
        assert_eq!(xt.local_vote, Some(true));
        assert_eq!(xt.decision, Some(true));
    }

    #[tokio::test]
    async fn send_vote_applies_existing_abort_peer_vote() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, None, 1000);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-3".to_string(), b"xt-77777-3".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.peer_votes.insert(ChainId(88888), false);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-3", true).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-3").unwrap();
        assert_eq!(xt.local_vote, Some(true));
        assert_eq!(xt.decision, Some(false));
    }

    use async_trait::async_trait;
    use compose_primitives::StateOverride;
    use compose_primitives::{CrossRollupDependency, CrossRollupMessage, SimulationResult};
    use compose_simulation::error::SimulationError;
    use compose_simulation::traits::Simulator;
    use std::sync::Arc;

    /// Simple stub simulator for testing: always returns success or always fails.
    struct StubSimulator {
        succeed: bool,
    }

    #[async_trait]
    impl Simulator for StubSimulator {
        async fn simulate(
            &self,
            _chain_id: ChainId,
            _tx: &[u8],
            _state_overrides: &StateOverride,
        ) -> Result<SimulationResult, SimulationError> {
            if self.succeed {
                Ok(SimulationResult {
                    success: true,
                    error: None,
                    state_overrides: None,
                    dependencies: Vec::new(),
                    outbound_messages: Vec::new(),
                })
            } else {
                Err(SimulationError::Failed("stub failure".to_string()))
            }
        }

        async fn simulate_with_mailbox(
            &self,
            chain_id: ChainId,
            tx: &[u8],
            state_overrides: &StateOverride,
            _already_sent_msgs: &[CrossRollupMessage],
            _fulfilled_deps: &[CrossRollupDependency],
        ) -> Result<SimulationResult, SimulationError> {
            self.simulate(chain_id, tx, state_overrides).await
        }
    }

    #[tokio::test]
    async fn process_xt_votes_true_on_success_with_no_deps() {
        let simulator = Arc::new(StubSimulator { succeed: true });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator),
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-10".to_string(), b"xt-77777-10".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-10").await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-10").unwrap();
        assert_eq!(xt.local_vote, Some(true));
    }

    #[tokio::test]
    async fn process_xt_votes_false_on_simulation_error() {
        let simulator = Arc::new(StubSimulator { succeed: false });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator),
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-11".to_string(), b"xt-77777-11".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xde, 0xad]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-11").await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-11").unwrap();
        assert_eq!(xt.local_vote, Some(false));
    }

    #[tokio::test]
    async fn process_xt_votes_false_when_no_local_txs() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, None, 1000);

        {
            let mut state = coordinator.state.write().await;
            // XT has transactions only for a different chain.
            let mut xt = PendingXt::new("xt-77777-12".to_string(), b"xt-77777-12".to_vec());
            xt.raw_txs.insert(ChainId(88888), vec![vec![0xff]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-12").await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-12").unwrap();
        assert_eq!(xt.local_vote, Some(false));
    }
}

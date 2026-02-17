//! Simulation pipeline and vote emission flow.

use std::time::Duration;

use compose_mailbox::matching::{contains_message, dep_key, matches_dependency};
use compose_primitives::{ChainId, CrossRollupDependency, CrossRollupMessage};
use compose_proto::rollup_v2::MailboxMessage;
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

use crate::coordinator::DefaultCoordinator;
use crate::model::pending_xt::PendingXt;

/// Maximum number of resimulation attempts per transaction.
const MAX_RESIMULATIONS: usize = 3;
const MAILBOX_POLL_INTERVAL_MS: u64 = 50;

fn same_mailbox_message(a: &MailboxMessage, b: &MailboxMessage) -> bool {
    a.instance_id == b.instance_id
        && a.source_chain == b.source_chain
        && a.destination_chain == b.destination_chain
        && a.source == b.source
        && a.receiver == b.receiver
        && a.label == b.label
        && a.data == b.data
        && a.session_id == b.session_id
}

impl DefaultCoordinator {
    /// Run the simulation pipeline for the local chain's portion of an XT.
    ///
    /// Simulates transactions sequentially, discovers mailbox dependencies,
    /// waits for CIRC messages, and sends a vote.
    pub(crate) async fn process_xt(&self, instance_id: &str, _xt: &PendingXt) {
        info!(instance_id, chain_id = %self.chain_id, "Processing XT");

        let (tx_bytes_list, mut current_overrides) = {
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
                let (already_sent_msgs, fulfilled_deps) = {
                    let state = self.state.read().await;
                    match state.pending.get(instance_id) {
                        Some(xt) => (xt.outbound_messages.clone(), xt.fulfilled_deps.clone()),
                        None => return,
                    }
                };

                match simulator
                    .simulate_with_mailbox(
                        self.chain_id,
                        tx_bytes,
                        &current_overrides,
                        &already_sent_msgs,
                        &fulfilled_deps,
                    )
                    .await
                {
                    Ok(result) => {
                        current_overrides = self
                            .record_simulation_state(instance_id, &result, &current_overrides)
                            .await;

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
                            if let Err(e) = self
                                .dispatch_outbound_mailbox(instance_id, &result.outbound_messages)
                                .await
                            {
                                error!(instance_id, error = %e, "Failed to dispatch mailbox messages");
                                let _ = self.send_vote(instance_id, &[], false).await;
                                return;
                            }
                            success = true;
                            break;
                        }

                        info!(
                            instance_id,
                            tx_index,
                            attempt = attempt + 1,
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

    async fn record_simulation_state(
        &self,
        instance_id: &str,
        result: &compose_primitives::SimulationResult,
        base_overrides: &serde_json::Value,
    ) -> serde_json::Value {
        let mut state = self.state.write().await;
        let Some(xt) = state.pending.get_mut(instance_id) else {
            return base_overrides.clone();
        };

        let merged_overrides = result
            .state_overrides
            .clone()
            .unwrap_or_else(|| base_overrides.clone());
        xt.state_overrides
            .insert(self.chain_id, merged_overrides.clone());

        for dep in &result.dependencies {
            if !xt
                .dependencies
                .iter()
                .any(|existing| dep_key(existing) == dep_key(dep))
            {
                xt.dependencies.push(dep.clone());
            }
        }

        for msg in &result.outbound_messages {
            if !contains_message(&xt.outbound_messages, msg) {
                xt.outbound_messages.push(msg.clone());
            }
        }

        merged_overrides
    }

    async fn wait_for_dependencies(
        &self,
        instance_id: &str,
        deps: &[CrossRollupDependency],
    ) -> bool {
        let deadline = Instant::now() + Duration::from_millis(self.circ_timeout_ms);
        loop {
            let fulfilled = self.fulfill_dependencies_from_mailbox(instance_id, deps).await;
            if fulfilled > 0 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            sleep(Duration::from_millis(MAILBOX_POLL_INTERVAL_MS)).await;
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
            if xt
                .fulfilled_deps
                .iter()
                .any(|existing| dep_key(existing) == dep_key(dep))
            {
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
    ) -> Result<(), crate::error::CoordinatorError> {
        if outbound_messages.is_empty() {
            return Ok(());
        }

        let Some(sender) = self.mailbox_sender.as_ref().cloned() else {
            warn!(instance_id, "Mailbox sender not configured, skipping outbound mailbox delivery");
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

                if !xt
                    .sent_mailbox
                    .iter()
                    .any(|sent| same_mailbox_message(sent, &mailbox_msg))
                {
                    xt.sent_mailbox.push(mailbox_msg.clone());
                    to_send.push(mailbox_msg);
                }
            }
        }

        for msg in to_send {
            sender.send(ChainId(msg.destination_chain), &msg).await?;
        }

        Ok(())
    }

    /// Send a vote for the given instance.
    pub(crate) async fn send_vote(
        &self,
        instance_id: &str,
        _instance_id_bytes: &[u8],
        vote: bool,
    ) -> Result<(), crate::error::CoordinatorError> {
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
            xt.locked_chains.insert(self.chain_id, true);
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
        }

        if !standalone_mode {
            if let Some(publisher) = &self.publisher {
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

#[cfg(test)]
mod tests {
    use compose_primitives::ChainId;

    use crate::coordinator::DefaultCoordinator;
    use crate::model::pending_xt::PendingXt;

    #[tokio::test]
    async fn send_vote_does_not_overwrite_existing_local_vote() {
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
            state.pending.insert(
                "xt-77777-1".to_string(),
                PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec()),
            );
        }

        coordinator.send_vote("xt-77777-1", b"xt-77777-1", true).await.unwrap();
        coordinator.send_vote("xt-77777-1", b"xt-77777-1", false).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-1").unwrap();
        assert_eq!(xt.local_vote, Some(true));
    }

    #[tokio::test]
    async fn send_vote_decides_when_peer_vote_already_present() {
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
            let mut xt = PendingXt::new("xt-77777-2".to_string(), b"xt-77777-2".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.peer_votes.insert(ChainId(88888), true);
            state.pending.insert("xt-77777-2".to_string(), xt);
        }

        coordinator.send_vote("xt-77777-2", b"xt-77777-2", true).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-2").unwrap();
        assert_eq!(xt.local_vote, Some(true));
        assert_eq!(xt.decision, Some(true));
    }

    #[tokio::test]
    async fn send_vote_applies_existing_abort_peer_vote() {
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
            let mut xt = PendingXt::new("xt-77777-3".to_string(), b"xt-77777-3".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.peer_votes.insert(ChainId(88888), false);
            state.pending.insert("xt-77777-3".to_string(), xt);
        }

        coordinator.send_vote("xt-77777-3", b"xt-77777-3", true).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-3").unwrap();
        assert_eq!(xt.local_vote, Some(true));
        assert_eq!(xt.decision, Some(false));
    }
}

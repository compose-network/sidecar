//! Handling for cross-chain transactions forwarded by peers.

use std::collections::HashMap;

use compose_primitives::{ChainId, SequenceNumber};
use tracing::{debug, info};

use crate::coordinator::DefaultCoordinator;
use crate::model::pending_xt::PendingXt;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Process an XT forwarded from another sidecar.
    pub async fn handle_forwarded_xt(
        &self,
        instance_id: &str,
        txs: HashMap<ChainId, Vec<Vec<u8>>>,
        origin_chain: ChainId,
        origin_seq: SequenceNumber,
    ) -> Result<(), CoordinatorError> {
        if instance_id.is_empty() {
            return Err(CoordinatorError::Other(
                "missing instance_id for forwarded XT".to_string(),
            ));
        }

        let clean_txs: HashMap<ChainId, Vec<Vec<u8>>> = txs
            .into_iter()
            .filter(|(_, chain_txs)| !chain_txs.is_empty())
            .collect();

        if clean_txs.is_empty() {
            return Err(CoordinatorError::NoTransactions);
        }

        let mut state = self.state.write().await;

        if state.pending.contains_key(instance_id) {
            return Ok(());
        }

        let has_local = clean_txs.contains_key(&self.chain_id);

        let mut xt = PendingXt::new(instance_id.to_string(), instance_id.as_bytes().to_vec());
        xt.raw_txs = clean_txs;
        xt.origin_chain = Some(origin_chain);
        xt.origin_seq = origin_seq;

        // Pre-lock so builder_poll won't spawn a duplicate simulation.
        if has_local {
            xt.locked_chains.insert(self.chain_id);
        }

        let raw_key = instance_id.as_bytes().to_vec();
        state.mailbox_index.insert(raw_key.clone(), xt.id.clone());
        state.pending.insert(xt.id.clone(), xt);

        // Drain messages that arrived before the XT was registered (race window).
        let buffered = state.drain_mailbox_buffer(&raw_key);
        if !buffered.is_empty() {
            if let Some(pending_xt) = state.pending.get_mut(instance_id) {
                debug!(
                    xt_id = instance_id,
                    count = buffered.len(),
                    "Attaching buffered mailbox messages to forwarded XT"
                );
                pending_xt.pending_mailbox.extend(buffered);
            }
        }

        info!(
            xt_id = instance_id,
            chains = state.pending[instance_id].raw_txs.len(),
            origin_chain = %origin_chain,
            origin_seq = origin_seq.0,
            "Received forwarded XT from peer"
        );

        // Release the write lock before spawning so process_xt can acquire it.
        drop(state);

        if has_local {
            let coordinator = self.clone();
            let id = instance_id.to_string();
            self.task_tracker.spawn(async move {
                coordinator.process_xt(&id).await;
            });
        }

        Ok(())
    }
}

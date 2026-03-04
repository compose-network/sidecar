//! Start-instance handling and sequencing validation.

use std::collections::HashMap;

use compose_primitives::{ChainId, InstanceId, PeriodId, SequenceNumber};
use compose_proto::conversions::chain_id_from_proto;
use compose_proto::rollup_v2::StartInstance;
use tracing::{debug, info, warn};

use crate::coordinator::DefaultCoordinator;
use crate::model::pending_xt::PendingXt;
use crate::pipeline::submission::xt_request_fingerprint;
use compose_primitives_traits::CoordinatorError;

/// Maximum number of pending XTs before new submissions are rejected.
const MAX_PENDING_XTS: usize = 100;

impl DefaultCoordinator {
    /// Process a new instance from the publisher. Validates the period and
    /// sequence, decodes transactions, and registers the XT.
    pub async fn handle_start_instance(&self, msg: &StartInstance) -> Result<(), CoordinatorError> {
        let instance_id = InstanceId::from_publisher_bytes(&msg.instance_id);
        let xt_request = msg
            .xt_request
            .as_ref()
            .ok_or_else(|| CoordinatorError::Other("missing xt_request".to_string()))?;

        // Check if local chain participates.
        let mut includes_local = false;
        for req in &xt_request.transaction_requests {
            let chain_id = chain_id_from_proto(req.chain_id);
            if chain_id == self.chain_id && !req.transaction.is_empty() {
                includes_local = true;
                break;
            }
        }

        // Decode transactions per chain.
        let mut raw_txs: HashMap<ChainId, Vec<Vec<u8>>> = HashMap::new();
        for req in &xt_request.transaction_requests {
            let chain_id = chain_id_from_proto(req.chain_id);
            for tx_bytes in &req.transaction {
                raw_txs.entry(chain_id).or_default().push(tx_bytes.clone());
            }
        }

        let mut state = self.state.write().await;

        if state.pending.contains_key(&instance_id) {
            return Err(CoordinatorError::InstanceAlreadyPending(
                instance_id.to_string(),
            ));
        }

        let undecided_count = state
            .pending
            .values()
            .filter(|xt| xt.decision.is_none())
            .count();
        if undecided_count >= MAX_PENDING_XTS {
            if let Some(m) = &self.metrics {
                m.xt_received_total.inc();
            }
            return Err(CoordinatorError::TooManyPendingInstances(MAX_PENDING_XTS));
        }

        if !state.period_initialized {
            drop(state);
            warn!(instance_id = %instance_id, "Period not initialized, rejecting");
            self.reject_start_instance(&instance_id, msg).await;
            return Ok(());
        }

        let msg_period = PeriodId(msg.period_id);
        if msg_period != state.current_period_id {
            drop(state);
            warn!(instance_id = %instance_id, "Period mismatch, rejecting");
            self.reject_start_instance(&instance_id, msg).await;
            return Ok(());
        }

        let msg_seq = SequenceNumber(msg.sequence_number);
        if msg_seq <= state.last_sequence_num {
            drop(state);
            warn!(instance_id = %instance_id, "Stale sequence, rejecting");
            self.reject_start_instance(&instance_id, msg).await;
            return Ok(());
        }

        if includes_local && state.has_active_instance(self.chain_id) {
            state.last_sequence_num = msg_seq;
            drop(state);
            warn!(instance_id = %instance_id, "Active instance blocking, rejecting");
            self.reject_start_instance(&instance_id, msg).await;
            return Ok(());
        }

        state.last_sequence_num = msg_seq;

        let mut xt = PendingXt::new(instance_id.to_string(), msg.instance_id.clone());
        xt.period_id = msg_period;
        xt.sequence_num = msg_seq;
        xt.raw_txs = raw_txs;

        // Pre-lock so builder_poll won't spawn a duplicate simulation.
        if includes_local {
            xt.locked_chains.insert(self.chain_id);
        }

        state
            .mailbox_index
            .insert(msg.instance_id.clone(), instance_id.clone());
        state.pending.insert(instance_id.clone(), xt);

        // If this XT was locally submitted, resolve the waiting caller with
        // the publisher-assigned instance_id.
        let fingerprint = xt_request_fingerprint(xt_request);
        if let Some(tx) = state.pending_submissions.remove(&fingerprint) {
            let _ = tx.send(instance_id.clone());
        }

        // Drain messages that arrived before the XT was registered (race window).
        let buffered = state.drain_mailbox_buffer(&msg.instance_id);
        if !buffered.is_empty() {
            if let Some(pending_xt) = state.pending.get_mut(&instance_id) {
                debug!(
                    instance_id = %instance_id,
                    count = buffered.len(),
                    "Attaching buffered mailbox messages to new instance"
                );
                pending_xt.pending_mailbox.extend(buffered);
            }
        }

        info!(
            instance_id = %instance_id,
            period_id = msg.period_id,
            sequence = msg.sequence_number,
            chains = state.pending[&instance_id].raw_txs.len(),
            "New instance started"
        );

        if let Some(m) = &self.metrics {
            m.xt_received_total.inc();
            m.xt_pending_count.inc();
        }

        // Release the write lock before spawning so process_xt can acquire it.
        drop(state);

        if includes_local {
            let coordinator = self.clone();
            let id = instance_id.clone();
            self.task_tracker.spawn(async move {
                coordinator.process_xt(&id).await;
            });
        }

        Ok(())
    }

    async fn reject_start_instance(&self, instance_id: &str, msg: &StartInstance) {
        warn!(
            instance_id,
            period_id = msg.period_id,
            sequence = msg.sequence_number,
            "Rejecting StartInstance"
        );
        // Send an abort vote for the rejected instance.
        let _ = self.send_vote(instance_id, false).await;
    }
}

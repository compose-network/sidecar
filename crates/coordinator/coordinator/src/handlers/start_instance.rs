//! Start-instance handling and sequencing validation.

use std::collections::HashMap;

use compose_primitives::{ChainId, PeriodId, SequenceNumber};
use compose_proto::conversions::chain_id_from_bytes;
use compose_proto::rollup_v2::StartInstance;
use tracing::{info, warn};

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;
use crate::model::pending_xt::PendingXt;

impl DefaultCoordinator {
    /// Process a new instance from the publisher. Validates the period and
    /// sequence, decodes transactions, and registers the XT.
    pub async fn handle_start_instance(&self, msg: &StartInstance) -> Result<(), CoordinatorError> {
        let instance_id = msg.instance_id_hex();
        let xt_request = msg
            .xt_request
            .as_ref()
            .ok_or_else(|| CoordinatorError::Other("missing xt_request".to_string()))?;

        // Check if local chain participates.
        let mut includes_local = false;
        for req in &xt_request.transactions {
            let chain_id = chain_id_from_bytes(&req.chain_id);
            if chain_id == self.chain_id && !req.transaction.is_empty() {
                includes_local = true;
                break;
            }
        }

        // Decode transactions per chain.
        let mut raw_txs: HashMap<ChainId, Vec<Vec<u8>>> = HashMap::new();
        for req in &xt_request.transactions {
            let chain_id = chain_id_from_bytes(&req.chain_id);
            for tx_bytes in &req.transaction {
                raw_txs.entry(chain_id).or_default().push(tx_bytes.clone());
            }
        }

        let mut state = self.state.write().await;

        if state.pending.contains_key(&instance_id) {
            return Err(CoordinatorError::InstanceAlreadyPending(instance_id));
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

        let mut xt = PendingXt::new(instance_id.clone(), msg.instance_id.clone());
        xt.period_id = msg_period;
        xt.sequence_num = msg_seq;
        xt.raw_txs = raw_txs;

        state.pending.insert(instance_id.clone(), xt);

        info!(
            instance_id = %instance_id,
            period_id = msg.period_id,
            sequence = msg.sequence_number,
            chains = state.pending[&instance_id].raw_txs.len(),
            "New instance started"
        );

        if let Some(m) = &self.metrics {
            m.xt_received_total.inc();
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

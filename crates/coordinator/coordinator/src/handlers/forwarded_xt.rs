//! Handling for cross-chain transactions forwarded by peers.

use std::collections::HashMap;

use compose_primitives::{ChainId, SequenceNumber};
use tracing::info;

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;
use crate::model::pending_xt::PendingXt;

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

        let mut xt = PendingXt::new(instance_id.to_string(), instance_id.as_bytes().to_vec());
        xt.raw_txs = clean_txs;
        xt.origin_chain = Some(origin_chain);
        xt.origin_seq = origin_seq;

        state.pending.insert(instance_id.to_string(), xt);

        info!(
            xt_id = instance_id,
            chains = state.pending[instance_id].raw_txs.len(),
            origin_chain = %origin_chain,
            origin_seq = origin_seq.0,
            "Received forwarded XT from peer"
        );

        Ok(())
    }
}

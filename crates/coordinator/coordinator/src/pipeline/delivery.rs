//! Helpers for turning committed XTs into builder payloads.

use compose_primitives::{ChainId, CrossRollupDependency, TransactionPayload};

/// A committed XT ready to be delivered to the builder.
#[derive(Debug)]
pub struct DeliverableXt {
    pub id: String,
    pub put_inbox_txs: Vec<Vec<u8>>,
    pub raw_txs: Vec<Vec<u8>>,
    pub deps: Vec<CrossRollupDependency>,
}

/// Filter dependencies to only those targeting the given chain.
pub fn deps_for_chain(
    deps: &[CrossRollupDependency],
    chain_id: ChainId,
) -> Vec<CrossRollupDependency> {
    deps.iter()
        .filter(|dep| dep.dest_chain_id == chain_id)
        .cloned()
        .collect()
}

/// Build transaction payloads for the builder from deliverable XTs.
///
/// This includes both the committed user transactions and any `putInbox`
/// transactions for fulfilled CIRC dependencies.
pub fn build_transaction_payloads(deliverable: &[DeliverableXt]) -> Vec<TransactionPayload> {
    let mut payloads = Vec::new();

    for entry in deliverable {
        for put_inbox_tx in &entry.put_inbox_txs {
            payloads.push(TransactionPayload {
                raw: format!("0x{}", hex::encode(put_inbox_tx)),
                required: true,
                instance_id: entry.id.clone(),
            });
        }

        for raw_tx in &entry.raw_txs {
            payloads.push(TransactionPayload {
                raw: format!("0x{}", hex::encode(raw_tx)),
                required: true,
                instance_id: entry.id.clone(),
            });
        }
    }

    payloads
}

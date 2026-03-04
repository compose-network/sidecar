//! Peer request payload types for XT forwarding and voting.

use compose_primitives::{ChainId, SequenceNumber};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Request to forward a cross-chain transaction to a peer sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XtForwardRequest {
    pub instance_id: String,
    pub transactions: HashMap<u64, Vec<String>>,
    pub origin_chain: u64,
    pub origin_seq: u64,
}

impl XtForwardRequest {
    pub fn new(
        instance_id: String,
        txs: &HashMap<ChainId, Vec<Vec<u8>>>,
        origin_chain: ChainId,
        origin_seq: SequenceNumber,
    ) -> Self {
        let transactions = txs
            .iter()
            .map(|(chain_id, chain_txs)| {
                let hex_txs = chain_txs.iter().map(hex::encode).collect();
                (chain_id.0, hex_txs)
            })
            .collect();

        Self {
            instance_id,
            transactions,
            origin_chain: origin_chain.0,
            origin_seq: origin_seq.0,
        }
    }

    /// Decode the transactions back to raw bytes.
    pub fn decode_transactions(&self) -> Result<HashMap<ChainId, Vec<Vec<u8>>>, hex::FromHexError> {
        self.transactions
            .iter()
            .map(|(chain_id, hex_txs)| {
                let txs: Result<Vec<Vec<u8>>, _> = hex_txs.iter().map(hex::decode).collect();
                Ok((ChainId(*chain_id), txs?))
            })
            .collect()
    }
}

/// Request to send a vote to a peer sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub instance_id: String,
    pub chain_id: u64,
    pub vote: bool,
}

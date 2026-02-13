//! Submission helpers for building and fingerprinting XT requests.

use std::collections::HashMap;

use compose_primitives::ChainId;
use compose_proto::conversions::chain_id_to_bytes;
use compose_proto::rollup_v2::{TransactionRequest, XtRequest};
use prost::Message;
use sha2::{Digest, Sha256};

/// Build a protobuf `XtRequest` from raw transactions keyed by chain.
pub fn build_xt_request(txs: &HashMap<ChainId, Vec<Vec<u8>>>) -> XtRequest {
    if txs.is_empty() {
        return XtRequest {
            transactions: Vec::new(),
        };
    }

    let mut chain_ids: Vec<ChainId> = txs.keys().copied().collect();
    chain_ids.sort();

    let transactions = chain_ids
        .iter()
        .filter_map(|chain_id| {
            let chain_txs = txs.get(chain_id)?;
            if chain_txs.is_empty() {
                return None;
            }
            Some(TransactionRequest {
                chain_id: chain_id_to_bytes(*chain_id),
                transaction: chain_txs.clone(),
            })
        })
        .collect();

    XtRequest { transactions }
}

/// Compute a fingerprint for an `XtRequest` for deduplication.
pub fn xt_request_fingerprint(req: &XtRequest) -> String {
    let data = req.encode_to_vec();
    let hash = Sha256::digest(&data);
    hex::encode(&hash[..16])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_xt_request_sorted() {
        let mut txs = HashMap::new();
        txs.insert(ChainId(902), vec![vec![2]]);
        txs.insert(ChainId(901), vec![vec![1]]);

        let req = build_xt_request(&txs);
        assert_eq!(req.transactions.len(), 2);
        // First entry should be chain 901.
        assert_eq!(
            compose_proto::conversions::chain_id_from_bytes(&req.transactions[0].chain_id),
            ChainId(901)
        );
    }

    #[test]
    fn fingerprint_deterministic() {
        let mut txs = HashMap::new();
        txs.insert(ChainId(901), vec![vec![1, 2, 3]]);

        let req = build_xt_request(&txs);
        let fp1 = xt_request_fingerprint(&req);
        let fp2 = xt_request_fingerprint(&req);
        assert_eq!(fp1, fp2);
        assert_eq!(fp1.len(), 32); // 16 bytes as hex
    }
}

//! Conversions between protobuf fields and shared primitive types.

use compose_primitives::{ChainId, SequenceNumber};

/// Extract a `ChainId` from a protobuf `uint64 chain_id` field.
pub fn chain_id_from_proto(val: u64) -> ChainId {
    ChainId(val)
}

/// Encode a `ChainId` for a protobuf `uint64 chain_id` field.
pub fn chain_id_to_proto(id: ChainId) -> u64 {
    id.0
}

/// Extract a sequence number from a protobuf u64 field.
pub fn sequence_number_from_proto(val: u64) -> SequenceNumber {
    SequenceNumber(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_id_round_trip() {
        let original = ChainId(901);
        let proto = chain_id_to_proto(original);
        let decoded = chain_id_from_proto(proto);
        assert_eq!(original, decoded);
    }
}

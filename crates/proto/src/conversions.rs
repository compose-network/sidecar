//! Conversions between protobuf fields and shared primitive types.

use compose_primitives::{ChainId, SequenceNumber, XtId};

use crate::rollup_v2;

impl From<&rollup_v2::XtId> for XtId {
    fn from(proto: &rollup_v2::XtId) -> Self {
        XtId::from_bytes(&proto.hash)
    }
}

impl From<&XtId> for rollup_v2::XtId {
    fn from(id: &XtId) -> Self {
        rollup_v2::XtId {
            hash: id.as_bytes().to_vec(),
        }
    }
}

/// Extract a `ChainId` from a big-endian byte slice (as used in protobuf
/// `bytes chain_id` fields).
pub fn chain_id_from_bytes(bytes: &[u8]) -> ChainId {
    let mut buf = [0u8; 8];
    let start = 8usize.saturating_sub(bytes.len());
    let copy_len = bytes.len().min(8);
    buf[start..start + copy_len].copy_from_slice(&bytes[..copy_len]);
    ChainId(u64::from_be_bytes(buf))
}

/// Encode a `ChainId` as big-endian bytes (minimum encoding).
pub fn chain_id_to_bytes(id: ChainId) -> Vec<u8> {
    let val = id.0;
    if val == 0 {
        return vec![0];
    }
    let bytes = val.to_be_bytes();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bytes[start..].to_vec()
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
        let bytes = chain_id_to_bytes(original);
        let decoded = chain_id_from_bytes(&bytes);
        assert_eq!(original, decoded);
    }

    #[test]
    fn chain_id_from_large_bytes() {
        // Chain ID as 8-byte big-endian
        let bytes = 42069u64.to_be_bytes();
        let id = chain_id_from_bytes(&bytes);
        assert_eq!(id, ChainId(42069));
    }

    #[test]
    fn xt_id_conversion() {
        let id = XtId::from_data(b"hello");
        let proto: rollup_v2::XtId = (&id).into();
        let back: XtId = (&proto).into();
        assert_eq!(id, back);
    }
}

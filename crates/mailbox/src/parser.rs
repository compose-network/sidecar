//! Mailbox trace parsing from call tracer output.

use alloy::primitives::{Address, U256};
use compose_primitives::ChainId;
use serde_json::Value;
use tracing::debug;

use crate::types::{MailboxCall, MailboxCallType, SimulationState};

/// Function selector for `write(uint256,address,uint256,bytes,bytes)`.
const WRITE_SELECTOR: &str = "0xcf80ca9a";

/// Function selector for `read(uint256,address,uint256,bytes)`.
const READ_SELECTOR: &str = "0xe8c7e15f";

/// Parse mailbox read/write calls from a geth `callTracer` output.
///
/// Recursively walks the call tree, identifying calls to the mailbox contract
/// by matching function selectors for `write` and `read`.
pub fn parse_call_trace(
    trace: &Value,
    mailbox_address: Address,
    local_chain_id: ChainId,
) -> SimulationState {
    let mut state = SimulationState::default();
    walk_trace(trace, mailbox_address, local_chain_id, &mut state);
    state
}

fn walk_trace(
    node: &Value,
    mailbox_address: Address,
    local_chain_id: ChainId,
    state: &mut SimulationState,
) {
    let from_str = node.get("from").and_then(|v| v.as_str()).unwrap_or("");
    let to_str = node.get("to").and_then(|v| v.as_str()).unwrap_or("");
    let input = node.get("input").and_then(|v| v.as_str()).unwrap_or("");
    let from_addr = from_str.parse::<Address>().ok();

    // Check if this call targets the mailbox contract.
    if let Ok(to_addr) = to_str.parse::<Address>() {
        if to_addr == mailbox_address && input.len() >= 10 {
            let selector = &input[..10];

            if selector == WRITE_SELECTOR {
                if let Some(caller) = from_addr {
                    if let Some(call) = decode_write(input, caller, local_chain_id) {
                        debug!(label = %call.label, "Parsed mailbox write call");
                        state.writes.push(call);
                    }
                }
            } else if selector == READ_SELECTOR {
                if let Some(caller) = from_addr {
                    if let Some(call) = decode_read(input, caller, local_chain_id) {
                        debug!(label = %call.label, "Parsed mailbox read call");
                        state.reads.push(call);
                    }
                }
            }
        }
    }

    // Recurse into child calls.
    if let Some(calls) = node.get("calls").and_then(|v| v.as_array()) {
        for child in calls {
            walk_trace(child, mailbox_address, local_chain_id, state);
        }
    }
}

/// Decode a `write(uint256,address,uint256,bytes,bytes)` call.
fn decode_write(input: &str, caller: Address, local_chain_id: ChainId) -> Option<MailboxCall> {
    let data = hex::decode(input.trim_start_matches("0x")).ok()?;
    if data.len() < 4 + 5 * 32 {
        return None;
    }
    let params = &data[4..];

    let dest_chain = U256::from_be_slice(&params[0..32]);
    let receiver = Address::from_slice(&params[44..64]);
    let session_id = U256::from_be_slice(&params[64..96]);

    let label_offset = U256::from_be_slice(&params[96..128])
        .try_into()
        .unwrap_or(0usize);
    let label = decode_bytes_param(params, label_offset)?;

    let data_offset = U256::from_be_slice(&params[128..160])
        .try_into()
        .unwrap_or(0usize);
    let call_data = decode_bytes_param(params, data_offset)?;

    Some(MailboxCall {
        call_type: MailboxCallType::Write,
        source_chain: local_chain_id,
        dest_chain: ChainId(dest_chain.try_into().unwrap_or(0)),
        sender: caller,
        receiver,
        label: String::from_utf8_lossy(&label).to_string(),
        data: call_data,
        session_id: Some(session_id),
    })
}

/// Decode a `read(uint256,address,uint256,bytes)` call.
fn decode_read(input: &str, caller: Address, local_chain_id: ChainId) -> Option<MailboxCall> {
    let data = hex::decode(input.trim_start_matches("0x")).ok()?;
    if data.len() < 4 + 4 * 32 {
        return None;
    }
    let params = &data[4..];

    let source_chain = U256::from_be_slice(&params[0..32]);
    let sender = Address::from_slice(&params[44..64]);
    let session_id = U256::from_be_slice(&params[64..96]);

    let label_offset = U256::from_be_slice(&params[96..128])
        .try_into()
        .unwrap_or(0usize);
    let label = decode_bytes_param(params, label_offset)?;

    Some(MailboxCall {
        call_type: MailboxCallType::Read,
        source_chain: ChainId(source_chain.try_into().unwrap_or(0)),
        dest_chain: local_chain_id,
        sender,
        receiver: caller,
        label: String::from_utf8_lossy(&label).to_string(),
        data: Vec::new(),
        session_id: Some(session_id),
    })
}

/// Decode a dynamic `bytes` parameter from ABI-encoded data.
fn decode_bytes_param(params: &[u8], offset: usize) -> Option<Vec<u8>> {
    if offset + 32 > params.len() {
        return None;
    }
    let len: usize = U256::from_be_slice(&params[offset..offset + 32])
        .try_into()
        .ok()?;
    let start = offset + 32;
    if start + len > params.len() {
        return None;
    }
    Some(params[start..start + len].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_write_call_trace() {
        let mailbox = "0xe5d5d610fb9767df117f4076444b45404201a097"
            .parse::<Address>()
            .unwrap();
        let caller = "0xf5fe1b951c5cdf2d4299f8e63444ff621cd2fed9"
            .parse::<Address>()
            .unwrap();
        let trace = json!({
            "from": format!("{caller:#x}"),
            "to": format!("{mailbox:#x}"),
            "input": "0xcf80ca9a0000000000000000000000000000000000000000000000000000000000015b38000000000000000000000000f5fe1b951c5cdf2d4299f8e63444ff621cd2fed902225347b4e7ad5a193d489b7170fd8530c06c3426e337fe1cfbfbd9ae4e7a2800000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000e0000000000000000000000000000000000000000000000000000000000000000453454e440000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800000000000000000000000004bcf3d44f2531497e82be4556f380b0a414aa9ce0000000000000000000000004bcf3d44f2531497e82be4556f380b0a414aa9ce00000000000000000000000047c286e684645f1ec602928707084edb241c57c70000000000000000000000000000000000000000000000001bc16d674ec80000"
        });

        let parsed = parse_call_trace(&trace, mailbox, ChainId(77777));
        assert_eq!(parsed.reads.len(), 0);
        assert_eq!(parsed.writes.len(), 1);
        assert_eq!(parsed.writes[0].source_chain, ChainId(77777));
        assert_eq!(parsed.writes[0].dest_chain, ChainId(88888));
        assert_eq!(parsed.writes[0].sender, caller);
        assert_eq!(parsed.writes[0].receiver, caller);
        assert_eq!(parsed.writes[0].label, "SEND");
    }

    #[test]
    fn parses_read_call_trace() {
        let mailbox = "0xe5d5d610fb9767df117f4076444b45404201a097"
            .parse::<Address>()
            .unwrap();
        let caller = "0xf5fe1b951c5cdf2d4299f8e63444ff621cd2fed9"
            .parse::<Address>()
            .unwrap();
        let trace = json!({
            "from": format!("{caller:#x}"),
            "to": format!("{mailbox:#x}"),
            "input": "0xe8c7e15f0000000000000000000000000000000000000000000000000000000000012fd1000000000000000000000000f5fe1b951c5cdf2d4299f8e63444ff621cd2fed902225347b4e7ad5a193d489b7170fd8530c06c3426e337fe1cfbfbd9ae4e7a280000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000000000000000453454e4400000000000000000000000000000000000000000000000000000000"
        });

        let parsed = parse_call_trace(&trace, mailbox, ChainId(88888));
        assert_eq!(parsed.writes.len(), 0);
        assert_eq!(parsed.reads.len(), 1);
        assert_eq!(parsed.reads[0].source_chain, ChainId(77777));
        assert_eq!(parsed.reads[0].dest_chain, ChainId(88888));
        assert_eq!(parsed.reads[0].sender, caller);
        assert_eq!(parsed.reads[0].receiver, caller);
        assert_eq!(parsed.reads[0].label, "SEND");
    }
}

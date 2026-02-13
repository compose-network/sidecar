//! Mailbox trace parsing from call tracer output.

use alloy::primitives::{Address, U256};
use compose_primitives::ChainId;
use serde_json::Value;
use tracing::debug;

use crate::types::{MailboxCall, MailboxCallType, SimulationState};

/// Function selector for `putInbox(uint256,address,address,uint256,bytes,bytes)`.
const PUT_INBOX_SELECTOR: &str = "0x7e41dc5e";

/// Function selector for `getInbox(uint256,address,address,uint256,bytes)`.
const GET_INBOX_SELECTOR: &str = "0x52d506e0";

/// Parse mailbox read/write calls from a geth `callTracer` output.
///
/// Recursively walks the call tree, identifying calls to the mailbox contract
/// by matching function selectors for `putInbox` and `getInbox`.
pub fn parse_call_trace(trace: &Value, mailbox_address: Address) -> SimulationState {
    let mut state = SimulationState::default();
    walk_trace(trace, mailbox_address, &mut state);
    state
}

fn walk_trace(node: &Value, mailbox_address: Address, state: &mut SimulationState) {
    let to_str = node.get("to").and_then(|v| v.as_str()).unwrap_or("");
    let input = node.get("input").and_then(|v| v.as_str()).unwrap_or("");

    // Check if this call targets the mailbox contract.
    if let Ok(to_addr) = to_str.parse::<Address>() {
        if to_addr == mailbox_address && input.len() >= 10 {
            let selector = &input[..10];

            if selector == PUT_INBOX_SELECTOR {
                if let Some(call) = decode_put_inbox(input) {
                    debug!(label = %call.label, "Parsed putInbox call");
                    state.writes.push(call);
                }
            } else if selector == GET_INBOX_SELECTOR {
                if let Some(call) = decode_get_inbox(input) {
                    debug!(label = %call.label, "Parsed getInbox call");
                    state.reads.push(call);
                }
            }
        }
    }

    // Recurse into child calls.
    if let Some(calls) = node.get("calls").and_then(|v| v.as_array()) {
        for child in calls {
            walk_trace(child, mailbox_address, state);
        }
    }
}

/// Decode a `putInbox(uint256,address,address,uint256,bytes,bytes)` call.
fn decode_put_inbox(input: &str) -> Option<MailboxCall> {
    let data = hex::decode(input.trim_start_matches("0x")).ok()?;
    if data.len() < 4 + 6 * 32 {
        return None;
    }
    let params = &data[4..];

    let source_chain = U256::from_be_slice(&params[0..32]);
    let sender = Address::from_slice(&params[44..64]);
    let receiver = Address::from_slice(&params[76..96]);
    let session_id = U256::from_be_slice(&params[96..128]);

    // label offset and length
    let label_offset = U256::from_be_slice(&params[128..160])
        .try_into()
        .unwrap_or(0usize);
    let label = decode_bytes_param(params, label_offset)?;

    // data offset and length
    let data_offset = U256::from_be_slice(&params[160..192])
        .try_into()
        .unwrap_or(0usize);
    let call_data = decode_bytes_param(params, data_offset)?;

    Some(MailboxCall {
        call_type: MailboxCallType::Write,
        source_chain: ChainId(source_chain.try_into().unwrap_or(0)),
        dest_chain: ChainId(0), // Filled by caller from context.
        sender,
        receiver,
        label: String::from_utf8_lossy(&label).to_string(),
        data: call_data,
        session_id: Some(session_id.try_into().unwrap_or(0)),
    })
}

/// Decode a `getInbox(uint256,address,address,uint256,bytes)` call.
fn decode_get_inbox(input: &str) -> Option<MailboxCall> {
    let data = hex::decode(input.trim_start_matches("0x")).ok()?;
    if data.len() < 4 + 5 * 32 {
        return None;
    }
    let params = &data[4..];

    let source_chain = U256::from_be_slice(&params[0..32]);
    let sender = Address::from_slice(&params[44..64]);
    let receiver = Address::from_slice(&params[76..96]);
    let session_id = U256::from_be_slice(&params[96..128]);

    let label_offset = U256::from_be_slice(&params[128..160])
        .try_into()
        .unwrap_or(0usize);
    let label = decode_bytes_param(params, label_offset)?;

    Some(MailboxCall {
        call_type: MailboxCallType::Read,
        source_chain: ChainId(source_chain.try_into().unwrap_or(0)),
        dest_chain: ChainId(0),
        sender,
        receiver,
        label: String::from_utf8_lossy(&label).to_string(),
        data: Vec::new(),
        session_id: Some(session_id.try_into().unwrap_or(0)),
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

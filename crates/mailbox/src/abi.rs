//! Mailbox ABI encoding utilities.

use alloy::primitives::{Address, U256};
use alloy::sol;
use alloy::sol_types::SolCall;

use crate::error::MailboxError;

// Generate the ABI bindings for the Mailbox contract's putInbox function.
sol! {
    function putInbox(
        uint256 chainMessageSender,
        address sender,
        address receiver,
        uint256 sessionId,
        bytes label,
        bytes data
    );
}

/// Encode a `putInbox` call for the Mailbox contract.
pub fn encode_put_inbox(
    source_chain_id: u64,
    sender: Address,
    receiver: Address,
    session_id: U256,
    label: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, MailboxError> {
    let call = putInboxCall {
        chainMessageSender: U256::from(source_chain_id),
        sender,
        receiver,
        sessionId: session_id,
        label: label.to_vec().into(),
        data: data.to_vec().into(),
    };
    Ok(call.abi_encode())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_put_inbox_produces_valid_calldata() {
        let data = encode_put_inbox(
            901,
            Address::ZERO,
            Address::ZERO,
            U256::ZERO,
            b"test",
            b"hello",
        )
        .unwrap();
        // Function selector (4 bytes) + encoded params
        assert!(data.len() > 4);
    }
}

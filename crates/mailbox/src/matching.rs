//! Matching helpers between dependencies and mailbox messages.

use compose_primitives::{ChainId, CrossRollupDependency};
use compose_proto::rollup_v2::MailboxMessage;

/// Check whether a mailbox message satisfies a cross-rollup dependency.
///
/// A message matches if source chain, destination chain, and label all agree.
pub fn matches_dependency(msg: &MailboxMessage, dep: &CrossRollupDependency) -> bool {
    if ChainId(msg.source_chain) != dep.source_chain_id {
        return false;
    }
    if ChainId(msg.destination_chain) != dep.dest_chain_id {
        return false;
    }

    let label_bytes = dep.label.as_slice();
    if msg.label.as_bytes() != label_bytes {
        return false;
    }

    true
}

/// Generate a deduplication key for a dependency.
pub fn dep_key(dep: &CrossRollupDependency) -> String {
    format!(
        "{}:{}:{}",
        dep.source_chain_id,
        dep.dest_chain_id,
        hex::encode(&dep.label)
    )
}

/// Check if a message list already contains an equivalent outbound message.
pub fn contains_message(
    msgs: &[compose_primitives::CrossRollupMessage],
    msg: &compose_primitives::CrossRollupMessage,
) -> bool {
    msgs.iter().any(|m| {
        m.source_chain_id == msg.source_chain_id
            && m.dest_chain_id == msg.dest_chain_id
            && m.label == msg.label
            && m.sender == msg.sender
            && m.receiver == msg.receiver
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;

    #[test]
    fn matching_message() {
        let msg = MailboxMessage {
            source_chain: 901,
            destination_chain: 902,
            label: "transfer".to_string(),
            ..Default::default()
        };
        let dep = CrossRollupDependency {
            source_chain_id: ChainId(901),
            dest_chain_id: ChainId(902),
            sender: Address::ZERO,
            receiver: Address::ZERO,
            label: b"transfer".to_vec(),
            data: None,
            session_id: None,
        };
        assert!(matches_dependency(&msg, &dep));
    }

    #[test]
    fn non_matching_chain() {
        let msg = MailboxMessage {
            source_chain: 901,
            destination_chain: 903,
            label: "transfer".to_string(),
            ..Default::default()
        };
        let dep = CrossRollupDependency {
            source_chain_id: ChainId(901),
            dest_chain_id: ChainId(902),
            sender: Address::ZERO,
            receiver: Address::ZERO,
            label: b"transfer".to_vec(),
            data: None,
            session_id: None,
        };
        assert!(!matches_dependency(&msg, &dep));
    }
}

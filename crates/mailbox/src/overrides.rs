//! State override merge helpers for mailbox interactions.

use alloy::primitives::{keccak256, map::FbBuildHasher, Address, B256, U256};
use alloy_rpc_types_eth::state::{AccountOverride, StateOverride};
use compose_primitives::{ChainId, CrossRollupDependency};
use std::collections::HashMap;

const INBOX_MAPPING_SLOT: u64 = 4;
const CREATED_KEYS_MAPPING_SLOT: u64 = 6;

/// Alloy's state-diff map type: B256 keys with a fixed-bytes hasher.
type SlotMap = HashMap<B256, B256, FbBuildHasher<32>>;

/// Merge `overlay` into `base`, with `overlay` taking precedence per address.
///
/// For `state` vs `stateDiff`:
/// - An overlay `state` (full replacement) replaces any existing `state` or
///   `stateDiff` for the same address.
/// - An overlay `stateDiff` is merged into an existing `stateDiff`, or applied
///   on top of an existing `state`.
pub fn merge_overrides(base: &mut StateOverride, overlay: &StateOverride) {
    for (addr, overlay_acct) in overlay {
        match base.entry(*addr) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let base_acct = entry.get_mut();

                if let Some(overlay_state) = &overlay_acct.state {
                    base_acct.state = Some(overlay_state.clone());
                    base_acct.state_diff = None;
                } else if let Some(overlay_diff) = &overlay_acct.state_diff {
                    if let Some(base_state) = &mut base_acct.state {
                        for (k, v) in overlay_diff {
                            base_state.insert(*k, *v);
                        }
                    } else {
                        let base_diff = base_acct.state_diff.get_or_insert_default();
                        for (k, v) in overlay_diff {
                            base_diff.insert(*k, *v);
                        }
                    }
                }

                if overlay_acct.nonce.is_some() {
                    base_acct.nonce = overlay_acct.nonce;
                }
                if overlay_acct.balance.is_some() {
                    base_acct.balance = overlay_acct.balance;
                }
                if overlay_acct.code.is_some() {
                    base_acct.code = overlay_acct.code.clone();
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(overlay_acct.clone());
            }
        }
    }
}

fn mapping_slot(key: B256, slot: u64) -> B256 {
    let mut blob = Vec::with_capacity(64);
    blob.extend_from_slice(key.as_slice());
    blob.extend_from_slice(&U256::from(slot).to_be_bytes::<32>());
    keccak256(blob)
}

fn encode_short_bytes(data: &[u8]) -> B256 {
    let mut word = [0u8; 32];
    let len = data.len().min(31);
    word[..len].copy_from_slice(&data[..len]);
    word[31] = (len as u8) * 2;
    B256::from(word)
}

fn apply_bytes_to_state_diff(state_diff: &mut SlotMap, slot: B256, data: &[u8]) {
    if data.len() <= 31 {
        state_diff.insert(slot, encode_short_bytes(data));
        return;
    }

    let len_word = U256::from(data.len()) * U256::from(2u64) + U256::from(1u64);
    state_diff.insert(slot, B256::from(len_word.to_be_bytes::<32>()));

    let base_slot_hash = keccak256(slot.as_slice());
    let base_slot = U256::from_be_bytes(base_slot_hash.into());

    for (i, chunk) in data.chunks(32).enumerate() {
        let mut word = [0u8; 32];
        word[..chunk.len()].copy_from_slice(chunk);
        let slot_i = base_slot + U256::from(i);
        state_diff.insert(B256::from(slot_i.to_be_bytes::<32>()), B256::from(word));
    }
}

fn mailbox_key(chain_id: ChainId, dep: &CrossRollupDependency) -> Option<B256> {
    let session_id = dep.session_id?;
    let mut preimage = Vec::with_capacity(32 + 32 + 20 + 20 + 32 + dep.label.len());
    preimage.extend_from_slice(&U256::from(dep.source_chain_id.0).to_be_bytes::<32>());
    preimage.extend_from_slice(&U256::from(chain_id.0).to_be_bytes::<32>());
    preimage.extend_from_slice(dep.sender.as_slice());
    preimage.extend_from_slice(dep.receiver.as_slice());
    preimage.extend_from_slice(&session_id.to_be_bytes::<32>());
    preimage.extend_from_slice(&dep.label);
    Some(keccak256(preimage))
}

/// Build mailbox state overrides for fulfilled dependencies.
///
/// Returns a typed `StateOverride` suitable for passing directly to
/// `debug_traceCall`, or `None` if there are no applicable dependencies.
pub fn build_mailbox_state_overrides(
    chain_id: ChainId,
    mailbox_address: Address,
    deps: &[CrossRollupDependency],
) -> Option<StateOverride> {
    let mut state_diff = SlotMap::default();

    for dep in deps {
        if dep.dest_chain_id != chain_id {
            continue;
        }
        let Some(data) = dep.data.as_ref() else {
            continue;
        };
        let Some(key) = mailbox_key(chain_id, dep) else {
            continue;
        };

        let inbox_slot = mapping_slot(key, INBOX_MAPPING_SLOT);
        let created_slot = mapping_slot(key, CREATED_KEYS_MAPPING_SLOT);
        apply_bytes_to_state_diff(&mut state_diff, inbox_slot, data);
        state_diff.insert(
            created_slot,
            B256::from(U256::from(1u64).to_be_bytes::<32>()),
        );
    }

    if state_diff.is_empty() {
        return None;
    }

    let account = AccountOverride {
        state_diff: Some(state_diff),
        ..Default::default()
    };
    let mut overrides = StateOverride::default();
    overrides.insert(mailbox_address, account);
    Some(overrides)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, U256};
    use alloy_rpc_types_eth::state::AccountOverride;
    use compose_primitives::{ChainId, CrossRollupDependency};

    #[test]
    fn merge_overrides_combines_accounts() {
        let addr: Address = "0xaabbccddeeaabbccddeeaabbccddeeaabbccddee"
            .parse()
            .unwrap();
        let mut base = StateOverride::default();
        base.insert(addr, AccountOverride { nonce: Some(1), ..Default::default() });
        let mut other = StateOverride::default();
        other.insert(
            addr,
            AccountOverride {
                balance: Some(U256::from(0x100u64)),
                ..Default::default()
            },
        );
        merge_overrides(&mut base, &other);
        let acct = base.get(&addr).unwrap();
        assert_eq!(acct.nonce, Some(1));
        assert_eq!(acct.balance, Some(U256::from(0x100u64)));
    }

    #[test]
    fn builds_mailbox_overrides_for_fulfilled_dep() {
        let dep = CrossRollupDependency {
            source_chain_id: ChainId(77777),
            dest_chain_id: ChainId(88888),
            sender: Address::repeat_byte(0x11),
            receiver: Address::repeat_byte(0x22),
            label: b"SEND".to_vec(),
            data: Some(vec![1, 2, 3]),
            session_id: Some(U256::from(42u64)),
        };

        let mailbox_addr: Address = "0xe5d5d610fb9767df117f4076444b45404201a097"
            .parse()
            .unwrap();
        let overrides =
            build_mailbox_state_overrides(ChainId(88888), mailbox_addr, &[dep]).unwrap();

        let account = overrides.get(&mailbox_addr).unwrap();
        let diff = account.state_diff.as_ref().unwrap();
        // inbox slot + length slot entries
        assert!(!diff.is_empty());
    }

    #[test]
    fn merge_overrides_merges_state_diff() {
        let addr: Address = "0xaabbccddeeaabbccddeeaabbccddeeaabbccddee"
            .parse()
            .unwrap();
        let slot1 = B256::repeat_byte(0x01);
        let slot2 = B256::repeat_byte(0x02);
        let val1 = B256::repeat_byte(0x10);
        let val2 = B256::repeat_byte(0x20);

        let mut diff1 = SlotMap::default();
        diff1.insert(slot1, val1);
        let mut base = StateOverride::default();
        base.insert(
            addr,
            AccountOverride {
                state_diff: Some(diff1),
                ..Default::default()
            },
        );

        let mut diff2 = SlotMap::default();
        diff2.insert(slot2, val2);
        let mut overlay = StateOverride::default();
        overlay.insert(
            addr,
            AccountOverride {
                state_diff: Some(diff2),
                ..Default::default()
            },
        );

        merge_overrides(&mut base, &overlay);
        let diff = base.get(&addr).unwrap().state_diff.as_ref().unwrap();
        assert_eq!(diff.len(), 2);
        assert_eq!(diff.get(&slot1), Some(&val1));
        assert_eq!(diff.get(&slot2), Some(&val2));
    }
}

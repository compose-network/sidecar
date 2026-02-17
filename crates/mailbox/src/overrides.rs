//! State override parsing, cloning, and merge helpers.

use alloy::primitives::{keccak256, Address, B256, U256};
use compose_primitives::{ChainId, CrossRollupDependency};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;

const INBOX_MAPPING_SLOT: u64 = 4;
const CREATED_KEYS_MAPPING_SLOT: u64 = 6;

/// Parse state overrides from a JSON value into a map.
pub fn parse_state_overrides(value: &Option<Value>) -> HashMap<String, Value> {
    match value {
        Some(Value::Object(map)) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        _ => HashMap::new(),
    }
}

/// Clone a state override map.
pub fn clone_state_overrides(overrides: &HashMap<String, Value>) -> HashMap<String, Value> {
    overrides.clone()
}

fn normalize_hex_map(value: &Value) -> HashMap<String, String> {
    match value {
        Value::Object(map) => map
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        _ => HashMap::new(),
    }
}

fn map_to_json_object(map: HashMap<String, String>) -> Value {
    let ordered: BTreeMap<_, _> = map.into_iter().collect();
    serde_json::to_value(ordered).unwrap_or(Value::Object(Default::default()))
}

/// Merge two state override maps, with `other` taking precedence.
pub fn merge_state_overrides(base: &mut HashMap<String, Value>, other: &HashMap<String, Value>) {
    for (addr, overrides) in other {
        match (base.get_mut(addr), overrides) {
            (Some(Value::Object(existing)), Value::Object(new)) => {
                if let Some(state) = new.get("state") {
                    existing.insert("state".to_string(), state.clone());
                    existing.remove("stateDiff");
                }

                if let Some(diff) = new.get("stateDiff") {
                    let overlay_diff = normalize_hex_map(diff);
                    if existing.contains_key("state") {
                        let mut merged_state =
                            normalize_hex_map(existing.get("state").unwrap_or(&Value::Null));
                        for (k, v) in overlay_diff {
                            merged_state.insert(k, v);
                        }
                        existing.insert("state".to_string(), map_to_json_object(merged_state));
                        existing.remove("stateDiff");
                    } else {
                        let mut merged_diff =
                            normalize_hex_map(existing.get("stateDiff").unwrap_or(&Value::Null));
                        for (k, v) in overlay_diff {
                            merged_diff.insert(k, v);
                        }
                        existing.insert("stateDiff".to_string(), map_to_json_object(merged_diff));
                    }
                }

                for (key, val) in new {
                    if key == "state" || key == "stateDiff" {
                        continue;
                    }
                    existing.insert(key.clone(), val.clone());
                }
            }
            _ => {
                base.insert(addr.clone(), overrides.clone());
            }
        }
    }
}

/// Merge two JSON state-override blobs.
pub fn merge_state_override_values(base: &Value, overlay: &Value) -> Value {
    let mut base_map = parse_state_overrides(&Some(base.clone()));
    let overlay_map = parse_state_overrides(&Some(overlay.clone()));
    merge_state_overrides(&mut base_map, &overlay_map);
    serde_json::to_value(base_map).unwrap_or(Value::Object(Default::default()))
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

fn apply_bytes_to_state_diff(state_diff: &mut HashMap<String, String>, slot: B256, data: &[u8]) {
    if data.len() <= 31 {
        state_diff.insert(format!("{slot:#x}"), format!("{:#x}", encode_short_bytes(data)));
        return;
    }

    let len_word = U256::from(data.len()) * U256::from(2u64) + U256::from(1u64);
    state_diff.insert(
        format!("{slot:#x}"),
        format!("{:#x}", B256::from(len_word.to_be_bytes::<32>())),
    );

    let base_slot_hash = keccak256(slot.as_slice());
    let base_slot = U256::from_be_bytes(base_slot_hash.into());

    for (i, chunk) in data.chunks(32).enumerate() {
        let mut word = [0u8; 32];
        word[..chunk.len()].copy_from_slice(chunk);
        let slot_i = base_slot + U256::from(i);
        state_diff.insert(
            format!("{:#x}", B256::from(slot_i.to_be_bytes::<32>())),
            format!("{:#x}", B256::from(word)),
        );
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
pub fn build_mailbox_state_overrides(
    chain_id: ChainId,
    mailbox_address: Address,
    deps: &[CrossRollupDependency],
) -> Option<Value> {
    let mut state_diff = HashMap::<String, String>::new();

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
            format!("{created_slot:#x}"),
            format!("{:#x}", B256::from(U256::from(1u64).to_be_bytes::<32>())),
        );
    }

    if state_diff.is_empty() {
        return None;
    }

    Some(serde_json::json!({
        format!("{mailbox_address:#x}"): {
            "stateDiff": state_diff
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, U256};
    use compose_primitives::{ChainId, CrossRollupDependency};
    use serde_json::json;

    #[test]
    fn merge_overrides() {
        let mut base = HashMap::from([("0xabc".to_string(), json!({"nonce": "0x1"}))]);
        let other = HashMap::from([
            ("0xabc".to_string(), json!({"balance": "0x100"})),
            ("0xdef".to_string(), json!({"nonce": "0x0"})),
        ]);
        merge_state_overrides(&mut base, &other);
        assert_eq!(base.len(), 2);

        let abc = base.get("0xabc").unwrap().as_object().unwrap();
        assert!(abc.contains_key("nonce"));
        assert!(abc.contains_key("balance"));
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

        let overrides = build_mailbox_state_overrides(
            ChainId(88888),
            "0xe5d5d610fb9767df117f4076444b45404201a097"
                .parse()
                .unwrap(),
            &[dep],
        )
        .unwrap();

        let root = overrides.as_object().unwrap();
        assert_eq!(root.len(), 1);
        let account = root.values().next().unwrap().as_object().unwrap();
        assert!(account.contains_key("stateDiff"));
    }

    #[test]
    fn merge_value_overrides_merges_state_diff() {
        let base = json!({
            "0xabc": {
                "stateDiff": {
                    "0x01": "0x10"
                }
            }
        });
        let overlay = json!({
            "0xabc": {
                "stateDiff": {
                    "0x02": "0x20"
                }
            }
        });

        let merged = merge_state_override_values(&base, &overlay);
        let obj = merged
            .get("0xabc")
            .and_then(|v| v.get("stateDiff"))
            .and_then(|v| v.as_object())
            .unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj.get("0x01").and_then(|v| v.as_str()), Some("0x10"));
        assert_eq!(obj.get("0x02").and_then(|v| v.as_str()), Some("0x20"));
    }
}

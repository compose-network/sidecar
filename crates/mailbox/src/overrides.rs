//! State override parsing, cloning, and merge helpers.

use serde_json::Value;
use std::collections::HashMap;

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

/// Merge two state override maps, with `other` taking precedence.
pub fn merge_state_overrides(base: &mut HashMap<String, Value>, other: &HashMap<String, Value>) {
    for (addr, overrides) in other {
        match (base.get_mut(addr), overrides) {
            (Some(Value::Object(existing)), Value::Object(new)) => {
                for (key, val) in new {
                    existing.insert(key.clone(), val.clone());
                }
            }
            _ => {
                base.insert(addr.clone(), overrides.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}

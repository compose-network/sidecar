//! Deterministic ordering rules for pending XTs.

use crate::model::pending_xt::PendingXt;

/// Canonical ordering for pending XTs: period → sequence → id.
pub fn xt_less(a_id: &str, a: &PendingXt, b_id: &str, b: &PendingXt) -> bool {
    if a.period_id != b.period_id {
        return a.period_id < b.period_id;
    }
    if a.sequence_num != b.sequence_num {
        return a.sequence_num < b.sequence_num;
    }
    a_id < b_id
}

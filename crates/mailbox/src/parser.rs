//! Mailbox trace parsing from call tracer output.

use crate::types::SimulationState;

/// Parse mailbox read/write calls from a geth `callTracer` output.
///
/// The current implementation returns an empty state and keeps trace parsing
/// as a no-op.
pub fn parse_call_trace(_trace: &serde_json::Value) -> SimulationState {
    SimulationState::default()
}

//! Simulation trait definitions used by the coordinator.

use async_trait::async_trait;
use compose_primitives::{ChainId, CrossRollupDependency, CrossRollupMessage, SimulationResult};

use crate::error::SimulationError;

/// Simulator runs EVM transaction simulations against an RPC node.
#[async_trait]
pub trait Simulator: Send + Sync + 'static {
    /// Simulate a single transaction with optional state overrides.
    async fn simulate(
        &self,
        chain_id: ChainId,
        tx: &[u8],
        state_overrides: &serde_json::Value,
    ) -> Result<SimulationResult, SimulationError>;

    /// Simulate a transaction with mailbox context (already-sent messages and
    /// fulfilled dependencies).
    async fn simulate_with_mailbox(
        &self,
        chain_id: ChainId,
        tx: &[u8],
        state_overrides: &serde_json::Value,
        already_sent_msgs: &[CrossRollupMessage],
        fulfilled_deps: &[CrossRollupDependency],
    ) -> Result<SimulationResult, SimulationError>;
}

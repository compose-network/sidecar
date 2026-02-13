//! Simulation backend error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("transaction decoding error: {0}")]
    Decode(String),
    #[error("simulation failed: {0}")]
    Failed(String),
    #[error("timeout")]
    Timeout,
    #[error("{0}")]
    Other(String),
}

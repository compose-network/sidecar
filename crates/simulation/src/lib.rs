//! Simulation backends used by coordinator XT processing.
//!
//! The coordinator uses this crate to execute transactions against per-chain
//! RPC endpoints and collect simulation outcomes.

pub mod error;
pub mod rpc;
pub mod traits;
pub mod types;

//! Shared protocol primitives used across sidecar crates.
//!
//! This crate contains common identifiers and payload types used by
//! coordination, simulation, mailbox, and server crates.

use alloy::primitives::{Address, B256, U256};
pub use alloy_rpc_types_eth::state::StateOverride;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;

/// Unique identifier for a rollup chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ChainId(pub u64);

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for ChainId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<ChainId> for u64 {
    fn from(value: ChainId) -> Self {
        value.0
    }
}

/// Identifier for a slot/period in the SBCP timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct PeriodId(pub u64);

impl From<u64> for PeriodId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<PeriodId> for u64 {
    fn from(value: PeriodId) -> Self {
        value.0
    }
}

/// Superblock number for ordering superblocks on L1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct SuperblockNumber(pub u64);

impl From<u64> for SuperblockNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<SuperblockNumber> for u64 {
    fn from(value: SuperblockNumber) -> Self {
        value.0
    }
}

/// Sequence number for ordering cross-chain transactions within a period.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord,
)]
pub struct SequenceNumber(pub u64);

impl From<u64> for SequenceNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<SequenceNumber> for u64 {
    fn from(value: SequenceNumber) -> Self {
        value.0
    }
}

/// Cross-chain transaction identifier (SHA-256 hash).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct XtId(pub B256);

impl XtId {
    /// Create an [`XtId`] from raw bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut hash = B256::ZERO;
        let len = bytes.len().min(32);
        hash[..len].copy_from_slice(&bytes[..len]);
        Self(hash)
    }

    /// Compute an [`XtId`] by hashing the given data with SHA-256.
    pub fn from_data(data: &[u8]) -> Self {
        let hash = Sha256::digest(data);
        Self(B256::from_slice(&hash))
    }

    /// Return the hex-encoded string representation.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Return the raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl fmt::Display for XtId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Cheap-clone cross-chain transaction instance identifier.
///
/// Backed by `Arc<str>` so that cloning is a reference-count bump rather
/// than a heap allocation.  Implements `Borrow<str>` and `Deref<Target=str>`
/// so it can be used as a `HashMap` key while still being looked up with `&str`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InstanceId(Arc<str>);

impl InstanceId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Create an `InstanceId` for standalone mode (no publisher).
    ///
    /// Format: `xt-{chain_id}-{seq}` — human-readable and unique per sidecar.
    /// This string is forwarded to peer sidecars as-is, so the format must
    /// be stable across versions.
    pub fn standalone(chain_id: ChainId, seq: u64) -> Self {
        Self(format!("xt-{chain_id}-{seq}").into())
    }

    /// Create an `InstanceId` from raw bytes assigned by the Shared Publisher.
    ///
    /// The bytes are hex-encoded so the result is a printable ASCII string
    /// that matches what `StartInstance::instance_id_hex()` returns.
    pub fn from_publisher_bytes(bytes: &[u8]) -> Self {
        Self(hex::encode(bytes).into())
    }
}

impl std::ops::Deref for InstanceId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for InstanceId {
    fn from(s: String) -> Self {
        Self(s.into())
    }
}

impl From<&str> for InstanceId {
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

impl std::borrow::Borrow<str> for InstanceId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// Status of a cross-chain transaction through its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum XtStatus {
    Pending,
    Simulating,
    WaitingCirc,
    Simulated,
    Voted,
    Committed,
    Aborted,
}

impl fmt::Display for XtStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Simulating => write!(f, "simulating"),
            Self::WaitingCirc => write!(f, "waiting_circ"),
            Self::Simulated => write!(f, "simulated"),
            Self::Voted => write!(f, "voted"),
            Self::Committed => write!(f, "committed"),
            Self::Aborted => write!(f, "aborted"),
        }
    }
}

/// Chain state snapshot from a builder poll.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainState {
    pub chain_id: ChainId,
    pub block_number: u64,
    pub flashblock_index: u64,
    pub state_root: B256,
    pub timestamp: u64,
    pub gas_limit: u64,
    pub state_overrides: Option<StateOverride>,
}

/// A cross-rollup dependency (mailbox read).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossRollupDependency {
    pub source_chain_id: ChainId,
    pub dest_chain_id: ChainId,
    pub sender: Address,
    pub receiver: Address,
    pub label: Vec<u8>,
    pub data: Option<Vec<u8>>,
    pub session_id: Option<U256>,
}

/// A cross-rollup outbound message (mailbox write).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossRollupMessage {
    pub source_chain_id: ChainId,
    pub dest_chain_id: ChainId,
    pub sender: Address,
    pub receiver: Address,
    pub label: String,
    pub data: Vec<u8>,
    pub session_id: Option<U256>,
}

/// Result of simulating a transaction.
#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub success: bool,
    pub error: Option<String>,
    pub state_overrides: Option<StateOverride>,
    pub dependencies: Vec<CrossRollupDependency>,
    pub outbound_messages: Vec<CrossRollupMessage>,
}

/// A single transaction payload returned to the builder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionPayload {
    pub raw: String,
    pub required: bool,
    pub instance_id: String,
}

/// Builder poll request from op-rbuilder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuilderPollRequest {
    pub chain_id: ChainId,
    pub block_number: u64,
    pub flashblock_index: u64,
    pub state_root: B256,
    pub timestamp: u64,
    pub gas_limit: u64,
    pub state_overrides: Option<StateOverride>,
}

/// Builder poll response to op-rbuilder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuilderPollResponse {
    pub hold: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transactions: Vec<TransactionPayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_after_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_hold_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_id_round_trip() {
        let id = ChainId(901);
        assert_eq!(u64::from(id), 901);
        assert_eq!(id.to_string(), "901");
    }

    #[test]
    fn xt_id_from_data() {
        let id = XtId::from_data(b"test transaction");
        assert_eq!(id.as_bytes().len(), 32);
        assert!(!id.to_hex().is_empty());
    }

    #[test]
    fn xt_status_display() {
        assert_eq!(XtStatus::Pending.to_string(), "pending");
        assert_eq!(XtStatus::WaitingCirc.to_string(), "waiting_circ");
        assert_eq!(XtStatus::Committed.to_string(), "committed");
    }
}

//! Mailbox domain types extracted from simulations.

use alloy::primitives::{Address, U256};
use compose_primitives::ChainId;

/// A parsed mailbox call from a call trace.
#[derive(Debug, Clone)]
pub struct MailboxCall {
    pub call_type: MailboxCallType,
    pub source_chain: ChainId,
    pub dest_chain: ChainId,
    pub sender: Address,
    pub receiver: Address,
    pub label: String,
    pub data: Vec<u8>,
    pub session_id: Option<U256>,
}

/// Whether a mailbox call is a read (dependency) or write (outbound message).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailboxCallType {
    Read,
    Write,
}

/// Simulation state containing parsed mailbox calls.
#[derive(Debug, Clone, Default)]
pub struct SimulationState {
    pub reads: Vec<MailboxCall>,
    pub writes: Vec<MailboxCall>,
}

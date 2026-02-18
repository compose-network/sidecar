//! In-memory representation of a pending cross-chain transaction.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use compose_primitives::{
    ChainId, ChainState, CrossRollupDependency, CrossRollupMessage, PeriodId, SequenceNumber,
    StateOverride,
};
use compose_proto::rollup_v2::MailboxMessage;

/// A cross-chain transaction in flight, tracking its full lifecycle from
/// submission through simulation, voting, and decision.
#[derive(Debug, Clone)]
pub struct PendingXt {
    /// Hex-encoded instance identifier.
    pub id: String,
    /// Raw instance ID bytes.
    pub instance_id: Vec<u8>,
    /// Period during which this XT was created.
    pub period_id: PeriodId,
    /// Sequence number within the period.
    pub sequence_num: SequenceNumber,

    /// Raw RLP-encoded transactions per chain.
    pub raw_txs: HashMap<ChainId, Vec<Vec<u8>>>,
    /// Chain state snapshots from builder polls.
    pub chain_states: HashMap<ChainId, ChainState>,
    /// State overrides accumulated during simulation, keyed by chain.
    pub state_overrides: HashMap<ChainId, StateOverride>,

    /// When the XT was created.
    pub created_at: Instant,
    /// When simulation completed.
    pub simulated_at: Option<Instant>,
    /// When a decision was recorded.
    pub decided_at: Option<Instant>,

    /// Final commit/abort decision.
    pub decision: Option<bool>,
    /// Whether the local vote was sent.
    pub vote_sent: bool,
    /// The local chain's vote.
    pub local_vote: Option<bool>,
    /// Votes received from peer sidecars.
    pub peer_votes: HashMap<ChainId, bool>,

    /// Chains that are currently locked (being simulated).
    pub locked_chains: HashSet<ChainId>,

    /// Origin chain for standalone-mode XTs.
    pub origin_chain: Option<ChainId>,
    /// Origin sequence number.
    pub origin_seq: SequenceNumber,

    /// Inbound CIRC messages waiting to be matched.
    pub pending_mailbox: Vec<MailboxMessage>,
    /// Already-sent outbound CIRC messages.
    pub sent_mailbox: Vec<MailboxMessage>,
    /// Cross-rollup dependencies discovered during simulation.
    pub dependencies: Vec<CrossRollupDependency>,
    /// Dependencies that have been fulfilled.
    pub fulfilled_deps: Vec<CrossRollupDependency>,
    /// Outbound messages from simulation.
    pub outbound_messages: Vec<CrossRollupMessage>,

    /// Chains to which committed transactions have been delivered.
    pub delivered_chains: HashSet<ChainId>,
}

impl PendingXt {
    pub fn new(id: String, instance_id: Vec<u8>) -> Self {
        Self {
            id,
            instance_id,
            period_id: PeriodId(0),
            sequence_num: SequenceNumber(0),
            raw_txs: HashMap::new(),
            chain_states: HashMap::new(),
            state_overrides: HashMap::new(),
            created_at: Instant::now(),
            simulated_at: None,
            decided_at: None,
            decision: None,
            vote_sent: false,
            local_vote: None,
            peer_votes: HashMap::new(),
            locked_chains: HashSet::new(),
            origin_chain: None,
            origin_seq: SequenceNumber(0),
            pending_mailbox: Vec::new(),
            sent_mailbox: Vec::new(),
            dependencies: Vec::new(),
            fulfilled_deps: Vec::new(),
            outbound_messages: Vec::new(),
            delivered_chains: HashSet::new(),
        }
    }

    /// Whether this XT has received a commit/abort decision.
    pub fn is_decided(&self) -> bool {
        self.decision.is_some()
    }

    /// Whether this XT was committed.
    pub fn is_committed(&self) -> bool {
        self.decision == Some(true)
    }
}

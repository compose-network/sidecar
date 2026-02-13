//! Primary coordinator type and shared mutable state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use compose_mailbox::traits::MailboxQueue;
use compose_peer::traits::PeerCoordinator;
use compose_primitives::{ChainId, ChainState, PeriodId, SequenceNumber, SuperblockNumber};
use compose_simulation::traits::Simulator;
use tokio::sync::RwLock;
use tracing::info;

use crate::error::CoordinatorError;
use crate::model::pending_xt::PendingXt;
use crate::model::xt_status::{determine_xt_status, XtStatusResponse};
use crate::nonce_manager::DeferredNonceManager;
use crate::traits::mailbox::MailboxSender;
use crate::traits::publisher::PublisherClient;
use crate::traits::put_inbox::PutInboxBuilder;

/// Shared coordinator state protected by a `RwLock`.
#[derive(Debug)]
pub(crate) struct CoordinatorState {
    pub pending: HashMap<String, PendingXt>,
    pub chain_states: HashMap<ChainId, ChainState>,
    pub current_period_id: PeriodId,
    pub current_superblock_num: SuperblockNumber,
    pub period_initialized: bool,
    pub last_sequence_num: SequenceNumber,
    pub last_known_blocks: HashMap<ChainId, u64>,
}

impl CoordinatorState {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
            chain_states: HashMap::new(),
            current_period_id: PeriodId(0),
            current_superblock_num: SuperblockNumber(0),
            period_initialized: false,
            last_sequence_num: SequenceNumber(0),
            last_known_blocks: HashMap::new(),
        }
    }

    /// Check if there is an active (undecided) instance with local chain transactions.
    pub(crate) fn has_active_instance(&self, chain_id: ChainId) -> bool {
        self.pending.values().any(|xt| {
            xt.decision.is_none()
                && xt
                    .raw_txs
                    .get(&chain_id)
                    .map(|txs| !txs.is_empty())
                    .unwrap_or(false)
        })
    }
}

/// The default coordinator implementation.
///
/// This struct is cheaply cloneable (all shared state is behind `Arc`).
#[derive(Clone)]
#[allow(dead_code)]
pub struct DefaultCoordinator {
    pub(crate) chain_id: ChainId,
    pub(crate) state: Arc<RwLock<CoordinatorState>>,
    pub(crate) nonce_manager: Arc<DeferredNonceManager>,
    pub(crate) simulator: Option<Arc<dyn Simulator>>,
    pub(crate) publisher: Option<Arc<dyn PublisherClient>>,
    pub(crate) mailbox_sender: Option<Arc<dyn MailboxSender>>,
    pub(crate) mailbox_queue: Option<Arc<dyn MailboxQueue>>,
    pub(crate) peer_coordinator: Option<Arc<dyn PeerCoordinator>>,
    pub(crate) put_inbox_builder: Option<Arc<dyn PutInboxBuilder>>,
    pub(crate) circ_timeout_ms: u64,
}

impl std::fmt::Debug for DefaultCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultCoordinator")
            .field("chain_id", &self.chain_id)
            .field("circ_timeout_ms", &self.circ_timeout_ms)
            .finish()
    }
}

impl DefaultCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: ChainId,
        simulator: Option<Arc<dyn Simulator>>,
        publisher: Option<Arc<dyn PublisherClient>>,
        mailbox_sender: Option<Arc<dyn MailboxSender>>,
        mailbox_queue: Option<Arc<dyn MailboxQueue>>,
        peer_coordinator: Option<Arc<dyn PeerCoordinator>>,
        put_inbox_builder: Option<Arc<dyn PutInboxBuilder>>,
        circ_timeout_ms: u64,
    ) -> Self {
        Self {
            chain_id,
            state: Arc::new(RwLock::new(CoordinatorState::new())),
            nonce_manager: Arc::new(DeferredNonceManager::new()),
            simulator,
            publisher,
            mailbox_sender,
            mailbox_queue,
            peer_coordinator,
            put_inbox_builder,
            circ_timeout_ms,
        }
    }

    /// Start the coordinator's background tasks (cleanup loop, etc.).
    pub async fn start(&self) -> Result<(), CoordinatorError> {
        info!(chain_id = %self.chain_id, "Starting coordinator");

        let coord = self.clone();
        tokio::spawn(async move {
            coord.cleanup_loop().await;
        });

        Ok(())
    }

    /// Gracefully shut down.
    pub async fn stop(&self) -> Result<(), CoordinatorError> {
        info!("Stopping coordinator");
        Ok(())
    }

    /// Remove decided XTs older than `max_age`.
    pub async fn cleanup(&self, max_age: Duration) {
        let mut state = self.state.write().await;
        let now = std::time::Instant::now();
        state.pending.retain(|_id, xt| {
            if let Some(decided_at) = xt.decided_at {
                now.duration_since(decided_at) <= max_age
            } else {
                true
            }
        });
    }

    async fn cleanup_loop(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            self.cleanup(Duration::from_secs(300)).await;
        }
    }

    /// Query the status of a cross-chain transaction.
    pub async fn get_xt_status(
        &self,
        instance_id: &str,
    ) -> Result<XtStatusResponse, CoordinatorError> {
        let state = self.state.read().await;
        let xt = state
            .pending
            .get(instance_id)
            .ok_or_else(|| CoordinatorError::InstanceNotFound(instance_id.to_string()))?;

        let status = determine_xt_status(xt);

        Ok(XtStatusResponse {
            instance_id: instance_id.to_string(),
            status,
            decision: xt.decision,
        })
    }

    /// Whether the publisher connection is currently active.
    pub(crate) async fn is_publisher_connected(&self) -> bool {
        self.publisher
            .as_ref()
            .map(|p| p.is_connected())
            .unwrap_or(false)
    }

    /// Cheap clone that shares all internal state (for spawning tasks).
    pub(crate) fn clone_ref(&self) -> Self {
        self.clone()
    }
}

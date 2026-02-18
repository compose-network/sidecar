//! Primary coordinator type and shared mutable state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use compose_mailbox::traits::MailboxQueue;
use compose_peer::traits::PeerCoordinator;
use compose_primitives::{ChainId, ChainState, PeriodId, SequenceNumber, SuperblockNumber};
use compose_simulation::traits::Simulator;
use prost::Message;
use tokio::sync::RwLock;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use compose_metrics::SidecarMetrics;
use compose_primitives_traits::{CoordinatorError, MailboxSender, PublisherClient, PutInboxBuilder};

use crate::model::chain_overlay::ChainOverlay;
use crate::model::pending_xt::PendingXt;
use crate::model::xt_status::{determine_xt_status, XtStatusResponse};
use crate::nonce_manager::DeferredNonceManager;
use crate::pipeline::submission::{build_xt_request, xt_request_fingerprint};

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
    /// Monotonic counter for locally-originated XTs in standalone mode.
    pub origin_seq: SequenceNumber,
    /// Per-chain overlay of post-simulation state diffs for the current
    /// block/flashblock window. Lets XT-B see the state produced by XT-A.
    pub chain_overlay: HashMap<ChainId, ChainOverlay>,
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
            origin_seq: SequenceNumber(0),
            chain_overlay: HashMap::new(),
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
    pub(crate) task_tracker: TaskTracker,
    pub(crate) metrics: Option<Arc<SidecarMetrics>>,
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
            task_tracker: TaskTracker::new(),
            metrics: None,
        }
    }

    /// Attach a metrics instance to this coordinator.
    pub fn set_metrics(&mut self, metrics: Arc<SidecarMetrics>) {
        self.metrics = Some(metrics);
    }

    /// Start the coordinator's background tasks (cleanup loop, etc.).
    pub async fn start(&self) -> Result<(), CoordinatorError> {
        info!(chain_id = %self.chain_id, "Starting coordinator");

        let coord = self.clone();
        self.task_tracker.spawn(async move {
            coord.cleanup_loop().await;
        });

        Ok(())
    }

    /// Gracefully shut down, waiting for all spawned tasks to complete.
    pub async fn stop(&self) -> Result<(), CoordinatorError> {
        info!("Stopping coordinator");
        self.task_tracker.close();
        self.task_tracker.wait().await;
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

    /// In standalone mode, compute whether the instance can be decided from the
    /// currently known local and peer votes.
    ///
    /// Rules are aligned with SCP/2PC docs:
    /// - any `false` vote decides `false` immediately;
    /// - `true` is decided only when all expected votes are collected.
    pub(crate) fn maybe_make_standalone_decision(
        &self,
        xt: &mut PendingXt,
    ) -> Option<(bool, usize, usize)> {
        if xt.decision.is_some() {
            return None;
        }

        let expected = xt.raw_txs.len();
        let mut collected = 0usize;
        let mut has_abort_vote = false;

        if let Some(local) = xt.local_vote {
            collected += 1;
            if !local {
                has_abort_vote = true;
            }
        }

        for (cid, &vote) in &xt.peer_votes {
            if *cid == self.chain_id {
                continue;
            }
            collected += 1;
            if !vote {
                has_abort_vote = true;
            }
        }

        if has_abort_vote {
            xt.decision = Some(false);
            xt.decided_at = Some(std::time::Instant::now());
            return Some((false, collected, expected));
        }

        if expected > 0 && collected >= expected {
            xt.decision = Some(true);
            xt.decided_at = Some(std::time::Instant::now());
            return Some((true, collected, expected));
        }

        None
    }

    /// Submit a cross-chain transaction.
    ///
    /// In publisher-connected mode, the XT is encoded as an `XtRequest` protobuf
    /// and sent to the publisher, which assigns the instance ID. In standalone
    /// mode, a local ID is generated and the XT is forwarded to peer sidecars.
    pub async fn submit_xt(
        &self,
        txs: HashMap<ChainId, Vec<Vec<u8>>>,
    ) -> Result<String, CoordinatorError> {
        if txs.is_empty() {
            return Err(CoordinatorError::NoTransactions);
        }

        if self.is_publisher_connected().await {
            self.submit_xt_publisher(txs).await
        } else {
            self.submit_xt_standalone(txs).await
        }
    }

    async fn submit_xt_publisher(
        &self,
        txs: HashMap<ChainId, Vec<Vec<u8>>>,
    ) -> Result<String, CoordinatorError> {
        let publisher = self
            .publisher
            .as_ref()
            .ok_or(CoordinatorError::PublisherNotConnected)?;

        let xt_request = build_xt_request(&txs);
        let fingerprint = xt_request_fingerprint(&xt_request);

        let wire = compose_proto::rollup_v2::WireMessage {
            sender_id: String::new(),
            payload: Some(compose_proto::rollup_v2::wire_message::Payload::XtRequest(
                xt_request,
            )),
        };
        let data = wire.encode_to_vec();

        publisher
            .send_raw(&data)
            .await
            .map_err(|e| CoordinatorError::Other(format!("failed to send XT to publisher: {e}")))?;

        info!(fingerprint = %fingerprint, "Submitted XT to publisher");
        Ok(fingerprint)
    }

    async fn submit_xt_standalone(
        &self,
        txs: HashMap<ChainId, Vec<Vec<u8>>>,
    ) -> Result<String, CoordinatorError> {
        let instance_id = {
            let mut state = self.state.write().await;
            state.origin_seq = SequenceNumber(state.origin_seq.0 + 1);
            let seq = state.origin_seq;
            let id = format!("xt-{}-{}", self.chain_id, seq.0);

            let mut xt = PendingXt::new(id.clone(), id.as_bytes().to_vec());
            xt.origin_chain = Some(self.chain_id);
            xt.origin_seq = seq;
            xt.raw_txs = txs.clone();

            state.pending.insert(id.clone(), xt);
            id
        };

        info!(instance_id = %instance_id, "Submitted XT locally (standalone mode)");

        if let Some(peer_coordinator) = &self.peer_coordinator {
            let id = instance_id.clone();
            let chain_id = self.chain_id;
            let origin_seq = {
                let state = self.state.read().await;
                state.origin_seq
            };
            let pc = peer_coordinator.clone();
            self.task_tracker.spawn(async move {
                if let Err(e) = pc.forward_xt(&id, &txs, chain_id, origin_seq).await {
                    error!(instance_id = %id, error = %e, "Failed to forward XT to peers");
                }
            });
        } else {
            warn!(
                instance_id = %instance_id,
                "No peer coordinator configured, XT will only be processed locally"
            );
        }

        Ok(instance_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn has_active_instance_returns_false_when_decided() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.decision = Some(true);
            xt.decided_at = Some(std::time::Instant::now());
            state.pending.insert("xt-77777-1".to_string(), xt);
        }

        let state = coordinator.state.read().await;
        assert!(!state.has_active_instance(ChainId(77777)));
    }

    #[tokio::test]
    async fn cleanup_removes_old_decided_xts() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            // Simulate a decision that happened a long time ago.
            xt.decision = Some(true);
            xt.decided_at = Some(
                std::time::Instant::now()
                    .checked_sub(Duration::from_secs(400))
                    .unwrap(),
            );
            state.pending.insert("xt-77777-1".to_string(), xt);
        }

        coordinator.cleanup(Duration::from_secs(300)).await;

        let state = coordinator.state.read().await;
        assert!(state.pending.is_empty());
    }
}

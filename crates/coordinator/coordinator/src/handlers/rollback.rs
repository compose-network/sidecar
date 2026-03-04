//! Rollback handling for aborting undecided instances.

use compose_primitives::PeriodId;
use tracing::warn;

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Abort all undecided instances and reset period state.
    pub async fn handle_rollback(
        &self,
        period_id: PeriodId,
        last_finalized_superblock_num: u64,
        _last_finalized_superblock_hash: &[u8],
    ) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().await;

        let mut aborted = 0;
        for xt in state.pending.values_mut() {
            if xt.decision.is_some() {
                continue;
            }
            xt.record_decision(false);
            aborted += 1;
        }

        state.period_initialized = false;
        state.last_sequence_num = Default::default();
        state.last_known_blocks.clear();
        state.chain_overlay.clear();
        state.mailbox_buffer.clear();
        state.pending_submissions.clear();

        warn!(
            period_id = period_id.0,
            last_finalized_superblock = last_finalized_superblock_num,
            aborted_instances = aborted,
            "Rollback received, all undecided instances aborted"
        );

        drop(state);

        if let Some(builder) = &self.put_inbox_builder {
            let b = builder.clone();
            if let Err(e) = self
                .nonce_manager
                .resync(move || {
                    let b = b.clone();
                    async move { b.pending_nonce_at().await }
                })
                .await
            {
                warn!(error = %e, "Failed to resync putInbox nonce on rollback");
            }
        }

        Ok(())
    }
}

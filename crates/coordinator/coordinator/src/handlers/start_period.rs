//! Start-period handling and period state transitions.

use std::time::Instant;

use compose_primitives::{PeriodId, SuperblockNumber};
use tracing::info;

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Handle a new period from the publisher. Aborts any stale undecided
    /// instances from prior periods.
    pub async fn handle_start_period(
        &self,
        period_id: PeriodId,
        superblock_num: SuperblockNumber,
    ) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().await;

        let mut aborted = 0;
        for xt in state.pending.values_mut() {
            if xt.decision.is_some() || xt.period_id.0 == 0 || xt.period_id >= period_id {
                continue;
            }
            xt.decision = Some(false);
            xt.decided_at = Some(Instant::now());
            aborted += 1;
        }

        state.current_period_id = period_id;
        state.current_superblock_num = superblock_num;
        state.period_initialized = true;
        state.last_sequence_num = Default::default();
        state.last_known_blocks.clear();

        info!(
            period_id = period_id.0,
            superblock_num = superblock_num.0,
            aborted_stale = aborted,
            "Started new period"
        );

        Ok(())
    }
}

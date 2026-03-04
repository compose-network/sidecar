//! Decision message handling and XT finalization updates.

use tracing::info;

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Record a commit/abort decision for an instance.
    pub async fn on_decision(
        &self,
        instance_id: &str,
        decision: bool,
    ) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().await;

        let xt = state
            .pending
            .get_mut(instance_id)
            .ok_or_else(|| CoordinatorError::InstanceNotFound(instance_id.to_string()))?;

        let latency = xt.created_at.elapsed();
        xt.record_decision(decision);

        info!(instance_id, decision, "Decision received");

        if let Some(m) = &self.metrics {
            if decision {
                m.xt_decided_commit_total.inc();
            } else {
                m.xt_decided_abort_total.inc();
            }
            m.xt_decision_latency_seconds.observe(latency.as_secs_f64());
            m.xt_pending_count.dec();
        }

        Ok(())
    }
}

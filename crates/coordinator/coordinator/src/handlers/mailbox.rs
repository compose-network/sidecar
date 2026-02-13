//! Inbound mailbox message handling and state updates.

use compose_proto::rollup_v2::MailboxMessage;
use tracing::debug;

use crate::coordinator::DefaultCoordinator;
use crate::error::CoordinatorError;

impl DefaultCoordinator {
    /// Handle an incoming CIRC message from a peer sidecar.
    pub async fn handle_mailbox_message(
        &self,
        msg: &MailboxMessage,
    ) -> Result<(), CoordinatorError> {
        let instance_key = hex::encode(&msg.instance_id);

        debug!(
            instance_id = %instance_key,
            source_chain = msg.source_chain,
            dest_chain = msg.destination_chain,
            label = %msg.label,
            "Received mailbox message from peer"
        );

        if let Some(queue) = &self.mailbox_queue {
            queue
                .record(msg)
                .await
                .map_err(|e| CoordinatorError::Mailbox(e.to_string()))?;
        }

        let mut state = self.state.write().await;
        if let Some(xt) = state.pending.get_mut(&instance_key) {
            xt.pending_mailbox.push(msg.clone());
            debug!(
                instance_id = %instance_key,
                pending_count = xt.pending_mailbox.len(),
                "Added mailbox message to pending XT"
            );
        }

        Ok(())
    }
}

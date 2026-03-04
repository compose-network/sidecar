//! Inbound mailbox message handling and state updates.

use compose_proto::rollup_v2::MailboxMessage;
use tracing::{debug, warn};

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Handle an incoming CIRC message from a peer sidecar.
    pub async fn handle_mailbox_message(
        &self,
        msg: &MailboxMessage,
    ) -> Result<(), CoordinatorError> {
        debug!(
            instance_id = %hex::encode(&msg.instance_id),
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

        if let Some(m) = &self.metrics {
            m.circ_messages_received_total.inc();
        }

        let notify = {
            let mut state = self.state.write().await;
            if let Some(xt_id) = state.mailbox_index.get(&msg.instance_id).cloned() {
                if let Some(xt) = state.pending.get_mut(&xt_id) {
                    xt.pending_mailbox.push(msg.clone());
                    debug!(
                        instance_id = %xt_id,
                        pending_count = xt.pending_mailbox.len(),
                        "Added mailbox message to pending XT"
                    );
                }
            } else {
                // XT not registered yet — buffer so it can be attached when
                // the XT arrives.  This handles the race where sidecar-a's
                // simulation completes in <1 ms and sends outbound mailbox
                // messages to sidecar-b before the forwarded XT arrives.
                warn!(
                    instance_id = %hex::encode(&msg.instance_id),
                    label = %msg.label,
                    "Buffering mailbox message for unregistered XT"
                );
                state.buffer_orphan_mailbox(msg.clone());
            }
            state.mailbox_notify.clone()
        };

        // Wake all simulations waiting on CIRC dependencies.
        notify.notify_waiters();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use compose_primitives::ChainId;
    use compose_proto::rollup_v2::MailboxMessage;

    use crate::coordinator::DefaultCoordinator;
    use crate::model::pending_xt::PendingXt;

    #[tokio::test]
    async fn mailbox_message_buffered_when_xt_not_yet_registered() {
        // Reproduce the race where the mailbox message arrives before the
        // forwarded XT is registered in mailbox_index.
        let coordinator =
            DefaultCoordinator::new(ChainId(88888), None, None, None, None, None, None, 1_000);

        let instance_id = "xt-77777-2".to_string();

        // Send the mailbox message BEFORE the XT is registered.
        let msg = MailboxMessage {
            instance_id: instance_id.as_bytes().to_vec(),
            source_chain: 77777,
            destination_chain: 88888,
            label: "SEND".to_string(),
            ..Default::default()
        };
        coordinator.handle_mailbox_message(&msg).await.unwrap();

        // Verify the message is in the buffer, not lost.
        {
            let state = coordinator.state.read().await;
            let buffered = state
                .mailbox_buffer
                .get(instance_id.as_bytes())
                .expect("message should be buffered");
            assert_eq!(buffered.len(), 1);
            assert_eq!(buffered[0].label, "SEND");
        }

        // Now register the XT via handle_forwarded_xt.
        use std::collections::HashMap;
        coordinator
            .handle_forwarded_xt(
                &instance_id,
                HashMap::from([(ChainId(88888), vec![vec![1]])]),
                ChainId(77777),
                compose_primitives::SequenceNumber(1),
            )
            .await
            .unwrap();

        // Buffer should be drained and message attached to the XT.
        let state = coordinator.state.read().await;
        assert!(
            !state.mailbox_buffer.contains_key(instance_id.as_bytes()),
            "buffer should be empty after XT registration"
        );
        let xt = state.pending.get(instance_id.as_str()).unwrap();
        assert_eq!(xt.pending_mailbox.len(), 1);
        assert_eq!(xt.pending_mailbox[0].label, "SEND");
    }

    #[tokio::test]
    async fn mailbox_message_attaches_to_pending_xt_by_raw_instance_id() {
        let coordinator =
            DefaultCoordinator::new(ChainId(88888), None, None, None, None, None, None, 1_000);

        let instance_id = "xt-77777-1".to_string();
        let xt = PendingXt::new(instance_id.clone(), instance_id.as_bytes().to_vec());

        {
            let mut state = coordinator.state.write().await;
            state
                .mailbox_index
                .insert(instance_id.as_bytes().to_vec(), xt.id.clone());
            state.pending.insert(xt.id.clone(), xt);
        }

        let msg = MailboxMessage {
            instance_id: instance_id.as_bytes().to_vec(),
            source_chain: 77777,
            destination_chain: 88888,
            label: "SEND".to_string(),
            ..Default::default()
        };

        coordinator.handle_mailbox_message(&msg).await.unwrap();

        let state = coordinator.state.read().await;
        let updated = state.pending.get(instance_id.as_str()).unwrap();

        assert_eq!(updated.pending_mailbox.len(), 1);
        assert_eq!(updated.pending_mailbox[0].label, "SEND");
    }
}

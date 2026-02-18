//! Inbound mailbox message handling and state updates.

use compose_proto::rollup_v2::MailboxMessage;
use tracing::debug;

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

        let mut state = self.state.write().await;
        let matched = state.pending.iter_mut().find_map(|(instance_key, xt)| {
            if xt.instance_id == msg.instance_id {
                xt.pending_mailbox.push(msg.clone());
                Some((instance_key.clone(), xt.pending_mailbox.len()))
            } else {
                None
            }
        });

        if let Some((instance_key, pending_count)) = matched {
            debug!(instance_id = %instance_key, pending_count, "Added mailbox message to pending XT");
        }

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
    async fn mailbox_message_attaches_to_pending_xt_by_raw_instance_id() {
        let coordinator = DefaultCoordinator::new(
            ChainId(88888),
            None,
            None,
            None,
            None,
            None,
            None,
            1_000,
        );

        let instance_id = "xt-77777-1".to_string();
        let xt = PendingXt::new(instance_id.clone(), instance_id.as_bytes().to_vec());

        {
            let mut state = coordinator.state.write().await;
            state.pending.insert(instance_id.clone(), xt);
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
        let updated = state.pending.get(&instance_id).unwrap();

        assert_eq!(updated.pending_mailbox.len(), 1);
        assert_eq!(updated.pending_mailbox[0].label, "SEND");
    }
}

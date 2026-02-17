//! Publisher message dispatch for inbound control-plane events.

use std::sync::Arc;

use bytes::Bytes;
use compose_coordinator::coordinator::DefaultCoordinator;
use compose_primitives::{PeriodId, SuperblockNumber};
use compose_proto::rollup_v2::{wire_message, WireMessage};
use prost::Message;
use tracing::{debug, error, warn};

/// Dispatch an inbound protobuf message from the publisher connection.
pub async fn handle_publisher_message(coordinator: Arc<DefaultCoordinator>, data: Bytes) {
    let msg = match WireMessage::decode(data) {
        Ok(m) => m,
        Err(e) => {
            error!(error = %e, "Failed to decode publisher message");
            return;
        }
    };

    match msg.payload {
        Some(wire_message::Payload::Decided(decided)) => {
            let xt_id = decided
                .xt_id
                .as_ref()
                .map(|id| hex::encode(&id.hash))
                .unwrap_or_default();
            if let Err(e) = coordinator.on_decision(&xt_id, decided.decision).await {
                error!(error = %e, xt_id, "Failed to handle decision");
            }
        }
        Some(wire_message::Payload::StartInstance(start_instance)) => {
            if let Err(e) = coordinator.handle_start_instance(&start_instance).await {
                error!(error = %e, "Failed to handle StartInstance");
            }
        }
        Some(wire_message::Payload::StartPeriod(start_period)) => {
            if let Err(e) = coordinator
                .handle_start_period(
                    PeriodId(start_period.period_id),
                    SuperblockNumber(start_period.superblock_number),
                )
                .await
            {
                error!(error = %e, "Failed to handle StartPeriod");
            }
        }
        Some(wire_message::Payload::MailboxMessage(mailbox_msg)) => {
            if let Err(e) = coordinator.handle_mailbox_message(&mailbox_msg).await {
                error!(error = %e, "Failed to handle MailboxMessage");
            }
        }
        Some(wire_message::Payload::Rollback(rollback)) => {
            if let Err(e) = coordinator
                .handle_rollback(
                    PeriodId(rollback.period_id),
                    rollback.last_finalized_superblock_num,
                    &rollback.last_finalized_superblock_hash,
                )
                .await
            {
                error!(error = %e, "Failed to handle Rollback");
            }
        }
        Some(wire_message::Payload::Vote(_)) => {
            debug!("Received vote from publisher (ignored by sidecar)");
        }
        other => {
            warn!(?other, "Unhandled publisher message type");
        }
    }
}

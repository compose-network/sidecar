//! Publisher message handling for inbound control-plane events.

use std::sync::Arc;

use bytes::Bytes;
use compose_coordinator::coordinator::DefaultCoordinator;
use compose_proto::rollup_v2::{wire_message, WireMessage};
use prost::Message;
use tracing::{debug, error, warn};

/// Process an inbound message from the publisher QUIC connection.
pub(crate) async fn handle_publisher_message(coordinator: Arc<DefaultCoordinator>, data: Bytes) {
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
        Some(wire_message::Payload::Vote(_)) => {
            debug!("Received vote from publisher (ignored by sidecar)");
        }
        other => {
            warn!(?other, "Unhandled publisher message type");
        }
    }
}

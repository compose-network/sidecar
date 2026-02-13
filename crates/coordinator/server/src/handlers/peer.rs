//! Peer-facing HTTP handlers for forwarded XTs, votes, and mailbox messages.

use axum::extract::State;
use axum::Json;
use compose_peer::types::{VoteRequest, XtForwardRequest};
use compose_primitives::{ChainId, SequenceNumber};
use compose_proto::rollup_v2::MailboxMessage;
use prost::Message;

use crate::error::ServerError;
use crate::state::AppState;

/// POST /xt/forward — receive a forwarded XT from a peer sidecar.
pub async fn handle_forward_xt(
    State(state): State<AppState>,
    Json(req): Json<XtForwardRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let txs = req
        .decode_transactions()
        .map_err(|e| ServerError::BadRequest(format!("invalid transactions: {e}")))?;

    state
        .coordinator
        .handle_forwarded_xt(
            &req.instance_id,
            txs,
            ChainId(req.origin_chain),
            SequenceNumber(req.origin_seq),
        )
        .await?;

    Ok(Json(serde_json::json!({ "status": "ok" })))
}

/// POST /xt/vote — receive a vote from a peer sidecar.
pub async fn handle_peer_vote(
    State(state): State<AppState>,
    Json(req): Json<VoteRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    state
        .coordinator
        .handle_peer_vote(&req.instance_id, ChainId(req.chain_id), req.vote)
        .await?;

    Ok(Json(serde_json::json!({ "status": "ok" })))
}

/// POST /mailbox — receive a CIRC mailbox message from a peer sidecar.
pub async fn handle_mailbox(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, ServerError> {
    let msg = MailboxMessage::decode(body)
        .map_err(|e| ServerError::BadRequest(format!("invalid protobuf: {e}")))?;

    state.coordinator.handle_mailbox_message(&msg).await?;

    Ok(Json(serde_json::json!({ "status": "ok" })))
}

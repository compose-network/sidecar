//! XT submission and status query HTTP handlers.

use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::Json;
use compose_primitives::ChainId;
use serde::{Deserialize, Serialize};

use crate::error::ServerError;
use crate::state::AppState;

/// Request body for XT submission.
#[derive(Debug, Deserialize)]
pub struct XtSubmitRequest {
    /// Map of `chain_id` → list of hex-encoded RLP transactions.
    pub transactions: HashMap<u64, Vec<String>>,
}

/// Response body for XT submission.
#[derive(Debug, Serialize)]
pub struct XtSubmitResponse {
    pub instance_id: String,
    pub status: String,
}

/// `POST /xt` — submit a cross-chain transaction.
pub async fn handle_submit_xt(
    State(state): State<AppState>,
    Json(req): Json<XtSubmitRequest>,
) -> Result<Json<XtSubmitResponse>, ServerError> {
    // Decode hex transactions.
    let mut txs: HashMap<ChainId, Vec<Vec<u8>>> = HashMap::new();
    for (chain_id, hex_txs) in &req.transactions {
        let mut decoded = Vec::new();
        for hex_tx in hex_txs {
            let bytes = hex::decode(hex_tx.trim_start_matches("0x"))
                .map_err(|e| ServerError::BadRequest(format!("invalid hex: {e}")))?;
            decoded.push(bytes);
        }
        if !decoded.is_empty() {
            txs.insert(ChainId(*chain_id), decoded);
        }
    }

    if txs.is_empty() {
        return Err(ServerError::BadRequest("no transactions".to_string()));
    }

    let instance_id = state.coordinator.submit_xt(txs).await?;

    Ok(Json(XtSubmitResponse {
        instance_id,
        status: "submitted".to_string(),
    }))
}

/// `GET /xt/{instance_id}` — query XT status.
pub async fn handle_get_xt_status(
    State(state): State<AppState>,
    Path(instance_id): Path<String>,
) -> Result<Json<compose_coordinator::model::xt_status::XtStatusResponse>, ServerError> {
    let resp = state.coordinator.get_xt_status(&instance_id).await?;
    Ok(Json(resp))
}

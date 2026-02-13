//! Builder polling HTTP handler.

use axum::extract::State;
use axum::Json;
use compose_primitives::{BuilderPollRequest, BuilderPollResponse};

use crate::error::ServerError;
use crate::state::AppState;

/// POST /transactions — builder poll for committed transactions.
pub async fn handle_builder_poll(
    State(state): State<AppState>,
    Json(req): Json<BuilderPollRequest>,
) -> Result<Json<BuilderPollResponse>, ServerError> {
    let resp = state.coordinator.handle_builder_poll(&req).await?;
    Ok(Json(resp))
}

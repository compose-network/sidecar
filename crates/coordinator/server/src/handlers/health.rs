//! Health and readiness probe handlers.

use axum::Json;
use serde_json::{json, Value};

/// GET /health — liveness probe.
pub async fn handle_health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// GET /ready — readiness probe.
pub async fn handle_ready() -> Json<Value> {
    Json(json!({ "status": "ready" }))
}

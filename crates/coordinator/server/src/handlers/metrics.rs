//! Prometheus metrics exposition endpoint.

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use prometheus_client::encoding::text::encode;

use crate::state::AppState;

/// GET /metrics — expose all registered Prometheus metrics in text format.
pub async fn handle_metrics(State(state): State<AppState>) -> Response {
    let Some(registry) = &state.registry else {
        return (StatusCode::NOT_FOUND, "metrics not configured").into_response();
    };

    let mut body = String::new();
    let registry = registry.lock().await;
    if encode(&mut body, &registry).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "failed to encode metrics").into_response();
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}

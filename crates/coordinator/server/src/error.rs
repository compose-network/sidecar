//! HTTP error types and coordinator-to-server error mapping.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };

        let body = axum::Json(json!({ "error": message }));
        (status, body).into_response()
    }
}

impl From<compose_coordinator::CoordinatorError> for ServerError {
    fn from(err: compose_coordinator::CoordinatorError) -> Self {
        match err {
            compose_coordinator::CoordinatorError::InstanceNotFound(id) => {
                Self::NotFound(format!("XT not found: {id}"))
            }
            compose_coordinator::CoordinatorError::NoTransactions => {
                Self::BadRequest("no transactions provided".to_string())
            }
            compose_coordinator::CoordinatorError::PublisherNotConnected => {
                Self::Internal("publisher not connected".to_string())
            }
            other => Self::Internal(other.to_string()),
        }
    }
}

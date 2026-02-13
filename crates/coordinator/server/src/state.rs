//! Shared server state container.

use std::sync::Arc;

use compose_coordinator::coordinator::DefaultCoordinator;

/// Shared application state passed to all HTTP handlers.
#[derive(Debug, Clone)]
pub struct AppState {
    pub coordinator: Arc<DefaultCoordinator>,
}

impl AppState {
    pub fn new(coordinator: DefaultCoordinator) -> Self {
        Self {
            coordinator: Arc::new(coordinator),
        }
    }

    pub fn from_arc(coordinator: Arc<DefaultCoordinator>) -> Self {
        Self { coordinator }
    }
}

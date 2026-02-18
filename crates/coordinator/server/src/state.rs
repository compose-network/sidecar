//! Shared server state container.

use std::sync::Arc;

use compose_coordinator::coordinator::DefaultCoordinator;
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

/// Shared application state passed to all HTTP handlers.
#[derive(Debug, Clone)]
pub struct AppState {
    pub coordinator: Arc<DefaultCoordinator>,
    /// Prometheus metrics registry (None when running without metrics).
    pub registry: Option<Arc<Mutex<Registry>>>,
}

impl AppState {
    pub fn new(coordinator: DefaultCoordinator) -> Self {
        Self {
            coordinator: Arc::new(coordinator),
            registry: None,
        }
    }

    pub fn from_arc(coordinator: Arc<DefaultCoordinator>) -> Self {
        Self {
            coordinator,
            registry: None,
        }
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = Some(Arc::new(Mutex::new(registry)));
        self
    }
}

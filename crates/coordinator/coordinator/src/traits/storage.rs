//! Storage trait for pending XT persistence.

use async_trait::async_trait;

use compose_primitives_traits::CoordinatorError;
use crate::model::pending_xt::PendingXt;

/// Storage backend for pending cross-chain transactions.
#[async_trait]
pub trait XtStorage: Send + Sync + 'static {
    /// Insert a new pending XT. Returns error if already exists.
    async fn insert(&self, id: &str, xt: PendingXt) -> Result<(), CoordinatorError>;

    /// Get a pending XT by ID.
    async fn get(&self, id: &str) -> Result<Option<PendingXt>, CoordinatorError>;

    /// Update an existing pending XT.
    async fn update(&self, id: &str, xt: PendingXt) -> Result<(), CoordinatorError>;

    /// Remove a pending XT by ID.
    async fn remove(&self, id: &str) -> Result<(), CoordinatorError>;

    /// List all pending XT IDs.
    async fn list_ids(&self) -> Result<Vec<String>, CoordinatorError>;
}

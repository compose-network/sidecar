//! In-memory storage backend for pending XT records.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::error::CoordinatorError;
use crate::model::pending_xt::PendingXt;
use crate::traits::storage::XtStorage;

/// In-memory storage backend for pending cross-chain transactions.
#[derive(Debug, Default, Clone)]
pub struct InMemoryStorage {
    store: Arc<RwLock<HashMap<String, PendingXt>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl XtStorage for InMemoryStorage {
    async fn insert(&self, id: &str, xt: PendingXt) -> Result<(), CoordinatorError> {
        let mut store = self.store.write().await;
        if store.contains_key(id) {
            return Err(CoordinatorError::InstanceAlreadyPending(id.to_string()));
        }
        store.insert(id.to_string(), xt);
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<PendingXt>, CoordinatorError> {
        let store = self.store.read().await;
        Ok(store.get(id).cloned())
    }

    async fn update(&self, id: &str, xt: PendingXt) -> Result<(), CoordinatorError> {
        let mut store = self.store.write().await;
        store.insert(id.to_string(), xt);
        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<(), CoordinatorError> {
        let mut store = self.store.write().await;
        store.remove(id);
        Ok(())
    }

    async fn list_ids(&self) -> Result<Vec<String>, CoordinatorError> {
        let store = self.store.read().await;
        Ok(store.keys().cloned().collect())
    }
}

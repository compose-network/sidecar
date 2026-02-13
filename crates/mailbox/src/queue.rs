//! In-memory mailbox queue implementation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use compose_proto::rollup_v2::MailboxMessage;
use tokio::sync::RwLock;

use crate::error::MailboxError;
use crate::traits::MailboxQueue;

/// In-memory mailbox queue. Stores messages keyed by instance ID.
#[derive(Debug, Default)]
pub struct InMemoryQueue {
    messages: Arc<RwLock<HashMap<Vec<u8>, Vec<MailboxMessage>>>>,
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MailboxQueue for InMemoryQueue {
    async fn record(&self, msg: &MailboxMessage) -> Result<(), MailboxError> {
        let mut map = self.messages.write().await;
        map.entry(msg.instance_id.clone())
            .or_default()
            .push(msg.clone());
        Ok(())
    }

    async fn pending(&self, instance_id: &[u8]) -> Result<Vec<MailboxMessage>, MailboxError> {
        let map = self.messages.read().await;
        Ok(map.get(instance_id).cloned().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn record_and_retrieve() {
        let queue = InMemoryQueue::new();
        let msg = MailboxMessage {
            instance_id: b"test-id".to_vec(),
            source_chain: 901,
            destination_chain: 902,
            label: "transfer".to_string(),
            ..Default::default()
        };

        queue.record(&msg).await.unwrap();
        let pending = queue.pending(b"test-id").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].source_chain, 901);
    }
}

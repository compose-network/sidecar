//! Deferred nonce reservation for delivery-time transaction building.

use std::future::Future;

use tokio::sync::Mutex;

use compose_primitives_traits::CoordinatorError;

/// Deferred nonce manager that assigns nonces at delivery time.
///
/// The nonce base is lazily fetched from the RPC on first use within a block
/// and then locally incremented for each reserved slot.
#[derive(Debug)]
pub struct DeferredNonceManager {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    current_block: u64,
    base_nonce: u64,
    next_nonce: u64,
    base_set: bool,
}

impl DeferredNonceManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                current_block: 0,
                base_nonce: 0,
                next_nonce: 0,
                base_set: false,
            }),
        }
    }

    /// Reset the nonce cursor when a new block number is observed.
    pub async fn reset_for_block(&self, block_number: u64) {
        let mut inner = self.inner.lock().await;
        if block_number == inner.current_block {
            return;
        }
        inner.current_block = block_number;
        inner.base_nonce = 0;
        inner.next_nonce = 0;
        inner.base_set = false;
    }

    /// Reserve a contiguous nonce range and return the starting nonce.
    ///
    /// `fetch_base` is called once per block to get the pending nonce from the
    /// RPC node.
    pub async fn reserve<F, Fut>(
        &self,
        count: usize,
        fetch_base: F,
    ) -> Result<u64, CoordinatorError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<u64, CoordinatorError>>,
    {
        if count == 0 {
            return Ok(0);
        }

        let mut inner = self.inner.lock().await;

        if !inner.base_set {
            let base = fetch_base().await?;
            inner.base_nonce = base;
            inner.next_nonce = base;
            inner.base_set = true;
        }

        let start = inner.next_nonce;
        inner.next_nonce += count as u64;
        Ok(start)
    }
}

impl Default for DeferredNonceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reserve_increments() {
        let mgr = DeferredNonceManager::new();
        mgr.reset_for_block(100).await;

        let start = mgr.reserve(3, || async { Ok(10) }).await.unwrap();
        assert_eq!(start, 10);

        let start2 = mgr
            .reserve(2, || async { panic!("should not call") })
            .await
            .unwrap();
        assert_eq!(start2, 13);
    }

    #[tokio::test]
    async fn reset_clears_state() {
        let mgr = DeferredNonceManager::new();
        mgr.reset_for_block(100).await;
        mgr.reserve(5, || async { Ok(0) }).await.unwrap();

        mgr.reset_for_block(101).await;
        let start = mgr.reserve(1, || async { Ok(42) }).await.unwrap();
        assert_eq!(start, 42);
    }
}

//! Monotonic nonce reservation for delivery-time putInbox transaction building.

use std::future::Future;

use tokio::sync::Mutex;

use compose_primitives_traits::CoordinatorError;

/// Monotonic nonce manager that assigns nonces at delivery time.
///
/// The nonce base is fetched from the RPC once on first use and then only
/// incremented locally. The counter is intentionally not reset between blocks.
///
/// `putInbox` transactions are injected directly into the flashblock builder
/// and never enter the standard mempool. Re-fetching `eth_getTransactionCount`
/// on each block returns the confirmed nonce rather than the true pending nonce,
/// which causes collisions when multiple XTs are committed across block
/// boundaries. A monotonic local counter avoids this entirely.
///
/// Call [`resync`] to re-seed the counter from the RPC after a known dropped
/// or rejected transaction.
#[derive(Debug)]
pub struct DeferredNonceManager {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    next_nonce: u64,
    initialized: bool,
}

impl DeferredNonceManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                next_nonce: 0,
                initialized: false,
            }),
        }
    }

    /// Reserve a contiguous nonce range and return the starting nonce.
    ///
    /// `fetch_base` is called exactly once in the lifetime of this manager to
    /// seed the counter from the RPC pending nonce. All subsequent calls
    /// increment the counter locally without touching the RPC.
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

        if !inner.initialized {
            inner.next_nonce = fetch_base().await?;
            inner.initialized = true;
        }

        let start = inner.next_nonce;
        inner.next_nonce += count as u64;
        Ok(start)
    }

    /// Re-sync the nonce counter from the RPC.
    ///
    /// Call this after a putInbox transaction is known to have been dropped or
    /// rejected so that future deliveries use the correct base nonce.
    pub async fn resync<F, Fut>(&self, fetch_base: F) -> Result<(), CoordinatorError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<u64, CoordinatorError>>,
    {
        let mut inner = self.inner.lock().await;
        inner.next_nonce = fetch_base().await?;
        inner.initialized = true;
        Ok(())
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
    async fn reserve_increments_without_re_fetching() {
        let mgr = DeferredNonceManager::new();

        // First call seeds from RPC.
        let n1 = mgr.reserve(3, || async { Ok(10) }).await.unwrap();
        assert_eq!(n1, 10);

        // Second call must NOT invoke fetch_base again.
        let n2 = mgr
            .reserve(2, || async { panic!("should not fetch again") })
            .await
            .unwrap();
        assert_eq!(n2, 13);

        // Third call — crossing a "block boundary" is irrelevant.
        let n3 = mgr
            .reserve(1, || async { panic!("should not fetch again") })
            .await
            .unwrap();
        assert_eq!(n3, 15);
    }

    #[tokio::test]
    async fn resync_resets_counter_from_rpc() {
        let mgr = DeferredNonceManager::new();
        mgr.reserve(5, || async { Ok(0) }).await.unwrap();
        assert_eq!(mgr.reserve(1, || async { panic!() }).await.unwrap(), 5);

        // Simulate a dropped tx: re-sync from RPC which now reports nonce 2.
        mgr.resync(|| async { Ok(2) }).await.unwrap();
        let n = mgr
            .reserve(1, || async { panic!("should not fetch") })
            .await
            .unwrap();
        assert_eq!(n, 2);
    }
}

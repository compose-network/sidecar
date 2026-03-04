//! Per-chain state overlay tracking for simulation windows.

use compose_primitives::StateOverride;

/// Accumulated state overlay for a chain within a single block/flashblock
/// window. Gives subsequent simulations the post-state of previously committed
/// XTs.
#[derive(Debug, Clone)]
pub struct ChainOverlay {
    pub block_number: u64,
    pub flashblock_index: u64,
    pub overlay: StateOverride,
}

impl ChainOverlay {
    pub fn new(block_number: u64, flashblock_index: u64) -> Self {
        Self {
            block_number,
            flashblock_index,
            overlay: StateOverride::default(),
        }
    }

    /// Whether this overlay matches the given block/flashblock window.
    pub fn matches(&self, block_number: u64, flashblock_index: u64) -> bool {
        self.block_number == block_number && self.flashblock_index == flashblock_index
    }
}

//! Simulation backend configuration types.

use compose_primitives::ChainId;

/// Configuration for an RPC simulator per chain.
#[derive(Debug, Clone)]
pub struct ChainRpcConfig {
    pub chain_id: ChainId,
    pub rpc_url: String,
}

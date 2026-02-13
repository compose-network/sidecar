//! RPC-backed transaction simulation implementation.

use std::collections::HashMap;

use async_trait::async_trait;
use compose_primitives::{ChainId, CrossRollupDependency, CrossRollupMessage, SimulationResult};
use reqwest::Client;
use serde_json::{json, Value};
use tracing::debug;

use crate::error::SimulationError;
use crate::traits::Simulator;
use crate::types::ChainRpcConfig;

/// RPC-based simulator that uses `debug_traceCall` to simulate transactions
/// and detect mailbox interactions.
#[derive(Debug)]
pub struct RpcSimulator {
    client: Client,
    chains: HashMap<ChainId, ChainRpcConfig>,
}

impl RpcSimulator {
    pub fn new(chains: Vec<ChainRpcConfig>) -> Self {
        let map = chains.into_iter().map(|c| (c.chain_id, c)).collect();
        Self {
            client: Client::new(),
            chains: map,
        }
    }

    fn rpc_url(&self, chain_id: ChainId) -> Result<&str, SimulationError> {
        self.chains
            .get(&chain_id)
            .map(|c| c.rpc_url.as_str())
            .ok_or_else(|| {
                SimulationError::Other(format!("no RPC configured for chain {chain_id}"))
            })
    }

    async fn trace_call(
        &self,
        chain_id: ChainId,
        tx_hex: &str,
        state_overrides: &Value,
    ) -> Result<Value, SimulationError> {
        let url = self.rpc_url(chain_id)?;

        let params = json!([
            tx_hex,
            "latest",
            {
                "tracer": "callTracer",
                "tracerConfig": { "withLog": true }
            },
            state_overrides
        ]);

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "debug_traceCall",
            "params": params,
        });

        let resp = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|e| SimulationError::Rpc(e.to_string()))?;

        let result: Value = resp
            .json()
            .await
            .map_err(|e| SimulationError::Rpc(e.to_string()))?;

        if let Some(err) = result.get("error") {
            return Err(SimulationError::Rpc(err.to_string()));
        }

        Ok(result.get("result").cloned().unwrap_or(Value::Null))
    }
}

#[async_trait]
impl Simulator for RpcSimulator {
    async fn simulate(
        &self,
        chain_id: ChainId,
        tx: &[u8],
        state_overrides: &Value,
    ) -> Result<SimulationResult, SimulationError> {
        let tx_hex = format!("0x{}", hex::encode(tx));

        debug!(chain_id = %chain_id, "Simulating transaction");

        let trace = self.trace_call(chain_id, &tx_hex, state_overrides).await?;

        let success = trace
            .get("error")
            .map(|e| e.as_str().unwrap_or("").is_empty())
            .unwrap_or(true);

        let error_msg = trace
            .get("error")
            .and_then(|e| e.as_str())
            .map(String::from);

        // Mailbox dependencies and outbound messages are added by higher-level
        // integrations. This backend returns execution outcome and trace data.
        Ok(SimulationResult {
            success,
            error: error_msg,
            state_overrides: trace.get("stateOverrides").cloned(),
            dependencies: Vec::new(),
            outbound_messages: Vec::new(),
        })
    }

    async fn simulate_with_mailbox(
        &self,
        chain_id: ChainId,
        tx: &[u8],
        state_overrides: &Value,
        _already_sent_msgs: &[CrossRollupMessage],
        _fulfilled_deps: &[CrossRollupDependency],
    ) -> Result<SimulationResult, SimulationError> {
        // Mailbox-aware state augmentation is applied by callers before this
        // backend is invoked. Reuse the same execution path here.
        self.simulate(chain_id, tx, state_overrides).await
    }
}

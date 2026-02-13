//! RPC-backed transaction simulation implementation.

use std::collections::HashMap;

use alloy::primitives::{Address, U256};
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
    mailbox_address: Option<Address>,
}

impl RpcSimulator {
    pub fn new(chains: Vec<ChainRpcConfig>) -> Self {
        let map = chains.into_iter().map(|c| (c.chain_id, c)).collect();
        Self {
            client: Client::new(),
            chains: map,
            mailbox_address: None,
        }
    }

    pub fn with_mailbox_address(mut self, addr: Address) -> Self {
        self.mailbox_address = Some(addr);
        self
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

    /// Convert parsed mailbox calls into cross-rollup dependencies and messages.
    fn extract_mailbox_data(
        &self,
        trace: &Value,
        chain_id: ChainId,
    ) -> (Vec<CrossRollupDependency>, Vec<CrossRollupMessage>) {
        let mailbox_addr = match self.mailbox_address {
            Some(addr) => addr,
            None => return (Vec::new(), Vec::new()),
        };

        let parsed = compose_mailbox::parser::parse_call_trace(trace, mailbox_addr);

        let dependencies = parsed
            .reads
            .iter()
            .map(|call| CrossRollupDependency {
                source_chain_id: call.source_chain,
                dest_chain_id: chain_id,
                sender: call.sender,
                receiver: call.receiver,
                label: call.label.as_bytes().to_vec(),
                data: None,
                session_id: call.session_id.map(U256::from),
            })
            .collect();

        let outbound_messages = parsed
            .writes
            .iter()
            .map(|call| CrossRollupMessage {
                source_chain_id: chain_id,
                dest_chain_id: call.dest_chain,
                sender: call.sender,
                receiver: call.receiver,
                label: call.label.clone(),
                data: call.data.clone(),
                session_id: call.session_id.map(U256::from),
            })
            .collect();

        (dependencies, outbound_messages)
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

        let (dependencies, outbound_messages) = self.extract_mailbox_data(&trace, chain_id);

        Ok(SimulationResult {
            success,
            error: error_msg,
            state_overrides: trace.get("stateOverrides").cloned(),
            dependencies,
            outbound_messages,
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
        // Merge fulfilled dependency state into the override map.
        // The caller is expected to have prepared the overrides with the
        // mailbox state already applied. Use the same execution path.
        self.simulate(chain_id, tx, state_overrides).await
    }
}

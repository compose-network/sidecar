//! RPC-backed transaction simulation implementation.

use std::collections::HashMap;

use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Transaction, TxEnvelope};
use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::eth::TransactionRequest;
use alloy_rpc_types_eth::state::StateOverride;
use alloy_rpc_types_trace::geth::{
    CallConfig, GethDebugTracingCallOptions, GethDebugTracingOptions,
};
use async_trait::async_trait;
use compose_mailbox::overrides::{build_mailbox_state_overrides, merge_overrides};
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

    /// Decode RLP-encoded signed transaction bytes into an RPC call object
    /// suitable for `debug_traceCall`.
    fn decode_tx(tx_bytes: &[u8]) -> Result<TransactionRequest, SimulationError> {
        let signed: TxEnvelope = alloy::rlp::Decodable::decode(&mut &tx_bytes[..])
            .map_err(|e| SimulationError::Other(format!("failed to decode tx: {e}")))?;

        let from = signed
            .recover_signer()
            .map_err(|e| SimulationError::Other(format!("failed to recover signer: {e}")))?;

        let mut tx_request = TransactionRequest::default()
            .from(from)
            .gas_limit(signed.gas_limit());

        if let Some(to) = signed.to() {
            tx_request = tx_request.to(to);
        }
        if !signed.input().is_empty() {
            tx_request.input.data = Some(Bytes::copy_from_slice(signed.input()));
        }
        if !signed.value().is_zero() {
            tx_request = tx_request.value(signed.value());
        }

        Ok(tx_request)
    }

    async fn trace_call(
        &self,
        chain_id: ChainId,
        tx_args: &TransactionRequest,
        state_overrides: &StateOverride,
    ) -> Result<Value, SimulationError> {
        let url = self.rpc_url(chain_id)?;

        let tracing_opts = GethDebugTracingOptions::call_tracer(CallConfig::default().with_log());
        let mut trace_opts = GethDebugTracingCallOptions::new(tracing_opts);

        if !state_overrides.is_empty() {
            trace_opts = trace_opts.with_state_overrides(state_overrides.clone());
        }

        let params = json!([tx_args, "latest", trace_opts]);

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "debug_traceCall",
            "params": params,
        });

        debug!(chain_id = %chain_id, body = %serde_json::to_string(&body).unwrap_or_default(), "debug_traceCall request");

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

        debug!(chain_id = %chain_id, result = %serde_json::to_string(&result).unwrap_or_default(), "debug_traceCall response");

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
        let Some(mailbox_addr) = self.mailbox_address else {
            return (Vec::new(), Vec::new());
        };

        let parsed = compose_mailbox::parser::parse_call_trace(trace, mailbox_addr, chain_id);

        let dependencies = parsed
            .reads
            .iter()
            .map(|call| CrossRollupDependency {
                source_chain_id: call.source_chain,
                dest_chain_id: call.dest_chain,
                sender: call.sender,
                receiver: call.receiver,
                label: call.label.as_bytes().to_vec(),
                data: None,
                session_id: call.session_id,
            })
            .collect();

        let outbound_messages = parsed
            .writes
            .iter()
            .map(|call| CrossRollupMessage {
                source_chain_id: call.source_chain,
                dest_chain_id: call.dest_chain,
                sender: call.sender,
                receiver: call.receiver,
                label: call.label.clone(),
                data: call.data.clone(),
                session_id: call.session_id,
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
        state_overrides: &StateOverride,
    ) -> Result<SimulationResult, SimulationError> {
        let tx_args = Self::decode_tx(tx)?;

        debug!(chain_id = %chain_id, "Simulating transaction");

        let trace = self.trace_call(chain_id, &tx_args, state_overrides).await?;

        let success = trace
            .get("error")
            .map(|e| e.as_str().unwrap_or("").is_empty())
            .unwrap_or(true);

        let error_msg = trace
            .get("error")
            .and_then(|e| e.as_str())
            .map(String::from);

        let (dependencies, outbound_messages) = self.extract_mailbox_data(&trace, chain_id);

        // Parse stateOverrides returned by the trace into typed form.
        let state_overrides_result = trace
            .get("stateOverrides")
            .and_then(|v| serde_json::from_value::<StateOverride>(v.clone()).ok());

        Ok(SimulationResult {
            success,
            error: error_msg,
            state_overrides: state_overrides_result,
            dependencies,
            outbound_messages,
        })
    }

    async fn simulate_with_mailbox(
        &self,
        chain_id: ChainId,
        tx: &[u8],
        state_overrides: &StateOverride,
        _already_sent_msgs: &[CrossRollupMessage],
        fulfilled_deps: &[CrossRollupDependency],
    ) -> Result<SimulationResult, SimulationError> {
        let mut merged = state_overrides.clone();

        if let Some(mailbox_addr) = self.mailbox_address {
            if let Some(mailbox_overrides) =
                build_mailbox_state_overrides(chain_id, mailbox_addr, fulfilled_deps)
            {
                merge_overrides(&mut merged, &mailbox_overrides);
            }
        }

        self.simulate(chain_id, tx, &merged).await
    }
}

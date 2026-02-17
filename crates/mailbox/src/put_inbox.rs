//! Signed `putInbox` transaction builder using alloy.

use alloy::eips::{BlockId, Encodable2718};
use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use compose_primitives::{ChainId, CrossRollupDependency};
use compose_primitives_traits::{CoordinatorError, PutInboxBuilder};

use crate::builder::encode_put_inbox_calldata;

/// Builds signed `putInbox` transactions for cross-rollup dependency delivery.
#[derive(Clone)]
pub struct PutInboxTxBuilder {
    chain_id: ChainId,
    rpc_url: String,
    mailbox_address: Address,
    signer: PrivateKeySigner,
    signer_address: Address,
}

impl std::fmt::Debug for PutInboxTxBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutInboxTxBuilder")
            .field("chain_id", &self.chain_id)
            .field("mailbox_address", &self.mailbox_address)
            .field("signer_address", &self.signer_address)
            .finish()
    }
}

impl PutInboxTxBuilder {
    pub fn new(
        chain_id: ChainId,
        rpc_url: String,
        mailbox_address: String,
        coordinator_key: String,
    ) -> Result<Self, CoordinatorError> {
        let mailbox_address: Address = mailbox_address
            .parse()
            .map_err(|e| CoordinatorError::Other(format!("invalid mailbox address: {e}")))?;

        let key = if coordinator_key.starts_with("0x") {
            coordinator_key
        } else {
            format!("0x{coordinator_key}")
        };
        let signer: PrivateKeySigner = key
            .parse()
            .map_err(|e| CoordinatorError::Other(format!("invalid coordinator key: {e}")))?;
        let signer_address = signer.address();

        Ok(Self {
            chain_id,
            rpc_url,
            mailbox_address,
            signer,
            signer_address,
        })
    }
}

#[async_trait]
impl PutInboxBuilder for PutInboxTxBuilder {
    async fn pending_nonce_at(&self) -> Result<u64, CoordinatorError> {
        let url = self
            .rpc_url
            .parse()
            .map_err(|e| CoordinatorError::Other(format!("invalid chain rpc url: {e}")))?;
        let provider = ProviderBuilder::new().connect_http(url);

        provider
            .get_transaction_count(self.signer_address)
            .block_id(BlockId::pending())
            .await
            .map_err(|e| CoordinatorError::Nonce(format!("get pending nonce: {e}")))
    }

    async fn build_put_inbox_tx_with_nonce(
        &self,
        dep: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let calldata = encode_put_inbox_calldata(dep)
            .map_err(|e| CoordinatorError::Mailbox(format!("encode putInbox calldata: {e}")))?;

        let tx = TransactionRequest::default()
            .with_from(self.signer_address)
            .with_to(self.mailbox_address)
            .with_chain_id(self.chain_id.0)
            .with_nonce(nonce)
            .gas_limit(500_000)
            .max_priority_fee_per_gas(1_000_000_000)
            .max_fee_per_gas(20_000_000_000)
            .with_input(calldata);

        let wallet = EthereumWallet::new(self.signer.clone());
        let signed = tx
            .build(&wallet)
            .await
            .map_err(|e| CoordinatorError::Other(format!("build signed putInbox tx: {e}")))?;

        Ok(signed.encoded_2718())
    }
}

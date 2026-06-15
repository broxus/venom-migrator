use everscale_jrpc_transaction_consumer::TransactionConsumerOptions;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tycho_types::models::StdAddr;
use tycho_util::serde_helpers;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RelayConfig {
    pub transaction_consumer: TransactionConsumerOptions,

    pub wallet: WalletConfig,
    pub deposit: DepositConfig,

    pub venom_rpc: String,
    pub tycho_rpc: String,

    #[serde(with = "serde_helpers::string")]
    pub min_transaction_lt: u64,

    #[serde(with = "serde_helpers::humantime")]
    pub rpc_retry_interval: Duration,
}

impl Default for RelayConfig {
    #[inline]
    fn default() -> Self {
        Self {
            transaction_consumer: Default::default(),
            wallet: Default::default(),
            deposit: Default::default(),
            venom_rpc: Default::default(),
            tycho_rpc: Default::default(),
            min_transaction_lt: 0,
            rpc_retry_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct DepositConfig {
    pub owner: StdAddr,
    pub token_roots: Vec<TokenRouteConfig>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct TokenRouteConfig {
    pub source_root: StdAddr,
    pub target_root: StdAddr,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct WalletConfig {
    pub address: StdAddr,
    #[serde(with = "serde_helpers::string")]
    pub min_required_balance: u128,
    pub transfer_batch_size: usize,
    #[serde(with = "serde_helpers::humantime")]
    pub transfer_batch_flush_interval: Duration,
}

impl Default for WalletConfig {
    #[inline]
    fn default() -> Self {
        Self {
            address: Default::default(),
            min_required_balance: 10_000_000_000,
            transfer_batch_size: 50,
            transfer_batch_flush_interval: Duration::from_secs(30),
        }
    }
}

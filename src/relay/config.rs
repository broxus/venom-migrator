use everscale_jrpc_transaction_consumer::{StartFrom, TransactionConsumerOptions};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;
use tycho_util::serde_helpers;

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct RelayConfig {
    pub transaction_consumer: TransactionConsumerConfig,

    pub wallet: WalletConfig,
    pub deposit: DepositConfig,

    pub venom_rpc: String,
    pub tycho_rpc: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionConsumerConfig {
    pub start_from: StartFrom,
    pub batch_size: u8,
}

impl Default for TransactionConsumerConfig {
    fn default() -> Self {
        let options = TransactionConsumerOptions::default();
        Self {
            start_from: options.start_from,
            batch_size: options.batch_size,
        }
    }
}

impl From<TransactionConsumerConfig> for TransactionConsumerOptions {
    fn from(value: TransactionConsumerConfig) -> Self {
        Self {
            start_from: value.start_from,
            batch_size: value.batch_size,
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
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
pub struct WalletConfig {
    pub secret: HashBytes,
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
            secret: Default::default(),
            min_required_balance: 10_000_000_000,
            transfer_batch_size: 50,
            transfer_batch_flush_interval: Duration::from_secs(30),
        }
    }
}

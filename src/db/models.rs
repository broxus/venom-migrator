use crate::relay::models::{NativeTransfer, TokenTransfer};
use anyhow::Context;
use bigdecimal::BigDecimal;
use num_traits::ToPrimitive;
use std::str::FromStr;
use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;

pub struct NativeTransferFromDb {
    pub transaction_hash: String,
    pub transaction_lt: BigDecimal,
    pub transaction_time: BigDecimal,
    pub sender_wc: i32,
    pub sender_account: String,
    pub recipient_wc: i32,
    pub recipient_account: String,
    pub value: BigDecimal,
}

impl TryFrom<NativeTransferFromDb> for NativeTransfer {
    type Error = anyhow::Error;

    fn try_from(value: NativeTransferFromDb) -> Result<Self, Self::Error> {
        Ok(Self {
            tx_hash: HashBytes::from_str(&value.transaction_hash)?,
            sender: StdAddr::new(
                i8::try_from(value.sender_wc)?,
                HashBytes::from_str(&value.sender_account).context("invalid sender")?,
            ),
            recipient: StdAddr::new(
                i8::try_from(value.recipient_wc)?,
                HashBytes::from_str(&value.recipient_account).context("invalid recipient")?,
            ),
            amount: value.value.to_u128().context("invalid transfer value")?,
            lt: value
                .transaction_lt
                .to_u64()
                .context("invalid transaction lt")?,
            now: value
                .transaction_time
                .to_u32()
                .context("invalid transaction time")?,
        })
    }
}

pub struct TokenTransferFromDb {
    pub transaction_hash: String,
    pub transaction_lt: BigDecimal,
    pub transaction_time: BigDecimal,
    pub sender_wc: i32,
    pub sender_account: String,
    pub recipient_wc: i32,
    pub recipient_account: String,
    pub value: BigDecimal,
    pub ticker: String,
    pub source_token_root_wc: i32,
    pub source_token_root_account: String,
    pub target_token_root_wc: i32,
    pub target_token_root_account: String,
    pub source_token_wallet_wc: i32,
    pub source_token_wallet_account: String,
    pub target_token_wallet_wc: i32,
    pub target_token_wallet_account: String,
}

impl TryFrom<TokenTransferFromDb> for TokenTransfer {
    type Error = anyhow::Error;

    fn try_from(value: TokenTransferFromDb) -> Result<Self, Self::Error> {
        Ok(Self {
            tx_hash: HashBytes::from_str(&value.transaction_hash)
                .context("invalid transaction hash")?,
            source_token_root: StdAddr::new(
                i8::try_from(value.source_token_root_wc)?,
                HashBytes::from_str(&value.source_token_root_account)
                    .context("invalid source token root")?,
            ),
            target_token_root: StdAddr::new(
                i8::try_from(value.target_token_root_wc)?,
                HashBytes::from_str(&value.target_token_root_account)
                    .context("invalid target token root")?,
            ),
            source_token_wallet: StdAddr::new(
                i8::try_from(value.source_token_wallet_wc)?,
                HashBytes::from_str(&value.source_token_wallet_account)
                    .context("invalid source token wallet")?,
            ),
            target_token_wallet: StdAddr::new(
                i8::try_from(value.target_token_wallet_wc)?,
                HashBytes::from_str(&value.target_token_wallet_account)
                    .context("invalid target token wallet")?,
            ),
            ticker: value.ticker,
            sender: StdAddr::new(
                i8::try_from(value.sender_wc)?,
                HashBytes::from_str(&value.sender_account).context("invalid sender")?,
            ),
            recipient: StdAddr::new(
                i8::try_from(value.recipient_wc)?,
                HashBytes::from_str(&value.recipient_account).context("invalid recipient")?,
            ),
            amount: value.value.to_u128().context("invalid token value")?,
            lt: value
                .transaction_lt
                .to_u64()
                .context("invalid transaction lt")?,
            now: value
                .transaction_time
                .to_u32()
                .context("invalid transaction time")?,
        })
    }
}

use std::sync::Arc;

use crate::db::SqlxClient;
use ed25519_dalek::SigningKey;
use everscale_jrpc_transaction_consumer::{
    ConsumedTransaction, TransactionConsumer as JrpcTransactionConsumer,
};
use futures_util::StreamExt;
use nekoton_core::contracts::blockchain_context::BlockchainContextBuilder;
use nekoton_core::transport::Transport;
use nekoton_transport::rpc::RpcTransport;
use tycho_types::cell::{CellBuilder, HashBytes};
use tycho_types::models::{MsgInfo, StdAddr, Transaction, TxInfo};
use tycho_types::num::Tokens;
use tycho_util::FastHashMap;

pub mod config;
pub mod models;
pub mod wallet;

use crate::relay::config::RelayConfig;
use crate::relay::models::{
    NativeTransfer, RelayTransfer, TokenTransfer, TokenWalletInfo, TxHandleStatus,
};
use crate::relay::wallet::HighloadWallet;
use crate::utils::abi::UnpackAbiPlain;
use crate::utils::pending_messages::{MessageStatus, PendingMessages};
use crate::utils::token_wallets;
use crate::utils::token_wallets::models::RootTokenContract;

pub async fn run(
    config: &RelayConfig,
    sqlx_client: SqlxClient,
    pending_messages: PendingMessages,
) -> anyhow::Result<()> {
    let transaction_consumer = JrpcTransactionConsumer::from_jrpc(
        config.venom_rpc.clone(),
        sqlx_client.pg_pool(),
        config.transaction_consumer.clone().into(),
    )
    .await?;

    let mut tx_handler = TxHandler::new(config, sqlx_client, pending_messages).await?;
    tx_handler.recover_new_transfers().await?;

    let mut flush_interval = tokio::time::interval(config.wallet.transfer_batch_flush_interval);
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let stream_transactions = transaction_consumer
        .stream_transactions(tx_handler.subscription_addresses())
        .await?;

    futures_util::pin_mut!(stream_transactions);

    loop {
        tokio::select! {
            raw_transaction = stream_transactions.next() => {
                let raw_transaction = match raw_transaction {
                    Some(Ok(raw_transaction)) => raw_transaction,
                    Some(Err(err)) => return Err(err),
                    None => {
                        tx_handler.flush().await?;
                        anyhow::bail!("transaction consumer stream finished");
                    }
                };

                match tx_handler.parse(&raw_transaction).await {
                    Ok(TxHandleStatus::Skipped) => {
                        raw_transaction.commit().await?;
                    }
                    Ok(TxHandleStatus::Parsed(transfer)) => {
                        let is_new = tx_handler.create_transfer_in_db(&transfer).await?;
                        if is_new {
                            tx_handler.push(*transfer);
                        }

                        raw_transaction.commit().await?;

                        if tx_handler.is_batch_full() {
                            tx_handler.flush().await?;
                        }
                    }
                    Err(err) => return Err(err),
                }
            }
            _ = flush_interval.tick() => {
                tx_handler.flush().await?;
            }
        }
    }
}

struct TxHandler {
    owner: StdAddr,
    wallet: HighloadWallet,
    sqlx_client: SqlxClient,
    batch: TxBatch,
    tokens: FastHashMap<StdAddr, TokenWalletInfo>,
}

impl TxHandler {
    async fn new(
        config: &RelayConfig,
        sqlx_client: SqlxClient,
        pending_messages: PendingMessages,
    ) -> anyhow::Result<Self> {
        let venom_ctx = {
            let endpoint = config.venom_rpc.parse()?;
            let transport = RpcTransport::new([endpoint], Default::default(), true).await?;

            let blockchain_config = transport.get_config().await?.config;

            BlockchainContextBuilder::new()
                .with_config(blockchain_config)
                .with_transport(Arc::new(transport))
                .build()?
        };

        let (tycho_transport, tycho_ctx) = {
            let endpoint = config.tycho_rpc.parse()?;
            let transport = RpcTransport::new([endpoint], Default::default(), true).await?;

            let blockchain_config = transport.get_config().await?.config;

            let context = BlockchainContextBuilder::new()
                .with_config(blockchain_config)
                .with_transport(Arc::new(transport.clone()))
                .build()?;

            (transport, context)
        };

        let wallet = HighloadWallet::new(
            Arc::new(SigningKey::from_bytes(config.wallet.secret.as_array())),
            tycho_transport.clone(),
            pending_messages,
            Tokens::new(config.wallet.min_required_balance),
        )?;

        anyhow::ensure!(
            config.wallet.transfer_batch_size <= wallet::MAX_GIFTS,
            "transfer_batch_size={} exceeds highload wallet limit {}",
            config.wallet.transfer_batch_size,
            wallet::MAX_GIFTS,
        );

        let mut tokens = FastHashMap::default();
        for route in &config.deposit.token_roots {
            let mut source_account = venom_ctx.clone().get_account(&route.source_root).await?;
            let mut target_account = tycho_ctx.clone().get_account(&route.target_root).await?;

            let mut source_contract = RootTokenContract(&mut source_account);
            let mut target_contract = RootTokenContract(&mut target_account);

            let source_token_wallet = source_contract.wallet_of(config.deposit.owner.clone())?;
            let target_token_wallet = target_contract.wallet_of(wallet.address().clone())?;

            let source_ticker = source_contract.symbol()?;
            let target_ticker = target_contract.symbol()?;

            anyhow::ensure!(
                source_ticker == target_ticker,
                "token route ticker mismatch: source_root={}, target_root={}, source_ticker={}, target_ticker={}",
                route.source_root,
                route.target_root,
                source_ticker,
                target_ticker,
            );

            tokens.insert(
                source_token_wallet.clone(),
                TokenWalletInfo {
                    ticker: source_ticker,
                    source_root: route.source_root.clone(),
                    target_root: route.target_root.clone(),
                    source_token_wallet,
                    target_token_wallet,
                },
            );
        }

        Ok(Self {
            owner: config.deposit.owner.clone(),
            tokens,
            wallet,
            sqlx_client,
            batch: TxBatch::new(config.wallet.transfer_batch_size),
        })
    }

    fn subscription_addresses(&self) -> Vec<StdAddr> {
        let mut subscriptions = vec![self.owner.clone()];
        for token in self.tokens.keys() {
            subscriptions.push(token.clone())
        }
        subscriptions
    }

    async fn parse(&self, raw_transaction: &ConsumedTransaction) -> anyhow::Result<TxHandleStatus> {
        let tx = &raw_transaction.transaction;
        let account = &raw_transaction.account;

        anyhow::ensure!(
            account.address == tx.account,
            "JRPC returned transaction for a different account"
        );

        let cell = CellBuilder::build_from(tx)?;
        let tx_hash = *cell.repr_hash();

        let TxInfo::Ordinary(info) = tx.load_info()? else {
            return Ok(TxHandleStatus::Skipped);
        };

        if info.aborted {
            return Ok(TxHandleStatus::Skipped);
        }

        let Some(in_msg) = tx.load_in_msg()? else {
            return Ok(TxHandleStatus::Skipped);
        };

        let MsgInfo::Int(header) = &in_msg.info else {
            return Ok(TxHandleStatus::Skipped);
        };

        if header.bounced {
            return Ok(TxHandleStatus::Skipped);
        }

        if account == &self.owner {
            let Some(parsed) = self.parse_native_transfer(account, tx, tx_hash, header) else {
                return Ok(TxHandleStatus::Skipped);
            };

            return Ok(TxHandleStatus::Parsed(Box::new(RelayTransfer::Native(
                Box::new(parsed),
            ))));
        }

        if let Some(token_info) = self.tokens.get(account) {
            let Some(parsed) = self.parse_token_transfer(tx, tx_hash, token_info, in_msg.body)
            else {
                return Ok(TxHandleStatus::Skipped);
            };

            return Ok(TxHandleStatus::Parsed(Box::new(RelayTransfer::Token(
                Box::new(parsed),
            ))));
        };

        Ok(TxHandleStatus::Skipped)
    }

    fn push(&mut self, transfer: RelayTransfer) {
        self.batch.push(transfer);
    }

    async fn create_transfer_in_db(&self, transfer: &RelayTransfer) -> anyhow::Result<bool> {
        let is_new = match transfer {
            RelayTransfer::Native(transfer) => self.sqlx_client.create_transfer(transfer).await?,
            RelayTransfer::Token(transfer) => {
                self.sqlx_client.create_token_transfer(transfer).await?
            }
        };

        Ok(is_new)
    }

    fn is_batch_full(&self) -> bool {
        self.batch.is_full()
    }

    async fn recover_new_transfers(&mut self) -> anyhow::Result<()> {
        let transfers = self.sqlx_client.load_new_relay_transfers().await?;
        if transfers.is_empty() {
            return Ok(());
        }

        tracing::info!(
            count = transfers.len(),
            "recovering new relay transfers from database"
        );

        for transfer in transfers {
            self.push(transfer);

            if self.is_batch_full() {
                self.flush().await?;
            }
        }

        self.flush().await
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        let transfers = std::mem::take(&mut self.batch.transfers);

        if transfers.is_empty() {
            return Ok(());
        }

        // flush errors are fatal for relay::run; after this point failed batches
        // must be recovered from DB instead of in-memory state.
        let mut native_hashes = Vec::new();
        let mut token_hashes = Vec::new();

        for transfer in &transfers {
            match transfer {
                RelayTransfer::Native(transfer) => native_hashes.push(transfer.tx_hash),
                RelayTransfer::Token(transfer) => token_hashes.push(transfer.tx_hash),
            }
        }

        let msg = self.wallet.prepare_message(transfers, 60).await?;
        let msg_hash = *CellBuilder::build_from(&msg.message)?.repr_hash();

        self.sqlx_client
            .mark_relay_transfers_pending(&native_hashes, &token_hashes, &msg_hash)
            .await?;

        match self
            .wallet
            .send_message(&msg.message, msg.expire_at)
            .await?
        {
            MessageStatus::Delivered => {}
            MessageStatus::Expired => {
                self.sqlx_client
                    .mark_relay_transfers_expired(&native_hashes, &token_hashes, &msg_hash)
                    .await?;
            }
        }

        Ok(())
    }

    fn parse_native_transfer(
        &self,
        account: &StdAddr,
        tx: &Transaction,
        tx_hash: HashBytes,
        header: &tycho_types::models::IntMsgInfo,
    ) -> Option<NativeTransfer> {
        let dst = header.dst.as_std()?;

        if dst != account {
            return None;
        }

        let recipient = header.src.as_std().cloned()?;

        let amount = header.value.tokens.into_inner();

        if amount == 0 {
            return None;
        }

        Some(NativeTransfer {
            tx_hash,
            recipient,
            amount,
            lt: tx.lt,
            now: tx.now,
        })
    }

    fn parse_token_transfer(
        &self,
        tx: &Transaction,
        tx_hash: HashBytes,
        token_wallet: &TokenWalletInfo,
        body: tycho_types::cell::CellSlice<'_>,
    ) -> Option<TokenTransfer> {
        let Ok(inputs) = token_wallets::accept_transfer().decode_internal_input(body) else {
            return None;
        };

        let transfer: token_wallets::AcceptTransferInputs = inputs.unpack().ok()?;

        if transfer.amount == 0 {
            return None;
        }

        Some(TokenTransfer {
            tx_hash,
            source_token_root: token_wallet.source_root.clone(),
            target_token_root: token_wallet.target_root.clone(),
            source_token_wallet: token_wallet.source_token_wallet.clone(),
            target_token_wallet: token_wallet.target_token_wallet.clone(),
            ticker: token_wallet.ticker.clone(),
            recipient: transfer.sender,
            amount: transfer.amount,
            lt: tx.lt,
            now: tx.now,
        })
    }
}

struct TxBatch {
    transfers: Vec<RelayTransfer>,
    max_len: usize,
}

impl TxBatch {
    fn new(max_len: usize) -> Self {
        Self {
            transfers: Vec::with_capacity(max_len),
            max_len,
        }
    }

    fn push(&mut self, transfer: RelayTransfer) {
        self.transfers.push(transfer);
    }

    fn is_full(&self) -> bool {
        self.transfers.len() >= self.max_len
    }
}

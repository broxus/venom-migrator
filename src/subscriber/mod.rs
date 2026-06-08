use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use futures_util::future::BoxFuture;
use tokio::sync::Notify;
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};
use tycho_types::cell::{CellSlice, HashBytes};
use tycho_types::models::{Message, MsgInfo, ShardIdent, StdAddr, TxInfo};

use crate::db::{SqlxClient, TransferConfirmation};
use crate::utils::abi::UnpackAbiPlain;
use crate::utils::pending_messages::PendingMessages;
use crate::utils::token_wallets;

const SYNC_READY_MAX_LAG_SEC: u32 = 60;

pub struct LightSubscriber {
    sqlx_client: SqlxClient,
    pending_messages: PendingMessages,
    wallet_address: StdAddr,
    sync_ready: Arc<Notify>,
    sync_notified: AtomicBool,
}

pub struct DeliveredMessage {
    account: HashBytes,
    message_hash: HashBytes,
}

pub struct LightSubscriberPrepared {
    shard: ShardIdent,
    gen_utime: u32,
    delivered_messages: Vec<DeliveredMessage>,
    failed_message_hashes: Vec<HashBytes>,
    completed_native_transfers: Vec<TransferConfirmation>,
    completed_token_transfers: Vec<TransferConfirmation>,
}

impl LightSubscriber {
    pub fn new(
        sqlx_client: SqlxClient,
        pending_messages: PendingMessages,
        wallet_address: StdAddr,
        sync_ready: Arc<Notify>,
    ) -> Self {
        Self {
            sqlx_client,
            pending_messages,
            wallet_address,
            sync_ready,
            sync_notified: AtomicBool::new(false),
        }
    }

    async fn prepare_block_impl(
        &self,
        cx: &BlockSubscriberContext,
    ) -> Result<LightSubscriberPrepared> {
        tracing::trace!(
            block_id = %cx.block.id(),
            mc_block_id = %cx.mc_block_id,
            "preparing block"
        );

        let block_info = cx.block.load_info()?;
        let mut delivered_messages = Vec::new();
        let mut failed_message_hashes = Vec::new();
        let mut completed_native_transfers = Vec::new();
        let mut completed_token_transfers = Vec::new();

        if block_info.shard.workchain() != self.wallet_address.workchain as i32 {
            return Ok(LightSubscriberPrepared {
                shard: block_info.shard,
                gen_utime: block_info.gen_utime,
                delivered_messages,
                failed_message_hashes,
                completed_native_transfers,
                completed_token_transfers,
            });
        }

        let extra = cx.block.load_extra()?;

        let account_blocks = extra.account_blocks.load()?;

        for entry in account_blocks.iter() {
            let (_, _, account_block) = entry?;
            let account = account_block.account;

            if account != self.wallet_address.address {
                continue;
            }

            for entry in account_block.transactions.iter() {
                let (_, _, tx) = entry?;

                let tx_hash = *tx.repr_hash();
                let tx = tx.load()?;

                let Some(in_msg) = &tx.in_msg else {
                    continue;
                };

                let message = in_msg.parse::<Message>()?;
                if !message.ty().is_external_in() {
                    continue;
                }

                let in_msg_hash = *in_msg.repr_hash();
                delivered_messages.push(DeliveredMessage {
                    account,
                    message_hash: in_msg_hash,
                });

                let TxInfo::Ordinary(info) = tx.load_info()? else {
                    continue;
                };

                if info.aborted {
                    failed_message_hashes.push(in_msg_hash);
                    continue;
                }

                let (native, token) =
                    parse_out_msgs(&tx, tx_hash, in_msg_hash, &self.wallet_address)?;

                completed_native_transfers.extend(native);
                completed_token_transfers.extend(token);
            }
        }

        Ok(LightSubscriberPrepared {
            shard: block_info.shard,
            gen_utime: block_info.gen_utime,
            delivered_messages,
            failed_message_hashes,
            completed_native_transfers,
            completed_token_transfers,
        })
    }

    async fn handle_block_impl(&self, prepared: LightSubscriberPrepared) -> Result<()> {
        self.sqlx_client
            .mark_relay_transfers_failed(&prepared.failed_message_hashes)
            .await?;

        self.sqlx_client
            .mark_relay_transfers_done(
                &prepared.completed_native_transfers,
                &prepared.completed_token_transfers,
            )
            .await?;

        for message in &prepared.delivered_messages {
            self.pending_messages
                .deliver_message(message.account, message.message_hash);
        }

        self.pending_messages
            .update(&prepared.shard, prepared.gen_utime);

        self.notify_synced(&prepared);

        Ok(())
    }

    fn notify_synced(&self, prepared: &LightSubscriberPrepared) {
        let lag = tycho_util::time::now_sec().saturating_sub(prepared.gen_utime);
        if lag > SYNC_READY_MAX_LAG_SEC {
            return;
        }

        if self.sync_notified.swap(true, Ordering::Relaxed) {
            return;
        }

        self.sync_ready.notify_waiters();
    }
}

fn parse_out_msgs(
    tx: &tycho_types::models::Transaction,
    tx_hash: HashBytes,
    msg_hash: HashBytes,
    wallet_address: &StdAddr,
) -> Result<(Vec<TransferConfirmation>, Vec<TransferConfirmation>)> {
    let mut native = Vec::new();
    let mut token = Vec::new();

    for out_msg in tx.iter_out_msgs() {
        let out_msg = out_msg?;
        let MsgInfo::Int(header) = &out_msg.info else {
            continue;
        };

        if header.src.as_std() != Some(wallet_address) {
            continue;
        }

        if let Some(source_tx_hash) = parse_source_tx_hash(out_msg.body) {
            native.push(TransferConfirmation {
                tx_hash,
                msg_hash,
                source_tx_hash,
            });

            continue;
        }

        if let Some(source_tx_hash) = parse_token_transfer_out_msg(out_msg.body)? {
            token.push(TransferConfirmation {
                tx_hash,
                msg_hash,
                source_tx_hash,
            });
        }
    }

    Ok((native, token))
}

fn parse_token_transfer_out_msg(body: CellSlice<'_>) -> Result<Option<HashBytes>> {
    let Ok(inputs) = token_wallets::transfer().decode_internal_input(body) else {
        return Ok(None);
    };

    let transfer: token_wallets::TransferInputs = match inputs.unpack() {
        Ok(transfer) => transfer,
        Err(_) => return Ok(None),
    };

    Ok(parse_source_tx_hash(transfer.payload.as_slice()?))
}

fn parse_source_tx_hash(mut body: CellSlice<'_>) -> Option<HashBytes> {
    if body.size_bits() != 256 || body.size_refs() != 0 {
        return None;
    }

    body.load_u256().ok()
}

impl BlockSubscriber for LightSubscriber {
    type Prepared = LightSubscriberPrepared;

    type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        Box::pin(self.prepare_block_impl(cx))
    }

    fn handle_block<'a>(
        &'a self,
        _cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(self.handle_block_impl(prepared))
    }
}

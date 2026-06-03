use std::collections::HashMap;
use std::collections::hash_map;
use std::str::FromStr;

use anyhow::Context;
use bigdecimal::BigDecimal;
use num_traits::ToPrimitive;
use tycho_types::cell::HashBytes;

use crate::db::models::{
    NativeTransferFromDb, PendingRelayMessage, PendingTransferFromDb, TokenTransferFromDb,
};
use crate::db::{SqlxClient, TransferConfirmation};
use crate::relay::models::{NativeTransfer, RelayTransfer, TokenTransfer};

impl SqlxClient {
    pub async fn load_pending_relay_messages(&self) -> anyhow::Result<Vec<PendingRelayMessage>> {
        let native = sqlx::query_as!(
            PendingTransferFromDb,
            r#"SELECT
                transaction_hash,
                sending_message_hash as "sending_message_hash!",
                expired_at as "expired_at!"
            FROM transfers
            WHERE status = 'Pending'::transaction_status
                AND sending_message_hash IS NOT NULL
                AND expired_at IS NOT NULL
            ORDER BY created_at, transaction_hash"#
        )
        .fetch_all(&self.pool)
        .await?;

        let token = sqlx::query_as!(
            PendingTransferFromDb,
            r#"SELECT
                transaction_hash,
                sending_message_hash as "sending_message_hash!",
                expired_at as "expired_at!"
            FROM token_transfers
            WHERE status = 'Pending'::transaction_status
                AND sending_message_hash IS NOT NULL
                AND expired_at IS NOT NULL
            ORDER BY created_at, transaction_hash"#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut messages = Vec::new();
        let mut indexes = HashMap::new();

        for transfer in native {
            push_pending_transfer(&mut messages, &mut indexes, transfer, false)?;
        }
        for transfer in token {
            push_pending_transfer(&mut messages, &mut indexes, transfer, true)?;
        }

        Ok(messages)
    }

    pub async fn load_new_relay_transfers(&self) -> anyhow::Result<Vec<RelayTransfer>> {
        let native = sqlx::query_as!(
            NativeTransferFromDb,
            r#"SELECT
                transaction_hash,
                transaction_lt,
                transaction_time,
                sender_wc,
                sender_account,
                recipient_wc,
                recipient_account,
                value
            FROM transfers
            WHERE status = 'New'::transaction_status
            ORDER BY created_at, transaction_hash"#
        )
        .fetch_all(&self.pool)
        .await?;

        let token = sqlx::query_as!(
            TokenTransferFromDb,
            r#"SELECT
                transaction_hash,
                transaction_lt,
                transaction_time,
                sender_wc,
                sender_account,
                recipient_wc,
                recipient_account,
                value,
                ticker,
                source_token_root_wc,
                source_token_root_account,
                target_token_root_wc,
                target_token_root_account,
                source_token_wallet_wc,
                source_token_wallet_account,
                target_token_wallet_wc,
                target_token_wallet_account
            FROM token_transfers
            WHERE status = 'New'::transaction_status
            ORDER BY created_at, transaction_hash"#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut transfers = Vec::with_capacity(native.len() + token.len());

        for transfer in native {
            transfers.push(RelayTransfer::Native(Box::new(transfer.try_into()?)));
        }
        for transfer in token {
            transfers.push(RelayTransfer::Token(Box::new(transfer.try_into()?)));
        }

        Ok(transfers)
    }

    pub async fn create_transfer(&self, payload: &NativeTransfer) -> anyhow::Result<bool> {
        let res = sqlx::query!(
            r#"INSERT INTO transfers (
                transaction_hash,
                transaction_lt,
                transaction_time,
                sender_wc,
                sender_account,
                recipient_wc,
                recipient_account,
                value,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'New'::transaction_status)
            ON CONFLICT DO NOTHING
            RETURNING transaction_hash"#,
            payload.tx_hash.to_string(),
            BigDecimal::from(payload.lt),
            BigDecimal::from(payload.now),
            payload.sender.workchain as i32,
            payload.sender.address.to_string(),
            payload.recipient.workchain as i32,
            payload.recipient.address.to_string(),
            BigDecimal::from(payload.amount),
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(res.is_some())
    }

    pub async fn create_token_transfer(&self, payload: &TokenTransfer) -> anyhow::Result<bool> {
        let res = sqlx::query!(
            r#"INSERT INTO token_transfers (
                transaction_hash,
                transaction_lt,
                transaction_time,
                sender_wc,
                sender_account,
                recipient_wc,
                recipient_account,
                value,
                ticker,
                source_token_root_wc,
                source_token_root_account,
                target_token_root_wc,
                target_token_root_account,
                source_token_wallet_wc,
                source_token_wallet_account,
                target_token_wallet_wc,
                target_token_wallet_account,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 'New'::transaction_status)
            ON CONFLICT DO NOTHING
            RETURNING transaction_hash"#,
            payload.tx_hash.to_string(),
            BigDecimal::from(payload.lt),
            BigDecimal::from(payload.now),
            payload.sender.workchain as i32,
            payload.sender.address.to_string(),
            payload.recipient.workchain as i32,
            payload.recipient.address.to_string(),
            BigDecimal::from(payload.amount),
            payload.ticker.as_str(),
            payload.source_token_root.workchain as i32,
            payload.source_token_root.address.to_string(),
            payload.target_token_root.workchain as i32,
            payload.target_token_root.address.to_string(),
            payload.source_token_wallet.workchain as i32,
            payload.source_token_wallet.address.to_string(),
            payload.target_token_wallet.workchain as i32,
            payload.target_token_wallet.address.to_string(),
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(res.is_some())
    }

    pub async fn mark_relay_transfers_pending(
        &self,
        native_tx_hashes: &[HashBytes],
        token_tx_hashes: &[HashBytes],
        message_hash: &HashBytes,
        expired_at: u32,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        let expired_at = BigDecimal::from(expired_at);

        for tx_hash in native_tx_hashes {
            let res = sqlx::query!(
                r#"UPDATE transfers
                SET status = 'Pending'::transaction_status,
                    sending_message_hash = $2,
                    expired_at = $3,
                    updated_at = current_timestamp
                WHERE transaction_hash = $1 AND status = 'New'::transaction_status
                RETURNING transaction_hash"#,
                tx_hash.to_string(),
                message_hash.to_string(),
                expired_at,
            )
            .fetch_optional(&mut *tx)
            .await?;

            anyhow::ensure!(
                res.is_some(),
                "failed to mark transfer as pending: {}",
                tx_hash
            );
        }

        for tx_hash in token_tx_hashes {
            let res = sqlx::query!(
                r#"UPDATE token_transfers
                SET status = 'Pending'::transaction_status,
                    sending_message_hash = $2,
                    expired_at = $3,
                    updated_at = current_timestamp
                WHERE transaction_hash = $1 AND status = 'New'::transaction_status
                RETURNING transaction_hash"#,
                tx_hash.to_string(),
                message_hash.to_string(),
                expired_at,
            )
            .fetch_optional(&mut *tx)
            .await?;

            anyhow::ensure!(
                res.is_some(),
                "failed to mark token transfer as pending: {}",
                tx_hash
            );
        }

        tx.commit().await?;

        Ok(())
    }

    pub async fn mark_relay_transfers_expired(
        &self,
        native_tx_hashes: &[HashBytes],
        token_tx_hashes: &[HashBytes],
        message_hash: &HashBytes,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        for tx_hash in native_tx_hashes {
            let res = sqlx::query!(
                r#"UPDATE transfers
                SET status = 'Expired'::transaction_status,
                    updated_at = current_timestamp
                WHERE transaction_hash = $1
                    AND sending_message_hash = $2
                    AND status IN ('Pending'::transaction_status, 'Expired'::transaction_status)
                RETURNING transaction_hash"#,
                tx_hash.to_string(),
                message_hash.to_string(),
            )
            .fetch_optional(&mut *tx)
            .await?;

            anyhow::ensure!(
                res.is_some(),
                "failed to mark transfer as expired: tx_hash={}, sending_message_hash={}",
                tx_hash,
                message_hash,
            );
        }

        for tx_hash in token_tx_hashes {
            let res = sqlx::query!(
                r#"UPDATE token_transfers
                SET status = 'Expired'::transaction_status,
                    updated_at = current_timestamp
                WHERE transaction_hash = $1
                    AND sending_message_hash = $2
                    AND status IN ('Pending'::transaction_status, 'Expired'::transaction_status)
                RETURNING transaction_hash"#,
                tx_hash.to_string(),
                message_hash.to_string(),
            )
            .fetch_optional(&mut *tx)
            .await?;

            anyhow::ensure!(
                res.is_some(),
                "failed to mark token transfer as expired: tx_hash={}, sending_message_hash={}",
                tx_hash,
                message_hash,
            );
        }

        tx.commit().await?;

        Ok(())
    }

    pub async fn mark_relay_transfers_failed(
        &self,
        message_hashes: &[HashBytes],
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        for message_hash in message_hashes {
            sqlx::query!(
                r#"UPDATE transfers
                SET status = 'Failed'::transaction_status,
                    updated_at = current_timestamp
                WHERE sending_message_hash = $1
                    AND status IN ('Pending'::transaction_status, 'Failed'::transaction_status)"#,
                message_hash.to_string(),
            )
            .execute(&mut *tx)
            .await?;

            sqlx::query!(
                r#"UPDATE token_transfers
                SET status = 'Failed'::transaction_status,
                    updated_at = current_timestamp
                WHERE sending_message_hash = $1
                    AND status IN ('Pending'::transaction_status, 'Failed'::transaction_status)"#,
                message_hash.to_string(),
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    pub async fn mark_relay_transfers_done(
        &self,
        native: &[TransferConfirmation],
        token: &[TransferConfirmation],
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        for confirmation in native {
            let res = sqlx::query!(
                r#"UPDATE transfers
                SET status = 'Done'::transaction_status,
                    sent_transaction_hash = $3,
                    updated_at = current_timestamp
                WHERE transaction_hash = $1
                    AND sending_message_hash = $2
                    AND (
                        status = 'Pending'::transaction_status
                        OR (
                            status = 'Done'::transaction_status
                            AND sent_transaction_hash = $3
                        )
                    )
                RETURNING transaction_hash"#,
                confirmation.source_tx_hash.to_string(),
                confirmation.msg_hash.to_string(),
                confirmation.tx_hash.to_string(),
            )
            .fetch_optional(&mut *tx)
            .await?;

            if res.is_none() {
                let exists = sqlx::query_scalar!(
                    r#"SELECT EXISTS(
                        SELECT 1
                        FROM transfers
                        WHERE transaction_hash = $1
                    ) as "exists!""#,
                    confirmation.source_tx_hash.to_string(),
                )
                .fetch_one(&mut *tx)
                .await?;

                if !exists {
                    tracing::warn!(
                        source_tx_hash = %confirmation.source_tx_hash,
                        sending_message_hash = %confirmation.msg_hash,
                        sent_tx_hash = %confirmation.tx_hash,
                        "skipping transfer confirmation because source transfer is missing in database"
                    );
                    continue;
                }

                anyhow::bail!(
                    "failed to mark transfer as done: source_tx_hash={}, sending_message_hash={}, sent_tx_hash={}",
                    confirmation.source_tx_hash,
                    confirmation.msg_hash,
                    confirmation.tx_hash,
                );
            }
        }

        for confirmation in token {
            let res = sqlx::query!(
                r#"UPDATE token_transfers
                SET status = 'Done'::transaction_status,
                    sent_transaction_hash = $3,
                    updated_at = current_timestamp
                WHERE transaction_hash = $1
                    AND sending_message_hash = $2
                    AND (
                        status = 'Pending'::transaction_status
                        OR (
                            status = 'Done'::transaction_status
                            AND sent_transaction_hash = $3
                        )
                    )
                RETURNING transaction_hash"#,
                confirmation.source_tx_hash.to_string(),
                confirmation.msg_hash.to_string(),
                confirmation.tx_hash.to_string(),
            )
            .fetch_optional(&mut *tx)
            .await?;

            if res.is_none() {
                let exists = sqlx::query_scalar!(
                    r#"SELECT EXISTS(
                        SELECT 1
                        FROM token_transfers
                        WHERE transaction_hash = $1
                    ) as "exists!""#,
                    confirmation.source_tx_hash.to_string(),
                )
                .fetch_one(&mut *tx)
                .await?;

                if !exists {
                    tracing::warn!(
                        source_tx_hash = %confirmation.source_tx_hash,
                        sending_message_hash = %confirmation.msg_hash,
                        sent_tx_hash = %confirmation.tx_hash,
                        "skipping token transfer confirmation because source transfer is missing in database"
                    );
                    continue;
                }

                anyhow::bail!(
                    "failed to mark token transfer as done: source_tx_hash={}, sending_message_hash={}, sent_tx_hash={}",
                    confirmation.source_tx_hash,
                    confirmation.msg_hash,
                    confirmation.tx_hash,
                );
            }
        }

        tx.commit().await?;

        Ok(())
    }
}

fn push_pending_transfer(
    messages: &mut Vec<PendingRelayMessage>,
    indexes: &mut HashMap<HashBytes, usize>,
    transfer: PendingTransferFromDb,
    is_token: bool,
) -> anyhow::Result<()> {
    let message_hash = HashBytes::from_str(&transfer.sending_message_hash)
        .context("invalid pending message hash")?;
    let tx_hash =
        HashBytes::from_str(&transfer.transaction_hash).context("invalid pending tx hash")?;
    let expired_at = transfer
        .expired_at
        .to_u32()
        .context("invalid pending expired_at")?;

    let message = match indexes.entry(message_hash) {
        hash_map::Entry::Occupied(entry) => {
            let message = &mut messages[*entry.get()];
            anyhow::ensure!(
                message.expired_at == expired_at,
                "pending transfers for the same message have different expired_at"
            );
            message
        }
        hash_map::Entry::Vacant(entry) => {
            let index = messages.len();
            entry.insert(index);
            messages.push(PendingRelayMessage {
                message_hash,
                expired_at,
                native_tx_hashes: Vec::new(),
                token_tx_hashes: Vec::new(),
            });
            &mut messages[index]
        }
    };

    if is_token {
        message.token_tx_hashes.push(tx_hash);
    } else {
        message.native_tx_hashes.push(tx_hash);
    }

    Ok(())
}

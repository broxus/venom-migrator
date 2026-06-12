use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use ed25519_dalek::SigningKey;
use nekoton_core::models::ContractState;
use nekoton_core::transport::Transport;
use nekoton_transport::rpc::RpcTransport;
use tycho_types::abi::AbiValue;
use tycho_types::boc::Boc;
use tycho_types::cell::{Cell, CellBuilder, CellFamily, HashBytes};
use tycho_types::dict::Dict;
use tycho_types::models::vm::SendMsgFlags;
use tycho_types::models::{
    AccountState, CurrencyCollection, ExtInMsgInfo, IntAddr, MsgInfo, OwnedMessage,
    OwnedRelaxedMessage, RelaxedIntMsgInfo, RelaxedMsgInfo, SignatureContext, StateInit, StdAddr,
    Transaction,
};
use tycho_types::num::Tokens;
use tycho_types::prelude::*;
use tycho_util::time::now_millis;

use crate::relay::models::RelayTransfer;
use crate::utils::pending_messages::{MessageStatus, MessageStatusRx, PendingMessages};
use crate::utils::token_wallets;

const WALLET_ID: u32 = 0;
pub(super) const MAX_GIFTS: usize = 250;
const DEFAULT_TOKEN_TRANSFER_VALUE: u128 = 200_000_000;
const DEFAULT_DEPLOY_TOKEN_WALLET_VALUE: u128 = 100_000_000;

#[derive(Clone)]
#[repr(transparent)]
pub struct HighloadWallet {
    inner: Arc<Inner>,
}

impl HighloadWallet {
    pub fn compute_address(secret: &HashBytes) -> Result<StdAddr> {
        let key = SigningKey::from_bytes(secret.as_array());
        InitData::from_key(&key.verifying_key())
            .with_wallet_id(WALLET_ID)
            .compute_addr(0)
    }

    pub fn new(
        key: Arc<SigningKey>,
        transport: RpcTransport,
        pending_messages: PendingMessages,
        min_required_balance: Tokens,
    ) -> Result<Self> {
        let address = Self::compute_address(&HashBytes::from_slice(key.as_bytes()))?;

        Ok(Self {
            inner: Arc::new(Inner {
                address,
                key,
                transport,
                pending_messages,
                min_required_balance,
            }),
        })
    }

    pub fn address(&self) -> &StdAddr {
        &self.inner.address
    }

    pub fn add_pending_message(
        &self,
        message_hash: HashBytes,
        expire_at: u32,
    ) -> Result<MessageStatusRx> {
        self.inner
            .pending_messages
            .add_message(self.inner.address.address, message_hash, expire_at)
    }

    pub async fn prepare_message(
        &self,
        transfers: Vec<RelayTransfer>,
        timeout: u32,
    ) -> Result<PreparedMessage> {
        let gifts = transfers
            .into_iter()
            .map(|transfer| match transfer {
                RelayTransfer::Native(transfer) => Ok(Gift {
                    flags: SendMsgFlags::PAY_FEE_SEPARATELY.bits(),
                    bounce: false,
                    destination: transfer.recipient,
                    amount: transfer.amount,
                    body: Some(CellBuilder::build_from(transfer.tx_hash)?),
                    state_init: None,
                }),
                RelayTransfer::Token(transfer) => {
                    let payload = CellBuilder::build_from(transfer.tx_hash)?;
                    let body = token_wallets::transfer()
                        .encode_internal_input(&[
                            AbiValue::uint(128, transfer.amount).named("amount"),
                            AbiValue::address(transfer.recipient).named("recipient"),
                            AbiValue::uint(128, DEFAULT_DEPLOY_TOKEN_WALLET_VALUE)
                                .named("deployWalletValue"),
                            AbiValue::address(self.inner.address.clone()).named("remainingGasTo"),
                            AbiValue::Bool(false).named("notify"),
                            AbiValue::Cell(payload).named("payload"),
                        ])?
                        .build()?;

                    Ok(Gift {
                        flags: SendMsgFlags::PAY_FEE_SEPARATELY.bits(),
                        bounce: true,
                        destination: transfer.target_token_wallet,
                        amount: DEFAULT_TOKEN_TRANSFER_VALUE,
                        body: Some(body),
                        state_init: None,
                    })
                }
            })
            .collect::<Result<Vec<_>>>()?;

        self.prepare_signed_message(gifts, timeout).await
    }

    pub async fn deploy(&self, timeout: u32) -> Result<HashBytes> {
        let message = self.prepare_deploy_message(timeout).await?;

        let tx = self
            .inner
            .transport
            .send_message_reliable(&message.message)
            .await
            .context("failed to send deploy message")?;

        let tx = CellBuilder::build_from(&tx)?;

        Ok(*tx.repr_hash())
    }

    pub async fn send_message(
        &self,
        message: &OwnedMessage,
        expire_at: u32,
    ) -> Result<MessageStatus> {
        let MsgInfo::ExtIn(info) = &message.info else {
            anyhow::bail!("expected external inbound message");
        };

        let account = info
            .dst
            .as_std()
            .context("external message destination is not a std address")?;

        let cell = CellBuilder::build_from(message)?;
        let message_hash = *cell.repr_hash();

        let rx =
            self.inner
                .pending_messages
                .add_message(account.address, message_hash, expire_at)?;

        self.inner
            .transport
            .send_message(message)
            .await
            .context("failed to send external message")?;

        rx.await.context("pending message status sender dropped")
    }

    pub async fn get_transaction(&self, message_hash: &HashBytes) -> Result<Option<Transaction>> {
        self.inner
            .transport
            .get_dst_transaction(message_hash)
            .await
            .context("failed to get transaction by message hash")
    }

    fn prepare_unsigned_message(
        &self,
        gifts: impl IntoIterator<Item = Gift>,
        expire_at: u32,
    ) -> Result<UnsignedHighloadMessage> {
        let gifts = gifts.into_iter().collect::<Vec<_>>();

        if gifts.is_empty() {
            anyhow::bail!("empty highload wallet transfer");
        }

        if gifts.len() > MAX_GIFTS {
            anyhow::bail!("too many highload wallet gifts: {}", gifts.len());
        }

        let init_data =
            InitData::from_key(&self.inner.key.verifying_key()).with_wallet_id(WALLET_ID);

        let (hash, payload) = init_data.make_transfer_payload(gifts.clone(), expire_at)?;

        Ok(UnsignedHighloadMessage {
            gifts,
            payload,
            hash,
            expire_at,
        })
    }

    async fn prepare_signed_message(
        &self,
        gifts: impl IntoIterator<Item = Gift>,
        timeout: u32,
    ) -> Result<PreparedMessage> {
        let this = self.inner.as_ref();

        let gifts = gifts.into_iter().collect::<Vec<_>>();

        let total_amount = gifts.iter().try_fold(0u128, |acc, gift| {
            acc.checked_add(gift.amount)
                .context("highload transfer amount overflow")
        })?;

        let target_balance = Tokens::new(
            total_amount
                .checked_add(this.min_required_balance.into_inner())
                .context("target highload wallet balance overflow")?,
        );

        let expire_at = (now_millis() / 1000) as u32 + timeout.clamp(1, 60);
        let unsigned = self.prepare_unsigned_message(gifts, expire_at)?;

        let state = self.wait_for_state(target_balance).await?;

        let config = this.transport.get_config().await?;
        let global_version = config.config.get_global_version()?;

        let context = SignatureContext {
            global_id: config.global_id,
            capabilities: global_version.capabilities,
        };

        let expire_at = unsigned.expire_at;
        let message = self.sign(unsigned, state.init, context)?;

        Ok(PreparedMessage { message, expire_at })
    }

    async fn prepare_deploy_message(&self, timeout: u32) -> Result<PreparedMessage> {
        let this = self.inner.as_ref();
        let expire_at = (now_millis() / 1000) as u32 + timeout.clamp(1, 60);

        let init_data = InitData::from_key(&this.key.verifying_key()).with_wallet_id(WALLET_ID);
        let (hash, payload) = init_data.make_transfer_payload(Vec::<Gift>::new(), expire_at)?;

        let config = this.transport.get_config().await?;
        let global_version = config.config.get_global_version()?;

        let context = SignatureContext {
            global_id: config.global_id,
            capabilities: global_version.capabilities,
        };

        let unsigned = UnsignedHighloadMessage {
            gifts: Vec::new(),
            payload,
            hash,
            expire_at,
        };

        let message = self.sign(
            unsigned,
            Some(make_state_init(&this.key.verifying_key())?),
            context,
        )?;

        Ok(PreparedMessage { message, expire_at })
    }

    async fn wait_for_state(&self, target_balance: Tokens) -> Result<WalletState> {
        const POLL_INTERVAL: Duration = Duration::from_secs(1);

        let this = self.inner.as_ref();
        let address = &this.address;
        let transport = &this.transport;

        let mut known_lt = None;
        let mut first = true;

        loop {
            'state: {
                let (account, last_transaction_id) =
                    match transport.get_contract_state(address, known_lt).await? {
                        ContractState::Exists {
                            account,
                            last_transaction_id,
                            ..
                        } => (account, last_transaction_id),
                        ContractState::NotExists { .. } => {
                            if std::mem::take(&mut first) {
                                tracing::warn!(
                                    %address,
                                    balance = %Tokens::ZERO,
                                    %target_balance,
                                    "highload wallet balance is not enough, waiting"
                                );
                            }
                            break 'state;
                        }
                        ContractState::Unchanged { .. } => break 'state,
                    };

                known_lt = Some(last_transaction_id.lt);

                let init = match &account.state {
                    AccountState::Uninit => Some(make_state_init(&this.key.verifying_key())?),
                    AccountState::Active(_) => None,
                    AccountState::Frozen(_) => anyhow::bail!("highload wallet is frozen"),
                };

                if account.balance.tokens >= target_balance {
                    return Ok(WalletState { init });
                }

                if std::mem::take(&mut first) {
                    tracing::warn!(
                        %address,
                        balance = %account.balance.tokens,
                        %target_balance,
                        "highload wallet balance is not enough, waiting"
                    );
                } else {
                    tracing::debug!(
                        balance = %account.balance.tokens,
                        %target_balance,
                        "highload wallet balance is not enough"
                    );
                }
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    fn sign(
        &self,
        unsigned: UnsignedHighloadMessage,
        init: Option<StateInit>,
        context: SignatureContext,
    ) -> Result<OwnedMessage> {
        let signature = context.sign(&self.inner.key, unsigned.hash.as_slice());
        unsigned.sign(&signature.to_bytes(), self.inner.address.clone(), init)
    }
}

pub struct PreparedMessage {
    pub message: OwnedMessage,
    pub expire_at: u32,
}

struct Inner {
    address: StdAddr,
    key: Arc<SigningKey>,
    transport: RpcTransport,
    min_required_balance: Tokens,
    pending_messages: PendingMessages,
}

struct WalletState {
    init: Option<StateInit>,
}

#[derive(Clone)]
pub struct Gift {
    pub flags: u8,
    pub bounce: bool,
    pub destination: StdAddr,
    pub amount: u128,
    pub body: Option<Cell>,
    pub state_init: Option<StateInit>,
}

pub struct UnsignedHighloadMessage {
    gifts: Vec<Gift>,
    payload: CellBuilder,
    hash: HashBytes,
    expire_at: u32,
}

impl UnsignedHighloadMessage {
    pub fn expire_at(&self) -> u32 {
        self.expire_at
    }

    pub fn hash(&self) -> &HashBytes {
        &self.hash
    }

    pub fn gifts(&self) -> &[Gift] {
        &self.gifts
    }

    pub fn sign(
        self,
        signature: &[u8; ed25519_dalek::SIGNATURE_LENGTH],
        dst: StdAddr,
        init: Option<StateInit>,
    ) -> Result<OwnedMessage> {
        let mut payload = self.payload;
        payload.prepend_raw(signature, (ed25519_dalek::SIGNATURE_LENGTH * 8) as u16)?;

        Ok(OwnedMessage {
            info: MsgInfo::ExtIn(ExtInMsgInfo {
                dst: IntAddr::Std(dst),
                ..Default::default()
            }),
            init,
            body: payload.build()?.into(),
            layout: None,
        })
    }
}

#[derive(Clone)]
struct InitData {
    wallet_id: u32,
    last_cleaned: u64,
    public_key: HashBytes,
    data: Dict<u64, Cell>,
}

impl InitData {
    fn from_key(key: &ed25519_dalek::VerifyingKey) -> Self {
        Self {
            wallet_id: 0,
            last_cleaned: 0,
            public_key: HashBytes::from_slice(key.as_bytes()),
            data: Dict::new(),
        }
    }

    fn with_wallet_id(mut self, wallet_id: u32) -> Self {
        self.wallet_id = wallet_id;
        self
    }

    fn compute_addr(&self, workchain: i8) -> Result<StdAddr> {
        let state_init = self.make_state_init()?;
        let state_init = CellBuilder::build_from(&state_init)?;
        Ok(StdAddr::new(workchain, *state_init.repr_hash()))
    }

    fn make_state_init(&self) -> Result<StateInit> {
        Ok(StateInit {
            code: Some(wallet_code().clone()),
            data: Some(self.serialize()?),
            ..Default::default()
        })
    }

    fn serialize(&self) -> Result<Cell> {
        let mut builder = CellBuilder::new();
        builder.store_u32(self.wallet_id)?;
        builder.store_u64(self.last_cleaned)?;
        builder.store_u256(&self.public_key)?;
        self.data.store_into(&mut builder, Cell::empty_context())?;
        Ok(builder.build()?)
    }

    fn make_transfer_payload(
        &self,
        gifts: impl IntoIterator<Item = Gift>,
        expire_at: u32,
    ) -> Result<(HashBytes, CellBuilder)> {
        let mut messages = Dict::<u16, (u8, Cell)>::new();

        for (i, gift) in gifts.into_iter().enumerate() {
            let internal_message = OwnedRelaxedMessage {
                info: RelaxedMsgInfo::Int(RelaxedIntMsgInfo {
                    ihr_disabled: true,
                    bounce: gift.bounce,
                    dst: IntAddr::Std(gift.destination),
                    value: CurrencyCollection::new(gift.amount),
                    ..Default::default()
                }),
                init: gift.state_init,
                body: gift.body.unwrap_or_default().into(),
                layout: None,
            };

            let cell = CellBuilder::build_from(internal_message)?;
            messages.set(i as u16, (gift.flags, cell))?;
        }

        let mut messages_builder = CellBuilder::new();
        messages.store_into(&mut messages_builder, Cell::empty_context())?;

        let messages_cell = messages_builder.clone().build()?;
        let messages_hash = messages_cell.repr_hash();

        let mut payload = CellBuilder::new();
        payload.store_u32(self.wallet_id)?;
        payload.store_u32(expire_at)?;
        payload.store_raw(&messages_hash.as_slice()[28..32], 32)?;
        payload.store_builder(&messages_builder)?;

        let hash = *payload.clone().build()?.repr_hash();
        Ok((hash, payload))
    }
}

fn make_state_init(public_key: &ed25519_dalek::VerifyingKey) -> Result<StateInit> {
    InitData::from_key(public_key)
        .with_wallet_id(WALLET_ID)
        .make_state_init()
}

fn wallet_code() -> &'static Cell {
    static CODE: OnceLock<Cell> = OnceLock::new();
    CODE.get_or_init(|| {
        Boc::decode(include_bytes!("../../res/highload_wallet_v2_code.boc"))
            .expect("invalid highload wallet v2 code")
    })
}

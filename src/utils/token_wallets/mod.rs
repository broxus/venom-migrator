use crate::utils::abi::declare_function;
use tycho_types::{
    abi::{AbiType, FromAbi, Function},
    cell::Cell,
    models::StdAddr,
};

pub mod models;

pub fn symbol() -> &'static Function {
    declare_function! {
        name: "symbol",
        inputs: vec![
            AbiType::Uint(32).named("answerId"),
        ],
        outputs: vec![
            AbiType::String.named("symbol"),
        ],
    }
}

pub fn wallet_of() -> &'static Function {
    declare_function! {
        name: "walletOf",
        inputs: vec![
            AbiType::Uint(32).named("answerId"),
            AbiType::Address.named("owner"),
        ],
        outputs: vec![
            AbiType::Address.named("walletAddress"),
        ],
    }
}

#[derive(Debug, Clone, FromAbi)]
pub struct TransferInputs {
    pub amount: u128,
    pub recipient: StdAddr,
    #[abi(name = "deployWalletValue")]
    pub deploy_wallet_value: u128,
    #[abi(name = "remainingGasTo")]
    pub remaining_gas_to: StdAddr,
    pub notify: bool,
    pub payload: Cell,
}

pub fn transfer() -> &'static Function {
    declare_function! {
        function_id: 0x73E22143,
        name: "transfer",
        inputs: vec![
            AbiType::Uint(128).named("amount"),
            AbiType::Address.named("recipient"),
            AbiType::Uint(128).named("deployWalletValue"),
            AbiType::Address.named("remainingGasTo"),
            AbiType::Bool.named("notify"),
            AbiType::Cell.named("payload"),
        ],
        outputs: Vec::new(),
    }
}

#[derive(Debug, Clone, FromAbi)]
pub struct AcceptTransferInputs {
    pub amount: u128,
    pub sender: StdAddr,
    #[abi(name = "remainingGasTo")]
    pub remaining_gas_to: StdAddr,
    pub notify: bool,
    pub payload: Cell,
}

pub fn accept_transfer() -> &'static Function {
    declare_function! {
        function_id: 0x67A0B95F,
        name: "acceptTransfer",
        inputs: vec![
            AbiType::Uint(128).named("amount"),
            AbiType::Address.named("sender"),
            AbiType::Address.named("remainingGasTo"),
            AbiType::Bool.named("notify"),
            AbiType::Cell.named("payload"),
        ],
        outputs: Vec::new(),
    }
}

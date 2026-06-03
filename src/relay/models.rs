use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;

#[derive(Debug, Clone)]
pub enum TxHandleStatus {
    Parsed(Box<RelayTransfer>),
    Skipped,
}

#[derive(Debug, Clone)]
pub enum RelayTransfer {
    Native(Box<NativeTransfer>),
    Token(Box<TokenTransfer>),
}

#[derive(Debug, Clone)]
pub struct NativeTransfer {
    pub tx_hash: HashBytes,
    pub sender: StdAddr,
    pub recipient: StdAddr,
    pub amount: u128,
    pub lt: u64,
    pub now: u32,
}

#[derive(Debug, Clone)]
pub struct TokenTransfer {
    pub tx_hash: HashBytes,
    pub source_token_root: StdAddr,
    pub target_token_root: StdAddr,
    pub source_token_wallet: StdAddr,
    pub target_token_wallet: StdAddr,
    pub ticker: String,
    pub sender: StdAddr,
    pub recipient: StdAddr,
    pub amount: u128,
    pub lt: u64,
    pub now: u32,
}

#[derive(Debug, Clone)]
pub struct TokenWalletInfo {
    pub ticker: String,
    pub source_root: StdAddr,
    pub target_root: StdAddr,
    pub source_token_wallet: StdAddr,
    pub target_token_wallet: StdAddr,
}

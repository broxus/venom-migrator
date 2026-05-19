use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tycho_types::cell::HashBytes;

mod models;
mod transfers;

#[derive(Clone)]
pub struct SqlxClient {
    pool: PgPool,
}

impl SqlxClient {
    pub fn new(pool: PgPool) -> SqlxClient {
        SqlxClient { pool }
    }

    pub fn pg_pool(&self) -> PgPool {
        self.pool.clone()
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct DbConfig {
    pub url: String,
    pub pool_size: u32,
}

pub struct TransferConfirmation {
    pub tx_hash: HashBytes,
    pub msg_hash: HashBytes,
    pub source_tx_hash: HashBytes,
}

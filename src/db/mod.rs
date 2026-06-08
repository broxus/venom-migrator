use serde::{Deserialize, Serialize};
use sqlx::PgPool;

mod models;
mod transfers;

pub use models::{
    PendingRelayMessage, TransferConfirmation, TransferFromDb, TransferStatus, TransfersSearch,
    TransfersSearchOrdering,
};

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

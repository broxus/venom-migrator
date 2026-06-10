use std::future::Future;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tycho_util::serde_helpers;

mod models;
mod transfers;

pub use models::{
    PendingRelayMessage, TransferConfirmation, TransferFromDb, TransferStatus, TransfersSearch,
    TransfersSearchOrdering,
};

#[derive(Clone)]
pub struct SqlxClient {
    pool: PgPool,
    retry_interval: Duration,
    retry_timeout: Duration,
}

impl SqlxClient {
    pub fn new(
        pool: PgPool,
        retry_interval: Duration,
        retry_timeout: Duration,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            retry_interval <= retry_timeout,
            "db retry_interval must be less than or equal to retry_timeout"
        );

        Ok(SqlxClient {
            pool,
            retry_interval,
            retry_timeout,
        })
    }

    pub fn pg_pool(&self) -> PgPool {
        self.pool.clone()
    }

    pub(crate) async fn retry<T, F, Fut>(
        &self,
        operation: &'static str,
        mut f: F,
    ) -> anyhow::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        tokio::select! {
            res = async {
                loop {
                    match f().await {
                        Ok(value) => return Ok(value),
                        Err(e) => {
                            tracing::error!(
                                operation,
                                retrying_in = ?self.retry_interval,
                                "database operation failed: {e:?}"
                            );
                            tokio::time::sleep(self.retry_interval).await;
                        }
                    }
                }
            } => res,
            _ = tokio::time::sleep(self.retry_timeout) => {
                anyhow::bail!(
                    "database operation retry timeout: operation={operation}, timeout={:?}",
                    self.retry_timeout,
                );
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct DbConfig {
    pub url: String,
    pub pool_size: u32,
    #[serde(with = "serde_helpers::humantime")]
    pub retry_interval: Duration,
    #[serde(with = "serde_helpers::humantime")]
    pub retry_timeout: Duration,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            url: Default::default(),
            pool_size: 5,
            retry_interval: Duration::from_secs(5),
            retry_timeout: Duration::from_secs(30),
        }
    }
}

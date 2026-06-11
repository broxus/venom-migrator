use anyhow::Result;
use clap::Parser;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;
use tycho_core::block_strider::{BlockProviderExt, MetricsSubscriber};
use tycho_core::node::{LightNodeConfig, LightNodeContext, NodeBaseConfig, NodeBootArgs};
use tycho_util::cli;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::LoggerConfig;
use tycho_util::cli::metrics::MetricsConfig;
use tycho_util::config::PartialConfig;

use venom_migrator::api::{self, ApiConfig};
use venom_migrator::db::{DbConfig, SqlxClient};
use venom_migrator::relay;
use venom_migrator::relay::config::RelayConfig;
use venom_migrator::relay::wallet::HighloadWallet;
use venom_migrator::subscriber::LightSubscriber;
use venom_migrator::utils::pending_messages::PendingMessages;

/// Run the Tycho node.
#[derive(Parser)]
pub struct Cmd {
    #[clap(flatten)]
    args: tycho_core::node::CmdRunArgs,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        self.args.init_config_or_run_light_node(async move |ctx| {
            let LightNodeContext::<NodeConfig> {
                node,
                config,
                boot_args,
                ..
            } = ctx;

            // Connect to DB
            let pool = PgPoolOptions::new()
                .max_connections(config.db.pool_size)
                .connect(&config.db.url)
                .await?;

            sqlx::migrate!().run(&pool).await?;

            // Sync node.
            let _init_block_id = node
                .init_ext(NodeBootArgs {
                    ignore_states: true,
                    ..boot_args
                })
                .await?;

            // Build strider.
            let archive_block_provider = node.build_archive_block_provider();
            let storage_block_provider = node.build_storage_block_provider();
            let blockchain_block_provider = node
                .build_blockchain_block_provider()
                .with_fallback(archive_block_provider.clone());

            let sqlx_client =
                SqlxClient::new(pool, config.db.retry_interval, config.db.retry_timeout)?;

            let wallet_address = HighloadWallet::compute_address(&config.relay.wallet.secret)?;
            anyhow::ensure!(
                wallet_address == config.relay.wallet.address,
                "wallet address mismatch: expected={}, got={}",
                config.relay.wallet.address,
                wallet_address,
            );

            let sync_ready = Arc::new(Notify::new());
            let pending_messages = PendingMessages::default();

            relay::recover_pending_messages(
                &config.relay,
                sqlx_client.clone(),
                pending_messages.clone(),
            )
            .await?;

            let block_strider = node.build_strider(
                archive_block_provider.chain((blockchain_block_provider, storage_block_provider)),
                (
                    LightSubscriber::new(
                        sqlx_client.clone(),
                        pending_messages.clone(),
                        wallet_address,
                        sync_ready.clone(),
                    ),
                    MetricsSubscriber,
                ),
            );

            tracing::info!("waiting for light subscriber to catch up before running services");

            let api_ready = sync_ready.clone().notified_owned();
            let relay_ready = sync_ready.clone().notified_owned();

            tokio::select! {
                res = block_strider.run() => res?,
                res = run_api(api_ready, &config.api, sqlx_client.clone()) => res?,
                res = run_relay(relay_ready, &config.relay, sqlx_client, pending_messages) => res?,
            }

            // Done
            Ok(())
        })
    }
}

async fn run_api(
    sync_ready: OwnedNotified,
    config: &ApiConfig,
    sqlx_client: SqlxClient,
) -> Result<()> {
    sync_ready.await;
    tracing::info!("light subscriber caught up, starting api");
    api::serve(config, sqlx_client).await
}

async fn run_relay(
    sync_ready: OwnedNotified,
    config: &RelayConfig,
    sqlx_client: SqlxClient,
    pending_messages: PendingMessages,
) -> Result<()> {
    sync_ready.await;
    tracing::info!("light subscriber caught up, starting relay");
    relay::run(config, sqlx_client, pending_messages).await
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialConfig)]
#[serde(default)]
struct NodeConfig {
    #[partial]
    #[serde(flatten)]
    base: NodeBaseConfig,
    #[important]
    threads: ThreadPoolConfig,
    #[important]
    logger_config: LoggerConfig,
    #[important]
    metrics: Option<MetricsConfig>,
    #[important]
    api: ApiConfig,
    #[important]
    relay: RelayConfig,
    #[important]
    db: DbConfig,
}

impl LightNodeConfig for NodeConfig {
    fn base(&self) -> &NodeBaseConfig {
        &self.base
    }

    fn threads(&self) -> &cli::config::ThreadPoolConfig {
        &self.threads
    }

    fn metrics(&self) -> Option<&cli::metrics::MetricsConfig> {
        self.metrics.as_ref()
    }

    fn logger(&self) -> Option<&cli::logger::LoggerConfig> {
        Some(&self.logger_config)
    }
}

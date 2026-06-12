use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use rand::RngExt;
use serde::Serialize;
use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;
use tycho_types::num::Tokens;

use nekoton_transport::rpc::RpcTransport;
use venom_migrator::relay::wallet::HighloadWallet;
use venom_migrator::utils::pending_messages::PendingMessages;

/// Highload wallet account commands.
#[derive(Parser)]
#[clap(subcommand_required = true)]
pub struct Cmd {
    #[clap(subcommand)]
    command: Command,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        match self.command {
            Command::Generate(cmd) => cmd.run(),
            Command::Deploy(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Generate new highload wallet account.
    Generate(GenerateCmd),
    /// Deploy highload wallet account.
    Deploy(DeployCmd),
}

#[derive(Parser)]
struct GenerateCmd;

impl GenerateCmd {
    #[allow(clippy::print_stdout)]
    fn run(self) -> Result<()> {
        let secret = rand::rng().random::<ed25519_dalek::SecretKey>();
        let key = ed25519_dalek::SigningKey::from_bytes(&secret);
        let address = HighloadWallet::compute_address(&HashBytes(secret))?;

        #[derive(Serialize)]
        struct Output<'a> {
            secret: HashBytes,
            public: &'a HashBytes,
            address: StdAddr,
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&Output {
                secret: HashBytes(secret),
                public: HashBytes::wrap(key.verifying_key().as_bytes()),
                address,
            })?,
        );

        Ok(())
    }
}

#[derive(Parser)]
struct DeployCmd {
    #[clap(long)]
    rpc: String,
    #[clap(long)]
    secret: HashBytes,
    #[clap(long, default_value = "60")]
    timeout: u32,
}

impl DeployCmd {
    #[allow(clippy::print_stdout)]
    fn run(self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        rt.block_on(async move {
            let endpoint = self.rpc.parse()?;
            let transport = RpcTransport::new([endpoint], Default::default(), false).await?;
            let wallet = HighloadWallet::new(
                Arc::new(ed25519_dalek::SigningKey::from_bytes(
                    self.secret.as_array(),
                )),
                transport,
                PendingMessages::default(),
                Tokens::ZERO,
            )?;

            let transaction_hash = wallet.deploy(self.timeout).await?;

            #[derive(Serialize)]
            struct Output {
                address: StdAddr,
                transaction_hash: HashBytes,
            }

            println!(
                "{}",
                serde_json::to_string_pretty(&Output {
                    address: wallet.address().clone(),
                    transaction_hash,
                })?,
            );

            Ok(())
        })
    }
}

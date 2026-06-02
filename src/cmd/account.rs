use anyhow::Result;
use clap::Parser;
use rand::RngExt;
use serde::Serialize;
use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;

use venom_migrator::relay::wallet::HighloadWallet;

/// Generate new highload wallet account.
#[derive(Parser)]
pub struct Cmd;

impl Cmd {
    #[allow(clippy::print_stdout)]
    pub fn run(self) -> Result<()> {
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

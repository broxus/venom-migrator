use nekoton_core::contracts::blockchain_context::BlockchainAccount;
use nekoton_core::contracts::function_ext::ExecutionOutput;
use tycho_types::abi::AbiValue;
use tycho_types::models::StdAddr;

use crate::utils::abi::UnpackFirst;

pub struct RootTokenContract<'a>(pub &'a mut BlockchainAccount);

impl RootTokenContract<'_> {
    pub fn symbol(&mut self) -> anyhow::Result<String> {
        let inputs = [AbiValue::uint(32, 0u32).named("answerId")];
        let ExecutionOutput { values, exit_code } =
            self.0.run_local_responsible(super::symbol(), &inputs)?;

        if exit_code != 0 {
            return Err(anyhow::anyhow!(
                "Failed to get symbol with exit code {exit_code}"
            ));
        }

        values.unpack_first()
    }

    pub fn decimals(&mut self) -> anyhow::Result<u8> {
        let inputs = [AbiValue::uint(32, 0u32).named("answerId")];
        let ExecutionOutput { values, exit_code } =
            self.0.run_local_responsible(super::decimals(), &inputs)?;

        if exit_code != 0 {
            return Err(anyhow::anyhow!(
                "Failed to get decimals with exit code {exit_code}"
            ));
        }

        values.unpack_first()
    }

    pub fn wallet_of(&mut self, owner: StdAddr) -> anyhow::Result<StdAddr> {
        let inputs = [
            AbiValue::uint(32, 0u32).named("answerId"),
            AbiValue::address(owner).named("owner"),
        ];
        let ExecutionOutput { values, exit_code } =
            self.0.run_local_responsible(super::wallet_of(), &inputs)?;

        if exit_code != 0 {
            return Err(anyhow::anyhow!(
                "Failed to get wallet_of with exit code {exit_code}"
            ));
        }

        values.unpack_first()
    }
}

use tycho_types::abi::{AbiValue, FromAbi, NamedAbiValue};

macro_rules! declare_function {
    (
        $(abi: $abi:ident,)?
        $(function_id: $id:literal,)?
        $(header: [$($header:ident),+],)?
        name: $name:literal,
        inputs: $inputs:expr,
        outputs: $outputs:expr$(,)?
    ) => {
        static ONCE: std::sync::OnceLock<tycho_types::abi::Function> = std::sync::OnceLock::new();
        ONCE.get_or_init(|| {
            let builder = tycho_types::abi::Function::builder($crate::utils::abi::declare_function!(@abi_version $($abi)?), ($name).to_string())
                .with_headers($crate::utils::abi::declare_function!(@header $($($header),+)?))
                .with_inputs($inputs as Vec<tycho_types::abi::NamedAbiType>)
                .with_outputs($outputs as Vec<tycho_types::abi::NamedAbiType>);

            $crate::utils::abi::declare_function!(@function_id builder $($id)?);

            builder
                .build()
        })
    };

    (@function_id $builder:ident $id:literal) => { let $builder = $builder.with_id($id); };
    (@function_id $builder:ident ) => {};

    (@abi_version) => { tycho_types::abi::AbiVersion::V2_2 };
    (@abi_version v2_0) => { tycho_types::abi::AbiVersion::V2_0 };
    (@abi_version v2_1) => { tycho_types::abi::AbiVersion::V2_1 };
    (@abi_version v2_2) => { tycho_types::abi::AbiVersion::V2_2 };
    (@abi_version v2_3) => { tycho_types::abi::AbiVersion::V2_3 };
    (@abi_version v2_7) => { tycho_types::abi::AbiVersion::V2_7 };

    (@header) => { Vec::new() };
    (@header $($header:ident),+) => {
        vec![$($crate::utils::abi::declare_function!(@header_item $header)),+]
    };
    (@header_item pubkey) => {
        tycho_types::abi::AbiHeaderType::PublicKey
    };
    (@header_item time) => {
        tycho_types::abi::AbiHeaderType::Time
    };
    (@header_item expire) => {
        tycho_types::abi::AbiHeaderType::Expire
    };
}

pub(crate) use declare_function;

pub trait UnpackAbiPlain<T> {
    fn unpack(self) -> anyhow::Result<T>;
}

impl<T> UnpackAbiPlain<T> for Vec<NamedAbiValue>
where
    T: FromAbi,
{
    fn unpack(self) -> anyhow::Result<T> {
        T::from_abi(AbiValue::Tuple(self))
    }
}

pub trait UnpackFirst {
    fn unpack_first<T: FromAbi>(self) -> anyhow::Result<T>;
}

impl UnpackFirst for Vec<NamedAbiValue> {
    fn unpack_first<T: FromAbi>(self) -> anyhow::Result<T> {
        let value = self
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing ABI output value"))?;

        T::from_abi(value.value)
    }
}

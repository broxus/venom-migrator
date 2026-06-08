use std::sync::OnceLock;

pub mod api;
pub mod db;
pub mod relay;
pub mod subscriber;
pub mod utils;

pub static BIN_VERSION: &str = env!("VENOM_MIGRATOR_VERSION");
pub static BIN_BUILD: &str = env!("VENOM_MIGRATOR_BUILD");

pub fn version_string() -> &'static str {
    static STRING: OnceLock<String> = OnceLock::new();
    STRING.get_or_init(|| format!("(release {BIN_VERSION}) (build {BIN_BUILD})"))
}

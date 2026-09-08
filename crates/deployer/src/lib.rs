//! Stateless Rust deployer for kardamom L1 contracts.
//!
//! All upgrade state stays on-chain. The deployer reads creation bytecode
//! embedded at build time (see [`embedded`]), builds a `DeploymentSpec[]`,
//! and sends one `applyDeployments` transaction through the factory. It uses
//! no local manifest, no per-environment state file, and no runtime
//! artifact I/O.

use anyhow::Context;

#[cfg(any(test, feature = "test-support"))]
pub mod abi;
pub mod addresses;
pub mod deployer;
#[cfg(any(test, feature = "test-support"))]
pub mod dev_keys;
pub mod embedded;
pub mod ids;
#[cfg(any(test, feature = "test-support"))]
pub mod mnemonic;
#[cfg(any(test, feature = "test-support"))]
pub mod signers;
pub mod spec;
#[cfg(any(test, feature = "test-support"))]
pub mod testkit;

pub use deployer::{
    DeployError, Deployer, FactoryStatus, RegistryEntry, VerifyMismatch, VerifyReport,
};
pub use ids::ContractId;
pub use spec::{
    Action, DeploymentSpec, Op, ProofOracleInit, build_spec, encode_address_arg,
    encode_address_pair, encode_init_calldata, encode_oracle_init_args,
};

/// Strip an optional `0x` prefix from a hex string. The prefix is
/// optional input, not an error case, so a string without it passes
/// through unchanged.
#[must_use]
pub fn strip_hex_prefix(s: &str) -> &str {
    s.strip_prefix("0x").unwrap_or(s)
}

/// A private-key CLI flag: either the hex key itself, or `env:VAR` to read
/// it from an environment variable. This is the one place the workspace
/// parses that convention.
pub struct KeyFlag(String);

impl KeyFlag {
    #[must_use]
    pub fn new(flag: impl Into<String>) -> Self {
        Self(flag.into())
    }

    /// Resolve `env:VAR` against the process environment. Any other value
    /// resolves to itself.
    ///
    /// # Errors
    /// Returns an error when `env:VAR` names a variable that is not set.
    pub fn resolve(self) -> anyhow::Result<String> {
        match self.0.strip_prefix("env:") {
            Some(var_name) => {
                std::env::var(var_name).with_context(|| format!("env var `{var_name}` not set"))
            }
            None => Ok(self.0),
        }
    }
}

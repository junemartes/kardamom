//! Stateless Rust deployer for kardamom L1 contracts.
//!
//! All upgrade state stays on-chain. The deployer reads creation bytecode
//! embedded at build time (see [`embedded`]), builds a `DeploymentSpec[]`,
//! and sends one `applyDeployments` transaction through the factory. It uses
//! no local manifest, no per-environment state file, and no runtime
//! artifact I/O.

pub mod addresses;
pub mod deployer;
pub mod embedded;
pub mod ids;
pub mod spec;

pub use deployer::{
    DeployError, Deployer, FactoryStatus, RegistryEntry, VerifyMismatch, VerifyReport,
};
pub use ids::ContractId;
pub use spec::{
    Action, DeploymentSpec, Op, build_spec, encode_address_arg, encode_address_pair,
    encode_init_calldata, encode_oracle_init_args, encode_proof_oracle_init_args,
};

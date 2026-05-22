//! Stateless Rust deployer for kardamom L1 contracts.
//!
//! All upgrade state is on-chain: the deployer reads build-time-embedded creation
//! bytecode (see [`embedded`]), builds `DeploymentSpec[]`, and submits one
//! `applyDeployments` tx through the factory. No local manifest, no
//! per-environment state files, no runtime artifact I/O.

pub mod addresses;
pub mod deployer;
pub mod embedded;
pub mod ids;
pub mod spec;

pub use deployer::{
    DeployError, Deployer, FactoryStatus, RegistryEntry, VerifyMismatch, VerifyReport,
};
pub use ids::ContractId;
pub use spec::{Action, DeploymentSpec, Op, build_spec, encode_address_arg, encode_init_calldata};

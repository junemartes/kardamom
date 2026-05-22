//! Built-in `BenchWorkflow` implementations.
//!
//! Each workflow is constructed via `Default` with sensible hardcoded
//! defaults (Anvil mnemonic, the `PUSH1 0x42 RETURN` contract for calls,
//! 1:4 transfer:call ratio for mixed). External crates can implement
//! their own `BenchWorkflow` and don't need to touch these.

pub mod calls;
pub mod mixed;
pub mod transfers;

pub use calls::CallsWorkflow;
pub use mixed::MixedWorkflow;
pub use transfers::TransfersWorkflow;

use alloy_primitives::{Address, Bytes, address, hex};

/// Anvil's well-known test mnemonic. Derived signers map to the standard
/// `0xf39F...` / `0x7099...` / etc. addresses developers know.
pub(crate) const ANVIL_MNEMONIC: &str =
    "test test test test test test test test test test test junk";

/// Hardcoded `eth_call` target for the built-in `CallsWorkflow` /
/// `MixedWorkflow`. The bytecode is `PUSH1 0x42 PUSH1 0x00 MSTORE PUSH1
/// 0x20 PUSH1 0x00 RETURN` — a 32-byte word containing `0x42`. Cheap and
/// deterministic.
pub(crate) const DEFAULT_CALL_CONTRACT: Address =
    address!("0000000000000000000000000000000000001234");

pub(crate) const fn default_call_bytecode() -> Bytes {
    Bytes::from_static(&hex!("604260005260206000f3"))
}

/// Address every prefunded signer transfers value *to* in the transfers /
/// mixed workloads. A 20-byte all-`0xBE` sink — non-precompile, doesn't
/// collide with the signers, doesn't matter where it lives.
pub(crate) const TRANSFER_SINK: Address = Address::new([0xBEu8; 20]);

/// Number of warmup work items each built-in workflow produces per task.
/// Sized to JIT-compile revm hot paths and warm jsonrpsee / hyper buffers
/// without dominating the run wall-clock.
pub(crate) const WARMUP_PER_TASK: usize = 100;

//! These are the built-in `BenchWorkflow` implementations.
//!
//! `Default` builds each workflow with sensible fixed defaults: the
//! Anvil mnemonic, the `PUSH1 0x42 RETURN` contract for calls, and a
//! 1:4 transfer-to-call ratio for mixed. An external crate can
//! implement its own `BenchWorkflow` and does not need these.

pub mod calls;
pub mod mixed;
pub mod transfers;

pub use calls::CallsWorkflow;
pub use mixed::{MixRatio, MixedWorkflow};
pub use transfers::TransfersWorkflow;

use alloy_primitives::{Address, Bytes, TxKind, address, hex};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionRequest};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;

/// Anvil's well-known test mnemonic. Derived signers map to the standard
/// `0xf39F...`, `0x7099...`, and similar addresses developers know.
pub(crate) use crate::ANVIL_MNEMONIC;

/// The fixed `eth_call` target for the built-in `CallsWorkflow` and
/// `MixedWorkflow`. The bytecode is `PUSH1 0x42 PUSH1 0x00 MSTORE PUSH1
/// 0x20 PUSH1 0x00 RETURN`. This returns a 32-byte word with `0x42`.
/// It is cheap and deterministic.
pub(crate) const DEFAULT_CALL_CONTRACT: Address =
    address!("0000000000000000000000000000000000001234");

pub(crate) const fn default_call_bytecode() -> Bytes {
    Bytes::from_static(&hex!("604260005260206000f3"))
}

/// The address every prefunded signer sends value to, in the transfers
/// and mixed workloads. This is a 20-byte all-`0xBE` sink. It is not a
/// precompile, it does not collide with the signers, and its exact
/// value does not matter.
pub(crate) const TRANSFER_SINK: Address = Address::new([0xBEu8; 20]);

/// The number of warmup work items each built-in workflow produces per
/// task. This is sized to JIT-compile revm hot paths and warm jsonrpsee
/// and hyper buffers, without taking over the run's wall-clock time.
pub(crate) const WARMUP_PER_TASK: usize = 100;

/// Build a `TransactionRequest` whose only set field is `to`. This is
/// the exact shape every built-in workflow's `eth_call` uses, for the
/// deploy probe and for dispatch.
pub(crate) fn call_req(addr: Address) -> TransactionRequest {
    TransactionRequest {
        to: Some(TxKind::Call(addr)),
        ..Default::default()
    }
}

/// Probe `addr` with `eth_call` and fail unless it returns a non-empty
/// result. Every built-in workflow needs its call target already
/// deployed at genesis; this catches a wiring mistake before dispatch
/// spends the run's wall-clock time on an all-failing method.
///
/// # Errors
///
/// Returns an error if the call fails, or if it returns an empty result.
pub(crate) async fn assert_contract_deployed(
    client: &HttpClient,
    addr: Address,
) -> anyhow::Result<()> {
    let probe: Result<Bytes, _> = client
        .request(
            "eth_call",
            rpc_params![call_req(addr), BlockNumberOrTag::Latest],
        )
        .await;
    match probe {
        Ok(b) if !b.is_empty() => Ok(()),
        _ => anyhow::bail!(
            "contract {addr} is not deployed (empty eth_call output). \
             Check that the workflow's `genesis_alloc()` was used to \
             build the node, or override the workflow's `contract` field \
             to point at a real deployment."
        ),
    }
}

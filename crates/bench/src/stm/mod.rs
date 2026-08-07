//! Block-STM P0: footprint statistics + oracle dependency analysis
//! (spec: `docs/agents/block-stm-executor-spec.md`).
//!
//! Everything here is OFFLINE analysis — no engine changes, no cluster.
//! The capture runner executes workload blocks sequentially through the
//! REAL engine with a fresh per-tx `Bal` (so `storage_reads` attribute per
//! tx), the classifier recovers mapping base-slots by keccak inversion,
//! and the oracle builds the TRUE dependency graph from actual read/write
//! sets — yielding the go/no-go numbers of the spec: critical-path ratio,
//! prediction hit-rates, over-merge cost, fee-sink identification.

pub mod capture;
pub mod classifier;
pub mod oracle;
pub mod uniswap;

use alloy_primitives::{Address, B256};

/// A state cell for conflict analysis. `Account` covers balance+nonce
/// (their updates are read-modify-write, so same-address account writes
/// always conflict); `Slot` is one storage slot.
///
/// Known approximation: pure BALANCE-opcode reads of third-party accounts
/// are not visible in the capture (EIP-7928 attributes storage reads, not
/// account reads) — such cross-tx edges are missed. They are second-order
/// on our workloads (ERC20 flows read storage, not native balances).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cell {
    Account(Address),
    Slot(Address, B256),
}

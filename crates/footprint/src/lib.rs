//! Footprint prediction core for the Block-STM campaign
//! (spec: `docs/agents/block-stm-executor-spec.md`).
//!
//! Pure data + algorithms, shared by two consumers with opposite lifecycles:
//! the OFFLINE P0 lab (`kardamom-bench`'s `stm` module: capture runner,
//! oracle report) and the ONLINE P1 shadow scheduler in the live executor
//! (`kardamom-engine::shadow`). Extracting the classifier here is what keeps
//! the two from drifting — the P0 GO verdict was measured on exactly this
//! inversion/prediction code, and P1's job is to validate those numbers in
//! the live pipeline, not on a reimplementation.
//!
//! Nothing in this crate touches an engine, a database, or a metric
//! registry: observations in, predictions/grades out.

pub mod classifier;
pub mod grade;
pub mod oracle;

use alloy_primitives::{Address, B256, U256};

/// A state cell for conflict analysis. `Account` covers balance+nonce
/// (their updates are read-modify-write, so same-address account writes
/// always conflict); `Slot` is one storage slot.
///
/// Known approximation: pure BALANCE-opcode reads of third-party accounts
/// are not visible in the offline capture (EIP-7928 attributes storage
/// reads, not account reads) — such cross-tx edges are missed there. They
/// are second-order on our workloads (ERC20 flows read storage, not native
/// balances). The live shadow path DOES see them (`TouchSet.account_reads`)
/// but keeps them out of the conflict cells for parity with the P0 yardstick,
/// counting them separately (the P2 Accumulator-guard signal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cell {
    Account(Address),
    Slot(Address, B256),
}

/// One observed transaction: scheduling-time knowledge (sender, to,
/// selector, args) + ground truth (reads, writes, gas).
#[derive(Debug, Clone)]
pub struct TxObs {
    /// Global canonical index across the whole capture / stream.
    pub index: u64,
    pub block: u64,
    pub sender: Address,
    pub to: Option<Address>,
    pub selector: Option<[u8; 4]>,
    /// First calldata words after the selector (ABI head), for derivation
    /// candidates.
    pub args: Vec<U256>,
    pub gas: u64,
    /// Native value attached (tier-1 recipient-account key when > 0).
    pub has_value: bool,
    pub reads: Vec<Cell>,
    pub writes: Vec<Cell>,
}

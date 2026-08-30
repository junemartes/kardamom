//! Footprint prediction core for the Block-STM campaign.
//!
//! This is pure data and algorithms. Two consumers share it, with opposite
//! lifecycles: the offline lab (`kardamom-bench`'s `stm` module: capture
//! runner, oracle report) and the online shadow scheduler in the live
//! executor (`kardamom-engine::shadow`). The classifier lives here so the
//! two do not drift apart. The offline GO verdict was measured on this
//! exact inversion and prediction code. The shadow scheduler's job is to
//! check those numbers in the live pipeline, not to reimplement them.
//!
//! This crate does not touch an engine, a database, or a metric registry.
//! It takes observations in and returns predictions and grades.

pub mod classifier;
pub mod grade;
pub mod oracle;

use alloy_primitives::{Address, B256, U256};

/// A state cell for conflict analysis. `Account` covers balance and nonce
/// (their updates are read-modify-write, so writes to the same address
/// always conflict). `Slot` is one storage slot.
///
/// Known approximation: the offline capture does not see plain BALANCE-opcode
/// reads of third-party accounts (EIP-7928 attributes storage reads, not
/// account reads), so it misses those cross-tx edges. This is a minor gap
/// for our workloads, since ERC20 flows read storage, not native balances.
/// The live shadow path does see them (`TouchSet.account_reads`), but keeps
/// them out of the conflict cells to match the offline yardstick. It counts
/// them separately instead (the Accumulator-guard signal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cell {
    Account(Address),
    Slot(Address, B256),
}

/// One observed transaction: scheduling-time knowledge (sender, to,
/// selector, args) and ground truth (reads, writes, gas).
#[derive(Debug, Clone)]
pub struct TxObs {
    /// Global canonical index across the whole capture or stream.
    pub index: u64,
    pub block: u64,
    pub sender: Address,
    pub to: Option<Address>,
    pub selector: Option<[u8; 4]>,
    /// First calldata words after the selector (ABI head), for derivation
    /// candidates.
    pub args: Vec<U256>,
    pub gas: u64,
    /// Native value attached (tier-1 recipient-account key when above zero).
    pub has_value: bool,
    pub reads: Vec<Cell>,
    pub writes: Vec<Cell>,
}

/// Decode the scheduling-time fields from a raw 2718 envelope:
/// `(to, selector, ABI-head words, has_value)`. The offline capture runner
/// and the live shadow share this function, so both build the same
/// derivation-candidate views. Undecodable bytes give the empty view
/// (no selector means tier-1-only prediction).
pub fn envelope_view(raw: &[u8]) -> (Option<Address>, Option<[u8; 4]>, Vec<U256>, bool) {
    use alloy_eips::eip2718::Decodable2718;
    let Ok(env) = alloy_consensus::TxEnvelope::decode_2718(&mut &raw[..]) else {
        return (None, None, Vec::new(), false);
    };
    decoded_view(&env)
}

/// [`envelope_view`] over an already-decoded envelope. Callers that hold
/// the decoded tx (the STM engine decodes once, for both schedule and
/// execution) skip the second RLP pass.
pub fn decoded_view(
    env: &alloy_consensus::TxEnvelope,
) -> (Option<Address>, Option<[u8; 4]>, Vec<U256>, bool) {
    use alloy_consensus::Transaction;
    let has_value = env.value() > U256::ZERO;
    let to = env.to();
    let input = env.input();
    let selector: Option<[u8; 4]> = input.get(..4).map(|s| s.try_into().unwrap());
    let mut args = Vec::new();
    if input.len() > 4 {
        for chunk in input[4..].chunks(32).take(6) {
            let mut w = [0u8; 32];
            w[..chunk.len()].copy_from_slice(chunk);
            args.push(U256::from_be_bytes(w));
        }
    }
    (to, selector, args, has_value)
}

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

/// Scheduling-time knowledge decoded from a raw or already-decoded
/// envelope: recipient, ABI selector, the first calldata words after
/// it, and whether value moved. No selector means tier-1-only
/// prediction (a native transfer, a create, or undecodable bytes).
#[derive(Debug, Clone)]
pub struct EnvelopeView {
    pub to: Option<Address>,
    pub selector: Option<[u8; 4]>,
    pub args: Vec<U256>,
    pub has_value: bool,
}

/// Decode the scheduling-time fields from a raw 2718 envelope. The
/// offline capture runner and the live shadow share this function, so
/// both build the same derivation-candidate views. Undecodable bytes
/// give the empty view.
#[must_use]
pub fn envelope_view(raw: &[u8]) -> EnvelopeView {
    use alloy_eips::eip2718::Decodable2718;
    let Ok(env) = alloy_consensus::TxEnvelope::decode_2718(&mut &raw[..]) else {
        return EnvelopeView {
            to: None,
            selector: None,
            args: Vec::new(),
            has_value: false,
        };
    };
    decoded_view(&env)
}

/// [`envelope_view`] over an already-decoded envelope. Callers that hold
/// the decoded tx (the STM engine decodes once, for both schedule and
/// execution) skip the second RLP pass.
#[must_use]
pub fn decoded_view(env: &alloy_consensus::TxEnvelope) -> EnvelopeView {
    use alloy_consensus::Transaction;
    let has_value = env.value() > U256::ZERO;
    let to = env.to();
    let input = env.input();
    let selector: Option<[u8; 4]> = input.first_chunk::<4>().copied();
    let args: Vec<U256> = if input.len() > 4 {
        input[4..]
            .chunks(32)
            .take(6)
            .map(|chunk| {
                let mut w = [0u8; 32];
                w[..chunk.len()].copy_from_slice(chunk);
                U256::from_be_bytes(w)
            })
            .collect()
    } else {
        Vec::new()
    };
    EnvelopeView {
        to,
        selector,
        args,
        has_value,
    }
}

/// Shared test fixtures: `grade.rs` and `classifier.rs` both build
/// [`TxObs`]s and small distinct addresses for their unit tests.
#[cfg(test)]
pub(crate) mod testkit {
    use super::{Cell, TxObs};
    use alloy_primitives::{Address, U256};

    /// A small distinct address (`0x00..0i`), for a set of unrelated
    /// senders in a test.
    pub(crate) fn addr(i: u8) -> Address {
        Address::with_last_byte(i)
    }

    /// A minimal observation: one sender, a fixed `to`/`selector`, one
    /// arg word, given writes and reads.
    pub(crate) fn obs(
        index: u64,
        sender: Address,
        to: Address,
        sel: [u8; 4],
        writes: Vec<Cell>,
        reads: Vec<Cell>,
    ) -> TxObs {
        TxObs {
            index,
            block: 1,
            sender,
            to: Some(to),
            selector: Some(sel),
            args: vec![U256::from(1u64)],
            gas: 100_000,
            has_value: false,
            reads,
            writes,
        }
    }
}

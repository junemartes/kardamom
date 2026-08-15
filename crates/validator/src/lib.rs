//! Validator node core: cross-check the local re-execution against the
//! sequencer's published artifacts, fail-stop on divergence.
//!
//! A validator reuses the whole [`kardamom_engine`] pipeline — the same reader/
//! join topology and `execute_tx` core the executor runs — but wires two
//! role-specific seams instead of publishing receipts:
//!
//! - [`ValidatorWriterQueue`] wraps the trie-aware state writer's
//!   [`StateWriterQueue`](kardamom_engine::StateWriterQueue). At each block
//!   close it receives the locally-computed
//!   [`BlockDelta`](kardamom_types::BlockDelta) (`submit(boundary, delta)`),
//!   cross-checks its **write-set** against the executor's per-block **BAL**
//!   (subscribed on `tx_bal`), and forwards the delta to the trie-aware writer
//!   (which advances the MPT state root). A write-set mismatch is a proven
//!   execution divergence → fail-stop.
//! - [`ValidatorReceiptSink`] implements
//!   [`TxReceiptsPublication`](kardamom_engine::TxReceiptsPublication). It does
//!   not publish anything; instead it cross-checks each locally-recomputed
//!   receipt against the executor's published receipt (subscribed on
//!   `tx_receipts`) for the same `tx_idx`. A receipt mismatch is also
//!   fail-stop.
//!
//! Both seams are the *existing* engine trait seams — no engine change is
//! needed. The buffers ([`BalBuffer`], [`ReceiptBuffer`]) are filled by the
//! binary's Aeron subscriber tasks and drained by the (sync) exec/commit
//! threads, blocking briefly for the matching artifact to arrive.
//!
//! Module layout: the seams live in `seams.rs`, their verification buffers in
//! `buffers.rs`; both are re-exported here so the crate root stays the single
//! import path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use kardamom_engine::ExecutorError;

/// L1 output attester: collects `MessagePassed` leaves from re-executed
/// blocks, builds the per-output withdrawals root, posts to the L1 oracle.
pub mod flight;
pub mod parallel;

pub mod attester;
pub mod epoch_verify;
pub mod metrics;
pub mod prover;
pub mod witness;

mod buffers;
mod seams;

pub use buffers::*;
pub use seams::*;

/// Shared divergence flag. Once tripped, the validator has observed a proven
/// discrepancy between its independent re-execution and the sequencer's output;
/// the surrounding seams return an error so the engine pipeline halts.
#[derive(Debug, Default)]
pub struct Divergence {
    halted: AtomicBool,
    reason: Mutex<Option<String>>,
}

impl Divergence {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a divergence (idempotent — the first reason wins), bump the metric.
    pub fn record(&self, reason: impl Into<String>) {
        if !self.halted.swap(true, Ordering::SeqCst) {
            let reason = reason.into();
            tracing::error!(reason = %reason, "validator divergence detected — halting");
            *self.reason.lock().unwrap() = Some(reason);
            metrics::counter_divergence();
        }
    }

    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::SeqCst)
    }

    pub fn reason(&self) -> Option<String> {
        self.reason.lock().unwrap().clone()
    }
}

/// Exit-semantics classification for the engine loop's terminal error (spec:
/// no-std-exec-core, 3a.1). A [`ExecutorError::RecordIdentity`] failure is
/// proof in hand — keccak/ecrecover refuted the canonical stream's claimed
/// identity — the same class as a proven divergence, so it must latch (exit
/// 2, page the humans) rather than exit 1 into the supervisor's restart
/// loop, where a forging sequencer would be retried forever as an outage.
/// No `Divergence` arm is needed: every validator seam that proves one
/// records it before surfacing the error. Everything else is availability.
/// Returns whether the error latched.
pub fn latch_integrity_failure(divergence: &Divergence, err: &ExecutorError) -> bool {
    match err {
        ExecutorError::RecordIdentity(reason) => {
            divergence.record(format!("record identity forged: {reason}"));
            true
        }
        _ => false,
    }
}

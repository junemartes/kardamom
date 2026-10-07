//! Validator node core: check the local re-execution against the sequencer's
//! published data. Stop the process on a mismatch.
//!
//! A validator reuses the whole [`kardamom_engine`] pipeline. It uses the
//! same reader/join topology and `execute_tx` core as the executor, but it
//! wires two role-specific seams instead of publishing receipts:
//!
//! - [`ValidatorWriterQueue`] wraps the trie-aware state writer's
//!   [`StateWriterQueue`](kardamom_engine::StateWriterQueue). At each block
//!   close, it receives the local
//!   [`BlockDelta`](kardamom_types::BlockDelta) (`submit(boundary, delta)`).
//!   It checks the delta's write-set against the executor's per-block BAL
//!   (subscribed on `tx_bal`), then sends the delta to the trie-aware
//!   writer, which advances the MPT state root. A write-set mismatch proves
//!   an execution divergence, so the validator stops.
//! - [`ValidatorReceiptSink`] implements
//!   [`TxReceiptsPublication`](kardamom_engine::TxReceiptsPublication). It
//!   does not publish data. Instead it checks each recomputed receipt
//!   against the executor's published receipt (subscribed on `tx_receipts`)
//!   for the same `tx_idx`. A receipt mismatch also stops the validator.
//!
//! Both seams are existing engine trait seams; the engine needs no change.
//! The binary's Aeron subscriber tasks fill the buffers ([`BalBuffer`],
//! [`ReceiptBuffer`]). The sync exec/commit threads drain the buffers and
//! wait briefly for the matching data to arrive.
//!
//! Every executor replica publishes its own BAL and receipts. The seams
//! compare the result of every replica, and a divergence names the
//! replica whose result differs ([`Attribution`]).
//!
//! Module layout: the seams are in `seams.rs`, their verification buffers
//! are in `buffers.rs`, and the per-replica check is in `replica.rs`. All
//! three re-export here, so the crate root is the one import path.

// The `#[async_trait]` and `#[rpc]` macros expand trait methods to functions
// that carry a bare `#[must_use]` and return a pinned boxed future, which is
// `#[must_use]` on its own. The lint fires in the macros' output, so the crate
// allows it here.
#![allow(clippy::double_must_use)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use kardamom_engine::ExecutorError;

/// Lock `m`, recovering the data if an earlier panic poisoned it. Every
/// caller in this crate protects plain data with no invariant a panic
/// mid-update could break, so recovery is always safe: a panicking
/// writer never leaves the guarded value in a state a later reader or
/// writer cannot use.
pub(crate) fn lock_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// L1 output attester. It collects `MessagePassed` leaves from re-executed
/// blocks, builds the per-output withdrawals root, and posts it to the L1
/// oracle.
pub mod flight;
pub mod parallel;

pub mod attester;
pub mod epoch_verify;
pub mod interop;
pub mod metrics;
pub mod prover;
pub mod verdict;
pub mod witness;

mod block_accum;
mod buffers;
mod replica;
mod seams;

pub use buffers::*;
pub use replica::{Arrival, Attribution, Check, ReplicaId, Taken};
pub use seams::*;

/// Shared divergence flag. Once set, the validator has found a proven
/// mismatch between its own re-execution and the sequencer's output. The
/// seams then return an error, so the engine pipeline stops.
#[derive(Debug, Default)]
pub struct Divergence {
    halted: AtomicBool,
    reason: OnceLock<String>,
    /// The replica that the first divergence names, when a replica's
    /// result proved it.
    replica: OnceLock<Attribution>,
}

impl Divergence {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a divergence and bump the metric. This is idempotent: the first
    /// reason wins.
    pub fn record(&self, reason: impl Into<String>) {
        self.latch(reason.into(), None);
    }

    /// Record the first divergence: the reason, the log line, and the
    /// metrics. A later call changes nothing.
    fn latch(&self, reason: String, replica: Option<Attribution>) {
        if self.halted.swap(true, Ordering::SeqCst) {
            return;
        }
        tracing::error!(
            reason = %reason,
            replica = replica.map(|a| a.replica.session()),
            check = replica.map(|a| a.check.id()),
            "validator divergence detected — halting"
        );
        let _ = self.reason.set(reason);
        metrics::counter_divergence();
        let Some(attribution) = replica else {
            return;
        };
        let _ = self.replica.set(attribution);
        metrics::counter_replica_divergence(attribution);
    }

    #[must_use]
    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn reason(&self) -> Option<String> {
        self.reason.get().cloned()
    }

    /// If halted, the recorded reason, or `default` if [`record`](Self::record)
    /// has been called but has not yet finished storing it (a race this
    /// crate's usage never actually hits, since every caller stores
    /// before returning; kept as a safe fallback rather than a panic).
    /// `None` when not halted. Callers wrap the string in their own
    /// `ExecutorError` variant, so this returns the reason only.
    #[must_use]
    pub fn halt_reason(&self, default: &str) -> Option<String> {
        self.is_halted()
            .then(|| self.reason().unwrap_or_else(|| default.to_string()))
    }

    /// Record `reason` as the divergence (idempotent — see
    /// [`record`](Self::record)) and hand it back, so a call site can
    /// record-and-return the same string in one expression.
    pub fn halt(&self, reason: String) -> String {
        self.record(reason.clone());
        reason
    }

    /// [`halt`](Self::halt) for a divergence that a replica's result
    /// proves. The reason ends with the replica, so the verdict file and
    /// the halt record name it too.
    pub fn halt_replica(&self, attribution: Attribution, detail: &str) -> String {
        let reason = format!("{detail} [{attribution}]");
        self.latch(reason.clone(), Some(attribution));
        reason
    }

    /// The replica that the recorded divergence names. `None` when no
    /// divergence stands, or when no replica result proved it (an epoch
    /// fault, a forged record).
    #[must_use]
    pub fn replica(&self) -> Option<Attribution> {
        self.replica.get().copied()
    }
}

/// Classify the engine loop's terminal error for exit semantics.
///
/// A [`ExecutorError::RecordIdentity`] error is proof: keccak or ecrecover
/// rejected the canonical stream's claimed identity. This is the same class
/// as a proven divergence, so the process must latch (exit 2, page the
/// humans). It must not exit 1 into the supervisor's restart loop, or a
/// forging sequencer would look like an outage and retry forever.
///
/// No `Divergence` arm is needed here: every validator seam that proves a
/// divergence records it before it returns the error. Every other error is
/// an availability issue.
///
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

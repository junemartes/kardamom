//! The sealer's answers to this shard's offers, as the publish loop
//! drains them: the terminal refusals, the contiguity rejects, and the
//! window-full answers.

use std::collections::HashMap;

use alloy_primitives::Address;

use super::{FLOOR_DRAIN_PER_ITER, ResyncController, drain_bounded};

/// One terminal refusal from the sealer: the ref of `sender` at `nonce`
/// is not ordered, and `reason` is what the client is told. The past
/// deadline, the DA lag and the record lag share this path, because the
/// remedy is the same: drop the ledger entry, and tell the client, who
/// resubmits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealerRefusal {
    pub sender: Address,
    pub nonce: u64,
    pub reason: kardamom_types::TxErrorReason,
}

/// One answer on the rewind channel, forwarded by the egress-watermark
/// thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealerAnswer {
    /// The contiguity guard expects `expected` of `sender`, and the ref
    /// at `nonce` is not that nonce.
    ContiguityReject {
        sender: Address,
        nonce: u64,
        expected: u64,
    },
    /// The window was full, so the sealer did not take the ref of
    /// `sender` at `nonce`. The window check runs before the contiguity
    /// guard, so this answer names no expected nonce.
    WindowFull { sender: Address, nonce: u64 },
}

/// One drain of the rewind channel. See [`ResyncController::drain_answers`].
#[derive(Debug, Default)]
pub struct AnswerDrain {
    /// `(sender, nonce)` of each reject below the expected nonce. The
    /// sealer ordered the ref, so its ledger entry drops like on a
    /// receipt.
    pub drops: Vec<(Address, u64)>,
    /// The lowest expected nonce of each sender with a reject at or above
    /// it. The sealer did not order that nonce.
    pub gaps: HashMap<Address, u64>,
    /// `(sender, nonce)` of each window-full answer. The ref republishes
    /// if the ledger still holds it. The answer never frees a nonce.
    pub window_full: Vec<(Address, u64)>,
}

impl AnswerDrain {
    /// Add one contiguity reject whose expected nonce is already raised
    /// to the receipt floor.
    fn add_reject(&mut self, sender: Address, nonce: u64, expected: u64) {
        if nonce < expected {
            self.drops.push((sender, nonce));
            return;
        }
        self.gaps
            .entry(sender)
            .and_modify(|e| *e = (*e).min(expected))
            .or_insert(expected);
    }
}

impl ResyncController {
    /// Drain the terminal refusals the sealer answered this shard with.
    /// Each one is a transaction no republish can order now.
    pub fn drain_deadline_rejects(&mut self) -> Vec<SealerRefusal> {
        drain_bounded(
            &self.deadline_rx,
            &mut self.deadline_rx_dead,
            FLOOR_DRAIN_PER_ITER,
            self.partition,
            "past-deadline producer disconnected",
        )
    }

    /// Drain the contiguity rejects and the window-full answers. Bounded
    /// per iteration, like the floor drain.
    ///
    /// - A reject below the expected nonce proves that the sealer ordered
    ///   the ref: the guard accepted it on the way up, and its window
    ///   entry aged out. The ledger entry drops. Without the drop, a ref
    ///   with no confirming receipt republishes on every confirm timeout.
    /// - A reject at or above the expected nonce is a gap: the sealer did
    ///   not order the expected nonce. A rejected batch gives one reject
    ///   per entry, so the gaps collapse to the lowest expected nonce of
    ///   each sender.
    /// - A window-full answer is back-pressure, not a verdict on the
    ///   nonce.
    pub fn drain_answers(&mut self) -> AnswerDrain {
        drain_bounded(
            &self.reject_rx,
            &mut self.reject_rx_dead,
            FLOOR_DRAIN_PER_ITER,
            self.partition,
            "contiguity-reject producer disconnected",
        )
        .into_iter()
        .fold(AnswerDrain::default(), |drain, answer| {
            self.fold_answer(drain, answer)
        })
    }

    /// Add one answer to `drain`. The expected nonce of a reject is raised
    /// to the receipt floor of the sender. A receipt proves that every
    /// nonce below the floor executed, so the sealer now expects the floor
    /// or a later nonce. A reject that names a lower nonce waited in the
    /// channel backlog while the chain went on. A rewind to that nonce
    /// puts the floor below the chain for good: no ref of the sender
    /// publishes, so no later receipt raises the floor again.
    fn fold_answer(&self, mut drain: AnswerDrain, answer: SealerAnswer) -> AnswerDrain {
        match answer {
            SealerAnswer::ContiguityReject {
                sender,
                nonce,
                expected,
            } => {
                let floor = self.floor(sender).unwrap_or(0);
                drain.add_reject(sender, nonce, floor.max(expected));
            }
            SealerAnswer::WindowFull { sender, nonce } => drain.window_full.push((sender, nonce)),
        }
        drain
    }
}

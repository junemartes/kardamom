//! The executed-truth observations the receipts stream gives the
//! sequencer, what each one proves about the nonce of its sender, and the
//! rollback that voids the observations above a block.

use alloy_primitives::Address;
use kardamom_types::ReceiptRollback;

/// One executed-truth observation. Two sources produce it:
///
/// - The `tx_receipts` stream: one update per receipt of this replica's
///   senders. See [`FloorUpdate::of_receipt`].
/// - The nonce lookup (`crate::lookup`): an executor reports the committed
///   account nonce `c`, so every nonce below `c` executed. The task sends
///   [`Outcome::Executed`] at `c - 1`, and nothing for `c == 0`. The
///   controller treats both sources the same: the floor rises to `c`, and
///   published refs at or below `c - 1` count as confirmed, because the
///   committed state proves them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorUpdate {
    pub sender: Address,
    pub outcome: Outcome,
    pub evidence: Evidence,
}

/// Where the evidence of a [`FloorUpdate`] comes from. A rollback voids it
/// by its source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    /// A receipt of `block`. A rollback below `block` voids it.
    Receipt { block: u64 },
    /// A lookup answer, asked while the controller had seen `generation`
    /// rollbacks. The committed state of a source has no single block of
    /// evidence, and a source can answer from a state that a rollback
    /// voids. So every later rollback voids the answer, and an answer to a
    /// request from before the latest rollback counts for nothing.
    Lookup { generation: u64 },
}

impl Evidence {
    /// Whether a rollback voids this evidence when the controller has
    /// seen `generation` rollbacks: a lookup answer to a request from
    /// before the latest of them.
    pub(crate) fn stale_at(self, generation: u64) -> bool {
        matches!(self, Self::Lookup { generation: asked } if asked < generation)
    }
}

/// One item of the floor channel, in the order of the receipt stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloorEvent {
    /// One executed-truth observation.
    Update(FloorUpdate),
    /// An executor rolled its state back: the receipts above `to_block`
    /// are void, and so is each floor that one of them set.
    Rollback(ReceiptRollback),
}

impl From<FloorUpdate> for FloorEvent {
    fn from(update: FloorUpdate) -> Self {
        Self::Update(update)
    }
}

/// What one [`FloorUpdate`] proves about its sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The tx at `nonce` used its nonce: it executed, or it skipped and
    /// burned its nonce ([`kardamom_types::Receipt::burns_nonce`]). It
    /// confirms the publish, and it is floor evidence: the floor rises to
    /// `nonce + 1`.
    Executed { nonce: u64 },
    /// A marker receipt: the tx at `nonce` was ordered, but it consumed no
    /// nonce. Ordering in the canonical log confirms the publish. The
    /// floor does not rise. The floor logic does not read `reason`.
    Skipped {
        nonce: u64,
        reason: kardamom_types::SkipReason,
    },
    /// An L1-originated deposit. It has no L2 nonce, so it is neither a
    /// publish confirmation nor floor evidence. `Receipt::tx_type` marks
    /// it, so a genuine nonce-0 tx is never taken for a deposit.
    Deposit,
}

impl Outcome {
    /// The nonce whose publish this confirms. `None` for a deposit.
    #[must_use]
    pub(crate) fn confirmed(self) -> Option<u64> {
        match self {
            Self::Executed { nonce } | Self::Skipped { nonce, .. } => Some(nonce),
            Self::Deposit => None,
        }
    }

    /// The floor this proves. `Some` only for an executed tx.
    #[must_use]
    pub(crate) fn floor(self) -> Option<u64> {
        match self {
            Self::Executed { nonce } => Some(nonce.saturating_add(1)),
            Self::Skipped { .. } | Self::Deposit => None,
        }
    }

    /// The nonce the chain expects next after a skip, as far as the
    /// receipt floor `floor` of the sender shows it. A skip uses no
    /// nonce, so a nonce floor above this value is above the chain.
    ///
    /// - A skip below `floor` is late: a lagging executor sent it after
    ///   later receipts raised the floor. It proves nothing new, and a
    ///   release to `floor` would put the floor below the refs in flight.
    ///   `None`.
    /// - A nonce-too-low skip proves nothing new: the chain is past its
    ///   nonce. `None`.
    /// - A nonce-too-high skip proves only that the chain is below its
    ///   nonce. The receipt floor is the best bound.
    /// - Every other skip leaves the chain at its nonce.
    ///
    /// `None` for an executed tx and for a deposit.
    #[must_use]
    pub(crate) fn released(self, floor: u64) -> Option<u64> {
        use kardamom_types::SkipReason;
        match self {
            Self::Skipped { nonce, .. } if nonce < floor => None,
            Self::Skipped {
                reason: SkipReason::NonceTooLow,
                ..
            }
            | Self::Executed { .. }
            | Self::Deposit => None,
            Self::Skipped {
                reason: SkipReason::NonceTooHigh,
                ..
            } => Some(floor),
            Self::Skipped { nonce, .. } => Some(nonce),
        }
    }
}

impl FloorUpdate {
    /// The update for one receipt. A deposit wins over a skip reason. A
    /// skip that burned its nonce is an executed nonce: the chain expects
    /// the next one.
    #[must_use]
    pub fn of_receipt(receipt: &kardamom_types::Receipt) -> Self {
        let outcome = match (receipt.is_deposit(), receipt.skip_reason) {
            (true, _) => Outcome::Deposit,
            (false, Some(reason)) if !receipt.burns_nonce() => Outcome::Skipped {
                nonce: receipt.nonce,
                reason,
            },
            (false, _) => Outcome::Executed {
                nonce: receipt.nonce,
            },
        };
        Self {
            sender: receipt.from,
            outcome,
            evidence: Evidence::Receipt {
                block: receipt.block_number,
            },
        }
    }

    /// An executed tx of `sender` at `nonce`.
    #[must_use]
    pub fn executed(sender: Address, nonce: u64) -> Self {
        Self {
            sender,
            outcome: Outcome::Executed { nonce },
            evidence: Evidence::Lookup { generation: 0 },
        }
    }

    /// The skipped tx of `sender` at `nonce`, which used no nonce.
    #[must_use]
    pub fn skip(sender: Address, nonce: u64, reason: kardamom_types::SkipReason) -> Self {
        Self {
            sender,
            outcome: Outcome::Skipped { nonce, reason },
            evidence: Evidence::Lookup { generation: 0 },
        }
    }

    /// The same update, with the evidence `evidence`.
    #[must_use]
    pub fn with_evidence(self, evidence: Evidence) -> Self {
        Self { evidence, ..self }
    }

    /// A deposit to `sender`.
    #[must_use]
    pub fn deposit(sender: Address) -> Self {
        Self {
            sender,
            outcome: Outcome::Deposit,
            evidence: Evidence::Lookup { generation: 0 },
        }
    }
}

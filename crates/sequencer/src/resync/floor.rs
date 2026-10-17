//! The executed-truth observations the receipts stream gives the
//! sequencer, and what each one proves about the nonce of its sender.

use alloy_primitives::{Address, B256};
use kardamom_cluster_adapter::wire::NonceRelease;

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
}

/// What one [`FloorUpdate`] proves about its sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The tx at `nonce` executed. It confirms the publish, and it is
    /// floor evidence: the floor rises to `nonce + 1`.
    Executed { nonce: u64 },
    /// A marker receipt: the tx at `nonce` was ordered, but it consumed no
    /// nonce. Ordering in the canonical log confirms the publish. The
    /// floor does not rise. The floor logic does not read `reason`.
    /// `tx_hash` names the skipped entry in the nonce release to the
    /// sealer.
    Skipped {
        nonce: u64,
        reason: kardamom_types::SkipReason,
        tx_hash: B256,
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
    /// The update for one receipt. A deposit wins over a skip reason.
    #[must_use]
    pub fn of_receipt(receipt: &kardamom_types::Receipt) -> Self {
        let outcome = match (receipt.is_deposit(), receipt.skip_reason) {
            (true, _) => Outcome::Deposit,
            (false, Some(reason)) => Outcome::Skipped {
                nonce: receipt.nonce,
                reason,
                tx_hash: receipt.tx_hash,
            },
            (false, None) => Outcome::Executed {
                nonce: receipt.nonce,
            },
        };
        Self {
            sender: receipt.from,
            outcome,
        }
    }

    /// An executed tx of `sender` at `nonce`.
    #[must_use]
    pub fn executed(sender: Address, nonce: u64) -> Self {
        Self {
            sender,
            outcome: Outcome::Executed { nonce },
        }
    }

    /// The skipped tx `tx_hash` of `sender` at `nonce`.
    #[must_use]
    pub fn skip(
        sender: Address,
        nonce: u64,
        reason: kardamom_types::SkipReason,
        tx_hash: B256,
    ) -> Self {
        Self {
            sender,
            outcome: Outcome::Skipped {
                nonce,
                reason,
                tx_hash,
            },
        }
    }

    /// The nonce release for the sealer's guard that this update proves,
    /// for a sender whose receipt floor is `floor` (`None` when no receipt
    /// gave a floor yet). The release names the skipped nonce, and the
    /// sealer lowers its guard to it, so the skip must prove that the chain
    /// was at that nonce:
    ///
    /// - The executor (revm) checks the code of the sender, then the nonce,
    ///   then the balance. So an insufficient-funds skip proves that the
    ///   nonce matched the chain, with or without a floor. A late one
    ///   below a known floor proves nothing new.
    /// - A gas, fee, init-code, header or undecodable skip comes from a
    ///   check before the nonce check, and so does a sender-has-code skip.
    ///   It proves nothing about the nonce. It releases only at the known
    ///   floor: the floor proves that the chain reached the nonce, and the
    ///   skip used none. Above the floor, the entry can be a later entry of
    ///   a run whose first skip already releases, and a late release of it
    ///   would set the guard below the chain.
    /// - A nonce skip never releases. A nonce-too-high skip follows a skip
    ///   at the chain nonce, whose release already reopens the run.
    #[must_use]
    pub(crate) fn sealer_release(self, floor: Option<u64>) -> Option<NonceRelease> {
        use kardamom_types::SkipReason;
        let Outcome::Skipped {
            nonce,
            reason,
            tx_hash,
        } = self.outcome
        else {
            return None;
        };
        let proven = match reason {
            SkipReason::NonceTooLow | SkipReason::NonceTooHigh => false,
            SkipReason::InsufficientFunds => floor.is_none_or(|f| nonce >= f),
            _ => floor == Some(nonce),
        };
        proven.then_some(NonceRelease {
            sender: self.sender,
            nonce,
            tx_hash,
        })
    }

    /// A deposit to `sender`.
    #[must_use]
    pub fn deposit(sender: Address) -> Self {
        Self {
            sender,
            outcome: Outcome::Deposit,
        }
    }
}

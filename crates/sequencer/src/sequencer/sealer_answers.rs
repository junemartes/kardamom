//! The sealer's answers to this shard's offers: the terminal refusals
//! and the contiguity rejects. Each one settles the publish ledger and
//! the nonce floor of one sender.

use std::time::Instant;

use alloy_primitives::Address;
use kardamom_types::{TxError, TxErrorReason};
use tracing::{trace, warn};

use super::{RefMetadata, Sequencer, SequencerPorts};
use crate::metrics;
use crate::resync::{AnswerDrain, SealerRefusal};
use crate::state::FreedNonce;

impl Sequencer {
    /// The sealer did not take refs of this sequencer. Three cases, split
    /// in the drain (see `ResyncController::drain_answers`):
    ///
    /// - Past nonce (nonce < expected): see [`Self::apply_past_reject`].
    /// - Gap (nonce >= expected): the sealer did not order `expected`.
    ///   See [`Self::apply_gap`].
    /// - Window full: see [`Self::republish_held`].
    pub(super) fn apply_sealer_answers<P: SequencerPorts>(
        &mut self,
        r: &mut crate::resync::ResyncController,
        ports: &mut P,
    ) {
        let AnswerDrain {
            drops,
            gaps,
            window_full,
        } = r.drain_answers();
        for (sender, nonce, expected) in drops {
            let past = PastReject {
                sender,
                nonce,
                expected,
                chain: r.floor(sender).unwrap_or(0),
            };
            self.apply_past_reject(ports, past);
        }
        for (sender, expected) in gaps {
            self.apply_gap(sender, expected);
        }
        for (sender, nonce) in window_full {
            self.republish_held(sender, nonce);
        }
    }

    /// One window-full answer: the window had no room for the ref of
    /// `sender` at `nonce`. The answer names no expected nonce, so it
    /// never frees a nonce. If the ledger still holds the ref, it and the
    /// later refs republish. Otherwise a receipt, a reject or a refusal
    /// already settled the ref, and nothing happens.
    fn republish_held(&mut self, sender: Address, nonce: u64) {
        if self.unconfirmed.contains(sender, nonce) {
            self.rewind_one_gap(sender, nonce);
        }
    }

    /// One gap contiguity reject: the sealer expects `expected` of
    /// `sender`, so it has not ordered that nonce. The decision depends
    /// only on that value and on the refs this replica holds, so the twins
    /// of a shard end in the same state.
    ///
    /// - This replica holds the ref at `expected`: the offer vanished (a
    ///   voided offer). Rewind and republish it with the later refs. See
    ///   [`Self::rewind_one_gap`].
    /// - This replica holds no ref at `expected`: the sealer refused it,
    ///   and only the client can sign it again. Free the nonce. See
    ///   [`Self::free_nonce`].
    fn apply_gap(&mut self, sender: Address, expected: u64) {
        if self.unconfirmed.contains(sender, expected) || self.state.holds(sender, expected) {
            self.rewind_one_gap(sender, expected);
        } else {
            self.free_nonce(sender, expected);
        }
    }

    /// The sealer expects `nonce` of `sender`, and this replica holds no
    /// ref at it. The floor goes back to `nonce`, so a resubmit at it
    /// publishes. The later refs leave the ledger and park: a republish
    /// cannot order them before the resubmit.
    fn free_nonce(&mut self, sender: Address, nonce: u64) {
        let later: Vec<_> = self
            .unconfirmed
            .take_gap_rewinds(sender, nonce)
            .into_iter()
            .map(|((_, n), meta)| (n, meta))
            .collect();
        warn!(
            sender = ?sender,
            nonce,
            parked = later.len(),
            "sealer expects a nonce this replica holds no ref for; freeing it for the resubmit"
        );
        self.state.free_nonce(
            Instant::now(),
            FreedNonce {
                sender,
                nonce,
                later,
            },
        );
    }

    /// Apply the terminal refusals the sealer answered this shard with: a
    /// ref past its inclusion deadline, or one the DA-lag guard or the
    /// record-lag guard refused. No republish can order it now: drop it from the unconfirmed
    /// ledger, so it never republishes, mark its nonce for the resubmit,
    /// and tell the client, so it can resubmit instead of waiting out its
    /// timeout.
    pub(super) fn apply_deadline_rejects<P: SequencerPorts>(
        &mut self,
        r: &mut crate::resync::ResyncController,
        ports: &mut P,
    ) {
        for refusal in r.drain_deadline_rejects() {
            self.report_refusal(ports, refusal);
        }
    }

    /// One sealer refusal (past its deadline, on a DA lag, or on a record
    /// lag), for [`Self::apply_deadline_rejects`]'s loop. The ledger entry
    /// carries the transaction's hash, so the `Rejected` status goes out
    /// with the error. An entry a receipt or a rewind already took gets
    /// the error only.
    fn report_refusal<P: SequencerPorts>(&mut self, ports: &mut P, refusal: SealerRefusal) {
        let tx_hash = self.take_refused(&refusal).map(|meta| meta.tx_hash);
        warn!(
            sender = ?refusal.sender,
            nonce = refusal.nonce,
            reason = ?refusal.reason,
            "the sealer refused the ref; reporting it to the client"
        );
        let err = TxError {
            sender: refusal.sender,
            nonce: refusal.nonce,
            reason: refusal.reason,
        };
        let (_, _, rc) = ports.split();
        match tx_hash {
            Some(tx_hash) => self.publish_rejection(rc, tx_hash, err),
            None => self.publish_error(rc, err),
        }
    }

    /// Take the refused ref out of the ledger, or out of the buffer when
    /// a freed nonce parked it there, so it never publishes. Mark the
    /// nonce for the resubmit. The floor stays: the sealer also refuses a
    /// late copy of a ref it ordered long ago. See
    /// [`crate::state::PartitionState::mark_refused`]. Returns the ref's
    /// metadata.
    ///
    /// Only the copy the refusal names leaves, so a resubmit that replaced
    /// it survives a late refusal. A rebuffered ref stays too: it waits on
    /// the publisher, the sealer refuses its republish again, and that
    /// refusal finds it in the ledger.
    fn take_refused(&mut self, refusal: &SealerRefusal) -> Option<RefMetadata> {
        let SealerRefusal {
            sender,
            nonce,
            reason,
        } = refusal;
        let named = |meta: &RefMetadata| meta.named_by(reason);
        let refused = self
            .unconfirmed
            .take_named(*sender, *nonce, named)
            .or_else(|| self.state.take_parked(*sender, *nonce, named))?;
        self.state.mark_refused(*sender, *nonce);
        Some(refused)
    }

    /// One contiguity reject below the expected nonce, for
    /// [`Self::apply_sealer_answers`]'s first loop. Two cases:
    ///
    /// - The sealer ordered the ref: the guard accepted it on the way up,
    ///   and its window entry aged out. Then every nonce below `expected`
    ///   is on the chain, or is in this replica's ledger on its way there.
    ///   Drop the ledger entry like on a receipt. See
    ///   `UnconfirmedLedger::drop_committed`.
    /// - The guard is ahead of the chain: `expected` is above the receipt
    ///   floor, and the ledger holds no ref between the rejected nonce and
    ///   `expected`. An executor skipped a transaction that the guard
    ///   counted, and the guard refuses every lower nonce of the sender.
    ///   No republish can order the ref. Refuse it like a sealer refusal,
    ///   with a reason that names both nonces, so the client hears of it.
    fn apply_past_reject<P: SequencerPorts>(&mut self, ports: &mut P, past: PastReject) {
        let PastReject {
            sender,
            nonce,
            expected,
            chain,
        } = past;
        let in_flight = self
            .unconfirmed
            .holds_in(sender, nonce.saturating_add(1)..expected);
        if expected <= chain || in_flight {
            self.drop_committed_and_trace(sender, nonce);
            return;
        }
        metrics::record_sealer_nonce_ahead(self.cfg.partition_index);
        self.report_refusal(
            ports,
            SealerRefusal {
                sender,
                nonce,
                reason: TxErrorReason::SealerNonceAhead {
                    expected_nonce: expected,
                    chain_nonce: chain,
                },
            },
        );
    }

    /// A committed-proof contiguity reject: drop the ledger entry.
    fn drop_committed_and_trace(&mut self, sender: Address, n: u64) {
        if self.unconfirmed.drop_committed(sender, n).is_some() {
            trace!(
                sender = ?sender,
                nonce = n,
                "contiguity reject proves commitment; dropping unconfirmed entry (#85)"
            );
        }
    }

    /// One gap contiguity reject, for
    /// [`Self::apply_sealer_answers`]'s second loop: rewind every
    /// unconfirmed ref the ledger holds for `sender` at or after
    /// `expected`, if any.
    fn rewind_one_gap(&mut self, sender: Address, expected: u64) {
        let taken = self.unconfirmed.take_gap_rewinds(sender, expected);
        if taken.is_empty() {
            return;
        }
        metrics::record_ref_republished(self.cfg.partition_index, taken.len());
        warn!(
            sender = ?sender,
            expected,
            count = taken.len(),
            "sealer contiguity reject; rewinding unconfirmed refs for republish (#85)"
        );
        self.rewind_for_republish(taken);
    }
}

/// One contiguity reject below the expected nonce, with the receipt
/// floor of its sender. See [`Sequencer::apply_past_reject`].
#[derive(Clone, Copy)]
struct PastReject {
    sender: Address,
    nonce: u64,
    /// The expected nonce of the reject, raised to the receipt floor.
    expected: u64,
    /// The receipt floor of the sender: the next nonce on the chain, as
    /// far as the receipts prove it. 0 for a sender with no executed
    /// receipt.
    chain: u64,
}

impl RefMetadata {
    /// Whether a refusal with `reason` names this copy of the ref. A
    /// past-deadline refusal carries the deadline of the refused copy, and
    /// a resubmit carries a later one. A DA-lag or a record-lag refusal
    /// carries no copy identity, so it names every copy at its nonce.
    fn named_by(&self, reason: &TxErrorReason) -> bool {
        match reason {
            TxErrorReason::PastDeadline {
                max_inclusion_block,
                ..
            } => self.max_inclusion_block == *max_inclusion_block,
            _ => true,
        }
    }
}

//! The sealer's answers to this shard's offers: the terminal refusals
//! and the contiguity rejects. Each one settles the publish ledger and
//! the nonce floor of one sender.

use std::time::Instant;

use alloy_primitives::Address;
use kardamom_types::num::usize_to_u64;
use kardamom_types::{TxError, TxErrorReason};
use tracing::warn;

use super::{RefMetadata, Sequencer, SequencerPorts};
use crate::metrics;
use crate::nonce_ahead::Verdict;
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
    /// lag), for [`Self::apply_deadline_rejects`]'s loop. The client gets
    /// the error also when a receipt or a rewind already took the entry.
    fn report_refusal<P: SequencerPorts>(&mut self, ports: &mut P, refusal: SealerRefusal) {
        self.take_refused(&refusal);
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
        self.publish_error(rc, err);
    }

    /// Take the refused ref out of the ledger, or out of the buffer when
    /// a freed nonce parked it there, so it never publishes. Mark the
    /// nonce for the resubmit. The floor stays: the sealer also refuses a
    /// late copy of a ref it ordered long ago. See
    /// [`crate::state::PartitionState::mark_refused`].
    ///
    /// Only the copy the refusal names leaves, so a resubmit that replaced
    /// it survives a late refusal. A rebuffered ref stays too: it waits on
    /// the publisher, the sealer refuses its republish again, and that
    /// refusal finds it in the ledger.
    fn take_refused(&mut self, refusal: &SealerRefusal) {
        let SealerRefusal {
            sender,
            nonce,
            reason,
        } = refusal;
        let named = |meta: &RefMetadata| meta.named_by(reason);
        let taken = self
            .unconfirmed
            .take_named(*sender, *nonce, named)
            .or_else(|| self.state.take_parked(*sender, *nonce, named))
            .is_some();
        if taken {
            self.state.mark_refused(*sender, *nonce);
        }
    }

    /// One contiguity reject below the expected nonce, for
    /// [`Self::apply_sealer_answers`]'s first loop. A copy or a republish
    /// of an ordered ref has the same id, so the sealer answers it as a
    /// duplicate or past its deadline before the guard. So a ref that
    /// reaches the guard below the expected nonce is another transaction
    /// at a nonce the sealer already holds, or the sealer counted a
    /// skipped transaction. Four cases:
    ///
    /// - The ledger no longer holds the ref: a receipt or an earlier answer
    ///   settled it. Nothing to tell.
    /// - `expected` is at most the receipt floor plus the refs in flight:
    ///   another transaction at the nonce is in flight, for example the
    ///   one a speed-up replaces. Refuse the ref with the retryable
    ///   `NonceInFlight`, so its client does not wait for a receipt that
    ///   never comes.
    /// - `expected` is above that bound, but the settle time has not
    ///   passed: the sealer can still hold an ordered ref that this
    ///   replica does not know, for example after a restart. Refuse the
    ///   ref with `NonceInFlight` too.
    /// - `expected` stays above that bound for the settle time: an
    ///   executor skipped a transaction that the guard counted, and the
    ///   guard refuses every lower nonce of the sender. Refuse the ref
    ///   with `SealerNonceAhead`, which names both nonces.
    ///
    /// Each refusal takes the ref out of the ledger and marks the nonce,
    /// so a resubmit is offered again.
    fn apply_past_reject<P: SequencerPorts>(&mut self, ports: &mut P, past: PastReject) {
        let PastReject {
            sender,
            nonce,
            expected,
            chain,
        } = past;
        if !self.unconfirmed.contains(sender, nonce) {
            return;
        }
        let in_flight = usize_to_u64(self.unconfirmed.count_from(sender, chain));
        let verdict = if expected <= chain.saturating_add(in_flight) {
            Verdict::InFlight
        } else {
            self.nonce_ahead.observe(sender, chain, Instant::now())
        };
        let reason = match verdict {
            Verdict::Stuck => {
                metrics::record_sealer_nonce_ahead(self.cfg.partition_index);
                TxErrorReason::SealerNonceAhead {
                    expected_nonce: expected,
                    chain_nonce: chain,
                }
            }
            Verdict::InFlight => TxErrorReason::NonceInFlight { chain_nonce: chain },
        };
        self.report_refusal(
            ports,
            SealerRefusal {
                sender,
                nonce,
                reason,
            },
        );
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

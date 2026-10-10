//! The sealer's answers to this shard's offers: the terminal refusals
//! and the contiguity rejects. Each one settles the publish ledger and
//! the nonce floor of one sender.

use std::time::Instant;

use alloy_primitives::Address;
use kardamom_types::TxError;
use tracing::{trace, warn};

use super::{RefMetadata, Sequencer, SequencerPorts};
use crate::metrics;
use crate::state::FreedNonce;

impl Sequencer {
    /// The sealer rejected this sequencer's ref because its nonce was not
    /// the sender's expected next one. Two cases, split in the drain:
    ///
    /// - Committed-proof (nonce < expected): drop the ledger entry
    ///   exactly like a receipt confirmation. See
    ///   `UnconfirmedLedger::drop_committed` for the full story.
    /// - Gap (nonce >= expected): the sealer did not order `expected`.
    ///   See [`Self::apply_gap`].
    pub(super) fn apply_contiguity_rejects(&mut self, r: &mut crate::resync::ResyncController) {
        let (drops, rewinds) = r.drain_contiguity_rejects();
        for (sender, n) in drops {
            self.drop_committed_and_trace(sender, n);
        }
        for (sender, expected) in rewinds {
            self.apply_gap(sender, expected);
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
    fn report_refusal<P: SequencerPorts>(
        &mut self,
        ports: &mut P,
        refusal: crate::resync::SealerRefusal,
    ) {
        let tx_hash = self
            .take_refused(refusal.sender, refusal.nonce)
            .map(|meta| meta.tx_hash);
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

    /// Take the refused ref of `sender` at `nonce` out of the ledger, or
    /// out of the buffer when a freed nonce parked it there, so it never
    /// publishes. Mark the nonce for the resubmit. The floor stays: the
    /// sealer also refuses a late copy of a ref it ordered long ago. See
    /// [`PartitionState::mark_refused`]. Returns the ref's metadata.
    fn take_refused(&mut self, sender: Address, nonce: u64) -> Option<RefMetadata> {
        let refused = self
            .unconfirmed
            .drop_committed(sender, nonce)
            .or_else(|| self.state.take_buffered(sender, nonce))?;
        self.state.mark_refused(sender, nonce);
        Some(refused)
    }

    /// One committed-proof contiguity reject, for
    /// [`Self::apply_contiguity_rejects`]'s first loop.
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
    /// [`Self::apply_contiguity_rejects`]'s second loop: rewind every
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

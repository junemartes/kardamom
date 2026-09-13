use crate::FEE_SINK;
use crate::schedule;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::executor::DecodedTx;
use kardamom_footprint::classifier::DomainKey;
use kardamom_footprint::classifier::Stats;
use kardamom_types::TxEnvelope;

/// Stable domain-to-worker mapping. Its quality only affects balance
/// across threads, never correctness: ordering comes from the DAG's
/// edges, and an idle worker may steal from any queue.
#[allow(
    clippy::cast_possible_truncation,
    reason = "h % (workers as u64) is < workers, which came from usize, so it always fits back in usize"
)]
pub(super) fn domain_hash(bytes: &[u8], workers: usize) -> usize {
    let h = bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3)
    });
    (h % workers as u64) as usize
}

/// Everything about a transaction that can be derived without touching the
/// graph: the RLP decode and the footprint prediction. Both are pure
/// functions of the envelope bytes and the stats snapshot, so they do not
/// belong on the executor's single feed thread. The pipeline computes them
/// upstream, where the work is already sharded.
///
/// In the live executor, the M `tx_data` reader threads produce this.
/// They touch every envelope anyway and run before the canonical order
/// arrives (the join buffer exists precisely because `tx_data` leads
/// `tx_ordering`), so the work lands in slack that already exists.
#[derive(Clone)]
pub struct Prepared {
    /// `None` when the envelope does not decode. The transaction gets a
    /// skip receipt.
    pub(crate) decoded: Option<DecodedTx>,
    /// Predicted contention domains, with the fee sink already excluded.
    pub(crate) domains: Vec<DomainKey>,
    /// A 64-bit hash per domain, computed here, off the feed thread, in
    /// parallel with every other transaction's preparation, so the
    /// serial feed only probes. See [`TouchTable`] for why hashing by
    /// value is sound.
    pub(crate) domain_hashes: Vec<u64>,
    /// The domain that decides which thread runs this transaction.
    pub(crate) primary: Option<DomainKey>,
    /// ⊤: an untrained selector. Orders behind everything outstanding.
    pub(crate) cold: bool,
}

impl Prepared {
    /// Decode and predict, off the feed thread. Combines [`Self::decode`]
    /// and [`Self::predict`]; a caller that wants the per-phase split
    /// (see `BlockSession::push_tx`) calls those two directly instead.
    pub fn new(envelope: &TxEnvelope, tx_idx: TxIndex, stats: &Stats) -> Prepared {
        let decoded = Self::decode(envelope, tx_idx);
        let (domains, domain_hashes, primary, cold) =
            Self::predict(envelope, decoded.as_ref(), stats);
        Prepared {
            decoded,
            domains,
            domain_hashes,
            primary,
            cold,
        }
    }

    /// Decode an envelope. Pure and cheap; split from [`Self::predict`]
    /// so a caller that wants a per-phase time split can time each
    /// phase.
    pub(super) fn decode(envelope: &TxEnvelope, tx_idx: TxIndex) -> Option<DecodedTx> {
        DecodedTx::decode(&envelope.raw_tx, tx_idx).ok()
    }

    /// Predict an envelope's contention domains against a trained
    /// snapshot. `stats` must be a snapshot trained on prior blocks
    /// only (in the live executor, this is an `Arc<Stats>` swapped at
    /// each boundary, so readers never take a lock).
    ///
    /// Getting this wrong is not a correctness problem: a bad
    /// prediction costs a mis-schedule, which surfaces as a wound and
    /// re-executes that one transaction at its canonical position.
    /// That is what makes it safe to compute here, concurrently, ahead
    /// of canonical order.
    pub(super) fn predict(
        envelope: &TxEnvelope,
        decoded: Option<&DecodedTx>,
        stats: &Stats,
    ) -> (Vec<DomainKey>, Vec<u64>, Option<DomainKey>, bool) {
        // The local index is irrelevant to prediction (it only labels
        // the observation), so preparation needs no position in the
        // block.
        let view = schedule::scheduling_view_decoded(0, envelope, decoded);
        let (domains, domain_hashes, primary, cold) = match stats.predict_domains(&view) {
            Some(predicted) => {
                let (domains, domain_hashes, primary) = predicted
                    .into_iter()
                    .filter(|c| *c != DomainKey::Account(FEE_SINK))
                    .fold(
                        (Vec::new(), Vec::new(), None),
                        |(mut domains, mut domain_hashes, primary): (_, _, Option<DomainKey>), c| {
                            // The primary contention domain is the first
                            // non-sender cell in canonical order. This is
                            // stable across transactions of one flow, which
                            // is what puts a pool's traffic on one thread.
                            // It falls back to the sender cell, the
                            // SenderChain lane, for tier-1-only
                            // transactions.
                            let is_sender = matches!(c, DomainKey::Account(a) if a == envelope.sender);
                            let primary_is_sender =
                                matches!(primary, Some(DomainKey::Account(a)) if a == envelope.sender);
                            let primary = if primary.is_none() || (!is_sender && primary_is_sender) {
                                Some(c)
                            } else {
                                primary
                            };
                            domain_hashes.push(Self::domain_hash64(&c));
                            domains.push(c);
                            (domains, domain_hashes, primary)
                        },
                    );
                (domains, domain_hashes, primary, false)
            }
            None => (Vec::new(), Vec::new(), None, true),
        };
        (domains, domain_hashes, primary, cold)
    }

    /// Hash a contention cell to 64 bits (see [`TouchTable`]: equal
    /// cells hash equal, and a collision can only fabricate a
    /// conservative edge).
    #[inline]
    pub(crate) fn domain_hash64(d: &DomainKey) -> u64 {
        use std::hash::{BuildHasher, Hasher};
        let mut h = crate::FnvBuild.build_hasher();
        match d {
            DomainKey::Account(a) => {
                h.write_u8(1);
                h.write(a.as_slice());
            }
            DomainKey::Fixed(a, k) => {
                h.write_u8(2);
                h.write(a.as_slice());
                h.write(k.as_slice());
            }
        }
        h.finish()
    }
}

//! Wire offers and replay of retained runs, before any new intake.

use std::collections::{BTreeSet, VecDeque};

use alloy_primitives::Address;

use kardamom_cluster_adapter::wire::{GuardHeader, position_deadline};

use super::{AdmissionLoop, RefPayload};
use crate::admission::{Admission, Candidate};
use crate::outbound::RefOffer;
use crate::sealer_rules::SealerVersion;

impl Candidate<RefPayload> {
    /// The offer follows the current decision version: bytes from version
    /// 5 on, and a tagged image position from version 4 on.
    pub(super) fn ref_offer(&self, version: &SealerVersion) -> RefOffer {
        let p = &self.payload;
        let deadline = if version.orders_by_position() {
            position_deadline(p.stream_position).unwrap_or(p.max_inclusion_block)
        } else {
            p.max_inclusion_block
        };
        RefOffer {
            tx_ref: kardamom_types::TxRef::new(
                self.tx_hash,
                p.lane,
                p.loc.position,
                p.loc.session_id,
            ),
            guard: GuardHeader {
                sender: self.sender,
                nonce: self.nonce,
                max_inclusion_block: deadline,
                tip: p.tip,
            },
            envelope: p.envelope.clone().filter(|_| version.carries_bytes()),
        }
    }
}

pub(super) struct OfferReplay<'a> {
    admission: &'a mut Admission<RefPayload>,
    outbox: &'a mut VecDeque<RefOffer>,
    version: &'a SealerVersion,
    partition: u32,
}

impl<'a> OfferReplay<'a> {
    pub(super) fn new(
        admission: &'a mut Admission<RefPayload>,
        outbox: &'a mut VecDeque<RefOffer>,
        version: &'a SealerVersion,
        partition: u32,
    ) -> Self {
        Self {
            admission,
            outbox,
            version,
            partition,
        }
    }

    /// Replace queued offers with the current unconfirmed runs in nonce
    /// order. Rows can confirm a prefix throughout a pause or reconnect.
    pub(super) fn queue(&mut self, reason: &str) {
        let offers: Vec<RefOffer> = self
            .admission
            .unconfirmed()
            .map(|candidate| candidate.ref_offer(self.version))
            .collect();
        tracing::warn!(
            partition = self.partition,
            offers = offers.len(),
            reason,
            "sequencer v2: offering the unconfirmed runs again"
        );
        self.outbox.clear();
        self.outbox.extend(offers);
    }

    /// Replace the queued offers of `senders` with their current runs in
    /// nonce order. The offers of other senders keep their place.
    #[cold]
    pub(super) fn queue_senders(&mut self, senders: &BTreeSet<Address>) {
        self.outbox
            .retain(|offer| !senders.contains(&offer.guard.sender));
        let admission = &*self.admission;
        let version = self.version;
        self.outbox.extend(
            senders
                .iter()
                .flat_map(|sender| admission.unconfirmed_of(*sender))
                .map(|candidate| candidate.ref_offer(version)),
        );
        tracing::debug!(
            partition = self.partition,
            senders = senders.len(),
            "sequencer v2: offering the refused runs again"
        );
    }
}

impl AdmissionLoop {
    /// The guard of the sealer expects `expected` of `sender`, and rejected
    /// `nonce`. Offer the transactions at both nonces again, when the
    /// filter holds the one at `expected`. Each such reject moves the
    /// expected nonce up by at least one, so the offers end. A guard that
    /// expects a nonce that the filter does not hold changes nothing: the
    /// loss window frees the run.
    pub(super) fn offer_rejected(&mut self, sender: Address, nonce: u64, expected: u64) {
        if self.admission.unconfirmed_at(sender, expected).is_none() {
            return;
        }
        let offers: Vec<RefOffer> = [expected, nonce]
            .into_iter()
            .filter_map(|n| self.admission.unconfirmed_at(sender, n))
            .map(|c| c.ref_offer(&self.sealer_version))
            .collect();
        self.outbox.extend(offers);
    }
}

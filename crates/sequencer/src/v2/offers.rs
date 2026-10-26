//! Wire offers and replay of retained runs, before any new intake.

use std::collections::VecDeque;

use kardamom_cluster_adapter::wire::{GuardHeader, position_deadline};

use super::RefPayload;
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

    #[cold]
    pub(super) fn resume(&mut self) {
        self.admission.resume_loss_window();
        self.queue("lifecycle resumed");
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
}

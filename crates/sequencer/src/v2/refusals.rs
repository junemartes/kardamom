//! Terminal sealer answers must settle each transaction that leaves admission.

use kardamom_types::TxErrorReason;

use super::{AdmissionLoop, RefPayload, reject};
use crate::admission::Candidate;
use crate::outbound::SideChannelPublisher;
use crate::resync::{ResyncController, SealerRefusal};

impl AdmissionLoop {
    /// Consume the terminal answers of the existing egress feed.
    pub fn enable_sealer_refusals(&mut self, controller: ResyncController) {
        self.sealer_refusals = Some(controller);
    }

    pub(super) fn apply_sealer_refusals<R: SideChannelPublisher>(&mut self, side: &mut R) -> bool {
        let answers = self
            .sealer_refusals
            .as_mut()
            .map(ResyncController::drain_deadline_rejects)
            .unwrap_or_default();
        let applied = !answers.is_empty();
        for answer in &answers {
            self.refused(side, answer);
        }
        applied
    }

    fn refused<R: SideChannelPublisher>(&mut self, side: &mut R, answer: &SealerRefusal) {
        let Some(candidate) = self.admission.unconfirmed_at(answer.sender, answer.nonce) else {
            return;
        };
        if let TxErrorReason::PastDeadline {
            max_inclusion_block,
            ..
        } = &answer.reason
            && candidate
                .ref_offer(&self.sealer_version)
                .guard
                .max_inclusion_block
                != *max_inclusion_block
        {
            return;
        }
        let run: Vec<_> = self
            .admission
            .unconfirmed_for(answer.sender)
            .cloned()
            .collect();
        self.outbox
            .retain(|offer| offer.guard.sender != answer.sender);
        self.admission
            .on_refused(answer.sender, answer.nonce, &mut self.verdicts);
        for candidate in &run {
            answer.report(side, candidate);
        }
    }
}

impl SealerRefusal {
    fn report<R: SideChannelPublisher>(&self, side: &mut R, candidate: &Candidate<RefPayload>) {
        let reason = match &self.reason {
            TxErrorReason::DaLag { .. } | TxErrorReason::RecordLag { .. } => self.reason.clone(),
            reason if candidate.nonce == self.nonce => reason.clone(),
            _ => TxErrorReason::Expired {
                expected_nonce: self.nonce,
            },
        };
        reject(
            side,
            candidate.tx_hash,
            candidate.sender,
            candidate.nonce,
            reason,
        );
    }
}

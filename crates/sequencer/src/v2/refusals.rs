//! Refusals preserve transaction identity and never silently discard a suffix.

use std::collections::BTreeSet;

use alloy_primitives::Address;
use kardamom_types::TxErrorReason;

use super::{AdmissionLoop, RefPayload};
use crate::admission::{Candidate, Verdict};
use crate::resync::SealerRefusal;

impl AdmissionLoop {
    #[cold]
    pub(super) fn on_reference_refused(
        &mut self,
        refused: kardamom_cluster_adapter::wire::RefRefused,
    ) -> bool {
        let eligible = self
            .admission
            .unconfirmed_at(refused.sender, refused.nonce)
            .is_some_and(|candidate| {
                candidate.tx_hash == refused.canonical_id
                    && (candidate.payload.envelope.is_some() || candidate.payload.canonical)
            });
        if eligible {
            return true;
        }
        let Some(candidate) = self.remove_refused(refused.sender, refused.nonce, |candidate| {
            candidate.tx_hash == refused.canonical_id && candidate.payload.envelope.is_none()
        }) else {
            return false;
        };
        self.verdicts.push(Verdict::DropReference(candidate));
        true
    }

    pub(super) fn on_sealer_refusal(&mut self, refusal: SealerRefusal) -> bool {
        if matches!(
            refusal.reason,
            TxErrorReason::DaLag { .. } | TxErrorReason::RecordLag { .. }
        ) {
            return self
                .admission
                .unconfirmed_at(refusal.sender, refusal.nonce)
                .is_some();
        }
        let named = |c: &Candidate<RefPayload>| {
            !c.payload.canonical
                && match &refusal.reason {
                    TxErrorReason::PastDeadline {
                        max_inclusion_block,
                        ..
                    } => c.payload.max_inclusion_block == *max_inclusion_block,
                    _ => true,
                }
        };
        let Some(candidate) = self.remove_refused(refusal.sender, refusal.nonce, named) else {
            return false;
        };
        self.verdicts
            .push(Verdict::SealerRefused(candidate, refusal.reason));
        true
    }

    pub(super) fn remove_refused(
        &mut self,
        sender: Address,
        nonce: u64,
        named: impl Fn(&Candidate<RefPayload>) -> bool,
    ) -> Option<Candidate<RefPayload>> {
        let run = self.admission.take_refused(sender, nonce, named)?;
        let candidate = if run.was_parked {
            self.depth.gave(run.candidate.payload.vslot);
            run.candidate
        } else {
            self.mark_answered(run.candidate)
        };
        run.suffix
            .into_iter()
            .for_each(|candidate| self.repark(candidate));
        Some(candidate)
    }

    fn mark_answered(&self, mut candidate: Candidate<RefPayload>) -> Candidate<RefPayload> {
        let answer_waits = self.verdicts.iter().any(|verdict| matches!(verdict, Verdict::Forward(c, _) if c.tx_hash == candidate.tx_hash && c.image == candidate.image));
        candidate.answered |= !answer_waits;
        candidate
    }

    fn repark(&mut self, candidate: Candidate<RefPayload>) {
        let candidate = self.mark_answered(candidate);
        self.depth.took(candidate.payload.vslot);
        self.admission.offer(candidate, &mut self.verdicts);
    }

    /// Coalesce a batch of notices into one replay. Pending forwards that
    /// a terminal refusal removed cannot escape from the verdict queue.
    /// A notice that names a sender replays and restarts only that
    /// sender's run. A rollback replays every run.
    pub(super) fn replay_after_refusals(&mut self, replay: &Replay) {
        if replay.all || !replay.senders.is_empty() {
            self.replay_noticed(replay);
        }
    }

    #[cold]
    fn replay_noticed(&mut self, replay: &Replay) {
        self.drop_removed_forwards();
        if replay.all {
            self.resume_unconfirmed("sealer refusal");
            return;
        }
        self.admission.restart_loss_window(&replay.senders);
        self.canonical.forget(&replay.senders);
        self.replay().queue_senders(&replay.senders);
    }

    fn drop_removed_forwards(&mut self) {
        let admission = &self.admission;
        let depth = &mut self.depth;
        self.verdicts.retain(|verdict| {
            let Verdict::Forward(candidate, _) = verdict else {
                return true;
            };
            let held = admission
                .unconfirmed_at(candidate.sender, candidate.nonce)
                .is_some_and(|c| c.tx_hash == candidate.tx_hash && c.image == candidate.image);
            if !held {
                depth.gave(candidate.payload.vslot);
            }
            held
        });
    }
}

/// What one account event asks of the replay.
#[derive(Clone, Copy)]
pub(super) enum Notice {
    Nothing,
    /// The sealer refused an offer of this sender.
    Sender(Address),
    /// Every held run can be wrong at the sealer.
    All,
}

impl Notice {
    /// A notice for `sender` when `requested`, else nothing.
    pub(super) fn named(requested: bool, sender: Address) -> Self {
        if requested {
            Self::Sender(sender)
        } else {
            Self::Nothing
        }
    }
}

/// The replay of one batch of account events.
#[derive(Default)]
pub(super) struct Replay {
    all: bool,
    senders: BTreeSet<Address>,
}

impl Replay {
    pub(super) fn note(mut self, notice: Notice) -> Self {
        match notice {
            Notice::Nothing => {}
            Notice::Sender(sender) => {
                self.senders.insert(sender);
            }
            Notice::All => self.all = true,
        }
        self
    }
}

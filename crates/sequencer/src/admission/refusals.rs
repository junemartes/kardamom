//! Removing one refused copy leaves a gap before its unconfirmed suffix.

use std::collections::{BTreeSet, VecDeque};

use alloy_primitives::Address;

use super::{Admission, Candidate};

pub(super) struct MarkRetention<'a> {
    pub(super) marks: &'a mut VecDeque<super::ForwardMark>,
    pub(super) stamped: &'a mut usize,
}

impl MarkRetention<'_> {
    pub(super) fn retain(&mut self, keep: impl Fn(&super::ForwardMark) -> bool) {
        let pending = self.marks.split_off(*self.stamped);
        self.marks.retain(&keep);
        *self.stamped = self.marks.len();
        self.marks.extend(pending.into_iter().filter(keep));
    }
}

pub(crate) struct RefusedRun<T> {
    pub(crate) candidate: Candidate<T>,
    pub(crate) suffix: VecDeque<Candidate<T>>,
    pub(crate) was_parked: bool,
}

impl<T: Clone> Admission<T> {
    pub(crate) fn take_refused(
        &mut self,
        sender: Address,
        nonce: u64,
        named: impl Fn(&Candidate<T>) -> bool,
    ) -> Option<RefusedRun<T>> {
        let run = self.senders.get_mut(&sender)?.take_refused(nonce, named)?;
        if !run.was_parked {
            self.remove_forward_marks(sender, nonce);
        }
        Some(run)
    }

    /// A replacement at the same nonce gets its own full loss window.
    /// The refused copy and its parked suffix leave no old loss marks.
    pub(super) fn remove_forward_marks(&mut self, sender: Address, nonce: u64) {
        let keep = |mark: &super::ForwardMark| mark.sender != sender || mark.nonce < nonce;
        MarkRetention {
            marks: &mut self.forwards,
            stamped: &mut self.stamped,
        }
        .retain(keep);
    }

    /// The sealer refused an offer of each sender of `senders`: their held
    /// runs get a new loss window from the newest block. The marks of other
    /// senders keep their blocks. The restarted marks move to the end of
    /// the stamped range, so the stamped marks stay in block order.
    #[cold]
    pub(crate) fn restart_loss_window(&mut self, senders: &BTreeSet<Address>) {
        let pending = self.forwards.split_off(self.stamped);
        let (restarted, kept): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.forwards)
            .into_iter()
            .partition(|mark| senders.contains(&mark.sender));
        let block = self.block;
        let held = &self.senders;
        self.forwards = kept;
        self.forwards.extend(
            restarted
                .into_iter()
                .filter(|mark| {
                    held.get(&mark.sender)
                        .is_some_and(|s| s.holds_loss_mark(mark.nonce, mark.generation))
                })
                .map(|mark| super::ForwardMark { block, ..mark }),
        );
        self.stamped = self.forwards.len();
        self.forwards.extend(pending);
    }

    /// The held run of `sender`, in nonce order.
    pub(crate) fn unconfirmed_of(&self, sender: Address) -> impl Iterator<Item = &Candidate<T>> {
        self.senders
            .get(&sender)
            .into_iter()
            .flat_map(super::sender::SenderState::unconfirmed)
    }
}

//! Removing one refused copy leaves a gap before its unconfirmed suffix.

use std::collections::VecDeque;

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
}

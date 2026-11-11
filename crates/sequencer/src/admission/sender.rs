//! The admission state of one sender: its chain state, its forwarded run,
//! and its parked transactions.

use std::collections::{BTreeMap, VecDeque};

use alloy_primitives::U256;

use super::{AccountState, AdmissionConfig, Candidate, Refusal, RunGeneration, Verdict};

/// When a parked transaction gets its ack.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// Now: the state of the sender is known, and the transaction waits
    /// for a nonce gap or a busy run.
    Accept,
    /// When the state of the sender comes: a lookup is out.
    Wait,
}

#[cfg(test)]
#[path = "sender_loss_tests.rs"]
mod tests;

pub(super) struct SenderState<T> {
    /// The newest chain state, `None` until a row or a lookup gives one.
    state: Option<AccountState>,
    /// A lookup is out for this sender.
    asked: bool,
    /// The state came from a lookup, not from rows. The state database of
    /// an executor commits after the receipts go out, so such a state can
    /// be behind a nonce that the rows showed before this replica started.
    from_lookup: bool,
    /// A relook of this sender waits in the queue of the filter.
    relook_due: bool,
    /// The nonce of the first forwarded transaction that no state confirmed.
    first: u64,
    /// Loss markers identify this run, including after a nonce is reused.
    generation: u32,
    /// The forwarded run, from `first` on. The filter keeps each
    /// transaction until a state confirms it, so the caller can offer the
    /// run again when the sealer can have lost an offer.
    forwarded: VecDeque<Candidate<T>>,
    /// The sum of `forwarded`.
    spend: U256,
    /// The transactions that wait for a nonce gap, or for a lookup.
    parked: BTreeMap<u64, Candidate<T>>,
}

impl<T> Default for SenderState<T> {
    fn default() -> Self {
        Self {
            state: None,
            asked: false,
            from_lookup: false,
            relook_due: false,
            first: 0,
            generation: 0,
            forwarded: VecDeque::new(),
            spend: U256::ZERO,
            parked: BTreeMap::new(),
        }
    }
}

impl<T: Clone> SenderState<T> {
    /// Remove only the copy the refusal names. A forwarded suffix leaves
    /// with it, so the caller parks it behind the missing nonce.
    pub(super) fn take_refused(
        &mut self,
        nonce: u64,
        named: impl Fn(&Candidate<T>) -> bool,
    ) -> Option<super::refusals::RefusedRun<T>> {
        if self.forwarded_at(nonce).is_some_and(&named) {
            let at = usize::try_from(nonce.checked_sub(self.first)?).ok()?;
            let mut suffix = self.forwarded.split_off(at);
            self.spend = suffix
                .iter()
                .fold(self.spend, |spend, c| spend.saturating_sub(c.cost));
            return suffix
                .pop_front()
                .map(|candidate| super::refusals::RefusedRun {
                    candidate,
                    suffix,
                    was_parked: false,
                });
        }
        if self.parked.get(&nonce).is_some_and(named) {
            return self
                .parked
                .remove(&nonce)
                .map(|candidate| super::refusals::RefusedRun {
                    candidate,
                    suffix: VecDeque::new(),
                    was_parked: true,
                });
        }
        None
    }

    /// Decide one transaction of this sender. Returns whether it parked.
    pub(super) fn offer(
        &mut self,
        c: Candidate<T>,
        cfg: &AdmissionConfig,
        out: &mut Vec<Verdict<T>>,
    ) -> bool {
        let Some(state) = self.state else {
            return self.park_for_lookup(c, cfg, out);
        };
        if c.nonce < state.nonce {
            out.push(Verdict::Reject(
                c,
                Refusal::NonceTooLow {
                    chain_nonce: state.nonce,
                },
            ));
            return false;
        }
        let next = self.next_nonce(&state);
        if c.nonce < next {
            let refusal = Self::held_at(self.forwarded_at(c.nonce), &c, next);
            out.push(Verdict::Reject(c, refusal));
            return false;
        }
        if c.nonce > next {
            return self.park(c, Answer::Accept, cfg, out);
        }
        let parked = self.forward_or_park(c, &state, cfg, out);
        while self.release_one(cfg, out) {}
        parked
    }

    /// Apply a newer chain state: confirm the forwarded run up to its
    /// nonce, refuse the parked transactions below it, and forward the
    /// parked ones that now continue the run.
    /// Any answer of a lookup ends the lookup that was out, also one that
    /// is not newer than the state held.
    pub(super) fn on_state(
        &mut self,
        state: AccountState,
        from_lookup: bool,
        cfg: &AdmissionConfig,
        out: &mut Vec<Verdict<T>>,
    ) {
        if from_lookup {
            self.asked = false;
        }
        if self.state.is_some_and(|held| !state.is_newer_than(&held)) {
            return;
        }
        self.state = Some(state);
        self.from_lookup = from_lookup;
        self.asked = false;
        while self.confirm_one(state.nonce) {}
        let keep = self.parked.split_off(&state.nonce);
        let below = std::mem::replace(&mut self.parked, keep);
        out.extend(below.into_values().map(|c| {
            Verdict::Reject(
                c,
                Refusal::NonceTooLow {
                    chain_nonce: state.nonce,
                },
            )
        }));
        while self.release_one(cfg, out) {}
        self.accept_parked(out);
    }

    /// Accept the parked transactions that waited for a state with no
    /// answer: the state came, and they still wait for a nonce gap.
    fn accept_parked(&mut self, out: &mut Vec<Verdict<T>>) {
        out.extend(
            self.parked
                .values_mut()
                .filter_map(Candidate::answer)
                .map(Verdict::Accept),
        );
    }

    /// The forwarded transaction at `nonce` is lost if the run still holds
    /// it: forget the run, and forward the parked transactions from the
    /// chain nonce. Returns whether the run changed.
    pub(super) fn on_lost(
        &mut self,
        nonce: u64,
        generation: Option<u32>,
        cfg: &AdmissionConfig,
        out: &mut Vec<Verdict<T>>,
    ) -> bool {
        let current = generation.map_or_else(
            || self.forwarded_at(nonce).is_some(),
            |generation| self.holds_loss_mark(nonce, generation),
        );
        if !current {
            return false;
        }
        self.forwarded.clear();
        self.spend = U256::ZERO;
        while self.release_one(cfg, out) {}
        true
    }

    /// Take the parked transaction at `nonce` out, when it is the one that
    /// parked at `unit` of its image.
    pub(super) fn unpark(&mut self, nonce: u64, unit: u64) -> Option<Candidate<T>> {
        let held = self
            .parked
            .get(&nonce)
            .is_some_and(|c| c.image.unit == unit);
        held.then(|| self.parked.remove(&nonce)).flatten()
    }

    /// Whether a parked transaction waits for a nonce gap over a state from
    /// a lookup, with nothing forwarded: the gap can be the lag of the
    /// state database, not a missing transaction.
    pub(super) fn waits_on_lookup_gap(&self) -> bool {
        let gap = self
            .state
            .zip(self.parked.first_key_value())
            .is_some_and(|(state, (nonce, _))| *nonce > state.nonce);
        self.from_lookup && self.forwarded.is_empty() && gap
    }

    /// Mark a relook as queued. Returns false when one is queued already.
    pub(super) fn queue_relook(&mut self) -> bool {
        !std::mem::replace(&mut self.relook_due, true)
    }

    /// The queued relook is due: ask again when the gap over the lookup
    /// state is still there and no lookup is out.
    pub(super) fn relook(&mut self) -> bool {
        self.relook_due = false;
        self.waits_on_lookup_gap() && self.ask_once()
    }

    /// A parked transaction expired. The lookup of a sender retries only
    /// for the lifetime of a parked transaction, and a lookup that ends
    /// without an answer sends nothing back. So at the expiry, the lookup
    /// that was out has ended, the first lookup and a relook alike: the
    /// next transaction, or the next relook, asks again. Without this, a
    /// sender whose lookup found no source stays parked until its rows
    /// come, and no row comes for a sender that nothing forwards.
    pub(super) fn on_expired(&mut self) {
        self.asked = false;
    }

    /// Forget the chain state: the next transaction asks for a lookup.
    pub(super) fn forget_state(&mut self) {
        self.state = None;
        self.asked = false;
    }

    /// Nothing forwarded waits for a state, and nothing is parked.
    pub(super) fn is_idle(&self) -> bool {
        self.forwarded.is_empty() && self.parked.is_empty()
    }

    /// The next nonce that the filter expects of the sender, 0 while its
    /// state is unknown.
    pub(super) fn expected_nonce(&self) -> u64 {
        self.state.map_or(0, |state| self.next_nonce(&state))
    }

    fn next_nonce(&self, state: &AccountState) -> u64 {
        if self.forwarded.is_empty() {
            return state.nonce;
        }
        self.first + self.forwarded.len() as u64
    }

    /// The forwarded transaction at `nonce` that no state confirmed.
    pub(super) fn forwarded_at(&self, nonce: u64) -> Option<&Candidate<T>> {
        let at = usize::try_from(nonce.checked_sub(self.first)?).ok()?;
        self.forwarded.get(at)
    }

    pub(super) fn forwarded_at_mut(&mut self, nonce: u64) -> Option<&mut Candidate<T>> {
        let at = usize::try_from(nonce.checked_sub(self.first)?).ok()?;
        self.forwarded.get_mut(at)
    }

    pub(super) fn retained_mut(&mut self) -> impl Iterator<Item = &mut Candidate<T>> {
        self.forwarded.iter_mut().chain(self.parked.values_mut())
    }

    pub(super) fn holds_loss_mark(&self, nonce: u64, generation: u32) -> bool {
        self.generation == generation && self.forwarded_at(nonce).is_some()
    }

    /// The forwarded transactions that no state confirmed, in nonce order.
    pub(super) fn unconfirmed(&self) -> impl Iterator<Item = &Candidate<T>> {
        self.forwarded.iter()
    }

    /// Drop the first forwarded transaction when the chain is past it.
    fn confirm_one(&mut self, chain_nonce: u64) -> bool {
        if self.forwarded.is_empty() || self.first >= chain_nonce {
            return false;
        }
        let cost = self.forwarded.pop_front().map_or(U256::ZERO, |c| c.cost);
        self.spend = self.spend.saturating_sub(cost);
        self.first += 1;
        true
    }

    /// The refusal of `c` at a nonce where the filter forwarded or holds
    /// `held`: the same transaction is [`Refusal::Known`], another one
    /// [`Refusal::InFlight`].
    fn held_at(held: Option<&Candidate<T>>, c: &Candidate<T>, expected_nonce: u64) -> Refusal {
        if held.is_some_and(|held| held.tx_hash == c.tx_hash) {
            Refusal::Known { expected_nonce }
        } else {
            Refusal::InFlight { expected_nonce }
        }
    }

    /// The test seam of the `floor-seams` feature: whether a row must
    /// wait, because this sender has no state. That is a sender that no
    /// row or lookup reached yet, and also a sender whose state a rollback
    /// cleared. With parked work, the sender asks a lookup once, as
    /// [`Self::park_for_lookup`] does.
    #[cfg(feature = "floor-seams")]
    pub(super) fn waits_for_lookup(
        &mut self,
        address: alloy_primitives::Address,
        out: &mut Vec<Verdict<T>>,
    ) -> bool {
        if self.state.is_some() {
            return false;
        }
        if !self.parked.is_empty() && self.ask_once() {
            out.push(Verdict::Lookup(address));
        }
        true
    }

    /// Park a transaction of a sender with no state, and ask for a lookup
    /// once.
    fn park_for_lookup(
        &mut self,
        c: Candidate<T>,
        cfg: &AdmissionConfig,
        out: &mut Vec<Verdict<T>>,
    ) -> bool {
        let sender = c.sender;
        let parked = self.park(c, Answer::Wait, cfg, out);
        if parked && self.ask_once() {
            out.push(Verdict::Lookup(sender));
        }
        parked
    }

    /// Mark a lookup as out. Returns whether none was out, so that the
    /// caller asks: a sender has one lookup out at most.
    fn ask_once(&mut self) -> bool {
        !std::mem::replace(&mut self.asked, true)
    }

    /// Park a transaction. With [`Answer::Accept`] the client gets its ack
    /// now, once; with [`Answer::Wait`] it waits for the state of the
    /// sender ([`Self::accept_parked`]).
    fn park(
        &mut self,
        mut c: Candidate<T>,
        answer: Answer,
        cfg: &AdmissionConfig,
        out: &mut Vec<Verdict<T>>,
    ) -> bool {
        let expected_nonce = self.expected_nonce();
        if let Some(held) = self.parked.get(&c.nonce) {
            let refusal = Self::held_at(Some(held), &c, expected_nonce);
            out.push(Verdict::Reject(c, refusal));
            return false;
        }
        if self.parked.len() >= cfg.max_parked.get() {
            out.push(Verdict::Reject(
                c,
                Refusal::TooManyPending { expected_nonce },
            ));
            return false;
        }
        if answer == Answer::Accept {
            out.extend(c.answer().map(Verdict::Accept));
        }
        self.parked.insert(c.nonce, c);
        true
    }

    /// Whether the run takes no more transactions now: it is full, or the
    /// account has delegated code and one transaction is out.
    fn busy(&self, state: &AccountState, cfg: &AdmissionConfig) -> bool {
        self.forwarded.len() >= cfg.max_forwarded.get()
            || (state.delegated && !self.forwarded.is_empty())
    }

    /// Forward a transaction at the next nonce when the balance covers it,
    /// or park it while the run is busy. Returns whether it parked.
    fn forward_or_park(
        &mut self,
        c: Candidate<T>,
        state: &AccountState,
        cfg: &AdmissionConfig,
        out: &mut Vec<Verdict<T>>,
    ) -> bool {
        if self.busy(state, cfg) {
            return self.park(c, Answer::Accept, cfg, out);
        }
        let available = state.balance.saturating_sub(self.spend);
        if available < c.cost {
            let cost = c.cost;
            out.push(Verdict::Reject(
                c,
                Refusal::InsufficientFunds { available, cost },
            ));
            return false;
        }
        let reset = if self.forwarded.is_empty() {
            self.first = c.nonce;
            // Rollover removes older tags before the first new loss marker.
            self.generation = self.generation.wrapping_add(1);
            self.generation == 0
        } else {
            false
        };
        self.spend = self.spend.saturating_add(c.cost);
        self.forwarded.push_back(c.clone());
        out.push(Verdict::Forward(
            c,
            RunGeneration {
                value: self.generation,
                reset,
            },
        ));
        false
    }

    /// Forward the parked transaction at the next nonce. Returns whether it
    /// went out, so the caller tries the one after it.
    fn release_one(&mut self, cfg: &AdmissionConfig, out: &mut Vec<Verdict<T>>) -> bool {
        let Some(state) = self.state else {
            return false;
        };
        let next = self.next_nonce(&state);
        if self.busy(&state, cfg)
            || self
                .parked
                .first_key_value()
                .is_none_or(|(n, _)| *n != next)
        {
            return false;
        }
        let Some((_, c)) = self.parked.pop_first() else {
            return false;
        };
        let before = self.forwarded.len();
        self.forward_or_park(c, &state, cfg, out);
        self.forwarded.len() > before
    }
}

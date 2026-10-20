//! The admission state of one sender: its chain state, its forwarded run,
//! and its parked transactions.

use std::collections::{BTreeMap, VecDeque};

use alloy_primitives::U256;

use super::{AccountState, AdmissionConfig, Candidate, Refusal, Verdict};

pub(super) struct SenderState<T> {
    /// The newest chain state, `None` until a row or a lookup gives one.
    state: Option<AccountState>,
    /// A lookup is out for this sender.
    asked: bool,
    /// The nonce of the first forwarded transaction that no state confirmed.
    first: u64,
    /// The worst-case costs of the forwarded run, from `first` on.
    forwarded: VecDeque<U256>,
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
            first: 0,
            forwarded: VecDeque::new(),
            spend: U256::ZERO,
            parked: BTreeMap::new(),
        }
    }
}

impl<T> SenderState<T> {
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
            out.push(Verdict::Reject(
                c,
                Refusal::InFlight {
                    expected_nonce: next,
                },
            ));
            return false;
        }
        if c.nonce > next {
            return self.park(c, cfg, out);
        }
        let parked = self.forward_or_park(c, &state, cfg, out);
        while self.release_one(cfg, out) {}
        parked
    }

    /// Apply a newer chain state: confirm the forwarded run up to its
    /// nonce, refuse the parked transactions below it, and forward the
    /// parked ones that now continue the run.
    pub(super) fn on_state(
        &mut self,
        state: AccountState,
        cfg: &AdmissionConfig,
        out: &mut Vec<Verdict<T>>,
    ) {
        if self.state.is_some_and(|held| held.tx_idx >= state.tx_idx) {
            return;
        }
        self.state = Some(state);
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
    }

    /// The forwarded transaction at `nonce` is lost if the run still holds
    /// it: forget the run, and forward the parked transactions from the
    /// chain nonce. Returns whether the run changed.
    pub(super) fn on_lost(
        &mut self,
        nonce: u64,
        cfg: &AdmissionConfig,
        out: &mut Vec<Verdict<T>>,
    ) -> bool {
        if !self.holds_forwarded(nonce) {
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

    fn holds_forwarded(&self, nonce: u64) -> bool {
        nonce >= self.first && nonce - self.first < self.forwarded.len() as u64
    }

    /// Drop the first forwarded transaction when the chain is past it.
    fn confirm_one(&mut self, chain_nonce: u64) -> bool {
        if self.forwarded.is_empty() || self.first >= chain_nonce {
            return false;
        }
        let cost = self.forwarded.pop_front().unwrap_or_default();
        self.spend = self.spend.saturating_sub(cost);
        self.first += 1;
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
        let parked = self.park(c, cfg, out);
        if parked && !self.asked {
            self.asked = true;
            out.push(Verdict::Lookup(sender));
        }
        parked
    }

    fn park(&mut self, c: Candidate<T>, cfg: &AdmissionConfig, out: &mut Vec<Verdict<T>>) -> bool {
        let expected_nonce = self.expected_nonce();
        if self.parked.contains_key(&c.nonce) {
            out.push(Verdict::Reject(c, Refusal::InFlight { expected_nonce }));
            return false;
        }
        if self.parked.len() >= cfg.max_parked.get() {
            out.push(Verdict::Reject(
                c,
                Refusal::TooManyPending { expected_nonce },
            ));
            return false;
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
            return self.park(c, cfg, out);
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
        if self.forwarded.is_empty() {
            self.first = c.nonce;
        }
        self.forwarded.push_back(c.cost);
        self.spend = self.spend.saturating_add(c.cost);
        out.push(Verdict::Forward(c));
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

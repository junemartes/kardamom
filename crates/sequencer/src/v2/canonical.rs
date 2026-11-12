//! A sender's next offer waits until its preceding offer is canonical.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;

use alloy_primitives::{Address, B256};

use super::RefPayload;
use crate::admission::{Admission, Candidate};
use crate::outbound::RefOffer;

struct Flight {
    nonce: u64,
    hash: B256,
}

pub(super) struct CanonicalGate {
    flights: HashMap<Address, Flight>,
    held: HashMap<B256, (Address, u64)>,
    seen: HashSet<B256>,
    history: VecDeque<B256>,
    history_cap: NonZeroUsize,
    selected: HashSet<Address>,
    ready: Vec<RefOffer>,
    /// The offers at the end of the outbox that no scan saw yet.
    fresh: usize,
    /// The reused buffer of a scan of the fresh offers.
    tail: Vec<RefOffer>,
    changed: bool,
    #[cfg(test)]
    pub(super) scans: usize,
}

impl CanonicalGate {
    pub(super) fn new(history_cap: NonZeroUsize) -> Self {
        Self {
            flights: HashMap::new(),
            held: HashMap::new(),
            seen: HashSet::new(),
            history: VecDeque::new(),
            history_cap,
            selected: HashSet::new(),
            ready: Vec::new(),
            fresh: 0,
            tail: Vec::new(),
            changed: true,
            #[cfg(test)]
            scans: 0,
        }
    }

    /// Every queued offer is in `held`. So a record of another hash leaves
    /// the outbox as it is, and the next flush does not scan it.
    pub(super) fn observe(&mut self, hash: B256, admission: &mut Admission<RefPayload>) {
        if let Some((address, nonce)) = self.held.remove(&hash) {
            self.changed();
            self.confirm_hash(address, nonce, hash, admission);
        }
        if !self.seen.insert(hash) {
            return;
        }
        self.history.push_back(hash);
        if self.history.len() > self.history_cap.get()
            && let Some(old) = self.history.pop_front()
        {
            self.seen.remove(&old);
        }
    }

    fn confirm_hash(
        &mut self,
        address: Address,
        nonce: u64,
        hash: B256,
        admission: &mut Admission<RefPayload>,
    ) {
        if let Some(candidate) = admission.unconfirmed_at_mut(address, nonce)
            && candidate.tx_hash == hash
        {
            candidate.payload.canonical = true;
        }
        if self
            .flights
            .get(&address)
            .is_some_and(|flight| flight.hash == hash)
        {
            self.flights.remove(&address);
        }
    }

    pub(super) fn track(&mut self, candidate: &Candidate<RefPayload>) {
        self.held
            .insert(candidate.tx_hash, (candidate.sender, candidate.nonce));
    }

    pub(super) fn confirmed(&mut self, address: Address, admission: &Admission<RefPayload>) {
        if self.flights.get(&address).is_some_and(|f| {
            admission
                .unconfirmed_at(address, f.nonce)
                .is_none_or(|c| c.tx_hash != f.hash)
        }) {
            self.flights.remove(&address);
        }
    }

    /// A reconnect or refusal reoffers each retained sender prefix first.
    /// Canonical observations remain valid across a session change.
    pub(super) fn reset(&mut self) {
        self.changed();
        self.flights.clear();
    }

    pub(super) fn rollback(&mut self) {
        self.reset();
        self.seen.clear();
        self.history.clear();
    }

    pub(super) fn prune(&mut self, admission: &Admission<RefPayload>) {
        self.changed();
        self.flights.retain(|address, flight| {
            admission
                .unconfirmed_at(*address, flight.nonce)
                .is_some_and(|c| c.tx_hash == flight.hash)
        });
        self.held.retain(|hash, (address, nonce)| {
            admission
                .unconfirmed_at(*address, *nonce)
                .is_some_and(|c| c.tx_hash == *hash && !c.payload.canonical)
        });
    }

    /// At most one offer per sender enters a batch. A per-entry refusal
    /// cannot leave a higher nonce already queued at the sealer.
    ///
    /// After a scan, every sender with a queued offer has a flight: the
    /// scan publishes the first offer of each other sender, and an offer
    /// that does not go out marks the gate as changed. So while nothing
    /// changed, only the fresh offers can be ready, and the scan reads only
    /// them.
    pub(super) fn prepare(
        &mut self,
        outbox: &mut VecDeque<RefOffer>,
        admission: &mut Admission<RefPayload>,
    ) -> Vec<RefOffer> {
        self.ready.clear();
        self.selected.clear();
        let fresh = std::mem::take(&mut self.fresh);
        if std::mem::take(&mut self.changed) {
            self.scan_all(outbox, admission);
        } else if fresh > 0 {
            self.scan_fresh(fresh, outbox, admission);
        }
        std::mem::take(&mut self.ready)
    }

    fn scan_all(&mut self, outbox: &mut VecDeque<RefOffer>, admission: &mut Admission<RefPayload>) {
        #[cfg(test)]
        {
            self.scans = self.scans.checked_add(1).unwrap();
        }
        outbox.retain(|offer| self.keep_queued(offer, admission));
    }

    fn scan_fresh(
        &mut self,
        fresh: usize,
        outbox: &mut VecDeque<RefOffer>,
        admission: &mut Admission<RefPayload>,
    ) {
        let mut tail = std::mem::take(&mut self.tail);
        tail.extend(outbox.drain(outbox.len().saturating_sub(fresh)..));
        outbox.extend(
            tail.drain(..)
                .filter(|offer| self.keep_queued(offer, admission)),
        );
        self.tail = tail;
    }

    /// Account events, replay and sweeps invalidate a blocked queue.
    /// Unpublished prefixes also require another scan on the next retry.
    pub(super) fn changed(&mut self) {
        self.changed = true;
    }

    /// A new offer went to the end of the outbox.
    pub(super) fn appended(&mut self) {
        self.fresh = self.fresh.saturating_add(1);
    }

    /// The sealer refused an offer of each sender of `senders`. Their runs
    /// go out again from the first held nonce.
    pub(super) fn forget(&mut self, senders: &BTreeSet<Address>) {
        self.changed();
        self.flights.retain(|address, _| !senders.contains(address));
    }

    fn keep_queued(&mut self, offer: &RefOffer, admission: &mut Admission<RefPayload>) -> bool {
        let address = offer.guard.sender;
        let Some(candidate) = admission
            .unconfirmed_at_mut(address, offer.guard.nonce)
            .filter(|c| c.tx_hash == offer.tx_ref.tx_hash)
        else {
            return false;
        };
        candidate.payload.canonical |= self.seen.contains(&offer.tx_ref.tx_hash);
        if candidate.payload.canonical {
            self.held.remove(&offer.tx_ref.tx_hash);
            return false;
        }
        self.track(candidate);
        if self.flights.contains_key(&address) || !self.selected.insert(address) {
            return true;
        }
        self.ready.push(offer.clone());
        false
    }

    pub(super) fn published(&mut self, offer: &RefOffer) {
        let address = offer.guard.sender;
        let hash = offer.tx_ref.tx_hash;
        self.flights.insert(
            address,
            Flight {
                nonce: offer.guard.nonce,
                hash,
            },
        );
    }

    pub(super) fn recycle(&mut self, ready: Vec<RefOffer>) {
        self.ready = ready;
    }
}

#[cfg(test)]
#[path = "canonical_gate_tests.rs"]
mod tests;

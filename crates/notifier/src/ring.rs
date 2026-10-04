//! The ring: the last `max_age` of status events, bounded by count, with
//! an index by transaction hash and by sender.
//!
//! Every event gets a sequence number when it is stored. A subscriber
//! replays the ring up to a sequence number, then follows the live feed
//! from the next one, so the hand-over between the two loses nothing and
//! repeats nothing.
//!
//! The ring is also the deduplicator: every executor publishes its copy
//! of a receipt, and both ingress replicas publish a `sealed` for every
//! relayed record. A transaction stores each stage once.
//!
//! The ring has one owner, the hub task. There are no locks here.

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use kardamom_types::TxStatus;

use crate::dto::{StatusFilter, TxStatusEvent};

/// The ring's bounds: by age, and by event count.
#[derive(Clone, Copy, Debug)]
pub struct RingConfig {
    pub max_age: Duration,
    pub max_events: NonZeroUsize,
}

/// One stored event with its sequence number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stamped {
    pub seq: u64,
    pub event: TxStatusEvent,
}

/// What [`Ring::insert`] did with a status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Insert {
    Stored(Stamped),
    /// The transaction already holds this stage.
    Duplicate,
}

struct Entry {
    seq: u64,
    at: Instant,
    event: TxStatusEvent,
}

/// What the ring knows about one transaction: its identity, once a
/// stage carried one, the stages it holds, and where its events sit.
#[derive(Default)]
struct TxEntry {
    identity: Option<(Address, u64)>,
    seen: u8,
    seqs: Vec<u64>,
}

pub struct Ring {
    cfg: RingConfig,
    events: VecDeque<Entry>,
    by_hash: HashMap<B256, TxEntry>,
    by_sender: HashMap<Address, Vec<B256>>,
    next_seq: u64,
}

impl Ring {
    #[must_use]
    pub fn new(cfg: RingConfig) -> Self {
        Self {
            cfg,
            events: VecDeque::new(),
            by_hash: HashMap::new(),
            by_sender: HashMap::new(),
            next_seq: 1,
        }
    }

    /// Store `status` at `now`, unless its transaction holds the stage
    /// already. The stored event carries the identity the ring knows
    /// for the hash, so a `sealed` that follows an `offered` names its
    /// sender.
    pub fn insert(&mut self, status: &TxStatus, now: Instant) -> Insert {
        let kind = status.kind();
        let entry = self.by_hash.entry(status.tx_hash).or_default();
        if entry.seen & kind.bit() != 0 {
            return Insert::Duplicate;
        }
        let learned = entry.identity.is_none() && status.identity().is_some();
        entry.identity = entry.identity.or(status.identity());
        entry.seen |= kind.bit();
        let seq = self.next_seq;
        // Saturating: a u64 of sequence numbers never runs out, and a
        // wrap would break the index arithmetic.
        self.next_seq = seq.saturating_add(1);
        entry.seqs.push(seq);
        let identity = entry.identity;
        if let Some((sender, _)) = identity.filter(|_| learned) {
            self.by_sender
                .entry(sender)
                .or_default()
                .push(status.tx_hash);
        }
        let event = TxStatusEvent::from_status(status, identity);
        self.events.push_back(Entry {
            seq,
            at: now,
            event: event.clone(),
        });
        Insert::Stored(Stamped { seq, event })
    }

    /// The hash of the transaction `sender` submitted at `nonce`, when the
    /// ring saw a stage that named both. A resubmitted nonce names the
    /// newest transaction.
    #[must_use]
    pub fn resolve(&self, sender: Address, nonce: u64) -> Option<B256> {
        self.by_sender
            .get(&sender)?
            .iter()
            .rev()
            .find(|hash| self.identity_of(hash) == Some((sender, nonce)))
            .copied()
    }

    fn identity_of(&self, hash: &B256) -> Option<(Address, u64)> {
        self.by_hash.get(hash).and_then(|e| e.identity)
    }

    /// Drop the events older than `max_age` at `now`, and the oldest
    /// events past `max_events`. Returns how many went.
    pub fn evict(&mut self, now: Instant) -> usize {
        let before = self.events.len();
        while self.evict_front(now).is_continue() {}
        before - self.events.len()
    }

    /// One eviction step: drop the oldest event if it is past a bound.
    fn evict_front(&mut self, now: Instant) -> ControlFlow<()> {
        let over = self.events.len() > self.cfg.max_events.get();
        let max_age = self.cfg.max_age;
        let Some(entry) = self
            .events
            .pop_front_if(|front| over || now.duration_since(front.at) > max_age)
        else {
            return ControlFlow::Break(());
        };
        self.forget(entry.event.tx_hash, entry.seq);
        ControlFlow::Continue(())
    }

    /// Remove `seq` from the transaction's index; forget the transaction
    /// once none of its events remain.
    fn forget(&mut self, hash: B256, seq: u64) {
        let Some(entry) = self.by_hash.get_mut(&hash) else {
            return;
        };
        entry.seqs.retain(|s| *s != seq);
        if !entry.seqs.is_empty() {
            return;
        }
        let identity = entry.identity;
        self.by_hash.remove(&hash);
        let Some((sender, _)) = identity else {
            return;
        };
        let Some(hashes) = self.by_sender.get_mut(&sender) else {
            return;
        };
        hashes.retain(|h| *h != hash);
        if hashes.is_empty() {
            self.by_sender.remove(&sender);
        }
    }

    /// The events `filter` selects with a sequence number above
    /// `after_seq`, oldest first, at most `limit`. A page shorter than
    /// `limit` is the last one.
    #[must_use]
    pub fn replay(&self, filter: &StatusFilter, after_seq: u64, limit: usize) -> Vec<Stamped> {
        match filter {
            StatusFilter::All { .. } => self.replay_all(after_seq, limit),
            StatusFilter::TxHash { tx_hash } => {
                self.replay_seqs(self.seqs_of(tx_hash), after_seq, limit)
            }
            StatusFilter::Sender { sender } => {
                let mut seqs: Vec<u64> = self
                    .by_sender
                    .get(sender)
                    .into_iter()
                    .flatten()
                    .flat_map(|hash| self.seqs_of(hash))
                    .collect();
                seqs.sort_unstable();
                self.replay_seqs(seqs, after_seq, limit)
            }
        }
    }

    fn seqs_of(&self, hash: &B256) -> Vec<u64> {
        self.by_hash
            .get(hash)
            .map(|e| e.seqs.clone())
            .unwrap_or_default()
    }

    fn replay_all(&self, after_seq: u64, limit: usize) -> Vec<Stamped> {
        let start = self.index_of(after_seq.saturating_add(1));
        self.events
            .range(start..)
            .take(limit)
            .map(Self::stamp)
            .collect()
    }

    fn replay_seqs(&self, seqs: Vec<u64>, after_seq: u64, limit: usize) -> Vec<Stamped> {
        seqs.into_iter()
            .filter(|seq| *seq > after_seq)
            .take(limit)
            .map(|seq| Self::stamp(&self.events[self.index_of(seq)]))
            .collect()
    }

    /// The deque index of `seq`, or of the oldest event when `seq` is
    /// older than the ring, or the end when `seq` is newer. Sequence
    /// numbers are dense and evicted oldest first, so the index is the
    /// distance from the front.
    fn index_of(&self, seq: u64) -> usize {
        let front = self.events.front().map_or(self.next_seq, |e| e.seq);
        usize::try_from(seq.saturating_sub(front))
            .unwrap_or(usize::MAX)
            .min(self.events.len())
    }

    fn stamp(entry: &Entry) -> Stamped {
        Stamped {
            seq: entry.seq,
            event: entry.event.clone(),
        }
    }

    /// The number of stored events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// The number of transactions with at least one stored event.
    #[must_use]
    pub fn transactions(&self) -> usize {
        self.by_hash.len()
    }

    /// The sequence number the next stored event gets.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

#[cfg(test)]
#[path = "ring_tests.rs"]
mod tests;

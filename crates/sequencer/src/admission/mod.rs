//! The admission filter of sequencer v2: what a sequencer forwards to the
//! sealer.
//!
//! The filter keeps spam off the ordered log. It is not a rule of the
//! chain: the executor decides every transaction again. So the filter
//! needs no agreement between racing replicas. The sealer orders a
//! reference once, whichever replica forwards it first.
//!
//! For each sender the filter holds:
//! - the newest chain state of the account ([`AccountState`]): the rows
//!   that the executor publishes for each block, or the answer of a
//!   lookup for a sender that it has not seen;
//! - the run of transactions that it forwarded and that no state confirmed
//!   yet, with their summed worst-case cost;
//! - the transactions that wait for a nonce gap to close.
//!
//! A transaction at the next nonce goes out when the balance of the state,
//! less the cost of the forwarded run, covers its worst-case cost. The
//! balance of an account goes down only through its own transactions, and
//! each one moves its nonce. So a state that is behind the chain can only
//! understate what the sender holds. An account with delegated code
//! (EIP-7702) is the exception: another transaction can move its balance
//! through its code. Such an account gets one forwarded transaction at a
//! time.
//!
//! Two expiries keep the state bounded:
//! - A forwarded transaction that no state confirms within
//!   [`AdmissionConfig::loss_blocks`] blocks is lost: the sealer dropped
//!   it, or the executor skipped it with no nonce. The filter forgets the
//!   run, and the sender can send from its chain nonce again.
//! - A parked transaction expires when its ingress image moves
//!   [`AdmissionConfig::park_width`] position units past it, so the
//!   sealer window still holds it when it goes out, or when it waited
//!   [`AdmissionConfig::park_ttl`], so the client gets an answer on a
//!   quiet lane. The time is the time of the last sweep.
//!
//! A sender with nothing forwarded and nothing parked is idle. Above
//! [`AdmissionConfig::max_senders`], a sweep forgets the idle senders: the
//! next transaction of one asks for a lookup.

mod sender;

use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::map::HashMap;
use alloy_primitives::{Address, U256};

use self::sender::SenderState;

/// The limits of the filter.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionConfig {
    /// The most transactions of one sender that wait for a nonce gap.
    pub max_parked: NonZeroUsize,
    /// The most forwarded transactions of one sender that no state confirmed.
    pub max_forwarded: NonZeroUsize,
    /// The blocks after which an unconfirmed forwarded transaction is lost.
    pub loss_blocks: NonZeroU64,
    /// The position units of an image after which a parked transaction expires.
    pub park_width: NonZeroU64,
    /// The time after which a parked transaction expires.
    pub park_ttl: Duration,
    /// The senders above which a sweep forgets the idle ones.
    pub max_senders: NonZeroUsize,
}

/// The chain state of one account.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountState {
    pub nonce: u64,
    pub balance: U256,
    /// The account has delegated code (EIP-7702).
    pub delegated: bool,
    /// The canonical position of the state. A higher position is newer.
    pub tx_idx: u64,
}

/// The place of a transaction in its ingress image. Racing replicas read
/// the same image, so they see the same values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ImagePos {
    pub lane: u8,
    pub session: i32,
    /// The stream position, in 32-byte units.
    pub unit: u64,
}

/// One transaction that the filter may forward. `payload` is what the
/// caller needs to publish it.
#[derive(Debug)]
pub struct Candidate<T> {
    pub sender: Address,
    pub nonce: u64,
    /// The most the transaction can be charged: gas limit times fee cap, plus value.
    pub cost: U256,
    pub image: ImagePos,
    pub payload: T,
}

/// Why the filter refused a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The chain is past the nonce.
    NonceTooLow { chain_nonce: u64 },
    /// The balance less the forwarded run does not cover the cost.
    InsufficientFunds { available: U256, cost: U256 },
    /// The sender has too many transactions that wait. `expected_nonce`
    /// is the next nonce of the sender, 0 while its state is unknown.
    TooManyPending { expected_nonce: u64 },
    /// The filter forwarded or holds a transaction at this nonce already.
    InFlight { expected_nonce: u64 },
    /// The image moved too far past the parked transaction.
    Expired { expected_nonce: u64 },
}

/// One outcome of the filter.
#[derive(Debug)]
pub enum Verdict<T> {
    /// Offer the transaction to the sealer.
    Forward(Candidate<T>),
    /// Tell the client why the filter did not forward it.
    Reject(Candidate<T>, Refusal),
    /// The filter holds no state of the sender: ask Redis, then the
    /// executor, and give the answer to [`Admission::on_lookup`].
    Lookup(Address),
}

/// A forwarded transaction, in forward order, for the loss expiry.
struct ForwardMark {
    block: u64,
    sender: Address,
    nonce: u64,
}

/// A parked transaction, in arrival order within its image, for the park expiry.
struct ParkMark {
    unit: u64,
    /// The time of the last sweep before it parked.
    at: Instant,
    sender: Address,
    nonce: u64,
}

/// The highest unit seen in one image, and its parked transactions.
#[derive(Default)]
struct ImageMarks {
    highest: u64,
    parked: VecDeque<ParkMark>,
}

/// The admission filter of one sequencer replica.
pub struct Admission<T> {
    cfg: AdmissionConfig,
    senders: HashMap<Address, SenderState<T>>,
    images: HashMap<(u8, i32), ImageMarks>,
    forwards: VecDeque<ForwardMark>,
    /// The newest block of rows.
    block: u64,
    /// The time of the last sweep.
    now: Instant,
}

impl<T> Admission<T> {
    #[must_use]
    pub fn new(cfg: AdmissionConfig, now: Instant) -> Self {
        Self {
            cfg,
            senders: HashMap::default(),
            images: HashMap::default(),
            forwards: VecDeque::new(),
            block: 0,
            now,
        }
    }

    /// The senders that the filter holds.
    #[must_use]
    pub fn senders(&self) -> usize {
        self.senders.len()
    }

    /// Decide one transaction. The verdicts go to `out`: the transaction's
    /// own, and those of the parked transactions of its sender that it frees.
    pub fn offer(&mut self, c: Candidate<T>, out: &mut Vec<Verdict<T>>) {
        let from = out.len();
        let key = (c.image.lane, c.image.session);
        let mark = ParkMark {
            unit: c.image.unit,
            at: self.now,
            sender: c.sender,
            nonce: c.nonce,
        };
        let marks = self.images.entry(key).or_default();
        marks.highest = marks.highest.max(mark.unit);
        let parked = self
            .senders
            .entry(c.sender)
            .or_default()
            .offer(c, &self.cfg, out);
        if parked {
            self.images.entry(key).or_default().parked.push_back(mark);
        }
        self.record_forwards(&out[from..]);
    }

    /// Apply the account rows of `block`. A row that is not newer than the
    /// state held changes nothing.
    pub fn on_rows(
        &mut self,
        block: u64,
        rows: impl IntoIterator<Item = (Address, AccountState)>,
        out: &mut Vec<Verdict<T>>,
    ) {
        self.block = self.block.max(block);
        for (address, state) in rows {
            self.on_lookup(address, state, out);
        }
    }

    /// Apply the state of one account: a row, or the answer of a lookup.
    pub fn on_lookup(&mut self, address: Address, state: AccountState, out: &mut Vec<Verdict<T>>) {
        let from = out.len();
        self.senders
            .entry(address)
            .or_default()
            .on_state(state, &self.cfg, out);
        self.record_forwards(&out[from..]);
    }

    /// The executor rolled back: every state may name void blocks. Each
    /// sender asks again on its next transaction. The forwarded runs and
    /// the parked transactions stay.
    pub fn on_rollback(&mut self) {
        self.senders
            .values_mut()
            .for_each(SenderState::forget_state);
    }

    /// Expire the forwarded runs that no state confirmed within the loss
    /// window, and the parked transactions that their image left behind or
    /// that waited longer than the time to live at `now`. Forget the idle
    /// senders above the sender bound.
    pub fn sweep(&mut self, now: Instant, out: &mut Vec<Verdict<T>>) {
        self.now = now;
        let window = self.cfg.loss_blocks.get();
        while self
            .forwards
            .front()
            .is_some_and(|f| f.block.saturating_add(window) <= self.block)
        {
            self.expire_forward(out);
        }
        let limit = ParkLimit {
            width: self.cfg.park_width.get(),
            oldest: now.checked_sub(self.cfg.park_ttl),
        };
        for marks in self.images.values_mut() {
            marks.expire(limit, &mut self.senders, out);
        }
        if self.senders.len() > self.cfg.max_senders.get() {
            self.senders.retain(|_, s| !s.is_idle());
        }
    }

    fn expire_forward(&mut self, out: &mut Vec<Verdict<T>>) {
        let from = out.len();
        let lost = self.forwards.pop_front();
        let freed = lost.is_some_and(|mark| {
            self.senders
                .get_mut(&mark.sender)
                .is_some_and(|s| s.on_lost(mark.nonce, &self.cfg, out))
        });
        if freed {
            self.record_forwards(&out[from..]);
        }
    }

    /// Mark each forward of `verdicts` for the loss expiry.
    fn record_forwards(&mut self, verdicts: &[Verdict<T>]) {
        let block = self.block;
        self.forwards
            .extend(verdicts.iter().filter_map(|v| match v {
                Verdict::Forward(c) => Some(ForwardMark {
                    block,
                    sender: c.sender,
                    nonce: c.nonce,
                }),
                Verdict::Reject(..) | Verdict::Lookup(_) => None,
            }));
    }
}

/// When a parked transaction expires: `width` units behind the highest
/// unit of its image, or parked at or before `oldest`.
#[derive(Clone, Copy)]
struct ParkLimit {
    width: u64,
    oldest: Option<Instant>,
}

impl ParkLimit {
    fn passed(self, mark: &ParkMark, highest: u64) -> bool {
        highest - mark.unit >= self.width || self.oldest.is_some_and(|oldest| mark.at <= oldest)
    }
}

impl ImageMarks {
    /// Expire the parked transactions of this image past `limit`. The mark
    /// of a transaction that left the park drops with no verdict.
    fn expire<T>(
        &mut self,
        limit: ParkLimit,
        senders: &mut HashMap<Address, SenderState<T>>,
        out: &mut Vec<Verdict<T>>,
    ) {
        while self
            .parked
            .front()
            .is_some_and(|p| limit.passed(p, self.highest))
        {
            self.expire_front(senders, out);
        }
    }

    fn expire_front<T>(
        &mut self,
        senders: &mut HashMap<Address, SenderState<T>>,
        out: &mut Vec<Verdict<T>>,
    ) {
        let expired = self.parked.pop_front().and_then(|mark| {
            senders.get_mut(&mark.sender).and_then(|s| {
                s.unpark(mark.nonce, mark.unit)
                    .map(|c| (c, s.expected_nonce()))
            })
        });
        out.extend(
            expired
                .map(|(c, expected_nonce)| Verdict::Reject(c, Refusal::Expired { expected_nonce })),
        );
    }
}

#[cfg(test)]
mod tests;

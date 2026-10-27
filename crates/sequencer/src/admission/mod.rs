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
//!   yet, with their summed worst-case cost. The filter keeps each of them
//!   until a state confirms it or the run is lost, so the caller can offer
//!   it again when the sealer can have lost the offer;
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
//! - A forwarded transaction that the sealer refused is lost at once, by
//!   the same rule.
//! - A parked transaction expires when its ingress image moves
//!   [`AdmissionConfig::park_width`] position units past it, so the
//!   sealer window still holds it when it goes out, or when it waited
//!   [`AdmissionConfig::park_ttl`], so the client gets an answer on a
//!   quiet lane. The time is the time of the last sweep.
//!
//! A state from a lookup can be behind the chain: the state database of an
//! executor commits after the receipts go out, and a replica that starts
//! late misses the rows before its start. While a parked transaction waits
//! for a gap over such a state, the sender asks again after
//! [`AdmissionConfig::relook_after`], until the gap closes or the
//! transaction expires. A gap over rows is a missing transaction, and the
//! filter does not ask again.
//!
//! A sender with nothing forwarded and nothing parked is idle. Above
//! [`AdmissionConfig::max_senders`], a sweep forgets the idle senders: the
//! next transaction of one asks for a lookup.

mod refusals;
mod sender;

use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::map::HashMap;
use alloy_primitives::{Address, B256, U256};

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
    /// The time after which a sender whose state came from a lookup asks
    /// again, while a parked transaction waits for a nonce gap over it.
    pub relook_after: Duration,
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

impl AccountState {
    /// Whether this state replaces `held`: a higher position, or position 0
    /// with a higher nonce. Position 0 names both the empty chain and the
    /// state after its first transaction.
    #[must_use]
    pub fn is_newer_than(&self, held: &Self) -> bool {
        self.tx_idx > held.tx_idx
            || (self.tx_idx == 0 && held.tx_idx == 0 && self.nonce > held.nonce)
    }
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
#[derive(Clone, Debug)]
pub struct Candidate<T> {
    pub sender: Address,
    pub nonce: u64,
    pub tx_hash: B256,
    /// The most the transaction can be charged: gas limit times fee cap, plus value.
    pub cost: U256,
    pub image: ImagePos,
    /// The id of the ingress that published the transaction: the top 16
    /// bits of its correlation id.
    pub ingress: u16,
    /// The client has its answer: the filter took the transaction into its
    /// mempool before, with an [`Verdict::Accept`]. A later verdict of the
    /// transaction is not an answer to the client. The caller sets it to
    /// `false`.
    pub answered: bool,
    pub payload: T,
}

/// The ack of one transaction that the filter took into its mempool, for
/// the ingress that published it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ack {
    pub ingress: u16,
    pub sender: Address,
    pub nonce: u64,
    pub tx_hash: B256,
}

impl<T> Candidate<T> {
    /// Mark the candidate answered, and return its ack. A candidate that
    /// has its answer gives none.
    pub fn answer(&mut self) -> Option<Ack> {
        (!std::mem::replace(&mut self.answered, true)).then_some(Ack {
            ingress: self.ingress,
            sender: self.sender,
            nonce: self.nonce,
            tx_hash: self.tx_hash,
        })
    }
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
    /// A terminal refusal identifies a copy that the sealer cannot order.
    SealerRefused(Candidate<T>, kardamom_types::TxErrorReason),
    /// The filter holds no state of the sender: ask Redis, then the
    /// executor, and give the answer to [`Admission::on_lookup`].
    Lookup(Address),
    /// The filter holds the transaction for a nonce gap or a busy run: it is
    /// in the mempool, so tell the client.
    Accept(Ack),
}

/// A forwarded transaction, in forward order, for the loss expiry. `block`
/// is the block of the first rows after the forward, so the loss window
/// never starts before the forward on a quiet chain.
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

/// A sender that asks for a lookup again at `due`, if its gap is still there.
struct Relook {
    due: Instant,
    sender: Address,
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
    /// The marks of `forwards` that have their block: the marks from this
    /// count on wait for the next rows.
    stamped: usize,
    /// The relooks in the order of their due time.
    relooks: VecDeque<Relook>,
    /// The newest block of rows.
    block: u64,
    /// The time of the last sweep.
    now: Instant,
}

impl<T: Clone> Admission<T> {
    #[must_use]
    pub fn new(cfg: AdmissionConfig, now: Instant) -> Self {
        Self {
            cfg,
            senders: HashMap::default(),
            images: HashMap::default(),
            forwards: VecDeque::new(),
            stamped: 0,
            relooks: VecDeque::new(),
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
        let sender = c.sender;
        let parked = self
            .senders
            .entry(sender)
            .or_default()
            .offer(c, &self.cfg, out);
        if parked {
            self.images.entry(key).or_default().parked.push_back(mark);
            self.queue_relook(sender);
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
            self.apply_state(address, state, false, out);
        }
        self.forwards
            .range_mut(self.stamped..)
            .for_each(|mark| mark.block = block);
        self.stamped = self.forwards.len();
    }

    /// Apply the state of one account: a row, or the answer of a lookup.
    pub fn on_lookup(&mut self, address: Address, state: AccountState, out: &mut Vec<Verdict<T>>) {
        self.apply_state(address, state, true, out);
        self.queue_relook(address);
    }

    fn apply_state(
        &mut self,
        address: Address,
        state: AccountState,
        from_lookup: bool,
        out: &mut Vec<Verdict<T>>,
    ) {
        let from = out.len();
        self.senders
            .entry(address)
            .or_default()
            .on_state(state, from_lookup, &self.cfg, out);
        self.record_forwards(&out[from..]);
    }

    /// Queue a relook of `sender` when its parked transaction waits for a
    /// gap over a state from a lookup.
    fn queue_relook(&mut self, sender: Address) {
        let queue = self
            .senders
            .get_mut(&sender)
            .is_some_and(|s| s.waits_on_lookup_gap() && s.queue_relook());
        if queue {
            // An `Instant` that cannot hold the sum is due at once.
            let due = self
                .now
                .checked_add(self.cfg.relook_after)
                .unwrap_or(self.now);
            self.relooks.push_back(Relook { due, sender });
        }
    }

    /// Ask again for each sender whose relook is due at `now`.
    fn relook_due(&mut self, now: Instant, out: &mut Vec<Verdict<T>>) {
        while self.relooks.front().is_some_and(|r| r.due <= now) {
            self.relook_front(out);
        }
    }

    fn relook_front(&mut self, out: &mut Vec<Verdict<T>>) {
        let asked = self.relooks.pop_front().and_then(|r| {
            let ask = self.senders.get_mut(&r.sender)?.relook();
            ask.then_some(r.sender)
        });
        out.extend(asked.map(Verdict::Lookup));
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
    /// window while publication is active. Expire parked transactions past
    /// their image window or time to live. Forget idle senders above the
    /// sender bound.
    pub fn sweep(&mut self, now: Instant, out: &mut Vec<Verdict<T>>) {
        self.sweep_mode::<false>(now, out);
    }

    /// Keep forwarded runs while an upstream pause prevents publication.
    /// Parked expiry and account lookups still run.
    pub(crate) fn sweep_while_paused(&mut self, now: Instant, out: &mut Vec<Verdict<T>>) {
        self.sweep_mode::<true>(now, out);
    }

    fn sweep_mode<const HOLD: bool>(&mut self, now: Instant, out: &mut Vec<Verdict<T>>) {
        self.now = now;
        let window = self.cfg.loss_blocks.get();
        while !HOLD
            && self.stamped > 0
            && self
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
        self.relook_due(now, out);
        if self.senders.len() > self.cfg.max_senders.get() {
            self.senders.retain(|_, s| !s.is_idle());
        }
    }

    /// Empty blocks during an upstream pause do not prove offer loss.
    /// On resume the retained runs get a full loss window for recovery;
    /// account rows can still confirm them throughout the pause.
    pub(crate) fn resume_loss_window(&mut self) {
        self.forwards
            .range_mut(..self.stamped)
            .for_each(|mark| mark.block = self.block);
    }

    fn expire_forward(&mut self, out: &mut Vec<Verdict<T>>) {
        let lost = self.forwards.pop_front();
        // The sweep calls this only while a stamped mark is at the front.
        self.stamped -= 1;
        if let Some(mark) = lost {
            self.lose(mark.sender, mark.nonce, out);
        }
    }

    /// The sealer refused the forwarded transaction of `sender` at `nonce`:
    /// forget the run, as for a lost transaction, so a resend at the nonce
    /// goes out again.
    pub fn on_refused(&mut self, sender: Address, nonce: u64, out: &mut Vec<Verdict<T>>) {
        self.lose(sender, nonce, out);
    }

    /// The forwarded transaction of `sender` at `nonce` that no state
    /// confirmed.
    #[must_use]
    pub fn unconfirmed_at(&self, sender: Address, nonce: u64) -> Option<&Candidate<T>> {
        self.senders.get(&sender)?.forwarded_at(nonce)
    }

    /// Every forwarded transaction that no state confirmed: the run of each
    /// sender, in nonce order.
    pub fn unconfirmed(&self) -> impl Iterator<Item = &Candidate<T>> {
        self.senders.values().flat_map(SenderState::unconfirmed)
    }

    /// The forwarded transaction of `sender` at `nonce` is gone. When the
    /// run still holds it, the run ends, and the parked transactions that
    /// go out from the chain nonce get their marks.
    fn lose(&mut self, sender: Address, nonce: u64, out: &mut Vec<Verdict<T>>) {
        let from = out.len();
        let freed = self
            .senders
            .get_mut(&sender)
            .is_some_and(|s| s.on_lost(nonce, &self.cfg, out));
        if freed {
            self.record_forwards(&out[from..]);
        }
    }

    /// Mark each forward of `verdicts` for the loss expiry.
    /// The next rows stamp the marks with their block.
    fn record_forwards(&mut self, verdicts: &[Verdict<T>]) {
        self.forwards
            .extend(verdicts.iter().filter_map(|v| match v {
                Verdict::Forward(c) => Some(ForwardMark {
                    block: 0,
                    sender: c.sender,
                    nonce: c.nonce,
                }),
                Verdict::Reject(..)
                | Verdict::SealerRefused(..)
                | Verdict::Lookup(_)
                | Verdict::Accept(_) => None,
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
    fn expire<T: Clone>(
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

    fn expire_front<T: Clone>(
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

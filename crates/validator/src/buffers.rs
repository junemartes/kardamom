//! Verification buffers: the replica buffers for the BAL (by block
//! number), receipts and account rows (by canonical `tx_idx`), and the
//! per-block claim index buffer. Each one is a
//! [`KeyedBuffer`](kardamom_engine::keyed_buffer::KeyedBuffer) with its
//! own slot type and bounds.
//!
//! A replica buffer keeps every distinct result of each key, with the
//! sessions that published it. A second result for a key never replaces
//! the first, so the consumer compares the result of every replica. A
//! session that repeats a result it already published adds nothing, so a
//! replica that restarts in a loop takes no extra space.
//!
//! The binary's Aeron subscriber tasks fill the buffers. The sync exec and
//! commit threads drain them, and wait briefly for the matching data to
//! arrive.

use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

pub use kardamom_engine::keyed_buffer::BufKey;
use kardamom_engine::keyed_buffer::{Bounds, KeyedBuffer, Skip, Slot};
use kardamom_types::{AccountRow, BPosition, BlockDelta, Receipt};

use crate::metrics;
use crate::replica::{Check, Distinct, ReplicaId, Taken};

/// The latest insert for a key replaces the earlier one. The claim
/// index is a scheduling hint, not a checked result, so one copy is
/// enough.
struct Latest<V>(V);

impl<V> Slot for Latest<V> {
    type Item = V;
    fn first(item: V) -> Self {
        Self(item)
    }
    fn join(&mut self, item: V) -> Result<(), Skip> {
        self.0 = item;
        Ok(())
    }
    fn results(&self) -> usize {
        1
    }
}

/// Upper bound on the distinct results of one key. Correct replicas
/// publish one value, so a second value already shows a divergence.
const MAX_DISTINCT: usize = 8;
/// Upper bound on the sessions named for one distinct result. It covers
/// the replicas, plus the new sessions of replicas that restart inside
/// the window.
const MAX_SESSIONS: usize = 32;

/// The distinct results of one key, in arrival order.
struct Results<V>(Vec<Distinct<V>>);

impl<V: PartialEq> Slot for Results<V> {
    type Item = (ReplicaId, V);
    fn first((replica, value): (ReplicaId, V)) -> Self {
        Self(vec![Distinct {
            value,
            replicas: vec![replica],
        }])
    }
    fn join(&mut self, (replica, value): (ReplicaId, V)) -> Result<(), Skip> {
        let full = self.0.len() >= MAX_DISTINCT;
        match self.0.iter_mut().find(|d| d.value == value) {
            Some(d) if d.replicas.contains(&replica) => Err(Skip::Repeat),
            Some(d) if d.replicas.len() >= MAX_SESSIONS => Err(Skip::Bound),
            Some(d) => {
                d.replicas.push(replica);
                Ok(())
            }
            None if full => Err(Skip::Bound),
            None => {
                self.0.push(Distinct {
                    value,
                    replicas: vec![replica],
                });
                Ok(())
            }
        }
    }
    fn results(&self) -> usize {
        Distinct::results(&self.0)
    }
}

/// Buffer of one kind of executor result: every distinct result of each
/// key, with the sessions that published it. The binary's subscriber
/// task calls [`insert`](Self::insert); the sync consumer thread calls
/// [`take`](Self::take). Every result the buffer does not hand over
/// counts in `validator_replica_results_unchecked_total`.
pub struct ReplicaBuffer<K, V> {
    core: KeyedBuffer<K, Results<V>>,
    check: Check,
}

impl<K: BufKey, V: PartialEq> ReplicaBuffer<K, V> {
    fn with(check: Check, bounds: Bounds) -> Self {
        Self {
            core: KeyedBuffer::new(bounds),
            check,
        }
    }

    /// Insert the result `value` that `replica` published for `key`.
    pub fn insert(&self, key: K, replica: ReplicaId, value: V) {
        let inserted = self.core.insert(key, (replica, value));
        if let Some(skip) = inserted.refused {
            metrics::counter_replica_unchecked(self.check, skip, 1);
        }
        metrics::counter_replica_unchecked(self.check, Skip::Evicted, inserted.evicted);
    }

    /// Take the results for `key`, and wait up to `timeout` for the first
    /// to arrive. Also hands over the results of lower keys that arrived
    /// after their take. An empty `current` means "could not verify", not
    /// a divergence. See [`KeyedBuffer::take`] for the deadline rule and
    /// [`KeyedBuffer::take_step`] for the catch-up skip.
    pub fn take(&self, key: K, timeout: Duration) -> Taken<K, V> {
        let took = self.core.take(key, timeout);
        metrics::counter_replica_unchecked(self.check, Skip::Ahead, took.beyond);
        Taken {
            current: took.current.map(|r| r.0).unwrap_or_default(),
            late: took.below.into_iter().map(|(k, r)| (k, r.0)).collect(),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.core.len()
    }
}

/// Buffer of executor-published BALs, keyed by block number. The Aeron
/// `tx_bal` subscriber task inserts; the sync exec thread takes, and
/// waits briefly for the matching block.
pub type BalBuffer = ReplicaBuffer<u64, BlockDelta>;

impl BalBuffer {
    /// How far below the live head a requested block must be to count
    /// as unrecoverable backlog. Its BAL has aged out of the live
    /// `tx_bal` multicast buffer and will never arrive, so the validator
    /// commits it unverified at once instead of waiting. A caught-up
    /// validator asks for blocks near the head (a smaller lag than this
    /// value), so it always waits and verifies. Only a validator catching
    /// up from a cold start, or after a lapse longer than the multicast
    /// buffer, skips the wait.
    pub(crate) const BACKLOG_LOOKBEHIND: u64 = 16;
    /// Bound on buffered blocks. Each block holds at most `MAX_DISTINCT`
    /// whole `BlockDelta` values, the heavyweight case. This is about 17
    /// to 35 minutes of chain at a 250ms-to-2s block rate, well beyond the
    /// verify window, so eviction fires only if the consumer stalls
    /// outright.
    pub(crate) const MAX_BUFFERED: NonZeroUsize =
        NonZeroUsize::new(1024).expect("compile-time constant");
    /// How many blocks below the checked block a replica's BAL is still
    /// compared: a replica that lags the validator by up to this many
    /// blocks is checked. The validator keeps one checked write-set for
    /// each block of the window.
    pub(crate) const CHECK_WINDOW: u64 = 64;
    /// How many blocks above the checked block a BAL is still taken:
    /// about 3 days of chain at 250ms blocks. A validator further behind
    /// than this rebuilds from a peer checkpoint, not by catch-up.
    pub(crate) const REACH: u64 = 1 << 20;
    const BOUNDS: Bounds = Bounds {
        cap: Self::MAX_BUFFERED,
        lookbehind: Self::BACKLOG_LOOKBEHIND,
        late_window: Self::CHECK_WINDOW,
        reach: Self::REACH,
    };

    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::with(Check::Bal, Self::BOUNDS))
    }

    #[cfg(test)]
    fn with_cap(cap: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self::with(
            Check::Bal,
            Bounds {
                cap,
                ..Self::BOUNDS
            },
        ))
    }
}

/// Buffer of executor-published receipts and account rows, both keyed by
/// canonical `tx_idx`. The `tx_receipts` subscriber task fills it; the
/// commit thread drains it.
pub struct ReceiptBuffer {
    pub receipts: ReplicaBuffer<BPosition, Receipt>,
    /// Published account rows, keyed by their batch's end position. Only a
    /// batch end carries rows, so most positions have no entry. The rows
    /// travel in the frame that carried the end receipt, so they are
    /// present whenever that receipt is, and the commit thread takes them
    /// with no wait. Rows have no late window: rows that arrive after the
    /// commit thread passed their position meet a later local state.
    pub rows: ReplicaBuffer<BPosition, Vec<AccountRow>>,
}

impl ReceiptBuffer {
    /// This mirrors [`BalBuffer::BACKLOG_LOOKBEHIND`], but in canonical
    /// records rather than blocks. When the buffered head is this far
    /// ahead of the requested `tx_idx`, the executor's receipt for the
    /// requested tx has aged out of the live `tx_receipts` stream and will
    /// never arrive. The buffer skips it at once, marked unverified,
    /// instead of blocking the commit thread for the full wait per
    /// historical tx. 4096 records is about the same reach as the BAL
    /// heuristic's 16 blocks, at a few hundred tx per block. A caught-up
    /// validator's requests trail the head by less than this, so it always
    /// waits and verifies.
    const BACKLOG_LOOKBEHIND: u64 = 4096;
    /// Bound on buffered positions. Each position holds at most
    /// `MAX_DISTINCT` receipts. Receipts are small structs; this cap is
    /// only a leak guard.
    const MAX_BUFFERED: NonZeroUsize = NonZeroUsize::new(1 << 16).expect("compile-time constant");
    /// How many canonical records below the checked one a replica's
    /// receipt is still compared. The validator keeps one checked receipt
    /// for each record of the window.
    pub(crate) const CHECK_WINDOW: u64 = 4096;
    /// How many canonical records above the checked one a receipt is still
    /// taken: [`BalBuffer::REACH`] blocks of 4096 records.
    const REACH: u64 = 1 << 32;
    const BOUNDS: Bounds = Bounds {
        cap: Self::MAX_BUFFERED,
        lookbehind: Self::BACKLOG_LOOKBEHIND,
        late_window: Self::CHECK_WINDOW,
        reach: Self::REACH,
    };

    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            receipts: ReplicaBuffer::with(Check::Receipt, Self::BOUNDS),
            rows: ReplicaBuffer::with(
                Check::Rows,
                Bounds {
                    late_window: 0,
                    ..Self::BOUNDS
                },
            ),
        })
    }
}

/// Per-block EIP-7928 claim index for parallel validation. This is separate
/// from [`BalBuffer`], so the merged-delta check path stays untouched. A
/// missing claim index falls back to sequential re-execution, never to a
/// gap in verification.
pub struct ClaimBuffer {
    core: KeyedBuffer<u64, Latest<(NonZeroU16, Arc<crate::parallel::ClaimIndex>)>>,
}

impl Default for ClaimBuffer {
    fn default() -> Self {
        Self {
            core: KeyedBuffer::new(Bounds {
                late_window: 0,
                ..BalBuffer::BOUNDS
            }),
        }
    }
}

impl ClaimBuffer {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Insert a block's claims with the granularity the frame declared. The
    /// validator's view of the ladder must come from the wire, from what
    /// the executor actually produced, never from local config. Zero is
    /// never a legal granularity; the caller parses it once, at the wire
    /// boundary (`bin/kardamom-validator/pumps.rs`, `BalPump::index_claims`), so it
    /// is already a `NonZeroU16` by the time it reaches here.
    pub fn insert(&self, block: u64, granularity: NonZeroU16, claims: crate::parallel::ClaimIndex) {
        self.core.insert(block, (granularity, Arc::new(claims)));
    }

    /// [`insert`](Self::insert) for an already-shared index — the BAL pump
    /// decodes each frame once and feeds both the parallel engine's buffer
    /// and the outbox extractor's without cloning the index.
    pub fn insert_arc(
        &self,
        block: u64,
        granularity: NonZeroU16,
        claims: Arc<crate::parallel::ClaimIndex>,
    ) {
        self.core.insert(block, (granularity, claims));
    }

    /// Take `block`'s claims, and wait up to `timeout`. On `None`, the
    /// caller re-executes this block sequentially.
    pub fn take(
        &self,
        block: u64,
        timeout: Duration,
    ) -> Option<(NonZeroU16, Arc<crate::parallel::ClaimIndex>)> {
        self.core
            .take(block, timeout)
            .current
            .map(|latest| latest.0)
    }
}

#[cfg(test)]
#[path = "buffers_tests.rs"]
mod tests;

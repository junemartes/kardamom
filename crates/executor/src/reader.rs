//! TxData / tx_ordering reader threads + join buffer (S4-arch-update,
//!,).
//!
//! Before the executor had **one** tx_ordering reader thread that
//! pulled full [`TxEnvelope`]s off tx_ordering and handed them downstream. After
//! the split-architecture refactor tx_ordering carries only
//! ~16-32 B [`TxOrderingMessage`] records (`TxRef | BoundaryStart`); the full
//! envelope bytes live on M per-sequencer **tx_data** archives.
//!
//! This module owns the M+1 reader thread topology:
//!
//! ```text
//!   ┌────────────────┐
//!   │ tx_data[0]   │──┐                                 ┌────────────┐
//!   │ reader thread  │  │   DashMap<(sid,tx_data_position),     │ exec thread│
//!   ├────────────────┤  │     TxEnvelope> "join buffer"   │ (revm)     │
//!   │ tx_data[1]   │──┤◄────insert────────────────────► │            │
//!   │ reader thread  │  │              ▲                  └────────────┘
//!   ├────────────────┤  │              │ lookup+remove          ▲
//!   │      …         │  │              │                        │ (BPosition,
//!   ├────────────────┤  │       ┌──────┴──────────┐             │  TxEnvelope)
//!   │ tx_data[M-1] │──┘       │ tx_ordering reader│─────────────┘
//!   └────────────────┘          │     thread      │ (also forwards
//!                               └─────────────────┘  BlockBoundaryStart
//!                                                    inline)
//! ```
//!
//! Each tx_data reader thread is dedicated to one Aeron subscription
//! (tx_data[i]). `rusteron_client::Aeron` is `!Send + !Sync`, so each
//! subscription must own its own Aeron client on its own OS thread. The
//! reader simply inserts every fragment into the shared [`JoinBuffer`].
//!
//! The single tx_ordering reader pulls [`TxOrderingMessage`] records in canonical
//! order (system invariant I1). For each:
//!
//! - `TxRef`: look up `(sequencer_id, tx_data_position)` in the join buffer; if
//!   present, hand `(b_position, TxEnvelope)` to the exec thread. If absent
//!   (A-publisher lag of a few µs), spin with a bounded backoff up to
//!   [`ReaderConfig::join_timeout`]; beyond that, return [`ExecutorError::
//!   JoinTimeout`] — something is wrong upstream.
//! - `BoundaryStart`: forward verbatim.
//!
//! `BPosition` handed to exec is the **tx_ordering** position (the canonical L2
//! position), not the tx_data position. Downstream consumers continue to
//! key on this.

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use crossbeam_channel::Sender;
use dashmap::DashMap;
use tracing::{debug, warn};

use kardamom_types::{
    BPosition, BlockBoundaryStart, Deposit, TxDataLoc, TxEnvelope, TxOrderingMessage,
};

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

pub mod cluster;

/// Subscription to one **tx_data[i]**.
///
/// One impl per sequencer partition. Implementations:
/// - in production: `kardamom_log::TxDataSubscriber` on a dedicated OS thread.
/// - in tests: `kardamom_log::testing::FakeTxDataSubscription`.
///
/// The contract: `next` blocks until the next `(tx_data_position, envelope)` is
/// available; returns `Err(ExecutorError::TxDataClosed { sequencer_id })`
/// when the underlying subscription closes cleanly.
pub trait TxDataSubscription: Send {
    /// Sequencer id this subscription is bound to. Used to key the join
    /// buffer and surface diagnostics.
    fn sequencer_id(&self) -> u8;

    fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError>;
}

/// Subscription to **tx_ordering** (the canonical orderer).
///
/// Yields tiny [`TxOrderingMessage`] records (`TxRef | DepositRef |
/// BoundaryStart`) each tagged with its canonical `BPosition`. The
/// `BPosition` is the system's canonical L2 tx ordering (invariant I1).
///
/// In production: `kardamom_log::TxOrderingSubscriber` on a dedicated OS thread.
/// In tests: see `kardamom_log::testing::FakeTxOrderingSubscription`.
pub trait TxOrderingSubscription: Send {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError>;
}

/// Subscription to **tx_deposits** (the DA-watcher → sequencer/executor
/// channel). Yields full [`Deposit`] envelopes the DA watcher published.
///
/// The deposit reader thread iterates this and inserts every
/// `(deposit_position, Deposit)` into the [`DepositJoinBuffer`] keyed by
/// `source_hash`. The tx_ordering reader then dedups `DepositRef`s by
/// `source_hash` and removes the buffered deposit.
pub trait DepositSubscription: Send {
    fn next(&mut self) -> Result<(BPosition, Deposit), ExecutorError>;
}

/// Lookup-and-remove join buffer keyed by
/// `(sequencer_id, session_id, tx_data_position)`.
///
/// TxData reader threads insert via [`JoinBuffer::insert`]. The
/// tx_ordering reader pulls via [`JoinBuffer::take`] (remove-on-hit). Bounded
/// by the in-flight window — typically a few thousand entries (~100 MB at
/// envelope-sized values).
///
/// The `session_id` (Aeron publisher session) is part of the key because Aeron
/// positions are per-session: under active/active ingress two publishers on one
/// shard can emit fragments with the same `(term_id, term_offset)`, so
/// `(sequencer_id, tx_data_position)` alone is ambiguous. The sequencer stamps
/// the session into `TxRef.tx_data_session_id`; the lookup uses it.
///
/// Shared across M+1 threads via `Arc`. `DashMap` over per-shard locks
/// since the access pattern is M concurrent inserts + one concurrent reader;
/// we never iterate.
#[derive(Clone, Default)]
pub struct JoinBuffer {
    inner: Arc<DashMap<(u8, i32, BPosition), TxEnvelope>>,
}

impl JoinBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &self,
        sequencer_id: u8,
        session_id: i32,
        tx_data_position: BPosition,
        env: TxEnvelope,
    ) {
        self.inner
            .insert((sequencer_id, session_id, tx_data_position), env);
    }

    /// Remove and return the envelope at
    /// `(sequencer_id, session_id, tx_data_position)`, or `None` if it isn't
    /// (yet) present.
    pub fn take(
        &self,
        sequencer_id: u8,
        session_id: i32,
        tx_data_position: BPosition,
    ) -> Option<TxEnvelope> {
        self.inner
            .remove(&(sequencer_id, session_id, tx_data_position))
            .map(|kv| kv.1)
    }

    /// Current entry count. Exposed for tests and the periodic
    /// growth-monitor warning emitted by the tx_ordering reader.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Lookup-and-remove join buffer keyed by `source_hash`.
///
/// The deposit reader thread inserts via [`DepositJoinBuffer::insert`]
/// every `(deposit_position, Deposit)` it pulls off tx_deposits. The
/// tx_ordering reader pulls via [`DepositJoinBuffer::take`] (remove-on-hit)
/// when it sees a `DepositRef`. Bounded by the in-flight L1 deposit
/// window — typically tiny (~tens of entries) since L1 finalised
/// deposits arrive at L1-block cadence.
#[derive(Clone, Default)]
pub struct DepositJoinBuffer {
    inner: Arc<DashMap<B256, (BPosition, Deposit)>>,
}

impl DepositJoinBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, deposit_position: BPosition, deposit: Deposit) {
        self.inner
            .insert(deposit.source_hash, (deposit_position, deposit));
    }

    /// Remove and return the deposit indexed by `source_hash`, or `None`
    /// if it isn't (yet) present.
    pub fn take(&self, source_hash: B256) -> Option<(BPosition, Deposit)> {
        self.inner.remove(&source_hash).map(|kv| kv.1)
    }
}

/// Tunables for the reader / join layer.
#[derive(Clone, Debug)]
pub struct ReaderConfig {
    /// Upper bound on how long the tx_ordering reader will wait for a
    /// `TxRef`'s envelope to land on its tx_data. 100 ms matches the
    /// "few µs of A-publisher lag is fine, anything more is upstream
    /// failure" comment in the plan.
    pub join_timeout: Duration,
    /// Polling interval used during the join wait. Trade-off: smaller =
    /// faster recovery from lag, more CPU; larger = vice versa. 50 µs is
    /// well below the 100 ms ceiling.
    pub join_poll_interval: Duration,
    /// Soft warn threshold on the join buffer's size; emits a `warn!` log
    /// when crossed (no back-pressure — that's the publisher's job).
    pub buffer_warn_threshold: usize,
    /// Capacity of the canonical-id dedup window on the tx_ordering reader.
    /// Duplicates of one canonical id are the P racing sequencers'
    /// republications, which land within the sequencers' publish spread of
    /// each other — the window only has to outlive that spread, not the
    /// whole stream. 2^20 ids (~32 MiB of hashes) gives ~10 s of headroom
    /// even at 100k tx/s.
    pub dedup_window: usize,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            join_timeout: Duration::from_millis(100),
            join_poll_interval: Duration::from_micros(50),
            buffer_warn_threshold: 10_000,
            dedup_window: 1 << 20,
        }
    }
}

/// Message routed from the tx_ordering reader to the executor's exec thread.
///
/// `tx_idx` is the **executor-local** monotone counter assigned in
/// canonical (B-position) arrival order; `position` is the tx_ordering
/// `BPosition` (the wire-level canonical id). The exec thread uses both:
/// `tx_idx` as a sanity-check newtype, `position` as the
/// downstream-published `Receipt.tx_idx`.
#[derive(Debug)]
pub enum ReaderToExec {
    Tx {
        tx_idx: TxIndex,
        envelope: TxEnvelope,
        position: BPosition,
    },
    Deposit {
        tx_idx: TxIndex,
        deposit: Deposit,
        position: BPosition,
    },
    Boundary(BlockBoundaryStart),
}

/// Spawn one tx_data reader thread for `a_sub`. Inserts every
/// `(TxDataLoc, envelope)` into `buffer` keyed by
/// `(a_sub.sequencer_id(), loc.session_id, loc.position)`. Returns when the
/// subscription closes cleanly (`Ok(())`) or propagates the first error.
pub fn spawn_tx_data_reader<A>(
    mut a_sub: A,
    buffer: JoinBuffer,
) -> JoinHandle<Result<(), ExecutorError>>
where
    A: TxDataSubscription + 'static,
{
    let sid = a_sub.sequencer_id();
    thread::Builder::new()
        .name(format!("executor-reader-a{sid}"))
        .spawn(move || {
            loop {
                match a_sub.next() {
                    Ok((loc, env)) => buffer.insert(sid, loc.session_id, loc.position, env),
                    Err(ExecutorError::TxDataClosed { .. }) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        })
        .expect("spawn tx_data reader")
}

/// Spawn the deposit reader thread. Pulls `(deposit_position, Deposit)`
/// records off tx_deposits and inserts them into `buffer` keyed by
/// `deposit.source_hash`. Returns `Ok(())` when the subscription closes
/// cleanly (mirrors [`spawn_tx_data_reader`]).
pub fn spawn_tx_deposits_reader<D>(
    mut d_sub: D,
    buffer: DepositJoinBuffer,
) -> JoinHandle<Result<(), ExecutorError>>
where
    D: DepositSubscription + 'static,
{
    thread::Builder::new()
        .name("executor-reader-deposits".into())
        .spawn(move || {
            loop {
                match d_sub.next() {
                    Ok((deposit_position, deposit)) => buffer.insert(deposit_position, deposit),
                    Err(ExecutorError::DepositsClosed) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        })
        .expect("spawn tx_deposits reader")
}

/// Bounded first-seen window for canonical-id dedup, FIFO-evicted.
///
/// `first_seen` returns `false` for an id already in the window. Once more
/// than `capacity` ids are held, the oldest is evicted — safe because the
/// duplicates of one canonical id (the racing sequencers' republications)
/// arrive within the publish spread of each other, far inside the window.
struct DedupWindow {
    seen: std::collections::HashSet<alloy_primitives::B256>,
    fifo: std::collections::VecDeque<alloy_primitives::B256>,
    capacity: usize,
}

impl DedupWindow {
    fn new(capacity: usize) -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            fifo: std::collections::VecDeque::new(),
            capacity,
        }
    }

    /// Records `id`; returns `false` if it is already in the window.
    fn first_seen(&mut self, id: alloy_primitives::B256) -> bool {
        if !self.seen.insert(id) {
            return false;
        }
        self.fifo.push_back(id);
        if self.fifo.len() > self.capacity
            && let Some(evicted) = self.fifo.pop_front()
        {
            self.seen.remove(&evicted);
        }
        true
    }
}

/// Spawn the single tx_ordering reader thread. Pulls
/// [`TxOrderingMessage`] records in canonical order; for each
/// `TxRef`, joins against `buffer` (with a bounded wait) and forwards
/// `(position, envelope)` to `exec_out`. For each `BoundaryStart`, forwards
/// directly.
pub fn spawn_tx_ordering_reader<B>(
    mut b_sub: B,
    buffer: JoinBuffer,
    deposit_buffer: DepositJoinBuffer,
    cfg: ReaderConfig,
    exec_out: Sender<ReaderToExec>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    B: TxOrderingSubscription + 'static,
{
    thread::Builder::new()
        .name("executor-reader-b".into())
        .spawn(move || {
            let mut next_tx_idx = TxIndex::ZERO;
            let mut last_warn_len: usize = 0;
            // Canonical-id dedup. Under the MDS topology the P sequencers per
            // shard each republish the same `(tx_hash, shard, tx_data_position)`
            // TxRef onto tx_ordering, so this reader sees P duplicates per
            // logical tx. Same story for deposits: all M sequencers race to
            // republish the same `DepositRef(source_hash, …)` onto tx_ordering.
            // Only the first occurrence drives a join-buffer take + exec
            // dispatch; the rest are silently dropped. tx_hash and source_hash
            // share a flat namespace (both B256) so we use one window.
            let mut seen_canonical_ids = DedupWindow::new(cfg.dedup_window);
            loop {
                let (position, msg) = match b_sub.next() {
                    Ok(p) => p,
                    Err(ExecutorError::TxOrderingClosed) => return Ok(()),
                    Err(e) => return Err(e),
                };
                match msg {
                    TxOrderingMessage::TxRef(tx_ref) => {
                        if !seen_canonical_ids.first_seen(tx_ref.tx_hash) {
                            // Duplicate from racing sequencers — drop.
                            debug!(
                                target: "kardamom_executor::reader",
                                tx_hash = ?tx_ref.tx_hash,
                                shard_id = tx_ref.shard_id,
                                "skipping duplicate TxRef (MDS racing sequencers)"
                            );
                            continue;
                        }
                        let env = match wait_for_envelope(
                            &buffer,
                            tx_ref.shard_id,
                            tx_ref.tx_data_session_id,
                            tx_ref.tx_data_position,
                            cfg.join_timeout,
                            cfg.join_poll_interval,
                        ) {
                            Some(e) => e,
                            None => {
                                warn!(
                                    target: "kardamom_executor::reader",
                                    sequencer_id = tx_ref.shard_id,
                                    session_id = tx_ref.tx_data_session_id,
                                    tx_data_position = ?tx_ref.tx_data_position,
                                    timeout_ms = cfg.join_timeout.as_millis() as u64,
                                    "join timeout: TxRef has no envelope on tx_data; aborting"
                                );
                                return Err(ExecutorError::JoinTimeout {
                                    sequencer_id: tx_ref.shard_id,
                                    tx_data_position: tx_ref.tx_data_position,
                                    timeout_ms: cfg.join_timeout.as_millis() as u64,
                                });
                            }
                        };

                        // Periodic warn — if the buffer grows unboundedly,
                        // either an A-publisher is racing far ahead of B
                        // (back-pressure issue) or a leak.
                        let cur = buffer.len();
                        if cur >= cfg.buffer_warn_threshold && cur > last_warn_len * 2 {
                            warn!(
                                target: "kardamom_executor::reader",
                                join_buffer_len = cur,
                                threshold = cfg.buffer_warn_threshold,
                                "join buffer growth: A-publisher likely outrunning B"
                            );
                            last_warn_len = cur;
                        }

                        let tx_idx = next_tx_idx;
                        next_tx_idx = next_tx_idx.next();
                        if exec_out
                            .send(ReaderToExec::Tx {
                                tx_idx,
                                envelope: env,
                                position,
                            })
                            .is_err()
                        {
                            return Ok(()); // exec thread shutting down
                        }
                    }
                    TxOrderingMessage::DepositRef(dep_ref) => {
                        if !seen_canonical_ids.first_seen(dep_ref.source_hash) {
                            // Duplicate from racing sequencers (MDS) — drop.
                            debug!(
                                target: "executor::reader",
                                source_hash = ?dep_ref.source_hash,
                                "skipping duplicate DepositRef"
                            );
                            continue;
                        }
                        let (resolved_pos, deposit) = match wait_for_deposit(
                            &deposit_buffer,
                            dep_ref.source_hash,
                            cfg.join_timeout,
                            cfg.join_poll_interval,
                        ) {
                            Some(d) => d,
                            None => {
                                warn!(
                                    target: "executor::reader",
                                    source_hash = ?dep_ref.source_hash,
                                    deposit_position = ?dep_ref.deposit_position,
                                    timeout_ms = cfg.join_timeout.as_millis() as u64,
                                    "join timeout: DepositRef has no deposit on tx_deposits; aborting"
                                );
                                return Err(ExecutorError::DepositJoinTimeout {
                                    source_hash: dep_ref.source_hash,
                                    deposit_position: dep_ref.deposit_position,
                                    timeout_ms: cfg.join_timeout.as_millis() as u64,
                                });
                            }
                        };
                        debug_assert_eq!(
                            resolved_pos, dep_ref.deposit_position,
                            "deposit_position observed by the deposit reader must match the ref"
                        );

                        let tx_idx = next_tx_idx;
                        next_tx_idx = next_tx_idx.next();
                        if exec_out
                            .send(ReaderToExec::Deposit {
                                tx_idx,
                                deposit,
                                position,
                            })
                            .is_err()
                        {
                            return Ok(()); // exec thread shutting down
                        }
                    }
                    TxOrderingMessage::BoundaryStart(b) => {
                        debug!(
                            target: "kardamom_executor::reader",
                            block_number = b.block_number,
                            end_tx_idx = ?b.end_tx_idx,
                            "forwarding BlockBoundaryStart"
                        );
                        if exec_out.send(ReaderToExec::Boundary(b)).is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        })
        .expect("spawn tx_ordering reader")
}

/// Spin-wait for `(sequencer_id, session_id, tx_data_position)` to appear in
/// `buffer`, returning `Some(env)` on success or `None` after `timeout`.
fn wait_for_envelope(
    buffer: &JoinBuffer,
    sequencer_id: u8,
    session_id: i32,
    tx_data_position: BPosition,
    timeout: Duration,
    poll_interval: Duration,
) -> Option<TxEnvelope> {
    if let Some(env) = buffer.take(sequencer_id, session_id, tx_data_position) {
        return Some(env);
    }
    let deadline = Instant::now() + timeout;
    loop {
        thread::sleep(poll_interval);
        if let Some(env) = buffer.take(sequencer_id, session_id, tx_data_position) {
            return Some(env);
        }
        if Instant::now() >= deadline {
            return None;
        }
    }
}

/// Mirror of [`wait_for_envelope`] for the deposit join. Spin with bounded
/// backoff until the deposit identified by `source_hash` lands on the
/// [`DepositJoinBuffer`], or the timeout elapses.
fn wait_for_deposit(
    buffer: &DepositJoinBuffer,
    source_hash: B256,
    timeout: Duration,
    poll_interval: Duration,
) -> Option<(BPosition, Deposit)> {
    if let Some(d) = buffer.take(source_hash) {
        return Some(d);
    }
    let deadline = Instant::now() + timeout;
    loop {
        thread::sleep(poll_interval);
        if let Some(d) = buffer.take(source_hash) {
            return Some(d);
        }
        if Instant::now() >= deadline {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;
    use crossbeam_channel::bounded;
    use kardamom_types::TxRef;
    use std::collections::VecDeque;

    fn envelope(signer: &PrivateKeySigner, nonce: u64) -> TxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(Address::from([0x22u8; 20])),
            value: U256::from(1u64),
            input: AlloyBytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        TxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
    }

    fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }

    /// In-memory tx_data subscription: a `VecDeque` of pre-baked
    /// `(TxDataLoc, TxEnvelope)` records.
    struct VecTxDataSub {
        sequencer_id: u8,
        queue: VecDeque<Result<(TxDataLoc, TxEnvelope), ExecutorError>>,
    }
    impl TxDataSubscription for VecTxDataSub {
        fn sequencer_id(&self) -> u8 {
            self.sequencer_id
        }
        fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError> {
            self.queue
                .pop_front()
                .unwrap_or(Err(ExecutorError::TxDataClosed {
                    sequencer_id: self.sequencer_id,
                }))
        }
    }

    /// Build a `TxDataLoc` with session `0` (the single-publisher default used
    /// by tests that don't model concurrent ingress).
    fn loc(off: i32) -> TxDataLoc {
        TxDataLoc::new(0, pos(off))
    }

    struct VecTxOrderingSub {
        queue: VecDeque<Result<(BPosition, TxOrderingMessage), ExecutorError>>,
    }
    impl TxOrderingSubscription for VecTxOrderingSub {
        fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
            self.queue
                .pop_front()
                .unwrap_or(Err(ExecutorError::TxOrderingClosed))
        }
    }

    #[test]
    fn channel_a_reader_drains_into_buffer() {
        let signer = PrivateKeySigner::random();
        let buf = JoinBuffer::new();
        let a = VecTxDataSub {
            sequencer_id: 3,
            queue: VecDeque::from(vec![
                Ok((loc(0), envelope(&signer, 0))),
                Ok((loc(100), envelope(&signer, 1))),
            ]),
        };
        let h = spawn_tx_data_reader(a, buf.clone());
        h.join().expect("no panic").expect("ok");
        assert_eq!(buf.len(), 2);
        assert!(buf.take(3, 0, pos(0)).is_some());
        assert!(buf.take(3, 0, pos(100)).is_some());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn channel_b_reader_emits_tx_and_boundary_in_canonical_order() {
        let signer = PrivateKeySigner::random();
        let buf = JoinBuffer::new();
        buf.insert(0, 0, pos(0), envelope(&signer, 0));
        buf.insert(1, 0, pos(50), envelope(&signer, 1));

        let b = VecTxOrderingSub {
            queue: VecDeque::from(vec![
                Ok((
                    pos(0),
                    TxOrderingMessage::TxRef(TxRef::new(
                        alloy_primitives::B256::repeat_byte(0xA1),
                        0,
                        pos(0),
                        0,
                    )),
                )),
                Ok((
                    pos(16),
                    TxOrderingMessage::TxRef(TxRef::new(
                        alloy_primitives::B256::repeat_byte(0xA2),
                        1,
                        pos(50),
                        0,
                    )),
                )),
                Ok((
                    pos(32),
                    TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                        block_number: 1,
                        end_tx_idx: pos(16),
                        l2_timestamp: 1_700_000_000,
                    }),
                )),
            ]),
        };
        let (tx, rx) = bounded::<ReaderToExec>(8);
        let h = spawn_tx_ordering_reader(
            b,
            buf,
            DepositJoinBuffer::new(),
            ReaderConfig::default(),
            tx,
        );
        h.join().expect("no panic").expect("ok");

        let mut out = Vec::new();
        while let Ok(m) = rx.recv() {
            out.push(m);
        }
        assert_eq!(out.len(), 3);
        match &out[0] {
            ReaderToExec::Tx {
                tx_idx, position, ..
            } => {
                assert_eq!(*tx_idx, TxIndex(0));
                assert_eq!(*position, pos(0));
            }
            _ => panic!("expected Tx"),
        }
        match &out[1] {
            ReaderToExec::Tx {
                tx_idx, position, ..
            } => {
                assert_eq!(*tx_idx, TxIndex(1));
                assert_eq!(*position, pos(16));
            }
            _ => panic!("expected Tx"),
        }
        match &out[2] {
            ReaderToExec::Boundary(b) => {
                assert_eq!(b.block_number, 1);
                assert_eq!(b.end_tx_idx, pos(16));
            }
            _ => panic!("expected Boundary"),
        }
    }

    /// Race test: `TxRef` arrives BEFORE its envelope. The B reader spins
    /// and picks it up once the A reader inserts.
    #[test]
    fn channel_b_reader_tolerates_a_publisher_lag() {
        let signer = PrivateKeySigner::random();
        let buf = JoinBuffer::new();
        let env = envelope(&signer, 0);

        // Configure a generous timeout so the test passes even on slow CI.
        let cfg = ReaderConfig {
            join_timeout: Duration::from_millis(500),
            join_poll_interval: Duration::from_micros(100),
            ..ReaderConfig::default()
        };

        // TxOrdering has the ref ready immediately. TxData's insert is
        // delayed by a background thread.
        let buf_for_a = buf.clone();
        let env_clone = env.clone();
        let a_inserter = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            buf_for_a.insert(2, 0, pos(0), env_clone);
        });

        let b = VecTxOrderingSub {
            queue: VecDeque::from(vec![Ok((
                pos(0),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 2, pos(0), 0)),
            ))]),
        };
        let (tx, rx) = bounded::<ReaderToExec>(2);
        let h = spawn_tx_ordering_reader(b, buf, DepositJoinBuffer::new(), cfg, tx);
        h.join().expect("no panic").expect("ok");
        a_inserter.join().unwrap();

        let mut out = Vec::new();
        while let Ok(m) = rx.recv() {
            out.push(m);
        }
        assert_eq!(out.len(), 1);
        match &out[0] {
            ReaderToExec::Tx { envelope: e, .. } => assert_eq!(e.tx_hash, env.tx_hash),
            _ => panic!("expected Tx"),
        }
    }

    /// If the envelope never arrives, the tx_ordering reader propagates
    /// `JoinTimeout`.
    #[test]
    fn channel_b_reader_join_timeout_aborts() {
        let buf = JoinBuffer::new();
        let cfg = ReaderConfig {
            join_timeout: Duration::from_millis(50),
            join_poll_interval: Duration::from_millis(5),
            ..ReaderConfig::default()
        };
        let b = VecTxOrderingSub {
            queue: VecDeque::from(vec![Ok((
                pos(0),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 7, pos(0), 0)),
            ))]),
        };
        let (tx, _rx) = bounded::<ReaderToExec>(2);
        let h = spawn_tx_ordering_reader(b, buf, DepositJoinBuffer::new(), cfg, tx);
        let res = h.join().expect("no panic");
        assert!(matches!(
            res,
            Err(ExecutorError::JoinTimeout {
                sequencer_id: 7,
                ..
            })
        ));
    }

    /// Duplicate `TxRef`s (the MDS racing-sequencer republications) collapse
    /// to a single exec dispatch; the join-buffer entry is taken only once.
    #[test]
    fn channel_b_reader_dedups_racing_sequencer_txrefs() {
        let signer = PrivateKeySigner::random();
        let buf = JoinBuffer::new();
        let env = envelope(&signer, 0);
        buf.insert(2, 0, pos(0), env.clone());

        let dup = TxOrderingMessage::TxRef(TxRef::new(env.tx_hash, 2, pos(0), 0));
        let b = VecTxOrderingSub {
            queue: VecDeque::from(vec![
                Ok((pos(0), dup.clone())),
                Ok((pos(16), dup.clone())),
                Ok((pos(32), dup)),
            ]),
        };
        let (tx, rx) = bounded::<ReaderToExec>(4);
        let h = spawn_tx_ordering_reader(
            b,
            buf,
            DepositJoinBuffer::new(),
            ReaderConfig::default(),
            tx,
        );
        h.join().expect("no panic").expect("ok");

        let mut out = Vec::new();
        while let Ok(m) = rx.recv() {
            out.push(m);
        }
        assert_eq!(out.len(), 1, "P duplicates must collapse to one dispatch");
        match &out[0] {
            ReaderToExec::Tx { envelope: e, .. } => assert_eq!(e.tx_hash, env.tx_hash),
            _ => panic!("expected Tx"),
        }
    }

    #[test]
    fn dedup_window_rejects_known_ids_and_evicts_fifo() {
        let id = |b: u8| alloy_primitives::B256::repeat_byte(b);
        let mut w = DedupWindow::new(2);

        assert!(w.first_seen(id(1)));
        assert!(!w.first_seen(id(1)), "second sighting is a duplicate");
        assert!(w.first_seen(id(2)));
        // Window is [1, 2]; inserting 3 evicts 1 (oldest first).
        assert!(w.first_seen(id(3)));
        assert!(!w.first_seen(id(2)), "2 still inside the window");
        assert!(!w.first_seen(id(3)), "3 still inside the window");
        // 1 was evicted above, so it counts as fresh again (and its
        // insertion evicts 2, keeping the window at capacity).
        assert!(w.first_seen(id(1)), "evicted id is fresh again");
        assert_eq!(w.seen.len(), 2);
        assert_eq!(w.fifo.len(), 2);
    }

    /// I-A core proof: under active/active ingress, two publishers on one shard
    /// have independent Aeron term spaces, so they can emit fragments at the
    /// SAME `(term_id, term_offset)`. The join key carries `session_id`, so each
    /// `TxRef` still resolves to its own envelope — no overwrite, no cross-wire.
    /// Pre-1a (key was `(shard, position)`) the second insert would clobber the
    /// first and the executor would join the wrong bytes.
    #[test]
    fn join_buffer_distinguishes_colliding_positions() {
        let signer = PrivateKeySigner::random();
        let buf = JoinBuffer::new();
        let env_a = envelope(&signer, 0);
        let env_b = envelope(&signer, 1);
        // Same shard, same BPosition, different publisher sessions.
        let p = pos(0);
        buf.insert(3, 100, p, env_a.clone());
        buf.insert(3, 200, p, env_b.clone());
        assert_eq!(buf.len(), 2, "distinct sessions must not collide");

        // Each session's take returns its own envelope.
        let got_a = buf.take(3, 100, p).expect("session 100 present");
        let got_b = buf.take(3, 200, p).expect("session 200 present");
        assert_eq!(got_a.tx_hash, env_a.tx_hash);
        assert_eq!(got_b.tx_hash, env_b.tx_hash);
        assert_eq!(buf.len(), 0);

        // A wrong-session lookup misses (it would have silently returned the
        // wrong envelope under the old `(shard, position)` key).
        buf.insert(3, 100, p, env_a.clone());
        assert!(buf.take(3, 999, p).is_none(), "wrong session must miss");
        assert!(buf.take(3, 100, p).is_some());
    }

    /// I-A integration through the real reader threads: two tx_data fragments on
    /// one shard at the SAME `BPosition` but different publisher sessions (the
    /// active/active collision). Two `TxRef`s — each carrying its publisher's
    /// session — must each join the correct envelope, in canonical order.
    #[test]
    fn reader_joins_two_sessions_at_same_position() {
        let signer = PrivateKeySigner::random();
        let env_a = envelope(&signer, 0);
        let env_b = envelope(&signer, 1);
        let buf = JoinBuffer::new();

        // One tx_data reader for shard 5, fed two colliding-position fragments
        // from two distinct sessions (what two active/active ingresses produce).
        let a = VecTxDataSub {
            sequencer_id: 5,
            queue: VecDeque::from(vec![
                Ok((TxDataLoc::new(100, pos(0)), env_a.clone())),
                Ok((TxDataLoc::new(200, pos(0)), env_b.clone())),
            ]),
        };
        spawn_tx_data_reader(a, buf.clone())
            .join()
            .expect("no panic")
            .expect("ok");
        assert_eq!(buf.len(), 2, "distinct sessions must both be buffered");

        // Canonical order interleaves them: env_b's ref first, then env_a's.
        // Each ref carries its session, so the join keys on session not position.
        let b = VecTxOrderingSub {
            queue: VecDeque::from(vec![
                Ok((
                    pos(0),
                    TxOrderingMessage::TxRef(TxRef::new(env_b.tx_hash, 5, pos(0), 200)),
                )),
                Ok((
                    pos(16),
                    TxOrderingMessage::TxRef(TxRef::new(env_a.tx_hash, 5, pos(0), 100)),
                )),
            ]),
        };
        let (tx, rx) = bounded::<ReaderToExec>(4);
        spawn_tx_ordering_reader(
            b,
            buf,
            DepositJoinBuffer::new(),
            ReaderConfig::default(),
            tx,
        )
        .join()
        .expect("no panic")
        .expect("ok");

        let mut out = Vec::new();
        while let Ok(m) = rx.recv() {
            out.push(m);
        }
        assert_eq!(out.len(), 2);
        match &out[0] {
            ReaderToExec::Tx { envelope: e, .. } => {
                assert_eq!(e.tx_hash, env_b.tx_hash, "first ref → session 200 envelope")
            }
            _ => panic!("expected Tx"),
        }
        match &out[1] {
            ReaderToExec::Tx { envelope: e, .. } => {
                assert_eq!(
                    e.tx_hash, env_a.tx_hash,
                    "second ref → session 100 envelope"
                )
            }
            _ => panic!("expected Tx"),
        }
    }
}

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

use crossbeam_channel::Sender;
use dashmap::DashMap;
use tracing::{debug, warn};

use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope, TxOrderingMessage};

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

/// Subscription to one **tx_data[i]**.
///
/// One impl per sequencer partition. Implementations:
/// - in production: `kardamom_log::TxDataSubscriber` on a dedicated OS thread.
/// - in tests: [`crate::testing::VecTxDataSub`] /
///   `FakeTxDataSubscription` from `kardamom_log::testing`.
///
/// The contract: `next` blocks until the next `(tx_data_position, envelope)` is
/// available; returns `Err(ExecutorError::TxDataClosed { sequencer_id })`
/// when the underlying subscription closes cleanly.
pub trait TxDataSubscription: Send {
    /// Sequencer id this subscription is bound to. Used to key the join
    /// buffer and surface diagnostics.
    fn sequencer_id(&self) -> u8;

    fn next(&mut self) -> Result<(BPosition, TxEnvelope), ExecutorError>;
}

/// Subscription to **tx_ordering** (the canonical orderer).
///
/// Yields tiny [`TxOrderingMessage`] records (`TxRef | BoundaryStart`) each
/// tagged with its canonical `BPosition`. The `BPosition` is the system's
/// canonical L2 tx ordering (invariant I1).
///
/// In production: `kardamom_log::TxOrderingSubscriber` on a dedicated OS thread.
/// In tests: see [`crate::testing::VecTxOrderingSub`] or
/// `kardamom_log::testing::FakeTxOrderingSubscription`.
pub trait TxOrderingSubscription: Send {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError>;
}

/// Lookup-and-remove join buffer keyed by `(sequencer_id, tx_data_position)`.
///
/// TxData reader threads insert via [`JoinBuffer::insert`]. The
/// tx_ordering reader pulls via [`JoinBuffer::take`] (remove-on-hit). Bounded
/// by the in-flight window — typically a few thousand entries (~100 MB at
/// envelope-sized values).
///
/// Shared across M+1 threads via `Arc`. `DashMap` over per-shard locks
/// since the access pattern is M concurrent inserts + one concurrent reader;
/// we never iterate.
#[derive(Clone, Default)]
pub struct JoinBuffer {
    inner: Arc<DashMap<(u8, BPosition), TxEnvelope>>,
}

impl JoinBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, sequencer_id: u8, tx_data_position: BPosition, env: TxEnvelope) {
        self.inner.insert((sequencer_id, tx_data_position), env);
    }

    /// Remove and return the envelope at `(sequencer_id, tx_data_position)`, or
    /// `None` if it isn't (yet) present.
    pub fn take(&self, sequencer_id: u8, tx_data_position: BPosition) -> Option<TxEnvelope> {
        self.inner
            .remove(&(sequencer_id, tx_data_position))
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
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            join_timeout: Duration::from_millis(100),
            join_poll_interval: Duration::from_micros(50),
            buffer_warn_threshold: 10_000,
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
    Boundary(BlockBoundaryStart),
}

/// Spawn one tx_data reader thread for `a_sub`. Inserts every
/// `(tx_data_position, envelope)` into `buffer` keyed by
/// `(a_sub.sequencer_id(), tx_data_position)`. Returns when the subscription
/// closes cleanly (`Ok(())`) or propagates the first error.
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
                    Ok((tx_data_position, env)) => buffer.insert(sid, tx_data_position, env),
                    Err(ExecutorError::TxDataClosed { .. }) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        })
        .expect("spawn tx_data reader")
}

/// Spawn the single tx_ordering reader thread. Pulls
/// [`TxOrderingMessage`] records in canonical order; for each
/// `TxRef`, joins against `buffer` (with a bounded wait) and forwards
/// `(position, envelope)` to `exec_out`. For each `BoundaryStart`, forwards
/// directly.
pub fn spawn_tx_ordering_reader<B>(
    mut b_sub: B,
    buffer: JoinBuffer,
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
            // tx_hash dedup. Under the MDS topology the P sequencers per
            // shard each republish the same `(tx_hash, shard, tx_data_position)`
            // TxRef onto tx_ordering, so this reader sees P duplicates per
            // logical tx. Only the first occurrence drives a join-buffer
            // take + exec dispatch; the rest are silently dropped.
            //
            // TODO: this set grows unboundedly. Switch to an LRU keyed by
            // tx_hash with a window large enough to outlive the longest
            // possible in-flight reorder, then evict by age (the canonical
            // order on B means once we're past nonce N we'll never see
            // nonce <N again from the same sender).
            let mut seen_tx_hashes: std::collections::HashSet<alloy_primitives::B256> =
                std::collections::HashSet::new();
            loop {
                let (position, msg) = match b_sub.next() {
                    Ok(p) => p,
                    Err(ExecutorError::TxOrderingClosed) => return Ok(()),
                    Err(e) => return Err(e),
                };
                match msg {
                    TxOrderingMessage::TxRef(tx_ref) => {
                        if !seen_tx_hashes.insert(tx_ref.tx_hash) {
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
                            tx_ref.tx_data_position,
                            cfg.join_timeout,
                            cfg.join_poll_interval,
                        ) {
                            Some(e) => e,
                            None => {
                                warn!(
                                    target: "kardamom_executor::reader",
                                    sequencer_id = tx_ref.shard_id,
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

/// Spin-wait for `(sequencer_id, tx_data_position)` to appear in `buffer`,
/// returning `Some(env)` on success or `None` after `timeout`.
fn wait_for_envelope(
    buffer: &JoinBuffer,
    sequencer_id: u8,
    tx_data_position: BPosition,
    timeout: Duration,
    poll_interval: Duration,
) -> Option<TxEnvelope> {
    if let Some(env) = buffer.take(sequencer_id, tx_data_position) {
        return Some(env);
    }
    let deadline = Instant::now() + timeout;
    loop {
        thread::sleep(poll_interval);
        if let Some(env) = buffer.take(sequencer_id, tx_data_position) {
            return Some(env);
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
    /// `(BPosition, TxEnvelope)` records.
    struct VecTxDataSub {
        sequencer_id: u8,
        queue: VecDeque<Result<(BPosition, TxEnvelope), ExecutorError>>,
    }
    impl TxDataSubscription for VecTxDataSub {
        fn sequencer_id(&self) -> u8 {
            self.sequencer_id
        }
        fn next(&mut self) -> Result<(BPosition, TxEnvelope), ExecutorError> {
            self.queue
                .pop_front()
                .unwrap_or(Err(ExecutorError::TxDataClosed {
                    sequencer_id: self.sequencer_id,
                }))
        }
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
                Ok((pos(0), envelope(&signer, 0))),
                Ok((pos(100), envelope(&signer, 1))),
            ]),
        };
        let h = spawn_tx_data_reader(a, buf.clone());
        h.join().expect("no panic").expect("ok");
        assert_eq!(buf.len(), 2);
        assert!(buf.take(3, pos(0)).is_some());
        assert!(buf.take(3, pos(100)).is_some());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn channel_b_reader_emits_tx_and_boundary_in_canonical_order() {
        let signer = PrivateKeySigner::random();
        let buf = JoinBuffer::new();
        buf.insert(0, pos(0), envelope(&signer, 0));
        buf.insert(1, pos(50), envelope(&signer, 1));

        let b = VecTxOrderingSub {
            queue: VecDeque::from(vec![
                Ok((
                    pos(0),
                    TxOrderingMessage::TxRef(TxRef::new(
                        alloy_primitives::B256::repeat_byte(0xA1),
                        0,
                        pos(0),
                    )),
                )),
                Ok((
                    pos(16),
                    TxOrderingMessage::TxRef(TxRef::new(
                        alloy_primitives::B256::repeat_byte(0xA2),
                        1,
                        pos(50),
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
        let h = spawn_tx_ordering_reader(b, buf, ReaderConfig::default(), tx);
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
            buffer_warn_threshold: 10_000,
        };

        // TxOrdering has the ref ready immediately. TxData's insert is
        // delayed by a background thread.
        let buf_for_a = buf.clone();
        let env_clone = env.clone();
        let a_inserter = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            buf_for_a.insert(2, pos(0), env_clone);
        });

        let b = VecTxOrderingSub {
            queue: VecDeque::from(vec![Ok((
                pos(0),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 2, pos(0))),
            ))]),
        };
        let (tx, rx) = bounded::<ReaderToExec>(2);
        let h = spawn_tx_ordering_reader(b, buf, cfg, tx);
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
            buffer_warn_threshold: 10_000,
        };
        let b = VecTxOrderingSub {
            queue: VecDeque::from(vec![Ok((
                pos(0),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 7, pos(0))),
            ))]),
        };
        let (tx, _rx) = bounded::<ReaderToExec>(2);
        let h = spawn_tx_ordering_reader(b, buf, cfg, tx);
        let res = h.join().expect("no panic");
        assert!(matches!(
            res,
            Err(ExecutorError::JoinTimeout {
                sequencer_id: 7,
                ..
            })
        ));
    }
}

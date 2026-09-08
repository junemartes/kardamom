//! The `ReaderToExec` message, the `tx_data` reader thread, and the
//! `tx_ordering` reader thread.

use std::ops::ControlFlow;
use std::thread::{self, JoinHandle};

use tracing::{debug, warn};

use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage};
use kardamom_types::{
    BPosition, BlockBoundaryStart, Deposit, EpochRecord, TxEnvelope, TxOrderingMessage,
};

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

use super::join::{DedupWindow, JoinBuffer, JoinWait, ReaderConfig, TxDataKey};
use super::ports::{
    ExecSink, JoinRecovery, JoinRecoveryFactory, TxDataSubscription, TxOrderingSubscription,
};

/// Message routed from the `tx_ordering` reader to the executor's exec thread.
///
/// `tx_idx` is the executor-local monotone counter, assigned in canonical
/// (B-position) arrival order. `position` is the `tx_ordering` `BPosition`,
/// the wire-level canonical id. The exec thread uses both: `tx_idx` as a
/// sanity-check newtype, and `position` as the downstream-published
/// `Receipt.tx_idx`.
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
    /// An L1 epoch marker. It advances the block's L1 origin and consumes the
    /// first slot of the epoch's range. It applies no transaction; the epoch's
    /// deposits follow as their own [`ReaderToExec::Deposit`] messages.
    Epoch {
        tx_idx: TxIndex,
        epoch: EpochRecord,
        position: BPosition,
    },
    /// A remote-epoch marker (interop): advances the pair's origin cursor
    /// and consumes the first slot of the record's range. Applies NO
    /// transaction — the record's messages follow as their own
    /// [`ReaderToExec::XChain`] messages. Carries the full record (the
    /// [`Epoch`](Self::Epoch) shape) so the [`RemoteEpochObserver`] seam
    /// observes exactly what traveled the canonical stream; boxed (as is the
    /// message below) so the rare interop arms don't grow the hot enum every
    /// Tx dispatch moves.
    RemoteEpoch {
        tx_idx: TxIndex,
        record: Box<RemoteEpochRecord>,
        position: BPosition,
    },
    /// One derived cross-chain message — a 0x7D tx on this chain.
    /// `origin_chain_id` rides alongside because execution aliases the
    /// sender and authenticates the Inbox call per origin, and the message
    /// itself deliberately does not repeat the pair identity on the wire.
    XChain {
        tx_idx: TxIndex,
        origin_chain_id: u64,
        message: Box<XChainMessage>,
        position: BPosition,
    },
    Boundary(BlockBoundaryStart),
}

/// Spawn one `tx_data` reader thread for `tx_data_sub`. It inserts every
/// `(TxDataLoc, envelope)` into `buffer`, keyed by a [`TxDataKey`] built from
/// `tx_data_sub.sequencer_id()` and the location's session and position. It
/// returns `Ok(())` when the subscription closes cleanly, or the first
/// error.
///
/// # Panics
///
/// Panics if the OS refuses to spawn the thread.
pub fn spawn_tx_data_reader<D>(
    mut tx_data_sub: D,
    buffer: JoinBuffer,
) -> JoinHandle<Result<(), ExecutorError>>
where
    D: TxDataSubscription + 'static,
{
    let sid = tx_data_sub.sequencer_id();
    thread::Builder::new()
        .name(format!("executor-reader-a{sid}"))
        .spawn(move || {
            loop {
                match tx_data_step(&mut tx_data_sub, &buffer, sid)? {
                    TxDataStep::Inserted => {}
                    TxDataStep::Closed => return Ok(()),
                }
            }
        })
        .expect("spawn tx_data reader")
}

/// One `tx_data` receive step: insert the envelope into the join buffer, or
/// report a clean subscription close. The loop in [`spawn_tx_data_reader`]
/// stays a plain dispatch on the result.
fn tx_data_step<D: TxDataSubscription>(
    tx_data_sub: &mut D,
    buffer: &JoinBuffer,
    sid: u8,
) -> Result<TxDataStep, ExecutorError> {
    match tx_data_sub.next() {
        Ok((loc, env)) => {
            buffer.insert(TxDataKey::new(sid, loc.session_id, loc.position), env);
            Ok(TxDataStep::Inserted)
        }
        Err(ExecutorError::TxDataClosed { .. }) => Ok(TxDataStep::Closed),
        Err(e) => Err(e),
    }
}

/// Outcome of one [`tx_data_step`] call.
enum TxDataStep {
    Inserted,
    Closed,
}

/// Continue the loop, or stop cleanly because the exec sink closed. This
/// mirrors `actor::exec_thread::Flow`, one layer down the pipeline.
enum Flow {
    Continue,
    Stop,
}

/// The `tx_ordering` reader's per-message loop state: the join buffer and its
/// optional archive recovery, the canonical-id dedup window, the
/// executor-local record counter, and the exec sink. One instance lives for
/// the reader thread's whole life.
struct OrderingLoop<S> {
    buffer: JoinBuffer,
    cfg: ReaderConfig,
    exec_out: S,
    recovery: Option<JoinRecovery>,
    next_tx_idx: TxIndex,
    last_warn_len: usize,
    // Canonical-id dedup. Under the MDS topology, the P sequencers per
    // shard each republish the same `(tx_hash, shard, tx_data_position)`
    // TxRef onto tx_ordering. So this reader sees P duplicates per logical
    // tx. The same happens for deposits: all M sequencers race to republish
    // the same `DepositRef(source_hash, …)` onto tx_ordering. Only the
    // first occurrence drives a join-buffer take and exec dispatch; the
    // rest are silently dropped. `tx_hash` and `source_hash` share one flat
    // namespace (both B256), so one window serves both.
    seen_canonical_ids: DedupWindow,
}

impl<S: ExecSink> OrderingLoop<S> {
    fn new(
        buffer: JoinBuffer,
        cfg: ReaderConfig,
        exec_out: S,
        recovery: Option<JoinRecovery>,
        start_tx_idx: TxIndex,
    ) -> Self {
        let seen_canonical_ids = DedupWindow::new(cfg.dedup_window);
        Self {
            buffer,
            cfg,
            exec_out,
            recovery,
            next_tx_idx: start_tx_idx,
            last_warn_len: 0,
            seen_canonical_ids,
        }
    }

    /// Allot the next executor-local record index.
    fn next_idx(&mut self) -> TxIndex {
        let idx = self.next_tx_idx;
        self.next_tx_idx = self.next_tx_idx.next();
        idx
    }

    /// Send one message to the exec sink. `Stop` means the exec thread is
    /// shutting down, not an error.
    fn send(&self, msg: ReaderToExec) -> Flow {
        match self.exec_out.send(msg) {
            Ok(()) => Flow::Continue,
            Err(_) => Flow::Stop,
        }
    }

    /// Dispatch a marker's expanded items: an epoch's deposits, or a
    /// remote epoch's messages. Each item gets its own slot position: the
    /// record claimed the range, and the sealer reserved one slot per
    /// item. The item's own count is its position, matching the offline
    /// replay's `global_pos` assignment.
    fn send_expanded<T>(
        &mut self,
        items: Vec<T>,
        make: impl Fn(TxIndex, T) -> ReaderToExec,
    ) -> Flow {
        let flow = items.into_iter().try_for_each(|item| {
            let tx_idx = self.next_idx();
            match self.send(make(tx_idx, item)) {
                Flow::Continue => ControlFlow::Continue(()),
                Flow::Stop => ControlFlow::Break(()),
            }
        });
        match flow {
            ControlFlow::Continue(()) => Flow::Continue,
            ControlFlow::Break(()) => Flow::Stop,
        }
    }

    /// A `TxRef`: dedup, join against the buffer, warn on buffer growth,
    /// then dispatch the joined envelope.
    fn on_tx_ref(
        &mut self,
        tx_ref: kardamom_types::TxRef,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        if !self.seen_canonical_ids.first_seen(tx_ref.tx_hash) {
            // Duplicate from racing sequencers. Drop it.
            debug!(
                target: "kardamom_executor::reader",
                tx_hash = ?tx_ref.tx_hash,
                shard_id = tx_ref.shard_id,
                "skipping duplicate TxRef (MDS racing sequencers)"
            );
            return Ok(Flow::Continue);
        }
        let wait = JoinWait::new(&self.buffer, &mut self.recovery, &tx_ref, &self.cfg)?;
        let Some(env) = wait.run() else {
            // `timeout_ms` is a config value, always small; `u64::MAX` is an
            // unreachable fallback, kept only so this never panics.
            let timeout_ms = u64::try_from(self.cfg.join_timeout.as_millis()).unwrap_or(u64::MAX);
            warn!(
                target: "kardamom_executor::reader",
                sequencer_id = tx_ref.shard_id,
                session_id = tx_ref.tx_data_session_id,
                tx_data_position = ?tx_ref.tx_data_position,
                timeout_ms,
                "join timeout: TxRef has no envelope on tx_data (archive refetch exhausted); aborting"
            );
            return Err(ExecutorError::JoinTimeout {
                sequencer_id: tx_ref.shard_id,
                tx_data_position: tx_ref.tx_data_position,
                timeout_ms,
            });
        };
        self.warn_on_buffer_growth();
        let tx_idx = self.next_idx();
        Ok(self.send(ReaderToExec::Tx {
            tx_idx,
            envelope: env,
            position,
        }))
    }

    /// Periodic warning. If the join buffer keeps growing, either an
    /// A-publisher is racing far ahead of B, a back-pressure issue, or
    /// there is a leak.
    fn warn_on_buffer_growth(&mut self) {
        let cur = self.buffer.len();
        if cur >= self.cfg.buffer_warn_threshold && cur > self.last_warn_len * 2 {
            warn!(
                target: "kardamom_executor::reader",
                join_buffer_len = cur,
                threshold = self.cfg.buffer_warn_threshold,
                "join buffer growth: A-publisher likely outrunning B"
            );
            self.last_warn_len = cur;
        }
    }

    /// An L1 epoch: dedup, dispatch the marker, then dispatch its deposits.
    /// An epoch claims a contiguous slot range: the marker, then one slot
    /// per deposit (see `wire::epoch_slots`). Dispatching the marker first
    /// keeps the exec side's per-record counter in step. This counter is
    /// the block-boundary alignment key. The marker itself gets no
    /// transaction. The deposits travel inside the epoch record. Unlike a
    /// `DepositRef`, there is no side-stream join to wait on; nothing here
    /// can time out or go missing.
    fn expand_epoch(&mut self, epoch: EpochRecord, position: BPosition) -> Flow {
        if !self.seen_canonical_ids.first_seen(epoch.canonical_id()) {
            debug!(
                target: "kardamom_executor::reader",
                l1_number = epoch.l1_number,
                "skipping duplicate Epoch (MDS racing sequencers)"
            );
            return Flow::Continue;
        }
        let deposits = epoch.deposits.clone();
        let marker_idx = self.next_idx();
        if let Flow::Stop = self.send(ReaderToExec::Epoch {
            tx_idx: marker_idx,
            epoch,
            position,
        }) {
            return Flow::Stop;
        }
        self.send_expanded(deposits, |tx_idx, deposit| ReaderToExec::Deposit {
            tx_idx,
            deposit,
            position: BPosition::from_index(tx_idx.0),
        })
    }

    /// A remote epoch (interop): dedup, dispatch the marker, then dispatch
    /// its messages. Same expansion contract as an L1 epoch: the record
    /// claims a contiguous slot range — the marker, then one slot per
    /// message (`wire::remote_epoch_slots`) — and racing sequencers
    /// republish byte-identical records, collapsed here on `canonical_id`.
    /// Messages travel inside the record, so as with epoch deposits there
    /// is no side-stream join to wait on.
    fn expand_remote_epoch(&mut self, rec: RemoteEpochRecord, position: BPosition) -> Flow {
        if !self.seen_canonical_ids.first_seen(rec.canonical_id()) {
            debug!(
                target: "kardamom_executor::reader",
                origin_chain_id = rec.origin_chain_id,
                first_seq = rec.first_seq,
                "skipping duplicate RemoteEpoch (MDS racing sequencers)"
            );
            return Flow::Continue;
        }
        let origin_chain_id = rec.origin_chain_id;
        let messages: Vec<XChainMessage> = rec.messages.iter().cloned().collect();
        let marker_idx = self.next_idx();
        if let Flow::Stop = self.send(ReaderToExec::RemoteEpoch {
            tx_idx: marker_idx,
            record: Box::new(rec),
            position,
        }) {
            return Flow::Stop;
        }
        self.send_expanded(messages, |tx_idx, message| ReaderToExec::XChain {
            tx_idx,
            origin_chain_id,
            message: Box::new(message),
            position: BPosition::from_index(tx_idx.0),
        })
    }

    /// One receive-then-dispatch step: pull the next `tx_ordering` message,
    /// and dispatch it to its handler. Returns `Flow::Stop` on a clean
    /// `tx_ordering` close. The loop in [`spawn_tx_ordering_reader`] stays
    /// a plain dispatch on the result.
    fn step<O: TxOrderingSubscription>(
        &mut self,
        tx_ordering_sub: &mut O,
    ) -> Result<Flow, ExecutorError> {
        let (position, msg) = match tx_ordering_sub.next() {
            Ok(p) => p,
            Err(ExecutorError::TxOrderingClosed) => return Ok(Flow::Stop),
            Err(e) => return Err(e),
        };
        match msg {
            TxOrderingMessage::TxRef(tx_ref) => self.on_tx_ref(tx_ref, position),
            TxOrderingMessage::Epoch(epoch) => Ok(self.expand_epoch(epoch, position)),
            TxOrderingMessage::RemoteEpoch(rec) => Ok(self.expand_remote_epoch(rec, position)),
            TxOrderingMessage::DepositRef(dep_ref) => {
                // A ref here means the stream carries deposits outside an
                // epoch record. This chain derives all deposits from
                // epochs, so this is a protocol violation, not a
                // recoverable condition. Fail loudly instead of silently
                // dropping a deposit.
                tracing::error!(
                    target: "kardamom_executor::reader",
                    source_hash = ?dep_ref.source_hash,
                    "legacy DepositRef on the canonical stream; this chain derives \
                     deposits from epochs"
                );
                Err(ExecutorError::State(format!(
                    "legacy DepositRef {:?}: deposits are carried by epochs on this chain",
                    dep_ref.source_hash
                )))
            }
            TxOrderingMessage::BoundaryStart(b) => {
                debug!(
                    target: "kardamom_executor::reader",
                    block_number = b.block_number,
                    end_tx_idx = ?b.end_tx_idx,
                    "forwarding BlockBoundaryStart"
                );
                Ok(self.send(ReaderToExec::Boundary(b)))
            }
        }
    }
}

/// Spawn the single `tx_ordering` reader thread. It pulls
/// [`TxOrderingMessage`] records in canonical order. For each `TxRef`, it
/// joins against `buffer` with a bounded wait, and forwards `(position,
/// envelope)` to `exec_out`. For each `BoundaryStart`, it forwards directly.
///
/// `start_tx_idx` seeds the executor-local record counter: 0 on a fresh
/// start, or the persisted cursor's record count on a resume. The canonical
/// source delivers from the cursor onward, and downstream checks the
/// indices this reader assigns against absolute boundary counts.
///
/// `recovery_factory`, when wired, turns a join miss into an archive
/// refetch instead of an immediate death; see [`JoinRecovery`]. It runs
/// once, inside this thread, because the recovery's Aeron resources are
/// thread-bound.
///
/// # Panics
///
/// Panics if the OS refuses to spawn the thread.
pub fn spawn_tx_ordering_reader<O, S>(
    mut tx_ordering_sub: O,
    buffer: JoinBuffer,
    cfg: ReaderConfig,
    exec_out: S,
    start_tx_idx: TxIndex,
    recovery_factory: Option<JoinRecoveryFactory>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    O: TxOrderingSubscription + 'static,
    S: ExecSink,
{
    thread::Builder::new()
        .name("executor-reader-b".into())
        .spawn(move || {
            let recovery: Option<JoinRecovery> = recovery_factory.map(JoinRecoveryFactory::build);
            let mut state = OrderingLoop::new(buffer, cfg, exec_out, recovery, start_tx_idx);
            while let Flow::Continue = state.step(&mut tx_ordering_sub)? {}
            Ok(())
        })
        .expect("spawn tx_ordering reader")
}

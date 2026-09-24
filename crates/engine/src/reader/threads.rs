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

use super::join::{JoinBuffer, JoinOutcome, JoinWait, ReaderConfig, TxDataKey};
use super::ports::{
    ExecSink, JoinRecovery, JoinRecoveryFactory, TxDataSubscription, TxOrderingSubscription,
};

/// Message routed from the `tx_ordering` reader to the executor's exec thread.
///
/// The channel is the only producer-to-consumer path, and it keeps order.
/// So the exec thread numbers each record from its own counter as it
/// arrives. A `Tx` carries its `tx_ordering` `BPosition`, the wire-level
/// canonical id, which becomes the published `Receipt.tx_idx`. An expanded
/// item (a deposit or a cross-chain message) has no wire position of its
/// own: its slot index is its position.
#[derive(Debug)]
pub enum ReaderToExec {
    Tx {
        envelope: TxEnvelope,
        position: BPosition,
    },
    Deposit(Deposit),
    /// An L1 epoch marker. It advances the block's L1 origin and consumes the
    /// first slot of the epoch's range. It applies no transaction; the epoch's
    /// deposits follow as their own [`ReaderToExec::Deposit`] messages.
    Epoch(EpochRecord),
    /// A remote-epoch marker (interop): advances the pair's origin cursor
    /// and consumes the first slot of the record's range. Applies NO
    /// transaction — the record's messages follow as their own
    /// [`ReaderToExec::XChain`] messages. Carries the full record (the
    /// [`Epoch`](Self::Epoch) shape) so the [`RemoteEpochObserver`] seam
    /// observes exactly what traveled the canonical stream; boxed (as is the
    /// message below) so the rare interop arms don't grow the hot enum every
    /// Tx dispatch moves.
    RemoteEpoch(Box<RemoteEpochRecord>),
    /// One derived cross-chain message — a 0x7D tx on this chain.
    /// `origin_chain_id` rides alongside because execution aliases the
    /// sender and authenticates the Inbox call per origin, and the message
    /// itself deliberately does not repeat the pair identity on the wire.
    XChain {
        origin_chain_id: u64,
        message: Box<XChainMessage>,
    },
    Boundary(BlockBoundaryStart),
}

/// One `tx_data` reader thread's state: its subscription, the shared join
/// buffer it inserts into, and the sequencer id that keys each insert.
pub struct TxDataReader<D> {
    sub: D,
    buffer: JoinBuffer,
    sid: u8,
}

impl<D: TxDataSubscription + 'static> TxDataReader<D> {
    /// Build the reader for `sub`. The sequencer id comes from the
    /// subscription, so every insert carries the lane it arrived on.
    pub fn new(sub: D, buffer: JoinBuffer) -> Self {
        let sid = sub.sequencer_id();
        Self { sub, buffer, sid }
    }

    /// Spawn the reader on its own OS thread. It inserts every
    /// `(TxDataLoc, envelope)` into the join buffer, keyed by a
    /// [`TxDataKey`] built from the sequencer id and the location's
    /// session and position. The thread returns `Ok(())` when the
    /// subscription closes cleanly, or the first error.
    ///
    /// # Panics
    ///
    /// Panics if the OS refuses to spawn the thread.
    pub fn spawn(self) -> JoinHandle<Result<(), ExecutorError>> {
        thread::Builder::new()
            .name(format!("executor-reader-a{}", self.sid))
            .spawn(move || self.run())
            .expect("spawn tx_data reader")
    }

    /// The reader loop: one [`Self::step`] per record until the
    /// subscription closes.
    fn run(mut self) -> Result<(), ExecutorError> {
        loop {
            match self.step()? {
                TxDataStep::Inserted => (),
                TxDataStep::Closed => return Ok(()),
            }
        }
    }

    /// One `tx_data` receive step: insert the envelope into the join
    /// buffer, or report a clean subscription close.
    fn step(&mut self) -> Result<TxDataStep, ExecutorError> {
        match self.sub.next() {
            Ok((loc, env)) => {
                self.buffer
                    .insert(TxDataKey::new(self.sid, loc.session_id, loc.position), env);
                Ok(TxDataStep::Inserted)
            }
            Err(ExecutorError::TxDataClosed { .. }) => Ok(TxDataStep::Closed),
            Err(e) => Err(e),
        }
    }
}

/// Outcome of one [`TxDataReader::step`] call.
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

/// The `tx_ordering` reader thread's state: the subscription, the join
/// buffer and its optional archive recovery, and the exec sink. One
/// instance lives for the reader thread's whole life.
///
/// The reader forwards every record it receives. The canonical stream is
/// the sealer's egress, which is already deduplicated and totally
/// ordered, and the cluster subscription drops any replay overlap by
/// canonical index. So there is no dedup here.
pub struct TxOrderingReader<O, S> {
    sub: O,
    buffer: JoinBuffer,
    cfg: ReaderConfig,
    exec_out: S,
    recovery: Option<JoinRecovery>,
    last_warn_len: usize,
}

/// Everything [`TxOrderingReader::spawn`] needs.
///
/// `recovery_factory`, when wired, turns a join miss into an archive
/// refetch instead of an immediate death; see [`JoinRecovery`]. The reader
/// builds it once, inside its own thread, because the recovery's Aeron
/// resources are thread-bound.
pub struct TxOrderingInputs<O, S> {
    pub sub: O,
    pub buffer: JoinBuffer,
    pub cfg: ReaderConfig,
    pub exec_out: S,
    pub recovery_factory: Option<JoinRecoveryFactory>,
}

impl<O, S> TxOrderingReader<O, S>
where
    O: TxOrderingSubscription + 'static,
    S: ExecSink,
{
    /// Spawn the single `tx_ordering` reader thread. It pulls
    /// [`TxOrderingMessage`] records in canonical order. For each `TxRef`,
    /// it joins against the buffer with a bounded wait, and forwards
    /// `(position, envelope)` to the exec sink. For each `BoundaryStart`,
    /// it forwards directly.
    ///
    /// The state is built on the spawned thread, so the archive recovery's
    /// Aeron resources stay thread-bound.
    ///
    /// # Panics
    ///
    /// Panics if the OS refuses to spawn the thread.
    pub fn spawn(inputs: TxOrderingInputs<O, S>) -> JoinHandle<Result<(), ExecutorError>> {
        thread::Builder::new()
            .name("executor-reader-b".into())
            .spawn(move || Self::new(inputs).run())
            .expect("spawn tx_ordering reader")
    }

    /// Build the loop state. Runs on the reader thread, so the recovery
    /// factory builds its Aeron resources there.
    fn new(inputs: TxOrderingInputs<O, S>) -> Self {
        let TxOrderingInputs {
            sub,
            buffer,
            cfg,
            exec_out,
            recovery_factory,
        } = inputs;
        let recovery = recovery_factory.map(JoinRecoveryFactory::build);
        Self {
            sub,
            buffer,
            cfg,
            exec_out,
            recovery,
            last_warn_len: 0,
        }
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
    /// remote epoch's messages. The record claimed a contiguous slot
    /// range, one slot per item, and the exec thread numbers the items in
    /// this order.
    fn send_expanded<T>(&self, items: Vec<T>, make: impl Fn(T) -> ReaderToExec) -> Flow {
        let outcome = items
            .into_iter()
            .try_for_each(|item| match self.send(make(item)) {
                Flow::Continue => ControlFlow::Continue(()),
                Flow::Stop => ControlFlow::Break(()),
            });
        match outcome {
            ControlFlow::Continue(()) => Flow::Continue,
            ControlFlow::Break(()) => Flow::Stop,
        }
    }

    /// Log a join that used its whole budget, and make its error. The line
    /// says whether every archive refused the range: then the data is gone
    /// and a restart meets the same entry again, otherwise an archive was
    /// unreachable and a restart can still recover.
    fn join_timeout(
        &self,
        tx_ref: &kardamom_types::TxRef,
        every_archive_refused: bool,
    ) -> ExecutorError {
        // `Duration::as_millis` already returns `u128`, so this needs no
        // fallible narrowing to a smaller integer.
        let timeout_ms = self.cfg.join_timeout.as_millis();
        warn!(
            target: "kardamom_executor::reader",
            sequencer_id = tx_ref.shard_id,
            session_id = tx_ref.tx_data_session_id,
            tx_data_position = ?tx_ref.tx_data_position,
            timeout_ms,
            every_archive_refused,
            "join timeout: TxRef has no envelope on tx_data (archive refetch exhausted); aborting"
        );
        ExecutorError::JoinTimeout {
            sequencer_id: tx_ref.shard_id,
            tx_data_position: tx_ref.tx_data_position,
            timeout_ms,
        }
    }

    /// A `TxRef`: join against the buffer, warn on buffer growth, then
    /// dispatch the joined envelope.
    fn on_tx_ref(
        &mut self,
        tx_ref: kardamom_types::TxRef,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        let wait = JoinWait::new(&self.buffer, &mut self.recovery, &tx_ref, &self.cfg)?;
        let env = match wait.run() {
            JoinOutcome::Joined(env) => env,
            JoinOutcome::Unjoinable => return Err(self.join_timeout(&tx_ref, true)),
            JoinOutcome::TimedOut => return Err(self.join_timeout(&tx_ref, false)),
        };
        self.warn_on_buffer_growth();
        Ok(self.send(ReaderToExec::Tx {
            envelope: env,
            position,
        }))
    }

    /// Periodic warning. If the join buffer keeps growing, either the
    /// `tx_data` publisher is racing far ahead of `tx_ordering`, a
    /// back-pressure issue, or there is a leak.
    fn warn_on_buffer_growth(&mut self) {
        let cur = self.buffer.len();
        if cur >= self.cfg.buffer_warn_threshold && cur > self.last_warn_len * 2 {
            warn!(
                target: "kardamom_executor::reader",
                join_buffer_len = cur,
                threshold = self.cfg.buffer_warn_threshold,
                "join buffer growth: tx_data publisher likely outrunning tx_ordering"
            );
            self.last_warn_len = cur;
        }
    }

    /// An L1 epoch: dispatch the marker, then dispatch its deposits. An
    /// epoch claims a contiguous slot range: the marker, then one slot per
    /// deposit (see `wire::epoch_slots`). Dispatching the marker first
    /// keeps the exec side's per-record counter in step. This counter is
    /// the block-boundary alignment key. The marker itself gets no
    /// transaction. The deposits travel inside the epoch record. Unlike a
    /// `DepositRef`, there is no side-stream join to wait on; nothing here
    /// can time out or go missing.
    fn expand_epoch(&self, epoch: EpochRecord) -> Flow {
        let deposits = epoch.deposits.clone();
        if let Flow::Stop = self.send(ReaderToExec::Epoch(epoch)) {
            return Flow::Stop;
        }
        self.send_expanded(deposits, ReaderToExec::Deposit)
    }

    /// A remote epoch (interop): dispatch the marker, then dispatch its
    /// messages. Same expansion contract as an L1 epoch: the record claims
    /// a contiguous slot range, the marker, then one slot per message
    /// (`wire::remote_epoch_slots`). Messages travel inside the record, so
    /// as with epoch deposits there is no side-stream join to wait on.
    fn expand_remote_epoch(&self, rec: RemoteEpochRecord) -> Flow {
        let origin_chain_id = rec.origin_chain_id;
        let messages: Vec<XChainMessage> = rec.messages.iter().cloned().collect();
        if let Flow::Stop = self.send(ReaderToExec::RemoteEpoch(Box::new(rec))) {
            return Flow::Stop;
        }
        self.send_expanded(messages, |message| ReaderToExec::XChain {
            origin_chain_id,
            message: Box::new(message),
        })
    }

    /// The reader loop: one [`Self::step`] per message until the
    /// subscription closes or the exec sink stops.
    fn run(mut self) -> Result<(), ExecutorError> {
        while let Flow::Continue = self.step()? {}
        Ok(())
    }

    /// One receive-then-dispatch step: pull the next `tx_ordering` message,
    /// and dispatch it to its handler. Returns `Flow::Stop` on a clean
    /// `tx_ordering` close. The loop in [`Self::run`] stays a plain
    /// dispatch on the result.
    fn step(&mut self) -> Result<Flow, ExecutorError> {
        let (position, msg) = match self.sub.next() {
            Ok(p) => p,
            Err(ExecutorError::TxOrderingClosed) => return Ok(Flow::Stop),
            Err(e) => return Err(e),
        };
        match msg {
            TxOrderingMessage::TxRef(tx_ref) => self.on_tx_ref(tx_ref, position),
            TxOrderingMessage::Epoch(epoch) => Ok(self.expand_epoch(epoch)),
            TxOrderingMessage::RemoteEpoch(rec) => Ok(self.expand_remote_epoch(rec)),
            // This reader sends every entry it passes to the executor, so a
            // void record that arrives here names an entry that this replica
            // executed. The replica and the canonical order disagree. Stop.
            // The sealer appends no void record until a reader can ask for
            // one, and that reader also learns to drop the entry.
            TxOrderingMessage::Void(void) => Err(ExecutorError::VoidOfExecutedEntry {
                index: void.index,
                tx_hash: void.tx_hash,
            }),
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

//! The `ReaderToExec` message, the `tx_data` reader thread, and the
//! `tx_ordering` reader thread.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::thread::{self, JoinHandle};

use tracing::{debug, info};

use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage};
use kardamom_types::{
    BPosition, BlockBoundaryStart, Deposit, EpochRecord, TxEnvelope, TxOrderingMessage, VoidRecord,
};

use crate::error::ExecutorError;

use super::cluster::slot_width;
use super::join::{JoinBuffer, ReaderConfig, TxDataKey};
use super::ports::{ExecSink, ExecStreamSink, TxDataSubscription, TxOrderingSubscription};
use super::source::{JoinAt, JoinSeed, Joined, TxJoin};
use super::void::ReadAhead;

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
        /// The reference the canonical stream carried: where the bytes
        /// are on a `tx_data` archive. The state writer keeps it with
        /// the receipt, so a block the sealer no longer retains can be
        /// rebuilt from the archive.
        tx_ref: kardamom_types::TxRef,
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
    /// A canonical slot that carries no transaction: a voided entry, or the
    /// void record that removed it. The executor only counts the slot, so its
    /// record counter stays equal to the sealer's at each boundary.
    Vacant {
        position: BPosition,
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
pub(super) enum Flow {
    Continue,
    Stop,
}

/// The `tx_ordering` reader thread's state: the subscription, the join of
/// its transaction source, the exec sink, and the executor-stream sink.
/// One instance lives for the reader thread's whole life.
///
/// The reader forwards every record it receives. The canonical stream is
/// the sealer's egress, which is already deduplicated and totally
/// ordered, and the cluster subscription drops any replay overlap by
/// canonical index. So there is no dedup here.
pub struct TxOrderingReader<O, S, E, J> {
    sub: O,
    cfg: ReaderConfig,
    exec_out: S,
    exec_stream: E,
    pub(super) join: J,
    /// The canonical index of the first record this reader read. A void
    /// record for an entry below it names an entry that an earlier run of
    /// this consumer already passed.
    first_index: Option<u64>,
    /// Messages that a void wait read ahead. The loop dispatches them, in
    /// order, before it reads the subscription again.
    backlog: ReadAhead,
    /// Indices of entries this reader dropped, whose void record is still
    /// in the backlog. The record's own slot counts when its turn comes.
    dropped: BTreeSet<u64>,
}

/// Everything [`TxOrderingReader::spawn`] needs.
///
/// `join` is the seed of the transaction source's join. The reader builds
/// the join once, inside its own thread, because a join can hold Aeron
/// resources that are bound to one thread.
///
/// `exec_stream` gets each joined record before the exec thread does, and
/// a progress mark after each message that takes a slot.
pub struct TxOrderingInputs<O, S, E, D> {
    pub sub: O,
    pub cfg: ReaderConfig,
    pub exec_out: S,
    pub exec_stream: E,
    pub join: D,
}

impl<O, S, E, J> TxOrderingReader<O, S, E, J>
where
    O: TxOrderingSubscription + 'static,
    S: ExecSink,
    E: ExecStreamSink,
    J: TxJoin,
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
    pub fn spawn<D>(inputs: TxOrderingInputs<O, S, E, D>) -> JoinHandle<Result<(), ExecutorError>>
    where
        D: JoinSeed<Join = J>,
    {
        thread::Builder::new()
            .name("executor-reader-b".into())
            .spawn(move || Self::new(inputs).run())
            .expect("spawn tx_ordering reader")
    }

    /// Build the loop state. Runs on the reader thread, so the join
    /// builds its Aeron resources there.
    pub(super) fn new<D: JoinSeed<Join = J>>(inputs: TxOrderingInputs<O, S, E, D>) -> Self {
        let TxOrderingInputs {
            sub,
            cfg,
            exec_out,
            exec_stream,
            join,
        } = inputs;
        let join = join.build(&cfg);
        Self {
            sub,
            cfg,
            exec_out,
            exec_stream,
            join,
            first_index: None,
            backlog: ReadAhead::new(),
            dropped: BTreeSet::new(),
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

    /// The error of a reader whose executor-stream publisher is gone. The
    /// reader stops, because it must not execute a record that the stream
    /// did not take.
    fn stream_closed() -> ExecutorError {
        ExecutorError::State("the executor stream publisher stopped".into())
    }

    /// A `TxRef`: get its bytes from the source, send the joined record
    /// to the executor stream, then dispatch the envelope. An entry that
    /// the canonical order voided goes to the executor as a vacant slot.
    fn on_tx_ref(
        &mut self,
        tx_ref: kardamom_types::TxRef,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        let at = JoinAt {
            tx_ref: &tx_ref,
            position,
            cfg: &self.cfg,
            order: &mut self.sub,
            backlog: &mut self.backlog,
        };
        let joined = self.join.join(at)?;
        self.on_joined(joined, tx_ref, position)
    }

    /// Dispatch the outcome of one join: the record goes to the executor
    /// stream and then to the exec thread, a voided entry becomes a vacant
    /// slot, and a closed order stops the loop.
    fn on_joined(
        &mut self,
        joined: Joined,
        tx_ref: kardamom_types::TxRef,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        let env = match joined {
            Joined::Tx(env) => env,
            Joined::Voided => return Ok(self.vacate(&tx_ref, position)),
            Joined::Closed => return Ok(Flow::Stop),
        };
        self.exec_stream
            .record(position.as_index(), &tx_ref, &env)
            .map_err(|_| Self::stream_closed())?;
        Ok(self.send(ReaderToExec::Tx {
            envelope: env,
            position,
            tx_ref,
        }))
    }

    /// Drop a voided entry: its slot goes to the executor as a vacant
    /// slot, and its void record, still ahead in the backlog, counts its
    /// own slot when its turn comes.
    fn vacate(&mut self, tx_ref: &kardamom_types::TxRef, position: BPosition) -> Flow {
        let index = position.as_index();
        info!(
            target: "kardamom_executor::reader",
            index,
            tx_hash = ?tx_ref.tx_hash,
            "the sealer voided the entry: dropping it"
        );
        self.dropped.insert(index);
        self.send(ReaderToExec::Vacant { position })
    }

    /// A void record. Its entry is one this reader dropped, or one below the
    /// first index that an earlier run passed: count the record's own slot.
    /// Any other void record names an entry that this reader sent to the
    /// executor. The replica and the canonical order then disagree. Stop.
    fn on_void(&mut self, void: &VoidRecord, position: BPosition) -> Result<Flow, ExecutorError> {
        let before_start = self.first_index.is_some_and(|first| void.index < first);
        if !self.dropped.remove(&void.index) && !before_start {
            return Err(ExecutorError::VoidOfExecutedEntry {
                index: void.index,
                tx_hash: void.tx_hash,
            });
        }
        Ok(self.send(ReaderToExec::Vacant { position }))
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
    pub(super) fn run(mut self) -> Result<(), ExecutorError> {
        while let Flow::Continue = self.step()? {}
        Ok(())
    }

    /// One receive-then-dispatch step: pull the next `tx_ordering` message,
    /// dispatch it to its handler, then send the message's progress mark
    /// to the executor stream. Returns `Flow::Stop` on a clean
    /// `tx_ordering` close. The loop in [`Self::run`] stays a plain
    /// dispatch on the result.
    fn step(&mut self) -> Result<Flow, ExecutorError> {
        let read = match self.backlog.pop_front() {
            Some(read) => Ok(read),
            None => self.sub.next(),
        };
        let (position, msg) = match read {
            Ok(p) => p,
            Err(ExecutorError::TxOrderingClosed) => return Ok(Flow::Stop),
            Err(e) => return Err(e),
        };
        self.note_first(position.as_index());
        let through = Self::last_slot(position, &msg);
        match self.dispatch(position, msg)? {
            Flow::Continue => self.mark(through),
            Flow::Stop => Ok(Flow::Stop),
        }
    }

    /// The last canonical slot that `msg` at `position` takes. A boundary
    /// takes no slot. Every other record takes `slot_width` slots from its
    /// own index.
    fn last_slot(position: BPosition, msg: &TxOrderingMessage) -> Option<u64> {
        if let TxOrderingMessage::BoundaryStart(_) = msg {
            return None;
        }
        position
            .as_index()
            .checked_add(slot_width(msg).checked_sub(1)?)
    }

    /// Keep the index of the first message, and tell the answers state.
    fn note_first(&mut self, index: u64) {
        if self.first_index.is_some() {
            return;
        }
        self.first_index = Some(index);
        self.cfg.tell_answers(|answers| answers.first(index));
    }

    /// Send the progress mark of a dispatched message, when it takes a
    /// slot, to the executor stream and to the answers state.
    fn mark(&self, through: Option<u64>) -> Result<Flow, ExecutorError> {
        through
            .map_or(Ok(()), |t| self.exec_stream.passed(t))
            .map_err(|_| Self::stream_closed())?;
        if let Some(t) = through {
            self.cfg.tell_answers(|answers| answers.passed(t));
        }
        Ok(Flow::Continue)
    }

    /// Dispatch one message to its handler.
    fn dispatch(
        &mut self,
        position: BPosition,
        msg: TxOrderingMessage,
    ) -> Result<Flow, ExecutorError> {
        match msg {
            TxOrderingMessage::TxRef(tx_ref) => self.on_tx_ref(tx_ref, position),
            TxOrderingMessage::Epoch(epoch) => Ok(self.expand_epoch(epoch)),
            TxOrderingMessage::RemoteEpoch(rec) => Ok(self.expand_remote_epoch(rec)),
            TxOrderingMessage::Void(void) => self.on_void(&void, position),
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

#[cfg(test)]
impl<O, S, E> TxOrderingReader<O, S, E, super::tx_data::TxDataJoin>
where
    O: TxOrderingSubscription + 'static,
    S: ExecSink,
    E: ExecStreamSink,
{
    /// The reader at the point a `tx_data` join reaches when every archive
    /// failed the entry: refused it (`every_archive_refused`) or gave no
    /// definite answer. No unit test can make an archive fail a range, so
    /// the void and peer tests enter here.
    pub(super) fn on_unjoined(
        &mut self,
        tx_ref: &kardamom_types::TxRef,
        position: BPosition,
        every_archive_refused: bool,
    ) -> Result<Flow, ExecutorError> {
        let at = JoinAt {
            tx_ref,
            position,
            cfg: &self.cfg,
            order: &mut self.sub,
            backlog: &mut self.backlog,
        };
        let joined = self.join.on_unjoined(at, every_archive_refused)?;
        self.on_joined(joined, *tx_ref, position)
    }
}

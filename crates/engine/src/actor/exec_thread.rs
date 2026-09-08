//! This is the sequential execution thread. It reads canonical-ordered
//! records from the reader layer. It executes each record against the
//! snapshot, then the parent layer, then the delta. It sends receipts and
//! durably-settled boundaries to the commit thread.
//!
//! The loop state lives in [`ExecState`]. Each `ReaderToExec` arm is one
//! `on_*` method, split across `exec_records.rs` (Tx, Deposit, `XChain`),
//! `exec_markers.rs` (Epoch, `RemoteEpoch`), and `exec_boundary.rs`
//! (`BoundaryStart`). The pipelined-commit settle sweep lives in
//! `exec_settle.rs`. The idle probe and the boundary arm both use it.

use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::ExecutorError;
use crate::reader::ReaderToExec;

use super::wiring::ExecPorts;

// Re-exported so sibling arm modules (`exec_records`, `exec_markers`,
// `exec_boundary`, `exec_settle`) can import both `ExecState` and `Flow`
// from this one module, matching where the loop and the spawn live.
pub(super) use super::exec_state::ExecState;
// Re-exported at crate visibility so `actor.rs` can, in turn, re-export
// them to the test submodules that build `ExecInputs` directly.
pub(crate) use super::exec_state::{ExecHooks, ExecInputs};

/// How long an IDLE exec thread waits before it probes the writer for
/// settled in-flight commits.
///
/// On an idle tail, the last blocks never publish their boundary closeouts
/// without this: no boundary arrives to trigger the settle sweep, and
/// downstream consumers stall.
///
/// Under load the timeout never fires, because records arrive faster.
/// When idle, the probe is a cheap, non-blocking read.
const IDLE_SETTLE_PROBE: Duration = Duration::from_millis(25);

/// Outcome of one receive attempt on the reader channel. See
/// [`ExecState::recv_next`].
enum Recv {
    Msg(ReaderToExec),
    IdleProbe,
    Closed,
}

/// Control-flow result of a message handler. Continue the loop, or stop
/// cleanly. Stop means the commit channel's receiver is gone: this is
/// shutdown, not an error.
pub(super) enum Flow {
    Continue,
    Stop,
}

impl<W: ExecPorts> ExecState<W> {
    /// The exec thread's main loop. It receives a message, or runs the idle
    /// probe, then dispatches to the matching handler. It stops cleanly when
    /// the commit channel's receiver is gone, or the reader channel closes.
    fn run(&mut self) -> Result<(), ExecutorError> {
        while let Flow::Continue = self.recv_and_dispatch()? {}
        Ok(())
    }

    /// One receive-then-dispatch step: [`Self::recv_next`], then either
    /// the idle probe or the matching `on_*` handler. [`Self::run`] stays
    /// a plain loop over this.
    fn recv_and_dispatch(&mut self) -> Result<Flow, ExecutorError> {
        let msg = match self.recv_next() {
            Recv::Msg(m) => m,
            Recv::Closed => {
                self.on_closed();
                return Ok(Flow::Stop);
            }
            Recv::IdleProbe => return self.on_idle_probe(),
        };
        self.dispatch(msg)
    }

    /// Dispatch one canonical-stream message to its handler.
    fn dispatch(&mut self, msg: ReaderToExec) -> Result<Flow, ExecutorError> {
        match msg {
            ReaderToExec::Tx {
                tx_idx,
                envelope,
                position,
            } => self.on_tx(tx_idx, envelope, position),
            ReaderToExec::Epoch {
                tx_idx,
                epoch,
                position,
            } => self.on_epoch(tx_idx, &epoch, position),
            ReaderToExec::Deposit {
                tx_idx,
                deposit,
                position,
            } => self.on_deposit(tx_idx, deposit, position),
            ReaderToExec::RemoteEpoch {
                tx_idx,
                record,
                position,
            } => self.on_remote_epoch(tx_idx, &record, position),
            ReaderToExec::XChain {
                tx_idx,
                origin_chain_id,
                message,
                position,
            } => self.on_xchain(tx_idx, origin_chain_id, message, position),
            ReaderToExec::Boundary(start) => self.on_boundary(&start),
        }
    }

    /// One receive attempt. With commits in flight, the wait is bounded by
    /// [`IDLE_SETTLE_PROBE`], so an idle tail still settles. With no commits
    /// in flight, the wait blocks indefinitely, because there is nothing to
    /// settle.
    fn recv_next(&self) -> Recv {
        if self.inflight.is_empty() {
            match self.rx.recv() {
                Ok(m) => Recv::Msg(m),
                Err(_) => Recv::Closed,
            }
        } else {
            match self.rx.recv_timeout(IDLE_SETTLE_PROBE) {
                Ok(m) => Recv::Msg(m),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => Recv::IdleProbe,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => Recv::Closed,
            }
        }
    }
}

/// Spawn the exec thread.
///
/// # Panics
///
/// Panics if the OS refuses to spawn the thread.
pub(crate) fn spawn_exec<W: ExecPorts + 'static>(
    inputs: ExecInputs<W>,
) -> JoinHandle<Result<(), ExecutorError>> {
    thread::Builder::new()
        .name("executor-exec".into())
        .spawn(move || -> Result<(), ExecutorError> { ExecState::new(inputs).run() })
        .expect("spawn exec")
}

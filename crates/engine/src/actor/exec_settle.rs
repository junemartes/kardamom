//! Pipelined-commit settling for the exec thread.
//!
//! This module has the shared settle sweep. The sweep forwards
//! durably-committed boundaries, swaps the snapshot, and rebuilds the
//! parent read layer. It also has two entry points for the sweep: the idle
//! probe, and the boundary arm's depth-capped variant. It also has the
//! end-of-stream close-out.
//!
//! The sweep is one method, [`ExecState::settle_ready`]. The boundary entry
//! point only adds its blocking full-depth `wait_committed` step first. One
//! sweep method serves both entry points, so an edit to the settle logic
//! cannot apply to only one of them.

use std::num::NonZeroUsize;
use std::time::Instant;

use kardamom_types::{BlockBoundary, SnapshotSource};
use tracing::debug;

use crate::error::ExecutorError;

use super::exec_thread::{ExecState, Flow};
use super::ports::StateWriterSignal;
use super::types::ExecToCommit;
use super::wiring::ExecPorts;

/// Matches `kardamom_state::geometry::HORIZON_BLOCKS`, the writer's own
/// bounded queue depth. A deeper exec pipeline would only block in
/// `submit` instead.
///
/// Typed `NonZeroUsize` even though the literal is safe today: at 0, the
/// depth-cap guard below (`inflight.len() >= COMMIT_PIPELINE_DEPTH`) always
/// holds, so the exec thread would block in `wait_committed` at every
/// boundary and the pipeline would silently collapse to depth 1.
pub(super) const COMMIT_PIPELINE_DEPTH: NonZeroUsize = NonZeroUsize::new(4).expect("4 is nonzero");

/// The commit-thread channel closed. The settle sweep must stop.
struct SendFailed;

impl<W: ExecPorts> ExecState<W> {
    /// The settle sweep. It pops every in-flight commit at or below
    /// `durable` and forwards its boundary. This way, downstream never
    /// sees a boundary that a crash could un-commit. When anything
    /// settles, the sweep swaps to the newest settled block's snapshot. It
    /// also rebuilds the merged parent read layer from the still-unsettled
    /// survivors, because a merged map cannot be subtracted from. The
    /// `msg` argument keeps the log shape of the two call sites
    /// distinguishable.
    pub(super) fn settle_ready(&mut self, durable: u64, msg: &'static str) -> Flow {
        let mut newest_settled = None;
        let mut stopped = false;
        while let Some((b, _)) = self
            .inflight
            .pop_front_if(|(b, _)| b.block_number <= durable)
        {
            let Ok(block_number) = self.forward_settled_boundary(b) else {
                stopped = true;
                break;
            };
            newest_settled = Some(block_number);
        }
        if stopped {
            return Flow::Stop;
        }
        if let Some(n) = newest_settled {
            debug!(
                target: "executor",
                durable,
                through_block = n,
                unsettled = self.inflight.len(),
                "{msg}"
            );
            self.snapshot = self.snapshots.snapshot_after(n);
            self.parent = self.inflight.iter().fold(None, |acc, (_, d)| match acc {
                None => Some(d.clone()),
                Some(mut m) => {
                    m.merge_from(d);
                    Some(m)
                }
            });
        }
        Flow::Continue
    }

    /// Forward `b` to the commit thread. Returns its block number on
    /// success, so the caller can record it as the newest settled block.
    fn forward_settled_boundary(&mut self, b: BlockBoundary) -> Result<u64, SendFailed> {
        #[allow(
            clippy::cast_precision_loss,
            reason = "block numbers stay far below 2^52"
        )]
        metrics::gauge!(crate::metrics::BLOCK_NUMBER).set(b.block_number as f64);
        let block_number = b.block_number;
        self.tx
            .send(ExecToCommit::Boundary(b))
            .map(|()| block_number)
            .map_err(|_| SendFailed)
    }

    /// Boundary-arm settling. This runs the non-blocking
    /// [`Self::settle_ready`] sweep, after the only wait the pipeline ever
    /// pays. The exec thread blocks for the oldest commit only when the
    /// pipeline is at full depth K. The recorded histogram measures
    /// exactly this residual stall. A sustained non-zero residual means
    /// the writer is K full block intervals behind, and this
    /// back-pressure is correct.
    pub(super) fn settle_at_boundary(&mut self) -> Result<Flow, ExecutorError> {
        let mut durable = self.sw_signal.committed()?;
        if self.inflight.len() >= COMMIT_PIPELINE_DEPTH.get()
            && self
                .inflight
                .front()
                .is_some_and(|(b, _)| b.block_number > durable)
        {
            let oldest = self.inflight.front().map_or(0, |(b, _)| b.block_number);
            let commit_wait = Instant::now();
            durable = self.sw_signal.wait_committed(oldest)?;
            metrics::histogram!(crate::metrics::STATE_COMMIT_DURATION_SECONDS)
                .record(commit_wait.elapsed().as_secs_f64());
        }
        Ok(self.settle_ready(durable, "pipelined commits settled; snapshot swapped"))
    }

    /// Idle-probe settling. This runs the same non-blocking settle sweep
    /// as the boundary arm, without the full-depth blocking wait. An idle
    /// probe must never park.
    pub(super) fn on_idle_probe(&mut self) -> Result<Flow, ExecutorError> {
        // Settle only at a block edge. With a block open (the scope is
        // materialized, or records are buffered), the live ExecScope's
        // cache is seeded against the current snapshot and parent.
        // Swapping the snapshot and rebuilding the parent mid-block would
        // mix read bases and cause execution to diverge.
        //
        // Between blocks, both are empty. This is the idle-tail case this
        // probe exists for. A mid-block gap simply defers to the next
        // boundary's sweep.
        if self.scope.is_some() || !self.buffered.is_empty() {
            return Ok(Flow::Continue);
        }
        let durable = self.sw_signal.committed()?;
        Ok(self.settle_ready(
            durable,
            "pipelined commits settled on idle probe; snapshot swapped",
        ))
    }

    /// Clean end of stream. Settle every in-flight commit so the pipeline
    /// does not silently drop the final boundaries.
    pub(super) fn on_closed(&mut self) {
        let Some((last, _)) = self.inflight.back() else {
            return;
        };
        let last_n = last.block_number;
        if self.sw_signal.wait_committed(last_n).is_ok() {
            self.flush_inflight_boundaries();
        }
    }

    /// Send every in-flight boundary to the commit thread, once its block
    /// is known durable. [`Self::on_closed`]'s branch stays free of a
    /// loop.
    fn flush_inflight_boundaries(&mut self) {
        for (b, _) in self.inflight.drain(..) {
            let _ = self.tx.send(ExecToCommit::Boundary(b));
        }
    }
}

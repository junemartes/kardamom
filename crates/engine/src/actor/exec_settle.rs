//! Pipelined-commit settling for the exec thread: the shared settle sweep
//! (forward durably-committed boundaries, swap the snapshot, rebuild the
//! parent read layer), its two entry points — the idle probe and the
//! boundary arm's depth-capped variant — and the end-of-stream close-out.
//!
//! The sweep itself is ONE method ([`ExecState::settle_ready`]); the boundary
//! entry point adds only its blocking full-depth `wait_committed` prelude.
//! Pre-split the sweep existed verbatim twice (idle-probe arm and boundary
//! arm) — a one-sided edit there would have been a real divergence bug.

use std::time::Instant;

use kardamom_types::SnapshotSource;
use tracing::debug;

use crate::error::ExecutorError;

use super::exec_thread::{ExecState, Flow};
use super::ports::{StateWriterQueue, StateWriterSignal};
use super::types::ExecToCommit;

/// Matches `kardamom_state::geometry::HORIZON_BLOCKS` — the writer's
/// own bounded queue depth; a deeper exec pipeline would just
/// block in `submit` instead.
pub(super) const COMMIT_PIPELINE_DEPTH: usize = 4;

impl<S, Q, P> ExecState<S, Q, P>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
{
    /// The settle sweep: pop every in-flight commit at or below `durable`
    /// and forward its boundary — downstream never observes a boundary a
    /// crash could un-commit. When anything settled, swap to the newest
    /// settled block's snapshot and rebuild the merged parent read layer
    /// from the still-unsettled survivors (a merged map cannot be
    /// subtracted from). `msg` keeps the two call sites' log shapes
    /// distinguishable.
    pub(super) fn settle_ready(
        &mut self,
        durable: u64,
        msg: &'static str,
    ) -> Result<Flow, ExecutorError> {
        let mut newest_settled = None;
        while self
            .inflight
            .front()
            .is_some_and(|(b, _)| b.block_number <= durable)
        {
            let (b, _) = self.inflight.pop_front().expect("front checked");
            metrics::gauge!(crate::metrics::BLOCK_NUMBER).set(b.block_number as f64);
            newest_settled = Some(b.block_number);
            if self.tx.send(ExecToCommit::Boundary(b)).is_err() {
                return Ok(Flow::Stop);
            }
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
        Ok(Flow::Continue)
    }

    /// Boundary-arm settling: the non-blocking [`Self::settle_ready`] sweep,
    /// preceded by the only wait the pipeline ever pays. Only when the
    /// pipeline is at full depth K does the exec thread block for the
    /// OLDEST commit — the recorded histogram measures exactly that
    /// residual stall (sustained non-zero residuals mean the writer is K
    /// full block intervals behind and this back-pressure is correct).
    pub(super) fn settle_at_boundary(&mut self) -> Result<Flow, ExecutorError> {
        let mut durable = self.sw_signal.committed()?;
        if self.inflight.len() >= COMMIT_PIPELINE_DEPTH
            && self
                .inflight
                .front()
                .is_some_and(|(b, _)| b.block_number > durable)
        {
            let oldest = self
                .inflight
                .front()
                .map(|(b, _)| b.block_number)
                .unwrap_or(0);
            let commit_wait = Instant::now();
            durable = self.sw_signal.wait_committed(oldest)?;
            metrics::histogram!(crate::metrics::STATE_COMMIT_DURATION_SECONDS)
                .record(commit_wait.elapsed().as_secs_f64());
        }
        self.settle_ready(durable, "pipelined commits settled; snapshot swapped")
    }

    /// Idle-probe settling: same non-blocking settle sweep as the boundary
    /// arm, minus the full-depth blocking wait (an idle probe must never
    /// park).
    pub(super) fn on_idle_probe(&mut self) -> Result<Flow, ExecutorError> {
        // Settle ONLY at a block edge: with a block OPEN
        // (scope materialized / records buffered), the live
        // ExecScope's cache is seeded against the CURRENT
        // snapshot ∘ parent — swapping the snapshot and
        // rebuilding parent under it mid-block mixes read
        // bases and diverges execution (caught by the load
        // shard's validator divergence latch on the first
        // soak of this probe). Between blocks — the idle-tail
        // case this probe exists for — both are empty, and a
        // mid-block gap simply defers to the next boundary's
        // sweep exactly as before the probe existed.
        if self.scope.is_some() || !self.buffered.is_empty() {
            return Ok(Flow::Continue);
        }
        let durable = self.sw_signal.committed()?;
        self.settle_ready(
            durable,
            "pipelined commits settled on idle probe; snapshot swapped",
        )
    }

    /// Clean end of stream: settle every in-flight commit
    /// so the final boundaries aren't silently dropped.
    pub(super) fn on_closed(&mut self) -> Result<(), ExecutorError> {
        if let Some((last, _)) = self.inflight.back() {
            let last_n = last.block_number;
            if self.sw_signal.wait_committed(last_n).is_ok() {
                for (b, _) in self.inflight.drain(..) {
                    let _ = self.tx.send(ExecToCommit::Boundary(b));
                }
            }
        }
        Ok(())
    }
}

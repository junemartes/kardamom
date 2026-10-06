//! The `BoundaryStart` arm: alignment check, the optional whole-block
//! strategy, block-close protocol actions, and the commit handoff.

use std::time::{Duration, Instant};

use kardamom_types::{BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta};
use revm::state::bal::Bal;

use crate::error::ExecutorError;

use super::exec_block::{BlockRun, BlockState, ExecutedBlock};
use super::exec_records::{Finished, RecordKind, Writes};
use super::exec_state::CommitPipeline;
use super::exec_thread::{ExecState, Flow};
use super::ports::StateWriterQueue;
use super::types::BufferedRecord;
use super::wiring::ExecPorts;

impl<W: ExecPorts> ExecState<W> {
    /// Run the block-close protocol actions for the block being sealed.
    ///
    /// This supplies the two state layers that `exec-core` cannot see on its
    /// own: the merged unsettled-parent delta, then the mdbx snapshot. The
    /// composed read checks `delta`, then `parent`, then `snapshot`, in that
    /// order, matching what the EVM sees through `seed_cache_layer`. Reading
    /// the snapshot alone would miss the current block's writes and up to K
    /// unsettled blocks. For a flag read, that would activate a feature
    /// late, or never, on a busy chain, while a quieter replica activates
    /// it on time. This is a divergence.
    fn apply_block_close_actions(
        &mut self,
        block_number: u64,
        header_ts_ms: u64,
    ) -> Result<(), ExecutorError> {
        // Destructure by field: `delta` is borrowed mutably, while
        // `apply_block_close_actions` reads `parent` and `snapshot`.
        let Self {
            block: BlockState { delta, .. },
            commits: CommitPipeline {
                parent, snapshot, ..
            },
            ..
        } = self;

        let outcome = kardamom_exec_core::features::apply_block_close_actions(
            delta,
            block_number,
            header_ts_ms,
            parent.as_ref(),
            snapshot,
        )?;

        if let Some(beat) = outcome.health_beat {
            metrics::counter!(crate::metrics::HEALTH_BEACON_BEATS_TOTAL).increment(1);
            tracing::info!(
                block_number,
                beat,
                l2_timestamp_ms = header_ts_ms,
                "health beacon"
            );
        }
        Ok(())
    }

    /// Check the boundary's declared record count against what the
    /// executor applied. `BlockBoundaryStart.end_tx_idx` carries the
    /// sealer's cumulative count of canonical records (`TxRef` and
    /// `DepositRef`) republished through the end of this block, encoded
    /// through `BPosition::from_index`. `next_tx_idx` tracks the same
    /// count on the executor side: it advances once per record, and never
    /// resets. The two values must match.
    ///
    /// A mismatch means the executor's view of the canonical stream
    /// diverged from the sealer's: a lost, extra, or reordered record.
    /// This is fatal. The error propagates so the process crash-loops,
    /// instead of committing a wrong block.
    fn check_alignment(
        &self,
        block_number: u64,
        end_tx_idx: BPosition,
    ) -> Result<(), ExecutorError> {
        let want = end_tx_idx.as_index();
        let have = self.cursor.next_tx_idx.0;
        if want != have {
            tracing::error!(
                block = block_number,
                want_count = want,
                have_count = have,
                "exec ERROR: BoundaryMisaligned (canonical record count)"
            );
            return Err(ExecutorError::BoundaryMisaligned {
                end: end_tx_idx,
                last_seen: BPosition::from_index(have),
            });
        }
        Ok(())
    }

    /// Run the whole-block execution strategy, in the whole-block mode (the
    /// validator's parallel path): execute everything buffered for this
    /// block, with the validator's batches running concurrently inside this
    /// call, then feed the receipts and delta into the same boundary path
    /// the streaming executor uses. Commit ordering, durability gating, and
    /// the write-set cross-check stay the same. Each record then finishes
    /// through [`Self::finish_record`], like a streaming record. The
    /// streaming mode leaves the delta untouched: the streaming arms
    /// already built it.
    fn run_block_exec(&mut self, block_number: u64) -> Result<Flow, ExecutorError> {
        let env = self.exec_env(block_number);
        let BlockRun::Whole(whole) = &mut self.block.run else {
            return Ok(Flow::Continue);
        };
        let apply_start = Instant::now();
        let ExecutedBlock {
            records,
            delta,
            bal,
        } = whole.execute(
            &self.commits.snapshot,
            self.commits.parent.as_ref(),
            env,
            block_number,
        )?;
        *self.block.apply_elapsed.get_or_insert(Duration::ZERO) += apply_start.elapsed();
        self.block.delta = delta;
        self.adopt_strategy_bal(block_number, bal);
        // The parallel path has no per-tx write sets. The block's merged
        // rows ride the last receipt, so no row is tagged with a position
        // before the writes it carries. The other receipts carry none. An
        // empty block has no receipts, so the subtraction saturates at 0.
        let rows = std::iter::repeat_with(Vec::new)
            .take(records.len().saturating_sub(1))
            .chain(std::iter::once(self.block.delta.account_rows()));
        for ((rec, receipt), rows) in records.into_iter().zip(rows) {
            if let Flow::Stop = self.finish_buffered(&rec, receipt, rows)? {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }

    /// BAL parity across strategies: a capturing strategy hands its folded
    /// per-block Bal here, so the boundary handoff publishes it the same
    /// way the streaming capture would. If a publishing role's strategy
    /// captured nothing, it would otherwise emit an empty BAL with no
    /// warning, and every validator would degrade to the sequential
    /// fallback with no signal. So this combination logs a warning.
    fn adopt_strategy_bal(&mut self, block_number: u64, bal: Option<Bal>) {
        match (self.block.bal.as_mut(), bal) {
            (Some(capture), Some(b)) => capture.bal = b,
            (Some(_), None) => {
                tracing::warn!(
                    block = block_number,
                    "block-exec strategy returned no BAL while BAL \
                     publication is on; publishing an empty capture"
                );
            }
            (None, _) => {}
        }
    }

    /// Finish one record the whole-block strategy executed, through the
    /// same result path as a streaming record.
    fn finish_buffered(
        &mut self,
        rec: &BufferedRecord,
        receipt: kardamom_types::Receipt,
        rows: Vec<kardamom_types::AccountRow>,
    ) -> Result<Flow, ExecutorError> {
        let (kind, position) = RecordKind::of_buffered(rec, self.cursor.block);
        self.finish_record(Finished {
            kind,
            position,
            result: Ok((receipt, Writes::Rows(rows))),
        })
    }

    pub(super) fn on_boundary(
        &mut self,
        start: &BlockBoundaryStart,
    ) -> Result<Flow, ExecutorError> {
        let BlockBoundaryStart {
            block_number,
            end_tx_idx,
            l2_timestamp,
            l1_origin,
        } = start.clone();
        if let Flow::Stop = self.settle_at_boundary()? {
            return Ok(Flow::Stop);
        }
        self.check_alignment(block_number, end_tx_idx)?;
        if let Flow::Stop = self.run_block_exec(block_number)? {
            return Ok(Flow::Stop);
        }

        // Record the block's total execution time. Only record this when
        // the block had at least one tx; skip empty blocks.
        if let Some(elapsed) = self.block.apply_elapsed.take() {
            metrics::histogram!(crate::metrics::BLOCK_APPLY_DURATION_SECONDS)
                .record(elapsed.as_secs_f64());
        }

        // Block-close protocol actions (L1-governed feature flags).
        //
        // This call must happen here: after every record of the block has
        // landed in `self.block.delta` (through the streaming arms above, or the
        // whole-block strategy's fold above), and before the code takes the
        // delta for the writer. This order lets an upgrade deposit activate a
        // feature for the very block that carried it, and puts the actions'
        // writes in the same delta the validator cross-checks.
        //
        // This code runs for every role: the executor, the streaming
        // validator, and the parallel validator. It is engine code. A role
        // that skipped it would diverge on the first active block.
        //
        // `l2_timestamp` is this boundary's own stamp, the block's own header
        // time. It is not the previous boundary's time, which is what the
        // block's txs executed with.
        self.apply_block_close_actions(block_number, l2_timestamp)?;

        // No state-root computation yet. The sealed BlockBoundary on
        // tx_receipts is slim; it carries no commitment. `l1_origin` passes
        // through unchanged from the sealer's marker. It identifies the L1
        // epoch this block belongs to. This is what lets a reconstructor
        // place the epoch's deposits. The block's base fee and gas used
        // ride along: the next block's base fee follows from them, on
        // every role that reads the boundary.
        let gas_used = self
            .block
            .receipts
            .last()
            .map_or(0, |r| r.cumulative_gas_used);
        let boundary = BlockBoundary {
            block_number,
            end_tx_idx,
            l2_timestamp,
            l1_origin,
            base_fee: self.cursor.fees.base_fee,
            gas_used,
        };

        // Drain the delta. Swap it out so the writer owns it, but keep a
        // clone as the next block's parent read layer. In pipelined commit,
        // cloning the block's write maps costs only a few ms, far less than
        // the fsync it takes off the critical path.
        //
        // The block's receipts ride inside the BlockDelta, in arrival order,
        // so the writer persists them durably. They also streamed out on
        // tx_receipts at execute time, above, ahead of durability. A crash in
        // that window re-executes the block on recovery and re-publishes
        // byte-identical receipts. So tx_receipts is at-least-once, and
        // every consumer must dedup on `tx_idx` (ingress already does).
        let pending = std::mem::take(&mut self.block.delta);
        self.block.run.seal(block_number);
        if let Some(capture) = self.block.bal.as_mut() {
            capture.handoff(&boundary, &pending);
        }
        match self.commits.parent.as_mut() {
            Some(m) => m.merge_from(&pending),
            None => self.commits.parent = Some(pending.clone()),
        }
        self.commits
            .inflight
            .push_back((boundary.clone(), pending.clone()));
        let bd: BlockDelta =
            pending.finalize(block_number, std::mem::take(&mut self.block.receipts));

        // Submit without waiting. The commit settles at a later boundary's
        // sweep, or at the end of the stream.
        let refs = std::mem::take(&mut self.block.refs);
        self.io.sw_queue.submit(boundary, bd, refs)?;
        // `block_number` comes from `BlockBoundaryStart` on the wire; a
        // corrupt value near `u64::MAX` must not wrap the next block back
        // to 0 and re-execute the chain.
        self.cursor.block = block_number.saturating_add(1);
        self.block.tx_index = 0;
        self.block.cumulative_gas_used = 0;
        // The next block's wall-clock timestamp arrives in its own
        // BlockBoundaryStart. Until then, keep the previous value as a
        // deterministic placeholder, for any tx that races ahead of the
        // sealer. In v0 the sealer is single-leader, so this branch is
        // purely defensive.
        self.cursor.l2_ts = l2_timestamp;
        self.cursor.fees = self.cursor.fees.next(gas_used);
        Ok(Flow::Continue)
    }
}

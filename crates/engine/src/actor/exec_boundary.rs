//! The `BoundaryStart` arm: alignment check, the optional whole-block
//! strategy, block-close protocol actions, and the commit handoff.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use kardamom_types::{BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta};

use crate::delta::PendingDelta;
use crate::error::ExecutorError;

use super::exec_thread::{ExecState, Flow};
use super::ports::StateWriterQueue;
use super::types::{BalHandoff, BlockExecStrategy, ExecToCommit};
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
            delta,
            parent,
            snapshot,
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
    /// through `BPosition::from_index`. `expected_tx_idx` tracks the same
    /// count on the executor side: it advances once per applied Tx or
    /// Deposit, and never resets. The two values must match.
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
        let have = self.expected_tx_idx.0;
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

    /// Run the whole-block execution strategy, when one is wired (the
    /// validator's parallel path): execute everything buffered for this
    /// block, with the validator's batches running concurrently inside this
    /// call, then feed the receipts and delta into the same boundary path
    /// the streaming executor uses. Commit ordering, durability gating, and
    /// the write-set cross-check stay the same. A `None` `block_exec`
    /// leaves the buffer and delta untouched: the streaming arms already
    /// built them.
    fn run_block_exec(&mut self, block_number: u64) -> Result<Flow, ExecutorError> {
        let Some(exec_block) = self.block_exec.as_ref() else {
            return Ok(Flow::Continue);
        };
        let env = self.exec_env(block_number);
        let apply_start = Instant::now();
        let out = exec_block.execute_block(
            &self.snapshot,
            self.parent.as_ref(),
            &self.buffered,
            env,
            block_number,
        )?;
        *self.block_apply_elapsed.get_or_insert(Duration::ZERO) += apply_start.elapsed();
        self.buffered.clear();
        self.delta = out.delta;
        // BAL parity across strategies: a capturing strategy hands its
        // folded per-block Bal here, so the boundary handoff below
        // publishes it the same way the streaming capture would. If a
        // publishing role's strategy captured nothing, it would otherwise
        // emit an empty BAL with no warning, and every validator would
        // degrade to the sequential fallback with no signal. So this
        // combination logs a warning.
        match out.bal {
            Some(b) => self.block_bal = b,
            None if self.bal_tx.is_some() => {
                tracing::warn!(
                    block = block_number,
                    "block-exec strategy returned no BAL while BAL \
                     publication is on; publishing an empty capture"
                );
            }
            None => {}
        }
        let flow = out.receipts.into_iter().try_for_each(|r| {
            self.block_receipts.push(r.clone());
            match self.tx.send(ExecToCommit::Receipt(r)) {
                Ok(()) => ControlFlow::Continue(()),
                Err(_) => ControlFlow::Break(()),
            }
        });
        match flow {
            ControlFlow::Continue(()) => Ok(Flow::Continue),
            ControlFlow::Break(()) => Ok(Flow::Stop),
        }
    }

    /// EIP-7928 handoff: move the block's Bal, and a receipts-free copy of
    /// the merged delta, to the publisher thread. Encoding and reliable
    /// delivery happen entirely off this thread. `try_send` keeps the BAL
    /// handoff off the critical path: a dropped frame costs one block of
    /// BAL retention, which verifies as `bal_missing`, a tolerated path. A
    /// dropped send also means the publisher is gone mid-shutdown; this is
    /// not fatal.
    fn handoff_bal(&mut self, boundary: &BlockBoundary, pending: &PendingDelta) {
        let Some(btx) = self.bal_tx.as_ref() else {
            return;
        };
        let block_number = boundary.block_number;
        let bal_delta = pending.clone().finalize(block_number, Vec::new());
        btx.try_handoff(
            BalHandoff {
                boundary: boundary.clone(),
                delta: bal_delta,
                bal: std::mem::take(&mut self.block_bal),
            },
            block_number,
            BAL_HANDOFF,
            || {},
        );
    }

    /// Footprint-shadow handoff: the same never-block discipline as the BAL
    /// handoff. A dropped block costs one block of measurement, which is
    /// counted, not the chain. Skips empty blocks; there is nothing to
    /// grade.
    fn handoff_shadow(&mut self, block_number: u64) {
        let Some(stx) = self.shadow_tx.as_ref() else {
            return;
        };
        if self.shadow_captures.is_empty() && self.shadow_serial == 0 {
            return;
        }
        let blk = crate::shadow::ShadowBlock {
            block_number,
            captures: std::mem::take(&mut self.shadow_captures),
            serial_records: std::mem::take(&mut self.shadow_serial),
        };
        stx.try_handoff(blk, block_number, SHADOW_HANDOFF, || {
            metrics::counter!(
                crate::metrics::FOOTPRINT_BLOCKS_TOTAL,
                "outcome" => "dropped"
            )
            .increment(1);
        });
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
        if let Some(elapsed) = self.block_apply_elapsed.take() {
            metrics::histogram!(crate::metrics::BLOCK_APPLY_DURATION_SECONDS)
                .record(elapsed.as_secs_f64());
        }

        // Block-close protocol actions (L1-governed feature flags).
        //
        // This call must happen here: after every record of the block has
        // landed in `self.delta` (through the streaming arms above, or the
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
        // place the epoch's deposits.
        let boundary = BlockBoundary {
            block_number,
            end_tx_idx,
            l2_timestamp,
            l1_origin,
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
        let pending = std::mem::take(&mut self.delta);
        // The block's execution scope dies with the block. The next block
        // gets a new parent layer and block env, and, once commits settle, a
        // fresh snapshot. This drop is unconditional: a scope reused across a
        // boundary would execute against the previous block's parent and env.
        self.scope = None;
        self.handoff_bal(&boundary, &pending);
        self.handoff_shadow(block_number);
        match self.parent.as_mut() {
            Some(m) => m.merge_from(&pending),
            None => self.parent = Some(pending.clone()),
        }
        self.inflight.push_back((boundary.clone(), pending.clone()));
        let bd: BlockDelta =
            pending.finalize(block_number, std::mem::take(&mut self.block_receipts));

        // Submit without waiting. The commit settles at a later boundary's
        // sweep, or at the end of the stream.
        self.sw_queue.submit(boundary, bd)?;
        // `block_number` comes from `BlockBoundaryStart` on the wire; a
        // corrupt value near `u64::MAX` must not wrap the next block back
        // to 0 and re-execute the chain.
        self.current_block = block_number.saturating_add(1);
        self.tx_index_in_block = 0;
        self.cumulative_gas_used = 0;
        // The next block's wall-clock timestamp arrives in its own
        // BlockBoundaryStart. Until then, keep the previous value as a
        // deterministic placeholder, for any tx that races ahead of the
        // sealer. In v0 the sealer is single-leader, so this branch is
        // purely defensive.
        self.current_l2_ts = l2_timestamp;
        Ok(Flow::Continue)
    }
}

/// Never-block handoff to a bounded auxiliary channel (the BAL publisher,
/// the footprint-shadow grader): `try_send`, then ignore a disconnected
/// receiver (the consumer is gone mid-shutdown, not fatal), or run
/// `on_full` (any extra bookkeeping, for example a dropped-block metric)
/// and warn when the channel is full.
///
/// An extension trait: `Sender` is foreign to this crate, so this cannot
/// be an inherent method.
trait SenderHandoff<T> {
    fn try_handoff(
        &self,
        item: T,
        block_number: u64,
        labels: HandoffLabels,
        on_full: impl FnOnce(),
    );
}

impl<T> SenderHandoff<T> for Sender<T> {
    fn try_handoff(
        &self,
        item: T,
        block_number: u64,
        labels: HandoffLabels,
        on_full: impl FnOnce(),
    ) {
        match self.try_send(item) {
            Ok(()) | Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                on_full();
                tracing::warn!(
                    block = block_number,
                    "{} handoff full; dropping this block's {}",
                    labels.what,
                    labels.dropped
                );
            }
        }
    }
}

/// One [`SenderHandoff::try_handoff`] call site's log wording: the name
/// used in the warn line, and the noun for what gets dropped.
struct HandoffLabels {
    what: &'static str,
    dropped: &'static str,
}

const BAL_HANDOFF: HandoffLabels = HandoffLabels {
    what: "BAL",
    dropped: "frame (publisher pump stalled?)",
};

const SHADOW_HANDOFF: HandoffLabels = HandoffLabels {
    what: "footprint-shadow",
    dropped: "capture",
};

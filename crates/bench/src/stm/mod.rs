//! This module does offline Block-STM analysis: footprint statistics and
//! oracle dependency analysis.
//!
//! Everything here is offline analysis: no engine changes, no cluster.
//! The capture runner executes workload blocks in order, through the
//! real engine, with a fresh per-transaction `Bal`, so `storage_reads`
//! is attributed per transaction. The classifier recovers mapping
//! base-slots by keccak inversion. The oracle builds the true
//! dependency graph from actual read and write sets. Together these
//! yield the go/no-go numbers: critical-path ratio, prediction
//! hit rates, over-merge cost, and fee-sink identification.

pub mod capture;
pub mod uniswap;
pub mod workload;

// The classifier, oracle, and cell model live in `kardamom-footprint`,
// so the live executor's shadow scheduler (`kardamom-engine::shadow`)
// grades on exactly the code the offline go verdict was measured with.
// This module re-exports them.
pub use kardamom_footprint::{Cell, TxObs, classifier, oracle};

use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::{Executor, TouchSet};
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::envelope_view;
use kardamom_types::{BPosition, BlockBoundary, BlockBoundaryStart, TxEnvelope};

/// A 0-based flow-block index, with the timestamp formula every
/// offline scenario and engine-execution test in this crate uses to
/// turn it into an [`ExecEnv`] or a [`BlockBoundary`]: block numbers
/// are 1-based (`index + 1`), and the timestamp advances 2 seconds per
/// block from a fixed epoch.
#[derive(Debug, Clone, Copy)]
pub struct BlockAt(pub usize);

impl BlockAt {
    /// The 1-based block number.
    #[must_use]
    pub fn number(self) -> u64 {
        (self.0 as u64).saturating_add(1)
    }

    /// The synthetic L2 timestamp: a fixed epoch, plus 2 seconds for
    /// each block before this one.
    #[must_use]
    pub fn timestamp(self) -> u64 {
        1_700_000_000u64.saturating_add((self.0 as u64).saturating_mul(2))
    }

    /// The `BlockBoundaryStart` this block would carry, ending at
    /// record index `end`.
    #[must_use]
    pub fn start(self, end: u64) -> BlockBoundaryStart {
        BlockBoundaryStart {
            block_number: self.number(),
            end_tx_idx: BPosition::from_index(end),
            l2_timestamp: self.timestamp(),
            l1_origin: 0,
        }
    }

    /// The `BlockBoundary` this block would publish, ending at record
    /// index `end`.
    #[must_use]
    pub fn boundary(self, end: u64) -> BlockBoundary {
        BlockBoundary {
            block_number: self.number(),
            end_tx_idx: BPosition::from_index(end),
            l2_timestamp: self.timestamp(),
            l1_origin: 0,
        }
    }

    /// The `ExecEnv` for this block, on `chain_id`. `end_tx_idx` plays
    /// no part in an `ExecEnv`, so this always starts from `0`.
    #[must_use]
    pub fn env(self, chain_id: u64) -> ExecEnv {
        ExecEnv::new(chain_id, &self.start(0))
    }
}

/// One transaction's receipt and write set, from [`SeqExec::run`].
pub struct Executed {
    pub receipt: kardamom_types::Receipt,
    pub write_set: kardamom_engine::delta::WriteSet,
}

/// Runs signed transactions sequentially, in order, through one open
/// engine scope, carrying `cumulative_gas_used` from one call to the
/// next. Every offline scenario and engine-execution test in this
/// crate that executes a block's transactions one at a time uses this.
///
/// A caller that re-scopes per block, or per some other window, builds
/// a new `SeqExec` for that window; [`SeqExec::resume`] carries the
/// cumulative gas total across the rebuild, for a window boundary that
/// is not also a gas-accounting boundary.
pub struct SeqExec<'a, S: kardamom_types::StateDatabase> {
    exec: kardamom_engine::executor::Executor<&'a S>,
    cumulative: u64,
}

impl<'a, S: kardamom_types::StateDatabase> SeqExec<'a, S> {
    /// Open a new scope on `snap`, seeded with `parent`, at `env`, with
    /// `cumulative_gas_used` starting at 0.
    ///
    /// # Errors
    ///
    /// Returns an error if opening the scope, or seeding `parent` into
    /// it, fails.
    pub fn new(
        snap: &'a S,
        parent: Option<&PendingDelta>,
        env: ExecEnv,
    ) -> Result<Self, kardamom_engine::error::ExecutorError> {
        Self::resume(snap, parent, env, 0)
    }

    /// Like [`SeqExec::new`], but starting `cumulative_gas_used` from
    /// `cumulative` instead of 0. Use this to rebuild the scope at a
    /// window boundary (a fresh `ExecEnv` or a bounded cache) that is
    /// not also a gas-accounting boundary, such as re-scoping every
    /// `BLOCK_TXS` transactions within one simulated block.
    ///
    /// # Errors
    ///
    /// Returns an error if opening the scope, or seeding `parent` into
    /// it, fails.
    pub fn resume(
        snap: &'a S,
        parent: Option<&PendingDelta>,
        env: ExecEnv,
        cumulative: u64,
    ) -> Result<Self, kardamom_engine::error::ExecutorError> {
        Ok(Self {
            exec: kardamom_engine::executor::Executor::new(snap, parent, env)?,
            cumulative,
        })
    }

    /// The running `cumulative_gas_used` total, after the last [`run`](Self::run) call.
    #[must_use]
    pub fn cumulative(&self) -> u64 {
        self.cumulative
    }

    /// Execute one transaction at `tx_idx`/`position`, the
    /// `tx_index_in_block`-th record in whatever window this scope
    /// covers. `bal`, if set, captures this transaction's touches into
    /// the caller's own per-block or per-transaction BAL.
    ///
    /// # Errors
    ///
    /// Returns an error if execution fails.
    pub fn run(
        &mut self,
        tx_idx: kardamom_engine::TxIndex,
        position: kardamom_types::BPosition,
        envelope: &kardamom_types::TxEnvelope,
        tx_index_in_block: u64,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<Executed, kardamom_engine::error::ExecutorError> {
        let (receipt, write_set) = self.exec.execute_tx(
            tx_idx,
            position,
            envelope,
            tx_index_in_block,
            self.cumulative,
            bal,
            None,
        )?;
        self.cumulative = receipt.cumulative_gas_used;
        Ok(Executed { receipt, write_set })
    }
}

/// Training on [`Stats`], the footprint classifier `kardamom-footprint`
/// owns: an extension trait, since this crate cannot add inherent
/// methods to a foreign type. Every offline scenario binary that
/// trains the classifier from real execution, or replays it from
/// pre-captured observations, uses this.
pub trait StatsExt {
    /// Fold a block's actual footprints in, from a fresh scope over
    /// `snapshot`. This is the capture pass the live shadow performs,
    /// so an offline scenario trains its schedule on the same code the
    /// live go/no-go verdict was measured with.
    ///
    /// # Errors
    ///
    /// Returns an error if opening the scope or executing a
    /// transaction fails.
    fn train_block<S: kardamom_types::StateDatabase>(
        &mut self,
        snapshot: &S,
        recs: &[(TxIndex, BPosition, TxEnvelope)],
        e: ExecEnv,
        base: Option<&PendingDelta>,
    ) -> anyhow::Result<()>;

    /// Train on every observation in `txs`, in order. The shadow loop
    /// used for offline analysis calls this once per block, in stream
    /// order, to grade then train.
    fn learn_all(&mut self, txs: &[TxObs]);
}

impl StatsExt for Stats {
    fn train_block<S: kardamom_types::StateDatabase>(
        &mut self,
        snapshot: &S,
        recs: &[(TxIndex, BPosition, TxEnvelope)],
        e: ExecEnv,
        base: Option<&PendingDelta>,
    ) -> anyhow::Result<()> {
        let mut scope = Executor::new(snapshot, base, e)?;
        let mut cumulative = 0u64;
        for (i, (tx_idx, position, envelope)) in recs.iter().enumerate() {
            let mut touches = TouchSet::default();
            let (receipt, ws) = scope.execute_tx(
                *tx_idx,
                *position,
                envelope,
                i as u64,
                cumulative,
                None,
                Some(&mut touches),
            )?;
            cumulative = receipt.cumulative_gas_used;
            let (to, selector, args, has_value) = envelope_view(&envelope.raw_tx);
            let mut reads: Vec<Cell> = touches
                .slot_reads
                .iter()
                .map(|(ad, k)| Cell::Slot(*ad, *k))
                .collect();
            reads.sort_unstable();
            reads.dedup();
            let mut writes: Vec<Cell> = ws
                .accounts
                .iter()
                .map(|(ad, _)| Cell::Account(*ad))
                .chain(ws.storage.iter().map(|((ad, k), _)| Cell::Slot(*ad, *k)))
                .collect();
            writes.sort_unstable();
            writes.dedup();
            self.learn_obs(&TxObs {
                index: i as u64,
                block: e.block_number,
                sender: envelope.sender,
                to,
                selector,
                args,
                gas: receipt.gas_used,
                has_value,
                reads,
                writes,
            });
        }
        Ok(())
    }

    fn learn_all(&mut self, txs: &[TxObs]) {
        for o in txs {
            self.learn_obs(o);
        }
    }
}

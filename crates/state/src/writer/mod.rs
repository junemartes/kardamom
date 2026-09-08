//! A single writer thread. It drains the [`WriteBatch`] channel, commits
//! one mdbx read-write transaction per block boundary, and publishes new
//! snapshots through the snapshot-swap channel.
//!
//! ## Coordination with the executor
//!
//! The executor's `submit(boundary: BlockBoundary, delta: BlockDelta)` is
//! the producer. This writer is the consumer. This crate accepts the pair
//! as a [`WriteBatch`], so the cursor persisted in the `meta` table
//! tracks all three values:
//!
//! - `last_committed_block` = `boundary.block_number`
//! - `last_committed_end_tx_position` = `boundary.end_tx_idx`
//! - `last_fsynced_b_position` = `boundary.end_tx_idx`. These are the
//!   same value, because the boundary's `end_tx_idx` is the last B
//!   position the executor committed through.
//!
//! `BlockDelta` lives in `kardamom-types`. This crate never redefines it.

mod apply;

use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender};
use kardamom_types::{BlockBoundary, BlockDelta};
use tracing::{debug, error, info, warn};

use crate::env::StateEnv;
use crate::error::StateError;
use crate::snapshot::StateSnapshot;
use crate::swap::{SnapshotHandle, SnapshotReceiver, channel as swap_channel};

/// One block's worth of state changes, submitted to the writer.
///
/// This pairs the boundary marker with its delta. The writer then
/// persists, in a single atomic mdbx commit:
///
/// - The block-level cursors.
/// - The per-key state mutations.
/// - The per-transaction receipts.
#[derive(Debug, Clone)]
pub struct WriteBatch {
    pub boundary: BlockBoundary,
    pub delta: BlockDelta,
}

impl WriteBatch {
    #[must_use]
    pub fn new(boundary: BlockBoundary, delta: BlockDelta) -> Self {
        Self { boundary, delta }
    }

    /// The worst-case encoded size, used by the writer to budget the mdbx
    /// transaction. This is a heuristic, not an exact value.
    #[must_use]
    pub(crate) fn approx_size_bytes(&self) -> usize {
        let acct = self.delta.accounts.len() * (20 + 96);
        let stor = self.delta.storage.len() * (52 + 32);
        let code: usize = self.delta.code.iter().map(|c| 32 + c.code.len()).sum();
        let receipts: usize = self.delta.receipts.len() * (8 + 256);
        let tx_index: usize = self.delta.receipts.len() * (32 + 8);
        let header = 8 + 20;
        acct + stor + code + receipts + tx_index + header
    }
}

/// The handle returned by [`StateWriter::spawn`]. Drop it to stop the
/// writer thread. This closes the delta sender, and the thread joins on
/// its next loop iteration.
pub struct WriterHandle {
    pub delta_tx: Sender<WriteBatch>,
    pub snapshot_rx: SnapshotReceiver,
    join: Option<JoinHandle<Result<(), StateError>>>,
}

impl WriterHandle {
    /// Stop the writer and wait for its thread to exit. Returns the writer's
    /// final result.
    ///
    /// The writer thread exits when the delta channel closes, so this handle's
    /// sender is replaced with a disconnected one before the join. Any OTHER
    /// live clone of `delta_tx` keeps the thread alive and makes this call
    /// block until that clone drops — callers must drop their adapters first.
    ///
    /// # Errors
    ///
    /// Returns the writer thread's own [`StateError`], if its last `apply`
    /// or snapshot open failed before it exited.
    ///
    /// # Panics
    ///
    /// Panics if the writer thread itself panicked, by propagating that
    /// panic into the caller.
    pub fn shutdown(&mut self) -> Result<(), StateError> {
        let (closed_tx, _) = crossbeam_channel::bounded(0);
        drop(std::mem::replace(&mut self.delta_tx, closed_tx));
        match self.join.take() {
            Some(j) => j.join().expect("writer thread panicked"),
            None => Ok(()),
        }
    }
}

impl Drop for WriterHandle {
    fn drop(&mut self) {
        self.shutdown()
            .unwrap_or_else(|e| error!(message = "Writer didn't shut down properly", err = ?e));
    }
}

/// How the writer maintains the state-root trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrieMode {
    /// No trie. The sequencer-side executor uses this; v0 emits no
    /// state-root commitment.
    Off,
    /// A node-incremental trie. This persists the canonical MPT root per
    /// block.
    Incremental,
    /// Incremental, plus a full-rebuild shadow-check every `every_n`
    /// blocks. This check stops the writer on a mismatch, as a canary
    /// against walker bugs.
    ShadowCheck { every_n: u64 },
}

/// The single-writer state thread. It owns the only read-write mdbx
/// transaction at a time.
pub struct StateWriter {
    env: StateEnv,
    delta_rx: Receiver<WriteBatch>,
    snapshot_handle: SnapshotHandle,
    /// The trie maintenance mode. `Off` for the executor. `Incremental` or
    /// `ShadowCheck` for the validator: each block commit then advances
    /// the canonical Ethereum MPT world-state root (see [`crate::trie`])
    /// inside the same atomic transaction.
    trie_mode: TrieMode,
}

impl StateWriter {
    /// Spawn the plain writer (no state-root trie) on a dedicated OS thread.
    /// This is the sequencer-side executor's backend.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if the schema-version check fails, or if the
    /// writer thread or its initial snapshot cannot be created.
    pub fn spawn(env: StateEnv) -> Result<WriterHandle, StateError> {
        Self::spawn_inner(env, TrieMode::Off)
    }

    /// Spawn the trie-aware writer with the given [`TrieMode`]. Each block
    /// commit then advances the Ethereum MPT state root inside the same
    /// atomic transaction. The validator uses this.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if the schema-version check fails, if
    /// `mode` is `ShadowCheck { every_n: 0 }`, or if the writer thread or
    /// its initial snapshot cannot be created.
    pub fn spawn_with_trie(env: StateEnv, mode: TrieMode) -> Result<WriterHandle, StateError> {
        // Parsed once, at the boundary: `every_n == 0` would silently
        // disable the shadow-check canary instead of running it every
        // block or refusing to start. `TrieMode::ShadowCheck` keeps a
        // plain `u64` field rather than `NonZeroU64` because the
        // validator's CLI constructs it directly from an `Option<u64>`
        // argument (Phase B: parse that argument into `NonZeroU64` and
        // change this field's type to match).
        if let TrieMode::ShadowCheck { every_n: 0 } = mode {
            return Err(StateError::Recovery(
                "TrieMode::ShadowCheck { every_n: 0 } disables the canary instead of running it; \
                 use TrieMode::Incremental to turn the shadow-check off"
                    .into(),
            ));
        }
        Self::spawn_inner(env, mode)
    }

    fn spawn_inner(env: StateEnv, trie_mode: TrieMode) -> Result<WriterHandle, StateError> {
        // This channel is bounded, HORIZON_BLOCKS deep. If the writer falls
        // behind by more than the version horizon, the executor blocks
        // here. At that point, the snapshot the executor holds is about
        // to become invalid anyway, so blocking is the correct fail-fast
        // behavior.
        let (delta_tx, delta_rx) =
            crossbeam_channel::bounded(crate::geometry::HORIZON_BLOCKS as usize);
        let (snapshot_handle, snapshot_rx) = swap_channel();

        // Write the schema-version meta key on first start (and verify it on
        // subsequent starts).
        env.ensure_schema_version()?;

        // Publish an initial snapshot at the current cursors.
        let initial = StateSnapshot::open(&env)?;
        snapshot_handle.publish(initial);

        let writer = StateWriter {
            env,
            delta_rx,
            snapshot_handle: snapshot_handle.clone(),
            trie_mode,
        };

        let join = thread::Builder::new()
            .name("kardamom-state-writer".into())
            .spawn(move || writer.run())?;

        Ok(WriterHandle {
            delta_tx,
            snapshot_rx,
            join: Some(join),
        })
    }

    fn run(self) -> Result<(), StateError> {
        info!(
            path = %self.env.path().display(),
            env_kind = ?self.env.raw().env_kind(),
            "state writer started"
        );
        // `StateEnvBuilder::open` already checked the env kind, so every
        // `StateEnv` this thread can see is one of the two kinds this
        // crate supports.
        loop {
            let Ok(batch) = self.delta_rx.recv() else {
                info!("delta channel closed; writer shutting down");
                return Ok(());
            };
            let block = batch.boundary.block_number;
            let size = batch.approx_size_bytes();
            debug!(block, size_bytes = size, "applying block delta");
            if let Err(e) = self.apply(&batch) {
                // Report this clearly on both channels: tracing for
                // production, and stderr unconditionally. A halted state
                // writer strands every consumer of the snapshot channel.
                // They block instead of erroring, so a silent failure
                // here is hard to find.
                eprintln!("kardamom-state-writer HALTING: block {block} apply failed: {e}");
                error!(block, error = %e, "block apply failed; halting writer");
                return Err(e);
            }
            // Publish the snapshot after this block. `SnapshotHandle::publish`
            // drops the old snapshot, which releases its read-only transaction.
            match StateSnapshot::open(&self.env) {
                Ok(snap) => self.snapshot_handle.publish(snap),
                Err(e) => {
                    eprintln!(
                        "kardamom-state-writer HALTING: snapshot open failed after block {block}: {e}"
                    );
                    warn!(block, error = %e, "snapshot open failed after commit");
                    return Err(e);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod trie_writer_tests;

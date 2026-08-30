//! Production state backend: bridges `kardamom-state`'s libmdbx `StateWriter`
//! and snapshot-swap channel into the executor's `SnapshotSource` /
//! `StateWriterQueue` / `StateWriterSignal` seams (`crate::actor`).
//!
//! The executor's exec thread (`crate::actor::spawn_exec`) drives all three:
//! per block it `submit`s the delta, `wait_committed`s for the writer's durable
//! ack, then opens the post-commit snapshot via `snapshot_after`. These adapters
//! are the thin glue — all storage logic lives in `kardamom-state`.
//!
//! `MdbxSnapshotSource` and `MdbxWriterSignal` each hold a clone of the
//! writer's `SnapshotReceiver`, so they read the same `Arc`-shared
//! latest-snapshot pointer and pull from the same bounded(1) notify channel.
//! That sharing is safe because only the single exec thread calls them, and it
//! does so strictly sequentially (`wait_committed(n)` then `snapshot_after(n)`)
//! — so the non-blocking `current()` peek and the blocking `recv()` never run
//! concurrently and never steal each other's wake-ups.

use crossbeam_channel::Sender;
use kardamom_state::{SnapshotReceiver, StateSnapshot, WriteBatch};
use kardamom_types::{BlockBoundary, BlockDelta, SnapshotSource};

use crate::actor::{StateWriterQueue, StateWriterSignal};
use crate::error::ExecutorError;

/// [`SnapshotSource`] backed by the writer's snapshot-swap channel. Hands the
/// exec thread the latest MVCC snapshot the writer has published.
#[derive(Clone)]
pub struct MdbxSnapshotSource {
    rx: SnapshotReceiver,
}

impl MdbxSnapshotSource {
    pub fn new(rx: SnapshotReceiver) -> Self {
        Self { rx }
    }
}

impl SnapshotSource for MdbxSnapshotSource {
    type Db = StateSnapshot;

    fn snapshot_after(&self, _block_number: u64) -> Self::Db {
        // The exec thread only calls this *after* `wait_committed(n)` returned,
        // and `StateWriter::spawn` publishes an initial snapshot synchronously
        // before the executor starts — so a snapshot is always present and is
        // already anchored at a block >= the requested one. `current()` is the
        // common path; the `recv()` fallback covers only a (logically
        // impossible here) empty channel. If the writer has been torn down the
        // exec thread cannot proceed without a state view, so we panic and let
        // the process crash-loop for the orchestrator to restart.
        self.rx
            .current()
            .or_else(|| self.rx.recv())
            .expect("state writer produced no snapshot")
    }
}

/// [`StateWriterQueue`] that hands each block's delta to the writer thread over
/// the bounded `WriteBatch` channel.
pub struct MdbxWriterQueue {
    delta_tx: Sender<WriteBatch>,
}

impl MdbxWriterQueue {
    pub fn new(delta_tx: Sender<WriteBatch>) -> Self {
        Self { delta_tx }
    }
}

impl StateWriterQueue for MdbxWriterQueue {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        // The channel is bounded (HORIZON_BLOCKS deep); `send` blocks when the
        // writer falls that far behind — the intended fail-fast backpressure.
        // A send error means the writer thread is gone, which is fatal.
        self.delta_tx
            .send(WriteBatch::new(block, delta))
            .map_err(|e| ExecutorError::State(format!("state writer channel closed: {e}")))
    }
}

/// [`StateWriterSignal`] that blocks until the writer has durably committed a
/// block `>= await_at_least`, reading the writer's published snapshots.
pub struct MdbxWriterSignal {
    rx: SnapshotReceiver,
}

impl MdbxWriterSignal {
    pub fn new(rx: SnapshotReceiver) -> Self {
        Self { rx }
    }
}

impl StateWriterSignal for MdbxWriterSignal {
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(self.rx.current().map(|s| s.block_number()).unwrap_or(0))
    }

    fn wait_committed(&mut self, await_at_least: u64) -> Result<u64, ExecutorError> {
        loop {
            // Peek first: the target block may already be published (and its
            // wake-up token already consumed by a prior call), in which case no
            // new `recv()` notification will ever arrive — peeking avoids that
            // deadlock.
            if let Some(s) = self.rx.current()
                && s.block_number() >= await_at_least
            {
                return Ok(s.block_number());
            }
            match self.rx.recv() {
                Some(s) if s.block_number() >= await_at_least => return Ok(s.block_number()),
                Some(_) => continue,
                None => {
                    return Err(ExecutorError::State(
                        "state writer stopped before committing block".into(),
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};
    use kardamom_state::{
        Durability, StateEnvBuilder, StateSnapshot, StateWriter, WriterHandle, read_recovery_point,
    };
    use kardamom_types::{AccountChange, BPosition, StateDatabase};

    /// Open a fresh SafeNoSync env + spawn its writer. Returns the handle and
    /// the temp dir (kept alive by the caller). Tests synchronize on the
    /// snapshot channel via `wait_committed` — never on wall-clock time.
    fn spawn_writer() -> (tempfile::TempDir, WriterHandle) {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        let handle = StateWriter::spawn(env).unwrap();
        (dir, handle)
    }

    fn block_delta(block_number: u64, addr: Address, balance: u64) -> BlockDelta {
        BlockDelta {
            block_number,
            accounts: vec![AccountChange {
                address: addr,
                nonce: block_number,
                balance: U256::from(balance),
                code_hash: B256::ZERO,
            }],
            storage: Vec::new(),
            code: Vec::new(),
            receipts: Vec::new(),
        }
    }

    fn boundary(block_number: u64) -> BlockBoundary {
        BlockBoundary {
            block_number,
            end_tx_idx: BPosition::from_index(block_number),
            l2_timestamp: 1_700_000_000 + block_number,
            l1_origin: 0,
        }
    }

    #[test]
    fn submit_then_wait_then_snapshot_roundtrips() {
        let (_dir, handle) = spawn_writer();
        let mut queue = MdbxWriterQueue::new(handle.delta_tx.clone());
        let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());
        let source = MdbxSnapshotSource::new(handle.snapshot_rx.clone());

        let addr = Address::from([0x42; 20]);
        queue
            .submit(boundary(1), block_delta(1, addr, 999))
            .unwrap();

        assert_eq!(signal.wait_committed(1).unwrap(), 1);

        let snap = source.snapshot_after(1);
        let (nonce, balance, _) = snap.basic(addr).unwrap().unwrap();
        assert_eq!(nonce, 1);
        assert_eq!(balance, U256::from(999u64));
        assert_eq!(snap.block_number(), 1);

        // `shutdown()` joins the writer, which only exits once every delta
        // sender is dropped — so release the queue's clone first. In production
        // the executor task owns the adapters and drops them before the binary
        // calls `writer.shutdown()`, giving the same ordering.
        drop(queue);
    }

    #[test]
    fn wait_committed_returns_ge_requested() {
        let (_dir, mut handle) = spawn_writer();
        let mut queue = MdbxWriterQueue::new(handle.delta_tx.clone());
        let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());

        let addr = Address::from([0x07; 20]);
        queue.submit(boundary(1), block_delta(1, addr, 1)).unwrap();
        queue.submit(boundary(2), block_delta(2, addr, 2)).unwrap();

        // Waiting for an already-surpassed block must not block forever and must
        // report the actual committed block (>= the request).
        assert!(signal.wait_committed(2).unwrap() >= 2);
        assert!(signal.wait_committed(1).unwrap() >= 1);

        drop(queue); // release the delta sender so the writer can exit (see above)
        handle.shutdown().unwrap();
    }

    #[test]
    fn snapshot_source_returns_initial_snapshot_before_any_commit() {
        // `StateWriter::spawn` publishes an initial snapshot synchronously, so
        // the source yields a usable (empty, block 0) view even before the exec
        // thread submits anything.
        let (_dir, mut handle) = spawn_writer();
        let source = MdbxSnapshotSource::new(handle.snapshot_rx.clone());

        let snap = source.snapshot_after(0);
        assert_eq!(snap.block_number(), 0);
        assert_eq!(snap.basic(Address::from([0x01; 20])).unwrap(), None);

        handle.shutdown().unwrap();
    }

    #[test]
    fn state_persists_across_writer_restart() {
        // The core "chain data is now persisted" proof: commit blocks through
        // the adapters, tear the writer down, reopen the *same* env path, and
        // confirm the committed state + cursor are still there.
        let dir = tempfile::tempdir().unwrap();
        let addr = Address::from([0xAB; 20]);

        // First boot: commit blocks 1..=3, then shut down.
        {
            let env = StateEnvBuilder::new(dir.path())
                .durability(Durability::SafeNoSync)
                .open()
                .unwrap();
            let mut handle = StateWriter::spawn(env).unwrap();
            let mut queue = MdbxWriterQueue::new(handle.delta_tx.clone());
            let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());
            for b in 1..=3 {
                queue
                    .submit(boundary(b), block_delta(b, addr, b * 10))
                    .unwrap();
            }
            assert_eq!(signal.wait_committed(3).unwrap(), 3);
            drop(queue); // release the delta sender so the writer can exit (see above)
            handle.shutdown().unwrap();
        }

        // Second boot: reopen the same path; the chain state survived.
        {
            let env = StateEnvBuilder::new(dir.path())
                .durability(Durability::SafeNoSync)
                .open()
                .unwrap();
            let rp = read_recovery_point(&env).unwrap();
            assert_eq!(rp.last_committed_block, 3);

            let snap = StateSnapshot::open(&env).unwrap();
            let (nonce, balance, _) = snap.basic(addr).unwrap().unwrap();
            assert_eq!(nonce, 3);
            assert_eq!(balance, U256::from(30u64));
        }
    }

    #[test]
    fn wait_committed_errors_when_writer_dropped() {
        let (_dir, handle) = spawn_writer();
        let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());

        // Drop the writer without ever committing block 1: closing `delta_tx`
        // ends the writer thread, which drops the snapshot producer and closes
        // the notify channel. `wait_committed` then sees the initial block-0
        // snapshot (< 1), blocks on `recv`, gets `None`, and surfaces an error
        // rather than hanging.
        drop(handle);

        let err = signal.wait_committed(1).unwrap_err();
        assert!(matches!(err, ExecutorError::State(_)));
    }
}

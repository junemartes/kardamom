//! Production state backend. It bridges `kardamom-state`'s libmdbx
//! `StateWriter` and its snapshot-swap channel to the executor's
//! `SnapshotSource`, `StateWriterQueue`, and `StateWriterSignal` seams
//! (`crate::actor`).
//!
//! The executor's exec thread (`crate::actor::spawn_exec`) drives all three.
//! For each block, it calls `submit` for the delta, calls `wait_committed` for
//! the writer's durable ack, then opens the post-commit snapshot with
//! `snapshot_after`. These adapters are thin glue. All storage logic lives in
//! `kardamom-state`.
//!
//! `MdbxSnapshotSource` and `MdbxWriterSignal` each hold a clone of the
//! writer's `SnapshotReceiver`. So they read the same `Arc`-shared
//! latest-snapshot pointer, and pull from the same bounded(1) notify channel.
//! This sharing is safe: only the exec thread calls them, and always in the
//! same order (`wait_committed(n)` then `snapshot_after(n)`). So the
//! non-blocking `current()` peek and the blocking `recv()` never run at the
//! same time, and never steal each other's wake-ups.

use crossbeam_channel::Sender;
use kardamom_state::{SnapshotReceiver, StateSnapshot, WriteBatch};
use kardamom_types::{BlockBoundary, BlockDelta, SnapshotSource};

use crate::actor::{StateWriterQueue, StateWriterSignal};
use crate::error::ExecutorError;

/// [`SnapshotSource`] backed by the writer's snapshot-swap channel. It gives
/// the exec thread the latest MVCC snapshot the writer has published.
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
        // The exec thread calls this only after `wait_committed(n)` returns. Also,
        // `StateWriter::spawn` publishes an initial snapshot before the executor
        // starts. So a snapshot is always present, already at a block >= the one
        // requested. `current()` is the common path. The `recv()` fallback only
        // covers an empty channel, which cannot happen here. If the writer has
        // shut down, the exec thread cannot proceed without a state view. So this
        // panics, and the orchestrator restarts the crashed process.
        self.rx
            .current()
            .or_else(|| self.rx.recv())
            .expect("state writer produced no snapshot")
    }
}

/// [`StateWriterQueue`] that sends each block's delta to the writer thread
/// over the bounded `WriteBatch` channel.
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
        // The channel is bounded, HORIZON_BLOCKS deep. `send` blocks when the
        // writer falls that far behind. This is the intended fail-fast
        // backpressure. A send error means the writer thread is gone. This is
        // fatal.
        self.delta_tx
            .send(WriteBatch::new(block, delta))
            .map_err(|e| ExecutorError::State(format!("state writer channel closed: {e}")))
    }
}

/// [`StateWriterSignal`] that blocks until the writer commits a block
/// `>= await_at_least`. It reads the writer's published snapshots.
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
            // Peek first. The target block may already be published, with its
            // wake-up token already used by a prior call. Then no new `recv()`
            // notification will ever arrive. Peeking avoids this deadlock.
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

    /// Open a fresh SafeNoSync env and spawn its writer. Return the handle and
    /// the temp dir; the caller must keep the dir alive. Tests sync on the
    /// snapshot channel with `wait_committed`, never on wall-clock time.
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

        // `shutdown()` joins the writer. The writer exits only after every
        // delta sender drops. So release the queue's clone first. In
        // production, the executor task owns the adapters and drops them
        // before the binary calls `writer.shutdown()`. This gives the same
        // order.
        drop(queue);
        handle.shutdown().unwrap();
    }

    #[test]
    fn wait_committed_returns_ge_requested() {
        let (_dir, handle) = spawn_writer();
        let mut queue = MdbxWriterQueue::new(handle.delta_tx.clone());
        let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());

        let addr = Address::from([0x07; 20]);
        queue.submit(boundary(1), block_delta(1, addr, 1)).unwrap();
        queue.submit(boundary(2), block_delta(2, addr, 2)).unwrap();

        // Waiting for an already-passed block must not block forever. It must
        // report the actual committed block, which is >= the request.
        assert!(signal.wait_committed(2).unwrap() >= 2);
        assert!(signal.wait_committed(1).unwrap() >= 1);

        drop(queue); // release the delta sender to let the writer exit (see above)
        handle.shutdown().unwrap();
    }

    #[test]
    fn snapshot_source_returns_initial_snapshot_before_any_commit() {
        // `StateWriter::spawn` publishes an initial snapshot right away. So
        // the source gives a usable, empty, block-0 view before the exec
        // thread submits anything.
        let (_dir, handle) = spawn_writer();
        let source = MdbxSnapshotSource::new(handle.snapshot_rx.clone());

        let snap = source.snapshot_after(0);
        assert_eq!(snap.block_number(), 0);
        assert_eq!(snap.basic(Address::from([0x01; 20])).unwrap(), None);

        handle.shutdown().unwrap();
    }

    #[test]
    fn state_persists_across_writer_restart() {
        // This is the main proof that chain data persists. Commit blocks
        // through the adapters, tear down the writer, reopen the same env
        // path, and check that the committed state and cursor are still
        // there.
        let dir = tempfile::tempdir().unwrap();
        let addr = Address::from([0xAB; 20]);

        // First boot: commit blocks 1..=3, then shut down.
        {
            let env = StateEnvBuilder::new(dir.path())
                .durability(Durability::SafeNoSync)
                .open()
                .unwrap();
            let handle = StateWriter::spawn(env).unwrap();
            let mut queue = MdbxWriterQueue::new(handle.delta_tx.clone());
            let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());
            for b in 1..=3 {
                queue
                    .submit(boundary(b), block_delta(b, addr, b * 10))
                    .unwrap();
            }
            assert_eq!(signal.wait_committed(3).unwrap(), 3);
            drop(queue); // release the delta sender to let the writer exit (see above)
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

        // Drop the writer without committing block 1. Closing `delta_tx` ends
        // the writer thread. This drops the snapshot producer and closes the
        // notify channel. `wait_committed` then sees the initial block-0
        // snapshot, which is < 1. It blocks on `recv`, gets `None`, and
        // returns an error instead of hanging.
        drop(handle);

        let err = signal.wait_committed(1).unwrap_err();
        assert!(matches!(err, ExecutorError::State(_)));
    }
}

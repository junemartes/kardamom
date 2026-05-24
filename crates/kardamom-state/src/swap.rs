//! Snapshot-swap protocol (§5).
//!
//! The writer publishes a fresh `StateSnapshot` after every successful RW
//! commit. The executor watches the channel and swaps its underlying snapshot
//! to the new one. Old snapshots are dropped, which releases their mdbx RO
//! txn and lets the freelist reclaim the corresponding pages.
//!
//! Implementation: zero async, single-producer/single-consumer. We keep the
//! latest snapshot behind a `Mutex<Option<_>>` and use a length-1
//! crossbeam_channel to wake the consumer.

use std::sync::{Arc, Mutex};

use crate::snapshot::StateSnapshot;

/// Producer side. The writer calls `publish(snapshot)` after every commit.
#[derive(Clone)]
pub struct SnapshotHandle {
    latest: Arc<Mutex<Option<StateSnapshot>>>,
    notify: crossbeam_channel::Sender<()>,
}

/// Consumer side. The executor calls `recv()` to block on the next snapshot,
/// or `current()` to peek without blocking.
#[derive(Clone)]
pub struct SnapshotReceiver {
    latest: Arc<Mutex<Option<StateSnapshot>>>,
    notify: crossbeam_channel::Receiver<()>,
}

/// Create a fresh swap channel. Returns the producer + consumer ends.
pub fn channel() -> (SnapshotHandle, SnapshotReceiver) {
    let latest = Arc::new(Mutex::new(None));
    let (tx, rx) = crossbeam_channel::bounded(1);
    (
        SnapshotHandle {
            latest: latest.clone(),
            notify: tx,
        },
        SnapshotReceiver { latest, notify: rx },
    )
}

impl SnapshotHandle {
    /// Replace the latest snapshot. Drops any prior unconsumed snapshot,
    /// which releases its mdbx RO txn — exactly the desired behavior since
    /// the consumer only ever needs the freshest one.
    pub fn publish(&self, snapshot: StateSnapshot) {
        *self.latest.lock().expect("snapshot mutex poisoned") = Some(snapshot);
        // try_send: if the slot is full, the receiver has not consumed yet —
        // the latest-pointer update above is sufficient.
        let _ = self.notify.try_send(());
    }
}

impl SnapshotReceiver {
    /// Non-blocking peek at the most recently published snapshot.
    pub fn current(&self) -> Option<StateSnapshot> {
        self.latest.lock().expect("snapshot mutex poisoned").clone()
    }

    /// Blocks until a new snapshot is published, then returns it. Returns
    /// `None` if the writer has been dropped.
    pub fn recv(&self) -> Option<StateSnapshot> {
        self.notify.recv().ok()?;
        self.current()
    }
}

#[cfg(test)]
mod tests {
    // Real swap behavior is tested in tests/snapshot_swap.rs (needs a live env).
    // Here we only test that the channel mechanics don't deadlock.
    use super::*;

    #[test]
    fn drop_writer_closes_recv() {
        let (handle, recv) = channel();
        drop(handle);
        assert!(recv.recv().is_none());
    }

    #[test]
    fn current_without_publish_is_none() {
        let (_handle, recv) = channel();
        assert!(recv.current().is_none());
    }
}

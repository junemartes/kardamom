//! Snapshot-swap protocol (spec section 5).
//!
//! The writer publishes a fresh `StateSnapshot` after every successful
//! read-write commit. The executor watches the channel and swaps in the new
//! snapshot. Dropping an old snapshot releases its mdbx read-only
//! transaction and lets the freelist reclaim its pages.
//!
//! This is a single-producer, single-consumer design with no async. We keep
//! the latest snapshot behind a `Mutex<Option<_>>` and use a length-1
//! crossbeam channel to wake the consumer.

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

/// Create a fresh swap channel. Returns the producer and consumer ends.
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
    /// Replace the latest snapshot. This drops any unconsumed prior
    /// snapshot and releases its mdbx read-only transaction. This is the
    /// desired behavior: the consumer only needs the freshest snapshot.
    pub fn publish(&self, snapshot: StateSnapshot) {
        *self.latest.lock().expect("snapshot mutex poisoned") = Some(snapshot);
        // Use try_send. If the slot is full, the receiver has not consumed
        // the last notification yet. The latest-pointer update above is enough.
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
    // tests/snapshot_swap.rs tests the real swap behavior; it needs a live
    // env. Here we only test that the channel mechanics do not deadlock.
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

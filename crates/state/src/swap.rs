//! Snapshot-swap protocol (spec section 5).
//!
//! The writer publishes a fresh `StateSnapshot` after every successful
//! read-write commit. The executor watches the channel and swaps in the new
//! snapshot. Dropping an old snapshot releases its mdbx read-only
//! transaction and lets the freelist reclaim its pages.
//!
//! Implementation: the sync path uses no async. It is a single-producer,
//! single-consumer design, with both ends on plain threads. This is a
//! "latest value wins" slot. No crossbeam channel gives this; they are
//! FIFO queues. So the newest snapshot lives in an `ArcSwapOption`. This
//! gives a lock-free load and one atomic swap to publish. A length-1
//! crossbeam channel wakes the sync consumer, the exec thread. A pending
//! wake coalesces with the next one.
//!
//! An async half mirrors every publish into a `tokio::sync::watch` slot.
//! Async consumers, such as the prover spool and the commit poller, park
//! on `changed()` instead of polling `current()` on a timer.
//! `watch::Sender` needs no runtime to send. So the writer thread stays
//! runtime-free.

use std::sync::Arc;

use arc_swap::ArcSwapOption;

use crate::snapshot::StateSnapshot;

/// Producer side. The writer calls `publish(snapshot)` after every commit.
#[derive(Clone)]
pub struct SnapshotHandle {
    latest: Arc<ArcSwapOption<StateSnapshot>>,
    notify: crossbeam_channel::Sender<()>,
    watch: tokio::sync::watch::Sender<Option<StateSnapshot>>,
}

/// Consumer side. The executor calls `recv()` to block on the next snapshot,
/// or `current()` to peek without blocking.
#[derive(Clone)]
pub struct SnapshotReceiver {
    latest: Arc<ArcSwapOption<StateSnapshot>>,
    notify: crossbeam_channel::Receiver<()>,
    watch: tokio::sync::watch::Receiver<Option<StateSnapshot>>,
}

/// Create a fresh swap channel. Returns the producer and consumer ends.
pub fn channel() -> (SnapshotHandle, SnapshotReceiver) {
    let latest = Arc::new(ArcSwapOption::empty());
    let (tx, rx) = crossbeam_channel::bounded(1);
    let (wtx, wrx) = tokio::sync::watch::channel(None);
    (
        SnapshotHandle {
            latest: latest.clone(),
            notify: tx,
            watch: wtx,
        },
        SnapshotReceiver {
            latest,
            notify: rx,
            watch: wrx,
        },
    )
}

impl SnapshotHandle {
    /// Replace the latest snapshot. This drops any unconsumed prior
    /// snapshot and releases its mdbx read-only transaction. This is the
    /// desired behavior: the consumer only needs the freshest snapshot.
    pub fn publish(&self, snapshot: StateSnapshot) {
        // The watch slot gets a clone. Clones share the inner Arc, so both
        // slots pin one mdbx RO txn. `send_replace` drops the prior value,
        // even with zero receivers.
        self.watch.send_replace(Some(snapshot.clone()));
        // `store` drops the previous value once no `load` guard holds it.
        self.latest.store(Some(Arc::new(snapshot)));
        // Use try_send. If the slot is full, the receiver has not consumed
        // the last notification yet. The latest-pointer update above is enough.
        let _ = self.notify.try_send(());
    }
}

impl SnapshotReceiver {
    /// Non-blocking peek at the most recently published snapshot.
    pub fn current(&self) -> Option<StateSnapshot> {
        // `StateSnapshot` is itself an `Arc` handle, so this clone is one
        // refcount bump; the load guard is released before returning.
        self.latest.load().as_deref().cloned()
    }

    /// Blocks until a new snapshot is published, then returns it. Returns
    /// `None` if the writer has been dropped.
    pub fn recv(&self) -> Option<StateSnapshot> {
        self.notify.recv().ok()?;
        self.current()
    }

    /// The async half: a `watch` receiver over the same publishes. Async
    /// consumers park on `changed().await` and read with
    /// `borrow_and_update()` — no timer, no missed-publish dedup. The
    /// slot is latest-wins, exactly like `current()`.
    pub fn watch(&self) -> tokio::sync::watch::Receiver<Option<StateSnapshot>> {
        self.watch.clone()
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

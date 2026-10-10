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
//! FIFO queues. So the newest snapshot lives in a `tokio::sync::watch`
//! slot, and `current()` reads it through `borrow()`. This trades a
//! lock-free load for one read lock per peek, in exchange for one slot
//! instead of two. A length-1 crossbeam channel wakes the sync consumer,
//! the exec thread. A pending wake coalesces with the next one.
//!
//! Async consumers, such as the prover spool and the commit poller, park
//! on `changed()` instead of polling `current()` on a timer. `watch::Sender`
//! needs no runtime to send. So the writer thread stays runtime-free.

use crate::snapshot::StateSnapshot;

/// Producer side. The writer calls `publish(snapshot)` after every commit.
#[derive(Clone)]
pub struct SnapshotHandle {
    notify: crossbeam_channel::Sender<()>,
    watch: tokio::sync::watch::Sender<Option<StateSnapshot>>,
}

/// Consumer side. The executor calls `recv()` to block on the next snapshot,
/// or `current()` to peek without blocking.
#[derive(Clone)]
pub struct SnapshotReceiver {
    notify: crossbeam_channel::Receiver<()>,
    watch: tokio::sync::watch::Receiver<Option<StateSnapshot>>,
}

/// Create a fresh swap channel. Returns the producer and consumer ends.
#[must_use]
pub fn channel() -> (SnapshotHandle, SnapshotReceiver) {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let (wtx, wrx) = tokio::sync::watch::channel(None);
    (
        SnapshotHandle {
            notify: tx,
            watch: wtx,
        },
        SnapshotReceiver {
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
        // `send_replace` drops the prior value, even with zero receivers.
        self.watch.send_replace(Some(snapshot));
        // Use try_send. If the slot is full, the receiver has not consumed
        // the last notification yet. The watch update above is enough.
        let _ = self.notify.try_send(());
    }
}

impl SnapshotReceiver {
    /// Non-blocking peek at the most recently published snapshot.
    #[must_use]
    pub fn current(&self) -> Option<StateSnapshot> {
        // `StateSnapshot` is itself an `Arc` handle, so this clone is one
        // refcount bump; the read lock is released before returning.
        self.watch.borrow().clone()
    }

    /// Blocks until a new snapshot is published, then returns it. Returns
    /// `None` if the writer has been dropped.
    ///
    /// The slot is latest-wins. One wake covers every publish since the
    /// previous `recv`, so the count of wakes is not the count of
    /// commits. A consumer that needs block `n` reads
    /// [`StateSnapshot::block_number`] and calls `recv` again until it is
    /// `n` or more.
    #[must_use]
    pub fn recv(&self) -> Option<StateSnapshot> {
        self.notify.recv().ok()?;
        self.current()
    }

    /// [`Self::recv`] with a bound. Returns `None` if the writer has been
    /// dropped, or if no publish arrives before `deadline`.
    #[must_use]
    pub fn recv_deadline(&self, deadline: std::time::Instant) -> Option<StateSnapshot> {
        self.notify.recv_deadline(deadline).ok()?;
        self.current()
    }

    /// The async half: a `watch` receiver over the same publishes. Async
    /// consumers park on `changed().await` and read with
    /// `borrow_and_update()` — no timer, no missed-publish dedup. The
    /// slot is latest-wins, exactly like `current()`.
    #[must_use]
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

    #[test]
    fn recv_deadline_without_publish_returns_none_at_deadline() {
        let (_handle, recv) = channel();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(50);
        assert!(recv.recv_deadline(deadline).is_none());
        assert!(std::time::Instant::now() >= deadline);
    }
}

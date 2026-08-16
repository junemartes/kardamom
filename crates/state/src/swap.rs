//! Snapshot-swap protocol (§5).
//!
//! The writer publishes a fresh `StateSnapshot` after every successful RW
//! commit. The executor watches the channel and swaps its underlying snapshot
//! to the new one. Old snapshots are dropped, which releases their mdbx RO
//! txn and lets the freelist reclaim the corresponding pages.
//!
//! Implementation: zero async. The latest snapshot lives behind a
//! `Mutex<Option<_>>`, and each CONSUMER gets its own length-1
//! crossbeam_channel to wake on.
//!
//! Per-consumer wakeups are load-bearing, not incidental. A single shared
//! channel is MPMC: a token taken by one consumer is invisible to every
//! other, and with a length-1 channel there is only ever one token to take.
//! Two blocking consumers on one channel therefore STEAL each other's
//! wakeups — each `recv()` that loses the race waits for the *next* publish
//! and falls a commit further behind, which under sustained load looks like
//! a consumer that has simply stopped following the chain. Giving every
//! [`SnapshotReceiver`] its own channel (see its `Clone`) makes an extra
//! consumer harmless.

use std::sync::{Arc, Mutex};

use crate::snapshot::StateSnapshot;

/// Shared state behind the handle and every receiver.
struct Inner {
    latest: Mutex<Option<StateSnapshot>>,
    /// One sender per live consumer. Publishing notifies all of them; a
    /// disconnected sender (its receiver dropped) is pruned in passing.
    subscribers: Mutex<Vec<crossbeam_channel::Sender<()>>>,
}

impl Inner {
    fn subscribe(self: &Arc<Self>) -> SnapshotReceiver {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.subscribers
            .lock()
            .expect("snapshot subscribers poisoned")
            .push(tx);
        SnapshotReceiver {
            inner: self.clone(),
            notify: rx,
        }
    }
}

/// Closes every consumer's channel when the last [`SnapshotHandle`] goes.
///
/// The senders live in the shared `Inner` so `publish` can reach them all,
/// which means dropping a handle does NOT by itself drop any sender. Without
/// this guard a consumer blocked in `recv()` would wait forever instead of
/// observing that the writer is gone — the shutdown path depends on that
/// `None` (the validator joins its snapshot feeder on it).
struct ProducerGuard {
    inner: Arc<Inner>,
}

impl Drop for ProducerGuard {
    fn drop(&mut self) {
        self.inner
            .subscribers
            .lock()
            .expect("snapshot subscribers poisoned")
            .clear();
    }
}

/// Producer side. The writer calls `publish(snapshot)` after every commit.
#[derive(Clone)]
pub struct SnapshotHandle {
    inner: Arc<Inner>,
    /// Shared across clones: the last handle to drop closes the consumers.
    _guard: Arc<ProducerGuard>,
}

/// Consumer side. The executor calls `recv()` to block on the next snapshot,
/// or `current()` to peek without blocking.
///
/// Cloning yields an INDEPENDENT consumer with its own wakeup stream, so two
/// clones can both block in `recv()` without stealing from one another.
pub struct SnapshotReceiver {
    inner: Arc<Inner>,
    notify: crossbeam_channel::Receiver<()>,
}

impl Clone for SnapshotReceiver {
    fn clone(&self) -> Self {
        self.inner.subscribe()
    }
}

/// Create a fresh swap channel. Returns the producer + consumer ends.
pub fn channel() -> (SnapshotHandle, SnapshotReceiver) {
    let inner = Arc::new(Inner {
        latest: Mutex::new(None),
        subscribers: Mutex::new(Vec::new()),
    });
    let rx = inner.subscribe();
    (
        SnapshotHandle {
            inner: inner.clone(),
            _guard: Arc::new(ProducerGuard { inner }),
        },
        rx,
    )
}

impl SnapshotHandle {
    /// Replace the latest snapshot. Drops any prior unconsumed snapshot,
    /// which releases its mdbx RO txn — exactly the desired behavior since
    /// consumers only ever need the freshest one.
    pub fn publish(&self, snapshot: StateSnapshot) {
        *self.inner.latest.lock().expect("snapshot mutex poisoned") = Some(snapshot);
        self.notify_all();
    }

    /// Wake every live consumer.
    ///
    /// try_send per consumer: a full slot means that consumer has not drained
    /// its previous wakeup yet, and the latest-pointer update is sufficient
    /// for it. Senders whose receiver is gone are pruned in passing rather
    /// than accumulating.
    fn notify_all(&self) {
        self.inner
            .subscribers
            .lock()
            .expect("snapshot subscribers poisoned")
            .retain(|tx| {
                !matches!(
                    tx.try_send(()),
                    Err(crossbeam_channel::TrySendError::Disconnected(_))
                )
            });
    }

    /// Fan out a wakeup with no snapshot attached, so the notification path
    /// can be tested without a live mdbx env.
    #[cfg(test)]
    fn notify_for_test(&self) {
        self.notify_all();
    }
}

impl SnapshotReceiver {
    /// Non-blocking peek at the most recently published snapshot.
    pub fn current(&self) -> Option<StateSnapshot> {
        self.inner
            .latest
            .lock()
            .expect("snapshot mutex poisoned")
            .clone()
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

    /// Two blocking consumers must BOTH be woken by a publish.
    ///
    /// With one shared length-1 channel this deadlocks: the first `recv()`
    /// takes the only token and the second waits forever for a wakeup that
    /// was already consumed. That is what starved the validator's
    /// commit-wait once the snapshot feeder became a second blocking
    /// consumer — it showed up as a validator stuck thousands of blocks
    /// behind the executor under load.
    #[test]
    fn two_consumers_do_not_steal_each_others_wakeups() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (handle, a) = channel();
        let b = a.clone();
        let (done_tx, done_rx) = mpsc::channel();

        for (name, rx) in [("a", a), ("b", b)] {
            let done_tx = done_tx.clone();
            std::thread::spawn(move || {
                // `None` (writer dropped) also unblocks; the test asserts on
                // the count of threads that woke at all.
                let _ = rx.recv();
                let _ = done_tx.send(name);
            });
        }
        drop(done_tx);
        // Give both threads time to park before publishing.
        std::thread::sleep(Duration::from_millis(50));
        handle.notify_for_test();

        let mut woken = 0;
        while done_rx.recv_timeout(Duration::from_secs(5)).is_ok() {
            woken += 1;
            if woken == 2 {
                break;
            }
        }
        assert_eq!(woken, 2, "both consumers must be woken by one publish");
    }
}

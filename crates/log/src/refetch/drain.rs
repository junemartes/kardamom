//! The drain of one bounded replay: a blocking receive with an idle
//! timeout, on a std thread with no tokio runtime.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use super::{ArchiveRefetcher, DRAIN_CAP, DRAIN_IDLE};
use crate::aeron_live::PollRecv;

impl ArchiveRefetcher {
    /// Drain a replay subscription: deliver until [`DRAIN_IDLE`] of
    /// silence after the last fragment (bounded replay exhausted) or the
    /// [`DRAIN_CAP`].
    ///
    /// The refetcher runs on the `tx_ordering` reader thread — a plain
    /// std thread with no tokio runtime entered — so each wait is a
    /// blocking receive with an idle timeout via [`recv_timeout`]: the
    /// thread parks on the channel and wakes on the next fragment. No
    /// `try_recv` + sleep busy loop, and no runtime of its own.
    pub(super) fn drain<S: PollRecv>(rx: &mut S, mut deliver: impl FnMut(S::Item)) -> u64 {
        let deadline = Instant::now() + DRAIN_CAP;
        let mut delivered = 0u64;
        loop {
            let ControlFlow::Continue(item) = drain_step(rx, deadline) else {
                return delivered;
            };
            deliver(item);
            delivered += 1;
        }
    }
}

/// One [`ArchiveRefetcher::drain`] step: `Break` means the idle budget ran out
/// or the source ended (replay exhausted, or the runtime is gone); either
/// way the caller stops. `Continue` carries one item the caller delivers
/// and counts.
fn drain_step<S: PollRecv>(rx: &mut S, deadline: Instant) -> ControlFlow<(), S::Item> {
    let budget = DRAIN_IDLE.min(deadline.saturating_duration_since(Instant::now()));
    if budget.is_zero() {
        return ControlFlow::Break(());
    }
    match recv_timeout(rx, budget) {
        Ok(Some(item)) => ControlFlow::Continue(item),
        // Idle timeout (replay exhausted) or channel closed (runtime gone).
        Err(RecvTimeout) | Ok(None) => ControlFlow::Break(()),
    }
}

/// The wait in [`recv_timeout`] elapsed with nothing received.
struct RecvTimeout;

/// Blocking receive with a timeout on a [`PollRecv`] subscription from a
/// thread that is NOT inside a tokio runtime. Both `PollRecv`
/// implementations wrap a tokio `UnboundedReceiver`, which only offers
/// `blocking_recv` (no timeout) and async `recv` (needs a timer for
/// `timeout`), so this drives `poll_recv` by hand with a waker that unparks
/// the calling thread: park until woken or the deadline, re-poll, repeat.
/// Spurious unparks just cause an extra poll.
///
/// `Ok(None)` means the channel closed; `Err(RecvTimeout)` means the deadline
/// passed.
fn recv_timeout<S: PollRecv>(
    rx: &mut S,
    timeout: Duration,
) -> Result<Option<S::Item>, RecvTimeout> {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    /// One poll-and-maybe-park step. `Break` carries [`recv_timeout`]'s
    /// answer: a ready item, or a timeout past `deadline`. `Continue`
    /// means the poll was pending and this thread parked until either
    /// woken or `deadline`; the caller polls again.
    fn step<S: PollRecv>(
        rx: &mut S,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> ControlFlow<Result<Option<S::Item>, RecvTimeout>> {
        if let Poll::Ready(item) = rx.poll_recv(cx) {
            return ControlFlow::Break(Ok(item));
        }
        let now = Instant::now();
        if now >= deadline {
            return ControlFlow::Break(Err(RecvTimeout));
        }
        std::thread::park_timeout(deadline - now);
        ControlFlow::Continue(())
    }

    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let deadline = Instant::now() + timeout;
    loop {
        if let ControlFlow::Break(result) = step(rx, &mut cx, deadline) {
            return result;
        }
    }
}

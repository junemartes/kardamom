//! The parked-publish retry scheduler for the Aeron thread, plus the
//! [`IdleBackoff`] cadence helper: one offer attempt per pending frame per
//! loop iteration, per-publication FIFO, deadline-bounded. The pure core
//! (`drain_pending_inner`) is unit-tested here without a media driver.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender as CbSender;
use rkyv::util::AlignedVec;
use tracing::warn;

use super::Pub;
use super::thread::decode_position;
use crate::error::LogError;
use crate::offer_retry::offer_code_str;
use kardamom_types::BPosition;

/// Escalating idle wait. This is the Rust analogue of Aeron's
/// `BackoffIdleStrategy`, which is what keeps the Java sealer stack's
/// duty-cycle threads cheap.
///
/// Poll loops here wait on a channel with a short timeout (the data-plane
/// poll cadence). Profiling showed that cadence dominating the sequencer's
/// CPU: three Aeron runtimes per process waking every 100 microseconds
/// cost about 66% of the process's cycles at 2k tx/s, almost all of it
/// crossbeam's pre-park spin (`sched_yield` storms), not work. The fix
/// mirrors the sealer's: stay at the base cadence while the loop is doing
/// something, and once it has seen `grace` consecutive empty iterations,
/// double the wait per iteration up to `cap`. Any work snaps it back to base.
///
/// Latency safety: senders wake the channel wait immediately regardless of
/// the timeout, so command/publish latency stays unaffected. Only the
/// first inbound data-plane fragment after a quiet spell can wait up to
/// `cap`. Under steady traffic, every poll finds fragments, and the
/// cadence never leaves base.
pub struct IdleBackoff {
    base: Duration,
    cap: Duration,
    grace: u32,
    streak: u32,
}

impl IdleBackoff {
    pub fn new(base: Duration, cap: Duration, grace: u32) -> Self {
        Self {
            base,
            cap,
            grace,
            streak: 0,
        }
    }

    /// The loop did work this iteration. Snap back to the base cadence.
    pub fn reset(&mut self) {
        self.streak = 0;
    }

    /// Record an empty iteration and return the wait to use for it.
    pub fn idle_wait(&mut self) -> Duration {
        self.streak = self.streak.saturating_add(1);
        if self.streak <= self.grace {
            return self.base;
        }
        let doublings = (self.streak - self.grace).min(10);
        self.base.saturating_mul(1u32 << doublings).min(self.cap)
    }
}

/// A publish awaiting delivery on the Aeron thread.
///
/// Why this queue exists: the Aeron thread is single-threaded and shared
/// by every publication and subscription in a process. If a publish is
/// offered in a blocking spin/sleep loop (the old `offer_blocking`, up to
/// [`OFFER_TIMEOUT`](crate::offer_retry::OFFER_TIMEOUT)), that same thread
/// stops polling its subscriptions for the whole back-pressure window. In
/// the cluster that starves the executor's `tx_ordering` subscription long
/// enough (more than Aeron's minimum flow-control receiver timeout, about
/// 2 s) that the sealer drops it from flow control and advances. The
/// subscription's image then develops an unfillable gap and goes
/// end-of-stream: a permanent freeze, since the executor uses
/// `no_unavailable_image_handler` and never re-subscribes. A must-deliver
/// publish must never starve a must-deliver subscribe.
///
/// So a back-pressured offer is parked here and retried one attempt per
/// loop iteration instead of blocking. The poll loop keeps draining
/// subscriptions between attempts. Per-publication FIFO order is
/// preserved: a publication with an older pending frame is not skipped
/// ahead.
pub(super) struct PendingPublish {
    pub(super) pub_id: u32,
    pub(super) bytes: AlignedVec,
    /// `Some` for an acknowledged publish (`publish_bytes`); `None` for
    /// best effort.
    pub(super) ack: Option<CbSender<Result<BPosition, LogError>>>,
    /// Give up (ack an error, or log) once this instant passes. Bounds a
    /// publish to a never-connecting subscriber, matching the old blocking
    /// deadline.
    pub(super) deadline: Instant,
}

/// Attempt one offer for each pending publish, oldest first, preserving
/// per-publication FIFO order. Once a publication back-pressures this
/// pass, its later frames are held too, so the stream never reorders.
/// Successful and expired (past-deadline) entries are removed;
/// transiently failing entries are retained for the next loop iteration.
///
/// This performs one offer attempt per entry and returns. It never spins
/// or sleeps, so the caller (`super::thread::run_aeron_thread`) goes
/// straight back to polling subscriptions. That is what stops a
/// back-pressured publish from starving a subscription image (see
/// [`PendingPublish`]).
pub(super) fn drain_pending(pubs: &[Pub], pending: &mut VecDeque<PendingPublish>) {
    drain_pending_inner(pending, Instant::now(), |item| {
        match pubs.get(item.pub_id as usize) {
            None => OfferResult::UnknownPub,
            Some(p) => OfferResult::Code(p.offer(
                item.bytes.as_slice(),
                rusteron_client::Handlers::no_reserved_value_supplier_handler(),
            )),
        }
    })
}

/// Outcome of attempting one offer for a [`PendingPublish`].
enum OfferResult {
    /// The publication id is not registered (a programming error or a
    /// use-after-close).
    UnknownPub,
    /// Aeron's raw offer return: `>= 0` is a stream position, `< 0` is a
    /// status code.
    Code(i64),
}

/// Resolve a failed pending publish: ack `msg` as the error for a
/// must-deliver publish, or warn (tagged with the `pub_id`) for best
/// effort. The three failure paths in [`drain_pending_inner`] (expired in
/// queue, unknown publication, offer deadline) all funnel through here, so
/// the ack-or-warn split cannot drift between them.
fn fail_item(item: &mut PendingPublish, msg: String) {
    match item.ack.take() {
        Some(ack) => {
            let _ = ack.send(Err(LogError::Aeron(msg)));
        }
        None => warn!(pub_id = item.pub_id, "best-effort publish failed: {msg}"),
    }
}

/// Pure core of [`drain_pending`], with the Aeron offer injected so the
/// FIFO, deadline, and back-pressure decisions are unit-testable without a
/// media driver. `now` is threaded in for the same reason (deterministic
/// deadline checks).
fn drain_pending_inner<F>(pending: &mut VecDeque<PendingPublish>, now: Instant, mut offer: F)
where
    F: FnMut(&PendingPublish) -> OfferResult,
{
    if pending.is_empty() {
        return;
    }
    // Publications that already back-pressured this pass. Their remaining
    // frames wait, so a stream is never delivered out of order.
    let mut blocked: Vec<u32> = Vec::new();
    let mut keep: VecDeque<PendingPublish> = VecDeque::with_capacity(pending.len());

    while let Some(mut item) = pending.pop_front() {
        if blocked.contains(&item.pub_id) {
            // Deadlines are enforced on every retained entry each pass, not
            // only when an entry reaches the head. Otherwise frames parked
            // behind a blocked head would expire one by one (about
            // OFFER_TIMEOUT each), so a caller two or more deep could hit
            // its ack timeout while its frame was still queued, and then
            // have the frame delivered late once the subscriber connected,
            // after the caller already treated the publish as failed (and
            // possibly resubmitted). Expiring on time here (OFFER_TIMEOUT
            // is shorter than publish_bytes's ack timeout) means every ack
            // resolves before its caller gives up, so a reported-failed
            // publish is never delivered afterwards.
            if now >= item.deadline {
                fail_item(
                    &mut item,
                    "aeron offer failed: expired while queued behind a blocked publication"
                        .to_string(),
                );
            } else {
                keep.push_back(item);
            }
            continue;
        }
        match offer(&item) {
            OfferResult::UnknownPub => {
                // Fail or log immediately; never retry.
                let msg = format!("publish: unknown pub_id {}", item.pub_id);
                fail_item(&mut item, msg);
            }
            OfferResult::Code(code) if code >= 0 => {
                // Delivered. Ack the stream position; best effort needs no
                // ack. Do not block this pub_id: a later frame for it may
                // also go now.
                if let Some(ack) = item.ack.take() {
                    let _ = ack.send(Ok(decode_position(code)));
                }
            }
            OfferResult::Code(code) if now >= item.deadline => {
                // Gave up, for example on a subscriber that never joined.
                // Surface the error, so an acknowledged must-deliver caller
                // can decide to resubmit.
                let msg = format!("aeron offer failed: {} ({code})", offer_code_str(code));
                fail_item(&mut item, msg);
            }
            OfferResult::Code(_) => {
                // Transient NOT_CONNECTED or BACK_PRESSURED: hold this
                // frame and every later frame on the same publication;
                // retry next iteration.
                blocked.push(item.pub_id);
                keep.push_back(item);
            }
        }
    }
    *pending = keep;
}

// ---------------------------------------------------------------------------
// Unit tests for the publish-retry scheduler (no media driver required).
//
// These pin the behavior that fixes the cluster `tx_ordering` freeze: a
// back-pressured publish is parked and retried, never blocking the loop,
// and per-publication FIFO order is preserved across retries. The
// matching real-Aeron end-to-end proof (a back-pressured publish must not
// delay a live subscription's delivery) lives in
// `tests/offer_starvation.rs`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod drain_pending_tests {
    use super::*;
    use crossbeam_channel::Receiver as CbReceiver;

    /// Build a pending publish with a one-byte payload tagged `marker` (so
    /// a test can identify which frame an offer is being asked about) and
    /// a deadline `dl_ms` from `base`. Returns the entry and its ack
    /// receiver.
    fn pending(
        pub_id: u32,
        marker: u8,
        base: Instant,
        dl_ms: u64,
    ) -> (PendingPublish, CbReceiver<Result<BPosition, LogError>>) {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let mut bytes = AlignedVec::new();
        bytes.extend_from_slice(&[marker]);
        (
            PendingPublish {
                pub_id,
                bytes,
                ack: Some(tx),
                deadline: base + Duration::from_millis(dl_ms),
            },
            rx,
        )
    }

    #[test]
    fn delivers_and_acks_then_empties_queue() {
        let now = Instant::now();
        let (p, rx) = pending(0, 0xAA, now, 5_000);
        let mut q = VecDeque::from([p]);
        // Offer always succeeds with a stream position.
        drain_pending_inner(&mut q, now, |_| OfferResult::Code(64));
        assert!(q.is_empty(), "delivered frame must be removed");
        match rx.try_recv() {
            Ok(Ok(_pos)) => {}
            other => panic!("expected an Ok position ack, got {other:?}"),
        }
    }

    #[test]
    fn back_pressure_retains_frame_for_next_iteration() {
        // The crux of the fix: a back-pressured offer (whose deadline has
        // not passed) is kept in the queue and retried. It is never
        // dropped and never blocks. The ack must stay pending.
        let now = Instant::now();
        let (p, rx) = pending(0, 0xAA, now, 5_000);
        let mut q = VecDeque::from([p]);
        drain_pending_inner(&mut q, now, |_| {
            OfferResult::Code(-2 /* BACK_PRESSURED */)
        });
        assert_eq!(q.len(), 1, "back-pressured frame must be retained");
        assert!(rx.try_recv().is_err(), "must not ack a retained frame");

        // Next iteration the subscriber has drained — now it delivers.
        drain_pending_inner(&mut q, now, |_| OfferResult::Code(0));
        assert!(q.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }

    #[test]
    fn preserves_per_publication_fifo_under_back_pressure() {
        // Two frames on pub 0 (A then B). The first attempt back-pressures
        // A; B must not be offered ahead of A, which would reorder the
        // stream. This asserts by recording which markers the offer fn is
        // asked about.
        let now = Instant::now();
        let (a, _ra) = pending(0, 0xA1, now, 5_000);
        let (b, _rb) = pending(0, 0xB2, now, 5_000);
        let mut q = VecDeque::from([a, b]);

        let mut offered: Vec<u8> = Vec::new();
        drain_pending_inner(&mut q, now, |item| {
            offered.push(item.bytes.as_slice()[0]);
            OfferResult::Code(-2) // A back-pressures
        });
        assert_eq!(
            offered,
            vec![0xA1],
            "only the head-of-line frame may be offered; B must not jump ahead"
        );
        assert_eq!(q.len(), 2, "both frames retained, still in order");
        assert_eq!(q[0].bytes.as_slice()[0], 0xA1);
        assert_eq!(q[1].bytes.as_slice()[0], 0xB2);
    }

    #[test]
    fn independent_publications_do_not_block_each_other() {
        // Pub 0 back-pressures, but pub 1 is fine; pub 1 must still deliver.
        let now = Instant::now();
        let (a, ra) = pending(0, 0xA1, now, 5_000);
        let (b, rb) = pending(1, 0xB2, now, 5_000);
        let mut q = VecDeque::from([a, b]);

        drain_pending_inner(&mut q, now, |item| {
            if item.pub_id == 0 {
                OfferResult::Code(-2)
            } else {
                OfferResult::Code(0)
            }
        });
        assert_eq!(
            q.len(),
            1,
            "only the back-pressured pub-0 frame is retained"
        );
        assert_eq!(q[0].pub_id, 0);
        assert!(ra.try_recv().is_err(), "pub 0 still pending");
        assert!(matches!(rb.try_recv(), Ok(Ok(_))), "pub 1 delivered");
    }

    #[test]
    fn expired_frame_acks_an_error_and_is_dropped() {
        // A frame still failing once its deadline has passed must error
        // out, so a must-deliver caller can resubmit, rather than spin
        // forever.
        let now = Instant::now();
        let (p, rx) = pending(0, 0xAA, now, 0 /* deadline == now */);
        let mut q = VecDeque::from([p]);
        // `now` already >= deadline, offer still negative.
        drain_pending_inner(&mut q, now, |_| {
            OfferResult::Code(-1 /* NOT_CONNECTED */)
        });
        assert!(q.is_empty(), "expired frame must be dropped");
        match rx.try_recv() {
            Ok(Err(LogError::Aeron(m))) => {
                assert!(
                    m.contains("NOT_CONNECTED"),
                    "error should name the code: {m}"
                )
            }
            other => panic!("expected an Aeron error ack, got {other:?}"),
        }
    }

    #[test]
    fn queued_frame_behind_blocked_head_expires_at_its_own_deadline() {
        // Regression test: a frame parked behind a blocked head must have
        // its deadline evaluated every pass, not only once it reaches the
        // head. Otherwise a caller could hit its ack timeout while the
        // frame is still queued, and the frame would be delivered late
        // after the caller already treated the publish as failed.
        let now = Instant::now();
        let (head, head_rx) = pending(0, 0xA1, now, 5_000); // head: not yet expired
        let (tail, tail_rx) = pending(0, 0xB2, now, 0); // tail: deadline == now
        let mut q = VecDeque::from([head, tail]);

        let mut offered: Vec<u8> = Vec::new();
        drain_pending_inner(&mut q, now, |item| {
            offered.push(item.bytes.as_slice()[0]);
            OfferResult::Code(-2) // head back-pressures
        });
        // Only the head was offered. The expired tail must not be
        // offered; that would deliver it out of FIFO order after the
        // caller gave up.
        assert_eq!(offered, vec![0xA1]);
        assert_eq!(q.len(), 1, "only the unexpired head is retained");
        assert_eq!(q[0].bytes.as_slice()[0], 0xA1);
        assert!(head_rx.try_recv().is_err(), "head still pending");
        match tail_rx.try_recv() {
            Ok(Err(LogError::Aeron(m))) => assert!(
                m.contains("expired while queued"),
                "expired-in-queue error should say so: {m}"
            ),
            other => panic!("expected an expired-in-queue error ack, got {other:?}"),
        }
    }

    #[test]
    fn queued_frame_behind_blocked_head_with_live_deadline_is_retained() {
        // Companion to the expiry test: an unexpired frame behind a blocked
        // head stays queued in order (no reordering, no premature error).
        let now = Instant::now();
        let (head, _head_rx) = pending(0, 0xA1, now, 5_000);
        let (tail, tail_rx) = pending(0, 0xB2, now, 5_000);
        let mut q = VecDeque::from([head, tail]);

        drain_pending_inner(&mut q, now, |_| OfferResult::Code(-2));
        assert_eq!(q.len(), 2, "both frames retained, still in order");
        assert_eq!(q[0].bytes.as_slice()[0], 0xA1);
        assert_eq!(q[1].bytes.as_slice()[0], 0xB2);
        assert!(tail_rx.try_recv().is_err(), "tail must not be acked yet");
    }

    #[test]
    fn unknown_publication_id_fails_immediately() {
        let now = Instant::now();
        let (p, rx) = pending(9, 0xAA, now, 5_000);
        let mut q = VecDeque::from([p]);
        drain_pending_inner(&mut q, now, |_| OfferResult::UnknownPub);
        assert!(q.is_empty(), "unknown-pub frame must not be retried");
        match rx.try_recv() {
            Ok(Err(LogError::Aeron(m))) => assert!(m.contains("unknown pub_id")),
            other => panic!("expected unknown-pub error, got {other:?}"),
        }
    }

    #[test]
    fn idle_backoff_escalates_after_grace_and_resets_on_work() {
        let base = Duration::from_micros(100);
        let cap = Duration::from_millis(1);
        let mut b = IdleBackoff::new(base, cap, 3);
        // Within grace: base cadence.
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
        // Past grace: doubles per idle iteration,
        assert_eq!(b.idle_wait(), Duration::from_micros(200));
        assert_eq!(b.idle_wait(), Duration::from_micros(400));
        assert_eq!(b.idle_wait(), Duration::from_micros(800));
        // and clamps at the cap, staying there.
        assert_eq!(b.idle_wait(), cap);
        for _ in 0..100 {
            assert_eq!(b.idle_wait(), cap);
        }
        // Any work snaps back to base for a full new grace window.
        b.reset();
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
    }
}

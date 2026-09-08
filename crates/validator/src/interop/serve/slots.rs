//! Live-subscription caps: how many outbox/attestation subscriptions this
//! server holds open at once, in total and per destination chain.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use jsonrpsee::server::PendingSubscriptionSink;
use jsonrpsee::types::ErrorObject;

use super::{FeedServerLimits, Handler, SUBSCRIPTION_CAP_ERROR_CODE};

/// Which cap a rejected subscription hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SubscriptionCap {
    Total,
    PerDest,
}

impl SubscriptionCap {
    /// The RPC error message and the log field: one string for both, so a
    /// caller cannot drift the two apart.
    fn as_str(self) -> &'static str {
        match self {
            Self::Total => "total subscription cap reached",
            Self::PerDest => "per-destination subscription cap reached",
        }
    }
}

/// Live-subscription counters, shared by both handlers.
#[derive(Default)]
pub(super) struct Slots {
    inner: Mutex<SlotCounts>,
}

#[derive(Default)]
struct SlotCounts {
    total: usize,
    per_dest: BTreeMap<u64, usize>,
}

/// Holds one subscription slot; drop releases it. Every return path of a
/// handler drops it, so a closed socket always frees its slot.
pub(super) struct SlotGuard {
    slots: Arc<Slots>,
    dest: Option<u64>,
}

impl Slots {
    /// Take a slot for a subscription, `dest` for outbox subscriptions.
    /// Checks each cap before it changes any count, so a rejected
    /// subscription never leaves a stray entry for a destination it never
    /// actually held a slot for.
    fn take(
        self: &Arc<Self>,
        limits: FeedServerLimits,
        dest: Option<u64>,
    ) -> Result<SlotGuard, SubscriptionCap> {
        // A panic under this lock (from another handler) must not poison
        // every later subscribe or guard-drop; recovering the guard is
        // correct because `SlotCounts` has no invariant a partial write
        // could break (each field is an independent counter).
        let mut g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if g.total >= limits.max_subscriptions.get() {
            return Err(SubscriptionCap::Total);
        }
        if let Some(d) = dest {
            // No entry means no subscription has counted against `d`
            // yet: 0 is the correct starting count, not a sentinel.
            let per_dest = g.per_dest.get(&d).copied().unwrap_or(0);
            if per_dest >= limits.max_subscriptions_per_dest.get() {
                return Err(SubscriptionCap::PerDest);
            }
            g.per_dest.insert(d, per_dest.saturating_add(1));
        }
        g.total = g.total.saturating_add(1);
        Ok(SlotGuard {
            slots: self.clone(),
            dest,
        })
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        // Same poison-recovery reasoning as `take`: a `Drop` impl cannot
        // return an error, and the counters carry no invariant that a
        // panic mid-update could leave broken.
        let mut g = self
            .slots
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.total = g.total.saturating_sub(1);
        if let Some(d) = self.dest
            && let Some(n) = g.per_dest.get_mut(&d)
        {
            *n = n.saturating_sub(1);
            // Drop the entry at zero, so a client that churns destination
            // ids cannot grow the map without bound.
            if *n == 0 {
                g.per_dest.remove(&d);
            }
        }
    }
}

/// A subscription that passed the caps: the still-pending sink, ready to
/// accept, and the slot guard held for the subscription's lifetime.
pub(super) struct Accepted {
    pub(super) pending: PendingSubscriptionSink,
    /// Held for the subscription's lifetime; never read, only kept alive.
    /// The caller must bind this by name (not `..`), or the slot frees the
    /// instant the pattern match completes.
    pub(super) _guard: SlotGuard,
}

/// A subscription rejected by a cap. The client already has the RPC error;
/// the caller returns `Ok(())`.
pub(super) struct Rejected;

impl Handler {
    /// Take a slot, or reject the pending subscription.
    pub(super) async fn take_slot(
        &self,
        pending: PendingSubscriptionSink,
        dest: Option<u64>,
    ) -> Result<Accepted, Rejected> {
        match self.slots.take(self.state.limits, dest) {
            Ok(guard) => Ok(Accepted {
                pending,
                _guard: guard,
            }),
            Err(cap) => {
                tracing::warn!(
                    target: "validator::interop::serve",
                    dest,
                    cap = cap.as_str(),
                    "feed subscription rejected"
                );
                crate::metrics::counter_feed_subscription_rejected();
                pending
                    .reject(ErrorObject::owned(
                        SUBSCRIPTION_CAP_ERROR_CODE,
                        cap.as_str(),
                        None::<()>,
                    ))
                    .await;
                Err(Rejected)
            }
        }
    }
}

//! Provider refusals remove a required source without changing finalized
//! data. `l1-transport-refusals` runs the refusal and the rate limit;
//! `l1-provider-unresponsive` runs the provider that answers nothing.

use std::time::{Duration, Instant};

use kardamom_l1_fault_proxy::Fault;

use super::followers::{Followers, await_archive_complete, await_resume};
use super::halt::await_followers_halted;
use crate::harness::Harness;
use crate::l1::L1;
use crate::poll::{self, Budget};

/// How long a refusal holds after the follower halted on it. Every
/// follower read through it fails, and L1 posting must stay still.
const HOLD_AFTER_HALT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy)]
enum Refusal {
    Unavailable,
    RateLimited,
    Unresponsive,
}

impl Refusal {
    fn fault(self) -> Fault {
        match self {
            Self::Unavailable => Fault::Down,
            Self::RateLimited => Fault::RateLimit,
            Self::Unresponsive => Fault::Unresponsive { all_sources: true },
        }
    }

    async fn run(self, h: &mut Harness, l1: &L1, ctx: &str) -> anyhow::Result<()> {
        let base = Followers::ready(h, ctx).await?;
        let before = l1.last_batch_index().await?;
        self.await_posting(l1, before, ctx).await?;
        let fault = self.fault();
        crate::log(format!("{ctx}: provider fault {fault:?}"));
        l1.set_faults(&[fault]).await?;
        let held = self.hold(h, l1, base, ctx).await;
        let cleared = Followers::at_clear(h, l1, base).await;
        let posted = held?;
        let stuck = cleared?;
        await_resume(h, stuck, ctx).await?;
        self.await_posting(l1, posted, ctx).await?;
        h.assert_progress().await
    }

    async fn hold(
        self,
        h: &mut Harness,
        l1: &L1,
        base: Followers,
        ctx: &str,
    ) -> anyhow::Result<u64> {
        let started = Instant::now();
        await_followers_halted(h, base, ctx).await?;
        let record = l1.posted().await?;
        let halted = Instant::now();
        h.assert_progress().await?;
        tokio::time::sleep(HOLD_AFTER_HALT.saturating_sub(halted.elapsed())).await;
        anyhow::ensure!(
            l1.posted().await? == record,
            "{}: {ctx}: L1 posting advances while its provider refuses every request",
            crate::FAIL_PREFIX
        );
        crate::log(format!(
            "{ctx}: {:?} held for {}s",
            self.fault(),
            started.elapsed().as_secs()
        ));
        Ok(record.last().map_or(0, |batch| batch.index))
    }

    /// The contract cursor survives a batcher restart; its process counter does not.
    async fn await_posting(self, l1: &L1, base: u64, ctx: &str) -> anyhow::Result<()> {
        let outcome = poll::until(Budget::secs(120, 3), |_| async move {
            let now = l1.last_batch_index().await?;
            Ok::<_, anyhow::Error>((now > base).then_some(now))
        })
        .await?;
        let (now, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: L1 holds no new batch within {}s of clearing {:?} (last batch {base})",
                t.as_secs(),
                self.fault()
            )
        })?;
        crate::log(format!(
            "{ctx}: L1 posting resumed after {}s ({base} -> {now})",
            elapsed.as_secs()
        ));
        Ok(())
    }
}

/// The provider refuses every request, then rate-limits every request.
pub(crate) async fn transport_refusals(h: &mut Harness) -> anyhow::Result<()> {
    Refusals {
        ctx: "l1-transport-refusals",
        each: &[Refusal::Unavailable, Refusal::RateLimited],
    }
    .run(h)
    .await
}

/// The provider answers no request: the follower halts on the silence.
pub(crate) async fn provider_unresponsive(h: &mut Harness) -> anyhow::Result<()> {
    Refusals {
        ctx: "l1-provider-unresponsive",
        each: &[Refusal::Unresponsive],
    }
    .run(h)
    .await
}

/// The refusals of one case, in order, then the DA record after them.
struct Refusals {
    ctx: &'static str,
    each: &'static [Refusal],
}

impl Refusals {
    async fn run(self, h: &mut Harness) -> anyhow::Result<()> {
        let l1 = L1::new(h).await?;
        for refusal in self.each {
            refusal.run(h, &l1, self.ctx).await?;
        }
        await_archive_complete(h, &l1, self.ctx).await?;
        l1.assert_contiguous(self.ctx).await
    }
}

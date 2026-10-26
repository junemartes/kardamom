//! Provider refusals remove a required source without changing finalized data.

use std::time::{Duration, Instant};

use kardamom_l1_fault_proxy::Fault;

use super::batcher::{await_posting, require_posting};
use super::followers::{Followers, await_archive_complete, await_resume};
use super::halt::await_followers_halted;
use crate::harness::Harness;
use crate::l1::L1;

#[derive(Clone, Copy)]
enum Refusal {
    Unavailable,
    RateLimited,
}

impl Refusal {
    const ALL: [Self; 2] = [Self::Unavailable, Self::RateLimited];

    fn fault(self) -> Fault {
        match self {
            Self::Unavailable => Fault::Down,
            Self::RateLimited => Fault::RateLimit,
        }
    }

    async fn run(self, h: &mut Harness, l1: &L1, ctx: &str) -> anyhow::Result<()> {
        let base = Followers::ready(h, ctx).await?;
        require_posting(h, ctx).await?;
        let fault = self.fault();
        crate::log(format!(
            "{ctx}: refuse the first provider with {fault:?}; the second remains reachable"
        ));
        l1.set_faults(&[fault]).await?;
        let held = self.hold(h, l1, base, ctx).await;
        let cleared = Followers::at_clear(h, l1, base).await;
        let posted = held?;
        let stuck = cleared?;
        await_resume(h, stuck, ctx).await?;
        await_posting(h, posted, Duration::from_secs(120), ctx).await?;
        h.assert_progress().await
    }

    async fn hold(
        self,
        h: &mut Harness,
        l1: &L1,
        base: Followers,
        ctx: &str,
    ) -> anyhow::Result<i64> {
        let started = Instant::now();
        await_followers_halted(h, base, ctx).await?;
        let posted = h.probes.batcher_posts().await.ok_or_else(|| {
            crate::chaos_fail!("{ctx}: the batcher exporter is missing under the refusal")
        })?;
        let record = l1.posted().await?;
        h.assert_progress().await?;
        tokio::time::sleep(h.knobs.l1_fault.saturating_sub(started.elapsed())).await;
        anyhow::ensure!(
            l1.posted().await? == record,
            "{}: {ctx}: L1 posting advances while its provider refuses every request",
            crate::FAIL_PREFIX
        );
        anyhow::ensure!(
            h.probes.batcher_posts().await == Some(posted),
            "{}: {ctx}: the batcher confirms new posts through a refusing provider",
            crate::FAIL_PREFIX
        );
        crate::log(format!(
            "{ctx}: {:?} held for {}s",
            self.fault(),
            started.elapsed().as_secs()
        ));
        Ok(posted)
    }
}

pub(crate) async fn transport_refusals(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "l1-transport-refusals";
    let l1 = L1::new(h).await?;
    for refusal in Refusal::ALL {
        refusal.run(h, &l1, ctx).await?;
    }
    await_archive_complete(h, &l1, ctx).await?;
    l1.assert_contiguous(ctx).await
}

//! `l1-liar`: the proxy serves a wrong block hash, then a broken parent
//! chain, then swallowed settlement logs, each for the fault window.

use std::time::{Duration, Instant};

use kardamom_l1_fault_proxy::Fault;

use super::batcher::{assert_posted_through, require_posting};
use super::deferred;
use super::followers::{
    Followers, await_archive_complete, await_resume, heal_single_source_followers,
};
use super::halt::await_followers_halted;
use crate::harness::Harness;
use crate::l1::{L1, STALE_POST_ALERT};

/// The least posts the batcher confirms through one fault window: it
/// posts every flush wait, so a one-minute window holds many more.
const MIN_POSTS_THROUGH_FAULT: i64 = 3;

/// How long the stale-post alert must hold to count as fired.
const ALERT_HOLD: Duration = Duration::from_secs(30);

/// The three lies, in order.
#[derive(Debug, Clone, Copy)]
enum Phase {
    WrongHash,
    BrokenChain,
    SwallowedLogs,
}

/// What one source can see of a lie.
enum Detection {
    /// The follower's own chain check catches it.
    OneSource,
    /// Only a second source can: nothing in one answer is wrong on its
    /// own.
    NeedsSecondSource,
}

/// How the followers come back once the lie stops.
enum Recovery {
    /// The chain check passes again by itself.
    SelfResume,
    /// The wrong hash reached the follower's anchor; a single-source
    /// follower stays halted until an operator acts.
    PoisonedAnchor,
    /// Nothing halted.
    NeverHalted,
}

impl Phase {
    const ALL: [Self; 3] = [Self::WrongHash, Self::BrokenChain, Self::SwallowedLogs];

    fn name(self) -> &'static str {
        match self {
            Self::WrongHash => "wrong-block-hash",
            Self::BrokenChain => "broken-parent-chain",
            Self::SwallowedLogs => "swallowed-settlement-logs",
        }
    }

    /// The fault, armed from the next L1 block so nothing a follower
    /// already accepted changes under it.
    async fn fault(self, l1: &L1) -> anyhow::Result<Fault> {
        let from_block = l1.head().await?.saturating_add(1);
        Ok(match self {
            Self::WrongHash => Fault::WrongBlockHash { from_block },
            Self::BrokenChain => Fault::BrokenParentChain { from_block },
            Self::SwallowedLogs => Fault::SwallowLogs {
                address: l1.settlement,
            },
        })
    }

    fn detection(self) -> Detection {
        match self {
            Self::WrongHash | Self::BrokenChain => Detection::OneSource,
            Self::SwallowedLogs => Detection::NeedsSecondSource,
        }
    }

    fn recovery(self) -> Recovery {
        match self {
            Self::WrongHash => Recovery::PoisonedAnchor,
            Self::BrokenChain => Recovery::SelfResume,
            Self::SwallowedLogs => Recovery::NeverHalted,
        }
    }

    /// One phase: arm the lie, prove the halt and the batcher's posts
    /// through it, let it run out, clear it, and prove the resume.
    async fn run(self, h: &mut Harness, l1: &L1, ctx: &str) -> anyhow::Result<()> {
        let base = Followers::ready(h, ctx).await?;
        let posted0 = require_posting(h, ctx).await?;
        let fault = self.fault(l1).await?;
        let window = h.knobs.l1_fault;
        crate::log(format!(
            "{ctx}: phase {}: {fault:?} for {}s ({})",
            self.name(),
            window.as_secs(),
            base.show()
        ));
        l1.set_faults(&[fault]).await?;
        let armed = Instant::now();
        match self.detection() {
            Detection::OneSource => await_followers_halted(h, base, ctx).await?,
            Detection::NeedsSecondSource => deferred(
                ctx,
                "the halt within 3 ticks on swallowed logs: one source cannot see a missing log; the cross-check of two can",
            ),
        }
        deferred(
            ctx,
            "kardamom_l1_source_disagreement_total is not exported yet; the halt is proven by the tick-outcome counters",
        );
        self.assert_alert(l1, window, ctx).await?;
        tokio::time::sleep(window.saturating_sub(armed.elapsed())).await;
        assert_posted_through(h, posted0, MIN_POSTS_THROUGH_FAULT, ctx).await?;
        let stuck = Followers::at_clear(h, l1).await?;
        self.assert_resume(h, stuck, ctx).await
    }

    /// The stale-post alert fires on the swallowed logs: the batcher's
    /// own posts vanish from its L1 view. The hash lies change nothing
    /// the batcher reads, so the alert's state is only logged there.
    async fn assert_alert(self, l1: &L1, window: Duration, ctx: &str) -> anyhow::Result<()> {
        match self {
            Self::SwallowedLogs => {
                l1.await_alert_held(STALE_POST_ALERT, ALERT_HOLD, window, ctx)
                    .await
            }
            Self::WrongHash | Self::BrokenChain => {
                let state = l1.alert_state(STALE_POST_ALERT).await?;
                crate::log(format!(
                    "{ctx}: the alert {STALE_POST_ALERT} is {} under {}",
                    state.unwrap_or_else(|| "inactive".to_string()),
                    self.name()
                ));
                Ok(())
            }
        }
    }

    async fn assert_resume(
        self,
        h: &mut Harness,
        stuck: Followers,
        ctx: &str,
    ) -> anyhow::Result<()> {
        match self.recovery() {
            Recovery::SelfResume | Recovery::NeverHalted => await_resume(h, stuck, ctx).await,
            Recovery::PoisonedAnchor => {
                deferred(
                    ctx,
                    "the resume by itself after a wrong hash: the single-source follower kept the wrong hash as its anchor",
                );
                heal_single_source_followers(h, ctx).await?;
                await_resume(h, stuck, ctx).await
            }
        }
    }
}

/// The three lies in order. Through each, the followers halt where one
/// source can see the lie, the batcher keeps posting, and the stale-post
/// alert fires on the swallowed logs; after each, the followers resume
/// and the archive catches up with L1; at the end, the DA record is
/// contiguous.
pub(crate) async fn liar(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "l1-liar";
    let l1 = L1::new(h).await?;
    l1.require_rule_loaded(STALE_POST_ALERT, ctx).await?;
    for phase in Phase::ALL {
        phase.run(h, &l1, ctx).await?;
    }
    await_archive_complete(h, &l1, ctx).await?;
    l1.assert_contiguous(ctx).await
}

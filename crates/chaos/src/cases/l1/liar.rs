//! `l1-liar`: the proxy serves a wrong block hash, then a broken parent
//! chain, then swallowed settlement logs. Each lie lasts until its
//! effects show, and at most the fault window.
//!
//! The L1 follower reads two sources: the proxy, which lies, and the
//! proxy's second source, which serves L1. Each lie is a disagreement of
//! the two: the follower halts once and publishes nothing of it; the
//! da-watcher and the batcher pause with the follower as their root; all
//! resume by themselves when the lie stops. No operator step.

use std::time::{Duration, Instant};

use kardamom_l1_fault_proxy::Fault;

use super::ALERT_HOLD;
use super::batcher::{assert_posted_through, require_posting};
use super::followers::{Followers, await_archive_complete, await_resume};
use super::halt::await_followers_halted;
use crate::harness::Harness;
use crate::l1::{L1, STALE_POST_ALERT};
use crate::poll::{self, Budget};

/// The least posts the batcher confirms through one lie: it posts every
/// flush wait, about every 5 s.
const MIN_POSTS_THROUGH_FAULT: i64 = 3;

/// How often a phase reads the post counter while the lie runs.
const POST_READ: Duration = Duration::from_secs(3);

/// The three lies, in order.
#[derive(Debug, Clone, Copy)]
enum Phase {
    WrongHash,
    BrokenChain,
    SwallowedLogs,
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

    /// One phase: arm the lie, prove the halt, the pauses and the
    /// batcher's posts through it, clear it, and prove the resume. The
    /// lie lasts until the batcher confirmed its posts through it, and at
    /// most the fault window.
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
        // The lie clears on every path, so no fault leaks into the next
        // phase or case.
        let held = self.hold(h, l1, base, posted0, ctx).await;
        let cleared = Followers::at_clear(h, l1, base).await;
        held?;
        let stuck = cleared?;
        await_resume(h, stuck, ctx).await
    }

    /// Through the armed lie: the halt, the alert, and the batcher's
    /// posts until the fault window ends.
    async fn hold(
        self,
        h: &Harness,
        l1: &L1,
        base: Followers,
        posted0: i64,
        ctx: &str,
    ) -> anyhow::Result<()> {
        let armed = Instant::now();
        let window = h.knobs.l1_fault;
        await_followers_halted(h, base, ctx).await?;
        self.assert_alert(l1, window, ctx).await?;
        Self::await_posts(h, posted0, window.saturating_sub(armed.elapsed())).await?;
        assert_posted_through(h, posted0, MIN_POSTS_THROUGH_FAULT, ctx).await
    }

    /// Wait until the batcher confirmed [`MIN_POSTS_THROUGH_FAULT`]
    /// posts since `posted0`, for at most `left` and at least one read.
    /// The caller judges the count.
    async fn await_posts(h: &Harness, posted0: i64, left: Duration) -> anyhow::Result<()> {
        let need = posted0.saturating_add(MIN_POSTS_THROUGH_FAULT);
        poll::until(
            Budget::new(left.max(POST_READ), POST_READ),
            |_| async move {
                Ok::<_, anyhow::Error>(h.probes.batcher_posts().await.filter(|p| *p >= need))
            },
        )
        .await
        .map(|_| ())
    }

    /// The stale-post alert fires on the swallowed logs: the batcher's
    /// own posts vanish from its L1 view. Under the hash lies its state
    /// is only logged.
    async fn assert_alert(self, l1: &L1, window: Duration, ctx: &str) -> anyhow::Result<()> {
        match self {
            Self::SwallowedLogs => {
                l1.await_alert_held(STALE_POST_ALERT, ALERT_HOLD, window, ctx)
                    .await
            }
            Self::WrongHash | Self::BrokenChain => {
                let state = l1
                    .alert_state(STALE_POST_ALERT)
                    .await
                    .unwrap_or_else(|e| Some(format!("unread ({e:#})")));
                crate::log(format!(
                    "{ctx}: the alert {STALE_POST_ALERT} is {} under {}",
                    state.unwrap_or_else(|| "inactive".to_string()),
                    self.name()
                ));
                Ok(())
            }
        }
    }
}

/// The three lies in order. Through each, the follower halts on the
/// disagreement of its two sources, the da-watcher and the batcher pause
/// on it, the batcher keeps posting, and the stale-post alert fires on
/// the swallowed logs; after each, every one of them resumes by itself
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

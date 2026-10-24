//! What the pipeline must do while the classes of a combined case are
//! down, and the readings that judge it.

use std::time::Duration;

use super::Class;
use crate::cases::cache::wait_readers_degraded;
use crate::cases::chain_status::ChainView;
use crate::cases::validator::COMMITTED;
use crate::harness::Harness;
use crate::poll::{self, Budget};

/// The window between the two head readings that judge the expectation.
const WINDOW: Duration = Duration::from_secs(15);
/// How long a live ingress gets to refuse a submit on the lost quorum:
/// the sealer silence an ingress tolerates, then its next status poll.
const REFUSAL_BUDGET: Duration = Duration::from_secs(60);
/// The cause an ingress names while no sealer answers.
const NO_QUORUM: &str = "sealer_no_quorum";

/// The highest head any live client reports, and the highest count of
/// applied transactions any executor reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Progress {
    /// The highest of the executor block, the sealed head an ingress
    /// reads from the sealer's status frames, and the validator's
    /// committed block. Each one follows the sealer, so the highest one
    /// is the head of the chain as the live clients see it.
    pub(super) block: i64,
    /// `None` while no executor exporter answers: no executor runs, so
    /// no transaction is applied.
    pub(super) applied: Option<i64>,
}

impl Progress {
    /// One reading, retried for twelve seconds. Some client of the
    /// sealer is up through every combined case, so no head at all is a
    /// harness failure.
    async fn read(h: &Harness, ctx: &str) -> anyhow::Result<Self> {
        let outcome = poll::until(Budget::secs(12, 3), |_| async move {
            let block = h
                .probes
                .executor_progress()
                .await
                .max(h.probes.ingress_sealed_head().await)
                .max(h.probes.val_metric(COMMITTED).await);
            let applied = h.probes.executor_tx_applied().await;
            Ok(block.map(|block| Self { block, applied }))
        })
        .await?;
        outcome
            .or_fail(|_| {
                crate::chaos_fail!(
                    "{ctx}: no client reports a head, so the outage cannot be observed"
                )
            })
            .map(|(progress, _)| progress)
    }

    pub(super) fn describe(self) -> String {
        let applied = self.applied.map_or("?".to_string(), |a| a.to_string());
        format!("head {}, applied {applied}", self.block)
    }
}

/// What the pipeline must do while the classes are down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expect {
    /// No block is committed: the head stays flat, and every live
    /// ingress refuses a submit with `sealer_no_quorum`.
    Stall,
    /// Blocks are still sealed, but no user transaction is applied: the
    /// head rises and the applied-transaction counter of a live executor
    /// stays flat. A head that reads lower is not a rise: a source of the
    /// head stopped answering.
    SealOnly,
}

impl Expect {
    /// Whether two readings, one window apart, show the expectation.
    pub(super) fn judge(self, before: Progress, after: Progress) -> Result<(), String> {
        let applied = before.applied.zip(after.applied);
        match self {
            Self::Stall if after.block != before.block => Err(format!(
                "the pipeline UNEXPECTEDLY progressed while the sealers were down (head {} -> {})",
                before.block, after.block
            )),
            Self::SealOnly if after.block <= before.block => Err(format!(
                "no block was sealed while the sealers were up (head {} -> {})",
                before.block, after.block
            )),
            Self::SealOnly if applied.is_some_and(|(b, a)| a != b) => Err(format!(
                "a user transaction was applied while the classes were down ({} -> {})",
                before.describe(),
                after.describe()
            )),
            Self::Stall | Self::SealOnly => Ok(()),
        }
    }

    pub(super) fn observed(self, before: Progress, after: Progress) -> String {
        match self {
            Self::Stall => format!(
                "the pipeline correctly STALLED (head {} -> {} over {}s)",
                before.block,
                after.block,
                WINDOW.as_secs()
            ),
            Self::SealOnly => format!(
                "the sealers tick with no user transaction ({} -> {} over {}s)",
                before.describe(),
                after.describe(),
                WINDOW.as_secs()
            ),
        }
    }

    /// Judge two readings one window apart. A stall with the ingresses
    /// up also needs every ingress to refuse on the lost quorum. With
    /// Redis down, a cold read must count as degraded, not stall.
    pub(super) async fn assert(self, h: &Harness, ctx: &str, down: &[Class]) -> anyhow::Result<()> {
        let before = Progress::read(h, ctx).await?;
        tokio::time::sleep(WINDOW).await;
        let after = Progress::read(h, ctx).await?;
        self.judge(before, after)
            .map_err(|why| crate::chaos_fail!("{ctx}: {why}"))?;
        crate::log(format!("{ctx}: {}", self.observed(before, after)));
        if self == Self::Stall && !down.contains(&Class::Ingress) {
            h.assert_ingresses_refuse(ctx).await?;
        }
        if down.contains(&Class::Redis) {
            wait_readers_degraded(h, ctx).await?;
        }
        Ok(())
    }
}

impl Harness {
    /// Every ingress refuses a submit on the lost quorum within the
    /// budget.
    async fn assert_ingresses_refuse(&self, ctx: &str) -> anyhow::Result<()> {
        let budget = Budget::new(REFUSAL_BUDGET, Duration::from_secs(2));
        let outcome = poll::until(budget, |_| async move {
            let answers = self.refusals().await;
            let all = answers.iter().all(|a| a.contains(NO_QUORUM));
            Ok::<_, anyhow::Error>(all.then_some(answers))
        })
        .await?;
        let (answers, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: an ingress does not refuse on {NO_QUORUM} after {}s without a sealer",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "{ctx}: every ingress refuses on {NO_QUORUM} after {}s ({})",
            elapsed.as_secs(),
            answers.join("; ")
        ));
        Ok(())
    }

    /// One line per ingress: its refusal, or that it takes submits.
    async fn refusals(&self) -> Vec<String> {
        let mut lines = Vec::with_capacity(self.probes.ingresses.len());
        for node in &self.probes.ingresses {
            let why = ChainView::refusal_at(&node.rpc_url(), self.knobs.chain_id).await;
            lines.push(format!(
                "{}: {}",
                node.container,
                why.unwrap_or_else(|| "takes submits".to_string())
            ));
        }
        lines
    }
}

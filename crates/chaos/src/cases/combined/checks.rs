//! The checks a combined case makes after every class is back: the
//! origin gap, the leadership terms, the sender floors, and the refill
//! of a lost epoch.

use std::collections::{BTreeMap, BTreeSet};

use crate::cases::coordinated::assert_no_ref_below_floor;
use crate::cases::da_watcher::assert_no_origin_gap;
use crate::evidence::number_after;
use crate::harness::Harness;
use crate::poll::{self, Budget, Outcome};
use crate::probes::DA_WATCHER_PORT;

/// The da-watcher's count of epochs it published again. A lost epoch
/// moves it when the da-watcher fills the gap. The sequencer's
/// origin-gap counter is not a baseline: it resets with the sequencer.
const EPOCHS_REPUBLISHED: &str = "kardamom_da_watcher_epochs_republished_total";

/// The counter that rises when a lost epoch is filled again. `None` when
/// the exporter did not answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Refills {
    pub(super) republished: Option<i64>,
}

impl Refills {
    pub(super) async fn read(h: &Harness) -> Self {
        Self {
            republished: h
                .probes
                .aux_metric_where(DA_WATCHER_PORT, EPOCHS_REPUBLISHED, "")
                .await,
        }
    }

    /// Whether the counter rose from `self` to `now`.
    pub(super) fn rose_to(self, now: Self) -> bool {
        self.republished
            .zip(now.republished)
            .is_some_and(|(a, b)| b > a)
    }

    pub(super) fn describe(self) -> String {
        self.republished
            .map_or("republished ?".to_string(), |v| format!("republished {v}"))
    }

    /// The counter rises past this baseline within the budget: the
    /// da-watcher published a lost epoch again.
    async fn assert_rose(self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let outcome = poll::until(Budget::secs(60, 3), |_| async move {
            let now = Self::read(h).await;
            Ok::<_, anyhow::Error>(self.rose_to(now).then_some(now))
        })
        .await?;
        let now = match outcome {
            Outcome::Ready { value, .. } => value,
            Outcome::TimedOut { elapsed } => {
                return Err(crate::chaos_fail!(
                    "{ctx}: no epoch was published again within {}s ({} -> {})",
                    elapsed.as_secs(),
                    self.describe(),
                    Self::read(h).await.describe()
                ));
            }
        };
        crate::log(format!(
            "{ctx}: a lost epoch was filled again ({} -> {})",
            self.describe(),
            now.describe()
        ));
        Ok(())
    }
}

/// The members that led each leadership term, from the `cluster TERM`
/// lines of every member. The members replay the same log, so every
/// member names the same leader for a term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Terms(pub(super) BTreeMap<u64, BTreeSet<u64>>);

impl Terms {
    pub(super) fn parse(logs: &str) -> Self {
        Self(
            logs.lines()
                .filter(|l| l.contains("cluster TERM "))
                .filter_map(|l| {
                    number_after(l, "leadershipTermId=").zip(number_after(l, "leaderMemberId="))
                })
                .fold(BTreeMap::new(), |mut terms, (term, leader)| {
                    terms
                        .entry(term)
                        .or_insert_with(BTreeSet::new)
                        .insert(leader);
                    terms
                }),
        )
    }

    /// The terms that more than one member led.
    pub(super) fn split(&self) -> Vec<String> {
        self.0
            .iter()
            .filter(|(_, leaders)| leaders.len() > 1)
            .map(|(term, leaders)| format!("term {term}: members {leaders:?}"))
            .collect()
    }

    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
}

/// At most one member led each leadership term, over at least one term.
async fn assert_one_leader_per_term(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let terms = Terms::parse(&h.evidence.cluster_logs().await?);
    anyhow::ensure!(
        terms.len() > 0,
        "{}: {ctx}: no member logged a leadership term",
        crate::FAIL_PREFIX
    );
    let split = terms.split();
    anyhow::ensure!(
        split.is_empty(),
        "{}: {ctx}: two members led one leadership term ({})",
        crate::FAIL_PREFIX,
        split.join("; ")
    );
    crate::log(format!(
        "{ctx}: one leader per leadership term over {} terms",
        terms.len()
    ));
    Ok(())
}

/// A check after the return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Check {
    /// The sealer's L1 origin reaches the last block the da-watcher
    /// published.
    NoOriginGap,
    /// No two members led the same leadership term.
    OneLeaderPerTerm,
    /// No lane-0 replica holds a ref below a sender's floor.
    NoRefBelowFloor,
    /// A lost epoch was filled again.
    EpochRefill,
}

impl Check {
    pub(super) async fn assert(
        self,
        h: &Harness,
        ctx: &str,
        refills: Refills,
    ) -> anyhow::Result<()> {
        match self {
            Self::NoOriginGap => assert_no_origin_gap(h, ctx).await,
            Self::OneLeaderPerTerm => assert_one_leader_per_term(h, ctx).await,
            Self::NoRefBelowFloor => assert_no_ref_below_floor(h, ctx).await,
            Self::EpochRefill => refills.assert_rose(h, ctx).await,
        }
    }
}

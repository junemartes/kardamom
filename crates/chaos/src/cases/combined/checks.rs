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

/// The da-watcher's count of epochs it published again, and the
/// sequencer's count of origin-gap rejects. A lost epoch moves one of
/// them when it is filled.
const EPOCHS_REPUBLISHED: &str = "kardamom_da_watcher_epochs_republished_total";
const ORIGIN_GAP_TOTAL: &str = "kardamom_sequencer_origin_gap_total";

/// The counters that rise when a lost epoch is filled again. `None` when
/// an exporter did not answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Refills {
    pub(super) republished: Option<i64>,
    pub(super) origin_gaps: Option<i64>,
}

impl Refills {
    pub(super) async fn read(h: &Harness) -> Self {
        let republished = h
            .probes
            .aux_metric_where(DA_WATCHER_PORT, EPOCHS_REPUBLISHED, "")
            .await;
        let mut origin_gaps: Option<i64> = Some(0);
        for i in 0..h.probes.sequencers.len() {
            let lane = h
                .probes
                .seq_lane0_metric_where(i, ORIGIN_GAP_TOTAL, "")
                .await;
            origin_gaps = origin_gaps.zip(lane).map(|(a, b)| a.saturating_add(b));
        }
        Self {
            republished,
            origin_gaps,
        }
    }

    /// Whether a counter rose from `self` to `now`.
    pub(super) fn rose_to(self, now: Self) -> bool {
        let rose = |a: Option<i64>, b: Option<i64>| a.zip(b).is_some_and(|(a, b)| b > a);
        rose(self.republished, now.republished) || rose(self.origin_gaps, now.origin_gaps)
    }

    pub(super) fn describe(self) -> String {
        let show = |v: Option<i64>| v.map_or("?".to_string(), |v| v.to_string());
        format!(
            "republished {}, origin gaps {}",
            show(self.republished),
            show(self.origin_gaps)
        )
    }

    /// A counter rises past this baseline within the budget: the
    /// da-watcher published a lost epoch again, or the sealer rejected an
    /// origin gap and the lane offered its epochs again.
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
                    "{ctx}: no epoch was filled again within {}s ({} -> {})",
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
pub(super) fn leaders_by_term(logs: &str) -> BTreeMap<u64, BTreeSet<u64>> {
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
        })
}

/// The terms that more than one member led.
pub(super) fn split_terms(terms: &BTreeMap<u64, BTreeSet<u64>>) -> Vec<String> {
    terms
        .iter()
        .filter(|(_, leaders)| leaders.len() > 1)
        .map(|(term, leaders)| format!("term {term}: members {leaders:?}"))
        .collect()
}

/// At most one member led each leadership term, over at least one term.
async fn assert_one_leader_per_term(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let terms = leaders_by_term(&h.evidence.cluster_logs().await?);
    anyhow::ensure!(
        !terms.is_empty(),
        "{}: {ctx}: no member logged a leadership term",
        crate::FAIL_PREFIX
    );
    let split = split_terms(&terms);
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

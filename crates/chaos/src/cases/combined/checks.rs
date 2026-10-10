//! The checks a combined case makes after every class is back: the
//! origin gap, the leadership terms, the sender floors, the refill of a
//! lost epoch, a receipt served from an executor's state, the rebuild
//! of every state mirror, the readers' return to Redis, and the floors
//! a restarted sequencer looked up.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::cases::cache::{mirror_rebuilds, wait_every_mirror_rebuilt, wait_readers_recovered};
use crate::cases::coordinated::assert_no_ref_below_floor;
use crate::cases::da_watcher::assert_no_origin_gap;
use crate::evidence::number_after;
use crate::harness::Harness;
use crate::poll::{self, Budget, Outcome};
use crate::probes::{DA_WATCHER_PORT, Probed};
use crate::rpc::Rpc;

/// The da-watcher's count of epochs it published again. A lost epoch
/// moves it when the da-watcher fills the gap. The sequencer's
/// origin-gap counter is not a baseline: it resets with the sequencer.
const EPOCHS_REPUBLISHED: &str = "kardamom_da_watcher_epochs_republished_total";
/// The ingress reader counter, by layer and outcome. The receipt layer
/// counts a `state_hit` when a receipt the memory cache does not hold
/// comes from an executor's state DB.
const CACHE_LOOKUPS: &str = "kardamom_cache_lookups_total";
const STATE_HIT: &str = "outcome=\"state_hit\"";
/// The sequencer's count of parks that asked for a floor, and its count
/// of lookups by outcome. `ok` is an executor's answer, `redis` the
/// projection's.
const LOOKUP_REQUESTS: &str = "kardamom_sequencer_nonce_lookup_requests_total";
const LOOKUPS: &str = "kardamom_sequencer_nonce_lookups_total";
const FROM_EXECUTOR: &str = "outcome=\"ok\"";
const FROM_REDIS: &str = "outcome=\"redis\"";
/// The Raft members. Every one is live after the return, and every one
/// must log a leadership term.
const SEALER_MEMBERS: u64 = 3;
/// How long a receipted transfer gets to execute before the fault.
const TRANSFER_BUDGET: Duration = Duration::from_secs(60);

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

/// What the checks compare against: the readings before the fault, and
/// the transfer a check asks the receipt of after it.
pub(super) struct Baseline {
    refills: Refills,
    /// The finished rebuilds in the state-mirror logs.
    rebuilds: usize,
    /// A transfer receipted through ingress-0 before the fault. Its
    /// receipt lives in the memory caches of the ingresses, which their
    /// restart empties, so a restarted ingress can serve it only from an
    /// executor's state.
    receipted: Option<String>,
}

impl Baseline {
    /// Read every baseline, and send the transfer when a check asks for
    /// its receipt. The transfer spends the gate account, which no case
    /// load spends and which every probe reads live.
    pub(super) async fn read(h: &Harness, ctx: &str, checks: &[Check]) -> anyhow::Result<Self> {
        let receipted = if checks.contains(&Check::ReceiptFromState) {
            Some(Self::receipted_transfer(h, ctx).await?)
        } else {
            None
        };
        Ok(Self {
            refills: Refills::read(h).await,
            rebuilds: mirror_rebuilds(h).await?,
            receipted,
        })
    }

    async fn receipted_transfer(h: &Harness, ctx: &str) -> anyhow::Result<String> {
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        let account = h.knobs.gate_account;
        let nonce = rpc.nonce_of(account).await?;
        let hash = rpc.transfer_hash(account, nonce, TRANSFER_BUDGET).await?;
        crate::log(format!(
            "{ctx}: transfer {hash} receipted before the fault; only the memory caches of the ingresses hold its receipt"
        ));
        Ok(hash)
    }

    /// Every restarted ingress serves the receipt of the transfer from
    /// an executor's state: the memory caches lost it.
    async fn assert_receipt_from_state(&self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let hash = self
            .receipted
            .as_deref()
            .ok_or_else(|| crate::chaos_fail!("{ctx}: no transfer was sent before the fault"))?;
        let probe = ReceiptProbe { h, ctx, hash };
        for node in &h.probes.ingresses {
            probe.assert_served_by(node).await?;
        }
        Ok(())
    }
}

/// The receipt of one transfer, asked of the restarted ingresses.
struct ReceiptProbe<'a> {
    h: &'a Harness,
    ctx: &'a str,
    hash: &'a str,
}

impl ReceiptProbe<'_> {
    /// The receipts the ingress on `node` served from an executor's state
    /// DB. The counter lives in the ingress process, so it starts at zero
    /// with a restarted ingress. An ingress that does not answer counts as
    /// zero.
    async fn state_hits(&self, node: &Probed) -> i64 {
        let probes = &self.h.probes;
        let body = probes.scrape().fetch(&probes.ingress_target(node)).await;
        body.and_then(|b| crate::metrics::sum_where(&b, CACHE_LOOKUPS, STATE_HIT))
            .unwrap_or(0)
    }

    /// The restarted ingress on `node` serves a receipt with status `0x1`
    /// within a minute, and it served a receipt from an executor's state
    /// since its start.
    async fn assert_served_by(&self, node: &Probed) -> anyhow::Result<()> {
        let (ctx, hash) = (self.ctx, self.hash);
        let rpc = Rpc::new(&node.rpc_url(), self.h.knobs.chain_id)?;
        let rpc = &rpc;
        let outcome = poll::until(Budget::secs(60, 3), |_| async move {
            Ok::<_, anyhow::Error>(rpc.receipt_status(hash).await.ok().flatten())
        })
        .await?;
        let (status, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: {} serves no receipt for {hash} {}s after its restart: the state query did not fill the lost cache",
                node.container,
                t.as_secs()
            )
        })?;
        anyhow::ensure!(
            status == "0x1",
            "{}: {ctx}: {} serves {hash} with status {status}",
            crate::FAIL_PREFIX,
            node.container
        );
        let hits = self.state_hits(node).await;
        anyhow::ensure!(
            hits > 0,
            "{}: {ctx}: {} serves {hash} with no state query since its restart: its empty memory cache cannot hold that receipt",
            crate::FAIL_PREFIX,
            node.container
        );
        crate::log(format!(
            "{ctx}: {} serves the receipt of {hash} from an executor's state after {}s ({hits} state hits since its restart)",
            node.container,
            elapsed.as_secs()
        ));
        Ok(())
    }
}

/// The members that led each leadership term, and the members that
/// logged a term, from the `cluster TERM` lines of every member. The
/// members replay the same log, so every member names the same leader for
/// a term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Terms {
    pub(super) leaders: BTreeMap<u64, BTreeSet<u64>>,
    pub(super) reporters: BTreeSet<u64>,
}

impl Terms {
    pub(super) fn parse(logs: &str) -> Self {
        let lines: Vec<&str> = logs
            .lines()
            .filter(|l| l.contains("cluster TERM "))
            .collect();
        let leaders = lines
            .iter()
            .filter_map(|l| {
                number_after(l, "leadershipTermId=").zip(number_after(l, "leaderMemberId="))
            })
            .fold(BTreeMap::new(), |mut terms, (term, leader)| {
                terms
                    .entry(term)
                    .or_insert_with(BTreeSet::new)
                    .insert(leader);
                terms
            });
        let reporters = lines
            .iter()
            .filter_map(|l| number_after(l, " memberId="))
            .collect();
        Self { leaders, reporters }
    }

    /// The terms that more than one member led.
    pub(super) fn split(&self) -> Vec<String> {
        self.leaders
            .iter()
            .filter(|(_, leaders)| leaders.len() > 1)
            .map(|(term, leaders)| format!("term {term}: members {leaders:?}"))
            .collect()
    }

    /// The members `0..members` that logged no term. A member whose log
    /// reads empty cannot prove that it named the same leader.
    pub(super) fn silent(&self, members: u64) -> Vec<u64> {
        (0..members)
            .filter(|m| !self.reporters.contains(m))
            .collect()
    }

    /// Every one of `members` live members logged a term, and at most one
    /// member led each term.
    pub(super) fn assert_one_leader(&self, ctx: &str, members: u64) -> anyhow::Result<()> {
        let silent = self.silent(members);
        anyhow::ensure!(
            silent.is_empty(),
            "{}: {ctx}: members {silent:?} logged no leadership term, so their leaders are not known",
            crate::FAIL_PREFIX
        );
        let split = self.split();
        anyhow::ensure!(
            split.is_empty(),
            "{}: {ctx}: two members led one leadership term ({})",
            crate::FAIL_PREFIX,
            split.join("; ")
        );
        crate::log(format!(
            "{ctx}: one leader per leadership term over {} terms, from all {members} members",
            self.leaders.len()
        ));
        Ok(())
    }
}

/// The floor lookups of one lane-0 replica: the parks that asked, and
/// the answers from an executor or from Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Lookups {
    pub(super) asked: i64,
    pub(super) answered: i64,
}

impl Lookups {
    /// The lookups of the lane-0 replica on sequencer node `i`. `None`
    /// when the exporter does not answer.
    async fn read(h: &Harness, i: usize) -> Option<Self> {
        let body = h
            .probes
            .scrape()
            .fetch(&h.probes.sequencer_lane0_target(i))
            .await?;
        let count = |label| crate::metrics::sum_where(&body, LOOKUPS, label).unwrap_or(0);
        Some(Self {
            asked: crate::metrics::sum_where(&body, LOOKUP_REQUESTS, "").unwrap_or(0),
            answered: count(FROM_EXECUTOR).saturating_add(count(FROM_REDIS)),
        })
    }

    /// A replica that restarted with no floor parked a sender and asked
    /// for its floor, and a source answered: the floor was looked up,
    /// not guessed.
    pub(super) fn looked_up(self) -> bool {
        self.asked > 0 && self.answered > 0
    }
}

impl Lookups {
    /// The lookups of every lane-0 replica, by sequencer node.
    async fn read_lanes(h: &Harness) -> Vec<Option<Self>> {
        let mut lanes = Vec::with_capacity(h.probes.sequencers.len());
        for i in 0..h.probes.sequencers.len() {
            lanes.push(Self::read(h, i).await);
        }
        lanes
    }

    /// Every lane-0 replica asked for a floor since its restart and got
    /// one from an executor or from Redis. The counters start at zero with
    /// the restarted process, so a positive count is a lookup of this case.
    async fn assert_every_lane(h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let outcome = poll::until(Budget::secs(120, 5), |_| async move {
            let lanes = Self::read_lanes(h).await;
            let all = lanes.iter().all(|l| l.is_some_and(Self::looked_up));
            Ok::<_, anyhow::Error>(all.then_some(lanes))
        })
        .await?;
        let (lanes, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: a lane-0 replica did not look a floor up within {}s of its return: it guessed one, or its sender never parked",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "{ctx}: every lane-0 replica parked and looked its floors up after {}s ({lanes:?})",
            elapsed.as_secs()
        ));
        Ok(())
    }
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
    /// Every ingress serves a receipt its memory cache lost, from an
    /// executor's state.
    ReceiptFromState,
    /// Every state mirror finished a rebuild.
    EveryMirrorRebuilt,
    /// The readers use Redis again with no degraded read.
    ReadersRecovered,
    /// Every lane-0 replica looked its floors up.
    FloorsLookedUp,
}

impl Check {
    pub(super) async fn assert(
        self,
        h: &Harness,
        ctx: &str,
        baseline: &Baseline,
    ) -> anyhow::Result<()> {
        match self {
            Self::NoOriginGap => assert_no_origin_gap(h, ctx).await,
            Self::OneLeaderPerTerm => Terms::parse(&h.evidence.cluster_logs().await?)
                .assert_one_leader(ctx, SEALER_MEMBERS),
            Self::NoRefBelowFloor => assert_no_ref_below_floor(h, ctx).await,
            Self::EpochRefill => baseline.refills.assert_rose(h, ctx).await,
            Self::ReceiptFromState => baseline.assert_receipt_from_state(h, ctx).await,
            Self::EveryMirrorRebuilt => wait_every_mirror_rebuilt(h, ctx, baseline.rebuilds).await,
            Self::ReadersRecovered => wait_readers_recovered(h, ctx).await,
            Self::FloorsLookedUp => Lookups::assert_every_lane(h, ctx).await,
        }
    }
}

//! The reads of the carry window on the sealer members and on the
//! sequencer replicas. A member serves `/status` and `/metrics` on its
//! admin port. A read that fails is `None`, never a value: the member may
//! start again.

use std::cell::{Cell, RefCell};
use std::time::{Duration, Instant};

use anyhow::Context;

use crate::harness::Harness;
use crate::metrics::{self, Target};
use crate::poll::{self, Budget};
use crate::probes::SEALER_ADMIN_PORT as ADMIN_PORT;

/// The member ids.
pub(super) const ALL: [u32; 3] = [0, 1, 2];

/// The sequencer lanes at shard map version 0.
const LANES: u8 = 2;

/// How long a member, a lane or a feature switch may take.
const WAIT: Budget = Budget {
    timeout: Duration::from_mins(3),
    interval: Duration::from_secs(1),
};

/// How often the switch of the leader is read. The leader dies within
/// 2 s after it switches, so the read must come well inside that.
const SWITCH_POLL: Budget = Budget {
    timeout: Duration::from_mins(3),
    interval: Duration::from_millis(100),
};

/// How often the hold of the probe transfer is read. The hold must come
/// before the lane leaves version 5, about one second after the upgrade
/// transaction is final.
const HOLD_POLL: Budget = Budget {
    timeout: Duration::from_secs(30),
    interval: Duration::from_millis(100),
};

/// One metric of a member, with the label fragment its samples carry.
#[derive(Debug, Clone, Copy)]
pub(super) struct Gauge {
    name: &'static str,
    label: &'static str,
}

pub(super) const DECISION_VERSION: Gauge = Gauge::of("kardamom_sealer_decision_version");
pub(super) const FULL_REF_BELOW_V5: Gauge = Gauge::of("kardamom_sealer_full_ref_below_v5_total");
pub(super) const REF_REFUSED: Gauge = Gauge::of("kardamom_sealer_ref_refused_total");
pub(super) const FEED_STATE: Gauge = Gauge::of("kardamom_sealer_feed_state");
const WINDOW_EXACT: Gauge = Gauge::of("kardamom_sealer_window_exact");
const RETAINED_FRAMES: Gauge = Gauge::of("kardamom_sealer_retained_frames");
const WINDOW_FEATURE: Gauge = Gauge {
    name: "kardamom_sealer_feature_state",
    label: "feature=\"5\"",
};

/// `kardamom_sealer_feed_state` of a feed that serves its executor live.
pub(super) const FEED_LIVE: i64 = 3;

impl Gauge {
    const fn of(name: &'static str) -> Self {
        Self { name, label: "" }
    }
}

/// The state of the carry window that every member reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Window {
    /// Feature 5 on: `carryWindow` true, the feature state 2, the window
    /// gauge 1, and the retained frames within `retention`.
    On,
    /// Feature 5 held with a later activation: `carryWindow` false, the
    /// feature state 1, and the window gauge 0.
    Off,
}

impl Window {
    pub(super) fn carry_window(self) -> bool {
        self == Self::On
    }

    pub(super) fn feature_state(self) -> i64 {
        match self {
            Self::On => 2,
            Self::Off => 1,
        }
    }

    pub(super) fn exact(self) -> i64 {
        match self {
            Self::On => 1,
            Self::Off => 0,
        }
    }
}

/// The admin endpoints of the members.
pub(super) struct Members<'a> {
    h: &'a Harness,
    http: reqwest::Client,
}

impl<'a> Members<'a> {
    pub(super) fn new(h: &'a Harness) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("build the admin client")?;
        Ok(Self { h, http })
    }

    fn admin(&self, id: u32) -> anyhow::Result<Target> {
        let node = self.h.contract.node(&format!("sealer-{id}"))?;
        Ok(Target::bridged(node.ip, &node.container, ADMIN_PORT))
    }

    /// The `/status` of member `id`, or `None` when it does not answer.
    async fn status(&self, id: u32) -> Option<serde_json::Value> {
        let node = self.h.contract.node(&format!("sealer-{id}")).ok()?;
        let url = format!("http://{}:{ADMIN_PORT}/status", node.ip);
        let answer = self.http.get(url).send().await.ok()?;
        answer.json().await.ok()
    }

    /// `gauge` on member `id`, summed over its samples.
    async fn read(&self, id: u32, gauge: Gauge) -> Option<i64> {
        let body = self.h.probes.scrape().fetch(&self.admin(id).ok()?).await?;
        metrics::sum_where(&body, gauge.name, gauge.label)
    }

    async fn reads(&self, ids: &[u32], gauge: Gauge) -> Vec<Option<i64>> {
        let mut values = Vec::with_capacity(ids.len());
        for id in ids {
            values.push(self.read(*id, gauge).await);
        }
        values
    }

    /// `gauge` on every member.
    ///
    /// # Errors
    ///
    /// Returns an error when a member does not answer.
    pub(super) async fn counts(&self, gauge: Gauge) -> anyhow::Result<Vec<i64>> {
        let values = self.reads(&ALL, gauge).await;
        values
            .iter()
            .copied()
            .collect::<Option<Vec<i64>>>()
            .ok_or_else(|| {
                crate::chaos_fail!("{} does not answer on every member: {values:?}", gauge.name)
            })
    }

    /// Wait until `want` holds for `gauge` on each member of `ids`.
    ///
    /// # Errors
    ///
    /// Returns an error when the wait ends first.
    pub(super) async fn await_all(
        &self,
        ids: &[u32],
        gauge: Gauge,
        want: impl Fn(i64) -> bool,
    ) -> anyhow::Result<()> {
        let last = RefCell::new(Vec::new());
        let (want, last_ref) = (&want, &last);
        let outcome = poll::until(WAIT, |_| async move {
            let values = self.reads(ids, gauge).await;
            let done = values.iter().all(|v| v.is_some_and(want));
            *last_ref.borrow_mut() = values;
            Ok(done.then_some(()))
        })
        .await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "{}{{{}}} on members {ids:?} not as wanted within {}s: {:?}",
                    gauge.name,
                    gauge.label,
                    t.as_secs(),
                    last.borrow()
                )
            })
            .map(|_| ())
    }

    /// Wait until each member of `ids` reports `carryWindow` as `on`.
    ///
    /// # Errors
    ///
    /// Returns an error when the wait ends first.
    pub(super) async fn await_carry_window(&self, ids: &[u32], on: bool) -> anyhow::Result<()> {
        let outcome = poll::until(WAIT, |_| async move {
            let mut done = true;
            for id in ids {
                done &= self.carry_window(*id).await == Some(on);
            }
            Ok(done.then_some(()))
        })
        .await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "carryWindow is not {on} on members {ids:?} within {}s",
                    t.as_secs()
                )
            })
            .map(|_| ())
    }

    async fn carry_window(&self, id: u32) -> Option<bool> {
        self.status(id).await?["carryWindow"].as_bool()
    }

    /// Wait until every member reports the window `state`.
    ///
    /// # Errors
    ///
    /// Returns an error when a member does not report it in time.
    pub(super) async fn await_window(&self, state: Window, retention: i64) -> anyhow::Result<()> {
        self.await_carry_window(&ALL, state.carry_window()).await?;
        self.await_all(&ALL, WINDOW_FEATURE, |v| v == state.feature_state())
            .await?;
        self.await_all(&ALL, WINDOW_EXACT, |v| v == state.exact())
            .await?;
        match state {
            Window::On => {
                self.await_all(&ALL, RETAINED_FRAMES, |v| v <= retention)
                    .await
            }
            Window::Off => Ok(()),
        }
    }

    /// The member whose `/status` names it the leader.
    ///
    /// # Errors
    ///
    /// Returns an error when no member is the leader within the leader
    /// SLO.
    pub(super) async fn leader(&self) -> anyhow::Result<u32> {
        let budget = Budget::new(self.h.knobs.leader_slo, Duration::from_secs(1));
        let outcome = poll::until(budget, |_| async move {
            let mut leader = None;
            for id in ALL {
                leader = leader.or(self.leads(id).await.then_some(id));
            }
            Ok(leader)
        })
        .await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!("no member reports role LEADER within {}s", t.as_secs())
            })
            .map(|(id, _)| id)
    }

    async fn leads(&self, id: u32) -> bool {
        self.status(id)
            .await
            .is_some_and(|s| s["role"].as_str() == Some("LEADER"))
    }

    /// Wait until member `id` reports `carryWindow` true, and return the
    /// moment of that read. The first read must report false: then the
    /// switch came less than one poll interval before the returned moment.
    ///
    /// # Errors
    ///
    /// Returns an error when the first read does not report false, or
    /// when the wait ends first.
    pub(super) async fn await_switch(&self, id: u32) -> anyhow::Result<Instant> {
        let first = Cell::new(None);
        let first_ref = &first;
        let outcome = poll::until(SWITCH_POLL, |_| async move {
            let on = self.carry_window(id).await;
            first_ref.set(first_ref.get().or(Some(on)));
            Ok((on == Some(true)).then(Instant::now))
        })
        .await?;
        let (at, _) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "member {id} did not switch the carry window on within {}s",
                t.as_secs()
            )
        })?;
        anyhow::ensure!(
            first.get() == Some(Some(false)),
            "{}: the first read of member {id} reported carryWindow {:?}, not false, so the time of its switch is unknown",
            crate::FAIL_PREFIX,
            first.get().flatten()
        );
        Ok(at)
    }
}

/// The sequencer replicas: one replica of each lane on each sequencer
/// node.
pub(super) struct Lanes<'a> {
    h: &'a Harness,
}

/// The decision version that a sequencer keeps.
const SEQ_SEALER_VERSION: &str = "kardamom_sequencer_sealer_version";
/// The transactions that the v2 loop holds for a nonce gap.
const SEQ_PENDING_DEPTH: &str = "kardamom_sequencer_pending_depth";

impl<'a> Lanes<'a> {
    pub(super) fn new(h: &'a Harness) -> Self {
        Self { h }
    }

    /// `metric`, summed over its samples, on each replica of `lane`.
    async fn lane(&self, lane: u8, metric: &str) -> Vec<Option<i64>> {
        let mut values = Vec::with_capacity(self.h.probes.sequencers.len());
        for i in 0..self.h.probes.sequencers.len() {
            values.push(self.replica(i, lane, metric).await);
        }
        values
    }

    async fn replica(&self, i: usize, lane: u8, metric: &str) -> Option<i64> {
        let target = self.h.probes.sequencer_lane_target(i, lane);
        let body = self.h.probes.scrape().fetch(&target).await?;
        metrics::sum(&body, metric)
    }

    async fn every_lane(&self, metric: &str) -> Vec<Option<i64>> {
        let mut values = Vec::new();
        for lane in 0..LANES {
            values.extend(self.lane(lane, metric).await);
        }
        values
    }

    /// Wait until every replica of every lane keeps `version`.
    ///
    /// # Errors
    ///
    /// Returns an error when the wait ends first.
    pub(super) async fn await_version(&self, version: i64) -> anyhow::Result<()> {
        let outcome = poll::until(WAIT, |_| async move {
            let values = self.every_lane(SEQ_SEALER_VERSION).await;
            Ok(values.iter().all(|v| *v == Some(version)).then_some(()))
        })
        .await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "not every sequencer replica keeps decision version {version} within {}s",
                    t.as_secs()
                )
            })
            .map(|_| ())
    }

    /// Whether a replica of `lane` holds a transaction for a nonce gap.
    pub(super) async fn holds(&self, lane: u8) -> bool {
        let values = self.lane(lane, SEQ_PENDING_DEPTH).await;
        values.iter().any(|v| v.is_some_and(|d| d > 0))
    }

    /// Whether every replica of `lane` keeps `version`.
    pub(super) async fn keeps(&self, lane: u8, version: i64) -> bool {
        let values = self.lane(lane, SEQ_SEALER_VERSION).await;
        values.iter().all(|v| *v == Some(version))
    }

    /// Wait until a replica of `lane` holds a transaction for a nonce gap.
    ///
    /// # Errors
    ///
    /// Returns an error when the wait ends first.
    pub(super) async fn await_held(&self, lane: u8) -> anyhow::Result<()> {
        let outcome = poll::until(HOLD_POLL, |_| async move {
            Ok(self.holds(lane).await.then_some(()))
        })
        .await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "no replica of lane {lane} holds the transfer for its nonce gap within {}s",
                    t.as_secs()
                )
            })
            .map(|_| ())
    }
}

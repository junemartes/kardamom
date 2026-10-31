//! The release check before an upgrade transaction. It reads the same
//! facts as `just sealer-upgrade-check`
//! (`deploy/cluster/ansible/roles/workloads/tasks/upgrade_check.yml`)
//! straight from Nomad and from the admin and metrics pages, and applies
//! the same refusal rules. The play runs about 2 s per task on the CI
//! runner, and one check is 49 to 94 tasks.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::Context;
use serde_json::Value;

use super::egress::{CURSORS, EGRESS_METRICS_PORT, REF_MAX_BLOCK, REF_MAX_INDEX};
use super::upgrade::Upgrade;
use crate::harness::Harness;
use crate::metrics::{self, Target};
use crate::nomad::{Alloc, SavedJob};
use crate::probes::{BATCHER_PORT, SEALER_ADMIN_PORT, VALIDATOR_PORT};

/// The sealer job.
const SEALER_JOB: &str = "cluster";

/// The state of feature 5 on a member: 1 held, 2 on.
const FEATURE_STATE: &str = "kardamom_sealer_feature_state";
const FEATURE_FIVE: &str = "feature=\"5\"";

/// The snapshot version that every member of this release reads.
const READS_RELEASE: i64 = 14;

/// The snapshot version that every member must read before each feature
/// with its own rule in the play.
const READS_FOR_FEATURE: [(u64, i64); 3] = [(3, 15), (4, 16), (5, 18)];

impl Upgrade {
    /// The snapshot version that every member must read before this
    /// upgrade.
    fn reads_needed(self) -> i64 {
        READS_FOR_FEATURE
            .iter()
            .find(|(f, _)| *f == self.feature())
            .map_or(READS_RELEASE, |(_, reads)| *reads)
    }
}

/// One job of the check: every job meta of `metas` is `true` on the
/// registered job, and every live allocation runs that job version
/// (`upgrade_check_job.yml`).
#[derive(Debug, Clone, Copy)]
pub(super) struct Need {
    job: &'static str,
    metas: &'static [&'static str],
}

/// `workloads_upgrade_jobs`: the services of the release.
const RELEASE_JOBS: [Need; 4] = [
    Need::of("sequencer", &["nonce_burn"]),
    Need::of("executor", &["nonce_burn", "chain_state_install"]),
    Need::of("validator", &["nonce_burn", "chain_state_install"]),
    Need::of("ingress", &["nonce_burn"]),
];

/// `workloads_upgrade_v4_jobs`: the services of decision version 4.
const V4_JOBS: [Need; 3] = [
    Need::of("sequencer", &["sealer_v4"]),
    Need::of("executor", &["first_receipt"]),
    Need::of("validator", &["first_receipt"]),
];

/// `workloads_upgrade_window_jobs` without the egress: the services of
/// the carry window.
const WINDOW_JOBS: [Need; 4] = [
    Need::of("sequencer", &["sealer_version_kept"]),
    Need::of("sequencer", &["v2_loop"]),
    Need::of("batcher", &["reads_egress"]),
    Need::of("validator", &["reads_egress"]),
];

/// The egress of `workloads_upgrade_window_jobs`: it keeps the mark of
/// the highest stored reference.
const EGRESS_JOB: Need = Need::of("egress", &["ref_mark"]);

impl Need {
    const fn of(job: &'static str, metas: &'static [&'static str]) -> Self {
        Self { job, metas }
    }

    /// The refusal of this job, or `None` when it passes.
    fn refusal(self, jobs: &BTreeMap<&'static str, Option<JobFacts>>) -> Option<String> {
        let Some(Some(facts)) = jobs.get(self.job) else {
            return Some(format!("Nomad does not know the {} job", self.job));
        };
        let missing: Vec<&str> = self
            .metas
            .iter()
            .copied()
            .filter(|m| !facts.metas.contains(*m))
            .collect();
        let versions: BTreeSet<u64> = facts.live.iter().copied().collect();
        let current = versions.len() == 1 && versions.contains(&facts.version);
        (!missing.is_empty() || !current).then(|| {
            format!(
                "the {} job lacks the job meta {missing:?}, no {} runs, or a live allocation runs an older job version (live {versions:?}, registered {})",
                self.job, self.job, facts.version
            )
        })
    }
}

/// The registered version of a job, its metas that are `true`, and the
/// job versions of its live (running or pending) allocations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct JobFacts {
    version: u64,
    metas: BTreeSet<String>,
    live: Vec<u64>,
}

impl JobFacts {
    fn of(definition: &Value, allocs: &[Alloc]) -> Self {
        let metas = definition["Meta"]
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(_, v)| v.as_str() == Some("true"))
            .map(|(k, _)| k.clone())
            .collect();
        Self {
            version: definition["Version"].as_u64().unwrap_or_default(),
            metas,
            live: allocs
                .iter()
                .filter(|a| matches!(a.client_status.as_str(), "running" | "pending"))
                .map(|a| a.job_version)
                .collect(),
        }
    }
}

/// The admin status of one running member (`sealer_status.yml`). A
/// field that the status lacks takes the default of the play.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MemberStatus {
    reads_max: i64,
    version: i64,
    store: String,
}

impl MemberStatus {
    fn of(status: &Value) -> Self {
        Self {
            reads_max: status["snapshotReadsMax"].as_i64().unwrap_or(0),
            version: status["decisionVersion"].as_i64().unwrap_or(0),
            store: status["retainedStore"]
                .as_str()
                .unwrap_or("none")
                .to_string(),
        }
    }
}

/// The facts of feature 5: the reference mark of each running egress
/// instance, and the egress cursor of each running batcher and validator.
/// A mark is `None` when its page does not answer or lacks a gauge. A
/// cursor is `None` when its page does not answer, and -1 when the page
/// lacks the gauge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct WindowFacts {
    marks: Vec<Option<(i64, i64)>>,
    batcher: Vec<Option<i64>>,
    validator: Vec<Option<i64>>,
}

/// Everything that the check reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Facts {
    /// The task groups of the registered sealer job; `None` when Nomad
    /// does not know the job.
    groups: Option<Vec<String>>,
    /// The status of each running member, by task group; `None` when its
    /// admin page does not answer.
    members: BTreeMap<String, Option<MemberStatus>>,
    jobs: BTreeMap<&'static str, Option<JobFacts>>,
    /// The state of feature 5 on each running member, for an upgrade
    /// that can turn version 5 off. `None` is a page that does not answer,
    /// and -1 a member that reports no state: it holds no feature 5.
    held: Vec<Option<i64>>,
    window: WindowFacts,
}

impl Facts {
    /// Every refusal of `upgrade`, in the order of the play. An empty
    /// list passes.
    pub(super) fn refusals(&self, upgrade: Upgrade) -> Vec<String> {
        let feature = upgrade.feature();
        let at_once = (feature == 2 && !upgrade.at_once())
            .then(|| "feature 2 allows only the activation time 0".to_string());
        let Some(groups) = &self.groups else {
            return at_once
                .into_iter()
                .chain(["Nomad does not know the sealer job".to_string()])
                .collect();
        };
        let v4: &[Need] = if feature == 3 { &V4_JOBS } else { &[] };
        at_once
            .into_iter()
            .chain(self.member_refusals(groups, upgrade))
            .chain(self.window_refusals(upgrade))
            .chain(self.held_refusal(upgrade))
            .chain(
                RELEASE_JOBS
                    .iter()
                    .chain(v4)
                    .filter_map(|need| need.refusal(&self.jobs)),
            )
            .collect()
    }

    /// Every configured member runs and answers, and reads the snapshot
    /// version that the feature needs.
    fn member_refusals(&self, groups: &[String], upgrade: Upgrade) -> Vec<String> {
        let down: Vec<&String> = groups
            .iter()
            .filter(|g| !self.members.contains_key(*g))
            .collect();
        let silent: Vec<&String> = self
            .members
            .iter()
            .filter(|(_, s)| s.is_none())
            .map(|(g, _)| g)
            .collect();
        let statuses: Vec<&MemberStatus> = self.members.values().flatten().collect();
        let reads_max = statuses.iter().map(|s| s.reads_max).min().unwrap_or(0);
        let needed = upgrade.reads_needed();
        let window = upgrade.feature() == 5;
        [
            (!down.is_empty()).then(|| format!("the sealer members {down:?} do not run")),
            (!silent.is_empty())
                .then(|| format!("the sealer members {silent:?} do not answer /status")),
            (reads_max < needed).then(|| {
                format!(
                    "a sealer member reads only up to snapshot version {reads_max}; feature {} needs {needed}",
                    upgrade.feature()
                )
            }),
            (window && statuses.iter().any(|s| s.version != 5))
                .then(|| "feature 5 needs decision version 5 on every member".to_string()),
            (window && statuses.iter().any(|s| s.store != "heap"))
                .then(|| "feature 5 needs the retained frames on the heap of every member".to_string()),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// Feature 5: the services of the window, the egress mark, and every
    /// reader cursor past the last stored reference.
    fn window_refusals(&self, upgrade: Upgrade) -> Vec<String> {
        if upgrade.feature() != 5 {
            return Vec::new();
        }
        let mut refused: Vec<String> = WINDOW_JOBS
            .iter()
            .chain([&EGRESS_JOB])
            .filter_map(|need| need.refusal(&self.jobs))
            .collect();
        let marks: Option<Vec<(i64, i64)>> = self.window.marks.iter().copied().collect();
        let Some(marks) =
            marks.filter(|m| !m.is_empty() && m.iter().all(|(i, b)| *i >= 0 && *b >= 0))
        else {
            let unmarked = format!(
                "an egress instance does not report its reference mark, or no egress runs: {:?}",
                self.window.marks
            );
            return refused.into_iter().chain([unmarked]).collect();
        };
        let r = marks.iter().map(|(index, _)| *index).max().unwrap_or(0);
        refused.extend(
            [("batcher", &self.window.batcher), ("validator", &self.window.validator)]
                .into_iter()
                .filter(|(_, cursors)| {
                    cursors.is_empty() || cursors.iter().any(|c| c.is_none_or(|c| c <= r))
                })
                .map(|(reader, cursors)| {
                    format!("the {reader} cursors {cursors:?} are not all above the index {r} of the last stored reference")
                }),
        );
        refused
    }

    /// Feature 3 or 4 with a later activation can turn version 5 off: no
    /// member may hold feature 5, and every member page must answer.
    fn held_refusal(&self, upgrade: Upgrade) -> Option<String> {
        let offs = matches!(upgrade.feature(), 3 | 4) && !upgrade.at_once();
        (offs && self.held.iter().any(|s| s.is_none_or(|s| s >= 1))).then(|| {
            format!(
                "feature {} with a later activation can turn version 5 off, and a member holds feature 5 or does not answer: {:?}",
                upgrade.feature(),
                self.held
            )
        })
    }
}

/// The reads of the check.
pub(super) struct UpgradeCheck<'a> {
    h: &'a Harness,
    http: reqwest::Client,
}

impl<'a> UpgradeCheck<'a> {
    pub(super) fn new(h: &'a Harness) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("build the admin client")?;
        Ok(Self { h, http })
    }

    /// Read every fact that the check of `upgrade` needs.
    ///
    /// # Errors
    ///
    /// Returns an error when Nomad does not list the allocations of a job.
    pub(super) async fn facts(&self, upgrade: Upgrade) -> anyhow::Result<Facts> {
        let sealer = self.job(SEALER_JOB).await?;
        let members = self.members().await?;
        let names: BTreeSet<&'static str> = RELEASE_JOBS
            .iter()
            .chain(&V4_JOBS)
            .chain(&WINDOW_JOBS)
            .chain([&EGRESS_JOB])
            .map(|n| n.job)
            .collect();
        let mut jobs = BTreeMap::new();
        for name in names {
            jobs.insert(name, self.job(name).await?.map(|(facts, _)| facts));
        }
        let held = if matches!(upgrade.feature(), 3 | 4) && !upgrade.at_once() {
            self.gauges(SEALER_JOB, SEALER_ADMIN_PORT, FEATURE_STATE, FEATURE_FIVE)
                .await?
        } else {
            Vec::new()
        };
        let window = if upgrade.feature() == 5 {
            self.window().await?
        } else {
            WindowFacts::default()
        };
        Ok(Facts {
            groups: sealer.map(|(_, groups)| groups),
            members,
            jobs,
            held,
            window,
        })
    }

    /// The facts of job `name` and its task group names, or `None` when
    /// Nomad does not know the job.
    async fn job(&self, name: &str) -> anyhow::Result<Option<(JobFacts, Vec<String>)>> {
        let Ok(saved) = SavedJob::capture(&self.h.nomad, name).await else {
            return Ok(None);
        };
        let definition = saved.definition();
        let allocs = self.h.nomad.allocations(name).await?;
        let groups = definition["TaskGroups"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|g| g["Name"].as_str().map(str::to_string))
            .collect();
        Ok(Some((JobFacts::of(definition, &allocs), groups)))
    }

    /// The running allocations of `job` that Nomad wants running.
    async fn running(&self, job: &str) -> anyhow::Result<Vec<Alloc>> {
        Ok(self
            .h
            .nomad
            .allocations(job)
            .await?
            .into_iter()
            .filter(|a| a.client_status == "running" && a.desired_status == "run")
            .collect())
    }

    /// The status of each running member, by task group.
    async fn members(&self) -> anyhow::Result<BTreeMap<String, Option<MemberStatus>>> {
        let mut members = BTreeMap::new();
        for alloc in self.running(SEALER_JOB).await? {
            members.insert(alloc.task_group.clone(), self.status(&alloc).await);
        }
        Ok(members)
    }

    async fn status(&self, alloc: &Alloc) -> Option<MemberStatus> {
        let node = self.h.contract.node(&alloc.node_name).ok()?;
        let url = format!("http://{}:{SEALER_ADMIN_PORT}/status", node.ip);
        let status: Value = self.http.get(url).send().await.ok()?.json().await.ok()?;
        Some(MemberStatus::of(&status))
    }

    /// The `/metrics` page of each running allocation of `job` on `port`.
    async fn pages(&self, job: &str, port: u16) -> anyhow::Result<Vec<Option<String>>> {
        let mut pages = Vec::new();
        for alloc in self.running(job).await? {
            pages.push(self.page(&alloc, port).await);
        }
        Ok(pages)
    }

    async fn page(&self, alloc: &Alloc, port: u16) -> Option<String> {
        let node = self.h.contract.node(&alloc.node_name).ok()?;
        let target = Target::bridged(node.ip, &node.container, port);
        self.h.probes.scrape().fetch(&target).await
    }

    /// The first sample of `gauge` with `label` on each running
    /// allocation of `job`: `None` when the page does not answer, and -1
    /// when the page lacks the gauge, as the play reads it.
    async fn gauges(
        &self,
        job: &str,
        port: u16,
        gauge: &str,
        label: &str,
    ) -> anyhow::Result<Vec<Option<i64>>> {
        Ok(self
            .pages(job, port)
            .await?
            .iter()
            .map(|page| {
                page.as_deref()
                    .map(|page| metrics::first_where(page, gauge, label).unwrap_or(-1))
            })
            .collect())
    }

    async fn window(&self) -> anyhow::Result<WindowFacts> {
        let marks = self
            .pages("egress", EGRESS_METRICS_PORT)
            .await?
            .iter()
            .map(|page| {
                let page = page.as_deref()?;
                Some((
                    metrics::first(page, REF_MAX_INDEX)?,
                    metrics::first(page, REF_MAX_BLOCK)?,
                ))
            })
            .collect();
        Ok(WindowFacts {
            marks,
            batcher: self
                .gauges("batcher", BATCHER_PORT, CURSORS.batcher, "")
                .await?,
            validator: self
                .gauges("validator", VALIDATOR_PORT, CURSORS.validator, "")
                .await?,
        })
    }
}

#[cfg(test)]
#[path = "check_tests.rs"]
mod tests;

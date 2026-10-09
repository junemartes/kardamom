//! The CI shards: each one brings up its own cluster and runs one slice
//! of the cases. The lists and the per-shard settings mirror the
//! `cluster-e2e` workflow, so a local shard run behaves like CI.

use crate::lifecycle::DeployVars;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shard {
    Executor,
    Ingress,
    Sequencer,
    Cluster,
    Fleet,
    Coordinated,
    CombinedOrdering,
    CombinedExec,
    Retention,
    Cache,
    L1,
    Integrity,
}

impl Shard {
    /// The shard name in the workflow matrix.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Executor => "chaos-executor",
            Self::Ingress => "chaos-ingress",
            Self::Sequencer => "chaos-sequencer",
            Self::Cluster => "chaos-cluster",
            Self::Fleet => "chaos-fleet",
            Self::Coordinated => "chaos-coordinated",
            Self::CombinedOrdering => "chaos-combined-ordering",
            Self::CombinedExec => "chaos-combined-exec",
            Self::Retention => "chaos-retention",
            Self::Cache => "chaos-cache",
            Self::L1 => "chaos-l1",
            Self::Integrity => "chaos-integrity",
        }
    }

    /// Whether `case` ends with the persisted-state stage. The L1 cases
    /// each leave a DA record a silent gap could hide in, so each one
    /// proves the rebuild from L1 before the next. The executor and
    /// sealer nodes die together in the first exec case, so the state
    /// every executor kept is compared with the validator's before the
    /// next case builds on it. The last case's stage is the one in the
    /// shard's tail.
    #[must_use]
    pub fn audits_after(self, case: &str) -> bool {
        match self {
            Self::L1 => true,
            Self::CombinedExec => case == "executor-sealer-loss-recover",
            _ => false,
        }
    }

    /// The cases of the shard, in run order. The sequencer shard runs
    /// the two resize cases last: a resize leaves the shard map at a
    /// later version, so the account-to-shard table no longer pins the
    /// cases after it.
    #[must_use]
    pub fn cases(self) -> &'static [&'static str] {
        match self {
            Self::Executor => &[
                "graceful-executor",
                "hard-executor",
                "node-failure-executor",
                "node-replace-executor",
                "state-checkpoint-restore",
                "replay-window-resync",
                "deploy-broken-image",
            ],
            Self::Ingress => &[
                "graceful-ingress",
                "hard-ingress",
                "archive-driver-loss",
                "archive-tx-data-wipe",
                "archive-corruption",
            ],
            Self::Sequencer => &[
                "graceful-sequencer",
                "hard-sequencer",
                "sequencer-replica-kill",
                "sequencer-lapse",
                "validator-lapse",
                "validator-join",
                "lookup-blackout",
                "resize-scale-out-in",
            ],
            Self::Cluster => &[
                "cluster-leader-kill",
                "cluster-follower-kill",
                "cluster-member-rejoin",
                "node-replace-sealer",
                "cpu-squeeze",
            ],
            // Every replica of one role down at once. Each case waits
            // for the whole fleet to return and then keeps the load on
            // it, so the shard runs its own cluster.
            // The sealer fleet wipe runs last: it restarts the chain from
            // a state rebuilt from L1, the longest recovery, and a
            // failure there must not hide the other cases.
            Self::Fleet => &[
                "executor-fleet-loss-recover",
                "executor-fleet-wipe-recover",
                "executor-fleet-total-wipe-recover",
                "redis-total-loss-recover",
                "cluster-quorum-loss-recover",
                "cluster-total-loss-recover",
                "sealer-fleet-total-wipe-recover",
            ],
            // Failures that cross the redundancy of a role. The blackout
            // runs last: it can leave a canonical entry whose transaction
            // data no archive serves, and the consumers then ask the
            // sealer to void that entry before the chain moves again.
            Self::Coordinated => &[
                "ingress-pair-loss-recover",
                "sequencer-lane-loss-recover",
                "pipeline-blackout-recover",
            ],
            // Two or three classes down at once, and the order of their
            // return. The pairs return in dependency order and against
            // it; the two triple cases take the sealers down with both
            // the sequencers and the ingresses, in both orders.
            Self::CombinedOrdering => &[
                "ingress-sequencer-loss-recover",
                "ingress-sealer-loss-recover",
                "sequencer-sealer-loss-recover",
                "ingress-sequencer-sealer-loss-recover",
                "ingress-sequencer-sealer-reverse",
            ],
            // The executors down with another class: the sealers, the
            // sealers and the validator, and the ingresses. The
            // executor-and-sealer case runs first and audits the persisted
            // state after it. The read-path and the sequencer-and-Redis
            // cases run by name only: a cold redis job crash-loops its
            // sentinels, and a sender sticks after an outage of every
            // executor.
            Self::CombinedExec => &[
                "executor-sealer-loss-recover",
                "executor-sealer-validator-recover",
                "ingress-executor-loss-recover",
            ],
            Self::Retention => &["retention-overrun", "retention-overrun-validator"],
            // A lying L1 in front of the followers. The outage past the
            // retention runs last: it holds the load until the sealers'
            // floor passes, the longest case, and a failure there must
            // not hide the liar cases.
            Self::L1 => &[
                "l1-liar",
                "l1-null-receipts",
                "two-day-outage",
                "batcher-outage-past-retention",
            ],
            // The mirror rebuild runs last: it flushes the projection.
            Self::Cache => &[
                "redis-partition-ingress",
                "redis-primary-kill",
                "redis-primary-freeze",
                "mirror-kill-rebuild",
            ],
            // The nightly shard: the long repetition cases, too slow for
            // every pull request.
            Self::Integrity => &["executor-restart-storm"],
        }
    }

    /// The deploy-time variables the shard's cluster needs. The cluster
    /// shard deploys the sealer with a short snapshot interval so the
    /// follower-kill case sees a snapshot; the retention shard deploys a
    /// small egress retention so a freeze can overrun it; the L1 shard
    /// takes both, plus the fault proxy in front of the followers. The
    /// executor shard turns the recorded cursor on, so `hard-executor`
    /// checks the sealer's best cursor while one executor is down.
    #[must_use]
    pub fn deploy_vars(self) -> DeployVars {
        match self {
            Self::Executor => DeployVars {
                exec_cursor: true,
                ..DeployVars::default()
            },
            Self::Cluster => DeployVars {
                cluster_snapshot_interval_s: Some(60),
                ..DeployVars::default()
            },
            Self::Retention => DeployVars {
                cluster_retention: Some(6144),
                ..DeployVars::default()
            },
            Self::L1 => DeployVars {
                cluster_snapshot_interval_s: Some(60),
                cluster_retention: Some(6144),
                l1_fault_proxy: true,
                indexer_poll_s: Some(2),
                ..DeployVars::default()
            },
            Self::Ingress
            | Self::Sequencer
            | Self::Fleet
            | Self::Coordinated
            | Self::CombinedOrdering
            | Self::Cache
            | Self::CombinedExec
            | Self::Integrity => DeployVars::default(),
        }
    }

    /// The settings of the shard beyond the shared chaos knobs, as
    /// `(name, value)` pairs, below the process environment. Every chaos
    /// shard runs no load stage (`RUN_LOAD=0`), which is what lets the
    /// resize case take a load-reserve account on the sequencer shard.
    #[must_use]
    pub fn env(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Cluster => &[
                ("RUN_LOAD", "0"),
                ("KARDAMOM_CLUSTER_SNAPSHOT_S", "60"),
                ("SQUEEZE_CYCLES", "3"),
                ("SQUEEZE_S", "60"),
                ("SQUEEZE_CPUS_PER_NODE", "0.4"),
            ],
            Self::Retention => &[("RUN_LOAD", "0"), ("KARDAMOM_CLUSTER_RETENTION", "6144")],
            // One minute per fault: every assertion holds at one minute,
            // and four cases with their state audits must fit the job's
            // budget.
            Self::L1 => &[
                ("RUN_LOAD", "0"),
                ("KARDAMOM_CLUSTER_RETENTION", "6144"),
                ("KARDAMOM_CLUSTER_SNAPSHOT_S", "60"),
                ("L1_FAULT_S", "60"),
            ],
            Self::Executor => &[("RUN_LOAD", "0"), ("KARDAMOM_EXEC_CURSOR", "on")],
            Self::Ingress
            | Self::Sequencer
            | Self::Fleet
            | Self::Coordinated
            | Self::CombinedOrdering
            | Self::Cache
            | Self::CombinedExec
            | Self::Integrity => &[("RUN_LOAD", "0")],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_shard_lists_distinct_cases_and_the_resize_runs_last() {
        let all: Vec<&str> = [
            Shard::Executor,
            Shard::Ingress,
            Shard::Sequencer,
            Shard::Cluster,
            Shard::Fleet,
            Shard::Coordinated,
            Shard::CombinedOrdering,
            Shard::CombinedExec,
            Shard::Retention,
            Shard::Cache,
            Shard::L1,
            Shard::Integrity,
        ]
        .iter()
        .flat_map(|s| s.cases().iter().copied())
        .collect();
        let mut unique = all.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(all.len(), unique.len(), "a case rides two shards");
        assert_eq!(all.len(), 54);
        assert_eq!(
            Shard::Sequencer.cases().last(),
            Some(&"resize-scale-out-in")
        );
        for shard in [
            Shard::Executor,
            Shard::Ingress,
            Shard::Sequencer,
            Shard::Cluster,
            Shard::Fleet,
            Shard::Coordinated,
            Shard::CombinedOrdering,
            Shard::CombinedExec,
            Shard::Retention,
            Shard::Cache,
            Shard::L1,
            Shard::Integrity,
        ] {
            assert!(
                shard.env().contains(&("RUN_LOAD", "0")),
                "{} runs no load stage",
                shard.name()
            );
        }
        assert!(Shard::L1.deploy_vars().l1_fault_proxy);
        // The executor shard deploys the recorded cursor and tells its
        // cases so through the knob of the same name.
        assert!(Shard::Executor.deploy_vars().exec_cursor);
        assert!(
            Shard::Executor
                .env()
                .contains(&("KARDAMOM_EXEC_CURSOR", "on"))
        );
        assert!(
            Shard::L1.audits_after("l1-liar")
                && !Shard::Retention.audits_after("retention-overrun")
        );
        assert!(Shard::CombinedExec.audits_after("executor-sealer-loss-recover"));
        assert!(!Shard::CombinedExec.audits_after("ingress-executor-loss-recover"));
        assert_eq!(
            Shard::CombinedExec.cases().first(),
            Some(&"executor-sealer-loss-recover")
        );
        assert_eq!(
            Shard::L1.cases().last(),
            Some(&"batcher-outage-past-retention")
        );
        assert_eq!(
            Shard::Fleet.cases().last(),
            Some(&"sealer-fleet-total-wipe-recover")
        );
        assert_eq!(Shard::Integrity.cases(), ["executor-restart-storm"]);
        assert_eq!(Shard::Integrity.name(), "chaos-integrity");
    }
}

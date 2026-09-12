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
    Retention,
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
            Self::Retention => "chaos-retention",
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
                "state-checkpoint-restore",
                "replay-window-resync",
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
                "cluster-quorum-loss-recover",
                "cpu-squeeze",
            ],
            Self::Retention => &["retention-overrun", "retention-overrun-validator"],
        }
    }

    /// The deploy-time variables the shard's cluster needs. The cluster
    /// shard deploys the sealer with a short snapshot interval so the
    /// follower-kill case sees a snapshot; the retention shard deploys a
    /// small egress retention so a freeze can overrun it.
    #[must_use]
    pub fn deploy_vars(self) -> DeployVars {
        match self {
            Self::Cluster => DeployVars {
                cluster_snapshot_interval_s: Some(60),
                cluster_retention: None,
            },
            Self::Retention => DeployVars {
                cluster_snapshot_interval_s: None,
                cluster_retention: Some(6144),
            },
            Self::Executor | Self::Ingress | Self::Sequencer => DeployVars::default(),
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
            Self::Executor | Self::Ingress | Self::Sequencer => &[("RUN_LOAD", "0")],
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
            Shard::Retention,
        ]
        .iter()
        .flat_map(|s| s.cases().iter().copied())
        .collect();
        let mut unique = all.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(all.len(), unique.len(), "a case rides two shards");
        assert_eq!(all.len(), 25);
        assert_eq!(
            Shard::Sequencer.cases().last(),
            Some(&"resize-scale-out-in")
        );
        for shard in [
            Shard::Executor,
            Shard::Ingress,
            Shard::Sequencer,
            Shard::Cluster,
            Shard::Retention,
        ] {
            assert!(
                shard.env().contains(&("RUN_LOAD", "0")),
                "{} runs no load stage",
                shard.name()
            );
        }
    }
}

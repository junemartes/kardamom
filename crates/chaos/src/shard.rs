//! The CI shards: each one brings up its own cluster and runs one slice
//! of the cases. The lists and the per-shard settings mirror the
//! `cluster-e2e` workflow, so a local shard run behaves like CI.
//!
//! Every regular shard brings up a carry-born cluster
//! (`KARDAMOM_GENESIS_CARRY=on`): the chain starts with the carry window
//! on, and the executors run in carry mode on the sealer nodes. Two shards
//! stay reference-born: `chaos-carry` tests the upgrade of a live chain
//! to the carry window, and the nightly `chaos-integrity` keeps the
//! reference mode, the recorded cursor, the voids and the seeded restart
//! of the sealer fleet under test.

use crate::knobs::Knobs;
use crate::lifecycle::{CarrySwitches, DeployVars, EgressStage};

/// The sealer retention of the carry shard, in frames. The exact window
/// keeps this many frames.
const CARRY_RETENTION: u64 = 16_384;
/// The sealer retention of the retention and L1 shards, in frames. A
/// freeze must pass twice this count to overrun the window: about 45 s
/// at the steady load, and 85 s at the L1 load.
const SMALL_RETENTION: u64 = 2048;
/// The snapshot interval of the retention and L1 shards, in seconds. A
/// floor pass waits for one snapshot, so the interval bounds that wait.
const SHORT_SNAPSHOT_S: u64 = 30;
const RETENTION_CASES: [&str; 5] = [
    "retention-overrun",
    "retention-overrun-validator",
    "prune-floor",
    "da-lag-halt",
    "canary-da-lag",
];

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
    Follower,
    Integrity,
    Carry,
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
            Self::Follower => "chaos-follower",
            Self::Integrity => "chaos-integrity",
            Self::Carry => "chaos-carry",
        }
    }

    /// Whether `case` ends with the persisted-state stage. The L1 and
    /// follower cases have none: each one proves inside its body that the
    /// DA record on L1 is contiguous and that the archive holds it, and
    /// the stage in the shard's tail rebuilds the whole chain from L1, so
    /// a gap that any case leaves fails that rebuild. The executors and the
    /// members die together in the first exec case, so the state every
    /// executor kept is compared with the validator's before the next
    /// case builds on it. The carry shard compares the state of the
    /// first executor in carry mode with the executors in reference
    /// mode. The last case's stage is the one in the shard's tail.
    #[must_use]
    pub fn audits_after(self, case: &str) -> bool {
        match self {
            Self::CombinedExec => case == "executor-sealer-loss-recover",
            Self::Carry => case == "carry-executor-first",
            _ => false,
        }
    }

    const EXECUTOR_CASES: &'static [&'static str] = &[
        "graceful-executor",
        "hard-executor",
        "node-failure-executor",
        "node-replace-executor",
        "state-checkpoint-restore",
        "checkpoint-corrupt-recover",
        "checkpoint-disk-full",
        "replay-window-resync",
        "deploy-broken-image",
    ];

    /// The replica drills kill lane 0 on sequencer-0 and lane 1 on
    /// sequencer-1, so they cover both lanes and both nodes.
    const SEQUENCER_CASES: &'static [&'static str] = &[
        "graceful-sequencer",
        "hard-sequencer",
        "sequencer-replica-kill-lane0-node0",
        "sequencer-replica-kill-lane1-node1",
        "sequencer-lapse",
        "validator-lapse",
        "validator-join",
        "lookup-blackout",
    ];

    /// The cases of the shard, in run order. The sequencer shard runs
    /// the two resize cases last: a resize leaves the shard map at a
    /// later version, so the account-to-shard table no longer pins the
    /// cases after it.
    #[must_use]
    pub fn cases(self) -> &'static [&'static str] {
        match self {
            Self::Executor => Self::EXECUTOR_CASES,
            Self::Ingress => &[
                "graceful-ingress",
                "hard-ingress",
                "archive-driver-loss",
                "archive-tx-data-wipe",
                "archive-corruption",
            ],
            Self::Sequencer => Self::SEQUENCER_CASES,
            Self::Cluster => &[
                "cluster-leader-kill",
                "raft-leader-one-way-partition",
                "raft-leader-packet-loss",
                "raft-leader-packet-delay",
                "cluster-follower-kill",
                "cluster-member-rejoin",
                "cluster-two-blank-recover",
                "node-replace-sealer",
                "cpu-squeeze",
            ],
            // Every replica of one role down at once. Each case waits
            // for the whole fleet to return and then keeps the load on
            // it, so the shard runs its own cluster. The seeded restart of
            // the sealer fleet runs on the nightly shard.
            Self::Fleet => &[
                "executor-fleet-loss-recover",
                "executor-fleet-wipe-recover",
                "executor-fleet-total-wipe-recover",
                "redis-total-loss-recover",
                "cluster-quorum-loss-recover",
                "cluster-total-loss-recover",
            ],
            // Failures that cross the redundancy of a role. The blackout
            // runs last: every pipeline node dies, the longest recovery.
            Self::Coordinated => &[
                "ingress-pair-loss-recover",
                "sequencer-lane-loss-recover",
                "control-plane-loss-recover",
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
            // sealers and the validator, the ingresses, and the read path
            // (Redis and the state mirrors). The executor-and-sealer case
            // runs first and audits the persisted state after it. The
            // sequencer-and-Redis case removes every source of a sender floor.
            Self::CombinedExec => &[
                "executor-sealer-loss-recover",
                "executor-sealer-validator-recover",
                "ingress-executor-loss-recover",
                "read-path-loss-recover",
                "sequencer-executor-redis-loss",
            ],
            Self::Retention => &RETENTION_CASES,
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
            // The loss and the disagreement of the L1 follower's
            // instances, behind the same lying L1. They are a shard of
            // their own: with the liar cases, one shard passed the job's
            // limit.
            Self::Follower => &[
                "l1-transport-refusals",
                "l1-provider-unresponsive",
                "follower-instance-loss",
                "follower-total-loss",
                "follower-disagreement",
            ],
            // The mirror rebuild runs last: it flushes the projection.
            Self::Cache => &[
                "mirror-backlog-client-restart",
                "redis-partition-ingress",
                "redis-primary-kill",
                "redis-primary-freeze",
                "mirror-kill-rebuild",
            ],
            // The nightly shard, on a reference-born cluster: the long
            // repetition cases, too slow for every pull request, and the
            // seeded restart of the sealer fleet. The fleet wipe runs last:
            // it restarts the chain from a state rebuilt from L1, the
            // longest recovery, and a failure there must not hide the
            // storm. The lane resize runs here, last, because the v2 loop
            // of the carry-born shards has no resize protocol yet: it needs
            // the v1 loop of a reference-born cluster, and it leaves the
            // shard map at a later version. The sequencer shard kills one
            // replica of each lane on its node; the other two replica
            // drills run here, first.
            Self::Integrity => &[
                "sequencer-replica-kill-lane0-node1",
                "sequencer-replica-kill-lane1-node0",
                "executor-restart-storm",
                "sealer-fleet-total-wipe-recover",
                "resize-scale-out-in",
            ],
            // The activation of the carry window in the order of the
            // staging rollout, then the faults around its switch. Each
            // step needs the steps before it.
            Self::Carry => &[
                "carry-lockbox",
                "carry-versions-on",
                "carry-deposits-provider-fault",
                "carry-held-offer-after-downgrade",
                "carry-readers-on-egress",
                "carry-window-on",
                "carry-executor-first",
                "carry-executor-second",
                "carry-executor-third",
                "carry-deposits-all-carry",
                "carry-window-off-on",
                "carry-leader-kill-at-switch",
                "carry-egress-restart",
            ],
        }
    }

    /// The deploy-time variables the shard's cluster needs. Every regular
    /// shard is carry-born (see the module doc); [`Self::carry_born`]
    /// adds the switch. The cluster shard deploys the sealer with a short
    /// snapshot interval so the follower-kill case sees a snapshot; the
    /// retention shard deploys a small retention so a freeze can overrun
    /// the window of the sealer; the L1 shard takes both, plus the fault
    /// proxy in front of the followers. The executor, retention and cache
    /// shards turn the block log of the checkpoints on: they run the
    /// checkpoint restore, the peer resync, the retention overrun and the
    /// state mirror rebuild. The other shards keep it off, so both modes
    /// stay tested. The carry shard deploys the member feed, the
    /// executors on the sealer nodes, the egress and the v2 loop of the
    /// sequencers, with a snapshot each minute, so a member that stops
    /// before the switch restores a snapshot, and a retention that the
    /// exact window keeps.
    #[must_use]
    pub fn deploy_vars(self) -> DeployVars {
        DeployVars {
            genesis_carry: self.carry_born(),
            ..self.shard_vars()
        }
    }

    /// Whether the shard brings up a carry-born cluster: every shard but
    /// the two reference-born ones of the module doc.
    #[must_use]
    pub fn carry_born(self) -> bool {
        !matches!(self, Self::Carry | Self::Integrity)
    }

    /// The knobs of the shard's cases: the environment, the shard's
    /// values below it, and the deploy variables of the shard above it,
    /// so the knobs that follow the deployed topology cannot drift from
    /// the deploy.
    ///
    /// # Errors
    ///
    /// Returns an error if a set variable does not parse.
    pub fn knobs(self) -> anyhow::Result<Knobs> {
        Knobs::deployed(self.env(), &self.deploy_vars())
    }

    /// The deploy variables of the shard beyond the genesis switch.
    fn shard_vars(self) -> DeployVars {
        match self {
            Self::Executor | Self::Cache => DeployVars {
                checkpoint_log: true,
                ..DeployVars::default()
            },
            Self::Cluster => DeployVars {
                cluster_snapshot_interval_s: Some(60),
                ..DeployVars::default()
            },
            Self::Retention => DeployVars {
                cluster_snapshot_interval_s: Some(SHORT_SNAPSHOT_S),
                canary_local: Some(true),
                cluster_retention: Some(SMALL_RETENTION),
                checkpoint_log: true,
                ..DeployVars::default()
            },
            Self::L1 => DeployVars {
                cluster_snapshot_interval_s: Some(SHORT_SNAPSHOT_S),
                cluster_retention: Some(SMALL_RETENTION),
                ..Self::Follower.shard_vars()
            },
            Self::Follower => DeployVars {
                cluster_snapshot_interval_s: Some(60),
                cluster_retention: Some(6144),
                l1_fault_proxy: true,
                indexer_poll_s: Some(2),
                da_watcher_silence_s: Some(30),
                ..DeployVars::default()
            },
            Self::Carry => DeployVars {
                l1_fault_proxy: true,
                indexer_poll_s: Some(2),
                da_watcher_silence_s: Some(30),
                cluster_snapshot_interval_s: Some(60),
                cluster_retention: Some(CARRY_RETENTION),
                carry: CarrySwitches {
                    exec_on_sealers: true,
                    egress: EgressStage::On,
                    admission: true,
                    ..CarrySwitches::default()
                },
                ..DeployVars::default()
            },
            Self::Ingress
            | Self::Sequencer
            | Self::Fleet
            | Self::Coordinated
            | Self::CombinedOrdering
            | Self::CombinedExec
            | Self::Integrity => DeployVars::default(),
        }
    }

    /// The settings of the shard beyond the shared chaos knobs, as
    /// `(name, value)` pairs, below the process environment. A setting of
    /// the deploy is not here: [`Self::knobs`] reads it from
    /// [`Self::deploy_vars`]. Every chaos shard runs no load stage
    /// (`RUN_LOAD=0`), which is what lets the resize case take a
    /// load-reserve account on the sequencer shard.
    #[must_use]
    pub fn env(self) -> &'static [(&'static str, &'static str)] {
        match self {
            // Two squeezes of 40 s, past the 30 s Aeron stall tolerance,
            // so sessions lapse and reconnect twice inside the deadline.
            Self::Cluster => &[
                ("RUN_LOAD", "0"),
                ("SQUEEZE_CYCLES", "2"),
                ("SQUEEZE_S", "40"),
                ("SQUEEZE_RELEASE_S", "20"),
                ("SQUEEZE_CPUS_PER_NODE", "0.4"),
            ],
            // The longest wait inside one fault is the stale-post alert:
            // it shows within about 10 s and must hold 30 s. The provider
            // outage of the carry deposit steps outlasts the 30 s halt
            // detection of the followers with a margin.
            Self::L1 | Self::Follower | Self::Carry => &[("RUN_LOAD", "0"), ("L1_FAULT_S", "45")],
            Self::Executor
            | Self::Ingress
            | Self::Sequencer
            | Self::Fleet
            | Self::Coordinated
            | Self::CombinedOrdering
            | Self::CombinedExec
            | Self::Retention
            | Self::Cache
            | Self::Integrity => &[("RUN_LOAD", "0")],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knobs::ExecutorNodes;

    /// Every shard, in the order of the workflow matrix.
    const ALL: [Shard; 14] = [
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
        Shard::Follower,
        Shard::Integrity,
        Shard::Carry,
    ];

    #[test]
    fn every_chaos_case_has_a_component_recovery_plan() {
        let catalog: serde_json::Value =
            serde_json::from_str(include_str!("../../../scripts/ci/component-recovery.json"))
                .unwrap();
        let rows = catalog["cases"].as_array().unwrap();
        let planned: std::collections::BTreeSet<_> = rows
            .iter()
            .map(|row| row["case"].as_str().unwrap())
            .collect();
        let scheduled: std::collections::BTreeSet<_> = ALL
            .iter()
            .flat_map(|shard| shard.cases().iter().copied())
            .collect();
        assert_eq!(planned.len(), rows.len(), "duplicate component plan");
        assert!(
            scheduled.is_subset(&planned),
            "new chaos cases need a local recovery plan"
        );
        assert!(
            planned
                .difference(&scheduled)
                .all(|case| catalog["pending_registration"][case].is_u64())
        );
        assert!(
            rows.iter()
                .all(|row| catalog["families"][row["family"].as_str().unwrap()].is_object())
        );
        rows.iter()
            .flat_map(|row| row["local_tests"].as_array().unwrap())
            .for_each(|test| {
                let suite = &catalog["local_suites"][test["suite"].as_str().unwrap()];
                assert!(suite["tests"].as_array().unwrap().contains(&test["test"]));
            });
    }

    #[test]
    fn every_regular_shard_is_carry_born_and_its_knobs_follow_the_deploy() {
        let reference = [Shard::Carry, Shard::Integrity];
        for shard in ALL {
            let born = !reference.contains(&shard);
            let knobs = shard.knobs().unwrap();
            assert_eq!(shard.carry_born(), born, "{}", shard.name());
            assert_eq!(shard.deploy_vars().genesis_carry, born, "{}", shard.name());
            assert_eq!(knobs.genesis_carry, born, "{}", shard.name());
            // A carry executor sends no recorded cursor.
            assert!(!shard.deploy_vars().exec_cursor, "{}", shard.name());
            // The carry shard deploys the executors on the sealer nodes
            // itself, in reference mode.
            let on_sealers = knobs.executor_nodes == ExecutorNodes::Sealer;
            assert_eq!(
                on_sealers,
                born || shard == Shard::Carry,
                "{}",
                shard.name()
            );
        }
    }

    #[test]
    fn the_retention_shard_funds_the_canary_without_lowering_its_da_guard() {
        let vars = Shard::Retention.deploy_vars();
        assert_eq!(vars.da_lag_budget_blocks, None);
        assert_eq!(vars.canary_local, Some(true));
        assert_eq!(vars.cluster_snapshot_interval_s, Some(SHORT_SNAPSHOT_S));
        assert_eq!(vars.cluster_retention, Some(SMALL_RETENTION));
        assert!(
            ALL.iter()
                .filter(|shard| **shard != Shard::Retention)
                .all(|shard| shard.deploy_vars().canary_local.is_none())
        );
    }

    #[test]
    fn every_shard_lists_distinct_cases_and_the_resize_runs_last() {
        let all: Vec<&str> = ALL.iter().flat_map(|s| s.cases().iter().copied()).collect();
        let mut unique = all.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(all.len(), unique.len(), "a case rides two shards");
        assert_eq!(all.len(), 88);
        assert_eq!(
            Shard::Integrity.cases().last(),
            Some(&"resize-scale-out-in")
        );
        for shard in ALL {
            assert!(
                shard.env().contains(&("RUN_LOAD", "0")),
                "{} runs no load stage",
                shard.name()
            );
        }
        assert!(Shard::L1.deploy_vars().l1_fault_proxy);
        assert!(Shard::Follower.deploy_vars().l1_fault_proxy);
        assert!(Shard::Carry.deploy_vars().l1_fault_proxy);
        assert_eq!(Shard::Follower.env(), Shard::L1.env());
        assert!(!Shard::Follower.audits_after("follower-total-loss"));
        // The shards that turn the block log on tell their cases so, and
        // the others keep it off.
        for shard in [Shard::Executor, Shard::Retention, Shard::Cache] {
            assert!(shard.deploy_vars().checkpoint_log, "{}", shard.name());
            assert!(shard.knobs().unwrap().checkpoint_log, "{}", shard.name());
        }
        assert!(!Shard::Fleet.deploy_vars().checkpoint_log);
        assert!(
            !Shard::L1.audits_after("l1-liar")
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
            Some(&"cluster-total-loss-recover")
        );
        assert_eq!(
            Shard::Integrity.cases(),
            [
                "sequencer-replica-kill-lane0-node1",
                "sequencer-replica-kill-lane1-node0",
                "executor-restart-storm",
                "sealer-fleet-total-wipe-recover",
                "resize-scale-out-in"
            ]
        );
        assert_eq!(Shard::Integrity.name(), "chaos-integrity");
        // The carry shard tells its probes where the executors run, and
        // its cases the retention that the exact window keeps.
        let carry = Shard::Carry.deploy_vars();
        assert!(carry.carry.exec_on_sealers && carry.carry.admission);
        assert_eq!(carry.carry.egress, EgressStage::On);
        assert!(carry.carry.carry_executors.is_empty());
        let knobs = Shard::Carry.knobs().unwrap();
        assert_eq!(knobs.executor_nodes, ExecutorNodes::Sealer);
        assert_eq!(
            knobs.cluster_retention.map(std::num::NonZeroU64::get),
            Some(CARRY_RETENTION)
        );
        assert!(Shard::Carry.audits_after("carry-executor-first"));
        assert_eq!(Shard::Carry.cases().first(), Some(&"carry-lockbox"));
        assert!(Shard::Carry.env().contains(&("L1_FAULT_S", "45")));
    }
}

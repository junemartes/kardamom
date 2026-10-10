//! The cases. Each one holds only its injection and its case-specific
//! assertions; the harness provides the load, the injection gate, and
//! the common tail.

use std::num::NonZeroU32;
use std::time::Duration;

use crate::accounts::Pin;
use crate::harness::Harness;
use crate::knobs::Knobs;

pub(crate) mod archive;
pub(crate) mod cache;
pub(crate) mod chain_status;
pub(crate) mod cluster;
pub(crate) mod combined;
pub(crate) mod component;
pub(crate) mod coordinated;
pub(crate) mod da_lag;
pub(crate) mod da_watcher;
pub(crate) mod deploy;
pub(crate) mod exec_stream;
pub(crate) mod fleet;
pub(crate) mod l1;
pub(crate) mod l1_canary;
pub(crate) mod recorded_cursor;
pub(crate) mod resize;
pub(crate) mod seq_retention;
pub(crate) mod squeeze;
pub(crate) mod validator;

/// Declares `Case`, the CI name of each case, and `ALL`, from one table.
/// A case cannot miss the list that `parse` searches, and the list has no
/// size to keep in step.
macro_rules! cases {
    ($($case:ident => $name:literal,)+) => {
        /// Every case, by its CI name.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Case {
            $($case,)+
        }

        /// Every case, in the order of the table.
        const ALL: &[Case] = &[$(Case::$case,)+];

        impl Case {
            /// The CI name.
            #[must_use]
            pub fn name(self) -> &'static str {
                match self {
                    $(Self::$case => $name,)+
                }
            }
        }
    };
}

cases! {
    GracefulExecutor => "graceful-executor",
    HardExecutor => "hard-executor",
    GracefulIngress => "graceful-ingress",
    HardIngress => "hard-ingress",
    GracefulSequencer => "graceful-sequencer",
    HardSequencer => "hard-sequencer",
    SequencerReplicaKill => "sequencer-replica-kill",
    NodeFailureExecutor => "node-failure-executor",
    NodeReplaceExecutor => "node-replace-executor",
    StateCheckpointRestore => "state-checkpoint-restore",
    ReplayWindowResync => "replay-window-resync",
    DeployBrokenImage => "deploy-broken-image",
    ClusterLeaderKill => "cluster-leader-kill",
    ClusterFollowerKill => "cluster-follower-kill",
    ClusterMemberRejoin => "cluster-member-rejoin",
    NodeReplaceSealer => "node-replace-sealer",
    ClusterQuorumLossRecover => "cluster-quorum-loss-recover",
    ClusterTotalLossRecover => "cluster-total-loss-recover",
    ExecutorFleetLossRecover => "executor-fleet-loss-recover",
    ExecutorFleetWipeRecover => "executor-fleet-wipe-recover",
    ExecutorFleetTotalWipeRecover => "executor-fleet-total-wipe-recover",
    SealerFleetTotalWipeRecover => "sealer-fleet-total-wipe-recover",
    IngressPairLossRecover => "ingress-pair-loss-recover",
    SequencerLaneLossRecover => "sequencer-lane-loss-recover",
    PipelineBlackoutRecover => "pipeline-blackout-recover",
    IngressSequencerLossRecover => "ingress-sequencer-loss-recover",
    IngressSealerLossRecover => "ingress-sealer-loss-recover",
    SequencerSealerLossRecover => "sequencer-sealer-loss-recover",
    IngressSequencerSealerLossRecover => "ingress-sequencer-sealer-loss-recover",
    IngressSequencerSealerReverse => "ingress-sequencer-sealer-reverse",
    ExecutorSealerLossRecover => "executor-sealer-loss-recover",
    ExecutorSealerValidatorRecover => "executor-sealer-validator-recover",
    IngressExecutorLossRecover => "ingress-executor-loss-recover",
    ReadPathLossRecover => "read-path-loss-recover",
    SequencerExecutorRedisLoss => "sequencer-executor-redis-loss",
    ArchiveDriverLoss => "archive-driver-loss",
    ArchiveTxDataWipe => "archive-tx-data-wipe",
    ArchiveCorruption => "archive-corruption",
    SequencerLapse => "sequencer-lapse",
    RetentionOverrun => "retention-overrun",
    RetentionOverrunValidator => "retention-overrun-validator",
    ValidatorLapse => "validator-lapse",
    ValidatorJoin => "validator-join",
    CpuSqueeze => "cpu-squeeze",
    ResizeScaleOutIn => "resize-scale-out-in",
    LookupBlackout => "lookup-blackout",
    RedisPrimaryFreeze => "redis-primary-freeze",
    RedisPrimaryKill => "redis-primary-kill",
    RedisPartitionIngress => "redis-partition-ingress",
    RedisTotalLossRecover => "redis-total-loss-recover",
    MirrorKillRebuild => "mirror-kill-rebuild",
    DaLagHalt => "da-lag-halt",
    CanaryDaLag => "canary-da-lag",
    PruneFloor => "prune-floor",
    L1Liar => "l1-liar",
    L1NullReceipts => "l1-null-receipts",
    TwoDayOutage => "two-day-outage",
    BatcherOutagePastRetention => "batcher-outage-past-retention",
    ExecutorRestartStorm => "executor-restart-storm",
}

impl Case {
    /// The case named `name`.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown name, before any load or account
    /// is spent.
    pub fn parse(name: &str) -> anyhow::Result<Self> {
        ALL.iter()
            .copied()
            .find(|c| c.name() == name)
            .ok_or_else(|| crate::chaos_fail!("unknown chaos case: {name}"))
    }

    /// The case load's rate. The L1 cases run below the steady rate: a
    /// fault that stops the batcher must not push its cursor past the
    /// small egress retention their shard deploys.
    #[must_use]
    pub fn tps(self, k: &Knobs) -> NonZeroU32 {
        match self {
            Self::L1Liar
            | Self::L1NullReceipts
            | Self::TwoDayOutage
            | Self::BatcherOutagePastRetention => k.l1_tps,
            _ => k.tps,
        }
    }

    /// Which funded account the case's load needs.
    #[must_use]
    pub fn pin(self) -> Pin {
        match self {
            Self::SequencerReplicaKill
            | Self::SequencerLapse
            | Self::GracefulSequencer
            | Self::HardSequencer
            | Self::SequencerLaneLossRecover
            | Self::LookupBlackout
            | Self::SequencerExecutorRedisLoss => Pin::Shard0,
            Self::ResizeScaleOutIn => Pin::MovesOnScaleOut,
            _ => Pin::Any,
        }
    }

    /// The per-case load window: the global window, widened to the
    /// case's floor so the load still flows through its whole
    /// inject-and-recover sequence.
    #[must_use]
    pub fn window(self, k: &Knobs) -> Duration {
        let inject = k.inject_delay;
        let floor = match self {
            Self::SequencerReplicaKill => inject + k.restart_slo + Duration::from_mins(1),
            Self::SequencerLapse => inject + k.seq_lapse + Duration::from_mins(1),
            Self::ValidatorLapse => inject + k.validator_lapse + Duration::from_mins(1),
            Self::RetentionOverrun
            | Self::RetentionOverrunValidator
            | Self::DaLagHalt
            | Self::CanaryDaLag
            | Self::PruneFloor => inject + k.retention_freeze_cap + Duration::from_mins(2),
            Self::ResizeScaleOutIn => inject + Duration::from_mins(13),
            // The failed deployment runs to the executor's healthy
            // deadline, then the real manifest replaces three executors.
            Self::DeployBrokenImage => inject + Duration::from_mins(12),
            Self::LookupBlackout => inject + k.restart_slo * 2 + Duration::from_mins(5),
            // The freeze, the election, and the recovery polls.
            Self::RedisPrimaryFreeze | Self::RedisPrimaryKill | Self::RedisPartitionIngress => {
                inject + k.restart_slo + Duration::from_mins(5)
            }
            // The mirrors restart, wait for a checkpoint, and rebuild.
            Self::MirrorKillRebuild => inject + k.restart_slo + Duration::from_mins(10),
            // The node replacement; the Redis loss and three rebuilds; the
            // total wipe, the rebuild from L1 and the install; or the
            // blackout and the return of every job.
            Self::NodeReplaceExecutor
            | Self::RedisTotalLossRecover
            | Self::ExecutorFleetTotalWipeRecover
            | Self::PipelineBlackoutRecover => inject + k.reschedule_slo + Duration::from_mins(7),
            Self::NodeReplaceSealer => {
                inject + k.reschedule_slo + k.rejoin_slo + Duration::from_mins(5)
            }
            // The quiet stop, two rebuilds from L1, a seeded election, a
            // restart without the seed, two executor starts, and the
            // return of every other job.
            Self::SealerFleetTotalWipeRecover => {
                inject + k.reschedule_slo + Duration::from_mins(15)
            }
            // The hold, the staggered return of up to three classes, a
            // full-restart election, and the ingress bind after it.
            Self::IngressSequencerLossRecover
            | Self::IngressSealerLossRecover
            | Self::SequencerSealerLossRecover
            | Self::IngressSequencerSealerLossRecover
            | Self::IngressSequencerSealerReverse => {
                inject + k.reschedule_slo + Duration::from_mins(5)
            }
            // The same, plus the replay of the executors after their
            // return.
            Self::ExecutorSealerLossRecover
            | Self::ExecutorSealerValidatorRecover
            | Self::IngressExecutorLossRecover
            | Self::ReadPathLossRecover
            | Self::SequencerExecutorRedisLoss => {
                inject + k.reschedule_slo + Duration::from_mins(6)
            }
            Self::CpuSqueeze => {
                let cycle = k.squeeze.window + k.squeeze.release;
                inject + cycle * k.squeeze.cycles.get() + Duration::from_secs(90)
            }
            // Three faults, each with its halt and resume waits.
            Self::L1Liar => inject + (k.l1_fault + Duration::from_mins(3)) * 3,
            // One fault, a batcher restart inside it, and the posts after.
            Self::L1NullReceipts => inject + k.l1_fault + k.restart_slo + Duration::from_mins(3),
            // The liar, two restarts, the floor passing, and the resume.
            Self::TwoDayOutage => {
                inject
                    + k.l1_fault
                    + k.restart_slo * 2
                    + k.retention_freeze_cap
                    + Duration::from_mins(5)
            }
            // The freeze until the floor passes, the restart, and the
            // rebuild of the gap after it.
            Self::BatcherOutagePastRetention => {
                inject + k.retention_freeze_cap + k.restart_slo + Duration::from_mins(8)
            }
            // Every round: a job stop, a restart within the SLO, and the
            // convergence of the fleet.
            Self::ExecutorRestartStorm => inject.saturating_add(
                k.restart_slo
                    .saturating_add(Duration::from_mins(1))
                    .saturating_mul(fleet::ROUNDS),
            ),
            _ => Duration::ZERO,
        };
        k.case_window.max(floor)
    }

    /// The load's per-submit retry count. The resize case rolls the
    /// ingress the load submits to, so it gets a wide retry. The
    /// quorum-loss case stalls ordering for about a minute, past the
    /// ingress's 30 s parked-submit timeout; a refused submit leaves a
    /// nonce hole, and every later transaction of that sender then
    /// executes as failed, which the verdict would count as bad receipts.
    /// Each attempt parks up to 30 s at the ingress while the stall
    /// lasts, so six attempts cover the stall; sixty made the case take
    /// 23 minutes and the shard hit its job timeout. A whole-fleet
    /// outage lasts up to the reschedule SLO plus an election, so its
    /// attempts cover that SLO in 30 s parks, plus two.
    #[must_use]
    pub fn load_retry(self, k: &Knobs) -> u32 {
        match self {
            Self::ResizeScaleOutIn => 60,
            // The chain refuses every submit while it is halted, at once,
            // and the load's retry delay grows with the attempt: 120
            // attempts cover a halt of about 24 minutes. The sealer fleet
            // wipe stops the ingresses for the whole rebuild, and a refused
            // submit would leave a nonce hole in the load's sender.
            Self::DaLagHalt | Self::CanaryDaLag | Self::SealerFleetTotalWipeRecover => 120,
            // The submit ingress is dead for the hold and the staggered
            // return, about five minutes, and a dead ingress refuses a
            // connection at once. The retry delay grows by 200 ms per
            // attempt, so ninety attempts span about fourteen minutes.
            // With the executors down and the ingress up, each attempt
            // parks 30 s at the ingress instead, for about the same time.
            Self::IngressSequencerLossRecover
            | Self::IngressSealerLossRecover
            | Self::SequencerSealerLossRecover
            | Self::IngressSequencerSealerLossRecover
            | Self::IngressSequencerSealerReverse
            | Self::ExecutorSealerLossRecover
            | Self::ExecutorSealerValidatorRecover
            | Self::IngressExecutorLossRecover
            | Self::ReadPathLossRecover
            | Self::SequencerExecutorRedisLoss => 90,
            Self::ClusterQuorumLossRecover => 6,
            Self::ClusterTotalLossRecover
            | Self::ExecutorFleetLossRecover
            | Self::ExecutorFleetWipeRecover
            | Self::ExecutorFleetTotalWipeRecover
            | Self::ExecutorRestartStorm
            | Self::IngressPairLossRecover
            | Self::SequencerLaneLossRecover
            | Self::PipelineBlackoutRecover => {
                u32::try_from(k.reschedule_slo.as_secs() / 30).unwrap_or(u32::MAX) + 2
            }
            _ => k.load_retry,
        }
    }

    /// The case body: the injection and the case-specific assertions.
    ///
    /// # Errors
    ///
    /// Returns the case's failure.
    pub async fn run(self, h: &mut Harness) -> anyhow::Result<()> {
        match self {
            Self::GracefulExecutor => component::graceful_executor(h).await,
            Self::HardExecutor => component::hard_executor(h).await,
            Self::GracefulIngress => component::graceful_ingress(h).await,
            Self::HardIngress => component::hard_ingress(h).await,
            Self::GracefulSequencer => component::graceful_sequencer(h).await,
            Self::HardSequencer => component::hard_sequencer(h).await,
            Self::SequencerReplicaKill => component::sequencer_replica_kill(h).await,
            Self::NodeFailureExecutor => component::node_failure_executor(h).await,
            Self::NodeReplaceExecutor => component::node_replace_executor(h).await,
            Self::StateCheckpointRestore => component::state_checkpoint_restore(h).await,
            Self::ReplayWindowResync => component::replay_window_resync(h).await,
            Self::DeployBrokenImage => deploy::broken_image(h).await,
            Self::ClusterLeaderKill => cluster::leader_kill(h).await,
            Self::ClusterFollowerKill => cluster::follower_kill(h).await,
            Self::ClusterMemberRejoin => cluster::member_rejoin(h).await,
            Self::NodeReplaceSealer => cluster::node_replace_sealer(h).await,
            Self::ClusterQuorumLossRecover => cluster::quorum_loss_recover(h).await,
            Self::ClusterTotalLossRecover => fleet::cluster_total_loss_recover(h).await,
            Self::ExecutorFleetLossRecover => fleet::executor_fleet_loss_recover(h).await,
            Self::ExecutorFleetWipeRecover => fleet::executor_fleet_wipe_recover(h).await,
            Self::ExecutorFleetTotalWipeRecover => {
                fleet::executor_fleet_total_wipe_recover(h).await
            }
            Self::SealerFleetTotalWipeRecover => fleet::sealer_fleet_total_wipe_recover(h).await,
            Self::IngressPairLossRecover => coordinated::ingress_pair_loss_recover(h).await,
            Self::SequencerLaneLossRecover => coordinated::sequencer_lane_loss_recover(h).await,
            Self::PipelineBlackoutRecover => coordinated::pipeline_blackout_recover(h).await,
            Self::IngressSequencerLossRecover => combined::INGRESS_SEQUENCER.run(h).await,
            Self::IngressSealerLossRecover => combined::INGRESS_SEALER.run(h).await,
            Self::SequencerSealerLossRecover => combined::SEQUENCER_SEALER.run(h).await,
            Self::IngressSequencerSealerLossRecover => combined::ALL_THREE.run(h).await,
            Self::IngressSequencerSealerReverse => combined::ALL_THREE_REVERSE.run(h).await,
            Self::ExecutorSealerLossRecover => combined::EXECUTOR_SEALER.run(h).await,
            Self::ExecutorSealerValidatorRecover => {
                combined::EXECUTOR_SEALER_VALIDATOR.run(h).await
            }
            Self::IngressExecutorLossRecover => combined::INGRESS_EXECUTOR.run(h).await,
            Self::ReadPathLossRecover => combined::READ_PATH.run(h).await,
            Self::SequencerExecutorRedisLoss => combined::SEQUENCER_EXECUTOR_REDIS.run(h).await,
            Self::ArchiveDriverLoss => archive::driver_loss(h).await,
            Self::ArchiveTxDataWipe => archive::tx_data_wipe(h).await,
            Self::ArchiveCorruption => archive::corruption(h).await,
            Self::SequencerLapse => seq_retention::sequencer_lapse(h).await,
            Self::RetentionOverrun => {
                seq_retention::retention_overrun(h, seq_retention::Victim::Executor).await
            }
            Self::RetentionOverrunValidator => {
                seq_retention::retention_overrun(h, seq_retention::Victim::Validator).await
            }
            Self::ValidatorLapse => validator::lapse(h).await,
            Self::ValidatorJoin => validator::join(h).await,
            Self::CpuSqueeze => squeeze::cpu_squeeze(h).await,
            Self::ResizeScaleOutIn => resize::scale_out_in(h).await,
            Self::LookupBlackout => resize::lookup_blackout(h).await,
            Self::RedisPrimaryFreeze => cache::redis_primary_freeze(h).await,
            Self::RedisPrimaryKill => cache::redis_primary_kill(h).await,
            Self::RedisPartitionIngress => cache::redis_partition_ingress(h).await,
            Self::RedisTotalLossRecover => cache::redis_total_loss_recover(h).await,
            Self::MirrorKillRebuild => cache::mirror_kill_rebuild(h).await,
            Self::DaLagHalt => da_lag::da_lag_halt(h).await,
            Self::CanaryDaLag => l1_canary::canary_da_lag(h).await,
            Self::PruneFloor => da_lag::prune_floor(h).await,
            Self::L1Liar => l1::liar(h).await,
            Self::L1NullReceipts => l1::null_receipts(h).await,
            Self::TwoDayOutage => l1::two_day_outage(h).await,
            Self::BatcherOutagePastRetention => l1::batcher_outage_past_retention(h).await,
            Self::ExecutorRestartStorm => fleet::executor_restart_storm(h).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_case_is_listed_once_and_parses_back() {
        // The table declares the enum and the list together, so every case
        // is in the list. Each one must also have its own name.
        let mut names: Vec<&str> = ALL.iter().map(|c| c.name()).collect();
        assert!(ALL.iter().all(|c| Case::parse(c.name()).unwrap() == *c));
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ALL.len(), "two cases share a name");
    }

    #[test]
    fn every_shard_case_parses_and_names_round_trip() {
        for shard in [
            crate::Shard::Executor,
            crate::Shard::Ingress,
            crate::Shard::Sequencer,
            crate::Shard::Cluster,
            crate::Shard::Fleet,
            crate::Shard::Coordinated,
            crate::Shard::CombinedOrdering,
            crate::Shard::CombinedExec,
            crate::Shard::Retention,
            crate::Shard::Cache,
            crate::Shard::L1,
            crate::Shard::Integrity,
        ] {
            shard
                .cases()
                .iter()
                .for_each(|name| assert_eq!(Case::parse(name).unwrap().name(), *name));
        }
        assert!(Case::parse("sealer-hard").is_err());
    }
}

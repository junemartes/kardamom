//! Load windows cover the fault, recovery and case-owned traffic proof.

use std::time::Duration;

use super::{Case, fleet, raft_partition::PacketLoss, sequencer_recovery};
use crate::knobs::Knobs;

/// A full workload play can take 29 minutes before the runtime proof.
/// This bounds pacing only; deployment and recovery retain their own deadlines.
pub(super) const FULL_DEPLOY_LOAD: Duration = Duration::from_mins(40);

impl Case {
    /// The per-case load window: the global window, widened to the
    /// case's floor so the load still flows through its whole
    /// inject-and-recover sequence.
    #[must_use]
    pub fn window(self, k: &Knobs) -> Duration {
        let inject = k.inject_delay;
        let floor = match self {
            Self::SequencerReplicaKill => {
                // An upper load bound caps at the largest representable duration.
                inject
                    .saturating_add(k.restart_slo.saturating_mul(4))
                    .saturating_add(sequencer_recovery::SequencerRecovery::PROOF_WINDOW)
            }
            Self::RaftLeaderPacketLoss => PacketLoss::window(k),
            Self::RaftLeaderOneWayPartition => {
                inject + k.leader_slo + k.rejoin_slo + Duration::from_mins(4)
            }
            Self::GracefulIngress | Self::HardIngress | Self::IngressPairLossRecover => {
                inject + k.reschedule_slo + Duration::from_mins(3)
            }
            Self::SequencerLapse => inject + k.seq_lapse + k.restart_slo + Duration::from_mins(2),
            Self::ControlPlaneLossRecover => inject
                .saturating_add(k.reschedule_slo)
                .saturating_add(Duration::from_mins(5)),
            Self::ValidatorLapse => {
                inject + k.validator_lapse + k.reschedule_slo + Duration::from_mins(2)
            }
            // The carry proof rolls the egress job twice, compares every
            // retained copy, and waits for posting before the common tail.
            Self::PruneFloor if k.genesis_carry => {
                inject + k.retention_freeze_cap + Duration::from_mins(19)
            }
            Self::RetentionOverrun | Self::RetentionOverrunValidator | Self::PruneFloor => {
                inject + k.retention_freeze_cap + Duration::from_mins(2)
            }
            // Halt, paging and posting recovery each keep their own deadline.
            Self::DaLagHalt => inject
                .saturating_add(k.retention_freeze_cap.saturating_mul(2))
                .saturating_add(Duration::from_mins(8)),
            Self::CanaryDaLag => inject
                .saturating_add(k.retention_freeze_cap.saturating_mul(2))
                .saturating_add(Duration::from_mins(16)),
            // Scale-out and scale-in each submit overlap, ingress and steady jobs.
            Self::ResizeScaleOutIn => inject + Duration::from_mins(30),
            // Two full Ansible passes surround the executor's failed
            // deployment deadline, then the common tail proves recovery.
            Self::DeployBrokenImage => inject
                .saturating_add(k.reschedule_slo)
                .saturating_add(FULL_DEPLOY_LOAD.saturating_mul(2)),
            Self::LookupBlackout => inject + k.restart_slo * 2 + Duration::from_mins(5),
            // The freeze, the election, and the recovery polls.
            Self::RedisPrimaryFreeze | Self::RedisPrimaryKill | Self::RedisPartitionIngress => {
                inject + k.restart_slo + Duration::from_mins(5)
            }
            // The mirrors restart, wait for a checkpoint, and rebuild.
            // The interrupted scan includes two full job starts and a new rebuild.
            Self::MirrorKillRebuild => inject
                .saturating_add(k.restart_slo)
                .saturating_add(Duration::from_mins(25)),
            Self::MirrorBacklogClientRestart => inject + k.restart_slo + Duration::from_mins(15),
            // The node replacement; the Redis loss and three rebuilds; the
            // total wipe, the rebuild from L1 and the install; or the
            // blackout and the return of every job.
            Self::RedisTotalLossRecover
            | Self::ExecutorFleetTotalWipeRecover
            | Self::PipelineBlackoutRecover => inject + k.reschedule_slo + Duration::from_mins(7),
            // Replacing a machine also runs Terraform and the full substrate play.
            Self::NodeReplaceExecutor | Self::NodeReplaceSealer => {
                inject + Duration::from_mins(30) + k.rejoin_slo
            }
            Self::ArchiveCorruption | Self::CheckpointCorruptRecover => {
                inject + Duration::from_mins(15)
            }
            Self::ArchiveDriverLoss | Self::ArchiveTxDataWipe => {
                inject + k.reschedule_slo + k.driver_restart_slo() + Duration::from_mins(5)
            }
            Self::ClusterFollowerKill => inject + Duration::from_mins(9) + k.restart_slo,
            Self::ClusterMemberRejoin => {
                inject + Duration::from_mins(9) + k.restart_slo + k.rejoin_slo
            }
            Self::ClusterQuorumLossRecover => {
                inject + k.reschedule_slo * 2 + Duration::from_mins(6)
            }
            Self::ClusterTwoBlankRecover => inject + Duration::from_mins(10) + k.rejoin_slo * 2,
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
            // Every round: a job stop, a restart within the SLO, and the
            // convergence of the fleet.
            Self::ExecutorRestartStorm => inject.saturating_add(
                k.restart_slo
                    .saturating_add(Duration::from_mins(1))
                    .saturating_mul(fleet::ROUNDS),
            ),
            Self::Carry(step) => step.floor(k),
            _ => Duration::ZERO,
        };
        let floor = floor.max(self.l1_window(k));
        // This is an upper load bound. Unused time stops after recovery.
        let recovery = inject
            .saturating_add(k.restart_slo)
            .saturating_add(k.reschedule_slo)
            .saturating_add(Duration::from_mins(3));
        k.case_window.max(floor).max(recovery)
    }

    fn l1_window(self, k: &Knobs) -> Duration {
        let inject = k.inject_delay;
        match self {
            // Three faults, each with its halt and resume waits.
            Self::L1Liar | Self::L1TransportRefusals => {
                inject + (k.l1_fault + Duration::from_mins(3)) * 3
            }
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
            // The freeze, the resume, and the commit of the last epoch.
            Self::FollowerInstanceLoss => inject + k.l1_fault + Duration::from_mins(5),
            // The stop, the resume, and the commit of the last epoch.
            Self::FollowerTotalLoss => inject + Duration::from_mins(9),
            // The lead, the halt, the heal, and the commit after it.
            Self::FollowerDisagreement => inject + Duration::from_mins(14),
            // The freeze until the floor passes, the restart, and the
            // rebuild of the gap after it.
            Self::BatcherOutagePastRetention => {
                inject + k.retention_freeze_cap + k.restart_slo + Duration::from_mins(8)
            }
            _ => Duration::ZERO,
        }
    }
}

#[cfg(test)]
mod tests;

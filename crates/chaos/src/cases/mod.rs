//! The cases. Each one holds only its injection and its case-specific
//! assertions; the harness provides the load, the injection gate, and
//! the common tail.

use std::time::Duration;

use crate::accounts::Pin;
use crate::harness::Harness;
use crate::knobs::Knobs;

pub(crate) mod archive;
pub(crate) mod cluster;
pub(crate) mod component;
pub(crate) mod resize;
pub(crate) mod seq_retention;
pub(crate) mod squeeze;
pub(crate) mod validator;

/// Every case, by its CI name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Case {
    GracefulExecutor,
    HardExecutor,
    GracefulIngress,
    HardIngress,
    GracefulSequencer,
    HardSequencer,
    SequencerReplicaKill,
    NodeFailureExecutor,
    StateCheckpointRestore,
    ReplayWindowResync,
    ClusterLeaderKill,
    ClusterFollowerKill,
    ClusterMemberRejoin,
    ClusterQuorumLossRecover,
    ArchiveDriverLoss,
    ArchiveTxDataWipe,
    ArchiveCorruption,
    SequencerLapse,
    RetentionOverrun,
    RetentionOverrunValidator,
    ValidatorLapse,
    ValidatorJoin,
    CpuSqueeze,
    ResizeScaleOutIn,
    LookupBlackout,
}

const ALL: [Case; 25] = [
    Case::GracefulExecutor,
    Case::HardExecutor,
    Case::GracefulIngress,
    Case::HardIngress,
    Case::GracefulSequencer,
    Case::HardSequencer,
    Case::SequencerReplicaKill,
    Case::NodeFailureExecutor,
    Case::StateCheckpointRestore,
    Case::ReplayWindowResync,
    Case::ClusterLeaderKill,
    Case::ClusterFollowerKill,
    Case::ClusterMemberRejoin,
    Case::ClusterQuorumLossRecover,
    Case::ArchiveDriverLoss,
    Case::ArchiveTxDataWipe,
    Case::ArchiveCorruption,
    Case::SequencerLapse,
    Case::RetentionOverrun,
    Case::RetentionOverrunValidator,
    Case::ValidatorLapse,
    Case::ValidatorJoin,
    Case::CpuSqueeze,
    Case::ResizeScaleOutIn,
    Case::LookupBlackout,
];

impl Case {
    /// The case named `name`.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown name, before any load or account
    /// is spent.
    pub fn parse(name: &str) -> anyhow::Result<Self> {
        ALL.into_iter()
            .find(|c| c.name() == name)
            .ok_or_else(|| crate::chaos_fail!("unknown chaos case: {name}"))
    }

    /// The CI name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::GracefulExecutor => "graceful-executor",
            Self::HardExecutor => "hard-executor",
            Self::GracefulIngress => "graceful-ingress",
            Self::HardIngress => "hard-ingress",
            Self::GracefulSequencer => "graceful-sequencer",
            Self::HardSequencer => "hard-sequencer",
            Self::SequencerReplicaKill => "sequencer-replica-kill",
            Self::NodeFailureExecutor => "node-failure-executor",
            Self::StateCheckpointRestore => "state-checkpoint-restore",
            Self::ReplayWindowResync => "replay-window-resync",
            Self::ClusterLeaderKill => "cluster-leader-kill",
            Self::ClusterFollowerKill => "cluster-follower-kill",
            Self::ClusterMemberRejoin => "cluster-member-rejoin",
            Self::ClusterQuorumLossRecover => "cluster-quorum-loss-recover",
            Self::ArchiveDriverLoss => "archive-driver-loss",
            Self::ArchiveTxDataWipe => "archive-tx-data-wipe",
            Self::ArchiveCorruption => "archive-corruption",
            Self::SequencerLapse => "sequencer-lapse",
            Self::RetentionOverrun => "retention-overrun",
            Self::RetentionOverrunValidator => "retention-overrun-validator",
            Self::ValidatorLapse => "validator-lapse",
            Self::ValidatorJoin => "validator-join",
            Self::CpuSqueeze => "cpu-squeeze",
            Self::ResizeScaleOutIn => "resize-scale-out-in",
            Self::LookupBlackout => "lookup-blackout",
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
            | Self::LookupBlackout => Pin::Shard0,
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
            Self::SequencerReplicaKill => inject + k.restart_slo + Duration::from_secs(60),
            Self::SequencerLapse => inject + k.seq_lapse + Duration::from_secs(60),
            Self::RetentionOverrun | Self::RetentionOverrunValidator => {
                inject + k.retention_freeze_cap + Duration::from_secs(120)
            }
            Self::ResizeScaleOutIn => inject + Duration::from_mins(13),
            Self::LookupBlackout => inject + k.restart_slo * 2 + Duration::from_secs(300),
            Self::CpuSqueeze => {
                let cycle = k.squeeze.window + k.squeeze.release;
                inject + cycle * k.squeeze.cycles.get() + Duration::from_secs(90)
            }
            _ => Duration::ZERO,
        };
        k.case_window.max(floor)
    }

    /// The load's per-submit retry count. The resize case rolls the
    /// ingress the load submits to, so it gets a wide retry.
    #[must_use]
    pub fn load_retry(self, k: &Knobs) -> u32 {
        match self {
            Self::ResizeScaleOutIn => 60,
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
            Self::StateCheckpointRestore => component::state_checkpoint_restore(h).await,
            Self::ReplayWindowResync => component::replay_window_resync(h).await,
            Self::ClusterLeaderKill => cluster::leader_kill(h).await,
            Self::ClusterFollowerKill => cluster::follower_kill(h).await,
            Self::ClusterMemberRejoin => cluster::member_rejoin(h).await,
            Self::ClusterQuorumLossRecover => cluster::quorum_loss_recover(h).await,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_shard_case_parses_and_names_round_trip() {
        for shard in [
            crate::Shard::Executor,
            crate::Shard::Ingress,
            crate::Shard::Sequencer,
            crate::Shard::Cluster,
            crate::Shard::Retention,
        ] {
            shard
                .cases()
                .iter()
                .for_each(|name| assert_eq!(Case::parse(name).unwrap().name(), *name));
        }
        assert!(Case::parse("sealer-hard").is_err());
    }
}

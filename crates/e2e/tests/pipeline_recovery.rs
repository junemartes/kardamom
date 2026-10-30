//! The ingress, sequencer and coordinated recovery cases of the chaos
//! suite on the local stack: the real ingress pair, sequencers, sealer,
//! executor and validator as processes on one host. Each test injects the
//! fault of one chaos case at the same boundary and judges the pipeline
//! with the checks that end every chaos case. See
//! `e2e::scenarios::recovery`.
//!
//! Prerequisites, as for the chain-semantics suite: the service binaries
//! (`cargo build --bins -p kardamom-ingress -p kardamom-sequencer -p
//! kardamom-executor -p kardamom-validator -p kardamom-state -p
//! kardamom-da-watcher -p kardamom-l1-indexer`), the sealer jar
//! (`just cluster-jar`), the cached aeron-all jar and Anvil.

#![cfg(feature = "full-pipeline-e2e")]

use e2e::harness::StackConfig;
use e2e::scenarios::recovery::{Case, Victim, run_case, with_l1, with_quorum};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn graceful_ingress_drains_returns_and_loses_no_transfer() {
    run_case(
        "graceful-ingress",
        Case::GracefulIngress,
        StackConfig::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn hard_ingress_returns_and_loses_no_transfer() {
    run_case(
        "hard-ingress",
        Case::HardIngress(Victim::CaseIngress),
        StackConfig::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn hard_twin_ingress_returns_and_serves_receipts_it_never_saw() {
    run_case(
        "hard-ingress",
        Case::HardIngress(Victim::Twin),
        StackConfig::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn ingress_pair_loss_returns_both_replicas() {
    run_case(
        "ingress-pair-loss-recover",
        Case::IngressPairLoss,
        StackConfig::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn graceful_sequencer_drains_and_a_cold_start_regains_its_senders() {
    run_case(
        "graceful-sequencer",
        Case::GracefulSequencer,
        StackConfig::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn sequencer_lane_loss_relearns_the_sender_floor() {
    run_case(
        "sequencer-lane-loss-recover",
        Case::SequencerLaneLoss,
        StackConfig::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn pipeline_blackout_restarts_every_process_and_reelects() {
    run_case(
        "pipeline-blackout-recover",
        Case::PipelineBlackout,
        with_quorum(StackConfig {
            archive_durability: true,
            ..with_l1()
        }),
    )
    .await;
}

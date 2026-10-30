//! The coordinated and sequencer recovery cases of the chaos
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
use e2e::scenarios::recovery::{Case, run_case, validated, with_l1};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn ingress_pair_loss_returns_both_replicas() {
    run_case(
        "ingress-pair-loss-recover",
        Case::IngressPairLoss,
        validated(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn graceful_sequencer_drains_and_a_cold_start_regains_its_senders() {
    run_case("graceful-sequencer", Case::GracefulSequencer, validated()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn sequencer_lane_loss_relearns_the_sender_floor() {
    run_case(
        "sequencer-lane-loss-recover",
        Case::SequencerLaneLoss,
        validated(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn pipeline_blackout_restarts_every_process_and_reelects() {
    run_case(
        "pipeline-blackout-recover",
        Case::PipelineBlackout,
        StackConfig {
            archive_durability: true,
            ..with_l1()
        },
    )
    .await;
}

/// The same blackout with Redis and the state mirror on the read path.
/// It runs apart from the L1 stack, so each test stays inside 60 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn pipeline_blackout_rebuilds_the_read_path() {
    run_case(
        "pipeline-blackout-recover",
        Case::PipelineBlackout,
        // No validator: its verdict ends the other blackout test, and the
        // mirror rebuild needs the rest of the 60 s.
        StackConfig {
            archive_durability: true,
            read_path: true,
            ..StackConfig::default()
        },
    )
    .await;
}

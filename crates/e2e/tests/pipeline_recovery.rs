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
//! (`just cluster-jar`), the cached aeron-all jar and Anvil. The sequencer,
//! executor and Redis loss also needs the `floor-seams` feature, on this
//! crate and on the sequencer binary (`cargo build --bins -p
//! kardamom-sequencer --features floor-seams`).

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

/// Every sequencer, the executor and Redis die, so no floor source is
/// left. The sequencers read Redis in their nonce lookup, as deployed,
/// and the case lane runs two replicas. With `--first-floor-from-lookup`
/// (the `floor-seams` feature), a replayed receipt sets no first floor,
/// so the floor comes only from a lookup. This test runs the v1 loop. The
/// chaos case runs the v2 loop: see the next test.
#[cfg(feature = "floor-seams")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn sequencer_executor_redis_loss_looks_every_floor_up() {
    run_case(
        "sequencer-executor-redis-loss",
        Case::SequencerExecutorRedis,
        floor_stack(false),
    )
    .await;
}

/// The same case on the v2 loop, which the deployed shard of the chaos
/// case runs: a row of an unknown sender sets no first state. The catalog
/// does not list this test, so CI does not run it: on the v2 loop, the
/// case load does not land within 30 s of the return in about half the
/// runs, also without the seam.
#[cfg(feature = "floor-seams")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn sequencer_executor_redis_loss_on_the_v2_loop_looks_every_floor_up() {
    run_case(
        "sequencer-executor-redis-loss-v2",
        Case::SequencerExecutorRedis,
        floor_stack(true),
    )
    .await;
}

/// The stack of the sequencer, executor and Redis loss. The returned
/// executor replays the references of the outage, so the archive must
/// keep their transaction data.
#[cfg(feature = "floor-seams")]
fn floor_stack(admission: bool) -> StackConfig {
    StackConfig {
        archive_durability: true,
        read_path: true,
        sequencer_cache: true,
        first_floor_from_lookup: true,
        admission,
        ..validated()
    }
}

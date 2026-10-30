//! The pipeline recovery cases of the chaos suite on the local stack:
//! the real ingress pair, sequencers, sealer, executor and validator, as
//! processes on one host. Each test injects the fault of one chaos case
//! at the same boundary and judges the pipeline with the checks that end
//! every chaos case. See `e2e::scenarios::recovery`.
//!
//! Prerequisites, as for the chain-semantics suite: the service binaries
//! (`cargo build --bins -p kardamom-ingress -p kardamom-sequencer -p
//! kardamom-executor -p kardamom-validator -p kardamom-state`), the
//! sealer jar (`just cluster-jar`) and the cached aeron-all jar.

#![cfg(feature = "full-pipeline-e2e")]

use std::time::Instant;

use e2e::harness::{LocalStack, StackConfig};
use e2e::scenarios::recovery::{Case, Run, cases};

/// The stack of a case whose checks read the L1 epochs: anvil, the
/// bridge, the L1 follower and the da-watcher.
fn with_l1() -> StackConfig {
    StackConfig {
        l1: true,
        ..StackConfig::default()
    }
}

/// The stack of a case that takes the sealer down: three members, so
/// the return closes a real election of a majority.
fn with_quorum(cfg: StackConfig) -> StackConfig {
    StackConfig {
        sealer_members: std::num::NonZeroUsize::new(3).unwrap(),
        ..cfg
    }
}

/// Launch a stack with a validator, run `case` on it, and print the
/// runtime. A failure dumps the process logs.
async fn run_case(name: &'static str, case: Case, cfg: StackConfig) {
    let started = Instant::now();
    // `launch_opt` also brings up the L1 stack; a missing anvil fails the
    // case instead of skipping it.
    let mut stack = LocalStack::launch_opt(StackConfig {
        validator: true,
        ..cfg
    })
    .await
    .expect("stack")
    .expect("the L1 cases need anvil");
    eprintln!(
        "{name}: stack up in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let outcome = match Run::new(&mut stack, name) {
        Ok(run) => run.run(case).await,
        Err(e) => Err(e),
    };
    if let Err(e) = outcome {
        stack.dump_tails();
        panic!("{name}: {e:#}");
    }
    eprintln!(
        "PIPELINE_RECOVERY {name} seconds={:.1}",
        started.elapsed().as_secs_f64()
    );
}

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
    run_case("hard-ingress", Case::HardIngress, StackConfig::default()).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn ingress_and_sequencer_loss_return_in_order() {
    run_case(
        "ingress-sequencer-loss-recover",
        Case::Combined(cases::INGRESS_SEQUENCER),
        with_l1(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn ingress_returns_before_the_sealer_and_waits() {
    run_case(
        "ingress-sealer-loss-recover",
        Case::Combined(cases::INGRESS_SEALER),
        with_quorum(StackConfig::default()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn sequencer_and_sealer_loss_stall_then_return_together() {
    run_case(
        "sequencer-sealer-loss-recover",
        Case::Combined(cases::SEQUENCER_SEALER),
        with_quorum(with_l1()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn ingress_sequencer_and_sealer_return_in_order() {
    run_case(
        "ingress-sequencer-sealer-loss-recover",
        Case::Combined(cases::ALL_THREE),
        with_quorum(with_l1()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-recovery component target"]
async fn ingress_sequencer_and_sealer_return_in_reverse_and_wait() {
    run_case(
        "ingress-sequencer-sealer-reverse",
        Case::Combined(cases::ALL_THREE_REVERSE),
        with_quorum(with_l1()),
    )
    .await;
}

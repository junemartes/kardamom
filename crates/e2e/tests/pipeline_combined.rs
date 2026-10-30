//! The combined outages of the chaos suite on the local stack: two or
//! three of the ingress, sequencer and sealer classes go down together
//! and return in the order of the case. See `e2e::scenarios::recovery`.
//!
//! Prerequisites, as for the chain-semantics suite: the service binaries
//! (`cargo build --bins -p kardamom-ingress -p kardamom-sequencer -p
//! kardamom-executor -p kardamom-validator -p kardamom-state -p
//! kardamom-da-watcher -p kardamom-l1-indexer`), the sealer jar
//! (`just cluster-jar`), the cached aeron-all jar and Anvil.

#![cfg(feature = "full-pipeline-e2e")]

use e2e::scenarios::recovery::{Case, cases, run_case, validated, with_l1, with_quorum};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-combined component target"]
async fn ingress_and_sequencer_loss_return_in_order() {
    run_case(
        "ingress-sequencer-loss-recover",
        Case::Combined(cases::INGRESS_SEQUENCER),
        with_l1(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-combined component target"]
async fn ingress_returns_before_the_sealer_and_waits() {
    run_case(
        "ingress-sealer-loss-recover",
        Case::Combined(cases::INGRESS_SEALER),
        with_quorum(validated()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-combined component target"]
async fn sequencer_and_sealer_loss_stall_then_return_together() {
    run_case(
        "sequencer-sealer-loss-recover",
        Case::Combined(cases::SEQUENCER_SEALER),
        with_quorum(with_l1()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-combined component target"]
async fn ingress_sequencer_and_sealer_return_in_order() {
    run_case(
        "ingress-sequencer-sealer-loss-recover",
        Case::Combined(cases::ALL_THREE),
        with_quorum(with_l1()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the pipeline-combined component target"]
async fn ingress_sequencer_and_sealer_return_in_reverse_and_wait() {
    run_case(
        "ingress-sequencer-sealer-reverse",
        Case::Combined(cases::ALL_THREE_REVERSE),
        with_quorum(with_l1()),
    )
    .await;
}

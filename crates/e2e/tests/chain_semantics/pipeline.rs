// The transaction-pipeline scenarios: nonce ordering, nonce gaps, and
// RPC liveness. This file is included from main.rs (see the header
// there). Shared helpers (`client_timeout`, `target`, …) live in
// main.rs.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s3_nonces_unordered_all_land() {
    let stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    let t = target(&stack);
    nonce_unordered::run(&t, nonce_unordered::Params::default())
        .await
        .expect("S3");
}

/// F02.1: a restarted sequencer regains an established sender through the
/// executor nonce lookup, with no twin to publish a receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s15_restarted_sequencer_regains_an_established_sender() {
    let mut stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    let params = sequencer_restart::Params::default();
    let applied = sequencer_restart::phase_before_restart(&target(&stack), &params)
        .await
        .expect("S15 before restart");

    let sender = sequencer_restart::sender_address(&params).expect("sender");
    let index = stack.sequencer_for(sender);
    stack.restart_sequencer(index).expect("restart sequencer");

    // The restarted replica has a new metrics port. Rebuild the target.
    sequencer_restart::phase_after_restart(&target(&stack), &params, applied)
        .await
        .expect("S15 after restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s4_nonce_gap_is_never_processed() {
    let park = Duration::from_secs(4);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    nonce_gap::run(&t, nonce_gap::Params::default())
        .await
        .expect("S4");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_rpc_endpoints_never_hang() {
    let park = Duration::from_secs(5);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    rpc_liveness::run(&t, rpc_liveness::Params::default())
        .await
        .expect("S5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_connection_cap_refusal_is_prompt() {
    let park = Duration::from_secs(5);
    let cap = 4usize;
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            rpc_max_connections: cap as u32,
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let url = stack.target(client_timeout(park)).expect("target").rpc.url;
    rpc_liveness::connection_cap_refusal(&url, e2e::harness::DEV_CHAIN_ID, cap, park, 1)
        .await
        .expect("S5 cap");
}

/// Regression test for a pending-registry leak, fixed by the
/// Weak-indexed registry. Client-aborted parked submits must leave no
/// registry entries behind: queue depth returns to baseline once the
/// park bound passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_queue_depth_recovers_after_client_aborts() {
    let park = Duration::from_secs(4);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    rpc_liveness::queue_depth_canary(&t, 1, 3)
        .await
        .expect("S5 canary");
}

/// The RPC golden vectors (docs/agents/l1-client-suite-port-spec.md):
/// the whole v0 contract as data. The Target-C `rpc-vectors` case runs
/// the same vectors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn rpc_golden_vectors_hold() {
    let stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    let t = target(&stack);
    rpc_vectors::run(&t, rpc_vectors::Params::default())
        .await
        .expect("rpc-vectors");
}
